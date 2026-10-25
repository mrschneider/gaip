//! GAIP reference participant — speaking the full GIOP-style message layer.
//!
//! It exercises everything the wire layer provides:
//! * `Request`/`Reply` around the six agent primitives,
//! * `Fragment` in both directions,
//! * `LocateRequest`/`LocateReply` with `OBJECT_FORWARD`,
//! * `MessageError` and `CloseConnection`,
//! * service contexts (trace, tenant, deadline, identity, route),
//! * `TASK_REQUEST` payloads validated against the IDL repository before
//!   any work starts,
//! * CSIv2 identity assertion + role authorization,
//! * a bidirectional request (the engine asks the caller for context
//!   mid-task over the same connection),
//! * deadlines that refuse work instead of starting it late.

use gaip_core::codec::{decode_wire, encode_wire};
use gaip_core::context::{now_ms, ContextList, ServiceContext};
use gaip_core::flow::Outbox;
use gaip_core::idl::InterfaceRepository;
use gaip_core::interceptor::ServerInterceptor;
use gaip_core::lifecycle::TaskRegistry;
use gaip_core::message::{LocateStatus, ReplyStatus, SystemMinor, WireMessage};
use gaip_core::policy::Policies;
use gaip_core::security::{authorize, Credentials, IdentityAssertion};
use gaip_core::{Envelope, GaipError, Ior, MessageType};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::io::BufReader;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc as StdArc;
use std::sync::{mpsc, Arc, Mutex};
use std::time::Duration;

/// A connection is either plain TCP or TLS; the protocol above it is
/// identical, which is the point of the transport half of CSIv2.
trait ReadWrite: Read + Write + Send {}
impl<T: Read + Write + Send> ReadWrite for T {}

type Io = StdArc<Mutex<Box<dyn ReadWrite>>>;

/// Write half handed to the Outbox: it locks the same connection the
/// reader uses, so a TLS session (which is stateful and cannot be
/// cloned) stays consistent.
struct OutHandle(Io);
impl Write for OutHandle {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().write(buf)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.0.lock().unwrap().flush()
    }
}

struct Peer {
    urn: String,
    /// bounded output queue: a stalled peer must not block the engine
    out: Arc<Outbox>,
    ir: Arc<std::sync::RwLock<InterfaceRepository>>,
    creds: Arc<Credentials>,
    policies: Policies,
    /// fragment runs in progress, keyed by request id
    fragments: Mutex<BTreeMap<u32, Vec<WireMessage>>>,
    /// bidirectional calls the engine started and is waiting on
    waits: Mutex<BTreeMap<u32, mpsc::Sender<WireMessage>>>,
    registry: Mutex<TaskRegistry>,
    cancelled: Mutex<BTreeSet<String>>,
    next_request_id: AtomicU32,
    next_msg: AtomicU64,
    /// simulated relocation target answered with OBJECT_FORWARD
    forward_to: Ior,
    /// portable interceptors: the hooks every request/reply crosses
    recorder: Arc<gaip_core::interceptor::Recorder>,
    /// named locks with leases (CosConcurrency-lite), process-wide
    locks: Arc<Mutex<gaip_core::locks::LockSet>>,
    /// run telemetry with QoS (CosNotification-lite), process-wide
    events: Arc<Mutex<gaip_core::events::EventChannel>>,
}

impl Peer {
    fn msg_id(&self, prefix: &str) -> String {
        format!(
            "{prefix}_{}",
            self.next_msg.fetch_add(1, Ordering::SeqCst) + 1
        )
    }

    fn send_raw(&self, msg: &WireMessage) -> Result<(), GaipError> {
        let bytes = encode_wire(msg)?;
        match self.out.send(bytes) {
            Ok(()) => Ok(()),
            // BACKPRESSURE: the peer is not draining. This becomes a
            // retryable Unavailable reply instead of a blocked write.
            Err(gaip_core::flow::FlowError::Full { queued }) => Err(GaipError::BadField {
                field: "flow",
                why: format!("peer is not draining ({queued} queued)"),
            }),
            Err(gaip_core::flow::FlowError::Closed) => Err(GaipError::BadField {
                field: "flow",
                why: "connection closed".to_string(),
            }),
        }
    }

    /// Send, fragmenting when the message exceeds the policy size.
    fn send(&self, msg: WireMessage) -> Result<(), GaipError> {
        let json_len = msg.to_json()?.len();
        if json_len > self.policies.fragment_size {
            self.policies.check_size(json_len)?;
            for f in msg.fragment(self.policies.fragment_size)? {
                self.send_raw(&f)?;
            }
            return Ok(());
        }
        self.send_raw(&msg)
    }

    fn reply(&self, request_id: u32, status: ReplyStatus, body: Option<Envelope>) {
        // interceptor: after the reply exists, before it is written
        match &status {
            ReplyStatus::SystemException { .. } => {
                self.recorder.receive_exception(request_id, &status);
            }
            _ => self.recorder.send_reply(request_id, &status, body.as_ref()),
        }
        let msg = WireMessage::Reply {
            request_id,
            status,
            body,
        };
        if let Err(e) = self.send(msg) {
            eprintln!("[engine] reply failed: {e}");
        }
    }

    fn reply_ok(&self, request_id: u32, env: Envelope) {
        self.reply(request_id, ReplyStatus::NoException, Some(env));
    }
}

fn main() {
    let mut host = "127.0.0.1".to_string();
    let mut port = 7610u16;
    let mut name = "urn:agent:rust:tokio:analytics-engine-v1".to_string();
    let mut idl_path = "schema/intents.idl".to_string();
    let mut key = "dev-shared-secret".to_string();
    let mut fragment_size = 64 * 1024usize;
    let mut tls_cert: Option<String> = None;
    let mut tls_key: Option<String> = None;
    let mut stats_interval: u64 = 0;

    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        match a.as_str() {
            "--host" => host = args.next().unwrap_or(host),
            "--port" => port = args.next().and_then(|p| p.parse().ok()).unwrap_or(port),
            "--name" => name = args.next().unwrap_or(name),
            "--idl" => idl_path = args.next().unwrap_or(idl_path),
            "--key" => key = args.next().unwrap_or(key),
            "--tls-cert" => tls_cert = args.next(),
            "--stats-interval" => {
                stats_interval = args.next().and_then(|p| p.parse().ok()).unwrap_or(0)
            }
            "--tls-key" => tls_key = args.next(),
            "--fragment-size" => {
                fragment_size = args
                    .next()
                    .and_then(|p| p.parse().ok())
                    .unwrap_or(fragment_size)
            }
            "-h" | "--help" => {
                println!(
                    "gaip-engine [--host 127.0.0.1] [--port 7610] [--name urn:...] \
                     [--idl schema/intents.idl] [--key secret] [--fragment-size 65536] \
                     [--tls-cert cert.pem --tls-key key.pem]"
                );
                return;
            }
            other => {
                eprintln!("unknown argument '{other}'");
                std::process::exit(2);
            }
        }
    }

    let idl = match std::fs::read_to_string(&idl_path) {
        Ok(t) => t,
        Err(e) => {
            eprintln!("gaip-engine: cannot read IDL at {idl_path}: {e}");
            std::process::exit(1);
        }
    };
    let ir = match InterfaceRepository::load(&idl) {
        Ok(ir) => ir,
        Err(e) => {
            eprintln!("gaip-engine: IDL error: {e}");
            std::process::exit(1);
        }
    };

    // CSIv2 transport half: TLS around the same protocol. The identity
    // assertion still proves *who*, TLS proves *with whom*.
    let tls: Option<StdArc<rustls::ServerConfig>> = match (tls_cert, tls_key) {
        (Some(cert_path), Some(key_path)) => {
            let certs: Vec<_> = rustls_pemfile::certs(&mut BufReader::new(
                std::fs::File::open(&cert_path).expect("tls cert"),
            ))
            .collect::<Result<_, _>>()
            .expect("tls cert parse");
            let key = rustls_pemfile::private_key(&mut BufReader::new(
                std::fs::File::open(&key_path).expect("tls key"),
            ))
            .expect("tls key parse")
            .expect("tls key present");
            let cfg = rustls::ServerConfig::builder()
                .with_no_client_auth()
                .with_single_cert(certs, key)
                .expect("tls config");
            Some(StdArc::new(cfg))
        }
        (None, None) => None,
        _ => {
            eprintln!("gaip-engine: --tls-cert and --tls-key go together");
            std::process::exit(2);
        }
    };

    let listener = match TcpListener::bind((host.as_str(), port)) {
        Ok(l) => l,
        Err(e) => {
            eprintln!("gaip-engine: cannot bind {host}:{port}: {e}");
            std::process::exit(1);
        }
    };
    eprintln!(
        "[engine] {name} listening on {host}:{port} ({} intents, fragment {fragment_size} B)",
        ir.len()
    );

    // STATUS SURFACE: the recorder is process-wide (shared across peer
    // threads), and --stats-interval prints it — measurements, not guesses
    let shared_recorder = Arc::new(gaip_core::interceptor::Recorder::default());
    let shared_locks = Arc::new(Mutex::new(gaip_core::locks::LockSet::new()));
    let shared_events = Arc::new(Mutex::new(gaip_core::events::EventChannel::new(
        gaip_core::events::Qos {
            max_events: 256,
            discard: gaip_core::events::Discard::Oldest,
        },
    )));
    if stats_interval > 0 {
        let rec = shared_recorder.clone();
        std::thread::spawn(move || loop {
            std::thread::sleep(std::time::Duration::from_secs(stats_interval));
            let (req, rep, exc) = rec.snapshot();
            eprintln!("[engine stats] requests={req} replies={rep} exceptions={exc}");
        });
    }
    eprintln!(
        "[engine] transport: {}",
        if tls.is_some() {
            "TLS (rustls)"
        } else {
            "plain TCP"
        }
    );
    for (i, stream) in listener.incoming().flatten().enumerate() {
        let _ = stream.set_nodelay(true);
        // a read timeout is what lets the reader release the connection
        // lock periodically (a TLS session cannot be cloned)
        let _ = stream.set_read_timeout(Some(Duration::from_millis(100)));
        let io: Io = match &tls {
            Some(cfg) => match rustls::ServerConnection::new(cfg.clone()) {
                Ok(conn) => StdArc::new(Mutex::new(
                    Box::new(rustls::StreamOwned::new(conn, stream)) as Box<dyn ReadWrite>,
                )),
                Err(e) => {
                    eprintln!("[engine] tls handshake setup failed: {e}");
                    continue;
                }
            },
            None => StdArc::new(Mutex::new(Box::new(stream) as Box<dyn ReadWrite>)),
        };
        let out = Arc::new(Outbox::spawn(OutHandle(io.clone()), 256));
        let policies = Policies {
            fragment_size,
            ..Policies::default()
        };
        let peer = Arc::new(Peer {
            urn: name.clone(),
            out,
            ir: Arc::new(std::sync::RwLock::new(ir.clone())),
            creds: Arc::new(Credentials::new(key.clone().into_bytes())),
            policies,
            fragments: Mutex::new(BTreeMap::new()),
            waits: Mutex::new(BTreeMap::new()),
            registry: Mutex::new(TaskRegistry::new()),
            cancelled: Mutex::new(BTreeSet::new()),
            next_request_id: AtomicU32::new(1),
            next_msg: AtomicU64::new(0),
            forward_to: Ior::new(
                "urn:agent:rust:tokio:analytics-engine:2.0",
                vec!["10.0.0.7:7610".to_string()],
                "1.0",
            )
            .with_capabilities(vec!["INVENTORY_OPTIMIZATION".to_string()]),
            recorder: shared_recorder.clone(),
            locks: shared_locks.clone(),
            events: shared_events.clone(),
        });
        let n = i + 1;
        std::thread::spawn(move || {
            eprintln!("[engine] peer {n} connected");
            serve(peer, io);
            eprintln!("[engine] peer {n} closed");
        });
    }
}

fn serve(peer: Arc<Peer>, io: Io) {
    let mut buf: Vec<u8> = Vec::new();
    let mut chunk = [0u8; 16384];
    loop {
        // hold the lock only for the read, then release it so the Outbox
        // writer can use the same (stateful, TLS) connection
        let read = {
            let mut guard = io.lock().unwrap();
            guard.read(&mut chunk)
        };
        match read {
            Ok(0) => return,
            Ok(n) => buf.extend_from_slice(&chunk[..n]),
            Err(e)
                if e.kind() == std::io::ErrorKind::WouldBlock
                    || e.kind() == std::io::ErrorKind::TimedOut =>
            {
                // STARVATION FIX: without this window the reader wins the
                // lock race every 100 ms and a queued reply waits for the
                // NEXT request — measured: a reply delayed a full second
                std::thread::sleep(Duration::from_millis(5));
                continue;
            }
            Err(_) => return,
        }
        loop {
            match decode_wire(&buf) {
                Ok(Some((msg, used))) => {
                    buf.drain(..used);
                    if !dispatch(&peer, msg) {
                        return;
                    }
                }
                Ok(None) => break,
                Err(e) => {
                    // GIOP answers a bad frame with MessageError instead of
                    // dropping the connection
                    eprintln!("[engine] bad frame: {e}");
                    let _ = peer.send(WireMessage::MessageError {
                        reason: e.to_string(),
                    });
                    buf.clear();
                    break;
                }
            }
        }
    }
}

/// Returns false when the connection must close.
fn dispatch(peer: &Arc<Peer>, msg: WireMessage) -> bool {
    match msg {
        WireMessage::CloseConnection => {
            eprintln!("[engine] peer asked to close");
            false
        }
        WireMessage::MessageError { reason } => {
            eprintln!("[engine] peer reported: {reason}");
            true
        }
        WireMessage::Fragment {
            request_id,
            fragment_num,
            more,
            data,
        } => {
            let frag = WireMessage::Fragment {
                request_id,
                fragment_num,
                more,
                data,
            };
            let assembled = {
                let mut runs = peer.fragments.lock().unwrap();
                let run = runs.entry(request_id).or_default();
                run.push(frag);
                if more {
                    None
                } else {
                    let run = runs.remove(&request_id).unwrap_or_default();
                    Some(WireMessage::reassemble(&run))
                }
            };
            match assembled {
                None => true,
                Some(Ok(inner)) => dispatch(peer, inner),
                Some(Err(e)) => {
                    let _ = peer.send(WireMessage::MessageError {
                        reason: format!("reassembly failed: {e}"),
                    });
                    true
                }
            }
        }
        WireMessage::LocateRequest { request_id, target } => {
            let status = if target == peer.urn {
                LocateStatus::ObjectHere
            } else if target == peer.forward_to.urn {
                // GIOP OBJECT_FORWARD: the agent moved, here is where to
                LocateStatus::ObjectForward {
                    forward: peer.forward_to.clone(),
                }
            } else {
                LocateStatus::UnknownObject
            };
            let _ = peer.send(WireMessage::LocateReply { request_id, status });
            true
        }
        WireMessage::CancelRequest { request_id } => {
            eprintln!("[engine] transport-level cancel of request {request_id}");
            true
        }
        WireMessage::Reply {
            request_id,
            status,
            body,
        } => {
            // a bidirectional answer the engine was waiting for
            let tx = peer.waits.lock().unwrap().remove(&request_id);
            match tx {
                Some(tx) => {
                    let _ = tx.send(WireMessage::Reply {
                        request_id,
                        status,
                        body,
                    });
                }
                None => eprintln!("[engine] unsolicited reply {request_id}"),
            }
            true
        }
        WireMessage::SchemaSync { idl } => {
            // merge a peer's declarations into our repository; a
            // conflicting redefinition is refused, not overwritten
            let mut repo = peer.ir.read().unwrap().clone();
            match gaip_core::schema_sync::merge(&mut repo, &gaip_core::SchemaSync::new("peer", idl))
            {
                Ok(report) => {
                    eprintln!(
                        "[engine] schema sync: {} added, {} unchanged",
                        report.added.len(),
                        report.unchanged.len()
                    );
                    *peer.ir.write().unwrap() = repo;
                }
                Err(e) => {
                    eprintln!("[engine] schema sync refused: {e}");
                    let _ = peer.send(WireMessage::MessageError {
                        reason: format!("schema sync refused: {e}"),
                    });
                }
            }
            true
        }
        WireMessage::LocateReply { request_id, .. } => {
            eprintln!("[engine] locate reply {request_id}");
            true
        }
        WireMessage::Request {
            request_id,
            contexts,
            body,
        } => {
            handle_request(peer, request_id, contexts, body);
            true
        }
    }
}

fn handle_request(peer: &Arc<Peer>, request_id: u32, contexts: Vec<ServiceContext>, env: Envelope) {
    let mut ctx = ContextList::from_vec(contexts);
    ctx.push_route("engine-v1");
    // interceptor: before dispatch, with the right to annotate the context
    peer.recorder.receive_request(&mut ctx, &env);
    // the run telemetry channel: task starts are events with QoS. The
    // operational intents (locks, event pulls) are not tasks and must not
    // pollute the channel — otherwise every pull would feed itself.
    let intent_name = env
        .payload
        .get("intent")
        .and_then(Value::as_str)
        .unwrap_or("");
    let is_operational = matches!(intent_name, "LOCK_ACQUIRE" | "LOCK_RELEASE" | "EVENT_PULL");
    if env.message_type == MessageType::TaskRequest && !is_operational {
        let mut ev = peer.events.lock().unwrap();
        let _ = ev.publish(
            "task_started",
            serde_json::json!({
                "correlation_id": env.correlation_id,
                "intent": intent_name,
            }),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis())
                .unwrap_or(0),
        );
    }

    // POLICY: a deadline that already passed refuses the work
    if let Some(status) = peer.policies.check_deadline(&ctx) {
        peer.reply(request_id, status, None);
        return;
    }

    // CSIv2: verify a presented identity token (signature, then expiry)
    let identity: Option<IdentityAssertion> = match ctx.get(gaip_core::context::id::IDENTITY) {
        Some(token) => {
            let sc = ServiceContext::new(gaip_core::context::id::IDENTITY, token.to_string());
            match peer.creds.verify(&sc) {
                Ok(id) => Some(id),
                Err(e) => {
                    peer.reply(
                        request_id,
                        ReplyStatus::user("E_BAD_IDENTITY", e.to_string()),
                        None,
                    );
                    return;
                }
            }
        }
        None => None,
    };

    if env.message_type == MessageType::TaskRequest {
        let Some(id) = identity.clone() else {
            peer.reply(
                request_id,
                ReplyStatus::user(
                    "E_NO_IDENTITY",
                    "TASK_REQUEST requires an identity assertion",
                ),
                None,
            );
            return;
        };
        if let Err(status) = authorize(&id, "executor") {
            peer.reply(request_id, status, None);
            return;
        }
    }

    if env.recipient != peer.urn {
        peer.reply(
            request_id,
            ReplyStatus::system(
                SystemMinor::ObjectNotExist,
                format!("{} is not this participant", env.recipient),
            ),
            None,
        );
        return;
    }

    // IR: validate the payload against the declared intent before working
    if env.message_type == MessageType::TaskRequest {
        let intent = env
            .payload
            .get("intent")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        if let Err(e) = peer
            .ir
            .read()
            .unwrap()
            .validate_payload(&intent, "1.0", &env.payload)
        {
            peer.reply(
                request_id,
                ReplyStatus::user("E_SCHEMA", e.to_string()),
                None,
            );
            return;
        }
    }

    // OPERATIONAL INTENTS: locks and event pulls are answered inline —
    // they are not tasks and must not enter the lifecycle registry
    let intent = env
        .payload
        .get("intent")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    match intent.as_str() {
        "LOCK_ACQUIRE" => {
            let resource = env
                .payload
                .get("resource")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            let ttl = env
                .payload
                .get("ttl_ms")
                .and_then(Value::as_u64)
                .unwrap_or(30_000);
            let mode = env
                .payload
                .get("mode")
                .and_then(Value::as_str)
                .unwrap_or("write")
                .to_string();
            let granted = {
                let mut locks = peer.locks.lock().unwrap();
                if mode == "read" {
                    locks.acquire_read(&env.sender, ttl)
                } else {
                    locks.acquire_write(&env.sender, ttl)
                }
            };
            if !granted {
                peer.reply(
                    request_id,
                    ReplyStatus::user(
                        "E_LOCKED",
                        format!("{resource} is locked by another holder"),
                    ),
                    None,
                );
                return;
            }
            peer.reply_ok(
                request_id,
                env.reply_to(
                    peer.msg_id("msg"),
                    json!({"intent": "LOCK_ACQUIRE", "resource": resource,
                           "granted": true, "holder": env.sender, "ttl_ms": ttl}),
                ),
            );
            return;
        }
        "LOCK_RELEASE" => {
            let resource = env
                .payload
                .get("resource")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            let released = {
                let mut locks = peer.locks.lock().unwrap();
                locks.release(&env.sender)
            };
            peer.reply_ok(
                request_id,
                env.reply_to(
                    peer.msg_id("msg"),
                    json!({"intent": "LOCK_RELEASE", "resource": resource, "released": released}),
                ),
            );
            return;
        }
        "EVENT_PULL" => {
            let after = env
                .payload
                .get("after_seq")
                .and_then(Value::as_u64)
                .unwrap_or(0);
            let (events, batch) = {
                let ev = peer.events.lock().unwrap();
                ev.drain_after(after)
            };
            peer.reply_ok(
                request_id,
                env.reply_to(
                    peer.msg_id("msg"),
                    json!({
                        "intent": "EVENT_PULL",
                        "events": events.iter().map(|e| json!({
                            "seq": e.seq, "kind": e.kind, "body": e.body, "at_ms": e.at_ms,
                        })).collect::<Vec<_>>(),
                        "last_seq": batch.last_seq, "gap": batch.gap,
                    }),
                ),
            );
            return;
        }
        _ => {}
    }

    {
        let mut reg = peer.registry.lock().unwrap();
        if let Err(e) = reg.accept(&env) {
            peer.reply(
                request_id,
                ReplyStatus::user("E_LIFECYCLE", e.to_string()),
                None,
            );
            return;
        }
    }

    match env.message_type {
        MessageType::CapabilityQuery => {
            let ir = peer.ir.read().unwrap();
            let intents: Vec<Value> = ir
                .list()
                .into_iter()
                .map(|(i, v)| {
                    let spec = ir.lookup(i, v).expect("listed");
                    json!({
                        "intent": i,
                        "version": v,
                        "fields": spec.fields.iter().map(|f| json!({
                            "name": f.name, "type": f.kind.name(), "optional": f.optional
                        })).collect::<Vec<_>>(),
                    })
                })
                .collect();
            let ior = Ior::new(peer.urn.clone(), vec!["this-connection".to_string()], "1.0");
            let mut reply = env.reply_to(
                peer.msg_id("msg"),
                json!({
                    "capabilities": ["INVENTORY_OPTIMIZATION", "DEMAND_FORECAST"],
                    "intents": intents,
                    "protocol_versions": ["1.0"],
                    "ior": ior.to_string(),
                    "echo_route": ctx.route(),
                    "echo_trace": ctx.trace_id(),
                    "echo_tenant": ctx.tenant(),
                }),
            );
            reply.payload["echo_contexts"] = json!(ctx.len());
            peer.reply(request_id, ReplyStatus::NoException, Some(reply));
        }
        MessageType::TaskRequest => {
            let needs_human = env
                .payload
                .get("requires_human")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            if needs_human {
                let pause = Envelope::new(
                    peer.msg_id("msg"),
                    env.correlation_id.clone(),
                    peer.urn.clone(),
                    env.sender.clone(),
                    MessageType::HumanIntervent,
                    json!({"reason": "production write", "approve_with": {"decision": "APPROVE"}}),
                );
                peer.reply_ok(request_id, pause);
                return;
            }
            let p = peer.clone();
            let e = env.clone();
            let c = ctx.clone();
            std::thread::spawn(move || run_task(p, request_id, e, c));
        }
        MessageType::TaskReply => {
            // a human decision on a paused task
            if let Some(decision) = env.payload.get("decision").and_then(Value::as_str) {
                let resume = Envelope::new(
                    peer.msg_id("msg"),
                    env.correlation_id.clone(),
                    env.recipient.clone(),
                    env.sender.clone(),
                    MessageType::TaskRequest,
                    json!({"intent": "INVENTORY_OPTIMIZATION", "facility_id": 0,
                           "horizon_days": 0, "artifacts_ref": [],
                           "resumed": true, "decision": decision}),
                );
                let p = peer.clone();
                let c = ctx.clone();
                std::thread::spawn(move || run_task(p, request_id, resume, c));
            } else {
                peer.reply(
                    request_id,
                    ReplyStatus::user("E_UNEXPECTED", "reply without a decision"),
                    None,
                );
            }
        }
        MessageType::CancelRequest => {
            peer.cancelled
                .lock()
                .unwrap()
                .insert(env.correlation_id.clone());
            peer.reply_ok(
                request_id,
                env.reply_to(peer.msg_id("msg"), json!({"status": "CANCELLED"})),
            );
        }
        MessageType::TaskStream | MessageType::HumanIntervent => {
            peer.reply(
                request_id,
                ReplyStatus::user("E_DIRECTION", "this primitive is raised by the engine"),
                None,
            );
        }
    }
}

fn run_task(peer: Arc<Peer>, request_id: u32, req: Envelope, ctx: ContextList) {
    let task = req.correlation_id.clone();
    let deadline = ctx.deadline_unix_ms();

    // BIDIRECTIONAL GIOP: ask the CALLER for context over the same
    // connection, mid-task
    let mut context_fetched = false;
    if !peer.cancelled.lock().unwrap().contains(&task) {
        let rid = peer.next_request_id.fetch_add(1, Ordering::SeqCst);
        let (tx, rx) = mpsc::channel();
        peer.waits.lock().unwrap().insert(rid, tx);
        let ask = Envelope::new(
            peer.msg_id("msg"),
            format!("{task}#ctx"),
            peer.urn.clone(),
            req.sender.clone(),
            MessageType::TaskRequest,
            json!({"intent": "FETCH_CONTEXT", "key": "warehouse_layout"}),
        );
        if peer.send(WireMessage::request(rid, ask)).is_ok() {
            if let Ok(WireMessage::Reply {
                status,
                body: Some(reply),
                ..
            }) = rx.recv_timeout(Duration::from_secs(3))
            {
                if status.is_ok() {
                    context_fetched = reply.payload.get("value").is_some();
                }
            }
        }
        peer.waits.lock().unwrap().remove(&rid);
    }

    for (progress, stage) in [(34u32, "loading"), (67, "solving"), (100, "materialising")] {
        std::thread::sleep(Duration::from_millis(200));
        if peer.cancelled.lock().unwrap().contains(&task) {
            return; // the ack already went out
        }
        if let Some(d) = deadline {
            if now_ms() >= d {
                peer.reply(
                    request_id,
                    ReplyStatus::system(SystemMinor::Timeout, "deadline passed mid-task"),
                    None,
                );
                return;
            }
        }
        let stream_env = req.stream_to(
            peer.msg_id("msg"),
            json!({"progress": progress, "stage": stage}),
        );
        peer.reply(request_id, ReplyStatus::NoException, Some(stream_env));
    }

    // echo the note back so a large request produces a large (fragmented)
    // reply as well
    let note = req
        .payload
        .get("note")
        .cloned()
        .unwrap_or(Value::String(String::new()));
    let reply = req.reply_to(
        peer.msg_id("msg"),
        json!({
            "status": "OK",
            "result": {"projected_saving_pct": 12.4, "rebalance_units": 1830},
            "artifacts_ref": ["s3://enterprise-data/plan_402.parquet"],
            "context_fetched": context_fetched,
            "echo_note": note,
            "route": ctx.route(),
        }),
    );
    peer.reply(request_id, ReplyStatus::NoException, Some(reply));
}
