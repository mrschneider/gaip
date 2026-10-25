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
use gaip_core::idl::InterfaceRepository;
use gaip_core::lifecycle::TaskRegistry;
use gaip_core::message::{LocateStatus, ReplyStatus, SystemMinor, WireMessage};
use gaip_core::policy::Policies;
use gaip_core::security::{authorize, Credentials, IdentityAssertion};
use gaip_core::{Envelope, GaipError, Ior, MessageType};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::time::Duration;

struct Peer {
    urn: String,
    writer: Arc<Mutex<TcpStream>>,
    ir: Arc<InterfaceRepository>,
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
        let mut w = self.writer.lock().unwrap();
        w.write_all(&bytes)
            .map_err(|e| GaipError::Json(e.to_string()))?;
        w.flush().map_err(|e| GaipError::Json(e.to_string()))
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

    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        match a.as_str() {
            "--host" => host = args.next().unwrap_or(host),
            "--port" => port = args.next().and_then(|p| p.parse().ok()).unwrap_or(port),
            "--name" => name = args.next().unwrap_or(name),
            "--idl" => idl_path = args.next().unwrap_or(idl_path),
            "--key" => key = args.next().unwrap_or(key),
            "--fragment-size" => {
                fragment_size = args
                    .next()
                    .and_then(|p| p.parse().ok())
                    .unwrap_or(fragment_size)
            }
            "-h" | "--help" => {
                println!(
                    "gaip-engine [--host 127.0.0.1] [--port 7610] [--name urn:...] \
                     [--idl schema/intents.idl] [--key secret] [--fragment-size 65536]"
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

    for (i, stream) in listener.incoming().flatten().enumerate() {
        let _ = stream.set_nodelay(true);
        let writer = match stream.try_clone() {
            Ok(s) => s,
            Err(_) => continue,
        };
        let policies = Policies {
            fragment_size,
            ..Policies::default()
        };
        let peer = Arc::new(Peer {
            urn: name.clone(),
            writer: Arc::new(Mutex::new(writer)),
            ir: Arc::new(ir.clone()),
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
        });
        let n = i + 1;
        std::thread::spawn(move || {
            eprintln!("[engine] peer {n} connected");
            serve(peer, stream);
            eprintln!("[engine] peer {n} closed");
        });
    }
}

fn serve(peer: Arc<Peer>, mut stream: TcpStream) {
    let mut buf: Vec<u8> = Vec::new();
    let mut chunk = [0u8; 16384];
    loop {
        match stream.read(&mut chunk) {
            Ok(0) => return,
            Ok(n) => buf.extend_from_slice(&chunk[..n]),
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
        if let Err(e) = peer.ir.validate_payload(&intent, "1.0", &env.payload) {
            peer.reply(
                request_id,
                ReplyStatus::user("E_SCHEMA", e.to_string()),
                None,
            );
            return;
        }
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
            let intents: Vec<Value> = peer
                .ir
                .list()
                .into_iter()
                .map(|(i, v)| {
                    let spec = peer.ir.lookup(i, v).expect("listed");
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
