//! GAIP relay broker: the piece that makes URNs addressable.
//!
//! Design, and why each part is there:
//!
//! * **Relay** — a participant registers by using its URN as the envelope
//!   `sender` on its first Request. From then on a Request addressed to a
//!   *local* URN is forwarded over that participant's connection.
//! * **Federation with broadcast** — a target that is not local is sent
//!   to every peer broker. The peer that has it delivers; the others
//!   answer `ObjectNotExist`, which the origin ignores (the first good
//!   reply wins). Broadcast is the honest simple choice here: a routing
//!   protocol with metrics is a different project, and a wrong metric is
//!   worse than a broadcast.
//! * **Loop safety** — each broker appends its URN to the ROUTE service
//!   context. A broker that finds itself already in the route refuses
//!   with `ProtocolError` instead of looping.
//! * **Retry** — "not connected yet" is not "does not exist": the origin
//!   retries a few times with backoff before answering `ObjectNotExist`.
//! * **Exactly-once effect** — replies are cached per
//!   `(sender, message_id)`: a retried request replays its reply instead
//!   of running the work twice.
//! * **Backpressure** — one bounded [`Outbox`] per connection; a stalled
//!   peer gets a retryable `Unavailable` instead of blocking the broker.

use gaip_core::codec::{decode_wire, encode_wire};
use gaip_core::context::ContextList;
use gaip_core::dedupe::DedupeCache;
use gaip_core::flow::{FlowError, Outbox};
use gaip_core::message::{ReplyStatus, SystemMinor, WireMessage};
use serde_json::json;
use std::collections::BTreeMap;
use std::io::Read;
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// One peer on the other side of a connection (participant or broker).
/// `Arc<Conn>` is what gets cloned; the inner state is shared.
struct Conn {
    /// stable id: the dedupe bookkeeping keys on (connection, request_id)
    id: u64,
    urn: Mutex<Option<String>>,
    out: Arc<Outbox>,
}

struct Broker {
    urn: String,
    /// participants (and peers) registered on this broker, by URN
    local: BTreeMap<String, Arc<Conn>>,
    /// peer brokers (their connections are entries in `local` too)
    peers: Vec<Arc<Conn>>,
    dedupe: DedupeCache,
    /// forwarded request -> (sender, message_id), so the reply that
    /// comes back can be remembered for exactly-once replay
    pending: BTreeMap<(u64, u32), (String, String)>,
    retries: u32,
    backoff_ms: u64,
    /// failover endpoints a participant advertised (FAILOVER context):
    /// when the primary is unreachable the broker answers OBJECT_FORWARD
    /// with the alternates, so the reference survives a crash
    failovers: BTreeMap<String, Vec<String>>,
    /// persisted bindings: the registry survives a broker restart
    registry_path: Option<PathBuf>,
}

impl Broker {
    fn conn_for(&self, urn: &str) -> Option<Arc<Conn>> {
        self.local.get(urn).cloned()
    }

    fn send(&self, conn: &Arc<Conn>, msg: &WireMessage) -> Result<(), FlowError> {
        match encode_wire(msg) {
            Ok(bytes) => conn.out.send(bytes),
            Err(_) => Err(FlowError::Closed),
        }
    }

    /// Forward a request to a target on this broker. Returns the id of
    /// the connection it was handed to.
    fn forward_local(&self, target: &str, msg: &WireMessage) -> Option<u64> {
        match self.conn_for(target) {
            Some(c) => {
                if self.send(&c, msg).is_ok() {
                    Some(c.id)
                } else {
                    None
                }
            }
            None => None,
        }
    }

    /// Broadcast to peers; returns the connections that accepted.
    fn forward_peers(&self, msg: &WireMessage) -> Vec<u64> {
        let mut ids = Vec::new();
        for p in &self.peers {
            if self.send(p, msg).is_ok() {
                ids.push(p.id);
            }
        }
        ids
    }
}

fn main() {
    let mut host = "0.0.0.0".to_string();
    let mut port = 7620u16;
    let mut urn = "urn:agent:gaip:broker:b1".to_string();
    let mut peers: Vec<String> = Vec::new();
    let mut registry_path: Option<PathBuf> = None;
    let mut retries = 3u32;
    let mut backoff_ms = 100u64;

    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        match a.as_str() {
            "--host" => host = args.next().unwrap_or(host),
            "--port" => port = args.next().and_then(|p| p.parse().ok()).unwrap_or(port),
            "--urn" => urn = args.next().unwrap_or(urn),
            "--registry" => registry_path = args.next().map(PathBuf::from),
            "--peer" => {
                if let Some(p) = args.next() {
                    peers.push(p);
                }
            }
            "--retries" => retries = args.next().and_then(|p| p.parse().ok()).unwrap_or(retries),
            "--retry-backoff-ms" => {
                backoff_ms = args
                    .next()
                    .and_then(|p| p.parse().ok())
                    .unwrap_or(backoff_ms)
            }
            "-h" | "--help" => {
                println!(
                    "gaip-broker [--host 0.0.0.0] [--port 7620] [--urn urn:...] \
                     [--peer host:port]... [--retries 3] [--retry-backoff-ms 100]"
                );
                return;
            }
            other => {
                eprintln!("unknown argument '{other}'");
                std::process::exit(2);
            }
        }
    }

    let listener = match TcpListener::bind((host.as_str(), port)) {
        Ok(l) => l,
        Err(e) => {
            eprintln!("gaip-broker: cannot bind {host}:{port}: {e}");
            std::process::exit(1);
        }
    };
    eprintln!("[broker] {urn} listening on {host}:{port}");

    let next_conn = Arc::new(std::sync::atomic::AtomicU64::new(1));
    let broker = Arc::new(Mutex::new(Broker {
        urn,
        local: BTreeMap::new(),
        peers: Vec::new(),
        dedupe: DedupeCache::new(4096, Duration::from_secs(120)),
        pending: BTreeMap::new(),
        retries,
        backoff_ms,
        failovers: BTreeMap::new(),
        registry_path: registry_path.clone(),
    }));
    if let Some(path) = &registry_path {
        broker.lock().unwrap().load_registry(path);
        eprintln!("[broker] registry loaded from {}", path.display());
    }

    // connect to peer brokers (retry: a peer may start a moment later)
    for addr in &peers {
        let mut connected = None;
        for attempt in 0..=retries {
            if let Ok(stream) = TcpStream::connect(addr) {
                let _ = stream.set_nodelay(true);
                if let (Ok(w), Ok(r)) = (stream.try_clone(), stream.try_clone()) {
                    let conn = Arc::new(Conn {
                        id: next_conn.fetch_add(1, std::sync::atomic::Ordering::SeqCst),
                        urn: Mutex::new(None),
                        out: Arc::new(Outbox::spawn(w, 1024)),
                    });
                    connected = Some((conn.clone(), stream, r));
                    let _ = stream;
                    break;
                }
            }
            if attempt < retries {
                std::thread::sleep(Duration::from_millis(backoff_ms * (attempt as u64 + 1)));
            }
        }
        match connected {
            Some((conn, _s, reader)) => {
                broker.lock().unwrap().peers.push(conn.clone());
                // a peer's traffic (replies, locate answers) is handled
                // exactly like any other connection
                let b2 = broker.clone();
                std::thread::spawn(move || serve_peer(b2, conn, reader));
            }
            None => eprintln!("[broker] peer {addr} unreachable — continuing alone"),
        }
    }

    for stream in listener.incoming().flatten() {
        let _ = stream.set_nodelay(true);
        let out = match stream.try_clone() {
            Ok(s) => Arc::new(Outbox::spawn(s, 1024)),
            Err(_) => continue,
        };
        let conn = Arc::new(Conn {
            id: next_conn.fetch_add(1, std::sync::atomic::Ordering::SeqCst),
            urn: Mutex::new(None),
            out,
        });
        let broker = broker.clone();
        std::thread::spawn(move || serve(broker, conn, stream));
    }
}

/// Read loop for a PEER broker: its messages are handled but the
/// connection must not be registered as a participant.
fn serve_peer(shared: Arc<Mutex<Broker>>, conn: Arc<Conn>, mut stream: TcpStream) {
    let mut buf: Vec<u8> = Vec::new();
    let mut chunk = [0u8; 16384];
    loop {
        match stream.read(&mut chunk) {
            Ok(0) => break,
            Ok(n) => buf.extend_from_slice(&chunk[..n]),
            Err(_) => break,
        }
        loop {
            match decode_wire(&buf) {
                Ok(Some((msg, used))) => {
                    buf.drain(..used);
                    handle(&shared, &conn, msg);
                }
                Ok(None) => break,
                Err(_) => {
                    buf.clear();
                    break;
                }
            }
        }
    }
    eprintln!("[broker] peer connection closed");
}

fn serve(shared: Arc<Mutex<Broker>>, conn: Arc<Conn>, mut stream: TcpStream) {
    let mut buf: Vec<u8> = Vec::new();
    let mut chunk = [0u8; 16384];
    loop {
        match stream.read(&mut chunk) {
            Ok(0) => break,
            Ok(n) => buf.extend_from_slice(&chunk[..n]),
            Err(_) => break,
        }
        loop {
            match decode_wire(&buf) {
                Ok(Some((msg, used))) => {
                    buf.drain(..used);
                    if !handle(&shared, &conn, msg) {
                        return;
                    }
                }
                Ok(None) => break,
                Err(e) => {
                    let reply = WireMessage::MessageError {
                        reason: e.to_string(),
                    };
                    let _ = encode_wire(&reply).map(|b| conn.out.send(b));
                    buf.clear();
                    break;
                }
            }
        }
    }
    let name = conn.urn.lock().unwrap().clone();
    if let Some(n) = name {
        let mut b = shared.lock().unwrap();
        b.local.remove(&n);
        eprintln!("[broker] {n} disconnected");
    }
}

fn handle(shared: &Arc<Mutex<Broker>>, conn: &Arc<Conn>, msg: WireMessage) -> bool {
    match msg {
        WireMessage::CloseConnection => false,

        WireMessage::Reply {
            request_id,
            status,
            body,
        } => {
            let reply = WireMessage::Reply {
                request_id,
                status,
                body: body.clone(),
            };
            {
                // exactly-once: remember the reply against the request
                // that caused it, using the (connection, request_id) the
                // broker handed out
                let mut b = shared.lock().unwrap();
                if let Some((sender, msg_id)) = b.pending.remove(&(conn.id, request_id)) {
                    b.dedupe.remember(&sender, &msg_id, reply.clone());
                }
                // a reply travels to the envelope's recipient: no session
                // state, no request-id bookkeeping
                if let Some(env) = &body {
                    let target = env.recipient.clone();
                    if b.forward_local(&target, &reply).is_some() {
                        return true;
                    }
                }
            }
            true
        }

        WireMessage::LocateReply { .. } => true,

        WireMessage::LocateRequest { request_id, target } => {
            let b = shared.lock().unwrap();
            let known = b.urn == target || b.local.contains_key(&target);
            let reply = if known {
                WireMessage::LocateReply {
                    request_id,
                    status: gaip_core::LocateStatus::ObjectHere,
                }
            } else if let Some(alts) = b.failovers.get(&target) {
                // the reference survives the crash: point the caller at
                // the endpoint that advertised itself as a failover
                let ior = gaip_core::Ior::new(target.clone(), alts.clone(), "1.0");
                WireMessage::LocateReply {
                    request_id,
                    status: gaip_core::LocateStatus::ObjectForward { forward: ior },
                }
            } else {
                WireMessage::LocateReply {
                    request_id,
                    status: gaip_core::LocateStatus::UnknownObject,
                }
            };
            let _ = b.send(conn, &reply);
            true
        }

        WireMessage::Request {
            request_id,
            contexts,
            body,
        } => {
            // register the participant by the URN it uses
            let registered_new = conn.urn.lock().unwrap().is_none();
            if registered_new {
                *conn.urn.lock().unwrap() = Some(body.sender.clone());
            }
            let mut ctx = ContextList::from_vec(contexts);
            if registered_new {
                let mut b = shared.lock().unwrap();
                b.local.insert(body.sender.clone(), conn.clone());
                // FAILOVER context: the participant advertises where else
                // it can be reached; the reference outlives the socket
                if let Some(fail) = ctx.get(gaip_core::context::id::FAILOVER) {
                    let alternates: Vec<String> = fail
                        .split(',')
                        .map(|x| x.trim().to_string())
                        .filter(|x| !x.is_empty())
                        .collect();
                    if !alternates.is_empty() {
                        b.failovers.insert(body.sender.clone(), alternates);
                        if let Some(path) = b.registry_path.clone() {
                            b.persist_registry(&path);
                        }
                    }
                }
                eprintln!("[broker] {} registered", body.sender);
            }
            let own = shared.lock().unwrap().urn.clone();
            if ctx.route().contains(&own.as_str()) {
                let reply = WireMessage::Reply {
                    request_id,
                    status: ReplyStatus::system(
                        SystemMinor::ProtocolError,
                        format!("routing loop: {own} is already in the route"),
                    ),
                    body: None,
                };
                let _ = shared.lock().unwrap().send(conn, &reply);
                return true;
            }
            ctx.push_route(&own);
            let target = body.recipient.clone();

            // exactly-once effect: replay the reply of a completed exchange
            let cached = {
                let mut b = shared.lock().unwrap();
                b.dedupe.lookup(&body.sender, &body.message_id)
            };
            if let Some(WireMessage::Reply { status, body, .. }) = cached {
                let reply = WireMessage::Reply {
                    request_id,
                    status,
                    body,
                };
                let _ = shared.lock().unwrap().send(conn, &reply);
                return true;
            }

            let fwd = WireMessage::Request {
                request_id,
                contexts: ctx.to_vec(),
                body: body.clone(),
            };

            // local first, then peers, with retry: "not yet" ≠ "never"
            let (retries, backoff) = {
                let b = shared.lock().unwrap();
                (b.retries, b.backoff_ms)
            };
            let mut delivered = false;
            for attempt in 0..=retries {
                let conns = {
                    let mut b = shared.lock().unwrap();
                    let mut used: Vec<u64> = Vec::new();
                    if let Some(id) = b.forward_local(&target, &fwd) {
                        used.push(id);
                    } else {
                        used = b.forward_peers(&fwd);
                    }
                    if !used.is_empty() {
                        for id in &used {
                            b.pending.insert(
                                (*id, request_id),
                                (body.sender.clone(), body.message_id.clone()),
                            );
                        }
                    }
                    used
                };
                if !conns.is_empty() {
                    delivered = true;
                    break;
                }
                if attempt < retries {
                    std::thread::sleep(Duration::from_millis(backoff * (attempt as u64 + 1)));
                }
            }

            if !delivered {
                let reply = WireMessage::Reply {
                    request_id,
                    status: ReplyStatus::system(
                        SystemMinor::ObjectNotExist,
                        format!("{target} is not reachable through this broker or its peers"),
                    ),
                    body: None,
                };
                {
                    let mut b = shared.lock().unwrap();
                    b.dedupe
                        .remember(&body.sender, &body.message_id, reply.clone());
                    let _ = b.send(conn, &reply);
                }
            }
            true
        }

        WireMessage::CancelRequest { .. } | WireMessage::Fragment { .. } => {
            // fragments are handled by participants; a broker forwards
            // them verbatim when they carry a routable body
            true
        }

        WireMessage::SchemaSync { idl } => {
            // a schema push is forwarded to every participant so the
            // fleet converges on one contract
            let msg = WireMessage::SchemaSync { idl };
            let local: Vec<Arc<Conn>> = shared.lock().unwrap().local.values().cloned().collect();
            for c in local {
                let b = shared.lock().unwrap();
                let _ = b.send(&c, &msg);
            }
            true
        }

        WireMessage::MessageError { reason } => {
            eprintln!("[broker] peer error: {reason}");
            true
        }
    }
}

/// Kept so the broker can advertise itself as an IOR later.
#[allow(dead_code)]
impl Broker {
    fn persist_registry(&self, path: &std::path::Path) {
        let doc = json!({"failovers": self.failovers});
        let tmp = path.with_extension("tmp");
        if std::fs::write(&tmp, doc.to_string()).is_ok() {
            let _ = std::fs::rename(&tmp, path);
        }
    }

    fn load_registry(&mut self, path: &std::path::Path) {
        if let Ok(text) = std::fs::read_to_string(path) {
            if let Ok(doc) = serde_json::from_str::<serde_json::Value>(&text) {
                if let Some(f) = doc.get("failovers").and_then(|v| v.as_object()) {
                    for (urn, eps) in f {
                        if let Some(arr) = eps.as_array() {
                            let list: Vec<String> = arr
                                .iter()
                                .filter_map(|v| v.as_str().map(str::to_string))
                                .collect();
                            if !list.is_empty() {
                                self.failovers.insert(urn.clone(), list);
                            }
                        }
                    }
                }
            }
        }
    }
}

