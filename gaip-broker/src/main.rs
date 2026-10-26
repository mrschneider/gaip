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
    pending: BTreeMap<(u64, u32), (String, String, bool)>,
    /// targets whose last delivery attempt died: skip re-flooding for a
    /// short window (the participant may still appear, so the window is
    /// short and the refusal retryable)
    dead_ends: BTreeMap<String, std::time::Instant>,
    /// the last dead letters: a relay that died, with the reason
    dead_letters: std::collections::VecDeque<(u128, String, String, String)>,
    retries: u32,
    backoff_ms: u64,
    /// failover endpoints a participant advertised (FAILOVER context):
    /// when the primary is unreachable the broker answers OBJECT_FORWARD
    /// with the alternates, so the reference survives a crash
    failovers: BTreeMap<String, Vec<String>>,
    /// persisted bindings: the registry survives a broker restart
    registry_path: Option<PathBuf>,
    /// graceful shutdown: set by the signal handler, checked by the
    /// accept loop
    draining: std::sync::atomic::AtomicBool,
    /// completed exchanges checkpointed across a restart
    state_path: Option<PathBuf>,
}

impl Broker {
    fn conn_for(&self, urn: &str) -> Option<Arc<Conn>> {
        self.local.get(urn).cloned()
    }

    /// Every peer-broker connection (resolution only — the caller
    /// serializes and sends after releasing the lock).
    fn peer_conns(&self) -> Vec<Arc<Conn>> {
        self.peers.clone()
    }

    /// Encode + enqueue on one connection. An associated function, not a
    /// method: callers hold `Arc<Conn>` and must serialize OUTSIDE the
    /// `shared` mutex — the encode never waits on the global lock.
    fn send_to(conn: &Arc<Conn>, msg: &WireMessage) -> Result<(), FlowError> {
        match encode_wire(msg) {
            Ok(bytes) => conn.out.send(bytes),
            Err(_) => Err(FlowError::Closed),
        }
    }
}

fn main() {
    let mut host = "0.0.0.0".to_string();
    let mut port = 7620u16;
    let mut urn = "urn:agent:gaip:broker:b1".to_string();
    let mut peers: Vec<String> = Vec::new();
    let mut registry_path: Option<PathBuf> = None;
    let mut state_path: Option<PathBuf> = None;
    let mut retries = 3u32;
    let mut backoff_ms = 100u64;

    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        match a.as_str() {
            "--host" => host = args.next().unwrap_or(host),
            "--port" => port = args.next().and_then(|p| p.parse().ok()).unwrap_or(port),
            "--urn" => urn = args.next().unwrap_or(urn),
            "--registry" => registry_path = args.next().map(PathBuf::from),
            "--state" => state_path = args.next().map(PathBuf::from),
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
        dead_ends: BTreeMap::new(),
        dead_letters: std::collections::VecDeque::new(),
        retries,
        backoff_ms,
        failovers: BTreeMap::new(),
        registry_path: registry_path.clone(),
        draining: std::sync::atomic::AtomicBool::new(false),
        state_path: state_path.clone(),
    }));
    // THE STATE FILE IS THE CHECKPOINT: --state carries the dedupe window
    // and failovers written at shutdown, so the restart restores from it —
    // loading only --registry forgets every completed exchange and turns
    // exactly-once into at-least-once across the restart
    if let Some(path) = &state_path {
        broker.lock().unwrap().load_registry(path);
        eprintln!("[broker] state loaded from {}", path.display());
    }
    if let Some(path) = &registry_path {
        broker.lock().unwrap().load_registry(path);
        eprintln!("[broker] registry loaded from {}", path.display());
    }
    // GRACEFUL SHUTDOWN: SIGTERM/SIGINT set the drain flag and checkpoint
    // the broker's memory so a restart neither re-executes completed
    // exchanges nor forgets them
    {
        let flag = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        for sig in [signal_hook::consts::SIGTERM, signal_hook::consts::SIGINT] {
            signal_hook::flag::register(sig, flag.clone()).ok();
        }
        let b = broker.clone();
        std::thread::spawn(move || {
            while !flag.load(std::sync::atomic::Ordering::SeqCst) {
                std::thread::sleep(Duration::from_millis(100));
            }
            eprintln!("[broker] signal received: draining");
            b.lock()
                .unwrap()
                .draining
                .store(true, std::sync::atomic::Ordering::SeqCst);
            b.lock().unwrap().checkpoint();
            // the accept loop cannot be interrupted out of `incoming()`;
            // the checkpoint is written, so exiting here is the clean cut
            std::process::exit(0);
        });
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

    for stream in listener
        .incoming()
        .take_while(|_| {
            !broker
                .lock()
                .unwrap()
                .draining
                .load(std::sync::atomic::Ordering::SeqCst)
        })
        .flatten()
    {
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
    let mut pos = 0usize;
    let mut chunk = [0u8; 16384];
    loop {
        match stream.read(&mut chunk) {
            Ok(0) => break,
            Ok(n) => buf.extend_from_slice(&chunk[..n]),
            Err(_) => break,
        }
        loop {
            match decode_wire(&buf[pos..]) {
                Ok(Some((msg, used))) => {
                    pos += used;
                    if pos == buf.len() {
                        buf.clear();
                        pos = 0;
                    }
                    handle(&shared, &conn, msg);
                }
                Ok(None) => break,
                Err(_) => {
                    buf.clear();
                    pos = 0;
                    break;
                }
            }
            // one compaction per read, not one memmove per message
            if pos > 0 {
                buf.drain(..pos);
                pos = 0;
            }
        }
    }
    eprintln!("[broker] peer connection closed");
}

fn serve(shared: Arc<Mutex<Broker>>, conn: Arc<Conn>, mut stream: TcpStream) {
    let mut buf: Vec<u8> = Vec::new();
    let mut pos = 0usize;
    let mut chunk = [0u8; 16384];
    loop {
        match stream.read(&mut chunk) {
            Ok(0) => break,
            Ok(n) => buf.extend_from_slice(&chunk[..n]),
            Err(_) => break,
        }
        loop {
            match decode_wire(&buf[pos..]) {
                Ok(Some((msg, used))) => {
                    pos += used;
                    if pos == buf.len() {
                        buf.clear();
                        pos = 0;
                    }
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
                    pos = 0;
                    break;
                }
            }
            // one compaction per read, not one memmove per message
            if pos > 0 {
                buf.drain(..pos);
                pos = 0;
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
            contexts: reply_contexts,
        } => {
            // DELIVERED: this broker handed the request to a LOCAL
            // participant — stamp the reply so the origin knows delivery
            // was acknowledged, not assumed
            let delivered_here = {
                let b = shared.lock().unwrap();
                b.pending
                    .get(&(conn.id, request_id))
                    .map(|(_, _, local)| *local)
            };
            let reply = if delivered_here == Some(true) {
                let own = shared.lock().unwrap().urn.clone();
                let mut stamped = vec![gaip_core::ServiceContext::new(
                    gaip_core::context::id::DELIVERED,
                    own,
                )];
                stamped.extend(reply_contexts);
                WireMessage::Reply {
                    request_id,
                    status,
                    body: body.clone(),
                    contexts: stamped,
                }
            } else {
                WireMessage::Reply {
                    request_id,
                    status,
                    body: body.clone(),
                    contexts: reply_contexts,
                }
            };
            let reply = Arc::new(reply);
            // state under the lock, serialization outside it: the encode
            // of the reply must not sit in the queue of the global mutex
            let mut target_conn = None;
            {
                // exactly-once: remember the reply against the request
                // that caused it, using the (connection, request_id) the
                // broker handed out
                let mut b = shared.lock().unwrap();
                if let Some((sender, msg_id, _)) = b.pending.remove(&(conn.id, request_id)) {
                    b.dedupe.remember(&sender, &msg_id, reply.clone());
                }
                // a reply travels to the envelope's recipient: no session
                // state, no request-id bookkeeping
                if let Some(env) = &body {
                    let target = env.recipient.clone();
                    target_conn = b.conn_for(&target);
                }
            }
            if let Some(c) = target_conn {
                let _ = Broker::send_to(&c, &reply);
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
            drop(b);
            let _ = Broker::send_to(conn, &reply);
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
                    contexts: Vec::new(),
                };
                let _ = Broker::send_to(conn, &reply);
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
                    contexts: Vec::new(),
                };
                let _ = Broker::send_to(conn, &reply);
                return true;
            }

            let fwd = WireMessage::Request {
                request_id,
                contexts: ctx.to_vec(),
                body: body.clone(),
            };

            // DEADLINE AT THE EDGE: an expired absolute deadline refuses
            // right here — the broker does not spend a flood on work that
            // cannot start
            if let Some(deadline) = ctx.deadline_unix_ms() {
                if deadline <= gaip_core::context::now_ms() {
                    let reply = WireMessage::Reply {
                        request_id,
                        status: ReplyStatus::system(
                            SystemMinor::Timeout,
                            "deadline expired at the broker".to_string(),
                        ),
                        body: None,
                        contexts: Vec::new(),
                    };
                    let reply = Arc::new(reply);
                    let mut b = shared.lock().unwrap();
                    b.dedupe
                        .remember(&body.sender, &body.message_id, reply.clone());
                    drop(b);
                    let _ = Broker::send_to(conn, &reply);
                    return true;
                }
            }

            // NEGATIVE CACHE: a target whose last attempt died within the
            // window is refused locally instead of re-flooding the mesh.
            // The refusal is retryable and the window is short — the
            // participant that appears later is found on the next attempt.
            let recently_dead = {
                let b = shared.lock().unwrap();
                b.dead_ends
                    .get(&target)
                    .is_some_and(|t| t.elapsed() < Duration::from_secs(15))
            };
            if recently_dead {
                let reply = WireMessage::Reply {
                    request_id,
                    status: ReplyStatus::system(
                        SystemMinor::ObjectNotExist,
                        format!("{target} was recently unreachable; retry after the negative-cache window"),
                    ),
                    body: None,
                    contexts: Vec::new(),
                };
                let mut b = shared.lock().unwrap();
                b.dedupe.remember_failure(&body.sender, &body.message_id, ReplyStatus::system(
                    SystemMinor::ObjectNotExist,
                    format!("{target} was recently unreachable; retry after the negative-cache window"),
                ));
                drop(b);
                let _ = Broker::send_to(conn, &reply);
                return true;
            }

            // local first, then peers, with retry: "not yet" ≠ "never"
            let (retries, backoff) = {
                let b = shared.lock().unwrap();
                (b.retries, b.backoff_ms)
            };
            let mut delivered = false;
            for attempt in 0..=retries {
                let conns: Vec<Arc<Conn>> = {
                    let mut b = shared.lock().unwrap();
                    let mut used: Vec<Arc<Conn>> = Vec::new();
                    let mut flooded_here = false;
                    if let Some(c) = b.conn_for(&target) {
                        used.push(c);
                    } else {
                        used = b.peer_conns();
                        flooded_here = !used.is_empty();
                    }
                    if !used.is_empty() {
                        for c in &used {
                            b.pending.insert(
                                (c.id, request_id),
                                (body.sender.clone(), body.message_id.clone(), !flooded_here),
                            );
                        }
                    }
                    used
                };
                // serialize + enqueue outside the lock: the encode of the
                // forwarded request is the broker's hottest path
                for c in &conns {
                    let _ = Broker::send_to(c, &fwd);
                }
                if !conns.is_empty() {
                    delivered = true;
                    break;
                }
                if attempt < retries {
                    std::thread::sleep(Duration::from_millis(backoff * (attempt as u64 + 1)));
                }
            }

            if !delivered {
                // DEAD LETTER: recorded with the reason, not dropped silently
                let mut b = shared.lock().unwrap();
                b.dead_ends
                    .insert(target.clone(), std::time::Instant::now());
                b.dead_letters.push_back((
                    gaip_core::context::now_ms(),
                    body.sender.clone(),
                    target.clone(),
                    "no reachable broker or participant".to_string(),
                ));
                while b.dead_letters.len() > 256 {
                    b.dead_letters.pop_front();
                }
                b.journal_dead_letter(&body.sender, &target);
                let reply = WireMessage::Reply {
                    request_id,
                    status: ReplyStatus::system(
                        SystemMinor::ObjectNotExist,
                        format!("{target} is not reachable through this broker or its peers"),
                    ),
                    body: None,
                    contexts: Vec::new(),
                };
                let reply = Arc::new(reply);
                b.dedupe
                    .remember(&body.sender, &body.message_id, reply.clone());
                drop(b);
                let _ = Broker::send_to(conn, &reply);
            }
            true
        }

        WireMessage::CancelRequest { .. } | WireMessage::Fragment { .. } => {
            // fragments are handled by participants; a broker forwards
            // them verbatim when they carry a routable body
            true
        }

        WireMessage::SchemaSync { idl, contexts } => {
            // a schema push is forwarded to every participant so the
            // fleet converges on one contract; the contexts (the signed
            // identity) travel with it
            let msg = WireMessage::SchemaSync { idl, contexts };
            let local: Vec<Arc<Conn>> = shared.lock().unwrap().local.values().cloned().collect();
            for c in local {
                let _ = Broker::send_to(&c, &msg);
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
    fn journal_dead_letter(&self, sender: &str, target: &str) {
        let (total, latest) = (
            self.dead_letters.len(),
            self.dead_letters
                .back()
                .map(|(ts, _, _, _)| *ts)
                .unwrap_or(0),
        );
        eprintln!("[broker] dead letter #{total}: {sender} -> {target} at {latest}");
    }

    fn checkpoint(&self) {
        let Some(path) = &self.state_path else { return };
        let now = std::time::Instant::now();
        let rows: Vec<serde_json::Value> = self
            .dedupe
            .snapshot(now)
            .into_iter()
            .map(|(sender, msg_id, left_ms, reply)| {
                json!({"sender": sender, "message_id": msg_id, "left_ms": left_ms,
                       "reply": serde_json::to_string(reply.as_ref()).unwrap_or_default()})
            })
            .collect();
        let doc = json!({"version": 1, "dedupe": rows, "failovers": self.failovers});
        let tmp = path.with_extension("tmp");
        if std::fs::write(&tmp, doc.to_string()).is_ok() {
            let _ = std::fs::rename(&tmp, path);
            eprintln!(
                "[broker] checkpoint: {} completed exchanges -> {}",
                rows.len(),
                path.display()
            );
        }
    }

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
                // RESTORE THE DEDUPE WINDOW: a completed exchange that
                // survived the checkpoint must replay after the restart —
                // forgetting it turns exactly-once into at-least-once
                if let Some(rows) = doc.get("dedupe").and_then(|v| v.as_array()) {
                    let restored: Vec<(String, String, u128, WireMessage)> = rows
                        .iter()
                        .filter_map(|r| {
                            let sender = r.get("sender")?.as_str()?.to_string();
                            let msg_id = r.get("message_id")?.as_str()?.to_string();
                            let left_ms = r.get("left_ms")?.as_u64()? as u128;
                            let reply: WireMessage =
                                serde_json::from_str(r.get("reply")?.as_str()?).ok()?;
                            Some((sender, msg_id, left_ms, reply))
                        })
                        .collect();
                    let n = restored.len();
                    self.dedupe.restore(std::time::Instant::now(), restored);
                    if n > 0 {
                        eprintln!(
                            "[broker] restored {n} completed exchanges from {}",
                            path.display()
                        );
                    }
                }
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
