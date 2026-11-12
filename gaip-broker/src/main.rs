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

use gaip_core::codec::{
    decode_wire_view, encode_reply_view, encode_request_view, encode_wire, message_error,
    RoutedReply, WireFrame,
};
use gaip_core::context::{ContextList, ServiceContext};
use gaip_core::dedupe::DedupeCache;
use gaip_core::envelope::Envelope;
use gaip_core::error::GaipError;
use gaip_core::flow::{FlowError, Outbox};
use gaip_core::message::{ReplyStatus, SystemMinor, WireMessage};
use serde_json::json;
use std::collections::BTreeMap;
use std::io::Read;
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

/// One peer on the other side of a connection (participant or broker).
/// `Arc<Conn>` is what gets cloned; the inner state is shared.
struct Conn {
    /// stable id: the dedupe bookkeeping keys on (connection, request_id)
    id: u64,
    urn: Mutex<Option<String>>,
    out: Arc<Outbox>,
}

/// (connection id, request id) of a forwarded request awaiting its reply.
type PendingKey = (u64, u32);
/// (sender, message_id) of the request that produced the pending reply.
type PendingEntry = (String, String);
/// Forwarded requests awaiting their reply, keyed by the connection the
/// reply will arrive on.
type PendingMap = BTreeMap<PendingKey, PendingEntry>;

struct Broker {
    urn: String,
    /// participants (and peers) registered on this broker, by URN
    local: BTreeMap<String, Arc<Conn>>,
    /// peer brokers (their connections are entries in `local` too)
    peers: Vec<Arc<Conn>>,
    /// exactly-once window behind its own Mutex so the routing decision
    /// (which runs under the broker's READ lock) never needs a write
    /// acquisition on the broker itself.
    dedupe: Mutex<DedupeCache>,
    /// forwarded LOCAL request -> (sender, message_id): the reply that
    /// comes back is remembered for exactly-once replay. Hot path.
    pending_local: Mutex<PendingMap>,
    /// forwarded FLOODED request -> (sender, message_id): the rare
    /// non-local path, kept off `pending_local`'s lock.
    pending_flood: Mutex<PendingMap>,
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

    /// Enqueue a frame that is ALREADY encoded (a forwarded request keeps
    /// the body bytes it arrived with). One allocation is shared by every
    /// target via `Arc` — no per-connection copy, no re-serialization.
    fn send_bytes(conn: &Arc<Conn>, bytes: &std::sync::Arc<[u8]>) -> Result<(), FlowError> {
        conn.out.send_arc(bytes.clone())
    }
}

/// Resolve the connections a request to `target` should go out on, and
/// record the pending bookkeeping for the exactly-once replay. Local
/// delivery first; a target that is not local floods every peer broker
/// (the first good reply wins). Called while the broker's READ lock is
/// held: the routing maps are read-only here, and only the short inner
/// pending Mutexes are written (never the global lock).
fn resolve_targets(
    b: &Broker,
    target: &str,
    request_id: u32,
    sender: &str,
    message_id: &str,
) -> Vec<Arc<Conn>> {
    if let Some(c) = b.conn_for(target) {
        b.pending_local.lock().unwrap().insert(
            (c.id, request_id),
            (sender.to_string(), message_id.to_string()),
        );
        return vec![c];
    }
    let used = b.peer_conns();
    if !used.is_empty() {
        let mut pf = b.pending_flood.lock().unwrap();
        for c in &used {
            pf.insert(
                (c.id, request_id),
                (sender.to_string(), message_id.to_string()),
            );
        }
    }
    used
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
    let broker = Arc::new(RwLock::new(Broker {
        urn,
        local: BTreeMap::new(),
        peers: Vec::new(),
        dedupe: Mutex::new(DedupeCache::new(4096, Duration::from_secs(120))),
        pending_local: Mutex::new(BTreeMap::new()),
        pending_flood: Mutex::new(BTreeMap::new()),
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
        broker.write().unwrap().load_registry(path);
        eprintln!("[broker] state loaded from {}", path.display());
    }
    if let Some(path) = &registry_path {
        broker.write().unwrap().load_registry(path);
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
            b.write()
                .unwrap()
                .draining
                .store(true, std::sync::atomic::Ordering::SeqCst);
            b.read().unwrap().checkpoint();
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
                broker.write().unwrap().peers.push(conn.clone());
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
                .read()
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
fn serve_peer(shared: Arc<RwLock<Broker>>, conn: Arc<Conn>, mut stream: TcpStream) {
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
            match decode_wire_view(&buf[pos..]) {
                Ok(Some((frame, used))) => {
                    // handle first: the frame borrows `buf`, so the buffer
                    // cannot be advanced or cleared under it
                    handle_frame(&shared, &conn, frame);
                    pos += used;
                    if pos == buf.len() {
                        buf.clear();
                        pos = 0;
                    }
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

fn serve(shared: Arc<RwLock<Broker>>, conn: Arc<Conn>, mut stream: TcpStream) {
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
            match decode_wire_view(&buf[pos..]) {
                Ok(Some((frame, used))) => {
                    // handle first: the frame borrows `buf`
                    let keep_going = handle_frame(&shared, &conn, frame);
                    pos += used;
                    if pos == buf.len() {
                        buf.clear();
                        pos = 0;
                    }
                    if !keep_going {
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
        shared.write().unwrap().local.remove(&n);
        eprintln!("[broker] {n} disconnected");
    }
}

/// What the relay needs from a `REQUEST` — the routing fields plus the
/// body in whatever form it arrived in.
struct ReqMeta<'a> {
    request_id: u32,
    contexts: Vec<ServiceContext>,
    sender: String,
    recipient: String,
    message_id: String,
    fwd: ForwardBody<'a>,
}

impl<'a> ReqMeta<'a> {
    /// From a fully parsed message (the fallback path).
    fn parsed(request_id: u32, contexts: Vec<ServiceContext>, body: Envelope) -> Self {
        Self {
            request_id,
            contexts,
            sender: body.sender.clone(),
            recipient: body.recipient.clone(),
            message_id: body.message_id.clone(),
            fwd: ForwardBody::Parsed(body),
        }
    }
}

/// The envelope as it will be forwarded: the relay never re-encodes a body
/// it received as bytes.
enum ForwardBody<'a> {
    /// Came off the wire: forward the exact envelope bytes.
    Raw(&'a serde_json::value::RawValue),
    /// Already a struct (fallback, or built in-process).
    Parsed(Envelope),
}

impl ForwardBody<'_> {
    fn encode(&self, request_id: u32, contexts: &[ServiceContext]) -> Result<Vec<u8>, GaipError> {
        match self {
            Self::Raw(envelope) => encode_request_view(request_id, contexts, envelope),
            Self::Parsed(env) => encode_wire(&WireMessage::Request {
                request_id,
                contexts: contexts.to_vec(),
                body: env.clone(),
            }),
        }
    }
}

fn handle_request(shared: &Arc<RwLock<Broker>>, conn: &Arc<Conn>, req: ReqMeta<'_>) -> bool {
    let request_id = req.request_id;
    let contexts = req.contexts;
    // register the participant by the URN it uses
    let registered_new = conn.urn.lock().unwrap().is_none();
    if registered_new {
        *conn.urn.lock().unwrap() = Some(req.sender.clone());
    }
    let mut ctx = ContextList::from_vec(contexts);
    if registered_new {
        let mut b = shared.write().unwrap();
        b.local.insert(req.sender.clone(), conn.clone());
        // FAILOVER context: the participant advertises where else
        // it can be reached; the reference outlives the socket
        if let Some(fail) = ctx.get(gaip_core::context::id::FAILOVER) {
            let alternates: Vec<String> = fail
                .split(',')
                .map(|x| x.trim().to_string())
                .filter(|x| !x.is_empty())
                .collect();
            if !alternates.is_empty() {
                b.failovers.insert(req.sender.clone(), alternates);
                if let Some(path) = b.registry_path.clone() {
                    b.persist_registry(&path);
                }
            }
        }
        eprintln!("[broker] {} registered", req.sender);
    }
    let target = req.recipient.clone();

    // ONE critical section for the whole decision, under the
    // broker's READ lock: route check, exactly-once lookup,
    // deadline, negative cache and the first resolution used to
    // take FIVE separate lock acquisitions per request.
    // Everything that needs shared state happens here; every
    // serialization and enqueue happens after the lock is
    // released.
    enum Decision {
        Loop(WireMessage),
        Replay(WireMessage),
        Expired(WireMessage),
        Refusal(WireMessage),
        Forward {
            conns: Vec<Arc<Conn>>,
            retries: u32,
            backoff: u64,
        },
    }
    let decision = {
        let b = shared.read().unwrap();
        let own = b.urn.clone();
        if ctx.route().contains(&own.as_str()) {
            Decision::Loop(WireMessage::Reply {
                request_id,
                status: ReplyStatus::system(
                    SystemMinor::ProtocolError,
                    format!("routing loop: {own} is already in the route"),
                ),
                body: None,
                contexts: Vec::new(),
            })
        } else {
            ctx.push_route(&own);
            // exactly-once effect: replay the reply of a completed
            // exchange. The probe runs FIRST, into a local, so the
            // MutexGuard is released before the else-chain: a guard
            // held across an `if let` scrutinee would deadlock the
            // `remember` calls below (std Mutex is not reentrant).
            let cached = b
                .dedupe
                .lock()
                .unwrap()
                .lookup_read(&req.sender, &req.message_id);
            if let Some(WireMessage::Reply { status, body, .. }) = cached {
                Decision::Replay(WireMessage::Reply {
                    request_id,
                    status,
                    body,
                    contexts: Vec::new(),
                })
            }
            // DEADLINE AT THE EDGE: an expired absolute deadline
            // refuses right here — the broker does not spend a
            // flood on work that cannot start
            else if let Some(deadline) = ctx.deadline_unix_ms() {
                if deadline <= gaip_core::context::now_ms() {
                    let reply = Arc::new(WireMessage::Reply {
                        request_id,
                        status: ReplyStatus::system(
                            SystemMinor::Timeout,
                            "deadline expired at the broker".to_string(),
                        ),
                        body: None,
                        contexts: Vec::new(),
                    });
                    b.dedupe
                        .lock()
                        .unwrap()
                        .remember(&req.sender, &req.message_id, reply.clone());
                    Decision::Expired(WireMessage::Reply {
                        request_id,
                        status: ReplyStatus::system(
                            SystemMinor::Timeout,
                            "deadline expired at the broker".to_string(),
                        ),
                        body: None,
                        contexts: Vec::new(),
                    })
                } else {
                    Decision::Forward {
                        conns: resolve_targets(
                            &b,
                            &target,
                            request_id,
                            &req.sender,
                            &req.message_id,
                        ),
                        retries: b.retries,
                        backoff: b.backoff_ms,
                    }
                }
            }
            // NEGATIVE CACHE: a target whose last attempt died
            // within the window is refused locally instead of
            // re-flooding the mesh. The refusal is retryable and
            // the window is short — the participant that appears
            // later is found on the next attempt.
            else if b
                .dead_ends
                .get(&target)
                .is_some_and(|t| t.elapsed() < Duration::from_secs(15))
            {
                let refusal = ReplyStatus::system(
                    SystemMinor::ObjectNotExist,
                    format!(
                        "{target} was recently unreachable; retry after the negative-cache window"
                    ),
                );
                b.dedupe.lock().unwrap().remember_failure(
                    &req.sender,
                    &req.message_id,
                    refusal.clone(),
                );
                Decision::Refusal(WireMessage::Reply {
                    request_id,
                    status: refusal,
                    body: None,
                    contexts: Vec::new(),
                })
            } else {
                Decision::Forward {
                    conns: resolve_targets(&b, &target, request_id, &req.sender, &req.message_id),
                    retries: b.retries,
                    backoff: b.backoff_ms,
                }
            }
        }
    };

    match decision {
        Decision::Loop(reply) | Decision::Expired(reply) | Decision::Refusal(reply) => {
            let _ = Broker::send_to(conn, &reply);
            return true;
        }
        Decision::Replay(reply) => {
            let _ = Broker::send_to(conn, &reply);
            return true;
        }
        Decision::Forward {
            conns,
            retries,
            backoff,
        } => {
            // One re-frame for every target: only the contexts
            // change, the envelope bytes ride through untouched.
            // ONE allocation for the whole fan-out: every target
            // queues a reference to the same bytes.
            let fwd: std::sync::Arc<[u8]> = match req.fwd.encode(request_id, &ctx.to_vec()) {
                Ok(bytes) => bytes.into(),
                Err(e) => {
                    // Adding a hop pushed the frame over MAX_FRAME.
                    let _ = Broker::send_to(conn, &message_error(e.to_string()));
                    return true;
                }
            };
            // local first, then peers, with retry: "not yet" ≠ "never".
            // The FIRST resolution came from the single critical
            // section above; a retry re-locks only when nothing was
            // found, because a participant may appear mid-backoff.
            let mut delivered = !conns.is_empty();
            for c in &conns {
                let _ = Broker::send_bytes(c, &fwd);
            }
            if !delivered {
                for attempt in 0..retries {
                    std::thread::sleep(Duration::from_millis(backoff * (attempt as u64 + 1)));
                    let conns = {
                        let b = shared.read().unwrap();
                        resolve_targets(&b, &target, request_id, &req.sender, &req.message_id)
                    };
                    // serialize + enqueue outside the lock: the
                    // encode of the forwarded request is the
                    // broker's hottest path
                    for c in &conns {
                        let _ = Broker::send_bytes(c, &fwd);
                    }
                    if !conns.is_empty() {
                        delivered = true;
                        break;
                    }
                }
            }
            if !delivered {
                // DEAD LETTER: recorded with the reason, not dropped silently
                let reply = {
                    let mut b = shared.write().unwrap();
                    b.dead_ends
                        .insert(target.clone(), std::time::Instant::now());
                    b.dead_letters.push_back((
                        gaip_core::context::now_ms(),
                        req.sender.clone(),
                        target.clone(),
                        "no reachable broker or participant".to_string(),
                    ));
                    while b.dead_letters.len() > 256 {
                        b.dead_letters.pop_front();
                    }
                    b.journal_dead_letter(&req.sender, &target);
                    let reply = Arc::new(WireMessage::Reply {
                        request_id,
                        status: ReplyStatus::system(
                            SystemMinor::ObjectNotExist,
                            format!("{target} is not reachable through this broker or its peers"),
                        ),
                        body: None,
                        contexts: Vec::new(),
                    });
                    b.dedupe
                        .lock()
                        .unwrap()
                        .remember(&req.sender, &req.message_id, reply.clone());
                    reply
                };
                let _ = Broker::send_to(conn, &reply);
            }
        }
    }
    true
}

/// One decoded frame from a read loop: a REQUEST goes through the routing
/// view (payload carried as bytes, never parsed); everything else is
/// already a full message.
fn handle_frame(shared: &Arc<RwLock<Broker>>, conn: &Arc<Conn>, frame: WireFrame<'_>) -> bool {
    match frame {
        WireFrame::Request(req) => handle_request(
            shared,
            conn,
            ReqMeta {
                request_id: req.request_id,
                contexts: req.contexts,
                sender: req.sender,
                recipient: req.recipient,
                message_id: req.message_id,
                fwd: ForwardBody::Raw(req.envelope),
            },
        ),
        WireFrame::Reply(reply) => handle_reply_view(shared, conn, &reply),
        WireFrame::Other(msg) => handle(shared, conn, msg),
    }
}

/// REPLY fast path: the body arrives as raw wire bytes (a [`RoutedReply`]),
/// so the relay routes on the envelope's recipient and forwards the bytes
/// it received — no parse into a `Value` tree, no re-serialization. The
/// exactly-once bookkeeping needs an owned `Envelope`, so that one is
/// materialized (as the old full-message path already did) and only the
/// dedupe cache sees it; the wire gets `encode_reply_view` splicing the
/// original bytes back.
fn handle_reply_view(
    shared: &Arc<RwLock<Broker>>,
    conn: &Arc<Conn>,
    reply: &RoutedReply<'_>,
) -> bool {
    // DELIVERED: this broker handed the request to a LOCAL participant —
    // stamp the reply so the origin knows delivery was acknowledged, not
    // assumed. READ lock: recipient resolution and the pending lookup are
    // pure reads; only the short inner Mutexes are written, and the encode
    // happens AFTER the guard drops, off the global lock.
    let mut target_conn = None;
    let stamped = {
        let b = shared.read().unwrap();
        // LOCAL delivery first (the hot path), then the flood map: both
        // entries are REMOVED here, so neither map can grow without bound.
        let key = (conn.id, reply.request_id);
        let local = b.pending_local.lock().unwrap().remove(&key);
        let delivered_here = local.is_some();
        let pending = local.or_else(|| b.pending_flood.lock().unwrap().remove(&key));
        let mut stamped = reply.contexts.clone();
        if delivered_here {
            stamped.insert(
                0,
                gaip_core::ServiceContext::new(gaip_core::context::id::DELIVERED, b.urn.clone()),
            );
        }
        // exactly-once: remember the reply against the request that caused
        // it, using the (connection, request_id) the broker handed out.
        // The dedupe cache stores an owned `WireMessage` (replayed on
        // retry), so the body is materialized once — the forwarded bytes
        // below stay the originals.
        if let Some((sender, msg_id)) = pending {
            let body = reply
                .body
                .map(|raw| serde_json::from_str::<gaip_core::envelope::Envelope>(raw.get()))
                .transpose()
                .ok()
                .flatten();
            b.dedupe.lock().unwrap().remember(
                &sender,
                &msg_id,
                WireMessage::Reply {
                    request_id: reply.request_id,
                    status: reply.status.clone(),
                    body,
                    contexts: stamped.clone(),
                },
            );
        }
        // a reply travels to the envelope's recipient: a bodyless reply
        // (system exception) is unroutable and dropped, like before.
        if let Some(target) = &reply.recipient {
            target_conn = b.conn_for(target);
        }
        stamped
    };
    if let Some(c) = target_conn {
        let bytes = encode_reply_view(reply.request_id, &reply.status, &stamped, reply.body).ok();
        if let Some(bytes) = bytes {
            let _ = Broker::send_bytes(&c, &Arc::from(bytes));
        } else {
            eprintln!(
                "[broker] reply encode failed (request {id})",
                id = reply.request_id
            );
        }
    }
    true
}

fn handle(shared: &Arc<RwLock<Broker>>, conn: &Arc<Conn>, msg: WireMessage) -> bool {
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
            // was acknowledged, not assumed. Resolved inside the single
            // critical section below.
            // state under the lock, serialization outside it: the encode
            // of the reply must not sit in the queue of the global mutex.
            // READ lock: recipient resolution and the local pending
            // lookup are pure reads; only the short inner Mutexes are
            // written. A FLOODED reply's remember is skipped — the
            // origin's retry is the safety net there, and flooding was
            // never on the hot path anyway.
            let mut target_conn = None;
            let reply = {
                let b = shared.read().unwrap();
                // LOCAL delivery first (the hot path), then the flood map:
                // both entries are REMOVED here, so neither map can grow
                // without bound, and a flooded reply is remembered exactly
                // like a local one (the origin must replay what it relayed).
                let key = (conn.id, request_id);
                let local = b.pending_local.lock().unwrap().remove(&key);
                let delivered_here = local.is_some();
                let pending = local.or_else(|| b.pending_flood.lock().unwrap().remove(&key));
                let mut stamped = reply_contexts;
                if delivered_here {
                    stamped.insert(
                        0,
                        gaip_core::ServiceContext::new(
                            gaip_core::context::id::DELIVERED,
                            b.urn.clone(),
                        ),
                    );
                }
                let reply = Arc::new(WireMessage::Reply {
                    request_id,
                    status,
                    body: body.clone(),
                    contexts: stamped,
                });
                // exactly-once: remember the reply against the request
                // that caused it, using the (connection, request_id) the
                // broker handed out
                if let Some((sender, msg_id)) = pending {
                    b.dedupe
                        .lock()
                        .unwrap()
                        .remember(&sender, &msg_id, reply.clone());
                }
                // a reply travels to the envelope's recipient: no session
                // state, no request-id bookkeeping
                if let Some(env) = &body {
                    let target = env.recipient.clone();
                    target_conn = b.conn_for(&target);
                }
                reply
            };
            if let Some(c) = target_conn {
                let _ = Broker::send_to(&c, &reply);
            }
            true
        }

        WireMessage::LocateReply { .. } => true,

        WireMessage::LocateRequest { request_id, target } => {
            let b = shared.read().unwrap();
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
        } => handle_request(shared, conn, ReqMeta::parsed(request_id, contexts, body)),

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
            let local: Vec<Arc<Conn>> = shared.read().unwrap().local.values().cloned().collect();
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
            .lock()
            .unwrap()
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
                    self.dedupe
                        .lock()
                        .unwrap()
                        .restore(std::time::Instant::now(), restored);
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
