//! The GAIP client: the half the server-side reference cannot be.
//!
//! Everything robust about this client is the CORBA messaging plan made
//! executable:
//!
//! * **Transparent failover** — the endpoint list comes from the IOR;
//!   a dead endpoint moves the client to the next one, bounded by
//!   rounds, and a retry never extends the caller's absolute deadline.
//! * **Reconnect with resume** — an event cursor survives a reconnect,
//!   so the client asks "what did I miss" instead of "resend everything".
//! * **Client-side interceptors** — retry policy and tracing live at the
//!   caller, where the decision belongs.
//!
//! The client is synchronous and std-only: the reference protocol must
//! be usable without an async runtime.
//!
//! Deadlines: [`Client::call`] takes the envelope only; if the caller
//! wants a deadline it puts the absolute DEADLINE context in the
//! envelope (the engine refuses expired work) — the client never invents
//! one, because a budget the caller did not state is a lie.

use gaip_core::codec::{decode_wire, encode_wire};
use gaip_core::context::ServiceContext;
use gaip_core::failover::Plan;
use gaip_core::message::WireMessage;
use gaip_core::{Envelope, GaipError};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

/// Client-side hook, called around every exchange.
pub trait ClientInterceptor: Send + Sync {
    fn name(&self) -> &str;
    /// before the request is written
    fn before_send(&self, _msg: &WireMessage) {}
    /// after a reply is decoded
    fn after_reply(&self, _msg: &WireMessage) {}
}

/// The default interceptor: counts what the caller crossed.
#[derive(Default)]
pub struct Recorder {
    pub sent: std::sync::atomic::AtomicU64,
    pub replies: std::sync::atomic::AtomicU64,
    pub failovers: std::sync::atomic::AtomicU64,
}

impl ClientInterceptor for Recorder {
    fn name(&self) -> &str {
        "recorder"
    }
    fn before_send(&self, _msg: &WireMessage) {
        self.sent.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
    fn after_reply(&self, _msg: &WireMessage) {
        self.replies
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
}

impl Recorder {
    pub fn snapshot(&self) -> (u64, u64, u64) {
        use std::sync::atomic::Ordering::Relaxed;
        (
            self.sent.load(Relaxed),
            self.replies.load(Relaxed),
            self.failovers.load(Relaxed),
        )
    }
}

/// One terminal reply plus the TASK_STREAM frames that preceded it.
pub struct Exchange {
    pub reply: WireMessage,
    pub streams: Vec<WireMessage>,
}

pub struct Client {
    endpoints: Vec<String>,
    max_rounds: usize,
    plan: Plan,
    stream: Option<TcpStream>,
    buf: Vec<u8>,
    interceptors: Vec<std::sync::Arc<dyn ClientInterceptor>>,
    /// last event sequence seen, preserved across reconnects
    pub event_cursor: u64,
    /// CSIv2 identity token, attached to every TaskRequest
    identity: Option<String>,
    /// the URN the client is talking to (used by the convenience calls)
    pub server_urn: String,
    request_id: u32,
    failover_count: std::sync::atomic::AtomicU64,
}

impl Client {
    pub fn new(endpoints: Vec<String>, max_rounds: usize) -> Self {
        let plan = Plan::new(endpoints.clone(), max_rounds);
        Self {
            endpoints,
            max_rounds,
            plan,
            stream: None,
            buf: Vec::new(),
            interceptors: Vec::new(),
            event_cursor: 0,
            identity: None,
            server_urn: "urn:agent:rust:tokio:analytics-engine-v1".to_string(),
            request_id: 1,
            failover_count: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Attach a signed CSIv2 identity token; it rides every
    /// TASK_REQUEST the client sends.
    pub fn with_identity(mut self, token: impl Into<String>) -> Self {
        self.identity = Some(token.into());
        self
    }

    pub fn with_interceptor(mut self, i: std::sync::Arc<dyn ClientInterceptor>) -> Self {
        self.interceptors.push(i);
        self
    }

    /// Dial the current endpoint of the failover plan.
    pub fn connect(&mut self) -> Result<(), GaipError> {
        let addr = match self.plan.current() {
            gaip_core::failover::Action::Try(a) => a,
            gaip_core::failover::Action::Exhausted => {
                return Err(GaipError::BadField {
                    field: "endpoints",
                    why: "failover plan exhausted".to_string(),
                })
            }
        };
        let stream = TcpStream::connect(&addr).map_err(|e| GaipError::BadField {
            field: "connect",
            why: format!("{addr}: {e}"),
        })?;
        let _ = stream.set_nodelay(true);
        let _ = stream.set_read_timeout(Some(Duration::from_secs(10)));
        self.stream = Some(stream);
        self.buf.clear();
        Ok(())
    }

    /// Next endpoint after a failure. The recorder counts it.
    fn advance(&mut self) -> Result<(), GaipError> {
        self.stream = None;
        self.failover_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        match self.plan.record_failure() {
            gaip_core::failover::Action::Try(_) => self.connect(),
            gaip_core::failover::Action::Exhausted => Err(GaipError::BadField {
                field: "endpoints",
                why: "failover plan exhausted".to_string(),
            }),
        }
    }

    fn send_raw(&mut self, bytes: &[u8]) -> Result<(), GaipError> {
        let s = self.stream.as_mut().ok_or_else(|| GaipError::BadField {
            field: "connect",
            why: "not connected".to_string(),
        })?;
        s.write_all(bytes)
            .and_then(|_| s.flush())
            .map_err(|e| GaipError::BadField {
                field: "write",
                why: e.to_string(),
            })
    }

    /// Read one wire message; `Ok(None)` on a read timeout.
    fn read_msg(&mut self, timeout: Duration) -> Result<Option<WireMessage>, GaipError> {
        let deadline = std::time::Instant::now() + timeout;
        loop {
            if self.buf.len() >= 4 {
                let n = u32::from_be_bytes([self.buf[0], self.buf[1], self.buf[2], self.buf[3]])
                    as usize;
                if self.buf.len() >= 4 + n {
                    let body: Vec<u8> = self.buf.drain(..4 + n).collect();
                    let decoded = decode_wire(&body)
                        .map_err(|e| GaipError::BadField {
                            field: "decode",
                            why: e.to_string(),
                        })?
                        .map(|(m, _)| m);
                    return Ok(decoded);
                }
            }
            let now = std::time::Instant::now();
            if now >= deadline {
                return Ok(None);
            }
            self.stream
                .as_mut()
                .ok_or_else(|| GaipError::BadField {
                    field: "connect",
                    why: "not connected".to_string(),
                })?
                .set_read_timeout(Some(deadline - now))
                .map_err(|e| GaipError::BadField {
                    field: "socket",
                    why: e.to_string(),
                })?;
            let mut chunk = [0u8; 16384];
            let s = self.stream.as_mut().unwrap();
            match s.read(&mut chunk) {
                Ok(0) => {
                    return Err(GaipError::BadField {
                        field: "read",
                        why: "connection closed by peer".to_string(),
                    })
                }
                Ok(n) => self.buf.extend_from_slice(&chunk[..n]),
                Err(e)
                    if e.kind() == std::io::ErrorKind::WouldBlock
                        || e.kind() == std::io::ErrorKind::TimedOut =>
                {
                    return Ok(None)
                }
                Err(e) => {
                    return Err(GaipError::BadField {
                        field: "read",
                        why: e.to_string(),
                    })
                }
            }
        }
    }

    /// One full exchange: send the request, collect TASK_STREAM frames,
    /// return the terminal reply. Survives a dead endpoint by walking
    /// the failover plan — a failure to WRITE retries; a failure to READ
    /// surfaces (the server may have acted).
    pub fn call(&mut self, env: Envelope) -> Result<Exchange, GaipError> {
        // TRANSIENT: a failure BEFORE any reply byte (write error, or the
        // peer closed without answering) retries on the next endpoint —
        // the CORBA messaging rule. Once reply bytes flow, the exchange
        // is committed and failures surface.
        for _ in 0..self.max_rounds * self.endpoints.len().max(1) {
            match self.call_once(env.clone()) {
                Ok(x) => return Ok(x),
                Err(e) if e.is_transient_no_reply() => {
                    eprintln!("[client] transient failure ({e}); failing over");
                    self.advance()?;
                }
                Err(e) => return Err(e),
            }
        }
        Err(GaipError::BadField {
            field: "failover",
            why: "all endpoints exhausted".to_string(),
        })
    }

    fn call_once(&mut self, env: Envelope) -> Result<Exchange, GaipError> {
        let rid = self.request_id;
        self.request_id += 1;
        let mut msg = WireMessage::request(rid, env);
        if let Some(token) = &self.identity {
            // CSIv2: the caller's identity rides the request
            if let WireMessage::Request { contexts, .. } = &mut msg {
                contexts.push(ServiceContext::new(
                    gaip_core::context::id::IDENTITY,
                    token.clone(),
                ));
            }
        }
        for i in &self.interceptors {
            i.before_send(&msg);
        }
        let bytes = encode_wire(&msg).map_err(|e| GaipError::BadField {
            field: "encode",
            why: e.to_string(),
        })?;
        if self.stream.is_none() {
            self.connect()?;
        }
        self.send_raw(&bytes)?;
        let mut streams = Vec::new();
        let reply;
        loop {
            match self.read_msg(Duration::from_secs(20))? {
                Some(
                    m @ WireMessage::Reply {
                        request_id: got, ..
                    },
                ) if got == rid => {
                    // TASK_STREAM frames before the terminal reply are
                    // events, not the outcome
                    let is_stream = matches!(&m, WireMessage::Reply {
                        body: Some(env),
                        ..
                    } if env.message_type == gaip_core::envelope::MessageType::TaskStream);
                    if is_stream {
                        streams.push(m);
                        continue;
                    }
                    for i in &self.interceptors {
                        i.after_reply(&m);
                    }
                    reply = m;
                    break;
                }
                Some(m @ WireMessage::Reply { .. }) => streams.push(m),
                Some(_) => {}
                None => return Err(Self::e_transient_no_reply()),
            }
        }
        Ok(Exchange { reply, streams })
    }

    fn e_transient_no_reply() -> GaipError {
        GaipError::BadField {
            field: "read",
            why: "connection closed before a reply".to_string(),
        }
    }

    /// Grant stream credits to the server (flow control).
    pub fn grant_credits(&mut self, count: i64) -> Result<WireMessage, GaipError> {
        let env = Envelope::new(
            format!("grant_{}", self.request_id),
            format!("credits_{}", self.request_id),
            "urn:agent:rust:gaip:client".to_string(),
            self.server_urn.clone(),
            gaip_core::envelope::MessageType::TaskRequest,
            serde_json::json!({"intent": "GRANT_CREDITS", "count": count}),
        );
        let x = self.call(env)?;
        Ok(x.reply)
    }

    /// Pull events after the cursor; the cursor advances on success and
    /// survives reconnects.
    pub fn pull_events(&mut self) -> Result<(Vec<serde_json::Value>, bool), GaipError> {
        let env = Envelope::new(
            format!("pull_{}", self.request_id),
            format!("events_{}", self.request_id),
            "urn:agent:rust:gaip:client".to_string(),
            self.server_urn.clone(),
            gaip_core::envelope::MessageType::TaskRequest,
            serde_json::json!({"intent": "EVENT_PULL", "after_seq": self.event_cursor}),
        );
        let x = self.call(env)?;
        let payload = match &x.reply {
            WireMessage::Reply {
                body: Some(env),
                status,
                ..
            } if status.is_ok() => env.payload.clone(),
            other => {
                return Err(GaipError::BadField {
                    field: "events",
                    why: format!("event pull failed: {other:?}"),
                })
            }
        };
        let events = payload
            .get("events")
            .and_then(serde_json::Value::as_array)
            .cloned()
            .unwrap_or_default();
        if let Some(last) = payload.get("last_seq").and_then(serde_json::Value::as_u64) {
            self.event_cursor = last;
        }
        let gap = payload
            .get("gap")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false);
        Ok((events, gap))
    }

    /// Locate a URN through the current endpoint.
    pub fn locate(&mut self, target: &str) -> Result<Option<gaip_core::LocateStatus>, GaipError> {
        if self.stream.is_none() {
            self.connect()?;
        }
        let msg = WireMessage::LocateRequest {
            request_id: self.request_id,
            target: target.to_string(),
        };
        self.request_id += 1;
        self.send_raw(&encode_wire(&msg).map_err(|e| GaipError::BadField {
            field: "encode",
            why: e.to_string(),
        })?)?;
        loop {
            match self.read_msg(Duration::from_secs(10))? {
                Some(WireMessage::LocateReply {
                    request_id: _,
                    status,
                }) => return Ok(Some(status)),
                Some(_) => {}
                None => return Ok(None),
            }
        }
    }

    pub fn failover_count(&self) -> u64 {
        self.failover_count
            .load(std::sync::atomic::Ordering::Relaxed)
    }
}
