//! Portable interceptors: the hooks around request/reply.
//!
//! GIOP's answer to "where does cross-cutting behaviour live?" is not the
//! handler — it is a ring of interceptors the ORB calls on every message.
//! Tracing, retry policy enforcement, context injection and metrics all
//! become the same shape, and a gateway can observe traffic it does not
//! understand without touching the protocol.
//!
//! Server side only for now: the engine is the reference participant and
//! the broker already has its own journal. Client-side interceptors are
//! the natural follow-up once a real client crate exists.

use crate::context::ContextList;
use crate::envelope::Envelope;
use crate::message::ReplyStatus;

pub trait ServerInterceptor: Send + Sync {
    /// Name that shows up in status dumps.
    fn name(&self) -> &str;

    /// Called before a Request is dispatched. MAY mutate the context
    /// list (inject trace, annotate route); MUST NOT change the payload.
    fn receive_request(&self, _ctx: &mut ContextList, _env: &Envelope) {}

    /// Called after the reply was produced, before it is written.
    fn send_reply(&self, _request_id: u32, _status: &ReplyStatus, _env: Option<&Envelope>) {}

    /// Called when the reply is a SYSTEM_EXCEPTION — the hook where retry
    /// policy lives.
    fn receive_exception(&self, _request_id: u32, _status: &ReplyStatus) {}
}

/// The recording interceptor the engine runs by default: counts what
/// crossed it, so the status surface reports measurements, not guesses.
#[derive(Default)]
pub struct Recorder {
    pub requests: std::sync::atomic::AtomicU64,
    pub replies: std::sync::atomic::AtomicU64,
    pub exceptions: std::sync::atomic::AtomicU64,
}

impl ServerInterceptor for Recorder {
    fn name(&self) -> &str {
        "recorder"
    }

    fn receive_request(&self, _ctx: &mut ContextList, _env: &Envelope) {
        self.requests
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    fn send_reply(&self, _request_id: u32, status: &ReplyStatus, _env: Option<&Envelope>) {
        match status {
            ReplyStatus::SystemException { .. } => {
                self.exceptions
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            _ => {
                self.replies
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        }
    }
}

impl Recorder {
    pub fn snapshot(&self) -> (u64, u64, u64) {
        use std::sync::atomic::Ordering::Relaxed;
        (
            self.requests.load(Relaxed),
            self.replies.load(Relaxed),
            self.exceptions.load(Relaxed),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::envelope::MessageType;
    #[allow(unused_imports)]
    use crate::message::SystemMinor;

    fn envelope() -> Envelope {
        let v: serde_json::Value = serde_json::json!({
            "protocol_version": "1.0",
            "message_id": "m1",
            "correlation_id": "t1",
            "sender": "urn:agent:python:langgraph:a",
            "recipient": "urn:agent:rust:tokio:b",
            "message_type": "TASK_REQUEST",
            "payload": {"intent": "PING", "parameters": {}}
        });
        Envelope::from_value(&v).expect("envelope")
    }

    #[test]
    fn the_recorder_counts_requests_replies_and_exceptions() {
        let r = Recorder::default();
        let mut ctx = ContextList::new();
        let env = envelope();
        r.receive_request(&mut ctx, &env);
        r.receive_request(&mut ctx, &env);
        r.send_reply(1, &ReplyStatus::NoException, Some(&env));
        r.send_reply(
            2,
            &ReplyStatus::system(SystemMinor::Timeout, "timeout".to_string()),
            None,
        );
        assert_eq!(r.snapshot(), (2, 1, 1));
    }

    #[test]
    fn an_interceptor_may_annotate_the_context_but_not_the_payload() {
        struct Tag;
        impl ServerInterceptor for Tag {
            fn name(&self) -> &str {
                "tag"
            }
            fn receive_request(&self, ctx: &mut ContextList, _env: &Envelope) {
                ctx.push_route("interceptor-hop");
            }
        }
        let mut ctx = ContextList::new();
        let env = envelope();
        Tag.receive_request(&mut ctx, &env);
        assert!(ctx
            .to_vec()
            .iter()
            .any(|sc| sc.data.contains("interceptor-hop")));
        assert_eq!(env.message_type, MessageType::TaskRequest);
    }
}
