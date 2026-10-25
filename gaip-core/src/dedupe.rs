//! Exactly-once *effect* on an at-least-once wire.
//!
//! The transport can always deliver a message twice: a retry after a
//! timeout, a reconnect, a broker replay. What a participant can
//! guarantee is that the *effect* happens once — GIOP calls this
//! idempotent retry with a request-id cache, and it is the only honest
//! form of "exactly once" over a network.
//!
//! [`DedupeCache`] remembers `(sender, message_id)` for a bounded window
//! and returns the reply it already produced, so a retry answers
//! identically instead of running the work again.

use crate::message::{ReplyStatus, WireMessage};
use std::collections::{BTreeMap, VecDeque};
use std::time::{Duration, Instant};

/// One remembered exchange.
#[derive(Debug, Clone)]
pub struct Remembered {
    pub reply: WireMessage,
    pub stored_at: Instant,
}

/// A bounded, TTL'd cache of completed requests.
#[derive(Debug)]
pub struct DedupeCache {
    entries: BTreeMap<(String, String), Remembered>,
    order: VecDeque<(String, String)>,
    capacity: usize,
    ttl: Duration,
    hits: u64,
}

impl DedupeCache {
    pub fn new(capacity: usize, ttl: Duration) -> Self {
        Self {
            entries: BTreeMap::new(),
            order: VecDeque::new(),
            capacity: capacity.max(1),
            ttl,
            hits: 0,
        }
    }

    fn key(sender: &str, message_id: &str) -> (String, String) {
        (sender.to_string(), message_id.to_string())
    }

    /// Was this exact message already handled? Returns the reply to
    /// replay, so the caller answers without doing the work again.
    pub fn lookup(&mut self, sender: &str, message_id: &str) -> Option<WireMessage> {
        let key = Self::key(sender, message_id);
        let now = Instant::now();
        match self.entries.get(&key) {
            Some(r) if now.duration_since(r.stored_at) <= self.ttl => {
                self.hits += 1;
                Some(r.reply.clone())
            }
            Some(_) => {
                self.entries.remove(&key);
                None
            }
            None => None,
        }
    }

    /// Remember the reply for a completed request. Evicts the oldest
    /// entry when the window is full: the guarantee is bounded by the
    /// window, and pretending otherwise would be a lie.
    pub fn remember(&mut self, sender: &str, message_id: &str, reply: WireMessage) {
        let key = Self::key(sender, message_id);
        if self.entries.len() >= self.capacity {
            if let Some(oldest) = self.order.pop_front() {
                self.entries.remove(&oldest);
            }
        }
        self.order.push_back(key.clone());
        self.entries.insert(
            key,
            Remembered {
                reply,
                stored_at: Instant::now(),
            },
        );
    }

    /// Remember a *refusal* too: a retried message that was refused must
    /// be refused again, not silently executed.
    pub fn remember_failure(&mut self, sender: &str, message_id: &str, status: ReplyStatus) {
        let msg = WireMessage::Reply {
            request_id: 0,
            status,
            body: None,
            contexts: Vec::new(),
        };
        self.remember(sender, message_id, msg);
    }

    pub fn hits(&self) -> u64 {
        self.hits
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Drop expired entries (called from the heartbeat/sweep path).
    /// Snapshot for a checkpoint. `now` anchors the TTLs: each entry is
    /// stored with the milliseconds it still has to live, so a restore
    /// keeps the window honest instead of resetting it.
    pub fn snapshot(&self, now: Instant) -> Vec<(String, String, u128, WireMessage)> {
        self.entries
            .iter()
            .filter_map(|((sender, msg_id), r)| {
                let left = now.duration_since(r.stored_at);
                let left_ms = self.ttl.as_millis().saturating_sub(left.as_millis());
                if left_ms == 0 {
                    return None;
                }
                Some((sender.clone(), msg_id.clone(), left_ms, r.reply.clone()))
            })
            .collect()
    }

    /// Restore a checkpoint taken by [`Self::snapshot`].
    pub fn restore(&mut self, now: Instant, rows: Vec<(String, String, u128, WireMessage)>) {
        for (sender, msg_id, left_ms, reply) in rows {
            if self.entries.len() >= self.capacity {
                break;
            }
            let key = (sender.clone(), msg_id.clone());
            self.entries.insert(
                key.clone(),
                Remembered {
                    reply,
                    stored_at: now
                        .checked_sub(Duration::from_millis(
                            (self.ttl.as_millis().saturating_sub(left_ms)) as u64,
                        ))
                        .unwrap_or(now),
                },
            );
            self.order.push_back(key);
        }
    }

    pub fn sweep(&mut self, now: Instant) -> usize {
        let before = self.entries.len();
        self.order.retain(|k| {
            let keep = self
                .entries
                .get(k)
                .is_some_and(|r| now.duration_since(r.stored_at) <= self.ttl);
            if !keep {
                self.entries.remove(k);
            }
            keep
        });
        before - self.entries.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::envelope::Envelope;
    use serde_json::json;

    fn reply(id: &str) -> WireMessage {
        let env = Envelope::task_request(
            "msg_1",
            "task_1",
            "urn:agent:rust:tokio:engine",
            "urn:agent:python:langgraph:planner",
            json!({"n": id}),
        );
        WireMessage::reply(1, env)
    }

    #[test]
    fn a_retried_message_replays_the_same_reply() {
        let mut c = DedupeCache::new(64, Duration::from_secs(60));
        c.remember("urn:agent:a", "msg_1", reply("first"));
        let again = c.lookup("urn:agent:a", "msg_1").expect("must replay");
        let first = c.lookup("urn:agent:a", "msg_1").unwrap();
        assert_eq!(again, first);
        assert_eq!(c.hits(), 2);
    }

    #[test]
    fn different_senders_with_the_same_message_id_are_different_messages() {
        let mut c = DedupeCache::new(64, Duration::from_secs(60));
        c.remember("urn:agent:a", "msg_1", reply("a"));
        assert!(
            c.lookup("urn:agent:b", "msg_1").is_none(),
            "sender is part of the key"
        );
    }

    #[test]
    fn the_window_is_bounded_and_evicts_oldest() {
        let mut c = DedupeCache::new(2, Duration::from_secs(60));
        c.remember("a", "1", reply("1"));
        c.remember("a", "2", reply("2"));
        c.remember("a", "3", reply("3"));
        assert_eq!(c.len(), 2);
        assert!(c.lookup("a", "1").is_none(), "the oldest left the window");
        assert!(c.lookup("a", "2").is_some());
        assert!(c.lookup("a", "3").is_some());
    }

    #[test]
    fn entries_expire_after_the_ttl() {
        let mut c = DedupeCache::new(8, Duration::from_millis(5));
        c.remember("a", "1", reply("1"));
        std::thread::sleep(Duration::from_millis(20));
        assert!(
            c.lookup("a", "1").is_none(),
            "expired entries must not replay"
        );
        assert_eq!(c.sweep(Instant::now()), 0, "lookup already dropped it");
    }

    #[test]
    fn a_refusal_is_replayed_as_a_refusal() {
        let mut c = DedupeCache::new(8, Duration::from_secs(60));
        c.remember_failure("a", "msg_bad", ReplyStatus::user("E_SCHEMA", "drift"));
        match c.lookup("a", "msg_bad") {
            Some(WireMessage::Reply { status, .. }) => {
                assert!(
                    !status.is_ok(),
                    "a retried refusal must not become a success"
                );
            }
            other => panic!("expected a refusal, got {other:?}"),
        }
    }
}

#[cfg(test)]
mod checkpoint_tests {
    use super::*;
    use crate::envelope::Envelope;
    use std::time::Duration;

    fn reply(msg_id: &str) -> WireMessage {
        WireMessage::reply(
            1,
            Envelope::new(
                msg_id.to_string(),
                "t".to_string(),
                "urn:agent:a:x:y".to_string(),
                "urn:agent:b:x:y".to_string(),
                crate::envelope::MessageType::TaskReply,
                serde_json::json!({"status": "OK"}),
            ),
        )
    }

    #[test]
    fn a_checkpoint_restores_the_window_honestly() {
        let now = Instant::now();
        let mut c = DedupeCache::new(8, Duration::from_secs(120));
        c.remember("urn:agent:a:x:y", "m1", reply("r1"));
        let rows = c.snapshot(now);
        assert_eq!(rows.len(), 1);

        // restore into a FRESH cache, as a restart would
        let mut c2 = DedupeCache::new(8, Duration::from_secs(120));
        c2.restore(now, rows);
        let hit = c2.lookup("urn:agent:a:x:y", "m1");
        assert!(hit.is_some(), "a restored exchange replays identically");
    }

    #[test]
    fn an_expired_row_is_not_checkpointed() {
        let mut c = DedupeCache::new(8, Duration::from_millis(50));
        c.remember("urn:agent:a:x:y", "m1", reply("r1"));
        std::thread::sleep(Duration::from_millis(80));
        let rows = c.snapshot(Instant::now());
        assert!(rows.is_empty(), "a dead entry does not survive the restart");
    }
}
