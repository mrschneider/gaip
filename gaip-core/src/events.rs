//! Event channel with QoS: CosNotification's useful half.
//!
//! Telemetry (progress, logs, metrics) must not be coupled to the task
//! that produced it: a slow consumer must not block the run, and the
//! channel's behaviour under pressure must be a *declared policy*, not
//! an accident. Two policies, like the CORBA notification service:
//!
//! * `Discard::Oldest` — keep the newest `max_events` (default);
//! * `Discard::Reject` — refuse new events when full, so the producer
//!   learns the consumer cannot keep up.
//!
//! Events are numbered monotonically; a consumer drains everything after
//! its last seen sequence and is told when a gap means it missed
//! discarded events. Delivery is explicit, never silent.

use serde_json::Value;
use std::collections::VecDeque;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Discard {
    Oldest,
    Reject,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Qos {
    pub max_events: usize,
    pub discard: Discard,
}

#[derive(Debug, Clone)]
pub struct Event {
    pub seq: u64,
    pub kind: String,
    pub body: Value,
    pub at_ms: u128,
}

#[derive(Debug)]
pub struct EventChannel {
    qos: Qos,
    events: VecDeque<Event>,
    next_seq: u64,
    total: u64,
    rejected: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Batch {
    pub last_seq: u64,
    /// true when discarded events sit between the consumer's cursor and
    /// the first delivered event — the consumer decides what a gap means
    pub gap: bool,
}

impl EventChannel {
    pub fn new(qos: Qos) -> Self {
        Self {
            qos,
            events: VecDeque::new(),
            next_seq: 1,
            total: 0,
            rejected: 0,
        }
    }

    pub fn publish(&mut self, kind: &str, body: Value, at_ms: u128) -> Result<(), &'static str> {
        self.total += 1;
        if self.events.len() >= self.qos.max_events {
            match self.qos.discard {
                Discard::Oldest => {
                    self.events.pop_front();
                }
                Discard::Reject => {
                    self.rejected += 1;
                    return Err("event channel full (discard policy: reject)");
                }
            }
        }
        let seq = self.next_seq;
        self.next_seq += 1;
        self.events.push_back(Event {
            seq,
            kind: kind.to_string(),
            body,
            at_ms,
        });
        Ok(())
    }

    /// Everything after `after_seq`. `Batch.gap` is true when events were
    /// discarded between the consumer's cursor and the oldest kept event:
    /// there is history the consumer will never see.
    pub fn drain_after(&self, after_seq: u64) -> (Vec<Event>, Batch) {
        let events: Vec<Event> = self
            .events
            .iter()
            .filter(|e| e.seq > after_seq)
            .cloned()
            .collect();
        // a gap exists when the oldest kept event is NOT the one that
        // immediately follows the consumer's cursor: history between the
        // two was discarded by policy
        let gap = self.events.front().is_some_and(|e| e.seq > after_seq + 1);
        let last_seq = events.last().map(|e| e.seq).unwrap_or(after_seq);
        (events, Batch { last_seq, gap })
    }

    pub fn stats(&self) -> (u64, u64) {
        (self.total, self.rejected)
    }

    pub fn len(&self) -> usize {
        self.events.len()
    }

    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn chan(max: usize, discard: Discard) -> EventChannel {
        EventChannel::new(Qos {
            max_events: max,
            discard,
        })
    }

    #[test]
    fn events_are_numbered_and_drained_in_order() {
        let mut c = chan(10, Discard::Oldest);
        for i in 0..5 {
            c.publish("progress", json!({"step": i}), 0).unwrap();
        }
        let (events, batch) = c.drain_after(0);
        assert_eq!(events.len(), 5);
        assert_eq!(events[0].seq, 1);
        assert_eq!(events[4].body["step"], 4);
        assert_eq!(batch.last_seq, 5);
        assert!(!batch.gap);
        let (again, _) = c.drain_after(5);
        assert!(again.is_empty(), "a consumer does not see events twice");
    }

    #[test]
    fn discard_oldest_keeps_the_newest() {
        let mut c = chan(3, Discard::Oldest);
        for i in 0..6 {
            c.publish("progress", json!({"step": i}), 0).unwrap();
        }
        let (events, batch) = c.drain_after(0);
        assert_eq!(events.len(), 3);
        assert_eq!(events[0].body["step"], 3, "the oldest three were discarded");
        assert!(batch.gap, "the consumer is told it missed events");
    }

    #[test]
    fn discard_reject_refuses_instead_of_dropping() {
        let mut c = chan(2, Discard::Reject);
        c.publish("a", json!({}), 0).unwrap();
        c.publish("b", json!({}), 0).unwrap();
        assert!(c.publish("c", json!({}), 0).is_err());
        let (events, _) = c.drain_after(0);
        assert_eq!(events.len(), 2, "nothing was silently dropped");
        let (total, rejected) = c.stats();
        assert_eq!((total, rejected), (3, 1));
    }

    #[test]
    fn no_gap_when_the_consumer_kept_up() {
        let mut c = chan(3, Discard::Oldest);
        for i in 0..3 {
            c.publish("progress", json!({"step": i}), 0).unwrap();
        }
        let (_, batch) = c.drain_after(0);
        assert!(!batch.gap);
    }
}
