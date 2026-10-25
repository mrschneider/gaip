//! Service contexts: the GIOP mechanism for cross-cutting data.
//!
//! A `service_context_list` travels with every message and carries things
//! the *operation* does not own: trace id, deadline, tenant, identity,
//! retry attempt, route history. Every participant can read and append
//! without inventing fields in the payload — and without knowing which
//! framework produced the message.
//!
//! That is the difference between a protocol and a payload convention: a
//! gateway can add a trace id to traffic it does not understand.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Context ids. GIOP reserves ranges for the OMG; this is GAIP's own
/// small registry (0x0001..=0x7FFF), with 0x8000+ left to deployments.
pub mod id {
    pub const TRACE: u16 = 0x0001;
    /// absolute deadline in unix milliseconds — absolute so it survives
    /// every hop without each hop having to know the original budget
    pub const DEADLINE: u16 = 0x0002;
    pub const TENANT: u16 = 0x0003;
    /// CSIv2 identity assertion (see `security`)
    pub const IDENTITY: u16 = 0x0004;
    pub const ATTEMPT: u16 = 0x0005;
    pub const ROUTE: u16 = 0x0006;
}

/// One context entry. `data` is opaque to the protocol; GAIP encodes it
/// as a JSON string so a packet capture stays readable (the same
/// trade-off as the text chunks in AIPP's payload plane).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServiceContext {
    pub id: u16,
    pub data: String,
}

impl ServiceContext {
    pub fn new(id: u16, data: impl Into<String>) -> Self {
        Self {
            id,
            data: data.into(),
        }
    }

    pub fn trace(trace_id: impl Into<String>) -> Self {
        Self::new(id::TRACE, trace_id)
    }

    pub fn tenant(name: impl Into<String>) -> Self {
        Self::new(id::TENANT, name)
    }

    /// Deadline as an absolute instant, so a hop that queues a message
    /// for 3 seconds cannot extend the caller's budget by accident.
    pub fn deadline_at(unix_ms: u128) -> Self {
        Self::new(id::DEADLINE, unix_ms.to_string())
    }

    pub fn attempt(n: u32) -> Self {
        Self::new(id::ATTEMPT, n.to_string())
    }
}

/// An ordered, de-duplicated context list (last write wins per id, as in
/// GIOP where a context id appears at most once).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ContextList {
    entries: BTreeMap<u16, String>,
}

impl ContextList {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_vec(v: Vec<ServiceContext>) -> Self {
        let mut list = Self::new();
        for c in v {
            list.set(c.id, c.data);
        }
        list
    }

    pub fn set(&mut self, id: u16, data: impl Into<String>) -> &mut Self {
        self.entries.insert(id, data.into());
        self
    }

    pub fn get(&self, id: u16) -> Option<&str> {
        self.entries.get(&id).map(String::as_str)
    }

    pub fn remove(&mut self, id: u16) -> Option<String> {
        self.entries.remove(&id)
    }

    pub fn to_vec(&self) -> Vec<ServiceContext> {
        self.entries
            .iter()
            .map(|(id, data)| ServiceContext::new(*id, data.clone()))
            .collect()
    }

    pub fn trace_id(&self) -> Option<&str> {
        self.get(id::TRACE)
    }

    pub fn tenant(&self) -> Option<&str> {
        self.get(id::TENANT)
    }

    pub fn attempt(&self) -> u32 {
        self.get(id::ATTEMPT)
            .and_then(|v| v.parse().ok())
            .unwrap_or(0)
    }

    /// Absolute deadline, if the caller set one.
    pub fn deadline_unix_ms(&self) -> Option<u128> {
        self.get(id::DEADLINE).and_then(|v| v.parse().ok())
    }

    /// Milliseconds left before the deadline at `now_ms`; `None` means no
    /// deadline at all, `Some(0)` means it is already gone.
    pub fn remaining_ms(&self, now_ms: u128) -> Option<u128> {
        self.deadline_unix_ms().map(|d| d.saturating_sub(now_ms))
    }

    pub fn expired_at(&self, now_ms: u128) -> bool {
        matches!(self.remaining_ms(now_ms), Some(0))
    }

    /// Append this hop to the route (bounded, so a routing loop is
    /// visible in the context instead of being invisible).
    pub fn push_route(&mut self, hop: &str) -> &mut Self {
        let mut route = self.get(id::ROUTE).unwrap_or("").to_string();
        if !route.is_empty() {
            route.push(',');
        }
        route.push_str(hop);
        // keep the last 16 hops: enough to spot a loop, bounded enough
        // that the header cannot grow without limit
        let hops: Vec<&str> = route.split(',').collect();
        let start = hops.len().saturating_sub(16);
        self.set(id::ROUTE, hops[start..].join(","))
    }

    pub fn route(&self) -> Vec<&str> {
        self.get(id::ROUTE)
            .map(|r| r.split(',').filter(|s| !s.is_empty()).collect())
            .unwrap_or_default()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }
}

/// Unix time in milliseconds — the one clock the contexts share.
pub fn now_ms() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn contexts_survive_a_roundtrip_through_the_wire() {
        let list = ContextList::from_vec(vec![
            ServiceContext::trace("trace-abc"),
            ServiceContext::tenant("acme"),
            ServiceContext::attempt(2),
        ]);
        let json = serde_json::to_string(&list.to_vec()).unwrap();
        let back: Vec<ServiceContext> = serde_json::from_str(&json).unwrap();
        let back = ContextList::from_vec(back);
        assert_eq!(back.trace_id(), Some("trace-abc"));
        assert_eq!(back.tenant(), Some("acme"));
        assert_eq!(back.attempt(), 2);
    }

    #[test]
    fn deadline_is_absolute_and_hops_cannot_extend_it() {
        let now = now_ms();
        let mut list = ContextList::new();
        list.set(id::DEADLINE, (now + 5_000).to_string());
        assert_eq!(list.remaining_ms(now), Some(5_000));
        // a hop that sleeps 4s sees 1s left, not the original 5s
        assert_eq!(list.remaining_ms(now + 4_000), Some(1_000));
        assert!(!list.expired_at(now + 4_999));
        assert!(list.expired_at(now + 5_000));
        assert!(list.expired_at(now + 60_000));
    }

    #[test]
    fn no_deadline_means_no_expiry() {
        let list = ContextList::new();
        assert_eq!(list.remaining_ms(now_ms()), None);
        assert!(!list.expired_at(u128::MAX / 2));
    }

    #[test]
    fn route_is_bounded_and_detectable() {
        let mut list = ContextList::new();
        for i in 0..40 {
            list.push_route(&format!("hop{i}"));
        }
        let hops = list.route();
        assert_eq!(hops.len(), 16, "the header must not grow without limit");
        assert_eq!(hops.last().copied(), Some("hop39"));
    }

    #[test]
    fn a_context_id_appears_once() {
        let list = ContextList::from_vec(vec![
            ServiceContext::trace("first"),
            ServiceContext::trace("second"),
        ]);
        assert_eq!(list.len(), 1);
        assert_eq!(
            list.trace_id(),
            Some("second"),
            "last write wins, as in GIOP"
        );
    }
}
