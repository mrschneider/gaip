//! CosConcurrency-lite: named locks with leases.
//!
//! The CORBA concurrency service without the ORB machinery. The rule
//! that makes it safe for a distributed setting is the **lease**: a lock
//! is held until released OR until its TTL passes — a crashed holder
//! cannot deadlock everyone. Readers share; a writer excludes.
//!
//! This is the resource-exclusion story the message-level dedupe does
//! not cover: dedupe stops a *retried* message from running twice; the
//! lock stops two *different* messages from mutating one resource at the
//! same time.

use std::collections::BTreeMap;
use std::time::Instant;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    Read,
    Write,
}

#[derive(Debug, Default)]
pub struct LockSet {
    writer: Option<(String, Instant)>,
    readers: BTreeMap<String, Instant>,
}

impl LockSet {
    pub fn new() -> Self {
        Self::default()
    }

    /// Drop every lease whose TTL passed. Returns the released names.
    pub fn sweep(&mut self) -> Vec<String> {
        let now = Instant::now();
        let mut released = Vec::new();
        if let Some((owner, until)) = &self.writer {
            if *until <= now {
                released.push(owner.clone());
                self.writer = None;
            }
        }
        let expired: Vec<String> = self
            .readers
            .iter()
            .filter(|(_, until)| **until <= now)
            .map(|(n, _)| n.clone())
            .collect();
        for n in expired {
            self.readers.remove(&n);
            released.push(n);
        }
        released
    }

    fn holder_alive(&self, now: Instant) -> bool {
        self.writer.as_ref().is_some_and(|(_, until)| *until > now)
    }

    /// Acquire the write lock for `ttl_ms`. A lease expiry releases it
    /// implicitly; a live writer (or any readers) refuse a second writer.
    pub fn acquire_write(&mut self, owner: &str, ttl_ms: u64) -> bool {
        self.sweep();
        let now = Instant::now();
        if self.holder_alive(now) || !self.readers.is_empty() {
            // re-entrant: the same owner may extend its own lease
            if self.writer.as_ref().is_some_and(|(o, _)| o == owner) {
                self.writer = Some((
                    owner.to_string(),
                    now + std::time::Duration::from_millis(ttl_ms),
                ));
                return true;
            }
            return false;
        }
        self.writer = Some((
            owner.to_string(),
            now + std::time::Duration::from_millis(ttl_ms),
        ));
        true
    }

    pub fn acquire_read(&mut self, owner: &str, ttl_ms: u64) -> bool {
        self.sweep();
        let now = Instant::now();
        if self.holder_alive(now) {
            return false;
        }
        self.readers.insert(
            owner.to_string(),
            now + std::time::Duration::from_millis(ttl_ms),
        );
        true
    }

    pub fn release(&mut self, owner: &str) -> bool {
        let was_writer = self.writer.take_if(|(o, _)| o == owner).is_some();
        let was_reader = self.readers.remove(owner).is_some();
        was_writer || was_reader
    }

    pub fn writer(&self) -> Option<&str> {
        self.writer.as_ref().map(|(o, _)| o.as_str())
    }

    pub fn reader_count(&self) -> usize {
        self.readers.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn a_writer_excludes_everyone_else() {
        let mut l = LockSet::new();
        assert!(l.acquire_write("a", 60_000));
        assert!(!l.acquire_write("b", 60_000));
        assert!(!l.acquire_read("b", 60_000));
        assert!(l.release("a"));
        assert!(l.acquire_write("b", 60_000));
        assert_eq!(l.writer(), Some("b"));
    }

    #[test]
    fn readers_share_and_a_writer_waits() {
        let mut l = LockSet::new();
        assert!(l.acquire_read("r1", 60_000));
        assert!(l.acquire_read("r2", 60_000));
        assert_eq!(l.reader_count(), 2);
        assert!(!l.acquire_write("w", 60_000));
        assert!(l.release("r1") && l.release("r2"));
        assert!(l.acquire_write("w", 60_000));
    }

    #[test]
    fn an_expired_lease_is_released_implicitly() {
        let mut l = LockSet::new();
        assert!(l.acquire_write("a", 30));
        std::thread::sleep(Duration::from_millis(50));
        // a fresh acquirer sweeps first: the dead holder cannot deadlock
        assert!(l.acquire_write("b", 60_000));
        assert_eq!(l.writer(), Some("b"));
    }

    #[test]
    fn sweep_reports_what_it_released() {
        let mut l = LockSet::new();
        assert!(l.acquire_read("r", 30));
        std::thread::sleep(Duration::from_millis(50));
        let released = l.sweep();
        assert_eq!(released, vec!["r".to_string()]);
        assert_eq!(l.reader_count(), 0);
    }

    #[test]
    fn the_owner_may_extend_its_own_lease() {
        let mut l = LockSet::new();
        assert!(l.acquire_write("a", 60_000));
        assert!(l.acquire_write("a", 60_000), "re-entrant renewal");
        assert!(!l.acquire_write("b", 60_000));
    }

    #[test]
    fn release_by_a_non_holder_is_reported() {
        let mut l = LockSet::new();
        assert!(l.acquire_write("a", 60_000));
        assert!(!l.release("b"));
        assert!(l.release("a"));
        assert!(!l.release("a"), "already gone");
    }
}
