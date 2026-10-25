//! Policies: the knobs GIOP gave a caller, in the form agents need.
//!
//! CORBA let a client choose `sync_scope` (when may the call return?),
//! request timeouts, and reply expectations. Agents need the same three
//! and for the same reason: a task that cannot be bounded in time cannot
//! be scheduled.
//!
//! The deadline is carried as an **absolute** instant in a service
//! context, so it survives every hop without renegotiation — a queue in
//! the middle cannot silently extend the caller's budget.

use crate::context::{now_ms, ContextList, ServiceContext};
use crate::error::GaipError;
use crate::message::ReplyStatus;
use crate::message::SystemMinor;

/// When the caller may resume. GIOP's three scopes, mapped to agents.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncScope {
    /// Return once the peer has accepted the request (fire and forget).
    None,
    /// Return once the peer has started working.
    WithServer,
    /// Return when the work is done (the default, and what a TASK_REPLY
    /// already implies).
    WithTarget,
}

#[derive(Debug, Clone)]
pub struct Policies {
    pub sync_scope: SyncScope,
    /// Default budget applied when the caller set none.
    pub request_timeout_ms: u64,
    /// Refuse a message that would need more fragments than this.
    pub max_fragments: usize,
    /// Fragment payload size in bytes.
    pub fragment_size: usize,
}

impl Default for Policies {
    fn default() -> Self {
        Self {
            sync_scope: SyncScope::WithTarget,
            request_timeout_ms: 30_000,
            max_fragments: 256,
            fragment_size: 64 * 1024,
        }
    }
}

impl Policies {
    /// Stamp a deadline on an outgoing request if it has none. Existing
    /// deadlines are never extended: that is the whole contract.
    pub fn apply_deadline(&self, list: &mut ContextList) {
        if list.deadline_unix_ms().is_none() {
            list.set(
                crate::context::id::DEADLINE,
                (now_ms() + self.request_timeout_ms as u128).to_string(),
            );
        }
    }

    pub fn deadline_context(&self) -> ServiceContext {
        ServiceContext::deadline_at(now_ms() + self.request_timeout_ms as u128)
    }

    /// Check the inbound deadline. `Some(status)` means the request must
    /// be refused with that reply — a `Timeout` system exception, which
    /// callers may retry.
    pub fn check_deadline(&self, list: &ContextList) -> Option<ReplyStatus> {
        if list.expired_at(now_ms()) {
            return Some(ReplyStatus::system(
                SystemMinor::Timeout,
                format!(
                    "deadline passed before the work started ({} ms budget)",
                    self.request_timeout_ms
                ),
            ));
        }
        None
    }

    /// Is a message of `bytes` too large to fragment under this policy?
    pub fn check_size(&self, bytes: usize) -> Result<(), GaipError> {
        let needed = bytes.div_ceil(self.fragment_size.max(1));
        if needed > self.max_fragments {
            return Err(GaipError::FrameTooLarge(bytes));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::id;

    #[test]
    fn a_deadline_is_stamped_when_missing() {
        let p = Policies::default();
        let mut list = ContextList::new();
        p.apply_deadline(&mut list);
        let remaining = list.remaining_ms(now_ms()).unwrap();
        assert!(remaining <= 30_000 && remaining > 29_000, "{remaining}");
    }

    #[test]
    fn an_existing_deadline_is_never_extended() {
        let p = Policies::default();
        let mut list = ContextList::new();
        list.set(id::DEADLINE, (now_ms() + 100).to_string());
        p.apply_deadline(&mut list);
        let remaining = list.remaining_ms(now_ms()).unwrap();
        assert!(
            remaining <= 100,
            "the caller's budget must not grow: {remaining}"
        );
    }

    #[test]
    fn an_expired_deadline_refuses_with_a_retryable_timeout() {
        let p = Policies::default();
        let mut list = ContextList::new();
        list.set(id::DEADLINE, (now_ms() - 1).to_string());
        let status = p.check_deadline(&list).expect("must refuse");
        assert!(status.retryable(), "a timeout is worth retrying");
        assert!(list.expired_at(now_ms()));
    }

    #[test]
    fn no_deadline_never_refuses() {
        let p = Policies::default();
        assert!(p.check_deadline(&ContextList::new()).is_none());
    }

    #[test]
    fn size_policy_bounds_the_fragment_count() {
        let p = Policies {
            fragment_size: 1024,
            max_fragments: 4,
            ..Policies::default()
        };
        assert!(p.check_size(4 * 1024).is_ok());
        assert!(p.check_size(4 * 1024 + 1).is_err());
    }
}
