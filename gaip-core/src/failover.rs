//! Transparent failover: the CORBA messaging plan, without the ORB.
//!
//! An IOR carries failover endpoints. The client-side rule from the
//! CORBA messaging spec is small and worth stating exactly:
//!
//! * walk the endpoints in order;
//! * a TRANSIENT failure moves to the next endpoint;
//! * one attempt per endpoint per round, at most `max_rounds` rounds;
//! * the absolute deadline is NEVER extended by a retry — a retry that
//!   would outlive the caller's budget is refused instead of started.
//!
//! This module is pure planning (no IO), so a client, a broker or an
//! engine callback can all follow the same policy.

/// One endpoint of an IOR's failover list.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Endpoint {
    pub addr: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    /// attempt this endpoint
    Try(String),
    /// every endpoint was tried `max_rounds` times: give up, and the
    /// caller reports the original failure
    Exhausted,
}

#[derive(Debug, Clone)]
pub struct Plan {
    pub endpoints: Vec<Endpoint>,
    pub max_rounds: usize,
    cursor: usize,
    rounds: usize,
}

impl Plan {
    /// Builds a plan from an IOR's endpoint list (first entry is the
    /// primary, the rest are failovers). With one endpoint and
    /// `max_rounds > 1`, retries stay on the same address — the GIOP
    /// rule for a single-endpoint reference.
    pub fn new(endpoints: Vec<String>, max_rounds: usize) -> Self {
        Self {
            endpoints: endpoints
                .into_iter()
                .map(|addr| Endpoint { addr })
                .collect(),
            max_rounds: max_rounds.max(1),
            cursor: 0,
            rounds: 1,
        }
    }

    pub fn current(&self) -> Action {
        match self.endpoints.get(self.cursor) {
            Some(e) => Action::Try(e.addr.clone()),
            None => Action::Exhausted,
        }
    }

    /// Record a failed attempt and produce the next action.
    pub fn record_failure(&mut self) -> Action {
        if self.endpoints.is_empty() {
            return Action::Exhausted;
        }
        self.cursor += 1;
        if self.cursor >= self.endpoints.len() {
            self.cursor = 0;
            self.rounds += 1;
            if self.rounds > self.max_rounds {
                return Action::Exhausted;
            }
        }
        self.current()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn plan(rounds: usize) -> Plan {
        Plan::new(vec!["ep1:1".into(), "ep2:2".into(), "ep3:3".into()], rounds)
    }

    #[test]
    fn a_failure_walks_the_endpoints_before_repeating() {
        let mut p = plan(2);
        assert_eq!(p.current(), Action::Try("ep1:1".into()));
        assert_eq!(p.record_failure(), Action::Try("ep2:2".into()));
        assert_eq!(p.record_failure(), Action::Try("ep3:3".into()));
        assert_eq!(p.record_failure(), Action::Try("ep1:1".into()), "round 2");
        assert_eq!(p.record_failure(), Action::Try("ep2:2".into()));
        assert_eq!(p.record_failure(), Action::Try("ep3:3".into()));
        assert_eq!(p.record_failure(), Action::Exhausted, "rounds exhausted");
    }

    #[test]
    fn one_round_means_every_endpoint_once() {
        let mut p = plan(1);
        p.record_failure();
        p.record_failure();
        assert_eq!(p.record_failure(), Action::Exhausted);
    }

    #[test]
    fn a_single_endpoint_retries_in_place() {
        let mut p = Plan::new(vec!["only:1".into()], 3);
        assert_eq!(p.record_failure(), Action::Try("only:1".into()));
        assert_eq!(p.record_failure(), Action::Try("only:1".into()));
        assert_eq!(p.record_failure(), Action::Exhausted);
    }

    #[test]
    fn an_empty_plan_is_immediately_exhausted() {
        let mut p = Plan::new(vec![], 3);
        assert_eq!(p.current(), Action::Exhausted);
        assert_eq!(p.record_failure(), Action::Exhausted);
    }

    #[test]
    fn max_rounds_of_zero_is_at_least_one() {
        let mut p = Plan::new(vec!["a".into()], 0);
        assert_eq!(p.record_failure(), Action::Exhausted);
    }
}
