//! The Trading Service: discovery by *attribute*, not by spelling.
//!
//! A Naming Service answers "where is `swarm/engine-1`?". A Trader
//! answers the question an orchestrator actually has: "which agents can
//! do INVENTORY_OPTIMIZATION, are Rust, and have at least 32 GiB free?".
//!
//! That distinction is why CORBA had both services and why neither gRPC
//! nor A2A has the second: a swarm cannot be addressed by a fixed graph.
//!
//! Scope note: the query is a **conjunction of typed constraints** —
//! capability, runtime, version prefix, numeric minimums, text
//! equalities. It is deliberately not a constraint language: a full
//! expression parser is a project of its own, and every version of this
//! that shipped as "just a little parser" became a security review item.

use crate::error::GaipError;
use crate::ior::Ior;
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AttrValue {
    Text(String),
    Number(i64),
}

impl AttrValue {
    pub fn as_number(&self) -> Option<i64> {
        match self {
            Self::Number(n) => Some(*n),
            Self::Text(s) => s.parse().ok(),
        }
    }

    pub fn as_text(&self) -> Option<&str> {
        match self {
            Self::Text(s) => Some(s),
            Self::Number(_) => None,
        }
    }
}

/// One agent's advertisement.
#[derive(Debug, Clone)]
pub struct Offer {
    pub ior: Ior,
    pub attributes: BTreeMap<String, AttrValue>,
}

impl Offer {
    pub fn new(ior: Ior) -> Self {
        Self {
            ior,
            attributes: BTreeMap::new(),
        }
    }

    pub fn with_attr(mut self, key: &str, value: AttrValue) -> Self {
        self.attributes.insert(key.to_string(), value);
        self
    }

    pub fn number(&self, key: &str) -> Option<i64> {
        self.attributes.get(key).and_then(AttrValue::as_number)
    }
}

/// A conjunction of constraints. Every field is optional; an empty query
/// matches every offer.
#[derive(Debug, Clone, Default)]
pub struct Query {
    pub capability: Option<String>,
    pub runtime: Option<String>,
    pub version_prefix: Option<String>,
    pub min: Vec<(String, i64)>,
    pub text: Vec<(String, String)>,
}

impl Query {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn capability(mut self, cap: &str) -> Self {
        self.capability = Some(cap.to_string());
        self
    }

    pub fn runtime(mut self, rt: &str) -> Self {
        self.runtime = Some(rt.to_string());
        self
    }

    pub fn version_prefix(mut self, v: &str) -> Self {
        self.version_prefix = Some(v.to_string());
        self
    }

    pub fn at_least(mut self, attr: &str, min: i64) -> Self {
        self.min.push((attr.to_string(), min));
        self
    }

    pub fn attr_equals(mut self, attr: &str, value: &str) -> Self {
        self.text.push((attr.to_string(), value.to_string()));
        self
    }

    /// Does `offer` satisfy every constraint?
    pub fn matches(&self, offer: &Offer) -> bool {
        let urn = match offer.ior.urn.parse::<crate::urn::AgentUrn>() {
            Ok(u) => u,
            Err(_) => return false,
        };
        if let Some(cap) = &self.capability {
            if !offer.ior.has_capability(cap) {
                return false;
            }
        }
        if let Some(rt) = &self.runtime {
            if &urn.runtime != rt {
                return false;
            }
        }
        if let Some(vp) = &self.version_prefix {
            if !offer.ior.version.starts_with(vp) {
                return false;
            }
        }
        for (attr, min) in &self.min {
            match offer.number(attr) {
                Some(v) if v >= *min => {}
                _ => return false,
            }
        }
        for (attr, want) in &self.text {
            match offer.attributes.get(attr).and_then(AttrValue::as_text) {
                Some(v) if v == want => {}
                _ => return false,
            }
        }
        true
    }
}

/// The offer table.
#[derive(Debug, Default, Clone)]
pub struct Trader {
    offers: Vec<Offer>,
}

impl Trader {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&mut self, offer: Offer) -> Result<(), GaipError> {
        offer.ior.validate()?;
        if self.offers.iter().any(|o| o.ior.urn == offer.ior.urn) {
            return Err(GaipError::BadField {
                field: "urn",
                why: format!("{} is already registered", offer.ior.urn),
            });
        }
        self.offers.push(offer);
        Ok(())
    }

    pub fn withdraw(&mut self, urn: &str) -> bool {
        let before = self.offers.len();
        self.offers.retain(|o| o.ior.urn != urn);
        self.offers.len() != before
    }

    /// Matching offers, best first. "Best" is the `vram_free_mb`
    /// attribute when present (a real capacity signal), then the URN for
    /// a stable order — never hash order.
    pub fn query(&self, q: &Query) -> Vec<&Offer> {
        let mut hits: Vec<&Offer> = self.offers.iter().filter(|o| q.matches(o)).collect();
        hits.sort_by(|a, b| {
            b.number("vram_free_mb")
                .unwrap_or(0)
                .cmp(&a.number("vram_free_mb").unwrap_or(0))
                .then(a.ior.urn.cmp(&b.ior.urn))
        });
        hits
    }

    pub fn best(&self, q: &Query) -> Option<&Offer> {
        self.query(q).into_iter().next()
    }

    pub fn len(&self) -> usize {
        self.offers.len()
    }

    pub fn is_empty(&self) -> bool {
        self.offers.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn offer(urn: &str, ep: &str, caps: &[&str], vram: i64, runtime: &str) -> Offer {
        Offer::new(
            Ior::new(urn, vec![ep.to_string()], "1.0")
                .with_capabilities(caps.iter().map(|c| c.to_string()).collect()),
        )
        .with_attr("vram_free_mb", AttrValue::Number(vram))
        .with_attr("runtime", AttrValue::Text(runtime.to_string()))
    }

    fn fixture() -> Trader {
        let mut t = Trader::new();
        t.register(offer(
            "urn:agent:rust:tokio:engine-a",
            "10.0.0.1:7610",
            &["INVENTORY_OPTIMIZATION"],
            64_000,
            "rust",
        ))
        .unwrap();
        t.register(offer(
            "urn:agent:rust:tokio:engine-b",
            "10.0.0.2:7610",
            &["INVENTORY_OPTIMIZATION", "DEMAND_FORECAST"],
            16_000,
            "rust",
        ))
        .unwrap();
        t.register(offer(
            "urn:agent:python:langgraph:planner",
            "10.0.0.3:7610",
            &["PLANNING"],
            8_000,
            "python",
        ))
        .unwrap();
        t
    }

    #[test]
    fn query_by_capability_and_minimum_capacity() {
        let t = fixture();
        let hits = t.query(
            &Query::new()
                .capability("INVENTORY_OPTIMIZATION")
                .at_least("vram_free_mb", 32_000),
        );
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].ior.primary_endpoint(), Some("10.0.0.1:7610"));
    }

    #[test]
    fn best_picks_the_biggest_capacity_first() {
        let t = fixture();
        let best = t
            .best(&Query::new().capability("INVENTORY_OPTIMIZATION"))
            .unwrap();
        assert_eq!(best.ior.urn, "urn:agent:rust:tokio:engine-a");
    }

    #[test]
    fn runtime_and_version_constraints_come_from_the_reference() {
        let t = fixture();
        assert_eq!(t.query(&Query::new().runtime("python")).len(), 1);
        assert_eq!(t.query(&Query::new().runtime("java")).len(), 0);
        assert_eq!(t.query(&Query::new().version_prefix("1.")).len(), 3);
        assert_eq!(t.query(&Query::new().version_prefix("2.")).len(), 0);
    }

    #[test]
    fn text_attributes_match_exactly() {
        let t = fixture();
        assert_eq!(
            t.query(&Query::new().attr_equals("runtime", "rust")).len(),
            2
        );
        assert_eq!(
            t.query(&Query::new().attr_equals("runtime", "RUST")).len(),
            0
        );
    }

    #[test]
    fn an_empty_query_matches_everything_and_duplicates_are_refused() {
        let mut t = fixture();
        assert_eq!(t.query(&Query::new()).len(), 3);
        let dup = offer(
            "urn:agent:rust:tokio:engine-a",
            "10.0.0.9:7610",
            &[],
            0,
            "rust",
        );
        let err = t.register(dup).unwrap_err();
        assert!(err.to_string().contains("already registered"), "{err}");
    }

    #[test]
    fn withdraw_removes_the_offer() {
        let mut t = fixture();
        assert!(t.withdraw("urn:agent:rust:tokio:engine-b"));
        assert!(!t.withdraw("urn:agent:rust:tokio:engine-b"));
        assert_eq!(t.len(), 2);
    }
}
