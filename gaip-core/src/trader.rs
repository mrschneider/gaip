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
use crate::urn::AgentUrn;
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
    /// The IOR's URN parsed once at construction. The URN never changes
    /// after registration, so every query re-parsing it was O(N·k) string
    /// splits for a value that cannot move. `None` only when the URN does
    /// not parse — such an offer matches nothing, as before.
    parsed_urn: Option<AgentUrn>,
}

impl Offer {
    pub fn new(ior: Ior) -> Self {
        Self {
            parsed_urn: ior.urn.parse::<AgentUrn>().ok(),
            ior,
            attributes: BTreeMap::new(),
        }
    }

    /// The parsed URN, when it is a valid agent URN.
    pub fn urn(&self) -> Option<&AgentUrn> {
        self.parsed_urn.as_ref()
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
    /// A parsed constraint expression (`Expr::parse`). When present it is
    /// evaluated in addition to the builder constraints.
    pub expr: Option<Expr>,
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

    /// Build a query from an expression string.
    pub fn parse(text: &str) -> Result<Self, GaipError> {
        Ok(Self {
            expr: Some(Expr::parse(text)?),
            ..Self::default()
        })
    }

    /// Does `offer` satisfy every constraint?
    pub fn matches(&self, offer: &Offer) -> bool {
        if let Some(e) = &self.expr {
            if !e.matches(offer) {
                return false;
            }
        }
        // Parsed once at registration; re-parsing per offer was the
        // dominant cost of a query.
        let urn = match offer.urn() {
            Some(u) => u,
            None => return false,
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

// ---------------------------------------------------------------- queries
//
// A constraint language, not a pile of builders: `capability =
// "INVENTORY_OPTIMIZATION" AND vram_free_mb > 30000 AND (runtime = "rust"
// OR runtime = "cuda")`. Precedence is the usual one (NOT > AND > OR),
// parentheses group, and comparisons are typed — `>` on text is refused
// at parse time instead of silently doing something surprising.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CmpOp {
    Eq,
    Ne,
    Gt,
    Lt,
    Ge,
    Le,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Lit {
    Text(String),
    Number(i64),
    Bool(bool),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Expr {
    Comparison {
        attr: String,
        op: CmpOp,
        value: Lit,
    },
    /// Substring match on text, membership on a list attribute.
    Contains {
        attr: String,
        value: String,
    },
    And(Box<Expr>, Box<Expr>),
    Or(Box<Expr>, Box<Expr>),
    Not(Box<Expr>),
}

fn tokenize(s: &str) -> Result<Vec<String>, GaipError> {
    let mut out = Vec::new();
    let chars: Vec<char> = s.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        if c.is_whitespace() {
            i += 1;
        } else if c == '"' {
            let start = i + 1;
            let mut j = start;
            while j < chars.len() && chars[j] != '"' {
                j += 1;
            }
            if j >= chars.len() {
                return Err(GaipError::BadField {
                    field: "query",
                    why: "unterminated string literal".to_string(),
                });
            }
            out.push(format!(
                "\"{}\"",
                chars[start..j].iter().collect::<String>()
            ));
            i = j + 1;
        } else if "()=<>!".contains(c) {
            // two-char operators first
            let two: String = chars[i..(i + 2).min(chars.len())].iter().collect();
            if two == "!=" || two == ">=" || two == "<=" {
                out.push(two);
                i += 2;
            } else {
                out.push(c.to_string());
                i += 1;
            }
        } else {
            let mut j = i;
            while j < chars.len() && !chars[j].is_whitespace() && !"()=<>!\"".contains(chars[j]) {
                j += 1;
            }
            let word: String = chars[i..j].iter().collect();
            out.push(word);
            i = j;
        }
    }
    Ok(out)
}

struct Parser {
    toks: Vec<String>,
    pos: usize,
}

impl Parser {
    fn peek(&self) -> Option<&str> {
        self.toks.get(self.pos).map(String::as_str)
    }

    fn take(&mut self) -> Option<String> {
        let t = self.toks.get(self.pos).cloned();
        if t.is_some() {
            self.pos += 1;
        }
        t
    }

    fn err(why: &str) -> GaipError {
        GaipError::BadField {
            field: "query",
            why: why.to_string(),
        }
    }

    fn parse_or(&mut self) -> Result<Expr, GaipError> {
        let mut left = self.parse_and()?;
        while self.peek().is_some_and(|t| t.eq_ignore_ascii_case("OR")) {
            self.take();
            let right = self.parse_and()?;
            left = Expr::Or(Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    fn parse_and(&mut self) -> Result<Expr, GaipError> {
        let mut left = self.parse_not()?;
        while self.peek().is_some_and(|t| t.eq_ignore_ascii_case("AND")) {
            self.take();
            let right = self.parse_not()?;
            left = Expr::And(Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    fn parse_not(&mut self) -> Result<Expr, GaipError> {
        if self.peek().is_some_and(|t| t.eq_ignore_ascii_case("NOT")) {
            self.take();
            return Ok(Expr::Not(Box::new(self.parse_not()?)));
        }
        self.parse_primary()
    }

    fn parse_primary(&mut self) -> Result<Expr, GaipError> {
        if self.peek() == Some("(") {
            self.take();
            let inner = self.parse_or()?;
            if self.take().as_deref() != Some(")") {
                return Err(Self::err("missing closing ')'"));
            }
            return Ok(inner);
        }
        let attr = self
            .take()
            .ok_or_else(|| Self::err("expected an attribute"))?;
        if attr.contains('"') || attr == ")" {
            return Err(Self::err(&format!("expected an attribute, got '{attr}'")));
        }
        let op = self
            .take()
            .ok_or_else(|| Self::err("expected an operator"))?;
        if op.eq_ignore_ascii_case("CONTAINS") {
            let value = self
                .take()
                .ok_or_else(|| Self::err("CONTAINS needs a value"))?;
            return Ok(Expr::Contains {
                attr,
                value: value.trim_matches('"').to_string(),
            });
        }
        let cmp = match op.as_str() {
            "=" => CmpOp::Eq,
            "!=" => CmpOp::Ne,
            ">" => CmpOp::Gt,
            "<" => CmpOp::Lt,
            ">=" => CmpOp::Ge,
            "<=" => CmpOp::Le,
            other => return Err(Self::err(&format!("unknown operator '{other}'"))),
        };
        let raw = self.take().ok_or_else(|| Self::err("expected a value"))?;
        let value = if raw.starts_with('"') && raw.ends_with('"') && raw.len() >= 2 {
            Lit::Text(raw[1..raw.len() - 1].to_string())
        } else if raw.eq_ignore_ascii_case("true") {
            Lit::Bool(true)
        } else if raw.eq_ignore_ascii_case("false") {
            Lit::Bool(false)
        } else if let Ok(n) = raw.parse::<i64>() {
            Lit::Number(n)
        } else {
            return Err(Self::err(&format!(
                "value '{raw}' must be a number, true/false, or a quoted string"
            )));
        };
        if matches!(cmp, CmpOp::Gt | CmpOp::Lt | CmpOp::Ge | CmpOp::Le)
            && !matches!(value, Lit::Number(_))
        {
            return Err(Self::err("'>' and '<' compare numbers only"));
        }
        Ok(Expr::Comparison {
            attr,
            op: cmp,
            value,
        })
    }
}

impl Expr {
    /// Parse a query expression.
    pub fn parse(text: &str) -> Result<Expr, GaipError> {
        let toks = tokenize(text)?;
        if toks.is_empty() {
            return Err(GaipError::BadField {
                field: "query",
                why: "empty query".to_string(),
            });
        }
        let mut p = Parser { toks, pos: 0 };
        let e = p.parse_or()?;
        if p.pos != p.toks.len() {
            return Err(GaipError::BadField {
                field: "query",
                why: format!("trailing token '{}'", p.toks[p.pos]),
            });
        }
        Ok(e)
    }

    /// Resolve special attributes (capability/runtime/framework/name/
    /// version/urn) from the reference, then fall back to the offer's own
    /// attributes.
    fn resolve(&self, attr: &str, offer: &Offer) -> Option<AttrValue> {
        match attr {
            "runtime" | "framework" | "name" | "urn" => {
                // The cached parse from registration, not a fresh one —
                // every resolved attribute used to re-split the URN.
                let urn = offer.urn()?;
                Some(AttrValue::Text(match attr {
                    "runtime" => urn.runtime.clone(),
                    "framework" => urn.framework.clone(),
                    "name" => urn.name.clone(),
                    _ => offer.ior.urn.clone(),
                }))
            }
            "version" => Some(AttrValue::Text(offer.ior.version.clone())),
            other => offer.attributes.get(other).cloned(),
        }
    }

    pub fn matches(&self, offer: &Offer) -> bool {
        match self {
            Self::And(a, b) => a.matches(offer) && b.matches(offer),
            Self::Or(a, b) => a.matches(offer) || b.matches(offer),
            Self::Not(a) => !a.matches(offer),
            Self::Contains { attr, value } => {
                if attr == "capability" {
                    return offer.ior.has_capability(value);
                }
                // resolved attributes too: `urn CONTAINS "engine"` reads
                // the reference, not a stored attribute
                match self.resolve(attr, offer) {
                    Some(AttrValue::Text(t)) => t.contains(value.as_str()),
                    Some(AttrValue::Number(n)) => n.to_string().contains(value.as_str()),
                    None => false,
                }
            }
            Self::Comparison { attr, op, value } => {
                if attr == "capability" {
                    // `capability = "X"` is membership, not equality of
                    // the capability list
                    if let Lit::Text(want) = value {
                        let has = offer.ior.has_capability(want);
                        return match op {
                            CmpOp::Eq => has,
                            CmpOp::Ne => !has,
                            _ => false,
                        };
                    }
                    return false;
                }
                match (self.resolve(attr, offer), value) {
                    (Some(AttrValue::Number(a)), Lit::Number(b)) => match op {
                        CmpOp::Eq => a == *b,
                        CmpOp::Ne => a != *b,
                        CmpOp::Gt => a > *b,
                        CmpOp::Lt => a < *b,
                        CmpOp::Ge => a >= *b,
                        CmpOp::Le => a <= *b,
                    },
                    (Some(AttrValue::Text(a)), Lit::Text(b)) => match op {
                        CmpOp::Eq => &a == b,
                        CmpOp::Ne => &a != b,
                        _ => false,
                    },
                    (Some(AttrValue::Text(a)), Lit::Bool(b)) => match op {
                        CmpOp::Eq => a == b.to_string(),
                        CmpOp::Ne => a != b.to_string(),
                        _ => false,
                    },
                    _ => false,
                }
            }
        }
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
        // Only the top hit is wanted: a full sort is O(n log n) for an
        // answer one `max_by` finds in O(n). `max_by` takes an ASCENDING
        // comparator — reusing the sort's descending one would return the
        // minimum — so this is the same ordering flipped. Same choice as
        // `query()[0]`, ties broken by URN either way.
        self.offers.iter().filter(|o| q.matches(o)).max_by(|a, b| {
            a.number("vram_free_mb")
                .unwrap_or(0)
                .cmp(&b.number("vram_free_mb").unwrap_or(0))
                .then(b.ior.urn.cmp(&a.ior.urn))
        })
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

    // ---- the constraint language -------------------------------------

    #[test]
    fn expressions_parse_and_evaluate() {
        let t = fixture();
        let q = Query::parse(r#"capability = "INVENTORY_OPTIMIZATION" AND vram_free_mb > 30000"#)
            .unwrap();
        let hits = t.query(&q);
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].ior.urn, "urn:agent:rust:tokio:engine-a");
    }

    #[test]
    fn or_and_not_and_parentheses_follow_precedence() {
        let t = fixture();
        // AND binds tighter than OR: (python) OR (rust AND vram>30000)
        let q = Query::parse(r#"runtime = "python" OR runtime = "rust" AND vram_free_mb > 30000"#)
            .unwrap();
        let urns: Vec<&str> = t.query(&q).iter().map(|o| o.ior.urn.as_str()).collect();
        assert_eq!(urns.len(), 2, "{urns:?}");
        assert!(urns.contains(&"urn:agent:python:langgraph:planner"));

        let q = Query::parse(
            r#"(runtime = "python" OR runtime = "rust") AND NOT vram_free_mb > 30000"#,
        )
        .unwrap();
        let urns: Vec<&str> = t.query(&q).iter().map(|o| o.ior.urn.as_str()).collect();
        assert_eq!(urns.len(), 2, "{urns:?}");
        assert!(!urns.contains(&"urn:agent:rust:tokio:engine-a"));
    }

    #[test]
    fn contains_matches_text_and_capability_membership() {
        let t = fixture();
        let q = Query::parse(r#"capability CONTAINS "FORECAST""#).unwrap();
        // CONTAINS on capability is exact membership, not substring
        assert!(t.query(&q).is_empty());

        let q = Query::parse(r#"capability = "DEMAND_FORECAST""#).unwrap();
        assert_eq!(t.query(&q).len(), 1);

        let q = Query::parse(r#"urn CONTAINS "engine""#).unwrap();
        assert_eq!(t.query(&q).len(), 2, "substring on a resolved attribute");
    }

    #[test]
    fn parse_errors_name_the_problem() {
        for (bad, needle) in [
            ("", "empty query"),
            ("vram_free_mb >", "expected a value"),
            ("vram_free_mb >\"text\"", "compare numbers only"),
            ("(runtime = \"rust\"", "missing closing"),
            ("runtime = \"rust\" AND", "expected an attribute"),
            ("runtime ~ \"rust\"", "unknown operator"),
            (
                "runtime = rust",
                "must be a number, true/false, or a quoted string",
            ),
            ("runtime = \"rust\" garbage", "trailing token"),
        ] {
            let err = Expr::parse(bad).unwrap_err().to_string();
            assert!(err.contains(needle), "{bad:?} -> {err}");
        }
    }

    #[test]
    fn the_legacy_builder_still_works_next_to_expressions() {
        let t = fixture();
        let q = Query::new()
            .capability("INVENTORY_OPTIMIZATION")
            .at_least("vram_free_mb", 32000);
        assert_eq!(t.query(&q).len(), 1);
        // and an expression can be added to a builder query
        let mut q2 = Query::new().runtime("rust");
        q2.expr = Some(Expr::parse("vram_free_mb > 30000").unwrap());
        assert_eq!(t.query(&q2).len(), 1);
    }

    #[test]
    fn withdraw_removes_the_offer() {
        let mut t = fixture();
        assert!(t.withdraw("urn:agent:rust:tokio:engine-b"));
        assert!(!t.withdraw("urn:agent:rust:tokio:engine-b"));
        assert_eq!(t.len(), 2);
    }
}
