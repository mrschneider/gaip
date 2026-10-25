//! Agent URNs: `urn:agent:<runtime>:<framework>:<name>[:<version>]`.
//!
//! The URN is the addressing contract. It is *not* a locator (the
//! transport knows where to send bytes); it is the identity a message
//! names so a reply comes back to the right component regardless of which
//! binding carried the request. It deliberately carries the runtime and
//! framework: when a Java connector starts misrouting, the first question
//! is "which participant is that", and the answer is in the address.

use crate::error::GAIPError;
use std::fmt;
use std::str::FromStr;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct AgentUrn {
    pub runtime: String,
    pub framework: String,
    pub name: String,
    pub version: Option<String>,
}

fn valid_segment(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 64
        && s.chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.')
}

impl AgentUrn {
    pub fn new(
        runtime: impl Into<String>,
        framework: impl Into<String>,
        name: impl Into<String>,
        version: Option<impl Into<String>>,
    ) -> Result<Self, GAIPError> {
        let urn = Self {
            runtime: runtime.into(),
            framework: framework.into(),
            name: name.into(),
            version: version.map(Into::into),
        };
        urn.validate()?;
        Ok(urn)
    }

    fn validate(&self) -> Result<(), GAIPError> {
        for (label, seg) in [
            ("runtime", &self.runtime),
            ("framework", &self.framework),
            ("name", &self.name),
        ] {
            if !valid_segment(seg) {
                return Err(GAIPError::BadUrn {
                    value: self.to_string(),
                    why: format!("{label} segment '{seg}' must be 1-64 chars of [A-Za-z0-9._-]"),
                });
            }
        }
        if let Some(v) = &self.version {
            if !valid_segment(v) {
                return Err(GAIPError::BadUrn {
                    value: self.to_string(),
                    why: format!("version segment '{v}' is not a valid token"),
                });
            }
        }
        Ok(())
    }

    /// Two participants are the same agent when the URNs match exactly:
    /// a different version is a different agent, and conflating them is
    /// how a rolling upgrade corrupts a task.
    pub fn same_agent(&self, other: &Self) -> bool {
        self == other
    }
}

impl FromStr for AgentUrn {
    type Err = GAIPError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let bad = |why: &str| GAIPError::BadUrn {
            value: s.to_string(),
            why: why.to_string(),
        };
        let rest = s
            .strip_prefix("urn:agent:")
            .ok_or_else(|| bad("must start with 'urn:agent:'"))?;
        let parts: Vec<&str> = rest.split(':').collect();
        if parts.len() < 3 || parts.len() > 4 {
            return Err(bad(
                "expected urn:agent:<runtime>:<framework>:<name>[:<version>]",
            ));
        }
        let urn = Self {
            runtime: parts[0].to_string(),
            framework: parts[1].to_string(),
            name: parts[2].to_string(),
            version: parts.get(3).map(|v| v.to_string()),
        };
        urn.validate()?;
        Ok(urn)
    }
}

impl fmt::Display for AgentUrn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.version {
            Some(v) => write!(
                f,
                "urn:agent:{}:{}:{}:{}",
                self.runtime, self.framework, self.name, v
            ),
            None => write!(
                f,
                "urn:agent:{}:{}:{}",
                self.runtime, self.framework, self.name
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_the_reference_urns() {
        let u: AgentUrn = "urn:agent:python:langgraph:planner-v2".parse().unwrap();
        assert_eq!(u.runtime, "python");
        assert_eq!(u.framework, "langgraph");
        assert_eq!(u.name, "planner-v2");
        assert_eq!(u.version, None);

        let v: AgentUrn = "urn:agent:rust:tokio:analytics-engine-v1:1.4.2"
            .parse()
            .unwrap();
        assert_eq!(v.version.as_deref(), Some("1.4.2"));
        assert_eq!(
            v.to_string(),
            "urn:agent:rust:tokio:analytics-engine-v1:1.4.2"
        );
    }

    #[test]
    fn roundtrips_through_display() {
        for s in [
            "urn:agent:python:langgraph:planner-v2",
            "urn:agent:java:spring:legacy-connector:2.1",
            "urn:agent:rust:tokio:engine_1",
        ] {
            let u: AgentUrn = s.parse().unwrap();
            assert_eq!(u.to_string(), s);
        }
    }

    #[test]
    fn rejects_with_a_reason() {
        for (bad, needle) in [
            ("agent:python:x:y", "must start with 'urn:agent:'"),
            ("urn:agent:python", "expected urn:agent:"),
            (
                "urn:agent:python:langgraph:planner:v:extra",
                "expected urn:agent:",
            ),
            ("urn:agent:py thon:langgraph:x", "must be 1-64 chars"),
            ("urn:agent:python::x", "must be 1-64 chars"),
        ] {
            let err = bad.parse::<AgentUrn>().unwrap_err();
            assert!(err.to_string().contains(needle), "'{bad}' -> '{err}'");
        }
    }

    #[test]
    fn version_is_part_of_identity() {
        let a: AgentUrn = "urn:agent:rust:tokio:engine:1.0".parse().unwrap();
        let b: AgentUrn = "urn:agent:rust:tokio:engine:1.1".parse().unwrap();
        assert!(!a.same_agent(&b), "a rolling upgrade is a different agent");
        assert!(a.same_agent(&a.clone()));
    }
}
