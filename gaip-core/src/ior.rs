//! Interoperable Object References: the address document.
//!
//! A GIOP IOR is what you hand someone instead of a host:port — profile,
//! type, endpoints, and the key that identifies the object. Its modern
//! use here is narrower but just as useful: one portable string that says
//! "this agent, these endpoints (in failover order), this version, these
//! capabilities".
//!
//! Two forms, both parseable:
//!
//! ```text
//! IOR:urn=urn:agent:rust:tokio:engine:2.0;v=1.0;ep=10.0.0.7:7610,10.0.0.8:7610;cap=INVENTORY_OPTIMIZATION
//! corbaloc:gaip@10.0.0.7:7610/urn:agent:rust:tokio:engine:2.0
//! ```
//!
//! `corbaloc` is the short form a human types; the `IOR:` form is what a
//! registry stores, because it carries the failover list and the
//! capabilities without a second lookup.

use crate::error::GaipError;
use crate::urn::AgentUrn;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Ior {
    pub urn: String,
    /// `host:port`, most preferred first (failover order).
    pub endpoints: Vec<String>,
    pub version: String,
    pub capabilities: Vec<String>,
}

fn valid_cap(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 64
        && s.chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-' || c == '.')
}

impl Ior {
    pub fn new(urn: impl Into<String>, endpoints: Vec<String>, version: impl Into<String>) -> Self {
        Self {
            urn: urn.into(),
            endpoints,
            version: version.into(),
            capabilities: Vec::new(),
        }
    }

    pub fn with_capabilities(mut self, caps: Vec<String>) -> Self {
        self.capabilities = caps;
        self
    }

    pub fn validate(&self) -> Result<(), GaipError> {
        self.urn.parse::<AgentUrn>()?;
        if self.endpoints.is_empty() {
            return Err(GaipError::BadUrn {
                value: self.to_string(),
                why: "an IOR needs at least one endpoint".to_string(),
            });
        }
        for ep in &self.endpoints {
            let (host, port) = ep.rsplit_once(':').ok_or_else(|| GaipError::BadUrn {
                value: self.to_string(),
                why: format!("endpoint '{ep}' is not host:port"),
            })?;
            if host.is_empty() || port.parse::<u16>().is_err() {
                return Err(GaipError::BadUrn {
                    value: self.to_string(),
                    why: format!("endpoint '{ep}' is not host:port"),
                });
            }
        }
        if !self
            .version
            .chars()
            .next()
            .is_some_and(|c| c.is_ascii_digit())
        {
            return Err(GaipError::BadUrn {
                value: self.to_string(),
                why: format!("version '{}' must start with a digit", self.version),
            });
        }
        for c in &self.capabilities {
            if !valid_cap(c) {
                return Err(GaipError::BadUrn {
                    value: self.to_string(),
                    why: format!("capability '{c}' has invalid characters"),
                });
            }
        }
        Ok(())
    }

    pub fn primary_endpoint(&self) -> Option<&str> {
        self.endpoints.first().map(String::as_str)
    }

    pub fn has_capability(&self, cap: &str) -> bool {
        self.capabilities.iter().any(|c| c == cap)
    }

    /// Parse the short `corbaloc` form.
    pub fn from_corbaloc(s: &str) -> Result<Self, GaipError> {
        let rest = s
            .strip_prefix("corbaloc:gaip@")
            .ok_or_else(|| GaipError::BadUrn {
                value: s.to_string(),
                why: "expected corbaloc:gaip@host:port/<urn>".to_string(),
            })?;
        let (endpoints, urn) = rest.split_once('/').ok_or_else(|| GaipError::BadUrn {
            value: s.to_string(),
            why: "missing '/<urn>' part".to_string(),
        })?;
        let ior = Self::new(
            urn,
            endpoints.split(',').map(str::to_string).collect(),
            "1.0",
        );
        ior.validate()?;
        Ok(ior)
    }

    /// The short form, for a human or a config file.
    pub fn to_corbaloc(&self) -> String {
        format!("corbaloc:gaip@{}/{}", self.endpoints.join(","), self.urn)
    }
}

impl fmt::Display for Ior {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "IOR:urn={};v={};ep={}",
            self.urn,
            self.version,
            self.endpoints.join(",")
        )?;
        if !self.capabilities.is_empty() {
            write!(f, ";cap={}", self.capabilities.join(","))?;
        }
        Ok(())
    }
}

impl FromStr for Ior {
    type Err = GaipError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.starts_with("corbaloc:") {
            return Self::from_corbaloc(s);
        }
        let rest = s.strip_prefix("IOR:").ok_or_else(|| GaipError::BadUrn {
            value: s.to_string(),
            why: "an IOR must start with 'IOR:' or 'corbaloc:'".to_string(),
        })?;
        let mut urn = String::new();
        let mut version = "1.0".to_string();
        let mut endpoints: Vec<String> = Vec::new();
        let mut caps: Vec<String> = Vec::new();
        for field in rest.split(';') {
            let (k, v) = field.split_once('=').ok_or_else(|| GaipError::BadUrn {
                value: s.to_string(),
                why: format!("field '{field}' is not key=value"),
            })?;
            match k {
                "urn" => urn = v.to_string(),
                "v" => version = v.to_string(),
                "ep" => endpoints = v.split(',').map(str::to_string).collect(),
                "cap" => {
                    caps = v
                        .split(',')
                        .filter(|c| !c.is_empty())
                        .map(str::to_string)
                        .collect()
                }
                other => {
                    return Err(GaipError::BadUrn {
                        value: s.to_string(),
                        why: format!("unknown IOR field '{other}'"),
                    })
                }
            }
        }
        let ior = Self {
            urn,
            endpoints,
            version,
            capabilities: caps,
        };
        ior.validate()?;
        Ok(ior)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn full_form_roundtrips_with_failover_and_capabilities() {
        let ior = Ior::new(
            "urn:agent:rust:tokio:engine:2.0",
            vec!["10.0.0.7:7610".into(), "10.0.0.8:7610".into()],
            "1.0",
        )
        .with_capabilities(vec![
            "INVENTORY_OPTIMIZATION".into(),
            "DEMAND_FORECAST".into(),
        ]);
        let text = ior.to_string();
        assert!(text.starts_with("IOR:urn=urn:agent:"));
        let back: Ior = text.parse().unwrap();
        assert_eq!(back, ior);
        assert_eq!(back.primary_endpoint(), Some("10.0.0.7:7610"));
        assert!(back.has_capability("DEMAND_FORECAST"));
        assert!(!back.has_capability("TRANSLATE"));
    }

    #[test]
    fn corbaloc_short_form_both_ways() {
        let ior = Ior::new(
            "urn:agent:rust:tokio:engine",
            vec!["127.0.0.1:7610".into()],
            "1.0",
        );
        let short = ior.to_corbaloc();
        assert_eq!(
            short,
            "corbaloc:gaip@127.0.0.1:7610/urn:agent:rust:tokio:engine"
        );
        let back: Ior = short.parse().unwrap();
        assert_eq!(back.urn, ior.urn);
        assert_eq!(back.endpoints, ior.endpoints);
    }

    #[test]
    fn rejects_broken_references_with_a_reason() {
        for (bad, needle) in [
            ("http://host/x", "must start with"),
            ("IOR:urn=bad;v=1.0;ep=h:1", "invalid agent URN"),
            ("IOR:urn=urn:agent:a:b:c;v=1.0", "at least one endpoint"),
            ("IOR:urn=urn:agent:a:b:c;v=1.0;ep=nohostport", "host:port"),
            (
                "IOR:urn=urn:agent:a:b:c;v=x;ep=h:1",
                "must start with a digit",
            ),
            (
                "IOR:urn=urn:agent:a:b:c;ep=h:1;bogus=1",
                "unknown IOR field",
            ),
            ("corbaloc:gaip@host:1", "missing '/<urn>'"),
        ] {
            let err = bad.parse::<Ior>().unwrap_err().to_string();
            assert!(err.contains(needle), "'{bad}' -> '{err}'");
        }
    }

    #[test]
    fn an_ior_is_not_a_locator_for_any_string() {
        let err = "urn:agent:rust:tokio:engine".parse::<Ior>().unwrap_err();
        assert!(err.to_string().contains("IOR:"), "{err}");
    }
}
