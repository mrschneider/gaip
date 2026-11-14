//! COSNaming, for agents: names are not addresses.
//!
//! Hardcoding a URN in every caller is how a swarm becomes a config
//! file. GIOP's Naming Service bound a hierarchical name to an object
//! reference so the binding could change without touching callers; the
//! agent equivalent is `swarm/backend/engine-1` -> [`Ior`], rebound when
//! an agent is replaced, upgraded or moved.
//!
//! Names are hierarchical with `/`, each segment 1-64 chars of
//! `[A-Za-z0-9._-]`, at most 8 levels. Resolution is exact; there is no
//! wildcard search here (that is the Trader's job — matching by
//! *attribute*, not by spelling).

use crate::error::GaipError;
use crate::ior::Ior;
use std::collections::BTreeMap;

fn valid_component(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 64
        && s.chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-' || c == '.')
}

pub fn validate_name(name: &str) -> Result<(), GaipError> {
    let bad = |why: &str| GaipError::BadField {
        field: "name",
        why: why.to_string(),
    };
    if name.is_empty() {
        return Err(bad("a binding name cannot be empty"));
    }
    let parts: Vec<&str> = name.split('/').collect();
    if parts.len() > 8 {
        return Err(bad("at most 8 name levels"));
    }
    for p in parts {
        if !valid_component(p) {
            return Err(bad(&format!(
                "segment '{p}' must be 1-64 chars of [A-Za-z0-9._-]"
            )));
        }
    }
    Ok(())
}

/// A name-to-reference binding table.
#[derive(Debug, Default, Clone)]
pub struct NamingService {
    bindings: BTreeMap<String, Ior>,
}

impl NamingService {
    pub fn new() -> Self {
        Self::default()
    }

    /// Bind a name. Rebinding an existing name is allowed on purpose —
    /// that is how an agent is replaced without its callers noticing —
    /// but it is an explicit [`rebind`](Self::rebind) so a typo does not
    /// silently take over a name.
    pub fn bind(&mut self, name: &str, ior: Ior) -> Result<(), GaipError> {
        validate_name(name)?;
        ior.validate()?;
        if self.bindings.contains_key(name) {
            return Err(GaipError::BadField {
                field: "name",
                why: format!("'{name}' is already bound (use rebind to replace it)"),
            });
        }
        self.bindings.insert(name.to_string(), ior);
        Ok(())
    }

    pub fn rebind(&mut self, name: &str, ior: Ior) -> Result<(), GaipError> {
        validate_name(name)?;
        ior.validate()?;
        self.bindings.insert(name.to_string(), ior);
        Ok(())
    }

    pub fn resolve(&self, name: &str) -> Option<&Ior> {
        self.bindings.get(name)
    }

    /// Try names in order. Failover across *names* (not endpoints) is
    /// what a caller writes when it knows the service has a hot standby
    /// under a different name.
    pub fn resolve_any(&self, names: &[&str]) -> Option<&Ior> {
        names.iter().find_map(|n| self.bindings.get(*n))
    }

    pub fn unbind(&mut self, name: &str) -> bool {
        self.bindings.remove(name).is_some()
    }

    /// Every binding under a prefix (`swarm/backend` -> its children).
    pub fn list(&self, prefix: &str) -> Vec<(String, Ior)> {
        let p = if prefix.is_empty() {
            String::new()
        } else {
            format!("{prefix}/")
        };
        self.bindings
            .iter()
            .filter(|(k, _)| k.starts_with(&p))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    pub fn len(&self) -> usize {
        self.bindings.len()
    }

    pub fn is_empty(&self) -> bool {
        self.bindings.is_empty()
    }

    /// Serialize the whole binding table to JSON.
    ///
    /// The in-memory table is the only copy of the bindings: a naming
    /// service that dies on restart turns every caller into a config file
    /// again. This is the form a registry writes to disk (or a checkpoint
    /// path) so a rebinding survives the process. Each row is a
    /// `["name", "IOR:..."]` pair; the IOR travels as its full form, which
    /// carries the failover list and capabilities, so no second lookup is
    /// needed to restore it.
    pub fn to_json(&self) -> Result<String, GaipError> {
        let rows: Vec<(String, String)> = self
            .bindings
            .iter()
            .map(|(k, v)| (k.clone(), v.to_string()))
            .collect();
        serde_json::to_string(&rows).map_err(|e| GaipError::BadField {
            field: "naming",
            why: format!("serialize: {e}"),
        })
    }

    /// Restore a table written by [`Self::to_json`].
    ///
    /// Every name and every IOR is re-validated on the way in, so a
    /// corrupt or hand-edited file is refused whole rather than letting a
    /// bad reference into the table (the same rule `bind` enforces). The
    /// table starts empty: a failure restores nothing.
    pub fn from_json(s: &str) -> Result<Self, GaipError> {
        let rows: Vec<(String, String)> = serde_json::from_str(s).map_err(|e| GaipError::BadField {
            field: "naming",
            why: format!("deserialize: {e}"),
        })?;
        let mut ns = Self::new();
        for (name, ior_text) in rows {
            let ior: Ior = ior_text.parse()?;
            // rebind, not bind: a duplicate row in the file is a
            // last-write-wins restore, not an error worth refusing the
            // whole table for.
            ns.rebind(&name, ior)?;
        }
        Ok(ns)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ior(name: &str, ep: &str) -> Ior {
        Ior::new(
            format!("urn:agent:rust:tokio:{name}"),
            vec![ep.to_string()],
            "1.0",
        )
    }

    #[test]
    fn bind_resolve_and_rebind_without_touching_callers() {
        let mut ns = NamingService::new();
        ns.bind("swarm/backend/engine-1", ior("engine-1", "10.0.0.7:7610"))
            .unwrap();
        assert_eq!(
            ns.resolve("swarm/backend/engine-1")
                .unwrap()
                .primary_endpoint(),
            Some("10.0.0.7:7610")
        );
        // the agent is replaced by a new one on another host
        ns.rebind("swarm/backend/engine-1", ior("engine-2", "10.0.0.9:7610"))
            .unwrap();
        assert!(
            ns.resolve("swarm/backend/engine-1")
                .unwrap()
                .urn
                .contains("engine-2"),
            "the name was rebound to the replacement agent"
        );
    }

    #[test]
    fn bind_refuses_to_silently_overwrite() {
        let mut ns = NamingService::new();
        ns.bind("a/b", ior("x", "h:1")).unwrap();
        let err = ns.bind("a/b", ior("y", "h:2")).unwrap_err();
        assert!(err.to_string().contains("rebind"), "{err}");
    }

    #[test]
    fn names_are_validated() {
        for bad in ["", "a//b", "a b/c", &"x".repeat(65), "a/b/c/d/e/f/g/h/i"] {
            assert!(validate_name(bad).is_err(), "'{bad}' must be invalid");
        }
        assert!(validate_name("swarm/backend/engine-1").is_ok());
        assert!(validate_name("a").is_ok());
    }

    #[test]
    fn a_bad_reference_never_enters_the_table() {
        let mut ns = NamingService::new();
        let mut bad = ior("x", "h:1");
        bad.endpoints.clear();
        assert!(ns.bind("a/b", bad).is_err());
        assert!(ns.is_empty(), "a refused binding must not create an entry");
    }

    #[test]
    fn failover_across_names() {
        let mut ns = NamingService::new();
        ns.bind("swarm/engine/primary", ior("primary", "10.0.0.1:7610"))
            .unwrap();
        ns.bind("swarm/engine/standby", ior("standby", "10.0.0.2:7610"))
            .unwrap();
        let found = ns
            .resolve_any(&["swarm/engine/gone", "swarm/engine/standby"])
            .unwrap();
        assert_eq!(found.primary_endpoint(), Some("10.0.0.2:7610"));
        assert!(ns.resolve_any(&["nope"]).is_none());
    }

    #[test]
    fn listing_by_prefix_and_unbinding() {
        let mut ns = NamingService::new();
        ns.bind("swarm/a", ior("a", "h:1")).unwrap();
        ns.bind("swarm/b", ior("b", "h:2")).unwrap();
        ns.bind("other/c", ior("c", "h:3")).unwrap();
        assert_eq!(ns.list("swarm").len(), 2);
        assert_eq!(ns.list("").len(), 3);
        assert!(ns.unbind("swarm/a"));
        assert!(!ns.unbind("swarm/a"));
        assert!(ns.resolve("swarm/a").is_none());
    }

    #[test]
    fn the_binding_table_round_trips_through_json() {
        let mut ns = NamingService::new();
        ns.bind(
            "swarm/backend/engine-1",
            ior("engine-1", "10.0.0.7:7610"),
        )
        .unwrap();
        ns.bind("swarm/engine/standby", ior("standby", "10.0.0.2:7610"))
            .unwrap();

        let wire = ns.to_json().unwrap();
        let back = NamingService::from_json(&wire).unwrap();
        assert_eq!(back.len(), 2);
        assert_eq!(
            back.resolve("swarm/backend/engine-1")
                .unwrap()
                .primary_endpoint(),
            Some("10.0.0.7:7610")
        );
        // capabilities and version ride along in the IOR's full form
        let rich = Ior::new("urn:agent:rust:tokio:big", vec!["10.0.0.9:7610".into()], "2.1")
            .with_capabilities(vec!["DEMAND_FORECAST".into()]);
        ns.rebind("swarm/big", rich).unwrap();
        let wire2 = ns.to_json().unwrap();
        let back2 = NamingService::from_json(&wire2).unwrap();
        let restored = back2.resolve("swarm/big").unwrap();
        assert!(restored.has_capability("DEMAND_FORECAST"));
        assert_eq!(restored.version, "2.1");
    }

    #[test]
    fn an_empty_table_round_trips_to_an_empty_table() {
        let ns = NamingService::new();
        let wire = ns.to_json().unwrap();
        assert_eq!(wire, "[]");
        let back = NamingService::from_json(&wire).unwrap();
        assert!(back.is_empty());
    }

    #[test]
    fn a_corrupt_table_is_refused_whole() {
        // not JSON at all
        assert!(NamingService::from_json("nope").is_err());
        // valid JSON, but the IOR field is not a valid reference: a bad
        // row must not create a partial table
        let bad = r#"[["a/b", "not-an-ior"]]"#;
        let err = NamingService::from_json(bad).unwrap_err();
        assert!(err.to_string().contains("naming") || err.to_string().contains("IOR"), "{err}");
        // valid JSON, wrong shape (a bare string, not a [name, ior] pair)
        assert!(NamingService::from_json(r#""just-a-string""#).is_err());
    }
}
