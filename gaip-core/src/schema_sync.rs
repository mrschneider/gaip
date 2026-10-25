//! Schema distribution: the IR is not a static file.
//!
//! A participant that learns a new intent must be able to tell its
//! peers, or the fleet drifts back into the problem the protocol exists
//! to stop. GIOP's Interface Repository was a lookup service; this is the
//! same idea with a push: [`SchemaSync`] carries IDL text, peers merge it
//! into their own [`InterfaceRepository`], and a conflicting redefinition
//! of the *same* intent+version is refused — a schema that changes shape
//! without changing version is precisely the drift to prevent.

use crate::error::GaipError;
use crate::idl::{parse, InterfaceRepository, TypeSpec};

/// A schema update in flight: the IDL source plus who published it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaSync {
    pub publisher: String,
    pub idl: String,
}

impl SchemaSync {
    pub fn new(publisher: impl Into<String>, idl: impl Into<String>) -> Self {
        Self {
            publisher: publisher.into(),
            idl: idl.into(),
        }
    }
}

/// The outcome of merging a peer's schema.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct MergeReport {
    pub added: Vec<(String, String)>,
    /// identical re-declarations: harmless idempotency
    pub unchanged: Vec<(String, String)>,
}

/// Merge IDL text into a repository.
///
/// * a new intent+version is added;
/// * the same intent+version with the *same* shape is a no-op (idempotent
///   retries must be safe);
/// * the same intent+version with a *different* shape is refused — that
///   is a schema change without a version bump.
pub fn merge(
    repo: &mut InterfaceRepository,
    update: &SchemaSync,
) -> Result<MergeReport, GaipError> {
    let specs: Vec<TypeSpec> = parse(&update.idl)?;
    let mut report = MergeReport::default();
    for spec in specs {
        match repo.lookup(&spec.intent, &spec.version) {
            Some(existing) if *existing == spec => {
                report.unchanged.push((spec.intent, spec.version));
            }
            Some(_) => {
                return Err(GaipError::BadField {
                    field: "schema",
                    why: format!(
                        "{} v{} is already declared with a different shape (publisher {}) — \
                         bump the version instead of redefining it",
                        spec.intent, spec.version, update.publisher
                    ),
                })
            }
            None => {
                report
                    .added
                    .push((spec.intent.clone(), spec.version.clone()));
                repo.insert(spec);
            }
        }
    }
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;

    const BASE: &str = r#"
        intent INVENTORY_OPTIMIZATION version 1.0 {
          facility_id: int
          horizon_days: int
          artifacts_ref: [string]
        }
    "#;

    #[test]
    fn a_new_intent_is_added() {
        let mut repo = InterfaceRepository::new();
        let update = SchemaSync::new(
            "urn:agent:python:planner",
            "intent DEMAND_FORECAST version 1.0 {\n  sku: string\n}",
        );
        let report = merge(&mut repo, &update).unwrap();
        assert_eq!(report.added, vec![("DEMAND_FORECAST".into(), "1.0".into())]);
        assert!(repo.lookup("DEMAND_FORECAST", "1.0").is_some());
    }

    #[test]
    fn an_identical_republish_is_idempotent() {
        let mut repo = InterfaceRepository::load(BASE).unwrap();
        let report = merge(&mut repo, &SchemaSync::new("urn:agent:a", BASE)).unwrap();
        assert_eq!(report.added.len(), 0);
        assert_eq!(
            report.unchanged,
            vec![("INVENTORY_OPTIMIZATION".into(), "1.0".into())],
            "a retried sync must not fail"
        );
    }

    #[test]
    fn a_redefinition_without_a_version_bump_is_refused() {
        let mut repo = InterfaceRepository::load(BASE).unwrap();
        let sneaky = r#"
            intent INVENTORY_OPTIMIZATION version 1.0 {
              facility_id: string
              horizon_days: int
              artifacts_ref: [string]
            }
        "#;
        let err = merge(&mut repo, &SchemaSync::new("urn:agent:evil", sneaky))
            .unwrap_err()
            .to_string();
        assert!(err.contains("bump the version"), "{err}");
    }

    #[test]
    fn a_new_minor_version_is_added_alongside() {
        let mut repo = InterfaceRepository::load(BASE).unwrap();
        let v11 = r#"
            intent INVENTORY_OPTIMIZATION version 1.1 {
              facility_id: int
              horizon_days: int
              artifacts_ref: [string]
              note: string?
            }
        "#;
        let report = merge(&mut repo, &SchemaSync::new("urn:agent:a", v11)).unwrap();
        assert_eq!(
            report.added,
            vec![("INVENTORY_OPTIMIZATION".into(), "1.1".into())]
        );
        assert!(
            repo.lookup("INVENTORY_OPTIMIZATION", "1.0").is_some(),
            "1.0 stays"
        );
        assert_eq!(
            repo.lookup_compatible("INVENTORY_OPTIMIZATION", "1.0")
                .unwrap()
                .version,
            "1.1",
            "the newest compatible minor serves"
        );
    }

    #[test]
    fn malformed_idl_in_a_push_is_refused() {
        let mut repo = InterfaceRepository::new();
        assert!(merge(
            &mut repo,
            &SchemaSync::new("urn:agent:a", "intent broken {")
        )
        .is_err());
        assert!(repo.is_empty(), "a bad push must not half-apply");
    }
}
