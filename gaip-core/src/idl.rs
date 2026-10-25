//! A small IDL for agent payloads, and the repository that serves it.
//!
//! CORBA's IDL is why a Java DTO and a C++ struct could talk: both were
//! generated from the same declaration instead of written twice. GAIP's
//! `payload` was `object` — free-form — which is honest but leaves the
//! "zero drift" thesis half-done: nothing could tell a peer what
//! `INVENTORY_OPTIMIZATION` actually contains.
//!
//! This is that declaration, in the smallest syntax that carries the
//! guarantee:
//!
//! ```text
//! intent INVENTORY_OPTIMIZATION version 1.0 {
//!   facility_id: int
//!   horizon_days: int
//!   artifacts_ref: [string]
//!   note: string?
//! }
//! ```
//!
//! Types: `int`, `float`, `string`, `bool`, and their list forms
//! `[int]`, `[float]`, `[string]`. A trailing `?` marks a field
//! optional. Anything else is a parse error with the line number — an
//! IDL that guesses is worse than no IDL.

use crate::error::GaipError;
use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FieldKind {
    Int,
    Float,
    Str,
    Bool,
    IntList,
    FloatList,
    StrList,
}

impl FieldKind {
    fn parse(tok: &str) -> Option<Self> {
        Some(match tok {
            "int" => Self::Int,
            "float" => Self::Float,
            "string" => Self::Str,
            "bool" => Self::Bool,
            "[int]" => Self::IntList,
            "[float]" => Self::FloatList,
            "[string]" => Self::StrList,
            _ => return None,
        })
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::Int => "int",
            Self::Float => "float",
            Self::Str => "string",
            Self::Bool => "bool",
            Self::IntList => "[int]",
            Self::FloatList => "[float]",
            Self::StrList => "[string]",
        }
    }

    /// Does a JSON value satisfy this type?
    pub fn accepts(self, v: &serde_json::Value) -> bool {
        match self {
            Self::Int => v.is_i64() || v.is_u64(),
            Self::Float => v.is_f64() || v.is_i64() || v.is_u64(),
            Self::Str => v.is_string(),
            Self::Bool => v.is_boolean(),
            Self::IntList => v
                .as_array()
                .is_some_and(|a| a.iter().all(|x| x.is_i64() || x.is_u64())),
            Self::FloatList => v
                .as_array()
                .is_some_and(|a| a.iter().all(|x| x.is_number())),
            Self::StrList => v
                .as_array()
                .is_some_and(|a| a.iter().all(|x| x.is_string())),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Field {
    pub name: String,
    pub kind: FieldKind,
    pub optional: bool,
}

/// The declared shape of one intent's payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TypeSpec {
    pub intent: String,
    pub version: String,
    pub fields: Vec<Field>,
}

impl TypeSpec {
    pub fn field(&self, name: &str) -> Option<&Field> {
        self.fields.iter().find(|f| f.name == name)
    }

    /// Validate a payload against this declaration.
    ///
    /// Strict on purpose: an unknown field is drift, and drift is the
    /// thing the protocol exists to stop. A peer that needs a new field
    /// bumps the intent's version.
    pub fn validate(&self, payload: &serde_json::Value) -> Result<(), GaipError> {
        let obj = payload.as_object().ok_or(GaipError::PayloadNotObject)?;
        for f in &self.fields {
            match obj.get(&f.name) {
                None if f.optional => {}
                None => {
                    return Err(GaipError::BadField {
                        field: "payload",
                        why: format!(
                            "{} v{} requires '{}: {}'",
                            self.intent,
                            self.version,
                            f.name,
                            f.kind.name()
                        ),
                    })
                }
                Some(v) => {
                    if !f.kind.accepts(v) {
                        return Err(GaipError::BadField {
                            field: "payload",
                            why: format!(
                                "{}.{} must be {} (got {})",
                                self.intent,
                                f.name,
                                f.kind.name(),
                                v
                            ),
                        });
                    }
                }
            }
        }
        for key in obj.keys() {
            if self.field(key).is_none() {
                return Err(GaipError::BadField {
                    field: "payload",
                    why: format!(
                        "{}.{} is not declared in {} v{} (drift)",
                        self.intent, key, self.intent, self.version
                    ),
                });
            }
        }
        Ok(())
    }
}

/// Parse an IDL document into type specifications.
pub fn parse(text: &str) -> Result<Vec<TypeSpec>, GaipError> {
    let mut specs: Vec<TypeSpec> = Vec::new();
    let mut current: Option<TypeSpec> = None;

    for (n, raw) in text.lines().enumerate() {
        let line = raw.split("//").next().unwrap_or("").trim();
        if line.is_empty() {
            continue;
        }
        let bad = |why: &str| GaipError::BadField {
            field: "idl",
            why: format!("line {}: {why}", n + 1),
        };
        if current.is_none() {
            let toks: Vec<&str> = line.split_whitespace().collect();
            // intent NAME version V {
            if toks.len() != 5 || toks[0] != "intent" || toks[2] != "version" || toks[4] != "{" {
                return Err(bad("expected 'intent <NAME> version <V> {'"));
            }
            let name = toks[1];
            if !name
                .chars()
                .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_')
            {
                return Err(bad("intent names are SCREAMING_SNAKE_CASE"));
            }
            current = Some(TypeSpec {
                intent: name.to_string(),
                version: toks[3].to_string(),
                fields: Vec::new(),
            });
            continue;
        }
        if line == "}" {
            let spec = current.take().expect("open spec");
            if specs
                .iter()
                .any(|s| s.intent == spec.intent && s.version == spec.version)
            {
                return Err(bad("duplicate intent version"));
            }
            specs.push(spec);
            continue;
        }
        let (name, ty) = line
            .split_once(':')
            .ok_or_else(|| bad("expected '<field>: <type>'"))?;
        let name = name.trim();
        if !name
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
        {
            return Err(bad("field names are lower_snake_case"));
        }
        let ty = ty.trim();
        let (ty, optional) = match ty.strip_suffix('?') {
            Some(t) => (t.trim(), true),
            None => (ty, false),
        };
        let kind = FieldKind::parse(ty).ok_or_else(|| bad(&format!("unknown type '{ty}'")))?;
        let spec = current.as_mut().expect("open spec");
        if spec.field(name).is_some() {
            return Err(bad(&format!("duplicate field '{name}'")));
        }
        spec.fields.push(Field {
            name: name.to_string(),
            kind,
            optional,
        });
    }
    if let Some(spec) = current {
        return Err(GaipError::BadField {
            field: "idl",
            why: format!(
                "intent {} v{} is never closed with '}}'",
                spec.intent, spec.version
            ),
        });
    }
    if specs.is_empty() {
        return Err(GaipError::BadField {
            field: "idl",
            why: "no intent declared".to_string(),
        });
    }
    Ok(specs)
}

/// The Interface Repository: intent+version -> declaration.
#[derive(Debug, Default, Clone)]
pub struct InterfaceRepository {
    specs: BTreeMap<(String, String), TypeSpec>,
}

impl InterfaceRepository {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn load(idl: &str) -> Result<Self, GaipError> {
        let mut ir = Self::new();
        for spec in parse(idl)? {
            ir.insert(spec);
        }
        Ok(ir)
    }

    pub fn insert(&mut self, spec: TypeSpec) {
        self.specs
            .insert((spec.intent.clone(), spec.version.clone()), spec);
    }

    pub fn lookup(&self, intent: &str, version: &str) -> Option<&TypeSpec> {
        self.specs.get(&(intent.to_string(), version.to_string()))
    }

    /// Same major version, any minor: schemas are additive by contract.
    pub fn lookup_compatible(&self, intent: &str, version: &str) -> Option<&TypeSpec> {
        let want_major = version.split('.').next().unwrap_or("");
        self.specs
            .iter()
            .filter(|((i, v), _)| i == intent && v.split('.').next() == Some(want_major))
            .map(|(_, s)| s)
            .max_by_key(|s| s.version.clone())
    }

    /// Validate a payload against the declared intent.
    pub fn validate_payload(
        &self,
        intent: &str,
        version: &str,
        payload: &serde_json::Value,
    ) -> Result<(), GaipError> {
        // `intent` is the DISCRIMINATOR (which schema applies), not a
        // declared field: validating it as an unknown field would refuse
        // every well-formed request
        let mut body = payload.clone();
        if let Some(obj) = body.as_object_mut() {
            obj.remove("intent");
        }
        match self.lookup_compatible(intent, version) {
            Some(spec) => spec.validate(&body),
            None => Err(GaipError::BadField {
                field: "payload",
                why: format!("no schema registered for intent '{intent}' v{version}"),
            }),
        }
    }

    pub fn list(&self) -> Vec<(&str, &str)> {
        self.specs
            .iter()
            .map(|((i, v), _)| (i.as_str(), v.as_str()))
            .collect()
    }

    pub fn len(&self) -> usize {
        self.specs.len()
    }

    pub fn is_empty(&self) -> bool {
        self.specs.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const IDL: &str = r#"
        // inventory optimisation, the reference intent
        intent INVENTORY_OPTIMIZATION version 1.0 {
          facility_id: int
          horizon_days: int
          artifacts_ref: [string]
          note: string?
        }

        intent DEMAND_FORECAST version 1.0 {
          sku: string
          horizon_days: int
        }
    "#;

    fn ir() -> InterfaceRepository {
        InterfaceRepository::load(IDL).unwrap()
    }

    #[test]
    fn parses_the_document_into_specs() {
        let ir = ir();
        assert_eq!(ir.len(), 2);
        let spec = ir.lookup("INVENTORY_OPTIMIZATION", "1.0").unwrap();
        assert_eq!(spec.fields.len(), 4);
        assert_eq!(spec.field("facility_id").unwrap().kind, FieldKind::Int);
        assert_eq!(
            spec.field("artifacts_ref").unwrap().kind,
            FieldKind::StrList
        );
        assert!(spec.field("note").unwrap().optional);
    }

    #[test]
    fn the_intent_discriminator_is_not_a_payload_field() {
        let ir = ir();
        let payload = json!({
            "intent": "INVENTORY_OPTIMIZATION",
            "facility_id": 402,
            "horizon_days": 30,
            "artifacts_ref": []
        });
        assert!(
            ir.validate_payload("INVENTORY_OPTIMIZATION", "1.0", &payload)
                .is_ok(),
            "the discriminator must be stripped before validation"
        );
    }

    #[test]
    fn a_valid_payload_passes() {
        let ir = ir();
        let payload = json!({
            "facility_id": 402,
            "horizon_days": 30,
            "artifacts_ref": ["s3://enterprise-data/warehouse_402.parquet"]
        });
        assert!(ir
            .validate_payload("INVENTORY_OPTIMIZATION", "1.0", &payload)
            .is_ok());
    }

    #[test]
    fn a_missing_required_field_names_the_field_and_its_type() {
        let ir = ir();
        let err = ir
            .validate_payload(
                "INVENTORY_OPTIMIZATION",
                "1.0",
                &json!({"facility_id": 402, "artifacts_ref": []}),
            )
            .unwrap_err()
            .to_string();
        assert!(err.contains("horizon_days"), "{err}");
        assert!(err.contains("int"), "{err}");
    }

    #[test]
    fn a_wrong_type_is_refused() {
        let ir = ir();
        let err = ir
            .validate_payload(
                "INVENTORY_OPTIMIZATION",
                "1.0",
                &json!({"facility_id": "402", "horizon_days": 30, "artifacts_ref": []}),
            )
            .unwrap_err()
            .to_string();
        assert!(err.contains("facility_id") && err.contains("int"), "{err}");
    }

    #[test]
    fn an_undeclared_field_is_drift_and_is_refused() {
        let ir = ir();
        let err = ir
            .validate_payload(
                "INVENTORY_OPTIMIZATION",
                "1.0",
                &json!({"facility_id": 1, "horizon_days": 2, "artifacts_ref": [],
                        "facilityId": 1}),
            )
            .unwrap_err()
            .to_string();
        assert!(err.contains("facilityId") && err.contains("drift"), "{err}");
    }

    #[test]
    fn optional_fields_may_be_absent_but_must_be_typed_when_present() {
        let ir = ir();
        assert!(ir
            .validate_payload(
                "INVENTORY_OPTIMIZATION",
                "1.0",
                &json!({"facility_id": 1, "horizon_days": 2, "artifacts_ref": []})
            )
            .is_ok());
        assert!(ir
            .validate_payload(
                "INVENTORY_OPTIMIZATION",
                "1.0",
                &json!({"facility_id": 1, "horizon_days": 2, "artifacts_ref": [], "note": 5})
            )
            .is_err());
    }

    #[test]
    fn unknown_intent_has_no_schema_to_validate_against() {
        let ir = ir();
        let err = ir
            .validate_payload("TRANSLATE", "1.0", &json!({}))
            .unwrap_err()
            .to_string();
        assert!(err.contains("no schema"), "{err}");
    }

    #[test]
    fn minor_versions_are_compatible_when_the_major_matches() {
        let mut ir = ir();
        ir.insert(TypeSpec {
            intent: "INVENTORY_OPTIMIZATION".into(),
            version: "1.2".into(),
            fields: vec![Field {
                name: "facility_id".into(),
                kind: FieldKind::Int,
                optional: false,
            }],
        });
        assert_eq!(
            ir.lookup_compatible("INVENTORY_OPTIMIZATION", "1.0")
                .unwrap()
                .version,
            "1.2",
            "the newest compatible minor wins"
        );
        assert!(ir
            .lookup_compatible("INVENTORY_OPTIMIZATION", "2.0")
            .is_none());
    }

    #[test]
    fn malformed_idl_reports_the_line() {
        let cases = [
            (
                "intent X 1.0 {\n}",
                "expected 'intent <NAME> version <V> {'",
            ),
            ("intent lower version 1.0 {\n}", "SCREAMING_SNAKE_CASE"),
            (
                "intent X version 1.0 {\n  a: blob\n}",
                "unknown type 'blob'",
            ),
            ("intent X version 1.0 {\n  a: int\n", "never closed"),
            (
                "intent X version 1.0 {\n  a int\n}",
                "expected '<field>: <type>'",
            ),
            (
                "intent X version 1.0 {\n  a: int\n  a: string\n}",
                "duplicate field",
            ),
            ("", "no intent declared"),
        ];
        for (text, needle) in cases {
            let err = parse(text).unwrap_err().to_string();
            assert!(err.contains(needle), "{text:?} -> {err}");
        }
    }
}
