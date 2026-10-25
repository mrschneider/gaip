//! Refusals, not repairs.
//!
//! Every rejection carries the offending value and the rule it broke. A
//! protocol that silently coerces a bad message is a protocol whose
//! failures show up three services later as "the task never ran".

use crate::envelope::MessageType;
use crate::lifecycle::TaskState;
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GAIPError {
    /// A verb outside the six universal primitives. This is the protocol's
    /// whole point: framework-specific event names do not travel.
    UnknownPrimitive {
        got: String,
        allowed: &'static [&'static str],
    },
    /// Major version mismatch — the envelope layout may have changed.
    VersionMismatch {
        got: String,
        expected: &'static str,
    },
    MissingField(&'static str),
    BadField {
        field: &'static str,
        why: String,
    },
    BadUrn {
        value: String,
        why: String,
    },
    PayloadNotObject,
    /// A TASK_REQUEST reused a correlation_id that is still open.
    TaskExists(String),
    NoSuchTask(String),
    /// The task lifecycle does not allow this primitive right now.
    IllegalTransition {
        task: String,
        from: TaskState,
        primitive: MessageType,
    },
    FrameTooLarge(usize),
    Truncated,
    Json(String),
}

impl fmt::Display for GAIPError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownPrimitive { got, allowed } => write!(
                f,
                "unknown primitive '{got}' — the vocabulary is closed: {}",
                allowed.join(", ")
            ),
            Self::VersionMismatch { got, expected } => {
                write!(
                    f,
                    "protocol_version '{got}' is incompatible with '{expected}'"
                )
            }
            Self::MissingField(name) => write!(f, "missing required field '{name}'"),
            Self::BadField { field, why } => write!(f, "field '{field}': {why}"),
            Self::BadUrn { value, why } => write!(f, "invalid agent URN '{value}': {why}"),
            Self::PayloadNotObject => write!(f, "payload must be a JSON object"),
            Self::TaskExists(id) => write!(f, "task '{id}' is already open"),
            Self::NoSuchTask(id) => write!(f, "no such task '{id}'"),
            Self::IllegalTransition {
                task,
                from,
                primitive,
            } => write!(
                f,
                "task '{task}' is in state {from:?}; {primitive:?} is not legal there"
            ),
            Self::FrameTooLarge(n) => write!(f, "frame of {n} bytes exceeds the cap"),
            Self::Truncated => write!(f, "truncated frame"),
            Self::Json(e) => write!(f, "malformed JSON: {e}"),
        }
    }
}

impl std::error::Error for GAIPError {}

impl From<serde_json::Error> for GAIPError {
    fn from(e: serde_json::Error) -> Self {
        Self::Json(e.to_string())
    }
}
