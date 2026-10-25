//! The typed envelope — GAIP's "zero-drift" wire frame.
//!
//! A participant deserializes this structure and nothing else. It never
//! sees a LangGraph state dict, a Spring DTO or a Rust struct: the
//! envelope is the only shared contract, and `payload` is the typed body
//! the schema pins down.

use crate::error::GaipError;
use crate::urn::AgentUrn;
use crate::PROTOCOL_VERSION;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// The six universal agent wire primitives. GIOP had eight message types
/// and no invented verbs; this is the same discipline for agents.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum MessageType {
    /// Delegate a job to a remote agent.
    TaskRequest,
    /// Final or incremental result, artifacts, or an error.
    TaskReply,
    /// Live tokens, logs, or progress telemetry for an in-flight task.
    TaskStream,
    /// Abort a running execution or tool call.
    CancelRequest,
    /// Introspect the remote agent's capabilities.
    CapabilityQuery,
    /// Pause and route the payload to a human for audit.
    HumanIntervent,
}

impl MessageType {
    /// The complete vocabulary. Anything else is refused — that is the
    /// point of importing GIOP's discipline instead of inventing
    /// `on_tool_start` #47.
    pub const ALL: &'static [&'static str] = &[
        "TASK_REQUEST",
        "TASK_REPLY",
        "TASK_STREAM",
        "CANCEL_REQUEST",
        "CAPABILITY_QUERY",
        "HUMAN_INTERVENT",
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::TaskRequest => "TASK_REQUEST",
            Self::TaskReply => "TASK_REPLY",
            Self::TaskStream => "TASK_STREAM",
            Self::CancelRequest => "CANCEL_REQUEST",
            Self::CapabilityQuery => "CAPABILITY_QUERY",
            Self::HumanIntervent => "HUMAN_INTERVENT",
        }
    }

    pub fn parse(s: &str) -> Result<Self, GaipError> {
        match s {
            "TASK_REQUEST" => Ok(Self::TaskRequest),
            "TASK_REPLY" => Ok(Self::TaskReply),
            "TASK_STREAM" => Ok(Self::TaskStream),
            "CANCEL_REQUEST" => Ok(Self::CancelRequest),
            "CAPABILITY_QUERY" => Ok(Self::CapabilityQuery),
            "HUMAN_INTERVENT" => Ok(Self::HumanIntervent),
            other => Err(GaipError::UnknownPrimitive {
                got: other.to_string(),
                allowed: Self::ALL,
            }),
        }
    }

    /// Does this primitive carry an in-flight task (and therefore need a
    /// live correlation)?
    pub fn requires_open_task(self) -> bool {
        matches!(
            self,
            Self::TaskStream | Self::TaskReply | Self::CancelRequest | Self::HumanIntervent
        )
    }
}

/// One GAIP message. Field names are the wire contract; do not rename
/// them without bumping `PROTOCOL_VERSION`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Envelope {
    pub protocol_version: String,
    pub message_id: String,
    pub correlation_id: String,
    pub sender: String,
    pub recipient: String,
    pub message_type: MessageType,
    pub payload: Value,
}

impl Envelope {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        message_id: impl Into<String>,
        correlation_id: impl Into<String>,
        sender: impl Into<String>,
        recipient: impl Into<String>,
        message_type: MessageType,
        payload: Value,
    ) -> Self {
        Self {
            protocol_version: PROTOCOL_VERSION.to_string(),
            message_id: message_id.into(),
            correlation_id: correlation_id.into(),
            sender: sender.into(),
            recipient: recipient.into(),
            message_type,
            payload,
        }
    }

    /// Convenience constructor for the most common message.
    pub fn task_request(
        message_id: impl Into<String>,
        task_id: impl Into<String>,
        sender: impl Into<String>,
        recipient: impl Into<String>,
        payload: Value,
    ) -> Self {
        Self::new(
            message_id,
            task_id,
            sender,
            recipient,
            MessageType::TaskRequest,
            payload,
        )
    }

    /// The counterpart of a request is bound to the SAME correlation id,
    /// which is what makes a reply routable without any session state.
    pub fn reply_to(&self, message_id: impl Into<String>, payload: Value) -> Self {
        Self::new(
            message_id,
            self.correlation_id.clone(),
            self.recipient.clone(),
            self.sender.clone(),
            MessageType::TaskReply,
            payload,
        )
    }

    pub fn stream_to(&self, message_id: impl Into<String>, payload: Value) -> Self {
        Self::new(
            message_id,
            self.correlation_id.clone(),
            self.recipient.clone(),
            self.sender.clone(),
            MessageType::TaskStream,
            payload,
        )
    }

    /// Full structural validation, in the order a human would ask:
    /// version, identity, addresses, payload shape.
    pub fn validate(&self) -> Result<(), GaipError> {
        check_version(&self.protocol_version)?;
        for (field, value) in [
            ("message_id", &self.message_id),
            ("correlation_id", &self.correlation_id),
        ] {
            if value.trim().is_empty() {
                return Err(GaipError::BadField {
                    field,
                    why: "must not be empty".to_string(),
                });
            }
            if value.len() > 128 {
                return Err(GaipError::BadField {
                    field,
                    why: "must be at most 128 characters".to_string(),
                });
            }
        }
        // name the FIELD when an address is bad: "invalid agent URN" alone
        // does not tell an operator which end of the message is wrong
        for (field, value) in [("sender", &self.sender), ("recipient", &self.recipient)] {
            if let Err(e) = value.parse::<AgentUrn>() {
                return Err(GaipError::BadField {
                    field,
                    why: e.to_string(),
                });
            }
        }
        if self.sender == self.recipient {
            return Err(GaipError::BadField {
                field: "recipient",
                why: "sender and recipient are the same agent".to_string(),
            });
        }
        if !self.payload.is_object() {
            return Err(GaipError::PayloadNotObject);
        }
        Ok(())
    }

    /// Parse + validate in one step. This is the only entrance a
    /// transport should use: unvalidated envelopes must not exist.
    pub fn from_json(json: &str) -> Result<Self, GaipError> {
        let env = Self::from_value(&serde_json::from_str(json)?)?;
        Ok(env)
    }

    /// Deserialize from a `Value` with a precise error for an unknown
    /// primitive (serde's own message would be a generic "unknown
    /// variant", which hides the word the peer actually sent).
    pub fn from_value(v: &Value) -> Result<Self, GaipError> {
        let obj = v.as_object().ok_or_else(|| GaipError::BadField {
            field: "envelope",
            why: "must be a JSON object".to_string(),
        })?;
        let need = |name: &'static str| -> Result<String, GaipError> {
            obj.get(name)
                .and_then(Value::as_str)
                .map(str::to_string)
                .ok_or(GaipError::MissingField(name))
        };
        let protocol_version = need("protocol_version")?;
        let message_id = need("message_id")?;
        let correlation_id = need("correlation_id")?;
        let sender = need("sender")?;
        let recipient = need("recipient")?;
        let raw_type = need("message_type")?;
        let message_type = MessageType::parse(&raw_type)?;
        let payload = obj
            .get("payload")
            .cloned()
            .ok_or(GaipError::MissingField("payload"))?;

        let env = Self {
            protocol_version,
            message_id,
            correlation_id,
            sender,
            recipient,
            message_type,
            payload,
        };
        env.validate()?;
        Ok(env)
    }

    pub fn to_json(&self) -> String {
        // serde_json on a struct with owned Strings cannot fail
        serde_json::to_string(self).unwrap_or_else(|_| "{}".to_string())
    }

    /// Pretty form, for logs and for humans reading a capture.
    pub fn to_json_pretty(&self) -> String {
        serde_json::to_string_pretty(self).unwrap_or_else(|_| "{}".to_string())
    }
}

/// Accept `1.x` for the current major, refuse anything else. Minor
/// versions are additive by contract; a major bump means the envelope
/// layout changed and a peer that guesses will corrupt a task.
pub fn check_version(got: &str) -> Result<(), GaipError> {
    let bad = || GaipError::VersionMismatch {
        got: got.to_string(),
        expected: PROTOCOL_VERSION,
    };
    let mut parts = got.split('.');
    let major: u32 = parts.next().ok_or_else(bad)?.parse().map_err(|_| bad())?;
    let expected_major: u32 = PROTOCOL_VERSION
        .split('.')
        .next()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1);
    if major != expected_major {
        return Err(bad());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn sample() -> Envelope {
        Envelope::task_request(
            "msg_987654321",
            "task_123456789",
            "urn:agent:python:langgraph:planner-v2",
            "urn:agent:rust:tokio:analytics-engine-v1",
            json!({"intent": "INVENTORY_OPTIMIZATION",
                   "parameters": {"facility_id": 402, "horizon_days": 30},
                   "artifacts_ref": ["s3://enterprise-data/warehouse_402.parquet"]}),
        )
    }

    #[test]
    fn the_spec_example_roundtrips_byte_for_byte() {
        let env = sample();
        let json = env.to_json();
        let back = Envelope::from_json(&json).unwrap();
        assert_eq!(back, env);
        // and the wire uses the documented field names
        for key in [
            "protocol_version",
            "message_id",
            "correlation_id",
            "sender",
            "recipient",
            "message_type",
            "payload",
        ] {
            assert!(
                json.contains(&format!("\"{key}\"")),
                "missing {key} in {json}"
            );
        }
    }

    #[test]
    fn the_vocabulary_is_exactly_six_primitives() {
        assert_eq!(MessageType::ALL.len(), 6);
        for name in MessageType::ALL {
            assert!(MessageType::parse(name).is_ok(), "{name} must parse");
        }
        assert!(!MessageType::ALL.contains(&"AGENT_STEP"));
    }

    #[test]
    fn framework_specific_event_names_are_refused() {
        for invented in ["AGENT_STEP", "on_tool_start", "RUN_COMPLETION", ""] {
            match MessageType::parse(invented) {
                Err(GaipError::UnknownPrimitive { got, allowed }) => {
                    assert_eq!(got, invented);
                    assert_eq!(allowed.len(), 6);
                }
                other => panic!("'{invented}' must be refused, got {other:?}"),
            }
        }
    }

    #[test]
    fn unknown_primitive_error_names_the_word_the_peer_sent() {
        let mut v: Value = serde_json::from_str(&sample().to_json()).unwrap();
        v["message_type"] = json!("AGENT_STEP");
        let err = Envelope::from_value(&v).unwrap_err();
        let text = err.to_string();
        assert!(text.contains("AGENT_STEP"), "{text}");
        assert!(
            text.contains("TASK_REQUEST"),
            "the error lists the vocabulary: {text}"
        );
    }

    #[test]
    fn version_major_is_enforced_but_minor_is_additive() {
        let mut env = sample();
        env.protocol_version = "1.7".into();
        assert!(env.validate().is_ok(), "minor versions are compatible");
        env.protocol_version = "2.0".into();
        let err = env.validate().unwrap_err();
        assert!(matches!(err, GaipError::VersionMismatch { .. }), "{err}");
        env.protocol_version = "one".into();
        assert!(env.validate().is_err());
    }

    #[test]
    fn validation_rejects_each_broken_field_with_its_name() {
        type Mutator = Box<dyn Fn(&mut Envelope)>;
        let cases: Vec<(&str, Mutator)> = vec![
            (
                "correlation_id",
                Box::new(|e: &mut Envelope| e.correlation_id = "".into()),
            ),
            (
                "sender",
                Box::new(|e: &mut Envelope| e.sender = "planner".into()),
            ),
            (
                "recipient",
                Box::new(|e: &mut Envelope| e.recipient = "urn:agent:x".into()),
            ),
            (
                "payload",
                Box::new(|e: &mut Envelope| e.payload = json!("not an object")),
            ),
        ];
        for (field, mutate) in cases {
            let mut env = sample();
            mutate(&mut env);
            let err = env.validate().unwrap_err();
            let text = err.to_string();
            match field {
                "payload" => assert!(
                    matches!(err, GaipError::PayloadNotObject),
                    "payload case: {text}"
                ),
                _ => assert!(text.contains(field), "'{field}' not named in '{text}'"),
            }
        }
    }

    #[test]
    fn reply_and_stream_inherit_the_correlations() {
        let req = sample();
        let reply = req.reply_to("msg_2", json!({"status": "OK"}));
        assert_eq!(reply.correlation_id, req.correlation_id);
        assert_eq!(
            reply.sender, req.recipient,
            "the reply goes back to the requester"
        );
        assert_eq!(reply.recipient, req.sender);
        assert_eq!(reply.message_type, MessageType::TaskReply);

        let stream = req.stream_to("msg_3", json!({"progress": 25}));
        assert_eq!(stream.correlation_id, req.correlation_id);
        assert_eq!(stream.message_type, MessageType::TaskStream);
    }

    #[test]
    fn a_task_cannot_address_itself() {
        let mut env = sample();
        env.recipient = env.sender.clone();
        let err = env.validate().unwrap_err();
        assert!(err.to_string().contains("recipient"), "{err}");
    }

    #[test]
    fn unknown_message_type_in_json_is_rejected_by_deserialize() {
        let text = sample().to_json().replace("TASK_REQUEST", "AGENT_STEP");
        assert!(serde_json::from_str::<Envelope>(&text).is_err());
        // the GAIP path gives the better error
        let err = Envelope::from_json(&text).unwrap_err();
        assert!(matches!(err, GaipError::UnknownPrimitive { .. }));
    }
}
