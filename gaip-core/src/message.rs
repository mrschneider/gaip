//! GIOP's message layer, applied to agent traffic.
//!
//! GIOP separates two things that agent frameworks fuse together: the
//! **transport message** (Request, Reply, LocateRequest, LocateReply,
//! CancelRequest, CloseConnection, MessageError, Fragment) and the
//! **operation** carried inside it. GAIP's six primitives are operations:
//! they live in `Request.body` / `Reply.body`, and the surrounding
//! message is what a transport can route, fragment and fail.
//!
//! That separation is what makes the rest of this crate possible:
//!
//! * [`Fragment`](WireMessage::Fragment) splits a message too large for
//!   one frame, with GIOP 1.1's continuation header.
//! * [`LocateRequest`](WireMessage::LocateRequest) answers "is it there?"
//!   cheaply and can *forward* the caller to a relocated agent.
//! * [`ReplyStatus`] turns failure into a typed reply: a user error the
//!   peer must handle, or a system error with a retryability flag.
//! * [`MessageError`](WireMessage::MessageError) is a refusal expressed in
//!   the protocol itself, instead of an ad-hoc `{"error": ...}` object
//!   outside the contract.

use crate::context::ServiceContext;
use crate::envelope::Envelope;
use crate::error::GaipError;
use crate::ior::Ior;
use serde::{Deserialize, Serialize};

/// GIOP-style minor codes for system exceptions. The retryability flag is
/// the part that matters operationally: an orchestrator that cannot tell
/// "try again" from "this will never work" either retries a poison
/// message forever or gives up on a transient blip.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SystemMinor {
    BadParam,
    NoPermission,
    Timeout,
    Unavailable,
    ProtocolError,
    ObjectNotExist,
}

impl SystemMinor {
    pub fn code(self) -> u16 {
        match self {
            Self::BadParam => 0x0001,
            Self::NoPermission => 0x0002,
            Self::Timeout => 0x0003,
            Self::Unavailable => 0x0004,
            Self::ProtocolError => 0x0005,
            Self::ObjectNotExist => 0x0006,
        }
    }

    /// Transient conditions are worth a retry; everything else is a bug,
    /// a permission problem, or a dead object.
    pub fn retryable(self) -> bool {
        matches!(self, Self::Timeout | Self::Unavailable)
    }
}

/// The outcome of a Request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ReplyStatus {
    /// Normal completion; `body` carries the reply envelope.
    NoException,
    /// The operation ran and refused on business grounds (user error).
    /// Not retryable: the same input will fail the same way.
    UserException { code: String, message: String },
    /// Infrastructure failed. `minor` says whether a retry can help.
    SystemException { minor: SystemMinor, message: String },
}

impl ReplyStatus {
    pub fn is_ok(&self) -> bool {
        matches!(self, Self::NoException)
    }

    pub fn retryable(&self) -> bool {
        match self {
            Self::SystemException { minor, .. } => minor.retryable(),
            _ => false,
        }
    }

    pub fn system(minor: SystemMinor, message: impl Into<String>) -> Self {
        Self::SystemException {
            minor,
            message: message.into(),
        }
    }

    pub fn user(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self::UserException {
            code: code.into(),
            message: message.into(),
        }
    }
}

/// Answer to a `LocateRequest`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum LocateStatus {
    /// The agent is here; send the request on this connection.
    ObjectHere,
    /// The agent moved. GIOP's `OBJECT_FORWARD`: the caller follows the
    /// reference and retries once, no renegotiation involved.
    ObjectForward { forward: Ior },
    /// No such agent, or it is gone.
    UnknownObject,
}

/// One GAIP wire message. Every transport moves these; the agent
/// primitives ride inside `Request`/`Reply`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "wire", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum WireMessage {
    /// An agent primitive (TASK_REQUEST, CAPABILITY_QUERY, ...).
    Request {
        request_id: u32,
        #[serde(default)]
        contexts: Vec<ServiceContext>,
        body: Envelope,
    },
    /// The answer to a Request. Service contexts ride replies too (GIOP
    /// 1.2): the broker that delivered a request stamps the reply with
    /// DELIVERED so the origin can tell "acknowledged" from "assumed".
    Reply {
        request_id: u32,
        status: ReplyStatus,
        #[serde(default)]
        body: Option<Envelope>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        contexts: Vec<ServiceContext>,
    },
    /// Cheap liveness/relocation probe, distinct from capability
    /// introspection.
    LocateRequest { request_id: u32, target: String },
    LocateReply {
        request_id: u32,
        status: LocateStatus,
    },
    /// Transport-level abort of an in-flight request. Distinct from the
    /// CANCEL_REQUEST *primitive*, which asks the agent to stop working.
    CancelRequest { request_id: u32 },
    /// This message could not be understood — expressed in the protocol.
    MessageError { reason: String },
    /// Graceful shutdown; no reply is expected.
    CloseConnection,
    /// Push of new interface declarations (IDL text). A transport-level
    /// message, not an agent primitive: distributing the contract is not
    /// an operation of the six.
    /// A schema push. Signed: the identity context is verified by the
    /// receiver, and a push without a schema-admin identity is refused.
    SchemaSync {
        idl: String,
        #[serde(default)]
        contexts: Vec<ServiceContext>,
    },
    /// Continuation of a fragmented message (GIOP 1.1+).
    Fragment {
        request_id: u32,
        fragment_num: u32,
        more: bool,
        /// Base64-free transport: the raw UTF-8 JSON chunk of the
        /// containing message, so a fragment is inspectable in a capture.
        data: String,
    },
}

impl WireMessage {
    pub fn request(request_id: u32, body: Envelope) -> Self {
        Self::Request {
            request_id,
            contexts: Vec::new(),
            body,
        }
    }

    pub fn with_contexts(mut self, contexts: Vec<ServiceContext>) -> Self {
        if let Self::Request {
            contexts: ref mut c,
            ..
        } = self
        {
            *c = contexts;
        }
        self
    }

    pub fn reply(request_id: u32, body: Envelope) -> Self {
        Self::Reply {
            request_id,
            status: ReplyStatus::NoException,
            body: Some(body),
            contexts: Vec::new(),
        }
    }

    pub fn reply_with(request_id: u32, status: ReplyStatus) -> Self {
        Self::Reply {
            request_id,
            status,
            body: None,
            contexts: Vec::new(),
        }
    }

    pub fn request_id(&self) -> Option<u32> {
        match self {
            Self::Request { request_id, .. }
            | Self::Reply { request_id, .. }
            | Self::LocateRequest { request_id, .. }
            | Self::LocateReply { request_id, .. }
            | Self::CancelRequest { request_id }
            | Self::Fragment { request_id, .. } => Some(*request_id),
            Self::MessageError { .. } | Self::CloseConnection | Self::SchemaSync { .. } => None,
        }
    }

    pub fn to_json(&self) -> Result<String, GaipError> {
        Ok(serde_json::to_string(self)?)
    }

    pub fn from_json(text: &str) -> Result<Self, GaipError> {
        let msg: Self = serde_json::from_str(text)?;
        msg.validate_envelopes()?;
        Ok(msg)
    }

    /// The embedded envelope must still validate: the wire layer is not
    /// a way to smuggle an invalid GAIP message past the contract.
    /// Runs on an already-deserialized message so transports validate in
    /// place instead of re-encoding the frame just to re-parse it.
    pub fn validate_envelopes(&self) -> Result<(), GaipError> {
        match self {
            Self::Request { body, .. } => body.validate()?,
            Self::Reply {
                body: Some(body), ..
            } => body.validate()?,
            _ => {}
        }
        Ok(())
    }

    /// Split an over-sized message into `Fragment`s whose JSON payload is
    /// at most `max_bytes` each.
    ///
    /// The first fragment carries the beginning of the encoded message;
    /// `fragment_num` is 0-based and `more` marks continuation, exactly
    /// as GIOP 1.1 does. A message that fits yields no fragments (callers
    /// send it whole).
    pub fn fragment(self, max_bytes: usize) -> Result<Vec<WireMessage>, GaipError> {
        if matches!(self, Self::Fragment { .. }) {
            return Err(GaipError::BadField {
                field: "fragment",
                why: "a fragment cannot be fragmented again".to_string(),
            });
        }
        let max = max_bytes.max(64);
        let json = self.to_json()?;
        if json.len() <= max {
            return Ok(vec![self]);
        }
        let request_id = self.request_id().unwrap_or(0);
        let chunks: Vec<&[u8]> = json.as_bytes().chunks(max).collect();
        let total = chunks.len();
        Ok(chunks
            .into_iter()
            .enumerate()
            .map(|(i, chunk)| WireMessage::Fragment {
                request_id,
                fragment_num: i as u32,
                more: i + 1 < total,
                data: String::from_utf8_lossy(chunk).to_string(),
            })
            .collect())
    }

    /// Reassemble a complete fragment run into the original message.
    /// Refuses gaps and out-of-order runs: a message that assembled out
    /// of order is worse than one that failed.
    pub fn reassemble(fragments: &[WireMessage]) -> Result<WireMessage, GaipError> {
        if fragments.is_empty() {
            return Err(GaipError::BadField {
                field: "fragment",
                why: "empty fragment run".to_string(),
            });
        }
        let mut data = String::new();
        let mut expected = 0u32;
        let mut request_id = None;
        for f in fragments {
            match f {
                WireMessage::Fragment {
                    request_id: rid,
                    fragment_num,
                    more,
                    data: chunk,
                } => {
                    if *fragment_num != expected {
                        return Err(GaipError::BadField {
                            field: "fragment_num",
                            why: format!("expected {expected}, got {fragment_num}"),
                        });
                    }
                    if let Some(prev) = request_id {
                        if prev != *rid {
                            return Err(GaipError::BadField {
                                field: "request_id",
                                why: "fragment run mixes two requests".to_string(),
                            });
                        }
                    }
                    request_id = Some(*rid);
                    data.push_str(chunk);
                    expected += 1;
                    if !*more && expected as usize != fragments.len() {
                        return Err(GaipError::BadField {
                            field: "more",
                            why: "a fragment after the final one".to_string(),
                        });
                    }
                }
                other => {
                    return Err(GaipError::BadField {
                        field: "fragment",
                        why: format!("{other:?} is not a Fragment"),
                    })
                }
            }
        }
        Self::from_json(&data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn env() -> Envelope {
        Envelope::task_request(
            "msg_1",
            "task_1",
            "urn:agent:python:langgraph:planner-v2",
            "urn:agent:rust:tokio:engine",
            json!({"intent": "INVENTORY_OPTIMIZATION"}),
        )
    }

    #[test]
    fn request_roundtrips_through_json() {
        let msg = WireMessage::request(7, env());
        let back = WireMessage::from_json(&msg.to_json().unwrap()).unwrap();
        assert_eq!(back, msg);
    }

    #[test]
    fn an_invalid_envelope_cannot_ride_the_wire_layer() {
        let mut bad = env();
        bad.protocol_version = "9.0".into();
        let text = serde_json::to_string(&WireMessage::request(1, bad)).unwrap();
        assert!(
            WireMessage::from_json(&text).is_err(),
            "the wire must not smuggle garbage"
        );
    }

    #[test]
    fn retryability_is_the_operational_flag() {
        assert!(SystemMinor::Timeout.retryable());
        assert!(SystemMinor::Unavailable.retryable());
        assert!(!SystemMinor::BadParam.retryable());
        assert!(!SystemMinor::NoPermission.retryable());
        assert!(ReplyStatus::system(SystemMinor::Timeout, "slow").retryable());
        assert!(!ReplyStatus::user("E_BAD_INPUT", "nope").retryable());
        assert!(ReplyStatus::NoException.is_ok());
    }

    #[test]
    fn fragment_split_and_reassemble_roundtrip() {
        let payload = json!({"text": "x".repeat(4000)});
        let mut e = env();
        e.payload = payload;
        let msg = WireMessage::request(9, e);
        let whole = msg.to_json().unwrap();
        assert!(whole.len() > 1000);

        let frags = msg.clone().fragment(500).unwrap();
        assert!(
            frags.len() > 3,
            "a 4 KB message at 500 B/frag: {}",
            frags.len()
        );
        for (i, f) in frags.iter().enumerate() {
            match f {
                WireMessage::Fragment {
                    fragment_num,
                    more,
                    data,
                    request_id,
                } => {
                    assert_eq!(*fragment_num, i as u32);
                    assert_eq!(*request_id, 9);
                    assert_eq!(*more, i + 1 < frags.len());
                    assert!(data.len() <= 500);
                }
                other => panic!("expected fragment, got {other:?}"),
            }
        }
        let back = WireMessage::reassemble(&frags).unwrap();
        assert_eq!(back, msg, "reassembly must be byte-exact");
    }

    #[test]
    fn a_message_that_fits_is_sent_whole() {
        let frags = WireMessage::request(1, env()).fragment(64 * 1024).unwrap();
        assert_eq!(frags.len(), 1);
        assert!(matches!(frags[0], WireMessage::Request { .. }));
    }

    #[test]
    fn reassembly_refuses_gaps_and_illegal_runs() {
        let msg = WireMessage::request(1, env());
        let frags = msg.fragment(80).unwrap();
        assert!(frags.len() >= 3);

        let mut gapped = frags.clone();
        gapped.remove(1);
        assert!(WireMessage::reassemble(&gapped).is_err(), "a gap must fail");

        let mut reordered = frags.clone();
        reordered.swap(0, 1);
        assert!(
            WireMessage::reassemble(&reordered).is_err(),
            "order matters"
        );

        let mixed = vec![
            frags[0].clone(),
            WireMessage::Fragment {
                request_id: 999,
                fragment_num: 1,
                more: false,
                data: "{}".into(),
            },
        ];
        assert!(
            WireMessage::reassemble(&mixed).is_err(),
            "run must be one request"
        );
    }

    #[test]
    fn a_fragment_cannot_be_fragmented_again() {
        let frag = WireMessage::Fragment {
            request_id: 1,
            fragment_num: 0,
            more: true,
            data: "x".into(),
        };
        assert!(frag.fragment(10).is_err());
    }

    #[test]
    fn locate_reply_can_forward_the_caller() {
        let ior = Ior::new(
            "urn:agent:rust:tokio:engine:2.0",
            vec!["10.0.0.7:7610".to_string()],
            "1.0",
        );
        let msg = WireMessage::LocateReply {
            request_id: 3,
            status: LocateStatus::ObjectForward {
                forward: ior.clone(),
            },
        };
        let back = WireMessage::from_json(&msg.to_json().unwrap()).unwrap();
        assert_eq!(back, msg);
    }
}
