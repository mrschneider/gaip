//! Framing: how an envelope becomes bytes on a stream binding.
//!
//! GIOP's own lesson: the message format and the transport framing are
//! separate concerns. GAIP's frame is a 4-byte big-endian length
//! followed by the UTF-8 JSON envelope. That is deliberately boring —
//! a 20-line client in any language can speak it, which is the whole
//! point of an interoperability protocol.
//!
//! Bindings that are already message-oriented (Kafka, NATS, SSE) skip
//! the length prefix and carry the JSON as the message body.

use crate::context::ServiceContext;
use crate::envelope::{check_version, Envelope, MessageType};
use crate::error::GaipError;
use crate::message::{ReplyStatus, SystemMinor, WireMessage};
use crate::urn::AgentUrn;
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;

/// 8 MiB. An agent payload above this is an artifact, and artifacts
/// travel by reference (`artifacts_ref`), not inline.
pub const MAX_FRAME: usize = 8 * 1024 * 1024;

/// Serialize `value` straight into a fresh buffer that already carries a
/// 4-byte big-endian length prefix — ONE pass, no intermediate `to_vec`
/// and no second copy. The length is patched after serialization fills
/// the body.
///
/// `reserve` is a capacity hint (an underestimate is only a reallocation,
/// never a correctness issue).
fn serialize_framed<T: serde::Serialize>(value: &T, reserve: usize) -> Result<Vec<u8>, GaipError> {
    let mut out = Vec::with_capacity(4 + reserve);
    out.extend_from_slice(&0u32.to_be_bytes()); // patched below
    serde_json::to_writer(&mut out, value)?;
    let len = out.len() - 4;
    if len > MAX_FRAME {
        return Err(GaipError::FrameTooLarge(len));
    }
    out[..4].copy_from_slice(&(len as u32).to_be_bytes());
    Ok(out)
}

/// Encode one envelope as a length-prefixed frame.
pub fn encode_frame(env: &Envelope) -> Result<Vec<u8>, GaipError> {
    env.validate()?;
    serialize_framed(env, 512)
}

/// Try to decode one frame from the front of `buf`.
///
/// * `Ok(Some((envelope, consumed)))` — a complete, validated envelope.
/// * `Ok(None)` — need more bytes.
/// * `Err(_)` — refuse with the reason (bad length, bad JSON, unknown
///   primitive, version mismatch...).
pub fn decode_frame(buf: &[u8]) -> Result<Option<(Envelope, usize)>, GaipError> {
    if buf.len() < 4 {
        return Ok(None);
    }
    let len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
    if len == 0 {
        return Err(GaipError::BadField {
            field: "frame",
            why: "zero-length frame".to_string(),
        });
    }
    if len > MAX_FRAME {
        return Err(GaipError::FrameTooLarge(len));
    }
    if buf.len() < 4 + len {
        return Ok(None);
    }
    let body = &buf[4..4 + len];
    let env = Envelope::from_value_owned(serde_json::from_slice(body)?)?;
    Ok(Some((env, 4 + len)))
}

/// Encode a wire message as a length-prefixed frame. One pass: the JSON
/// is written directly into the framed buffer instead of an intermediate
/// `to_vec` that `encode_body` would copy again.
pub fn encode_wire(msg: &WireMessage) -> Result<Vec<u8>, GaipError> {
    serialize_framed(msg, 512)
}

/// Wrap an already-serialized body in a frame. Callers that measured the
/// message from their own serialization hand that body here instead of
/// paying for a second `serde_json::to_vec`.
pub fn encode_body(body: Vec<u8>) -> Result<Vec<u8>, GaipError> {
    if body.len() > MAX_FRAME {
        return Err(GaipError::FrameTooLarge(body.len()));
    }
    let mut out = Vec::with_capacity(4 + body.len());
    out.extend_from_slice(&(body.len() as u32).to_be_bytes());
    out.extend_from_slice(&body);
    Ok(out)
}

/// Decode one wire message. An unparseable frame yields a
/// `MessageError`-worthy [`GaipError`]; the caller decides whether to
/// answer with `WireMessage::MessageError` (GIOP does exactly that
/// instead of dropping the connection).
pub fn decode_wire(buf: &[u8]) -> Result<Option<(WireMessage, usize)>, GaipError> {
    if buf.len() < 4 {
        return Ok(None);
    }
    let len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
    if len == 0 {
        return Err(GaipError::BadField {
            field: "frame",
            why: "zero-length frame".to_string(),
        });
    }
    if len > MAX_FRAME {
        return Err(GaipError::FrameTooLarge(len));
    }
    if buf.len() < 4 + len {
        return Ok(None);
    }
    let msg = serde_json::from_slice::<WireMessage>(&buf[4..4 + len])?;
    // validate the embedded envelope in place — no re-encode/re-parse
    msg.validate_envelopes()?;
    Ok(Some((msg, 4 + len)))
}

// ── routing view: relay a REQUEST without parsing its payload ──────────
//
// A relay decides on `wire`, `request_id`, `contexts`, and five envelope
// fields (version, ids, addresses). It then forwards the message with the
// ROUTE context appended — and today it pays for the whole thing twice:
// the payload is parsed into a `Value` tree and then re-serialized on the
// way out. On this wire that is most of what a relayed message costs.
//
// [`decode_wire_view`] keeps the envelope's `payload` as [`RawValue`]: the
// bytes are validated (shape, not content) and carried through verbatim,
// so the relay never builds the tree. [`encode_request_view`] splices
// those exact bytes back into a new frame.

/// The routing half of a `REQUEST`: the fields a relay reads, with the
/// envelope body still in wire form.
#[derive(Debug)]
pub struct RoutedRequest<'a> {
    pub request_id: u32,
    pub contexts: Vec<ServiceContext>,
    /// The whole envelope object, byte-for-byte as it arrived: the relay
    /// forwards these bytes, it never rebuilds the envelope.
    pub envelope: &'a RawValue,
    /// Validated envelope fields: the relay routes on them, the forward
    /// does not need to look at them again.
    pub sender: String,
    pub recipient: String,
    pub message_id: String,
}

/// A decoded frame: a `REQUEST` a relay can forward without parsing the
/// payload, or anything else (already a full [`WireMessage`]).
#[derive(Debug)]
pub enum WireFrame<'a> {
    Request(RoutedRequest<'a>),
    Other(WireMessage),
}

/// The borrowed envelope: the same fields as [`Envelope`], plus a raw
/// payload.
///
/// `validate` runs before the caller sees it, so a frame accepted here is
/// exactly the frame [`decode_wire`] would have accepted — the difference
/// is that the payload was checked for shape and never turned into a tree.
#[derive(Deserialize)]
#[serde(bound(deserialize = "'de: 'a"))]
struct RoutedEnvelope<'a> {
    protocol_version: String,
    message_id: String,
    correlation_id: String,
    sender: String,
    recipient: String,
    /// Parsed so an unknown primitive is refused here, with the word the
    /// peer sent — the relay never routes on it, which is why it is `_`.
    #[serde(rename = "message_type")]
    _message_type: MessageType,
    #[serde(borrow)]
    payload: &'a RawValue,
}

impl RoutedEnvelope<'_> {
    /// The checks [`Envelope::validate`] performs, on borrowed fields.
    fn validate(&self) -> Result<(), GaipError> {
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
        // `RawValue` is guaranteed-valid JSON, so the first meaningful byte
        // decides the type: an object starts with '{'.
        if !self.payload.get().trim_start().starts_with('{') {
            return Err(GaipError::PayloadNotObject);
        }
        Ok(())
    }
}

/// The outer frame, read in ONE pass with the body captured as a raw
/// span. A tagged *enum* cannot do this: `#[serde(tag = ...)]` buffers the
/// whole map into `Content`, which destroys every borrow (a `&RawValue`
/// inside it fails with "invalid type: newtype struct"). A plain struct
/// keeps the deserializer in streaming mode, so `body` stays a slice of
/// the input and is only entered if the tag says REQUEST.
#[derive(Deserialize)]
#[serde(bound(deserialize = "'de: 'a"))]
struct FrameView<'a> {
    /// Read as owned: a hostile tag with escapes must be reported, not
    /// silently reinterpreted.
    wire: String,
    #[serde(default)]
    request_id: u32,
    #[serde(default)]
    contexts: Vec<ServiceContext>,
    #[serde(default)]
    body: Option<&'a RawValue>,
}

/// Decode a frame for a relay: a `REQUEST` comes back as a
/// [`RoutedRequest`] with its payload untouched; anything else is handed
/// to [`decode_wire`], so there is exactly one implementation of the other
/// arms. The pass done here for those frames allocates nothing for the
/// body — it is captured as a span and never entered.
pub fn decode_wire_view(buf: &[u8]) -> Result<Option<(WireFrame<'_>, usize)>, GaipError> {
    if buf.len() < 4 {
        return Ok(None);
    }
    let len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
    if len == 0 {
        return Err(GaipError::BadField {
            field: "frame",
            why: "zero-length frame".to_string(),
        });
    }
    if len > MAX_FRAME {
        return Err(GaipError::FrameTooLarge(len));
    }
    if buf.len() < 4 + len {
        return Ok(None);
    }
    let body = &buf[4..4 + len];
    let view: FrameView = serde_json::from_slice(body)?;
    if view.wire != "REQUEST" {
        let (msg, used) = decode_wire(buf)?.expect("frame length already checked");
        return Ok(Some((WireFrame::Other(msg), used)));
    }
    let raw = view.body.ok_or(GaipError::BadField {
        field: "body",
        why: "a REQUEST must carry an envelope".to_string(),
    })?;
    let env: RoutedEnvelope = serde_json::from_str(raw.get())?;
    env.validate()?;
    Ok(Some((
        WireFrame::Request(RoutedRequest {
            request_id: view.request_id,
            contexts: view.contexts,
            envelope: raw,
            sender: env.sender,
            recipient: env.recipient,
            message_id: env.message_id,
        }),
        4 + len,
    )))
}

/// Re-frame a `REQUEST` around a body it already had: new contexts, the
/// envelope bytes untouched. No payload parse, no re-serialization.
pub fn encode_request_view(
    request_id: u32,
    contexts: &[ServiceContext],
    env_body: &RawValue,
) -> Result<Vec<u8>, GaipError> {
    // The envelope goes back in as `RawValue`, so this writes only the
    // frame's own fields plus the bytes it was handed.
    #[derive(Serialize)]
    #[serde(tag = "wire", rename_all = "SCREAMING_SNAKE_CASE")]
    enum Out<'a> {
        Request {
            request_id: u32,
            // `WireMessage::Request` always emits `contexts` (no skip), so
            // the re-framed bytes match the original exactly.
            contexts: &'a [ServiceContext],
            body: &'a RawValue,
        },
    }
    serialize_framed(
        &Out::Request {
            request_id,
            contexts,
            body: env_body,
        },
        512 + env_body.get().len(),
    )
}

/// A refusal expressed as a wire message (GIOP's MessageError) rather
/// than an ad-hoc object outside the contract.
pub fn message_error(reason: impl Into<String>) -> WireMessage {
    WireMessage::MessageError {
        reason: reason.into(),
    }
}

/// A system exception reply for a failed request.
pub fn system_reply(
    request_id: u32,
    minor: SystemMinor,
    message: impl Into<String>,
) -> WireMessage {
    WireMessage::reply_with(request_id, ReplyStatus::system(minor, message))
}

/// Parse a bare JSON envelope (message-oriented bindings: Kafka/NATS/SSE).
pub fn decode_json(text: &str) -> Result<Envelope, GaipError> {
    Envelope::from_json(text)
}

/// A version probe that does not require a full envelope: useful for a
/// gateway that must answer `PROTOCOL_VERSION` before parsing payloads.
pub fn probe_version(text: &str) -> Option<String> {
    let v: serde_json::Value = serde_json::from_str(text).ok()?;
    v.get("protocol_version")?.as_str().map(str::to_string)
}

/// Is this peer speaking a version we can serve?
pub fn version_compatible(text: &str) -> Result<(), GaipError> {
    match probe_version(text) {
        Some(v) => check_version(&v),
        None => Err(GaipError::MissingField("protocol_version")),
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
    fn frame_has_a_big_endian_length_prefix() {
        let bytes = encode_frame(&env()).unwrap();
        let len = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;
        assert_eq!(len, bytes.len() - 4);
        assert_eq!(&bytes[4..5], b"{", "the body is the JSON envelope");
    }

    #[test]
    fn roundtrip_and_consumed_length() {
        let e = env();
        let bytes = encode_frame(&e).unwrap();
        let (back, used) = decode_frame(&bytes).unwrap().unwrap();
        assert_eq!(back, e);
        assert_eq!(used, bytes.len());
    }

    #[test]
    fn partial_buffers_ask_for_more_instead_of_failing() {
        let bytes = encode_frame(&env()).unwrap();
        for cut in 0..bytes.len() {
            assert!(
                matches!(decode_frame(&bytes[..cut]), Ok(None)),
                "cut at {cut} must be incomplete"
            );
        }
    }

    #[test]
    fn two_frames_back_to_back() {
        let a = encode_frame(&env()).unwrap();
        let mut b_env = env();
        b_env.message_id = "msg_2".into();
        let b = encode_frame(&b_env).unwrap();
        let mut wire = a.clone();
        wire.extend_from_slice(&b);
        let (first, n1) = decode_frame(&wire).unwrap().unwrap();
        let (second, n2) = decode_frame(&wire[n1..]).unwrap().unwrap();
        assert_eq!(first.message_id, "msg_1");
        assert_eq!(second.message_id, "msg_2");
        assert_eq!(n1 + n2, wire.len());
    }

    #[test]
    fn hostile_length_and_zero_length_are_refused() {
        let huge = (MAX_FRAME as u32 + 1).to_be_bytes();
        assert!(matches!(
            decode_frame(&huge).unwrap_err(),
            GaipError::FrameTooLarge(_)
        ));
        let zero = 0u32.to_be_bytes();
        assert!(
            decode_frame(&zero).is_err(),
            "a zero-length frame is a bug, not silence"
        );
    }

    #[test]
    fn version_probe_works_without_a_full_envelope() {
        let text = env().to_json();
        assert_eq!(probe_version(&text).as_deref(), Some("1.0"));
        assert!(version_compatible(&text).is_ok());
        let v2 = text.replace("\"1.0\"", "\"2.0\"");
        assert!(version_compatible(&v2).is_err());
        assert!(version_compatible("not json").is_err());
    }

    // ── the routing view must be behaviourally identical to decode_wire ──

    fn request_frame() -> Vec<u8> {
        encode_wire(&WireMessage::Request {
            request_id: 7,
            contexts: vec![],
            body: env(),
        })
        .unwrap()
    }

    #[test]
    fn view_roundtrip_is_byte_identical_when_contexts_are_unchanged() {
        let frame = request_frame();
        let (view, used) = decode_wire_view(&frame).unwrap().unwrap();
        assert_eq!(used, frame.len());
        let WireFrame::Request(req) = view else {
            panic!("a REQUEST must take the routing path");
        };
        let reframed = encode_request_view(req.request_id, &req.contexts, req.envelope).unwrap();
        assert_eq!(
            reframed, frame,
            "re-framing without touching the contexts must reproduce the bytes"
        );
    }

    #[test]
    fn view_forward_carries_the_payload_and_the_new_contexts() {
        let frame = request_frame();
        let (view, _) = decode_wire_view(&frame).unwrap().unwrap();
        let WireFrame::Request(req) = view else {
            panic!("expected the routing path");
        };
        assert_eq!(req.sender, "urn:agent:python:langgraph:planner-v2");
        assert_eq!(req.recipient, "urn:agent:rust:tokio:engine");
        assert_eq!(req.message_id, "msg_1");

        let hop = ServiceContext::new(crate::context::id::ROUTE, "urn:agent:gaip:broker:b1");
        let forwarded = encode_request_view(7, &[hop], req.envelope).unwrap();
        let (back, _) = decode_wire(&forwarded).unwrap().unwrap();
        match back {
            WireMessage::Request {
                request_id,
                contexts,
                body,
            } => {
                assert_eq!(request_id, 7);
                assert_eq!(contexts.len(), 1);
                assert_eq!(body.message_id, "msg_1");
                assert_eq!(
                    body.payload,
                    json!({"intent": "INVENTORY_OPTIMIZATION"}),
                    "the payload survived the passthrough unchanged"
                );
            }
            other => panic!("expected a REQUEST, got {other:?}"),
        }
    }

    #[test]
    fn view_falls_back_to_the_full_parse_for_anything_that_is_not_a_request() {
        let reply = WireMessage::Reply {
            request_id: 9,
            status: ReplyStatus::NoException,
            body: Some(env()),
            contexts: vec![],
        };
        let frame = encode_wire(&reply).unwrap();
        match decode_wire_view(&frame).unwrap().unwrap().0 {
            WireFrame::Other(msg) => assert_eq!(msg, reply),
            WireFrame::Request(_) => panic!("a REPLY is not a routable request"),
        }
    }

    #[test]
    fn view_refuses_exactly_what_the_full_decode_refuses() {
        let good = request_frame();
        let text = String::from_utf8(good[4..].to_vec()).unwrap();
        let cases = [
            ("version", text.replace("\"1.0\"", "\"2.0\"")),
            (
                "sender",
                text.replace("urn:agent:python:langgraph:planner-v2", "not-a-urn"),
            ),
            (
                "payload",
                text.replace(r#"{"intent":"INVENTORY_OPTIMIZATION"}"#, "[1,2,3]"),
            ),
            ("primitive", text.replace("TASK_REQUEST", "AGENT_STEP")),
            ("empty id", text.replace("\"msg_1\"", "\"\"")),
        ];
        for (what, body) in cases {
            let mut frame = (body.len() as u32).to_be_bytes().to_vec();
            frame.extend_from_slice(body.as_bytes());
            assert!(
                decode_wire(&frame).is_err(),
                "full decode must refuse a bad {what}"
            );
            assert!(
                decode_wire_view(&frame).is_err(),
                "the routing view must refuse the same bad {what}"
            );
        }
    }

    #[test]
    fn view_rejects_a_payload_that_is_not_an_object() {
        let frame = request_frame();
        let text = String::from_utf8(frame[4..].to_vec()).unwrap();
        let body = text.replace(r#"{"intent":"INVENTORY_OPTIMIZATION"}"#, "42");
        let mut framed = (body.len() as u32).to_be_bytes().to_vec();
        framed.extend_from_slice(body.as_bytes());
        assert!(matches!(
            decode_wire_view(&framed).unwrap_err(),
            GaipError::PayloadNotObject
        ));
    }
}
