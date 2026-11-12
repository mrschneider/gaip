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
///
/// `REQUEST` takes a fast path: the tagged *enum* derive would buffer the
/// whole frame into `Content` (parsing the payload into a map and
/// re-serializing it for the `Envelope`) before dispatching. The struct
/// below keeps the deserializer streaming, so the body is captured as a
/// span and the `Envelope` is parsed straight from it — one pass, no
/// intermediate tree. Every other tag falls through to the full enum
/// (one cheap extra pass for the tag, which the rare control frames can
/// afford).
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
    let body = &buf[4..4 + len];
    let view: WireView = serde_json::from_slice(body)?;
    if view.wire == "REQUEST" {
        let request_id = view.request_id.ok_or(GaipError::BadField {
            field: "request_id",
            why: "a REQUEST must carry a request_id".to_string(),
        })?;
        let raw = view.body.ok_or(GaipError::BadField {
            field: "body",
            why: "a REQUEST must carry an envelope".to_string(),
        })?;
        // One streaming pass straight into the Envelope (derive) +
        // validate() — no intermediate Value tree, no field-by-field
        // move-out. The derive only fills fields; validation is the
        // same validate() the enum path runs. A missing `payload` is
        // refused (Json), matching from_json: the hot path no longer
        // coerces a hole into {}.
        let env = serde_json::from_str::<Envelope>(raw.get())?;
        env.validate()?;
        return Ok(Some((
            WireMessage::Request {
                request_id,
                contexts: view.contexts,
                body: env,
            },
            4 + len,
        )));
    }
    // Non-REQUEST frames are small and rare: hand them to the full enum,
    // which stays the single implementation of the other arms.
    let msg = serde_json::from_slice::<WireMessage>(body)?;
    // validate the embedded envelope in place — no re-encode/re-parse
    msg.validate_envelopes()?;
    Ok(Some((msg, 4 + len)))
}

/// The wire fields of a `REQUEST`, read in ONE streaming pass with the
/// envelope body captured as a raw span.
///
/// A tagged *enum* cannot do this: `#[serde(tag = ...)]` buffers the
/// whole map into `Content`, which both destroys every borrow and pays
/// for the payload tree twice. A plain struct keeps the deserializer in
/// streaming mode, so `body` stays a slice of the input and is only
/// entered when the tag says REQUEST.
#[derive(Deserialize)]
#[serde(bound(deserialize = "'de: 'a"))]
struct WireView<'a> {
    /// Read as owned: a hostile tag with escapes must be reported, not
    /// silently reinterpreted.
    wire: String,
    /// `Option` (not `#[serde(default)]`): a REQUEST without a
    /// `request_id` must fail exactly as the tagged-enum path does.
    #[serde(default)]
    request_id: Option<u32>,
    #[serde(default)]
    contexts: Vec<ServiceContext>,
    #[serde(default)]
    body: Option<&'a RawValue>,
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

/// The routing half of a `REPLY`: the fields a relay reads, with the body
/// (when present) still in wire form.
#[derive(Debug)]
pub struct RoutedReply<'a> {
    pub request_id: u32,
    pub status: ReplyStatus,
    /// The whole body envelope, byte-for-byte, `None` for a bodyless
    /// (exception) reply.
    pub body: Option<&'a RawValue>,
    pub contexts: Vec<ServiceContext>,
    /// The reply's destination, read from the body: a `None` body (a
    /// system exception) has no envelope and is unroutable.
    pub recipient: Option<String>,
}

/// A decoded frame: a `REQUEST` a relay can forward without parsing the
/// payload, a `REPLY` it can route and forward without re-serializing
/// the body, or anything else (already a full [`WireMessage`]).
#[derive(Debug)]
pub enum WireFrame<'a> {
    Request(RoutedRequest<'a>),
    Reply(RoutedReply<'a>),
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
struct EnvelopeView<'a> {
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

impl EnvelopeView<'_> {
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
            if !crate::urn::is_valid(value) {
                return Err(GaipError::BadField {
                    field,
                    why: format!("'{value}' is not a valid agent URN"),
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
    /// No `wire` field: the tag was already proven `REQUEST` by the byte
    /// peek in `wire_tag`, and serde here ignores the unknown key.
    #[serde(default)]
    request_id: u32,
    #[serde(default)]
    contexts: Vec<ServiceContext>,
    #[serde(default)]
    body: Option<&'a RawValue>,
}

/// Decode a frame for a relay: a `REQUEST` comes back as a
/// [`RoutedRequest`] with its payload untouched; a `REPLY` as a
/// [`RoutedReply`] with its body untouched; anything else is handed to
/// [`decode_wire`], so there is exactly one implementation of the other
/// arms.
///
/// The tag is peeked from the raw bytes BEFORE any serde pass: a frame
/// that is not a REQUEST/REPLY (the rare control messages) used to pay a
/// full view parse and then a full enum parse for the same frame.
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
    match wire_tag(body) {
        Some("REQUEST") => {
            let view: FrameView = serde_json::from_slice(body)?;
            let raw = view.body.ok_or(GaipError::BadField {
                field: "body",
                why: "a REQUEST must carry an envelope".to_string(),
            })?;
            let env: EnvelopeView = serde_json::from_str(raw.get())?;
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
        Some("REPLY") => {
            let view: ReplyView = serde_json::from_slice(body)?;
            let recipient = match &view.body {
                Some(raw) => {
                    let env: EnvelopeView = serde_json::from_str(raw.get())?;
                    env.validate()?;
                    Some(env.recipient)
                }
                None => None,
            };
            Ok(Some((
                WireFrame::Reply(RoutedReply {
                    request_id: view.request_id,
                    status: view.status,
                    body: view.body,
                    contexts: view.contexts,
                    recipient,
                }),
                4 + len,
            )))
        }
        _ => {
            // not a REQUEST/REPLY: one pass, no wasted view parse
            let (msg, used) = decode_wire(buf)?.expect("frame length already checked");
            Ok(Some((WireFrame::Other(msg), used)))
        }
    }
}

/// Peek the `"wire":"<TAG>"` discriminator out of a frame body without
/// running serde: the broker's read loop sees REPLY as often as REQUEST,
/// and the old code paid a full view parse to find out it was neither.
///
/// Conservative by design: it only trusts the peek when `"wire"` is the
/// FIRST key — which is how every frame this protocol's own serializer
/// produces them (the tag leads). Any other layout (a hostile peer, an
/// escape in an earlier string) falls back to `None`, and the caller
/// runs the full serde pass. A misread here would reject a valid frame,
/// so the peek must be able to fail, never to lie.
fn wire_tag(body: &[u8]) -> Option<&str> {
    let text = std::str::from_utf8(body).ok()?;
    let s = text.trim_start();
    if !s.starts_with('{') {
        return None;
    }
    let rest = &s[1..];
    let open = rest.find('"')?;
    let close = rest[open + 1..].find('"')?;
    let key = &rest[open + 1..open + 1 + close];
    if key != "wire" {
        return None; // the tag is not first: let serde judge
    }
    let after = rest[open + 1 + close + 1..].trim_start();
    let after = after.strip_prefix(':')?.trim_start();
    let v = after.strip_prefix('"')?;
    let end = v.find('"')?;
    Some(&v[..end])
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

/// The outer `REPLY` frame, read with the body captured as a raw span.
#[derive(Deserialize)]
#[serde(bound(deserialize = "'de: 'a"))]
struct ReplyView<'a> {
    #[serde(default)]
    request_id: u32,
    status: ReplyStatus,
    #[serde(default)]
    body: Option<&'a RawValue>,
    #[serde(default)]
    contexts: Vec<ServiceContext>,
}

/// Re-frame a `REPLY` around the body bytes it arrived with: new contexts
/// (the broker stamps `DELIVERED`), the envelope untouched. The old path
/// deep-cloned the parsed body under the lock and re-serialized it — for
/// every reply crossing every broker.
pub fn encode_reply_view(
    request_id: u32,
    status: &ReplyStatus,
    contexts: &[ServiceContext],
    body: Option<&RawValue>,
) -> Result<Vec<u8>, GaipError> {
    #[derive(Serialize)]
    #[serde(tag = "wire", rename_all = "SCREAMING_SNAKE_CASE")]
    enum Out<'a> {
        Reply {
            request_id: u32,
            status: &'a ReplyStatus,
            #[serde(skip_serializing_if = "Option::is_none")]
            body: Option<&'a RawValue>,
            #[serde(skip_serializing_if = "ctx_empty")]
            contexts: &'a [ServiceContext],
        },
    }
    fn ctx_empty(c: &[ServiceContext]) -> bool {
        c.is_empty()
    }
    serialize_framed(
        &Out::Reply {
            request_id,
            status,
            body,
            contexts,
        },
        512 + body.map(|b| b.get().len()).unwrap_or(0),
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
    fn reply_takes_the_routing_path_and_reframes_its_body_untouched() {
        let reply = WireMessage::Reply {
            request_id: 9,
            status: ReplyStatus::NoException,
            body: Some(env()),
            contexts: vec![],
        };
        let frame = encode_wire(&reply).unwrap();
        let (view, used) = decode_wire_view(&frame).unwrap().unwrap();
        assert_eq!(used, frame.len());
        let WireFrame::Reply(rep) = view else {
            panic!("a REPLY must take the routing path");
        };
        assert_eq!(rep.request_id, 9);
        assert_eq!(rep.status, ReplyStatus::NoException);
        assert_eq!(
            rep.recipient.as_deref(),
            Some("urn:agent:rust:tokio:engine")
        );
        let body = rep.body.as_ref().unwrap();
        // stamp a hop and re-frame: the envelope bytes must survive verbatim
        let hop = ServiceContext::new(crate::context::id::ROUTE, "urn:agent:gaip:broker:b1");
        let out = encode_reply_view(
            9,
            &ReplyStatus::NoException,
            std::slice::from_ref(&hop),
            Some(body),
        )
        .unwrap();
        let (back, _) = decode_wire(&out).unwrap().unwrap();
        match back {
            WireMessage::Reply {
                request_id,
                contexts,
                body: b,
                ..
            } => {
                assert_eq!(request_id, 9);
                assert_eq!(contexts, vec![hop.clone()]);
                assert_eq!(b.as_ref().unwrap().message_id, "msg_1");
            }
            other => panic!("expected a REPLY, got {other:?}"),
        }
    }

    #[test]
    fn bodyless_exception_reply_has_no_recipient_and_no_body_key() {
        let reply = WireMessage::Reply {
            request_id: 10,
            status: ReplyStatus::system(SystemMinor::ObjectNotExist, "gone"),
            body: None,
            contexts: vec![],
        };
        let frame = encode_wire(&reply).unwrap();
        let (view, _) = decode_wire_view(&frame).unwrap().unwrap();
        let WireFrame::Reply(rep) = view else {
            panic!("expected the routing path");
        };
        assert!(rep.body.is_none());
        assert!(rep.recipient.is_none(), "an exception reply is unroutable");
        let out = encode_reply_view(
            10,
            &ReplyStatus::system(SystemMinor::ObjectNotExist, "gone"),
            &[],
            None,
        )
        .unwrap();
        let text = String::from_utf8(out[4..].to_vec()).unwrap();
        assert!(
            !text.contains("\"body\""),
            "bodyless reply must omit body: {text}"
        );
    }

    #[test]
    fn wire_tag_only_trusts_a_leading_wire_key() {
        assert_eq!(
            wire_tag(br#"{"wire":"REQUEST","request_id":1}"#),
            Some("REQUEST")
        );
        assert_eq!(wire_tag(b"  {\"wire\":\"REPLY\"}"), Some("REPLY"));
        // tag not first: let serde judge, the peek must not
        assert_eq!(wire_tag(br#"{"request_id":1,"wire":"REQUEST"}"#), None);
        assert_eq!(wire_tag(b"[1,2,3]"), None);
        assert_eq!(wire_tag(b"null"), None);
        assert_eq!(wire_tag(br#"{"note":"wire","request_id":1}"#), None);
    }

    #[test]
    fn view_falls_back_to_the_full_parse_for_control_messages() {
        let msg = message_error("refused");
        let frame = encode_wire(&msg).unwrap();
        match decode_wire_view(&frame).unwrap().unwrap().0 {
            WireFrame::Other(back) => assert_eq!(back, msg),
            other => panic!("a control message is not routable, got {other:?}"),
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
