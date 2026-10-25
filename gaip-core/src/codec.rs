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

use crate::envelope::{check_version, Envelope};
use crate::error::GAIPError;

/// 8 MiB. An agent payload above this is an artifact, and artifacts
/// travel by reference (`artifacts_ref`), not inline.
pub const MAX_FRAME: usize = 8 * 1024 * 1024;

/// Encode one envelope as a length-prefixed frame.
pub fn encode_frame(env: &Envelope) -> Result<Vec<u8>, GAIPError> {
    env.validate()?;
    let body = serde_json::to_vec(env)?;
    if body.len() > MAX_FRAME {
        return Err(GAIPError::FrameTooLarge(body.len()));
    }
    let mut out = Vec::with_capacity(4 + body.len());
    out.extend_from_slice(&(body.len() as u32).to_be_bytes());
    out.extend_from_slice(&body);
    Ok(out)
}

/// Try to decode one frame from the front of `buf`.
///
/// * `Ok(Some((envelope, consumed)))` — a complete, validated envelope.
/// * `Ok(None)` — need more bytes.
/// * `Err(_)` — refuse with the reason (bad length, bad JSON, unknown
///   primitive, version mismatch...).
pub fn decode_frame(buf: &[u8]) -> Result<Option<(Envelope, usize)>, GAIPError> {
    if buf.len() < 4 {
        return Ok(None);
    }
    let len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
    if len == 0 {
        return Err(GAIPError::BadField {
            field: "frame",
            why: "zero-length frame".to_string(),
        });
    }
    if len > MAX_FRAME {
        return Err(GAIPError::FrameTooLarge(len));
    }
    if buf.len() < 4 + len {
        return Ok(None);
    }
    let body = &buf[4..4 + len];
    let env = Envelope::from_value(&serde_json::from_slice(body)?)?;
    Ok(Some((env, 4 + len)))
}

/// Parse a bare JSON envelope (message-oriented bindings: Kafka/NATS/SSE).
pub fn decode_json(text: &str) -> Result<Envelope, GAIPError> {
    Envelope::from_json(text)
}

/// A version probe that does not require a full envelope: useful for a
/// gateway that must answer `PROTOCOL_VERSION` before parsing payloads.
pub fn probe_version(text: &str) -> Option<String> {
    let v: serde_json::Value = serde_json::from_str(text).ok()?;
    v.get("protocol_version")?.as_str().map(str::to_string)
}

/// Is this peer speaking a version we can serve?
pub fn version_compatible(text: &str) -> Result<(), GAIPError> {
    match probe_version(text) {
        Some(v) => check_version(&v),
        None => Err(GAIPError::MissingField("protocol_version")),
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
            GAIPError::FrameTooLarge(_)
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
}
