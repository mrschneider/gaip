//! SSE binding: the envelope as an `text/event-stream` event.
//!
//! Streaming a task's progress to a UI is not an RPC — it is a
//! long-lived HTTP response. The same envelope travels, framed as:
//!
//! ```text
//! event: TASK_STREAM
//! data: {"protocol_version":"1.0", ...}
//!
//! ```
//!
//! SSE's own framing rules matter here: a `data:` line cannot contain a
//! raw newline (that would end the event), so a pretty-printed envelope
//! is emitted as several `data:` lines, which the client rejoins with
//! `\n` — exactly the rule in the WHATWG spec.

use crate::envelope::Envelope;
use crate::error::GaipError;

/// Render one envelope as one SSE event, ready to write to the response.
pub fn encode_event(env: &Envelope) -> Result<String, GaipError> {
    env.validate()?;
    let json = serde_json::to_string_pretty(env)?;
    let mut out = String::new();
    out.push_str("event: ");
    out.push_str(env.message_type.as_str());
    out.push('\n');
    for line in json.lines() {
        out.push_str("data: ");
        out.push_str(line);
        out.push('\n');
    }
    out.push('\n');
    Ok(out)
}

/// Render a heartbeat comment (`: keep-alive`). Proxies drop idle SSE
/// connections; a comment is the sanctioned way to keep it open without
/// inventing an event type — the vocabulary stays closed.
pub fn keepalive() -> &'static str {
    ": keep-alive\n\n"
}

/// Parse one SSE event (the `event:`/`data:` block) back into an
/// envelope. Returns `Ok(None)` for comment-only blocks.
pub fn decode_event(block: &str) -> Result<Option<Envelope>, GaipError> {
    let mut event: Option<String> = None;
    let mut data = String::new();
    for raw in block.lines() {
        let line = raw.trim_end_matches('\r');
        if line.is_empty() || line.starts_with(':') {
            continue;
        }
        if let Some(rest) = line.strip_prefix("event:") {
            event = Some(rest.trim().to_string());
        } else if let Some(rest) = line.strip_prefix("data:") {
            let rest = rest.strip_prefix(' ').unwrap_or(rest);
            if !data.is_empty() {
                data.push('\n');
            }
            data.push_str(rest);
        }
    }
    if data.is_empty() {
        return Ok(None); // comment/keep-alive block
    }
    let env = Envelope::from_json(&data)?;
    // the event name is a redundant copy of message_type: if they
    // disagree, the frame has been tampered with or mis-framed
    if let Some(name) = event {
        if name != env.message_type.as_str() {
            return Err(GaipError::BadField {
                field: "event",
                why: format!(
                    "SSE event '{name}' disagrees with message_type '{}'",
                    env.message_type.as_str()
                ),
            });
        }
    }
    Ok(Some(env))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn stream() -> Envelope {
        Envelope::new(
            "msg_9",
            "task_1",
            "urn:agent:rust:tokio:engine",
            "urn:agent:python:langgraph:planner-v2",
            crate::MessageType::TaskStream,
            json!({"progress": 25, "tokens": "SELECT * FROM"}),
        )
    }

    #[test]
    fn event_carries_the_primitive_name_and_multiline_data() {
        let text = encode_event(&stream()).unwrap();
        assert!(text.starts_with("event: TASK_STREAM\n"), "{text}");
        assert!(text.ends_with("\n\n"), "SSE events end with a blank line");
        assert!(
            text.lines().filter(|l| l.starts_with("data: ")).count() > 1,
            "pretty JSON is split across data lines"
        );
        assert!(!text.contains("\ndata: \n"), "no empty data line mid-event");
    }

    #[test]
    fn roundtrip_through_decode() {
        let env = stream();
        let text = encode_event(&env).unwrap();
        let back = decode_event(&text).unwrap().unwrap();
        assert_eq!(back, env);
    }

    #[test]
    fn keepalive_is_a_comment_not_an_event() {
        assert!(keepalive().starts_with(':'));
        assert!(decode_event(keepalive()).unwrap().is_none());
    }

    #[test]
    fn event_name_must_match_message_type() {
        let env = stream();
        let text = encode_event(&env)
            .unwrap()
            .replace("event: TASK_STREAM", "event: TASK_REPLY");
        let err = decode_event(&text).unwrap_err();
        assert!(err.to_string().contains("event"), "{err}");
    }

    #[test]
    fn crlf_framing_is_tolerated() {
        let text = encode_event(&stream()).unwrap().replace('\n', "\r\n");
        assert!(decode_event(&text).unwrap().is_some());
    }
}
