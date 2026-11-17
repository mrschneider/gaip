//! Isolate what a dedupe HIT costs: the combined-key String allocation
//! vs. `materialized()` re-parsing the raw body into an Envelope.
use gaip_core::dedupe::DedupeCache;
use gaip_core::envelope::Envelope;
use gaip_core::message::{ReplyStatus, WireMessage};
use serde_json::json;
use std::time::Instant;

fn body() -> String {
    let env = Envelope::new(
        String::from("msg_1"),
        String::from("task_1"),
        String::from("urn:agent:python:langgraph:planner"),
        String::from("urn:agent:rust:tokio:engine"),
        gaip_core::envelope::MessageType::TaskReply,
        json!({"intent":"INVENTORY_OPTIMIZATION","facility_id":42,"n":1234567890}),
    );
    env.to_json()
}

fn main() {
    let iters: u64 = std::env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(2_000_000);
    let b = body();

    // Case A: stored WITH raw body (the broker's relay path) -> hit parses.
    let mut c = DedupeCache::new(4096, std::time::Duration::from_secs(120));
    for i in 0..4096 {
        c.remember_raw(
            &format!("urn:agent:{i}"),
            &format!("msg_{i}"),
            WireMessage::Reply {
                request_id: 1,
                status: ReplyStatus::NoException,
                body: None,
                contexts: vec![],
            },
            Some(b.clone()),
        );
    }
    for _ in 0..10_000 {
        let _ = c.lookup_read("urn:agent:123", "msg_123");
    }
    let start = Instant::now();
    for i in 0..iters {
        let _ = c.lookup_read(&format!("urn:agent:{i}"), &format!("msg_{i}"));
    }
    let ns = start.elapsed().as_nanos() as f64 / iters as f64;
    println!("[HIT, body stored raw -> re-parses on lookup] {ns:.1} ns/op");

    // Case B: stored WITHOUT raw body (parsed entry) -> hit clones Arc only.
    let mut c2 = DedupeCache::new(4096, std::time::Duration::from_secs(120));
    for i in 0..4096 {
        let env = Envelope::new(
            format!("msg_{i}"),
            format!("t{i}"),
            "urn:agent:python:langgraph:planner".to_string(),
            "urn:agent:rust:tokio:engine".to_string(),
            gaip_core::envelope::MessageType::TaskReply,
            json!({"n": i}),
        );
        c2.remember(
            "urn:agent:python:langgraph:planner",
            &format!("msg_{i}"),
            WireMessage::Reply {
                request_id: 1,
                status: ReplyStatus::NoException,
                body: Some(env),
                contexts: vec![],
            },
        );
    }
    for _ in 0..10_000 {
        let _ = c2.lookup_read("urn:agent:python:langgraph:planner", "msg_123");
    }
    let start = Instant::now();
    for i in 0..iters {
        let _ = c2.lookup_read("urn:agent:python:langgraph:planner", &format!("msg_{i}"));
    }
    let ns = start.elapsed().as_nanos() as f64 / iters as f64;
    println!("[HIT, parsed entry -> Arc clone only] {ns:.1} ns/op");

    // Case C: the combined-key String allocation alone, fresh String each call.
    let start = Instant::now();
    for i in 0..iters {
        let _ = format!("urn:agent:{i}\u{1}msg_{i}");
    }
    let ns = start.elapsed().as_nanos() as f64 / iters as f64;
    println!("[combined-key String alloc alone] {ns:.1} ns/op");
}
