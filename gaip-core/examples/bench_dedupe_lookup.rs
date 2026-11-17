//! Measure the per-lookup cost of the dedupe cache and the cost of a
//! comparable String allocation, to size the ROI of a zero-alloc lookup key.
use gaip_core::dedupe::DedupeCache;
use gaip_core::envelope::Envelope;
use gaip_core::message::{ReplyStatus, WireMessage};
use serde_json::json;
use std::time::Instant;

fn body() -> String {
    let env = Envelope::new(
        String::from("msg_1"), String::from("task_1"),
        String::from("urn:agent:python:langgraph:planner"),
        String::from("urn:agent:rust:tokio:engine"),
        gaip_core::envelope::MessageType::TaskReply,
        json!({"intent":"INVENTORY_OPTIMIZATION","facility_id":42}),
    );
    env.to_json()
}

fn main() {
    let iters: u64 = std::env::args().nth(1).and_then(|s| s.parse().ok()).unwrap_or(2_000_000);
    // Populate a realistic window.
    let mut c = DedupeCache::new(4096, std::time::Duration::from_secs(120));
    let b = body();
    for i in 0..4096 {
        c.remember_raw(&format!("urn:agent:{i}"), &format!("msg_{i}"),
            WireMessage::Reply { request_id: 1, status: ReplyStatus::NoException, body: None, contexts: vec![] },
            Some(b.clone()));
    }
    // Warmup + measure lookup_read (the hot read-only probe).
    for _ in 0..10_000 { let _ = c.lookup_read("urn:agent:123", "msg_123"); }
    let start = Instant::now();
    for i in 0..iters { let _ = c.lookup_read(&format!("urn:agent:{i}"), &format!("msg_{i}")); }
    let ns = start.elapsed().as_nanos() as f64 / iters as f64;
    println!("[lookup_read, HIT] {ns:.1} ns/op");

    // Warmup + measure lookup_miss (key still allocated, not found).
    for _ in 0..10_000 { let _ = c.lookup_read("urn:agent:miss", "msg_miss"); }
    let start = Instant::now();
    for i in 0..iters { let _ = c.lookup_read(&format!("urn:agent:miss{i}"), &format!("msg_miss{i}")); }
    let ns = start.elapsed().as_nanos() as f64 / iters as f64;
    println!("[lookup_read, MISS] {ns:.1} ns/op");

    // Baseline: allocating one String of comparable size (what key() does).
    let sample = format!("urn:agent:python:langgraph:planner\u{1}msg_000000");
    for _ in 0..10_000 { let _ = sample.clone(); }
    let start = Instant::now();
    for i in 0..iters { let _ = sample.clone(); }
    let ns = start.elapsed().as_nanos() as f64 / iters as f64;
    println!("[String clone ~60B] {ns:.1} ns/op  <-- what the combined key costs per lookup");
}
