//! Hot-path micro-benchmark: encode_wire, encode+decode, to_vec+encode_body.
//! Run with: cargo run --release -p gaip-core --example bench_wire [-- N]
use gaip_core::codec::{decode_wire, encode_body, encode_wire};
use gaip_core::envelope::{Envelope, MessageType};
use gaip_core::message::WireMessage;
use std::time::Instant;

fn sample_msg() -> WireMessage {
    let env = Envelope::new(
        "msg_bench_0001".to_string(),
        "task_bench_0001".to_string(),
        "urn:agent:python:langgraph:planner-v2".to_string(),
        "urn:agent:rust:tokio:engine".to_string(),
        MessageType::TaskRequest,
        serde_json::json!({
            "intent": "INVENTORY_OPTIMIZATION",
            "facility_id": 42,
            "horizon_days": 30,
            "constraints": {"budget": 1250000.0, "service_level": 0.98},
            "artifacts_ref": [],
            "notes": "synthetic payload sized like a typical request (~617 bytes of JSON)"
        }),
    );
    WireMessage::Request {
        request_id: 7,
        contexts: vec![],
        body: env,
    }
}

fn bench(name: &str, iters: u64, f: impl Fn()) {
    // warmup
    for _ in 0..10_000 {
        f();
    }
    let start = Instant::now();
    for _ in 0..iters {
        f();
    }
    let elapsed = start.elapsed();
    let ns = elapsed.as_nanos() as f64 / iters as f64;
    println!("{name:>22}: {ns:10.2} ns/op  ({:.0} M/s)", 1e9 / ns / 1e6);
}

fn main() {
    let iters: u64 = std::env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(2_000_000);
    let msg = sample_msg();
    let wire_len = encode_wire(&msg).unwrap().len();
    println!("wire size: {wire_len} bytes, {iters} iterations\n");

    let bytes = encode_wire(&msg).unwrap();
    bench("encode_wire", iters, || {
        let _ = encode_wire(&msg);
    });
    bench("encode+decode", iters, || {
        let b = encode_wire(&msg).unwrap();
        let _ = decode_wire(&b).unwrap();
    });
    let body = serde_json::to_vec(&msg).unwrap();
    bench("to_vec+encode_body", iters, || {
        let b = serde_json::to_vec(&msg).unwrap();
        let _ = encode_body(b);
    });
    // keep the samples alive so the optimizer cannot fold them away
    std::hint::black_box((&bytes, &body));
}
