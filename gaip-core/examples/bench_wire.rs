//! Hot-path micro-benchmark: encode_wire, encode+decode, to_vec+encode_body,
//! and the relay's routing path (read the routing fields, re-frame with the
//! body bytes untouched).
//! Run with: cargo run --release -p gaip-core --example bench_wire [-- N]
use gaip_core::codec::{
    decode_wire, decode_wire_view, encode_body, encode_reply_view, encode_request_view,
    encode_wire, WireFrame,
};
use gaip_core::envelope::{Envelope, MessageType};
use gaip_core::message::{ReplyStatus, WireMessage};
use std::time::Instant;

fn sample_env() -> Envelope {
    Envelope::new(
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
    )
}

fn sample_msg() -> WireMessage {
    WireMessage::Request {
        request_id: 7,
        contexts: vec![],
        body: sample_env(),
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
    println!("{name:>26}: {ns:10.2} ns/op  ({:.0} M/s)", 1e9 / ns / 1e6);
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
    bench("decode_wire (participant)", iters, || {
        let _ = decode_wire(&bytes).unwrap();
    });
    bench("decode_wire_view (relay)", iters, || {
        let _ = decode_wire_view(&bytes).unwrap();
    });
    // What a relay does per request: read the routing fields, then re-frame
    // with one extra service context and the body bytes it arrived with.
    bench("relay: full decode + encode", iters, || {
        let (msg, _) = decode_wire(&bytes).unwrap().unwrap();
        let _ = encode_wire(&msg);
    });
    bench("relay: view + reframe", iters, || {
        let (frame, _) = decode_wire_view(&bytes).unwrap().unwrap();
        if let WireFrame::Request(req) = frame {
            let hop = gaip_core::ServiceContext::new(
                gaip_core::context::id::ROUTE,
                "urn:agent:gaip:broker:b1",
            );
            let _ = encode_request_view(req.request_id, &[hop], req.envelope);
        }
    });
    // The REPLY leg of the same relay: a participant answers, the broker
    // routes the reply to the origin and re-frames with the hop stamped —
    // body bytes carried verbatim, no Value tree.
    let reply = WireMessage::reply(7, sample_env());
    let reply_bytes = encode_wire(&reply).unwrap();
    bench("relay: reply view + reframe", iters, || {
        let (frame, _) = decode_wire_view(&reply_bytes).unwrap().unwrap();
        if let WireFrame::Reply(rep) = frame {
            let hop = gaip_core::ServiceContext::new(
                gaip_core::context::id::ROUTE,
                "urn:agent:gaip:broker:b1",
            );
            let _ = encode_reply_view(rep.request_id, &ReplyStatus::NoException, &[hop], rep.body);
        }
    });
    let body = serde_json::to_vec(&msg).unwrap();
    bench("to_vec+encode_body", iters, || {
        let b = serde_json::to_vec(&msg).unwrap();
        let _ = encode_body(b);
    });
    // keep the samples alive so the optimizer cannot fold them away
    std::hint::black_box((&bytes, &body));
}
