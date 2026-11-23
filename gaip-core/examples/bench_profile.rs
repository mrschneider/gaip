//! Detailed profile of the broker relay hot path: decode_wire_view,
//! encode_request_view, encode_reply_view, and the individual sub-operations.
//!
//! Run: cargo run --release -p gaip-core --example bench_profile [-- N]

use gaip_core::codec::{
    decode_wire_view, encode_reply_view, encode_request_view, encode_wire, WireFrame,
};
use gaip_core::context::ContextList;
use gaip_core::envelope::{Envelope, MessageType};
use gaip_core::message::{ReplyStatus, WireMessage};
use std::hint::black_box;
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
            "notes": "synthetic payload sized like a typical request"
        }),
    )
}

fn bench(name: &str, iters: u64, f: impl Fn()) {
    for _ in 0..10_000 {
        f();
    }
    let start = Instant::now();
    for _ in 0..iters {
        f();
    }
    let elapsed = start.elapsed();
    let ns = elapsed.as_nanos() as f64 / iters as f64;
    println!("{name:>40}: {ns:10.2} ns/op  ({:.0} M/s)", 1e9 / ns / 1e6);
}

fn main() {
    let iters: u64 = std::env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(2_000_000);

    let env = sample_env();
    let msg = WireMessage::Request {
        request_id: 7,
        contexts: vec![],
        body: env.clone(),
    };
    let bytes = encode_wire(&msg).unwrap();
    println!("wire size: {} bytes, {} iterations\n", bytes.len(), iters);

    // ── Full relay path ──
    bench("decode_wire_view (full)", iters, || {
        let _ = decode_wire_view(&bytes).unwrap();
    });

    bench("encode_request_view (full)", iters, || {
        let (frame, _) = decode_wire_view(&bytes).unwrap().unwrap();
        if let WireFrame::Request(req) = frame {
            let hop = gaip_core::ServiceContext::new(
                gaip_core::context::id::ROUTE,
                "urn:agent:gaip:broker:b1",
            );
            let _ = encode_request_view(req.request_id, &[hop], req.envelope);
        }
    });

    // ── Sub-operations ──

    // ContextList operations (per request)
    bench("ContextList::from_vec (empty)", iters, || {
        let _ = ContextList::from_vec(vec![]);
    });
    bench("ContextList::from_vec (3 ctx)", iters, || {
        let v = vec![
            gaip_core::ServiceContext::trace("t1"),
            gaip_core::ServiceContext::new(gaip_core::context::id::DEADLINE, "9999999999999"),
            gaip_core::ServiceContext::new(
                gaip_core::context::id::ROUTE,
                "urn:agent:gaip:broker:b0",
            ),
        ];
        let _ = ContextList::from_vec(v);
    });
    bench("ContextList::route_contains", iters, || {
        let mut cl = ContextList::new();
        cl.push_route("urn:agent:gaip:broker:b0");
        cl.push_route("urn:agent:gaip:broker:b1");
        black_box(cl.route_contains("urn:agent:gaip:broker:b2"));
    });
    bench("ContextList::push_route", iters, || {
        let mut cl = ContextList::new();
        cl.push_route("urn:agent:gaip:broker:b0");
        black_box(&mut cl);
    });
    bench("ContextList::to_vec (3 ctx)", iters, || {
        let mut cl = ContextList::new();
        cl.set(gaip_core::context::id::TRACE, "t1");
        cl.set(gaip_core::context::id::DEADLINE, "9999999999999");
        cl.set(gaip_core::context::id::ROUTE, "urn:agent:gaip:broker:b0");
        black_box(cl.to_vec());
    });

    // serde_json sub-operations
    let env_text = env.to_json();
    bench("serde_json::from_str::<Envelope>", iters, || {
        let _ = serde_json::from_str::<Envelope>(&env_text).unwrap();
    });

    // The outer FrameView parse (request_id + contexts + body)
    bench("serde_json outer FrameView", iters, || {
        #[derive(serde::Deserialize)]
        #[serde(bound(deserialize = "'de: 'a"))]
        #[allow(dead_code)] // fields exist only to shape the parse under benchmark
        struct FV<'a> {
            #[serde(default)]
            request_id: u32,
            #[serde(default)]
            contexts: Vec<gaip_core::ServiceContext>,
            #[serde(default)]
            body: Option<&'a serde_json::value::RawValue>,
        }
        let _ = serde_json::from_slice::<FV>(&bytes[4..]).unwrap();
    });

    // encode_reply_view
    let reply = WireMessage::reply(7, env.clone());
    let reply_bytes = encode_wire(&reply).unwrap();
    bench("encode_reply_view (full)", iters, || {
        let (frame, _) = decode_wire_view(&reply_bytes).unwrap().unwrap();
        if let WireFrame::Reply(rep) = frame {
            let hop = gaip_core::ServiceContext::new(
                gaip_core::context::id::ROUTE,
                "urn:agent:gaip:broker:b1",
            );
            let _ = encode_reply_view(rep.request_id, &ReplyStatus::NoException, &[hop], rep.body);
        }
    });

    // The reply view parse (outer)
    bench("serde_json ReplyView (outer)", iters, || {
        #[derive(serde::Deserialize)]
        #[serde(bound(deserialize = "'de: 'a"))]
        #[allow(dead_code)] // fields exist only to shape the parse under benchmark
        struct RV<'a> {
            #[serde(default)]
            request_id: u32,
            status: ReplyStatus,
            #[serde(default)]
            body: Option<&'a serde_json::value::RawValue>,
            #[serde(default)]
            contexts: Vec<gaip_core::ServiceContext>,
        }
        let _ = serde_json::from_slice::<RV>(&reply_bytes[4..]).unwrap();
    });

    println!("\n--- Breakdown summary ---");
    println!("The relay REQUEST path is:");
    println!("  1. wire_tag peek (byte scan)");
    println!("  2. serde_json outer FrameView (request_id, contexts, body span)");
    println!("  3. serde_json inner EnvelopeView (routing fields, payload span)");
    println!("  4. EnvelopeView::validate (version, URN checks, payload shape)");
    println!("  5. ContextList::from_vec + push_route + to_vec");
    println!("  6. encode_request_view (splices body bytes back)");
    println!();
    println!("The relay REPLY path is:");
    println!("  1. wire_tag peek (byte scan)");
    println!("  2. serde_json ReplyView (request_id, status, body span, contexts)");
    println!("  3. serde_json inner EnvelopeView (recipient only)");
    println!("  4. EnvelopeView::validate");
    println!("  5. dedupe remember_raw (store body bytes)");
    println!("  6. encode_reply_view (splices body bytes back)");

    black_box((&bytes, &reply_bytes));
}
