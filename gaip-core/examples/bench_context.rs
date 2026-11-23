//! Isolates the broker's per-request context handling: the OLD path
//! (from_vec + to_vec, which clones every context string, plus the old
//! alloc-heavy push_route) versus the NEW path (from_owned +
//! into_sorted_vec, which moves the strings, plus the single-alloc
//! push_route). This is the exact code the broker runs between decoding
//! a request and re-framing it, so the delta here is the real per-relay
//! gain.
//!
//! Run: cargo run --release -p gaip-core --example bench_context [-- N]

use gaip_core::context::{ContextList, ServiceContext};
use std::hint::black_box;
use std::time::Instant;

/// A realistic context list as it arrives off the wire: trace, deadline,
/// tenant, attempt, and a growing route (the common relay shape).
fn wire_contexts(hops: usize) -> Vec<ServiceContext> {
    let mut v = vec![
        ServiceContext::trace("trace-0f8a2c1e"),
        ServiceContext::new(gaip_core::context::id::DEADLINE, "1799999999999"),
        ServiceContext::tenant("acme"),
        ServiceContext::attempt(1),
    ];
    let route = (0..hops)
        .map(|i| format!("urn:agent:gaip:broker:b{i}"))
        .collect::<Vec<_>>()
        .join(",");
    v.push(ServiceContext::new(gaip_core::context::id::ROUTE, route));
    v
}

/// The OLD push_route, replicated verbatim so the A/B is honest:
/// combined-string build + Vec of hops + join. (Inlined in the OLD bench
/// below; kept here as documentation of the allocation shape.)
#[allow(dead_code)]
fn old_push_route_emu(route: &str, hop: &str) -> String {
    let mut combined = route.to_string();
    if !combined.is_empty() {
        combined.push(',');
    }
    combined.push_str(hop);
    let hs: Vec<&str> = combined.split(',').collect();
    let start = hs.len().saturating_sub(16);
    hs[start..].join(",")
}

fn bench(name: &str, iters: u64, f: impl Fn()) -> f64 {
    for _ in 0..10_000 {
        f();
    }
    let start = Instant::now();
    for _ in 0..iters {
        f();
    }
    let elapsed = start.elapsed();
    let ns = elapsed.as_nanos() as f64 / iters as f64;
    println!("{name:>44}: {ns:10.2} ns/op");
    ns
}

fn main() {
    let iters: u64 = std::env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(2_000_000);

    for hops in [1usize, 4, 8] {
        println!("\n=== route depth = {hops} hops, {iters} iterations ===");

        // OLD path: from_vec (clones) + route_contains + old push_route
        // (3 allocs, emulated) + to_vec (clones).
        let old = bench("OLD: from_vec+contains+push(3alloc)+to_vec", iters, || {
            let ctxs = wire_contexts(hops);
            let mut cl = ContextList::from_vec(ctxs);
            black_box(cl.route_contains("urn:agent:gaip:broker:bX"));
            // emulate the old push_route's allocation cost (string+vec+join)
            let route_str = cl.route().join(",");
            let _joined = old_push_route_emu(&route_str, "urn:agent:gaip:broker:bNEW");
            black_box(cl.to_vec());
        });

        // NEW path: from_owned (moves) + route_contains + push_route (1
        // alloc) + into_sorted_vec (moves).
        let new = bench("NEW: from_owned+contains+push(1alloc)+into", iters, || {
            let ctxs = wire_contexts(hops);
            let mut cl = ContextList::from_owned(ctxs);
            black_box(cl.route_contains("urn:agent:gaip:broker:bX"));
            cl.push_route("urn:agent:gaip:broker:bNEW");
            black_box(cl.into_sorted_vec());
        });

        println!(
            "  -> saved {saved:.2} ns/op ({pct:.1}% faster)",
            saved = old - new,
            pct = (old - new) / old * 100.0
        );
    }
}
