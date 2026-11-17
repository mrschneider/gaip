//! Concurrency probe for the single global dedupe Mutex the broker holds
//! under its read lock. Single-threaded throughput is fine; the question
//! is whether N threads hammering the same Mutex plateaus (serialize) or
//! scales. Run with: cargo run --release -p gaip-core --example bench_dedupe_concurrency [-- threads]
use gaip_core::dedupe::DedupeCache;
use gaip_core::envelope::Envelope;
use gaip_core::message::{ReplyStatus, WireMessage};
use serde_json::json;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;
use std::sync::Arc;
use std::thread;
use std::time::Instant;

fn reply_body() -> String {
    let env = Envelope::new(
        String::from("msg_1"),
        String::from("task_1"),
        String::from("urn:a"),
        String::from("urn:b"),
        gaip_core::envelope::MessageType::TaskReply,
        json!({"intent":"INVENTORY_OPTIMIZATION","n":42}),
    );
    env.to_json()
}

fn main() {
    let threads = std::env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(8);
    let per_thread: u64 = 200_000;

    // Single-thread baseline.
    {
        let mut c = DedupeCache::new(1 << 20, std::time::Duration::from_secs(120));
        let body = reply_body();
        let start = Instant::now();
        for i in 0..per_thread {
            let sender = format!("urn:agent:{}", i % 1000);
            let msg_id = format!("msg_{i}");
            c.remember_raw(
                &sender,
                &msg_id,
                WireMessage::Reply {
                    request_id: 1,
                    status: ReplyStatus::NoException,
                    body: None,
                    contexts: vec![],
                },
                Some(body.clone()),
            );
            let _ = c.lookup_read(&sender, &msg_id);
        }
        let ns = start.elapsed().as_nanos() as f64 / per_thread as f64;
        println!(
            "[single-thread] {per_thread} ops: {ns:.1} ns/op ({:.2} M/s)",
            1e9 / ns / 1e6
        );
    }

    // Multi-threaded: many workers on ONE shared cache (the broker's real shape).
    for n in [1u32, 2, 4, 8, 16] {
        if n > threads {
            break;
        }
        let cache = Arc::new(std::sync::Mutex::new(DedupeCache::new(
            1 << 20,
            std::time::Duration::from_secs(120),
        )));
        let counter = Arc::new(AtomicU64::new(0));
        let (tx, rx) = mpsc::channel();
        let start = Instant::now();
        let mut handles = Vec::new();
        for t in 0..n {
            let cache = cache.clone();
            let counter = counter.clone();
            let tx = tx.clone();
            handles.push(thread::spawn(move || {
                let body = reply_body();
                let mut local = 0u64;
                for i in 0..per_thread {
                    let sender = format!("urn:agent:{}:{}", t, i % 500);
                    let msg_id = format!("msg_{t}_{i}");
                    cache.lock().unwrap().remember_raw(
                        &sender,
                        &msg_id,
                        WireMessage::Reply {
                            request_id: 1,
                            status: ReplyStatus::NoException,
                            body: None,
                            contexts: vec![],
                        },
                        Some(body.clone()),
                    );
                    let _ = cache.lock().unwrap().lookup_read(&sender, &msg_id);
                    local += 1;
                }
                counter.fetch_add(local, Ordering::Relaxed);
                tx.send(local).unwrap();
            }));
        }
        drop(tx);
        let mut total = 0;
        for _ in 0..n {
            total += rx.recv().unwrap();
        }
        for h in handles {
            h.join().unwrap();
        }
        let ns = start.elapsed().as_nanos() as f64 / total as f64;
        println!(
            "[{n:2} threads] {total} ops: {ns:.1} ns/op ({:.2} M/s)",
            1e9 / ns / 1e6
        );
    }
}
