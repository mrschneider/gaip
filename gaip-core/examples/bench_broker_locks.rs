//! Concurrency probe for the broker's dedupe cache in its REAL shape:
//! every relayed request does a READ-ONLY exactly-once probe
//! (`lookup_read`), and only a completed reply does a WRITE (`remember_raw`).
//!
//! The broker today wraps the cache in a Mutex, so even the read probes
//! serialize. This bench measures a `RwLock` around the cache (probes take
//! `.read()`, stores take `.write()`) with std vs parking_lot, so the gain
//! from (a) the lock choice and (b) read/write splitting is isolated.
//!
//! Run: cargo run --release -p gaip-core --example bench_broker_locks [-- threads]
use gaip_core::dedupe::DedupeCache;
use gaip_core::envelope::Envelope;
use gaip_core::message::{ReplyStatus, WireMessage};
use serde_json::json;
use std::sync::mpsc;
use std::sync::{Arc, RwLock as StdRwLock};
use std::thread;
use std::time::Instant;

const PER_THREAD: u64 = 200_000;
/// 1 store per 8 probes: a relay probes on every request and stores once the
/// reply comes back, so probes dominate.
const STORE_EVERY: u64 = 8;

fn reply_body() -> String {
    let env = Envelope::new(
        String::from("msg_1"),
        String::from("task_1"),
        String::from("urn:a"),
        String::from("urn:b"),
        gaip_core::envelope::MessageType::TaskReply,
        json!({"intent": "INVENTORY_OPTIMIZATION", "n": 42}),
    );
    env.to_json()
}

fn main() {
    let max_threads = std::env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(8);
    let body = reply_body();
    let mut report = String::new();

    for n in [1u32, 2, 4, 8, 16] {
        if n > max_threads {
            break;
        }

        // ── std::RwLock (read probes + occasional write stores) ───────
        {
            let cache = Arc::new(StdRwLock::new(DedupeCache::new(
                1 << 20,
                std::time::Duration::from_secs(120),
            )));
            let (tx, rx) = mpsc::channel();
            let start = Instant::now();
            let mut handles = Vec::new();
            for t in 0..n {
                let cache = cache.clone();
                let value = body.clone();
                let tx = tx.clone();
                handles.push(thread::spawn(move || {
                    let mut local = 0u64;
                    for i in 0..PER_THREAD {
                        let sender = format!("urn:agent:{}:{}", t, i % 500);
                        let msg_id = format!("msg_{t}_{i}");
                        if i % STORE_EVERY == 0 {
                            cache.write().unwrap().remember_raw(
                                &sender,
                                &msg_id,
                                WireMessage::Reply {
                                    request_id: 1,
                                    status: ReplyStatus::NoException,
                                    body: None,
                                    contexts: vec![],
                                },
                                Some(value.clone()),
                            );
                        } else {
                            let _ = cache.read().unwrap().lookup_read(&sender, &msg_id);
                        }
                        local += 1;
                    }
                    tx.send(local).unwrap();
                }));
            }
            drop(tx);
            let total: u64 = rx.iter().sum();
            for h in handles {
                h.join().unwrap();
            }
            let ns = start.elapsed().as_nanos() as f64 / total as f64;
            report.push_str(&format!(
                "[std::RwLock]     [{n:2} threads] {ns:8.1} ns/op ({:.2} M/s)\n",
                1e9 / ns / 1e6
            ));
        }

        // ── parking_lot::RwLock (same read-heavy shape) ───────────────
        {
            let cache = Arc::new(parking_lot::RwLock::new(DedupeCache::new(
                1 << 20,
                std::time::Duration::from_secs(120),
            )));
            let (tx, rx) = mpsc::channel();
            let start = Instant::now();
            let mut handles = Vec::new();
            for t in 0..n {
                let cache = cache.clone();
                let value = body.clone();
                let tx = tx.clone();
                handles.push(thread::spawn(move || {
                    let mut local = 0u64;
                    for i in 0..PER_THREAD {
                        let sender = format!("urn:agent:{}:{}", t, i % 500);
                        let msg_id = format!("msg_{t}_{i}");
                        if i % STORE_EVERY == 0 {
                            cache.write().remember_raw(
                                &sender,
                                &msg_id,
                                WireMessage::Reply {
                                    request_id: 1,
                                    status: ReplyStatus::NoException,
                                    body: None,
                                    contexts: vec![],
                                },
                                Some(value.clone()),
                            );
                        } else {
                            let _ = cache.read().lookup_read(&sender, &msg_id);
                        }
                        local += 1;
                    }
                    tx.send(local).unwrap();
                }));
            }
            drop(tx);
            let total: u64 = rx.iter().sum();
            for h in handles {
                h.join().unwrap();
            }
            let ns = start.elapsed().as_nanos() as f64 / total as f64;
            report.push_str(&format!(
                "[parking_lot RwLock] [{n:2} threads] {ns:8.1} ns/op ({:.2} M/s)\n",
                1e9 / ns / 1e6
            ));
        }
    }
    print!("{report}");
}
