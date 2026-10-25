//! GAIP reference participant: a "heavy computation" engine.
//!
//! It exists to prove the protocol end to end, not to be clever: it
//! listens on a framed TCP binding, accepts `TASK_REQUEST`, streams
//! progress with `TASK_STREAM`, answers `CAPABILITY_QUERY`, honours
//! `CANCEL_REQUEST` mid-flight, and pauses on `HUMAN_INTERVENT` until a
//! human decision arrives as a `TASK_REPLY`.
//!
//! The participant keeps a [`TaskRegistry`], so an out-of-order message is
//! refused with the state named instead of corrupting a run.

use gaip_core::codec::{decode_frame, encode_frame, MAX_FRAME};
use gaip_core::lifecycle::TaskRegistry;
use gaip_core::{Envelope, GAIPError, MessageType};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Per-connection participant state.
struct Agent {
    urn: String,
    writer: Arc<Mutex<TcpStream>>,
    registry: Mutex<TaskRegistry>,
    cancelled: Mutex<BTreeSet<String>>,
    decisions: Mutex<BTreeMap<String, String>>,
    msg_seq: AtomicU64,
}

impl Agent {
    fn next_msg_id(&self, prefix: &str) -> String {
        let n = self.msg_seq.fetch_add(1, Ordering::SeqCst) + 1;
        format!("{prefix}_{n}")
    }

    fn send(&self, env: &Envelope) {
        match encode_frame(env) {
            Ok(bytes) => {
                let mut w = self.writer.lock().unwrap();
                let _ = w.write_all(&bytes);
                let _ = w.flush();
            }
            Err(e) => eprintln!("[engine] cannot encode: {e}"),
        }
    }

    /// A frame that is not an envelope (the peer's envelope could not be
    /// parsed, so answering with one would be a lie). Bindings carry it
    /// as a length-prefixed JSON object; clients detect it by the
    /// `gaip_error` key.
    fn send_transport_error(&self, message: &str) {
        let body = json!({"gaip_error": {"message": message}}).to_string();
        let mut bytes = Vec::with_capacity(4 + body.len());
        bytes.extend_from_slice(&(body.len() as u32).to_be_bytes());
        bytes.extend_from_slice(body.as_bytes());
        let mut w = self.writer.lock().unwrap();
        let _ = w.write_all(&bytes);
        let _ = w.flush();
    }

    fn reply(&self, req: &Envelope, payload: Value) {
        let env = req.reply_to(self.next_msg_id("msg"), payload);
        self.send(&env);
    }

    fn stream(&self, req: &Envelope, payload: Value) {
        let env = req.stream_to(self.next_msg_id("msg"), payload);
        self.send(&env);
    }
}

fn main() {
    let mut host = "127.0.0.1".to_string();
    let mut port = 7610u16;
    let mut name = "urn:agent:rust:tokio:analytics-engine-v1".to_string();

    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        match a.as_str() {
            "--host" => host = args.next().unwrap_or(host),
            "--port" => port = args.next().and_then(|p| p.parse().ok()).unwrap_or(port),
            "--name" => name = args.next().unwrap_or(name),
            "-h" | "--help" => {
                println!("gaip-engine [--host 127.0.0.1] [--port 7610] [--name urn:agent:...]");
                return;
            }
            other => {
                eprintln!("unknown argument '{other}'");
                std::process::exit(2);
            }
        }
    }

    let listener = match TcpListener::bind((host.as_str(), port)) {
        Ok(l) => l,
        Err(e) => {
            eprintln!("gaip-engine: cannot bind {host}:{port}: {e}");
            std::process::exit(1);
        }
    };
    eprintln!("[engine] {name} listening on {host}:{port}");

    for (id, stream) in listener.incoming().flatten().enumerate() {
        let id = id as u64 + 1;
        let _ = stream.set_nodelay(true);
        let writer = match stream.try_clone() {
            Ok(s) => s,
            Err(_) => continue,
        };
        let agent = Arc::new(Agent {
            urn: name.clone(),
            writer: Arc::new(Mutex::new(writer)),
            registry: Mutex::new(TaskRegistry::new()),
            cancelled: Mutex::new(BTreeSet::new()),
            decisions: Mutex::new(BTreeMap::new()),
            msg_seq: AtomicU64::new(0),
        });
        std::thread::spawn(move || {
            eprintln!("[engine] peer {id} connected");
            serve(agent, stream);
            eprintln!("[engine] peer {id} closed");
        });
    }
}

fn serve(agent: Arc<Agent>, mut stream: TcpStream) {
    let mut buf: Vec<u8> = Vec::new();
    let mut chunk = [0u8; 8192];
    loop {
        match stream.read(&mut chunk) {
            Ok(0) => return,
            Ok(n) => buf.extend_from_slice(&chunk[..n]),
            Err(_) => return,
        }
        loop {
            match decode_frame(&buf) {
                Ok(Some((env, used))) => {
                    buf.drain(..used);
                    if !handle(&agent, env) {
                        return;
                    }
                }
                Ok(None) => break, // need more bytes
                Err(e) => {
                    // the envelope could not be parsed: answer in the
                    // transport's own error form and drop the buffer
                    agent.send_transport_error(&e.to_string());
                    eprintln!("[engine] refused a frame: {e}");
                    buf.clear();
                    break;
                }
            }
        }
        if buf.len() > MAX_FRAME {
            agent.send_transport_error("buffer exceeded the frame cap");
            return;
        }
    }
}

/// Returns `false` when the connection must close.
fn handle(agent: &Arc<Agent>, env: Envelope) -> bool {
    // the participant is the addressee or nothing
    if env.recipient != agent.urn {
        agent.send_transport_error(&format!(
            "{} is not a participant of this message (recipient {})",
            agent.urn, env.recipient
        ));
        return true;
    }
    {
        let mut reg = agent.registry.lock().unwrap();
        if let Err(e) = reg.accept(&env) {
            drop(reg);
            agent.send_transport_error(&e.to_string());
            return true;
        }
    }

    match env.message_type {
        MessageType::CapabilityQuery => {
            agent.reply(
                &env,
                json!({
                    "capabilities": ["INVENTORY_OPTIMIZATION", "DEMAND_FORECAST", "STREAM_PROGRESS"],
                    "runtime": "rust",
                    "framework": "tokio",
                    "max_concurrency": 8,
                    "protocol_versions": ["1.0"],
                }),
            );
        }
        MessageType::TaskRequest => {
            let needs_human = env
                .payload
                .get("requires_human")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            if needs_human {
                // ask for the audit BEFORE computing: the payload names
                // what the human is approving
                let pause = Envelope::new(
                    agent.next_msg_id("msg"),
                    env.correlation_id.clone(),
                    agent.urn.clone(),
                    env.sender.clone(),
                    MessageType::HumanIntervent,
                    json!({
                        "reason": "writes to a production warehouse",
                        "proposed_action": env.payload.get("intent").cloned().unwrap_or(Value::Null),
                        "approve_with": {"decision": "APPROVE"},
                    }),
                );
                agent.send(&pause);
                return true; // the worker starts when the decision lands
            }
            let agent = agent.clone();
            let req = env.clone();
            std::thread::spawn(move || run_task(agent, req));
        }
        MessageType::TaskReply => {
            // a decision on a paused task
            if let Some(decision) = env.payload.get("decision").and_then(Value::as_str) {
                agent
                    .decisions
                    .lock()
                    .unwrap()
                    .insert(env.correlation_id.clone(), decision.to_string());
                let req = Envelope::new(
                    "resume",
                    env.correlation_id.clone(),
                    env.recipient.clone(),
                    env.sender.clone(),
                    MessageType::TaskRequest,
                    json!({"intent": "INVENTORY_OPTIMIZATION", "resumed": true}),
                );
                let agent2 = agent.clone();
                std::thread::spawn(move || run_task(agent2, req));
            }
        }
        MessageType::CancelRequest => {
            agent
                .cancelled
                .lock()
                .unwrap()
                .insert(env.correlation_id.clone());
            // exactly one acknowledgement, carrying status CANCELLED
            agent.reply(
                &env,
                json!({"status": "CANCELLED", "stage": "acknowledged"}),
            );
        }
        MessageType::TaskStream => {
            agent.send_transport_error("a client cannot stream TASK_STREAM to the engine");
        }
        MessageType::HumanIntervent => {
            agent.send_transport_error("HUMAN_INTERVENT is raised by the engine, not sent to it");
        }
    }
    true
}

/// The "heavy computation": stream progress, honour cancellation, reply.
fn run_task(agent: Arc<Agent>, req: Envelope) {
    let task = req.correlation_id.clone();
    let stages = [
        (25u32, "loading warehouse_402.parquet"),
        (50, "solving min-cost flow"),
        (75, "materialising plan"),
    ];
    for (progress, note) in stages {
        std::thread::sleep(Duration::from_millis(250));
        if agent.cancelled.lock().unwrap().contains(&task) {
            return; // the ack already went out from the reader thread
        }
        agent.stream(
            &req,
            json!({"progress": progress, "stage": note, "correlation": task}),
        );
    }
    agent.reply(
        &req,
        json!({
            "status": "OK",
            "intent": "INVENTORY_OPTIMIZATION",
            "result": {"projected_saving_pct": 12.4, "rebalance_units": 1830},
            "artifacts_ref": ["s3://enterprise-data/plan_402.parquet"],
        }),
    );
    let _ = GAIPError::Truncated; // keep the import honest for future use
}
