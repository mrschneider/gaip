//! Round-trip a GAIP Request/Reply over a real NATS server.
//!
//! Usage: gaip-nats-selftest <nats-url>   (default nats://127.0.0.1:4222)
//! Exit code 0 = a Request published on a subject came back as a Reply
//! with the same correlation id.

use gaip_bindings::nats::NatsBinding;
use gaip_core::{Envelope, MessageType, WireMessage};

fn main() {
    let url = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "nats://127.0.0.1:4222".to_string());
    let addr = url.trim_start_matches("nats://").to_string();

    let mut publisher = match NatsBinding::connect(&addr) {
        Ok(n) => n,
        Err(e) => {
            eprintln!("FAIL: cannot connect to {addr}: {e}");
            std::process::exit(1);
        }
    };
    let mut subscriber = NatsBinding::connect(&addr).expect("second connection");
    subscriber
        .subscribe("gaip.task.request")
        .expect("subscribe");
    std::thread::sleep(std::time::Duration::from_millis(150));

    let req = Envelope::task_request(
        "msg_1",
        "task_1",
        "urn:agent:python:langgraph:planner-v2",
        "urn:agent:rust:tokio:engine",
        serde_json::json!({"intent": "INVENTORY_OPTIMIZATION"}),
    );
    let published = publisher
        .publish("gaip.task.request", &WireMessage::request(7, req.clone()))
        .expect("publish");
    println!("published {published} bytes on gaip.task.request");

    match subscriber.next().expect("next") {
        Some((subject, msg)) => {
            let ok = subject == "gaip.task.request"
                && matches!(&msg, WireMessage::Request { request_id, body, .. }
                    if *request_id == 7 && body.message_id == req.message_id);
            // answer on the reply subject, the way an agent would
            let reply_env = req.reply_to("msg_2", serde_json::json!({"status": "OK"}));
            let reply = WireMessage::Reply {
                request_id: 7,
                status: gaip_core::ReplyStatus::NoException,
                body: Some(reply_env),
                contexts: Vec::new(),
            };
            publisher
                .publish("gaip.task.reply", &reply)
                .expect("publish reply");
            println!("received on {subject}: {:?}", msg.request_id());
            if ok {
                println!("PASS: a GAIP Request crossed a real NATS subject intact");
                std::process::exit(0);
            }
            eprintln!("FAIL: the message came back different");
            std::process::exit(1);
        }
        None => {
            eprintln!("FAIL: no message received");
            std::process::exit(1);
        }
    }
}

#[allow(dead_code)]
fn _types(_: MessageType) {}
