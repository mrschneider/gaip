//! The client against a real engine process: failover, cursor resume,
//! credits.

use gaip_client::{Client, ClientInterceptor, Recorder};
use gaip_core::message::WireMessage;
use std::io::Read;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

fn identity_token(subject: &str, roles: &[&str]) -> String {
    use digest::KeyInit;
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    let expires = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
        + 120_000;
    let canonical = format!("v1|{}|{}||{}", subject, roles.join(","), expires);
    let mut mac = <Hmac<Sha256> as digest::KeyInit>::new_from_slice(b"dev-shared-secret").unwrap();
    mac.update(canonical.as_bytes());
    let sig = hex(mac.finalize().into_bytes().as_slice());
    json(&[
        ("subject", subject.into()),
        ("roles", serde_json::json!(roles)),
        ("on_behalf_of", serde_json::Value::Null),
        ("expires_at_ms", serde_json::json!(expires)),
        ("sig", serde_json::json!(sig)),
    ])
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

fn json(fields: &[(&str, serde_json::Value)]) -> String {
    let m: serde_json::Map<String, serde_json::Value> = fields
        .iter()
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect();
    serde_json::Value::Object(m).to_string()
}

fn spawn_engine(port: u16, log: &str) -> Child {
    let bin = concat!(env!("CARGO_MANIFEST_DIR"), "/../target/debug/gaip-engine");
    Command::new(bin)
        .args([
            "--port",
            &port.to_string(),
            "--key",
            "dev-shared-secret",
            "--name",
            "urn:agent:rust:tokio:analytics-engine-v1",
            "--idl",
            concat!(env!("CARGO_MANIFEST_DIR"), "/../schema/intents.idl"),
        ])
        .stdout(Stdio::from(std::fs::File::create(log).unwrap()))
        .stderr(Stdio::null())
        .spawn()
        .expect("engine binary built (cargo build --workspace)")
}

fn wait_port(port: u16) {
    for _ in 0..40 {
        if std::net::TcpStream::connect(("127.0.0.1", port)).is_ok() {
            return;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("engine on {port} never came up");
}

fn wait_down(port: u16) {
    for _ in 0..40 {
        if std::net::TcpStream::connect(("127.0.0.1", port)).is_err() {
            return;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

#[test]
fn a_dead_endpoint_fails_over_and_the_cursor_survives_the_reconnect() {
    let mut a = spawn_engine(7821, "/tmp/gaip_client_a.log");
    wait_port(7821);

    let mut client = Client::new(
        vec!["127.0.0.1:7821".to_string(), "127.0.0.1:7822".to_string()],
        2,
    )
    .with_identity(identity_token("agent:planner-v2", &["planner", "executor"]))
    .with_interceptor(std::sync::Arc::new(Recorder::default()));
    client.connect().expect("dial the primary");

    // FLOW CONTROL: the consumer grants credits before the producer
    // streams — the engine starts with two, which is a handshake, not a
    // licence to stream forever
    let g0 = client.grant_credits(8).expect("grant credits");
    if let WireMessage::Reply { body, status, .. } = &g0 {
        assert!(status.is_ok(), "grant status: {status:?}");
        let granted = body.as_ref().and_then(|e| e.payload.get("granted"));
        assert_eq!(
            granted,
            Some(&serde_json::json!(8)),
            "grant payload: {body:?}"
        );
    } else {
        panic!("expected a reply to the credit grant: {g0:?}");
    }

    // a real task so the engine publishes events
    let env = gaip_core::Envelope::task_request(
        "m1",
        "task_1",
        "urn:agent:gaip:client:self",
        "urn:agent:rust:tokio:analytics-engine-v1",
        serde_json::json!({"intent": "INVENTORY_OPTIMIZATION", "facility_id": 1,
                           "horizon_days": 1, "artifacts_ref": []}),
    );
    let x = client.call(env).expect("the call succeeds on the primary");
    let reply = match &x.reply {
        WireMessage::Reply { body, status, .. } => {
            assert!(status.is_ok());
            body.as_ref().expect("reply envelope").clone()
        }
        other => panic!("expected a reply, got {other:?}"),
    };
    let _ = reply;

    // pull events: the task_started event is there, the cursor advances
    let (events, _gap) = client.pull_events().unwrap_or_else(|e| {
        panic!("event pull failed: {e:?}");
    });
    assert!(!events.is_empty(), "at least the task_started event");
    let cursor = client.event_cursor;
    assert!(cursor > 0);

    // the primary dies — the same client walks the plan to the alternate
    let pid = a.id();
    let killed = a.kill().is_ok();
    eprintln!("[test] engine A pid={pid} killed={killed}");
    wait_down(7821);
    eprintln!("[test] port 7821 is down");
    let mut b = spawn_engine(7822, "/tmp/gaip_client_b.log");
    wait_port(7822);

    // credits: the moved server starts with two; grant more explicitly
    let g = client
        .grant_credits(4)
        .expect("grant credits after failover");
    if let WireMessage::Reply {
        body: Some(env),
        status,
        ..
    } = &g
    {
        assert!(status.is_ok());
        assert_eq!(env.payload.get("granted"), Some(&serde_json::json!(4)));
    } else {
        panic!("expected a reply to GRANT_CREDITS");
    }

    // another real task on the moved endpoint, then the cursor resumes:
    // no event is delivered twice
    let env2 = gaip_core::Envelope::task_request(
        "m2",
        "task_2",
        "urn:agent:gaip:client:self",
        "urn:agent:rust:tokio:analytics-engine-v1",
        serde_json::json!({"intent": "INVENTORY_OPTIMIZATION", "facility_id": 2,
                           "horizon_days": 1, "artifacts_ref": []}),
    );
    client.call(env2).expect("the call succeeds after failover");
    let (events2, _) = client.pull_events().expect("pull after failover");
    assert!(
        !events2.is_empty(),
        "the moved engine's task_started is visible through the same cursor"
    );
    let seqs: Vec<u64> = events2
        .iter()
        .filter_map(|e| e.get("seq"))
        .filter_map(|v| v.as_u64())
        .collect();
    assert!(
        seqs.iter().all(|s| *s > cursor),
        "no event is delivered twice across the reconnect: {seqs:?}"
    );
    assert!(
        client.failover_count() >= 1,
        "the failover was counted, got {}",
        client.failover_count()
    );

    let _ = b.kill();
}
