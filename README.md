# GAIP

**The wire protocol that lets agents from different runtimes talk without a translator.**

[![CI](https://github.com/mrschneider/gaip/actions/workflows/ci.yml/badge.svg)](https://github.com/mrschneider/gaip/actions/workflows/ci.yml)
[![Release](https://github.com/mrschneider/gaip/actions/workflows/release.yml/badge.svg)](https://github.com/mrschneider/gaip/actions/workflows/release.yml)
[![Version](https://img.shields.io/badge/version-0.5.13-blue.svg)](releases)
[![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

A Python/LangGraph planner, a Java Spring service and a Rust execution
engine speak different shapes of the same ideas. Integrating them today
means translators — fragile, per-pair, drifting. GAIP gives them **one
typed envelope, six universal primitives and transports you can swap**.

It is CORBA's GIOP rebuilt for agents: the parts that made GIOP survive
production (a closed operation vocabulary, deadline and retry policies,
references that outlive crashes) without the parts that aged (no CDR,
no TypeCode/`Any`, no DII).

## One envelope. Six primitives. Zero translators.

```json
{
  "protocol_version": "1.0",
  "message_id": "msg_987654321",
  "correlation_id": "task_123456789",
  "sender": "urn:agent:python:langgraph:planner-v2",
  "recipient": "urn:agent:rust:tokio:analytics-engine-v1",
  "message_type": "TASK_REQUEST",
  "payload": {
    "intent": "INVENTORY_OPTIMIZATION",
    "parameters": {"facility_id": 402, "horizon_days": 30}
  }
}
```

`TASK_REQUEST · TASK_REPLY · TASK_STREAM · CANCEL_REQUEST ·
CAPABILITY_QUERY · HUMAN_INTERVENT`

That is the whole vocabulary. A framework-specific verb is refused
**with the word the peer sent** — a protocol that quietly accepts
`on_tool_start` is a protocol with one translator per framework pair.

## What you get

| Capability | What it means for you |
|---|---|
| **5 transports, one contract** | TCP, SSE, TLS 1.3, gRPC/HTTP-2, NATS — payloads never change with the wire |
| **Exactly-once across restarts** | The broker checkpoints its dedupe window on SIGTERM; a restarted exchange replays identically, no re-execution |
| **Crash-proof references** | An IOR with failover endpoints; a Locate for a dead participant answers `OBJECT_FORWARD` with where it can still be reached |
| **Rich discovery** | Naming Service + Trader with a real constraint language (`capability = "X" AND vram_free_mb > 30000`) |
| **Flow control on the wire** | Per-connection stream credits; a starved producer reports Timeout instead of racing its reader |
| **Signed schema pushes** | IDL updates ride a verified CSIv2 identity — a poisoned declaration is a refusal |
| **Backpressure** | Bounded outboxes; a stalled peer gets a retryable refusal, never a frozen broker |
| **Security** | HMAC identity assertions with roles and delegation (CSIv2), TLS 1.3 on every plane, signed schema registry |
| **A client that does the right thing** | Failover driving a real socket, event cursor resuming across reconnects, interceptors for tracing and retry |

## Quickstart

```bash
cargo build --release

# reference participant
./target/release/gaip-engine --port 7610 --idl schema/intents.idl --key dev-shared-secret

# relay broker with a persistent checkpoint
./target/release/gaip-broker --port 7620 --urn urn:agent:gaip:broker:b1 --state broker.json

# TLS in one line
openssl req -x509 -newkey rsa:2048 -nodes -keyout key.pem -out cert.pem -days 1 -subj /CN=localhost
./target/release/gaip-engine --port 7615 --tls-cert cert.pem --tls-key key.pem

# federation: point a spoke at the hub
./target/release/gaip-broker --port 7621 --peer 127.0.0.1:7620
```

A client is one crate away:

```rust
let mut c = Client::new(vec!["127.0.0.1:7610".into()], 2)
    .with_identity(signed_token)
    .with_interceptor(Arc::new(Recorder::default()));
let x = c.call(task_request)?;       // fails over if this endpoint dies
let (events, gap) = c.pull_events()?; // "what did I miss", cursor-aware
```

## Proven in CI

Every claim above is backed by a script in this repo. CI re-runs the
full gate on every push and before packaging any release binary.

| Check | Result |
|---|---|
| Unit + integration tests | 154 |
| GIOP layer end-to-end | 28/28 |
| Federated brokers end-to-end | 9/9 |
| TLS 1.3 end-to-end | 4/4 |
| NATS (real nats-server) + gRPC (real HTTP/2) | PASS |
| Robustness (failover, locks, events, stats) | 12/12 |
| Checkpoint cycle (restart + replay) | 3/3 |

## Status

| Area | State |
|---|---|
| Core protocol, broker, engine, client | **Stable** |
| TLS, NATS, gRPC bindings | **Stable** |
| Kafka binding | In progress |
| GSSAPI/Kerberos | Not planned (HMAC + TLS covers the use case) |
| Broker peer authentication | Address-trusted (federation peers) |

## Repository

```
gaip-core/       the contract (no IO)
gaip-engine/     reference participant
gaip-broker/     the relay
gaip-client/     the caller half
gaip-bindings/   NATS, gRPC
schema/          envelope JSON-Schema + intents IDL
experiments/     7 end-to-end proofs
```

## Version history

See [GitHub Releases](https://github.com/mrschneider/gaip/releases) for
the full changelog.

## License

Apache-2.0.
