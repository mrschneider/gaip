# GAIP

**The wire protocol that lets agents from different runtimes talk without a translator.**

[![CI](https://github.com/mrschneider/gaip/actions/workflows/ci.yml/badge.svg)](https://github.com/mrschneider/gaip/actions/workflows/ci.yml)
[![Release](https://github.com/mrschneider/gaip/actions/workflows/release.yml/badge.svg)](https://github.com/mrschneider/gaip/actions/workflows/release.yml)
[![Crates.io-style version](https://img.shields.io/badge/version-0.5.2-blue.svg)](releases)
[![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

A Python/LangGraph planner, a Java Spring service and a Rust execution
engine speak different shapes of the same ideas. Integrating them is
translators — fragile, per-pair, drifting. GAIP gives them **one typed
envelope, six universal primitives and transports you can swap**.

It is CORBA's GIOP rebuilt for agents: the parts that made GIOP survive
production (a closed operation vocabulary, deadline and retry policies,
references that outlive crashes) without the parts that aged
(no CDR, no TypeCode/`Any`, no DII).

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
CAPABILITY_QUERY · HUMAN_INTERVENT` — that is the whole vocabulary. A
framework-specific verb is refused **with the word the peer sent**,
because a protocol that quietly accepts `on_tool_start` is a protocol
with one translator per framework pair.

## What you get

| | |
|---|---|
| **5 transports, one contract** | framed TCP, SSE, TLS 1.3, gRPC/HTTP/2, NATS — payloads never change with the wire |
| **Exactly-once across restarts** | the broker checkpoints its dedupe window on SIGTERM; a restarted exchange replays identically, no re-execution |
| **At-least-once, acknowledged** | the broker stamps `DELIVERED` on what it actually handed to a participant; dead letters are journaled with the reason |
| **References that survive crashes** | an IOR with failover endpoints; a Locate for a dead participant answers `OBJECT_FORWARD` with where it can still be reached |
| **Discovery that answers questions** | a Naming Service, a Trader with a real constraint language (`capability = "X" AND vram_free_mb > 30000`), and `corbaname:` |
| **Flow control on the wire** | stream credits per connection; a starved producer reports Timeout instead of racing its reader |
| **Locks with leases** | CosConcurrency-lite: a crashed holder cannot deadlock anyone |
| **Signed schema pushes** | IDL updates ride a verified CSIv2 identity — a poisoned declaration is a refusal |
| **Interceptors** | server- and client-side hooks for tracing, metrics and retry policy |
| **Backpressure** | bounded outboxes; a stalled peer gets a retryable refusal, never a frozen broker |
| **Nothing wasted on the hot path** | one serialization per message (decode validates in place); the broker's encode never waits on its global mutex |
| **A client that does the right thing** | `gaip-client`: failover driving a real socket, event cursor resuming across reconnects, CSIv2 on every call |

**Security**: HMAC identity assertions with roles and delegation (CSIv2),
TLS 1.3 on every plane, and a signed schema registry.

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
let x = c.call(task_request)?;      // fails over if this endpoint dies
let (events, gap) = c.pull_events()?; // "what did I miss", cursor-aware
```

## Proven, not promised

Every number below is produced by a script in this repo — CI re-runs
001–006 on every push and the release workflow re-runs the same gate
before packaging any binary (007, the checkpoint cycle, runs on demand).

| Check | Result |
|---|---|
| `cargo test --workspace` | 135 tests (unit + hostile input + client integration) |
| GIOP layer end-to-end | 28/28 |
| Federated brokers end-to-end | 9/9 |
| NATS (real nats-server) | PASS |
| gRPC (real HTTP/2) | PASS |
| TLS 1.3 end-to-end | 4/4 |
| Robustness set (failover, locks, events, stats) | 12/12 |
| Checkpoint cycle (restart + replay) | 3/3 |

## Repository

```
gaip-core/      the contract — no IO: envelope, primitives, wire layer,
                fragments, contexts, IDL+IR, naming/trader, CSIv2, locks,
                events, failover planning
gaip-engine/    reference participant: the six primitives, deadlines,
                callbacks, TLS, interceptors, locks, events
gaip-broker/    the relay: URN routing, federation, dedupe, checkpoint,
                failover registry, dead letters
gaip-client/    the caller half: failover, cursor, interceptors
gaip-bindings/  NATS, gRPC
schema/         the contract on disk: envelope JSON-Schema +
                intents IDL
experiments/    7 end-to-end proofs
```

## Honest scope

* **Kafka binding** — mapped, not written. A binding is not claimed
  before it is tested.
* **GSSAPI/Kerberos** — the token is the HMAC assertion; TLS is the
  transport half.
* **Broker authentication** — federation peers are trusted by address.
* **Exactly-once window** — 4096 entries / 120 s; beyond it a duplicate
  is a new message. Stated, not implied.
* **Locks and event channels are engine-local** — they do not span
  brokers.

## Version history

| Version | What landed |
|---|---|
| **v0.5.2** | performance round 2: read loops consume by offset (one
  compaction per read), envelope parse moves the payload, dedupe replies
  shared by Arc (no clone under the broker lock), compact SSE events,
  release profile with thin LTO |
| v0.5.1 | performance: decode validates in place (one serialization
  per message), IDL validation without cloning the payload, the broker
  encodes outside its global mutex |
| v0.5.0 | delivery stamps + dead letters, stream credits, signed schema push, deadline at the edge + negative cache, `gaip-client`, broker checkpoint |
| v0.4.0 | interceptors, transparent failover, `corbaname:`, locks with leases, event channel with QoS |
| v0.3.0 | federated broker, schema push, trader query language, NATS + gRPC + TLS bindings, backpressure |
| v0.2.0 | GIOP wire layer, service contexts, exceptions, IOR/naming/trader, IDL+IR, CSIv2 assertion |
| v0.1.0 | typed envelope, six primitives, lifecycle, framed TCP + SSE |

## License

Apache-2.0.
