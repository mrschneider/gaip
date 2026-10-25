# GAIP — Gaiko Agentic Interoperability Protocol

**A wire format, not a framework.** CORBA's GIOP applied to agents: one
typed envelope, six universal primitives, and transports you can swap
without touching the payloads.

A Python LangGraph agent speaks dicts and pydantic models. A Java Spring
system speaks DTOs. A Rust execution engine wants zero-copy structs.
Every framework also invents its own event names — `on_tool_start`,
`agent_step`, `run_completion` — so every pair of runtimes needs a
translator, and every translator drifts.

GAIP imports the three ideas that made GIOP survive:

| GIOP / CORBA | GAIP |
|---|---|
| CDR: agreed byte layout so endianness stops mattering | **Versioned typed envelope** — semantic drift stops mattering |
| Eight message types, no invented verbs | **Six universal primitives**, everything else refused |
| GIOP is the spec, IIOP is a binding | **One payload contract**, five tested transports |

---

## Two layers, never fused

```
WireMessage (transport)            Envelope (operation)
├── Request  ─────────────────►   TASK_REQUEST / TASK_REPLY / TASK_STREAM
├── Reply                        CANCEL_REQUEST / CAPABILITY_QUERY
├── LocateRequest/LocateReply     HUMAN_INTERVENT
├── CancelRequest
├── MessageError
├── CloseConnection
├── Fragment
└── SchemaSync
```

The operation layer is what an agent author thinks about. The wire layer
is what a transport routes, fragments, retries and fails. GIOP kept them
apart and that is why a message could survive a different binding; GAIP
does the same, and `gaip-core` has **no IO at all**.

## The envelope

The only thing participants share (`schema/gaip-envelope.schema.json` is
the normative JSON-Schema):

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
    "parameters": {"facility_id": 402, "horizon_days": 30},
    "artifacts_ref": ["s3://enterprise-data/warehouse_402.parquet"]
  }
}
```

* `correlation_id` is the task; a reply inherits it, so replies are
  routable with **no session state**.
* `sender`/`recipient` are URNs (`urn:agent:<runtime>:<framework>:<name>[:<version>]`)
  — the runtime and framework are in the address on purpose.
* Version policy: a different **major** is refused, minor is additive.
* `payload` is validated against the IDL declaration for its intent
  before any work starts.
* Big artifacts travel by reference, not inline.

## Six primitives — and nothing else

| Message type | Purpose |
|---|---|
| `TASK_REQUEST` | delegate a job with parameters and context |
| `TASK_REPLY` | final/incremental result, artifacts, or an error |
| `TASK_STREAM` | live tokens, logs, progress telemetry |
| `CANCEL_REQUEST` | abort a running execution or tool call |
| `CAPABILITY_QUERY` | introspect the remote agent (CORBA LocateRequest's job, split from liveness) |
| `HUMAN_INTERVENT` | pause and route the payload for human audit |

A framework-specific verb is refused **with the word the peer sent**:

```text
unknown primitive 'AGENT_STEP' — the vocabulary is closed:
TASK_REQUEST, TASK_REPLY, TASK_STREAM, CANCEL_REQUEST,
CAPABILITY_QUERY, HUMAN_INTERVENT
```

That refusal is the feature. A protocol that quietly accepts
`agent_step` is a protocol with one translator per framework pair.

## The wire layer

| Message | What it buys |
|---|---|
| `Request` / `Reply` | the operation plus its outcome; `Reply` carries `NO_EXCEPTION`, a `USER_EXCEPTION` (business refusal) or a `SYSTEM_EXCEPTION` with a **retryable** flag |
| `Fragment` | GIOP 1.1 style numbered runs with a `more` flag; gaps, reordering and mixed runs are refused; reassembly is byte-exact; a message that fits returns as one element |
| `LocateRequest` / `LocateReply` | cheap liveness **and relocation**: `ObjectHere`, `ObjectForward(Ior)`, `UnknownObject` |
| `CancelRequest` | transport-level abort of an in-flight request — distinct from the `CANCEL_REQUEST` primitive, which asks the agent to stop working |
| `MessageError` | a refusal expressed *in* the protocol, for a frame that cannot be parsed |
| `CloseConnection` | clean shutdown, no reply expected |
| `SchemaSync` | pushes IDL declarations to peers; distributing the contract is transport, not an operation |
| `Fragment` + ROUTE | a bundle's hop path is bounded, so a loop is visible in the header |

### Service contexts

`trace`, `tenant`, `deadline` (absolute — a hop cannot extend the
caller's budget), `identity`, `attempt`, `route` ride **every** message.
A gateway can add a trace id to traffic it does not understand, which is
the difference between a protocol and a payload convention. The route is
bounded to 16 hops.

### Policies

`sync_scope`, a default budget, deadlines that are never extended, and
fragment bounds. An expired deadline refuses the work with a **retryable
timeout** instead of starting it late.

### Exceptions

`USER_EXCEPTION` = the same input will fail the same way (not retryable).
`SYSTEM_EXCEPTION` = infrastructure: `Timeout` and `Unavailable` are
retryable, `BadParam`, `NoPermission`, `ObjectNotExist`, `ProtocolError`
are not. An orchestrator that cannot tell "try again" from "this will
never work" either retries a poison message forever or gives up on a
blip.

---

## Discovery and addressing

| Mechanism | What it does |
|---|---|
| **IOR + `corbaloc`** | one portable reference: URN, failover endpoints, version, capabilities. `IOR:urn=…;v=1.0;ep=host:port,host2:port;cap=…` and the short `corbaloc:gaip@host:port/urn` |
| **Naming Service** | hierarchical bindings (`swarm/backend/engine-1` → reference) with an explicit `rebind`, prefix listing and failover across names |
| **Trader** | discovery by attribute with a real constraint language |
| **Relay broker** | URN routing across participants and brokers (below) |

The trader query language is parsed, not a pile of builders:

```text
capability = "INVENTORY_OPTIMIZATION"
  AND vram_free_mb > 30000
  AND (runtime = "rust" OR runtime = "cuda")
  AND NOT version < 1
```

`NOT > AND > OR`, parentheses, `= != > < >= <=`, `CONTAINS` (substring on
text, membership on `capability`), typed literals, and parse errors that
name the problem (`'>' and '<' compare numbers only`). Attributes resolve
from the reference first (`runtime`, `framework`, `name`, `version`,
`urn`) and then from the offer.

## The relay broker

`gaip-broker` is what makes a URN reachable without a hardcoded graph.

* **Relay** — a participant registers by using its URN as the `sender` of
  its first Request; a Request for a local URN is forwarded over that
  connection.
* **Federated discovery** — an unknown target is broadcast to peer
  brokers; the one that has it delivers, the others answer
  `ObjectNotExist` and the origin ignores them. Broadcast is deliberate: a
  routing protocol with a *wrong* metric is worse than a broadcast.
* **Retry** — "not connected yet" is not "does not exist": retried with
  backoff before `ObjectNotExist`.
* **Exactly-once effect** — the reply is remembered against the
  `(connection, request_id)` the broker handed out, keyed by
  `(sender, message_id)`; a retried request replays the reply instead of
  running the work twice. A retried *refusal* stays a refusal.
* **Loop safety** — each hop appends its URN to the ROUTE context and
  refuses with `ProtocolError` when it finds itself already there.
* **Backpressure** — one bounded `Outbox` per connection; a stalled peer
  gets a retryable `Unavailable` instead of blocking the broker.

## Transport bindings

The payload contract does not change with the transport: a binding moves
`WireMessage` bytes and nothing else.

| Binding | State | Framing |
|---|---|---|
| **Framed TCP** (reference) | implemented, tested | `u32` big-endian length + JSON |
| **TLS** | implemented, **tested (TLS 1.3)** | same frames over rustls; `--tls-cert/--tls-key` |
| **gRPC / HTTP/2** | implemented, **tested over real HTTP/2** | unary `Send` = Request/Reply; bidi `Stream` = TASK_STREAM |
| **NATS** | implemented, **tested against nats-server 2.10** | `WireMessage` JSON as the payload; `gaip.<primitive>.<correlation>` |
| **SSE** (`text/event-stream`) | implemented (encoder/decoder + keep-alive) | `event: <PRIMITIVE>` + multi-line `data:` |
| **Kafka** | **not implemented** | mapped only: envelope as the message value, key = `correlation_id` |

A gRPC client and a TCP client interoperate because the bytes are the
same bytes — that is the whole point of a binding.

## Security

Two halves, both present:

* **Identity assertion (CSIv2 token)** — subject, roles, `on_behalf_of`
  delegation, absolute expiry, HMAC-SHA256 over a canonical string,
  constant-time comparison, role authorization returning a
  `USER_EXCEPTION`. `TASK_REQUEST` requires a signed identity with the
  `executor` role; a tampered role, a wrong key and an expired token are
  all refused.
* **TLS transport** — the engine serves TLS 1.3; a plaintext client gets
  no GAIP reply (no downgrade). The assertion proves *who*, TLS proves
  *with whom*.

## Quickstart

```bash
cargo build --release

# reference participant (Rust engine)
./target/release/gaip-engine --port 7610 --idl schema/intents.idl --key dev-shared-secret

# with TLS (openssl req -x509 -newkey rsa:2048 -nodes -keyout key.pem -out cert.pem -days 1 -subj /CN=localhost)
./target/release/gaip-engine --port 7615 --tls-cert cert.pem --tls-key key.pem

# relay broker (add --peer host:port to federate)
./target/release/gaip-broker --port 7620 --urn urn:agent:gaip:broker:b1

# NATS binding self-test against a real bus
docker run -d --rm -p 4222:4222 nats:2.10-alpine
./target/release/gaip-nats-selftest nats://127.0.0.1:4222
```

## Verified results

Every claim below is produced by a script in this repo, run in this
environment. Numbers are the actual output.

**`cargo test --workspace` → 130 unit tests.** They cover the envelope
(round-trip of the spec's own example), URN and IOR/corbaloc parsing, the
closed vocabulary, version policy, lifecycle (cancel acknowledgement,
human resume), fragmentation and reassembly (gaps, reorder, mixed runs),
service contexts (absolute deadlines, bounded route), IDL parsing and IR
validation (drift, types, optional fields), naming, the trader language,
policies, CSIv2 (tamper, forged role, expiry, delegation), dedupe,
backpressure, schema merge, and the gRPC/NATS codecs.

**`experiments/experiment_001_giop_layer.py` → 28/28 PASS** — a Python
orchestrator against the Rust engine over real TCP:

```
PASS: contexts: the trace id survives the hop and is echoed back
PASS: contexts: tenant and attempt reach the engine too
PASS: contexts: the engine appended itself to the route
PASS: idl: the capability reply publishes the intent schemas from the IR
PASS: idl: field types come from the declaration, not from guessing
PASS: ior: the peer hands back a parseable reference
PASS: naming: a name resolves to the reference without hardcoding an endpoint
PASS: naming: the corbaloc short form addresses the same reference
PASS: locate: the object is here
PASS: locate: a relocated agent answers OBJECT_FORWARD with a reference
PASS: locate: an unknown agent answers UnknownObject
PASS: exceptions: an undeclared payload field is a USER exception naming the drift
PASS: exceptions: a user exception is not retryable
PASS: policies: an expired deadline refuses the work instead of starting it late
PASS: policies: the timeout is marked retryable (the caller knows to retry)
PASS: fragment: the oversized request was fragmented and reassembled by the engine
PASS: lifecycle: three stream replies carried progress
PASS: fragment: the oversized reply came back whole (reassembled by us)
PASS: bidirectional: the engine asked US for context mid-task and used the answer
PASS: bidirectional: the callback carried a correlation of its own
PASS: contexts: the route shows the engine hop on the task reply
PASS: csi2: a correctly signed identity with the executor role is accepted
PASS: csi2: a TASK_REQUEST without an identity is refused
PASS: csi2: a signed identity without the required role is refused
PASS: csi2: a tampered role breaks the signature and is refused
PASS: csi2: an expired identity token is refused
PASS: exceptions: a malformed frame is answered with a MessageError in-protocol
PASS: exceptions: CloseConnection ends the session without a reply
28 passed, 0 failed
```

**`experiments/experiment_002_broker.py` → 9/9 PASS** — two federated
brokers, a participant on one and an orchestrator on the other:

```
PASS: federation: a request reaches a participant on a peer broker
PASS: federation: the reply comes back to the requester
PASS: relay: the broker appended its hop to the route and the reply survived it
PASS: exactly-once: a retried message_id replays the cached reply
PASS: exactly-once: the work did not run twice
PASS: relay: a new message_id is a new execution
PASS: retry: an unreachable target ends in a retryable ObjectNotExist
PASS: retry: after the participant appears, the same target resolves
PASS: loop safety: a request whose route already contains the broker is refused
9 passed, 0 failed
```

**`experiments/experiment_003_nats.sh` → PASS** — real container
(`nats:2.10-alpine`, nats-server v2.10.29):

```
published 299 bytes on gaip.task.request
received on gaip.task.request: Some(7)
PASS: a GAIP Request crossed a real NATS subject intact
```

**`experiments/experiment_004_grpc.sh` → 3/3 PASS** — real HTTP/2:

```
test grpc::tests::unary_send_carries_a_gaip_request_and_returns_a_reply ... ok
test grpc::tests::bidi_stream_answers_every_frame_on_the_same_stream ... ok
test grpc::tests::a_frame_that_is_not_a_gaip_message_is_refused_with_invalid_argument ... ok
```

**`experiments/experiment_006_robustness.py` → 12/12 PASS**:

```
PASS: failover: OBJECT_FORWARD carries the advertised alternate
PASS: persistence: the failover registry survives a broker restart
PASS: failover: the moved reference serves the same URN
PASS: locks: the first holder acquires
PASS: locks: a second writer is refused with E_LOCKED
PASS: locks: the holder releases
PASS: locks: after release the next acquirer wins
PASS: locks: a short lease is granted
PASS: locks: an expired lease is released implicitly
PASS: events: run telemetry is pullable with a cursor
PASS: events: a cursor is not rewound (nothing twice)
PASS: stats: the surface reports counts, not guesses
```

**`experiments/experiment_005_tls.py` → 4/4 PASS**:

```
PASS: tls: the handshake completes against the engine's certificate
   negotiated: TLSv1.3 / TLS_AES_256_GCM_SHA384
PASS: tls: a GAIP Request is answered over the encrypted channel
PASS: tls: the reply carries the same typed payload as over plain TCP
PASS: tls: a plaintext client gets no GAIP reply (no downgrade)
```

## Layout

```
gaip-core/       the contract — no IO at all
                 envelope, primitives, lifecycle, codec, SSE, message layer,
                 service contexts, IOR/corbaloc, naming, trader (+ query
                 language), IDL+IR, schema sync, policies, CSIv2, dedupe,
                 flow control
gaip-engine/     reference participant: the six primitives, IR validation,
                 bidirectional callbacks, deadlines, TLS, Outbox writes,
                 interceptors (recorder + stats surface), locks with
                 leases, event channel, operational intents
                 (LOCK_ACQUIRE/LOCK_RELEASE/EVENT_PULL)
gaip-core/…/     failover (the messaging plan), interceptor hooks, locks,
                 events — the CORBA-derived robustness modules
gaip-broker/     relay: URN routing, federated discovery, retry,
                 exactly-once effect, loop safety, backpressure
gaip-bindings/   NATS, gRPC (proto/gaip.proto), and the NATS self-test binary
schema/          gaip-envelope.schema.json (wire contract), intents.idl
experiments/     5 runnable proofs (Python peers on purpose: the point is
                 cross-runtime interop)
```

## Deliberately not implemented

Honest scope, so nothing here is mistaken for done:

* **Kafka binding** — mapped and documented (envelope as the message
  value, `correlation_id` as the key so a task stays ordered on one
  partition) but **not written and not tested**. A binding is not claimed
  before it is tested.
* **GSSAPI / Kerberos** — the CSIv2 token type here is the HMAC identity
  assertion, not a GSS token. TLS is the transport half.
* **Dynamic trust between brokers** — peers are configured and trusted by
  address; there is no broker authentication handshake.
* **Trader over the network** — the query language and the offer table
  are library-level; the broker does not expose a query endpoint.
* **Per-intent payload schema registry service** — the IR is local and
  fed by `SchemaSync`; there is no central registry with history.
* **Exactly-once only within the dedupe window** — 4096 entries / 120 s in
  the broker; beyond that a duplicate is a new message. The window is
  stated, not implied.
* **Client-side interceptors** — the interceptor ring is server-side;
  there is no real client crate yet (the experiments are Python).
* **Failover is reference-level, not connection-level** — the broker
  hands the caller a forwarded reference and the caller dials the
  alternate; the broker does not proxy the stream itself.
* **The lock and event surfaces are engine-local** — locks with leases
  and the event channel are per process; they do not cross brokers.

## Version history

| Version | What landed |
|---|---|
| **v0.4.0** | CORBA-derived robustness: portable interceptors + status surface, transparent failover (OBJECT_FORWARD with persisted registry), persistent references, locks with leases (CosConcurrency-lite), event channel with QoS (CosNotification-lite), corbaname |
| **v0.3.0** | broker (federation, retry, dedupe, loop safety), schema push, trader query language, backpressure, NATS + gRPC + TLS bindings |
| v0.2.0 | GIOP wire layer: fragments, service contexts, locate/OBJECT_FORWARD, typed exceptions, IOR/naming/trader, IDL+IR, bidirectional calls, policies, CSIv2 assertion |
| v0.1.0 | typed envelope, six primitives, lifecycle, framed TCP + SSE |

## License

Apache-2.0.
