# GAIP — Gaiko Agentic Interoperability Protocol

**A wire format, not a framework.** CORBA's GIOP for agents: one typed
envelope, six universal primitives, and bindings you can swap without
touching the payloads.

A Python LangGraph agent speaks dicts and pydantic models. A Java Spring
system speaks DTOs. A Rust execution engine wants zero-copy structs.
Every framework also invents its own event names — `on_tool_start`,
`agent_step`, `run_completion` — so every pair of frameworks needs a
translator, and every translator drifts.

GAIP imports the three ideas that actually made GIOP survive:

| GIOP / CORBA | GAIP |
|---|---|
| CDR: agreed byte layout so endianness stops mattering | **Versioned typed envelope** — semantic drift stops mattering |
| Eight message types, no invented verbs | **Six universal primitives**, everything else refused |
| GIOP is the spec, IIOP is a binding | **One payload contract**, many bindings |

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

* `correlation_id` is the task. A reply inherits it, so a reply is
  routable with **no session state** — that is the whole trick.
* `sender`/`recipient` are URNs carrying runtime and framework, because
  "which participant is that?" is the first question in an incident.
* Version policy: a different **major** is refused; minor is additive.
  A peer that guesses at a changed layout corrupts a task.
* Payloads stay typed. Big artifacts travel by reference, not inline.

## The GIOP layer: message types around the primitives

The six primitives are *operations*. GIOP's second lesson is that the
**transport message** is a separate thing from the operation it carries,
and that separation is what makes the rest work:

```
WireMessage
├── Request        { request_id, service_contexts, body: Envelope }
├── Reply          { request_id, status, body: Envelope? }
├── LocateRequest  { request_id, target }
├── LocateReply    { request_id, status: ObjectHere | ObjectForward(Ior) | UnknownObject }
├── CancelRequest  { request_id }        (transport abort, not the CANCEL primitive)
├── MessageError   { reason }
├── CloseConnection
└── Fragment       { request_id, fragment_num, more, data }
```

| Mechanism | What it buys an agent system |
|---|---|
| **Fragment** (GIOP 1.1 style) | a 3000-char context or a token burst travels as numbered fragments, reassembled byte-exactly; a gap or a reordered run is refused |
| **Service contexts** | `trace`, `tenant`, `deadline`, `identity`, `attempt`, `route` ride every message — a gateway can add a trace id to traffic it does not understand |
| **LocateRequest / OBJECT_FORWARD** | cheap liveness distinct from capability introspection, and relocation: an agent that moved hands back a reference instead of a failure |
| **Exception model** | `NO_EXCEPTION` / `USER_EXCEPTION` / `SYSTEM_EXCEPTION` with a `retryable` flag — a poisoned message is never retried forever |
| **MessageError / CloseConnection** | a refusal expressed *in* the protocol and a clean shutdown, instead of an ad-hoc `{"error": ...}` object |
| **IOR + corbaloc** | one portable reference: URN, failover endpoints, version, capabilities |
| **Naming Service** | `swarm/backend/engine-1` → reference, rebindable when the agent is replaced |
| **Trader** | discovery by attribute (`capability` + `vram_free_mb >= 32000` + runtime), not by name |
| **IDL + Interface Repository** | payloads declared per intent, validated before the work starts; an undeclared field is drift and is refused |
| **Bidirectional GIOP** | the server calls back over the client's own connection mid-task (NAT-friendly) |
| **Policies** | `sync_scope`, absolute deadlines that no hop may extend, fragment bounds |
| **CSIv2** | signed identity assertion with roles and delegation, verified before any work |

Deadlines are **absolute** on purpose: a queue in the middle cannot
silently extend the caller's budget.

## Six primitives — and nothing else

| Message type | Purpose |
|---|---|
| `TASK_REQUEST` | delegate a job with parameters and context |
| `TASK_REPLY` | final/incremental result, artifacts, or an error |
| `TASK_STREAM` | live tokens, logs, progress telemetry |
| `CANCEL_REQUEST` | abort a running execution or tool call |
| `CAPABILITY_QUERY` | introspect the remote agent (CORBA LocateRequest) |
| `HUMAN_INTERVENT` | pause and route the payload for human audit |

A framework-specific verb is **refused with the word named**:

```text
unknown primitive 'AGENT_STEP' — the vocabulary is closed: TASK_REQUEST,
TASK_REPLY, TASK_STREAM, CANCEL_REQUEST, CAPABILITY_QUERY, HUMAN_INTERVENT
```

That refusal is the feature. A protocol that quietly accepts
`agent_step` is a protocol with a translator per pair of frameworks.

## Lifecycle — the part a vocabulary alone cannot give you

```
          TASK_REQUEST
               |
               v
  +--------> Requested --TASK_STREAM--> Streaming --TASK_REPLY--> Replied
  |             |                          |
  |             +----HUMAN_INTERVENT-------+--> AwaitingHuman
  |             |                          |         |
  +--(any pre-terminal)----CANCEL_REQUEST--+--> Cancelled
```

Enforced by `TaskRegistry`, so an out-of-order message is refused with
its state named instead of corrupting a run:

* a reused `correlation_id` while the task is open → `TaskExists`
* `TASK_STREAM`/`TASK_REPLY` for an unopened task → `NoSuchTask`
* anything after a terminal state → `IllegalTransition`
* a **cancelled** task accepts exactly one reply, carrying
  `{"status": "CANCELLED"}` — that is the acknowledgement
* a **paused** task is *resolved*, not completed: a `TASK_REPLY` with a
  `decision` returns it to `Streaming`; the work then finishes normally

## Bindings

The payload contract is transport-agnostic; `gaip-core` is IO-free.

| Binding | State | Framing |
|---|---|---|
| **Framed TCP** (reference) | implemented + tested | `u32` big-endian length + JSON envelope |
| **SSE** (`text/event-stream`) | implemented (encoder/decoder + keep-alive) | `event: <PRIMITIVE>` + multi-line `data:` |
| **NATS** | implemented + tested against nats-server 2.10 | `WireMessage` JSON as the payload; `gaip.<primitive>.<correlation>` |
| **gRPC / HTTP/2** | implemented + tested over real HTTP/2 | unary `Send` = Request/Reply, bidi `Stream` = TASK_STREAM |
| **TLS** | implemented + tested (TLS 1.3) | same frames over a rustls-wrapped socket, `--tls-cert/--tls-key` |
| gRPC / HTTP/2 | planned — same envelope as the message body | unary `Send(Envelope) returns (Envelope)` + server-stream for `TASK_STREAM` |
| Kafka / NATS | planned — same envelope as the message value | key = `correlation_id` so a task's frames stay ordered on one partition |

The SSE binding follows the WHATWG rule that a `data:` line cannot contain
a raw newline, so a pretty envelope is emitted across several `data:`
lines. Keep-alive is a **comment** (`: keep-alive`), never an invented
event type — the vocabulary stays closed even in transport chatter.

## Quickstart

```bash
cargo build --release

# reference participant (Rust engine on 127.0.0.1:7610)
./target/release/gaip-engine --port 7610

# drive it
python3 experiments/experiment_001_primitives.py
```

A minimal client is ~20 lines in any language: write `u32` length +
JSON, read the same back. That boringness is intentional.

## Verified results

`python3 experiments/experiment_001_giop_layer.py` — a Python
orchestrator against the Rust engine over the real TCP binding, covering
all ten mechanisms:

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
EXPERIMENT_001: PASS
```

Unit tests: `cargo test --workspace` → **86 passed**: envelope round-trip
of the spec's example, URN and IOR/corbaloc parsing, closed-vocabulary
refusal, version policy, lifecycle (cancel-ack, human resume), message
fragmentation/reassembly (gaps, reorder, mixed runs refused), service
contexts (absolute deadlines, bounded route), IDL parsing and IR
validation (drift, types, optional fields), naming, trader queries,
policies and CSIv2 (tamper, forged role, expiry, delegation).

## Layout

```
gaip-core/      the contract (no IO at all): envelope, primitives,
                lifecycle, codec, SSE, message layer, contexts, IOR,
                naming, trader, IDL+IR, policies, CSIv2
gaip-engine/    reference participant: a Rust engine speaking the six
                primitives over the framed TCP binding
schema/         gaip-envelope.schema.json (wire contract) and
                intents.idl (payload declarations served by the IR)
experiments/    runnable end-to-end proof (a Python peer on purpose:
                the point is cross-runtime interop)
```

## Deliberately not implemented (v0.1)

* **gRPC and Kafka/NATS bindings** — the contract and the mapping are
  documented; only framed TCP and SSE exist as code. A binding must not
  be claimed before it is tested.
* **Authentication and authorization** — URNs identify, they do not
  authenticate. Anyone can claim any URN; deploy behind mTLS.
* **Relay / broker** — participants address each other directly. There is
  no message broker, no discovery service, no retry.
* **Exactly-once delivery** — `message_id` exists for deduplication but
  the reference participant does not dedupe; at-least-once at best.
* **Dynamic schema distribution** — the IR is served over
  `CAPABILITY_QUERY` and validated locally; there is no registry that
  pushes new intents to peers.
* **CSIv2 over TLS — done, with one caveat** — the engine serves TLS 1.3
  (`--tls-cert/--tls-key`), verified by `experiment_005_tls.py`; GSSAPI
  (Kerberos) is still not implemented, so the token type is the identity
  assertion, not a GSS token.
* **Trader query language** — constraints are a conjunction of typed
  predicates (capability, runtime, version prefix, numeric minimum, text
  equality), not an expression language.
* **Multi-broker routing** — `OBJECT_FORWARD` hands back a reference;
  following it across brokers is the caller's job.
* **Kafka binding** — not implemented. The other two bindings prove the
  pattern; this one needs a broker container and `rdkafka`.

## License

Apache-2.0.
