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

`python3 experiments/experiment_001_primitives.py` — a Python orchestrator
against the Rust engine over the real TCP binding:

```
PASS: introspection: CAPABILITY_QUERY is answered with a typed TASK_REPLY
PASS: introspection: the answer is correlated to the query
PASS: lifecycle: TASK_REQUEST produces three TASK_STREAM frames
PASS: lifecycle: then exactly one TASK_REPLY
PASS: typing: every frame carries the same correlation_id
PASS: typing: the reply carries the typed result, not prose
PASS: addressing: the reply is sent back to the requester URN
PASS: cancel: the task starts streaming before we abort it
PASS: cancel: CANCEL_REQUEST is acknowledged with status CANCELLED
PASS: cancel: no further frames arrive for the aborted task
PASS: human: a risky task raises HUMAN_INTERVENT before computing
PASS: human: the intervention names what is being approved
PASS: human: the task does not run while it waits
PASS: human: the decision resumes the work (streams continue)
PASS: human: and the task then completes normally
PASS: drift: a framework-specific verb is refused, naming the word
PASS: drift: the refusal lists the six legal primitives
PASS: version: protocol_version 2.0 is refused
PASS: version: a 1.x minor version is accepted (additive by contract)
PASS: addressing: a message addressed to another participant is refused

20 passed, 0 failed
EXPERIMENT_001: PASS
```

Unit tests: `cargo test --workspace` → **34 passed** (envelope round-trip
of the spec's own example, URN parsing, closed-vocabulary refusal,
version policy, lifecycle transitions incl. cancel-ack and human resume,
frame codec with partial/hostile buffers, SSE encode/decode).

## Layout

```
gaip-core/      the contract: envelope, primitives, lifecycle, codec, SSE
                (no IO at all)
gaip-engine/    reference participant: a Rust engine speaking the six
                primitives over the framed TCP binding
schema/         gaip-envelope.schema.json — the normative wire contract
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
* **Payload schema registry** — `payload` is `object`; per-intent
  schemas (`INVENTORY_OPTIMIZATION` etc.) are not published yet.
* **Backpressure** — a slow peer blocks its writer; no flow control.

## License

Apache-2.0.
