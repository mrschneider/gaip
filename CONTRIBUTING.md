# Contributing to GAIP

## The rules that actually matter

1. **A claim needs its proof.** A mechanism is not "done" until an
   experiment in `experiments/` exercises it against a real process (or,
   for the bindings, a real NATS server / real HTTP/2). "It compiles and
   I believe it" is not a state to commit.
2. **The contract has two layers, never fused.** Wire messages live in
   `message.rs`, operations in `envelope.rs`. A new transport touches
   neither; a new operation touches no transport.
3. **The vocabulary is closed.** Six primitives, full stop. A new
   operation is either one of them or it is an *intent* declared in the
   IDL, not a new primitive.
4. **Refusals must name the problem.** `unknown primitive 'AGENT_STEP' —
   the vocabulary is closed: ...` is the standard; "bad request" is not.
5. **Honesty in docs.** The README's "deliberately not implemented"
   section is a contract. Moving something out of it requires the test
   that proves it exists.

## Gates (all of them, every commit)

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
python3 experiments/experiment_001_giop_layer.py
python3 experiments/experiment_002_broker.py
python3 experiments/experiment_005_tls.py
python3 experiments/experiment_006_robustness.py
bash experiments/experiment_003_nats.sh     # needs docker
bash experiments/experiment_004_grpc.sh     # needs protoc (vendored)
```

CI runs the same list. A PR that turns one red is not ready.

## Protocol changes

* A new wire message is a **minor** version; changing an existing one is
  a **major**. Different majors are refused on the wire.
* A new service context id goes in `context::id` with a comment saying
  who writes it and who may read it.
* Lifecycle rules live in `TaskRegistry` — a new state transition must
  be refused everywhere else, not just unhandled.

## Commits

One commit per mechanism, message says what and why. English for code
and commits.
