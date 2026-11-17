# Performance Analysis & Optimization — gaip dedupe cache

**Date:** 2026-11-17
**Target:** `/home/guilherme/apps/gaip` — `gaip-core` dedupe lookup hot path
**Author:** Cyber-Tiel-Coder (variant of Ornith-1.5-35B-A3B)

## 1. Objective

Analyze the performance of the gaip codebase's hot path (the dedupe
exactly-once check that runs under the broker's read lock on every relayed
request) and implement a concrete optimization.

## 2. Findings — the codebase was already well-architected

Before optimizing, I audited the existing hot path. It was already highly
optimized; the remaining waste was narrow and isolated:

| Optimization | Status before |
|---|---|
| View-based codec (`encode_request_view`/`encode_reply_view`) | ✅ Relayed request costs 1512ns vs 2758ns for full decode+encode — no re-serialization, body bytes carried verbatim |
| Raw-byte dedupe (`remember_raw`) | ✅ Stores wire bytes as `String`, no parse on store (~0ns store path) |
| Per-connection `Arc<Conn>` outboxes + `send_bytes` sharing one `Arc<[u8]>` | ✅ No per-connection copy |
| Single read-lock critical sections in broker | ✅ |
| Global `dedupe` Mutex | ✅ Degrades only ~1.9x at 8 threads — small, well-isolated critical section |

### The one real hot-path waste

`DedupeCache::lookup_read` built a combined `String` key (`sender \u{1}
message_id`) on **every probe — hit and miss —** costing ~60ns/op of pure
allocation on the broker's read lock. The stored key was already a `String`
(`sender \u{1} message_id`), so the allocation was redundant: the probe only
needed to *compare* against it, not *rebuild* it.

Measured baseline (before fix):

```
[lookup_read, HIT]  ~152 ns/op
```

## 3. The fix — zero-copy composite lookup key

Added a `CompositeKey<'a>` value type in `gaip-core/src/dedupe.rs` that hashes
and compares the **identical byte sequence** a stored `String` hashes, without
ever allocating:

- `Hash` folds `sender` → separator → `message_id` (same order/bytes as
  `DedupeCache::key`).
- `Equivalent<String>` compares against the stored key byte-for-byte by
  splitting on the separator.
- The stored key **stays a `String`** — `snapshot`/`restore`/FIFO `order`
  invariants are untouched. Only the probe avoids building the combined key.

### The one thing that had to change: `Borrow` vs `Equivalent`

The first attempt failed to compile. std's `HashMap::get` requires the query
type to implement `Borrow<K>` (the *stored key* type) — impossible for a
borrowed `CompositeKey`. The correct fix is to use hashbrown's `Equivalent`
trait directly, which is what hashbrown's `HashMap::get` actually requires.

Two changes:

1. Added `hashbrown = "0.17"` as a direct dependency of `gaip-core` (matching
   std's internal hashbrown version 0.17.1).
2. Switched `DedupeCache::entries` from `std::collections::HashMap` to
   `hashbrown::HashMap`, which uses `Equivalent` for its `get` contract.

`CompositeKey` derives `PartialEq`/`Eq` (Q=K comparison) and implements
`hashbrown::Equivalent<String>` (the actual byte comparison against the stored
`String`).

## 4. Verification

### Tests — all pass

```
dedupe::tests::10 passed
gaip-core full suite: 147 passed, 0 failed
doc-tests: 1 passed
```

Existing dedupe tests cover the exact invariants the fix touches:
replay, per-sender keying, bounded eviction, lazy TTL eviction, expiry,
refusal replay, raw-entry materialization, and checkpoint round-trip.

### Broker builds clean

```
cargo build --release -p gaip-broker  →  Finished (release)
```

### Benchmarks — the gain is real

```
[lookup_read, HIT]  98.4 ns/op   (was ~152 ns/op — ~46ns / ~30% faster)
[lookup_read, MISS] 95.9 ns/op
```

Supporting benchmarks (unchanged behavior, sanity checks):

```
[HIT, body stored raw -> re-parses on lookup] 101.8 ns/op
[HIT, parsed entry -> Arc clone only]          67.3 ns/op
[combined-key String alloc alone]              69.7 ns/op  <-- what we eliminated
```

The eliminated allocation (~69.7ns for a ~60B key) matches the measured
improvement (~46ns net after hashbrown's `Equivalent` probe cost).

## 5. Files changed

| File | Change |
|---|---|
| `gaip-core/Cargo.toml` | Added `hashbrown = "0.17"` direct dependency |
| `gaip-core/src/dedupe.rs` | Added `CompositeKey<'a>` (zero-copy lookup key); switched `entries` to `hashbrown::HashMap`; `lookup_read` routes through the composite key |
| `gaip-broker/src/main.rs` | (prior change) hot path calls `lookup_read_q(&CompositeKey { sender, message_id })` |

## 6. Caveats

- The gain is per-lookup (~46ns). It compounds only at high request volume
  (every relayed request probes this under the read lock).
- The `Equivalent<String>` probe still splits the stored key on the separator
  for every lookup — a fully zero-copy comparison would require the stored key
  to keep the two halves separate, which would complicate `snapshot`/`restore`.
  The current design is a good tradeoff: no allocation, one split, one byte
  compare.
- hashbrown is now a direct dependency of gaip-core (previously transitive via
  std). It is a stable, widely-used crate; the version is pinned to match std's
  internal version.
