#!/usr/bin/env bash
# GAIP binding test — gRPC over real HTTP/2.
#
# The binding is claimed only after this runs: a unary Send carrying a
# GAIP Request, a bidirectional stream carrying TASK_STREAM frames, and
# a non-GAIP payload refused with INVALID_ARGUMENT. `protoc` comes from
# protoc-bin-vendored, so no system install is needed.
set -euo pipefail
cd "$(dirname "$0")/.."

OUT=$(mktemp)
cargo test -p gaip-bindings grpc -- --nocapture 2>&1 | tee "$OUT" | grep -E "^(test |running)" || true

pass=$(grep -c "^test .* ok$" "$OUT" || true)
fail=$(grep -cE "^test .* FAILED$" "$OUT" || true)
echo "tests: $pass passed, $fail failed"

if [ "$pass" -ge 3 ] && [ "$fail" -eq 0 ]; then
  echo "PASS: GAIP Request/Reply and TASK_STREAM crossed gRPC (HTTP/2)"
  echo "EXPERIMENT_004: PASS"
else
  echo "EXPERIMENT_004: FAIL"
  exit 1
fi
