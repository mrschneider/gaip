#!/usr/bin/env bash
# GAIP binding test — NATS, against a REAL nats-server container.
#
# Proves the binding is a binding: a GAIP Request published on a subject
# arrives byte-identical at another connection, and the Reply goes back
# on a subject. No mock that agrees with itself.
set -euo pipefail
cd "$(dirname "$0")/.."

PORT=4223
NAME=gaip-nats-test

cleanup() { docker rm -f "$NAME" >/dev/null 2>&1 || true; }
trap cleanup EXIT
cleanup

docker run -d --rm --name "$NAME" -p 127.0.0.1:$PORT:4222 nats:2.10-alpine >/dev/null
for i in $(seq 1 40); do
  if docker exec "$NAME" sh -c 'true' >/dev/null 2>&1 && \
     (echo > /dev/tcp/127.0.0.1/$PORT) >/dev/null 2>&1; then break; fi
  sleep 0.25
done

echo "== nats-server version:"
docker exec "$NAME" nats-server --version 2>&1 | head -1

cargo build --quiet -p gaip-bindings 2>/dev/null
echo "== roundtrip:"
./target/debug/gaip-nats-selftest "nats://127.0.0.1:$PORT" | tee /tmp/gaip_nats.out
grep -q "PASS:" /tmp/gaip_nats.out
echo "EXPERIMENT_003: PASS"
