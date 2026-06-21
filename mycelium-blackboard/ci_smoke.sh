#!/usr/bin/env bash
# WS-G / G3 · Phase 5 smoke: run the community-microgrid worked example Docker-free and assert it
# demonstrates the rd/in split (shared reads + competitive exactly-once claims). Retry-hardened for
# constrained CI runners (the example binds an ephemeral port + forms a 1-node cluster).
set -uo pipefail

ATTEMPTS="${BB_SMOKE_ATTEMPTS:-3}"
MARKER="consumed exactly once"

run_once() {
  local out
  out="$(cargo run -q -p mycelium-blackboard --example microgrid 2>/dev/null)"
  local code=$?
  echo "$out"
  [ "$code" -eq 0 ] && echo "$out" | grep -q "$MARKER"
}

for i in $(seq 1 "$ATTEMPTS"); do
  echo "── microgrid smoke attempt $i/$ATTEMPTS ──"
  if run_once; then
    echo "microgrid smoke: PASS"
    exit 0
  fi
  echo "attempt $i failed; retrying…"
  sleep 2
done

echo "microgrid smoke: FAIL after $ATTEMPTS attempts"
exit 1
