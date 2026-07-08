#!/usr/bin/env bash
# v3.0 Tier-3 smoke: run the fleet-reasoning worked example Docker-free and assert it
# demonstrates all three wedges — artifact-aware resume (model ready), capability-routed
# inference (routed + failover), and fleet-reasoning traces (trace replay). Retry-hardened
# for constrained CI runners (the example binds three ephemeral ports into one mesh).
set -uo pipefail

ATTEMPTS="${REASON_SMOKE_ATTEMPTS:-3}"
MARKERS=("model ready" "routed" "failover" "trace replay")

run_once() {
  local out
  out="$(cargo run -q -p mycelium-reason --features llm --example fleet_reasoning 2>/dev/null)"
  local code=$?
  echo "$out"
  [ "$code" -eq 0 ] || return 1
  local m
  for m in "${MARKERS[@]}"; do
    echo "$out" | grep -q "$m" || { echo "missing marker: $m"; return 1; }
  done
}

for i in $(seq 1 "$ATTEMPTS"); do
  echo "── fleet_reasoning smoke attempt $i/$ATTEMPTS ──"
  if run_once; then
    echo "fleet_reasoning smoke: PASS"
    exit 0
  fi
  echo "attempt $i failed; retrying…"
  sleep 2
done

echo "fleet_reasoning smoke: FAIL after $ATTEMPTS attempts"
exit 1
