#!/usr/bin/env bash
# v3.0 guardrails smoke: run the guardrail-wedge worked example Docker-free and assert it
# demonstrates the hard-prevention wedge — a policy strength report (the legibility), an
# unauthorized agent structurally stopped at the provider gate, an authorized agent admitted,
# and a tamper-evident cryptographic proof reconstructed by a neutral observer. Retry-hardened
# for constrained CI runners (the example binds four ephemeral ports into one tls mesh).
set -uo pipefail

ATTEMPTS="${GUARDRAILS_SMOKE_ATTEMPTS:-3}"
MARKERS=("strength report" "structurally stopped" "admitted" "tamper-evident" "WEDGE OK")

run_once() {
  local out
  out="$(cargo run -q -p mycelium-guardrails --features compliance --example guardrail_wedge 2>/dev/null)"
  local code=$?
  echo "$out"
  [ "$code" -eq 0 ] || return 1
  local m
  for m in "${MARKERS[@]}"; do
    echo "$out" | grep -q "$m" || { echo "missing marker: $m"; return 1; }
  done
}

for i in $(seq 1 "$ATTEMPTS"); do
  echo "── guardrail_wedge smoke attempt $i/$ATTEMPTS ──"
  if run_once; then
    echo "guardrail_wedge smoke: PASS"
    exit 0
  fi
  echo "attempt $i failed; retrying…"
  sleep 2
done

echo "guardrail_wedge smoke: FAIL after $ATTEMPTS attempts"
exit 1
