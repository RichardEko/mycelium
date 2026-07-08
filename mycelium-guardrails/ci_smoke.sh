#!/usr/bin/env bash
# v3.0 guardrails smoke: run the guardrail worked examples Docker-free and assert they demonstrate
# the guardrail story on the printed markers. Two demos:
#   · guardrail_wedge — the hard-prevention wedge end-to-end (a policy strength report, an
#     unauthorized agent structurally stopped at the provider gate, an authorized agent admitted, a
#     tamper-evident proof reconstructed by a neutral observer).
#   · guardrail_fleet — all THREE strength tiers actually firing in one constructive-domain co-op:
#     a boundary drop (Tier A), a denied tool blocked at a state transition (Tier B), and an
#     unauthorized caller rejected + sealed + proven (Tier C).
# Retry-hardened for constrained CI runners (each example binds ephemeral ports into one tls mesh).
set -uo pipefail

ATTEMPTS="${GUARDRAILS_SMOKE_ATTEMPTS:-3}"

# run_demo <example-name> <ok-marker> <marker...> — run one example up to $ATTEMPTS times, passing
# when it exits 0 and every marker is present in its output.
run_demo() {
  local example="$1"
  shift
  local markers=("$@")

  local i
  for i in $(seq 1 "$ATTEMPTS"); do
    echo "── $example smoke attempt $i/$ATTEMPTS ──"
    local out code
    out="$(cargo run -q -p mycelium-guardrails --features compliance --example "$example" 2>/dev/null)"
    code=$?
    echo "$out"
    if [ "$code" -eq 0 ]; then
      local ok=1 m
      for m in "${markers[@]}"; do
        echo "$out" | grep -q "$m" || { echo "missing marker: $m"; ok=0; break; }
      done
      if [ "$ok" -eq 1 ]; then
        echo "$example smoke: PASS"
        return 0
      fi
    fi
    echo "attempt $i failed; retrying…"
    sleep 2
  done

  echo "$example smoke: FAIL after $ATTEMPTS attempts"
  return 1
}

run_demo guardrail_wedge "strength report" "structurally stopped" "admitted" "tamper-evident" "WEDGE OK" || exit 1
run_demo guardrail_fleet "Tier A" "Tier B" "Tier C" "tamper-evident" "FLEET OK" || exit 1

echo "guardrails smoke: PASS (guardrail_wedge + guardrail_fleet)"
exit 0
