#!/usr/bin/env bash
# Docker-free smoke for the Food-Rescue Co-op suite. Runs each shipped demo and asserts on its
# output. Mirrors examples/fluid_pipeline/ci_smoke.sh. Exits non-zero on any failure.
#
# Each demo is a multi-node in-process TLS cluster whose convergence is timing-sensitive; on a
# shared/constrained CI runner a single attempt can miss a structural-poll deadline. So each demo
# gets up to ATTEMPTS tries — a real regression fails all of them, a transient slow-runner miss does
# not. (The same posture as the wasm-host e2e port retry and the tuple-space rpc_roundtrip primary
# poll.) Output of the last attempt is always printed for diagnosis.
set -uo pipefail

cd "$(dirname "$0")/../.."

ATTEMPTS="${COOP_SMOKE_ATTEMPTS:-3}"

# run_demo <label> <bin> <required-substring>...
# Passes if some attempt's combined output contains every required substring.
run_demo() {
  local label="$1" bin="$2"; shift 2
  local markers=("$@")
  echo "── ${label} ─────────────────────────────────────────────"
  local attempt out ok
  # `provisioning` + `catalog` need the WASM autonomic-provisioner host, gated behind the `wasm`
  # feature so the other ten demos build without compiling wasmtime (see examples/coop/Cargo.toml).
  local feat=""
  case "$bin" in provisioning|catalog) feat="--features wasm";; esac
  for attempt in $(seq 1 "$ATTEMPTS"); do
    out="$(cargo run -q -p mycelium-coop-examples $feat --bin "$bin" 2>&1 || true)"
    ok=1
    for m in "${markers[@]}"; do
      echo "$out" | grep -q -- "$m" || { ok=0; break; }
    done
    if [ "$ok" = 1 ]; then
      echo "$out" | grep -E "All assertions passed|^\[" | tail -3
      echo "  ✓ ${label} (attempt ${attempt}/${ATTEMPTS})"
      echo
      return 0
    fi
    echo "  …attempt ${attempt}/${ATTEMPTS} did not show all markers; retrying" >&2
  done
  echo "FAIL: ${label} did not pass after ${ATTEMPTS} attempts. Last output:" >&2
  echo "$out" >&2
  exit 1
}

run_demo "01 · mailbox_llm"      mailbox_llm      "All assertions passed" "triage replied"
run_demo "02 · stigmergy"        stigmergy        "All assertions passed"
run_demo "03 · elastic_intent"   elastic_intent   "All assertions passed"
run_demo "04 · provisioning"     provisioning     "All assertions passed" "self-healed"
run_demo "05 · federation_facts" federation_facts "All assertions passed" "verified the self-signature"
run_demo "06 · rotation"         rotation         "All assertions passed" "STILL verifies the old-key-signed field"
run_demo "07 · consensus"        consensus        "All assertions passed" "reads as reopened"
run_demo "08 · llm_pipeline"     llm_pipeline     "All assertions passed" "both LLM stages"
run_demo "09 · mcp_toolgrowth"   mcp_toolgrowth   "All assertions passed" "loaded the MCP tool and offered it out"
run_demo "10 · llm_council"      llm_council      "All assertions passed" "fanned out to 3 specialists, synthesized, and refined"
run_demo "11 · catalog"          catalog          "All assertions passed" "pulled the artifact bytes over the mesh"
run_demo "12 · diagnostics"      diagnostics      "All assertions passed" "diagnosed the governed-group conflict"

echo "All co-op smokes passed."
