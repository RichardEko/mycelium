#!/usr/bin/env bash
# Integration test entry point — runs inside the test-runner container.
set -euo pipefail

PASS=0
FAIL=0
SCENARIOS_DIR=/tests/scenarios

source /tests/lib/helpers.sh

banner() { printf '\n\033[1;34m══ %s ══\033[0m\n' "$1"; }
ok()     { printf '\033[0;32mPASS\033[0m  %s\n' "$1"; }
fail()   { printf '\033[0;31mFAIL\033[0m  %s\n' "$1"; }

# Each scenario is a multi-node convergence check with fixed poll windows; on a shared/loaded
# CI runner a single attempt can overrun a window (S13's inflight→0 / secondary-visible polls
# are the usual victim). So give each scenario up to ATTEMPTS tries — a real regression fails
# all of them, a transient slow-runner miss does not. Same posture as the coop + fluid smokes.
# (Scenarios are self-contained — each sets up its own namespace/keys — so a retry re-runs clean.)
ATTEMPTS="${SCENARIO_ATTEMPTS:-2}"
run_scenario() {
    local label="$1" script="$2"
    printf '  %-45s ' "$label"
    local attempt
    for attempt in $(seq 1 "$ATTEMPTS"); do
        if bash "$script" 2>/tmp/scenario.err; then
            PASS=$((PASS + 1))
            if [ "$attempt" -gt 1 ]; then ok "$label (attempt $attempt/$ATTEMPTS)"; else ok "$label"; fi
            return
        fi
    done
    FAIL=$((FAIL + 1))
    fail "$label (failed $ATTEMPTS attempts)"
    sed 's/^/    /' /tmp/scenario.err >&2
}

# ── Phase 0: wait for core nodes to be healthy ────────────────────────────────
banner "Waiting for cluster to be ready"
wait_for_health "${NODE_A_HOST:-node-a}" "${NODE_HTTP_PORT:-8300}" 60
wait_for_health "${NODE_B_HOST:-node-b}" "${NODE_HTTP_PORT:-8300}" 60
wait_for_health "${MGMT_HOST:-mgmt}"     "${MGMT_HTTP_PORT:-8090}" 60

# Wait for capability advertisements to propagate to mgmt before running scenarios.
converged() {
    count=$(curl -sf --max-time 3 "http://${MGMT_HOST:-mgmt}:${MGMT_HTTP_PORT:-8090}/api/state" \
        2>/dev/null | jq '.nodes | length' 2>/dev/null || echo 0)
    [ "$count" -ge 3 ]
}
poll_until 30 converged || true  # warn but don't block; scenario 02 will fail descriptively
echo "  Core nodes healthy — starting scenarios"

# ── Scenarios ─────────────────────────────────────────────────────────────────
banner "Running scenarios"

run_scenario "01 mesh convergence"           "$SCENARIOS_DIR/01_mesh_convergence.sh"
run_scenario "02 management API + dashboard" "$SCENARIOS_DIR/02_mgmt_api.sh"
run_scenario "03 KV persistence restart"     "$SCENARIOS_DIR/03_kv_persistence.sh"
run_scenario "04 full-cluster restart"       "$SCENARIOS_DIR/04_full_cluster_restart.sh"
run_scenario "05 anti-entropy late joiner"   "$SCENARIOS_DIR/05_late_joiner.sh"
run_scenario "06 signal propagation"         "$SCENARIOS_DIR/06_signal_propagation.sh"
run_scenario "07 capability discovery"       "$SCENARIOS_DIR/07_capability_discovery.sh"
run_scenario "08 scatter-gather fan-out"     "$SCENARIOS_DIR/08_scatter_gather.sh"
run_scenario "09 invoke.bulk large payload"  "$SCENARIOS_DIR/09_invoke_bulk.sh"
run_scenario "10 event mailbox delivery"     "$SCENARIOS_DIR/10_event_mailbox.sh"
run_scenario "11 agentic flow network (AFN)" "$SCENARIOS_DIR/11_afn_pipeline.sh"
run_scenario "12 prompt skills (KV + invoke)" "$SCENARIOS_DIR/12_prompt_skills.sh"
run_scenario "13 tuple space (pull pipeline)" "$SCENARIOS_DIR/13_tuple_space.sh"

# ── Summary ───────────────────────────────────────────────────────────────────
banner "Results"
printf '  Passed: %d   Failed: %d\n\n' "$PASS" "$FAIL"

[ "$FAIL" -eq 0 ]
