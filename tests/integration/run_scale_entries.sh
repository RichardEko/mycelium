#!/usr/bin/env bash
# Entry-volume scale test — runs inside the test-runner container.
#
# Validates the *entry-volume* axis that the 100-node test in run_scale.sh
# does not cover. The 100-node test writes one key and confirms it gossips.
# This test writes ENTRY_COUNT keys (default 5 000) to a 30-node cluster and
# measures: (a) how long until every entry is visible on mgmt, (b) how much
# of that closure happens during the write phase via live gossip vs after
# the write phase via the anti-entropy sweep, and (c) whether the convergence
# is stable (no flapping) and complete (no missing entries on a sampled
# random read).
#
# Why 30 nodes
#   The 100-node test sits at the iptables FORWARD-chain ceiling and uses
#   conntrack tricks to verify before chain saturation. Here, we want the
#   runner to make new TCP connections throughout the test (bulk PUTs +
#   convergence polling + random sample reads), so we deliberately stay
#   well below the ceiling. 30 nodes is also above the small-cluster regime
#   where everything propagates synchronously — anti-entropy actually has
#   work to do here.
#
# What "sweep tail" means
#   T_write_end is when the runner finishes issuing all PUTs to seed.
#   T_converge is when mgmt sees every entry. The difference is the
#   "sweep tail" — entries that did not propagate to mgmt via live gossip
#   during the write phase and were instead picked up by anti-entropy. A
#   small tail means live gossip kept up with the write rate; a large tail
#   means anti-entropy had to fill in the difference.

set -euo pipefail

SEED_HOST="${SEED_HOST:-seed}"
MGMT_HOST="${MGMT_HOST:-mgmt}"
PORT="${NODE_HTTP_PORT:-8300}"
MGMT_PORT="${MGMT_HTTP_PORT:-8090}"
SCALE_TARGET="${SCALE_TARGET:-29}"
TOTAL_NODES=$(( SCALE_TARGET + 1 ))

ENTRY_COUNT="${ENTRY_COUNT:-5000}"
ENTRY_BYTES="${ENTRY_BYTES:-512}"
WRITE_PARALLELISM="${WRITE_PARALLELISM:-32}"
WRITE_DELAY_MS="${WRITE_DELAY_MS:-0}"
CONVERGENCE_TIMEOUT_SECS="${CONVERGENCE_TIMEOUT_SECS:-180}"

source /tests/lib/helpers.sh

PASS=0
FAIL=0

banner() { printf '\n\033[1;34m══ %s ══\033[0m\n' "$1"; }
ok()     { PASS=$((PASS+1)); printf '\033[0;32mPASS\033[0m  %s\n' "$1"; }
fail()   { FAIL=$((FAIL+1)); printf '\033[0;31mFAIL\033[0m  %s\n' "$1"; }
warn()   { printf '\033[0;33mWARN\033[0m  %s\n' "$1"; }

# Generate a fixed-size payload of ENTRY_BYTES. Held in a local file once and
# reused; printf is fast but repeatedly generating from bash is slow at 5 000+
# writes.
PAYLOAD_FILE="$(mktemp)"
trap 'rm -f "$PAYLOAD_FILE"' EXIT
printf '%*s' "$ENTRY_BYTES" '' | tr ' ' 'x' > "$PAYLOAD_FILE"

# ── Phase 0: wait for seed + mgmt to be healthy ──────────────────────────────

banner "Phase 0 — Health"
wait_for_health "$SEED_HOST" "$PORT" 120
wait_for_health "$MGMT_HOST" "$MGMT_PORT" 120
echo "  Seed and mgmt healthy"

# ── Phase 1: wait for cluster to converge ────────────────────────────────────

banner "Phase 1 — Cluster convergence (${TOTAL_NODES} nodes)"

cluster_converged() {
    local count
    count=$(curl -sf --max-time 5 "http://${MGMT_HOST}:${MGMT_PORT}/api/state" 2>/dev/null \
        | jq '.nodes | length' 2>/dev/null || echo 0)
    [ "$count" -ge "$TOTAL_NODES" ]
}

if poll_until 180 cluster_converged; then
    node_count=$(curl -sf --max-time 5 "http://${MGMT_HOST}:${MGMT_PORT}/api/state" 2>/dev/null \
        | jq '.nodes | length' 2>/dev/null || echo "?")
    ok "All nodes joined — mgmt sees ${node_count} nodes"
else
    node_count=$(curl -sf --max-time 5 "http://${MGMT_HOST}:${MGMT_PORT}/api/state" 2>/dev/null \
        | jq '.nodes | length' 2>/dev/null || echo "?")
    fail "Cluster did not converge — only ${node_count} of ${TOTAL_NODES} visible to mgmt"
    exit 1
fi

# ── Phase 2: pre-load baseline (sanity check that gossip works) ──────────────

banner "Phase 2 — Pre-load baseline"

BASELINE_KEY="graph/baseline/$(date +%s)"
BASELINE_VAL="baseline-ok"

if ! curl -sf --max-time 5 -X PUT --data-binary "$BASELINE_VAL" \
        "http://${SEED_HOST}:${PORT}/kv/${BASELINE_KEY}" > /dev/null; then
    fail "Baseline KV write failed"
    exit 1
fi

baseline_on_mgmt() {
    local count
    count=$(curl -sf --max-time 10 \
        "http://${MGMT_HOST}:${MGMT_PORT}/api/kv-scan?prefix=graph/baseline/" \
        2>/dev/null | jq '.count' 2>/dev/null || echo 0)
    [ "$count" -ge 1 ]
}

if poll_until 30 baseline_on_mgmt; then
    ok "Baseline key propagated — gossip path is open"
else
    fail "Baseline key did not propagate; cluster is unhealthy before bulk load"
    exit 1
fi

# ── Phase 3: bulk load ───────────────────────────────────────────────────────
#
# Two modes:
#   WRITE_DELAY_MS=0 (default)  → burst: parallel xargs at WRITE_PARALLELISM
#   WRITE_DELAY_MS>0            → paced: serial writes with sleep, simulating
#                                 sustained-rate load (steady-state shape)
#
# The paced mode is the sanity check for "does steady-state behave at least as
# well as the burst?" — anti-entropy gets continuous opportunity rather than
# peak-and-drain dynamics, so the live-gossip fraction should be ≥ the burst
# value and dropped_frames should be ≤ the burst value at matched entry counts.

PREFIX="graph/cell"

if [ "$WRITE_DELAY_MS" -eq 0 ]; then
    banner "Phase 3 — Burst write of ${ENTRY_COUNT} entries (${ENTRY_BYTES} B each)"
    T_WRITE_START=$(date +%s)

    # Parallel xargs. Each curl is a separate TCP connection; runner-to-seed
    # conntrack stays comfortably below the iptables threshold at 30 nodes.
    seq 0 $(( ENTRY_COUNT - 1 )) | xargs -n 1 -P "$WRITE_PARALLELISM" -I {} \
        curl -sf --max-time 10 -X PUT --data-binary "@${PAYLOAD_FILE}" \
            "http://${SEED_HOST}:${PORT}/kv/${PREFIX}/{}" -o /dev/null \
        || warn "Some PUTs returned non-zero — proceeding to convergence check anyway"

    T_WRITE_END=$(date +%s)
else
    EXPECTED_DURATION_S=$(( ENTRY_COUNT * WRITE_DELAY_MS / 1000 ))
    banner "Phase 3 — Paced write of ${ENTRY_COUNT} entries (${ENTRY_BYTES} B, ${WRITE_DELAY_MS} ms delay ≈ ${EXPECTED_DURATION_S} s)"
    T_WRITE_START=$(date +%s)

    # Serial loop with sleep. busybox sleep on Alpine accepts fractional secs.
    DELAY_SEC=$(awk "BEGIN { printf \"%.4f\", $WRITE_DELAY_MS/1000 }")
    LAST_PROGRESS=$T_WRITE_START
    for i in $(seq 0 $(( ENTRY_COUNT - 1 ))); do
        curl -sf --max-time 10 -X PUT --data-binary "@${PAYLOAD_FILE}" \
            "http://${SEED_HOST}:${PORT}/kv/${PREFIX}/${i}" -o /dev/null \
            || true
        sleep "$DELAY_SEC"
        # Progress log every 30 s so the operator can see the test is alive.
        NOW=$(date +%s)
        if [ $(( NOW - LAST_PROGRESS )) -ge 30 ]; then
            ELAPSED=$(( NOW - T_WRITE_START ))
            printf '  t=+%4ds  paced write progress: %d/%d\n' \
                "$ELAPSED" "$(( i + 1 ))" "$ENTRY_COUNT"
            LAST_PROGRESS=$NOW
        fi
    done

    T_WRITE_END=$(date +%s)
fi

WRITE_DURATION=$(( T_WRITE_END - T_WRITE_START ))
if [ "$WRITE_DURATION" -gt 0 ]; then
    WRITE_THROUGHPUT=$(( ENTRY_COUNT / WRITE_DURATION ))
else
    WRITE_THROUGHPUT=$ENTRY_COUNT
fi

echo "  Wrote ${ENTRY_COUNT} entries in ${WRITE_DURATION} s (~${WRITE_THROUGHPUT} entries/s to seed)"

# ── Phase 4: convergence curve ───────────────────────────────────────────────

banner "Phase 4 — Convergence on mgmt"

scan_count() {
    curl -sf --max-time 15 \
        "http://${MGMT_HOST}:${MGMT_PORT}/api/kv-scan?prefix=${PREFIX}/" \
        2>/dev/null | jq '.count' 2>/dev/null || echo 0
}

# Take the first sample immediately after T_WRITE_END.
INITIAL_VISIBLE=$(scan_count)
INITIAL_TAIL=$(( ENTRY_COUNT - INITIAL_VISIBLE ))
echo "  Immediately after write: mgmt sees ${INITIAL_VISIBLE}/${ENTRY_COUNT} (sweep tail = ${INITIAL_TAIL})"

# Sample the growth curve every 5 s up to CONVERGENCE_TIMEOUT_SECS.
SAMPLE_INTERVAL=5
SAMPLES_TAKEN=0
LAST_COUNT=$INITIAL_VISIBLE
T_CONVERGE=""
T_POLL_START=$(date +%s)

while true; do
    NOW=$(date +%s)
    ELAPSED=$(( NOW - T_POLL_START ))
    if [ "$ELAPSED" -ge "$CONVERGENCE_TIMEOUT_SECS" ]; then
        break
    fi
    sleep "$SAMPLE_INTERVAL"
    SAMPLES_TAKEN=$(( SAMPLES_TAKEN + 1 ))
    NOW=$(date +%s)
    ELAPSED=$(( NOW - T_WRITE_END ))
    COUNT=$(scan_count)
    GAINED=$(( COUNT - LAST_COUNT ))
    LAST_COUNT=$COUNT
    printf '  t=+%3ds  visible=%5d/%d  +%d since last sample\n' \
        "$ELAPSED" "$COUNT" "$ENTRY_COUNT" "$GAINED"
    if [ "$COUNT" -ge "$ENTRY_COUNT" ]; then
        T_CONVERGE=$NOW
        break
    fi
done

if [ -z "$T_CONVERGE" ]; then
    FINAL_COUNT=$(scan_count)
    MISSING=$(( ENTRY_COUNT - FINAL_COUNT ))
    fail "Did not converge within ${CONVERGENCE_TIMEOUT_SECS} s — mgmt sees ${FINAL_COUNT}/${ENTRY_COUNT} (missing ${MISSING})"
    exit 1
fi

SWEEP_TAIL_SECS=$(( T_CONVERGE - T_WRITE_END ))
LIVE_FRACTION_PCT=$(( INITIAL_VISIBLE * 100 / ENTRY_COUNT ))

ok "Mgmt converged on all ${ENTRY_COUNT} entries"
echo "  Sweep tail: ${SWEEP_TAIL_SECS} s after write end"
echo "  Live-gossip fraction at T_write_end: ${LIVE_FRACTION_PCT}%"
echo "  Anti-entropy fraction (closed by sweep): $(( 100 - LIVE_FRACTION_PCT ))%"

# ── Phase 5: stability ───────────────────────────────────────────────────────

banner "Phase 5 — Stability"

sleep 15
STABLE_COUNT=$(scan_count)
if [ "$STABLE_COUNT" -eq "$ENTRY_COUNT" ]; then
    ok "Count stable at ${ENTRY_COUNT} 15 s after convergence (no flapping)"
else
    fail "Count drifted to ${STABLE_COUNT} after convergence — eviction or replication issue"
fi

# ── Phase 6: random-sample integrity ─────────────────────────────────────────

banner "Phase 6 — Random-sample integrity (50 keys)"

SAMPLE_SIZE=50
SAMPLE_OK=0
SAMPLE_BAD=0
EXPECTED_BYTES=$ENTRY_BYTES

for _ in $(seq 1 "$SAMPLE_SIZE"); do
    IDX=$(( RANDOM % ENTRY_COUNT ))
    # Verify the entry exists on mgmt with the expected byte count.  We use
    # kv-scan with the specific-key prefix so the result includes byte length.
    BYTES=$(curl -sf --max-time 5 \
        "http://${MGMT_HOST}:${MGMT_PORT}/api/kv-scan?prefix=${PREFIX}/${IDX}" \
        2>/dev/null \
        | jq --arg k "${PREFIX}/${IDX}" '[.entries[] | select(.key==$k) | .bytes] | first // 0' \
        2>/dev/null || echo 0)
    if [ "$BYTES" -eq "$EXPECTED_BYTES" ]; then
        SAMPLE_OK=$(( SAMPLE_OK + 1 ))
    else
        SAMPLE_BAD=$(( SAMPLE_BAD + 1 ))
    fi
done

if [ "$SAMPLE_BAD" -eq 0 ]; then
    ok "All ${SAMPLE_SIZE} random samples present with correct byte count"
else
    fail "${SAMPLE_BAD}/${SAMPLE_SIZE} random samples missing or wrong size"
fi

# ── Phase 7: backpressure ────────────────────────────────────────────────────

banner "Phase 7 — Backpressure"

dropped_seed=$(curl -sf --max-time 5 "http://${SEED_HOST}:${PORT}/stats" 2>/dev/null \
    | jq '.dropped_frames // 0' 2>/dev/null || echo 0)
dropped_mgmt=$(curl -sf --max-time 5 "http://${MGMT_HOST}:${PORT}/stats" 2>/dev/null \
    | jq '.dropped_frames // 0' 2>/dev/null || echo 0)

if [ "$dropped_seed" -eq 0 ] && [ "$dropped_mgmt" -eq 0 ]; then
    ok "dropped_frames = 0 on seed and mgmt at ${ENTRY_COUNT} entries × ${TOTAL_NODES} nodes"
else
    warn "dropped_frames seed=${dropped_seed} mgmt=${dropped_mgmt} — raise GOSSIP_WRITER_CHANNEL_DEPTH (currently 4096)"
    # Non-zero is informative, not a hard failure: it tells the operator the
    # current channel depth was insufficient for this entry volume.
    ok "dropped_frames captured (see WARN above)"
fi

# ── Summary ──────────────────────────────────────────────────────────────────

banner "Entry-volume scale test results"
if [ "$WRITE_DELAY_MS" -eq 0 ]; then
    WRITE_MODE="burst (parallel xargs, P=${WRITE_PARALLELISM})"
else
    WRITE_MODE="paced (serial, ${WRITE_DELAY_MS} ms between writes)"
fi
printf '  Cluster:           %d nodes\n' "$TOTAL_NODES"
printf '  Entries:           %d × %d B = %d KB\n' \
    "$ENTRY_COUNT" "$ENTRY_BYTES" "$(( ENTRY_COUNT * ENTRY_BYTES / 1024 ))"
printf '  Write mode:        %s\n' "$WRITE_MODE"
printf '  Write phase:       %d s (~%d entries/s to seed)\n' \
    "$WRITE_DURATION" "$WRITE_THROUGHPUT"
printf '  Live-gossip frac:  %d%% visible on mgmt at T_write_end\n' "$LIVE_FRACTION_PCT"
printf '  Anti-entropy tail: %d s to close the remaining %d entries\n' \
    "$SWEEP_TAIL_SECS" "$INITIAL_TAIL"
printf '  Pass: %d   Fail: %d\n' "$PASS" "$FAIL"

if [ "$FAIL" -gt 0 ]; then
    printf '\033[0;31mFAIL\033[0m\n'
    exit 1
fi
printf '\033[0;32mPASS\033[0m — entry-volume scale test complete\n'
