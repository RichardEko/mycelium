#!/usr/bin/env bash
# Scale resilience test — runs inside the test-runner container.
#
# Validates crash recovery, anti-entropy sync, late-joiner catchup, and
# cluster stability under churn at ~20-node scale (default; see Makefile).
#
# Phases
#   0 — Health gate: seed + mgmt healthy
#   1 — Formation: wait for all nodes, write 100 baseline keys
#   2 — Crash: stop 5 workers, write 20 keys during outage, restart, verify recovery
#   3 — Late joiner: start a fresh probe node, verify it receives full KV history
#   4 — Churn: 3 × (stop 10 workers → write 10 keys → restart → verify no loss)
#
# All cluster-state verification uses mgmt (/api/state, /api/kv-scan) as the
# stable observation point — avoids the Docker bridge iptables O(N²) constraint
# documented in run_scale.sh.  Phase 3 additionally queries the probe's own HTTP
# port to confirm anti-entropy inbound delivery.
#
# Requirements
#   /var/run/docker.sock  — mounted into this container (see compose file)
#   docker-cli            — already present in Dockerfile.test-runner
set -euo pipefail

SEED_HOST="${SEED_HOST:-seed}"
MGMT_HOST="${MGMT_HOST:-mgmt}"
PORT="${NODE_HTTP_PORT:-8300}"
MGMT_PORT="${MGMT_HTTP_PORT:-8090}"
RESILIENCE_WORKERS="${RESILIENCE_WORKERS:-50}"
COMPOSE_PROJECT="${COMPOSE_PROJECT:-mycelium-resilience}"
DOCKER_NETWORK="${DOCKER_NETWORK:-${COMPOSE_PROJECT}_mesh}"
WORKER_IMAGE="${WORKER_IMAGE:-mycelium-resilience-worker}"
TOTAL_NODES=$(( RESILIENCE_WORKERS + 1 ))   # workers + seed; mgmt counted separately by /api/state
PROBE_NAME="${COMPOSE_PROJECT}-late-joiner"

source /tests/lib/helpers.sh

PASS=0
FAIL=0
PROBE_STARTED=0   # track for EXIT trap
KEEPALIVE_PID=0   # background seed:PORT ping loop

banner() { printf '\n\033[1;34m══ %s ══\033[0m\n' "$*"; }
ok()     { PASS=$(( PASS + 1 )); printf '\033[0;32mPASS\033[0m  %s\n' "$*"; }
fail()   { FAIL=$(( FAIL + 1 )); printf '\033[0;31mFAIL\033[0m  %s\n' "$*"; }
die()    { fail "$*"; printf '\nAborting.\n'; exit 1; }

# Ping seed:PORT every 5 s to keep the kernel conntrack entry ESTABLISHED
# throughout the test.  Without this, Phase 2/4 KV writes open new TCP
# connections that must traverse the iptables FORWARD chain — which can
# time out once all inter-node connections have saturated it.
# Only role=node (seed, workers) registers /kv PUT routes via with_http_routes;
# mgmt:PORT has no KV write endpoint, so all writes go to seed.
seed_keepalive() {
    while true; do
        curl -sf --max-time 3 \
            "http://${SEED_HOST}:${PORT}/health" > /dev/null 2>&1 || true
        sleep 5
    done
}

cleanup() {
    if [ "$PROBE_STARTED" -eq 1 ]; then
        docker rm -f "$PROBE_NAME" > /dev/null 2>&1 || true
    fi
    [ "$KEEPALIVE_PID" -gt 0 ] && kill "$KEEPALIVE_PID" 2>/dev/null || true
}
trap cleanup EXIT

# ── Helpers ───────────────────────────────────────────────────────────────────

# Count live nodes visible to mgmt
mgmt_node_count() {
    curl -sf --max-time 5 \
        "http://${MGMT_HOST}:${MGMT_PORT}/api/state" 2>/dev/null \
        | jq '.nodes | length' 2>/dev/null || echo 0
}

# Count KV keys under resilience/ prefix as seen by mgmt
kv_count_on_mgmt() {
    curl -sf --max-time 10 \
        "http://${MGMT_HOST}:${MGMT_PORT}/api/kv-scan?prefix=resilience/" \
        2>/dev/null | jq '.count' 2>/dev/null || echo 0
}

# Predicates for poll_until (pass the threshold as argument)
cluster_at_least() { [ "$(mgmt_node_count)" -ge "$1" ]; }
kv_at_least()      { [ "$(kv_count_on_mgmt)" -ge "$1" ]; }

# Write COUNT keys to HOST under PREFIX with zero-padded index from OFFSET.
# All phases write to seed:PORT — the only node with /kv PUT routes registered
# (via with_http_routes / init_node_routes).  The background seed_keepalive
# loop (started in Phase 0) maintains the runner→seed conntrack entry so the
# write succeeds even after inter-node connections have saturated iptables.
write_keys() {
    local host="$1" prefix="$2" count="$3" offset="${4:-0}"
    local i end=$(( offset + count - 1 ))
    for i in $(seq "$offset" "$end"); do
        local key="${prefix}/$(printf '%04d' "$i")"
        curl -sf --max-time 5 -X PUT -d "v${i}" \
            "http://${host}:${PORT}/kv/${key}" > /dev/null 2>&1 \
            || echo "  WARN: write failed for key ${key}" >&2
    done
}

# Return up to N names of running worker containers for this project
list_running_workers() {
    local n="${1:-5}"
    docker ps \
        --filter "name=${COMPOSE_PROJECT}-worker" \
        --filter "status=running" \
        --format '{{.Names}}' \
        | head -"$n"
}

# ── Phase 0: health gate ──────────────────────────────────────────────────────

banner "Phase 0: Waiting for seed and mgmt to be healthy"
wait_for_health "$SEED_HOST" "$PORT"       120
wait_for_health "$MGMT_HOST" "$MGMT_PORT"  120
printf '  seed (%s:%s) and mgmt (%s:%s) healthy\n' \
    "$SEED_HOST" "$PORT" "$MGMT_HOST" "$MGMT_PORT"

# Start keepalive after health gates pass — seed is up and its /health
# endpoint is reachable.  The keepalive runs until cleanup() kills it.
seed_keepalive &
KEEPALIVE_PID=$!

# ── Phase 1: cluster formation + 100-key baseline ────────────────────────────

banner "Phase 1: Formation — waiting for ${TOTAL_NODES} nodes (${RESILIENCE_WORKERS} workers + seed)"

if poll_until 240 cluster_at_least "$TOTAL_NODES"; then
    ok "Formation: $(mgmt_node_count) nodes visible to mgmt"
else
    die "Cluster did not converge within 240 s — $(mgmt_node_count)/${TOTAL_NODES} joined"
fi

echo "  Writing 100 baseline keys (resilience/baseline/0000–0099)…"
write_keys "$SEED_HOST" "resilience/baseline" 100 0

if poll_until 60 kv_at_least 100; then
    ok "Baseline: 100 keys propagated to mgmt"
else
    die "Baseline keys did not reach mgmt within 60 s (saw $(kv_count_on_mgmt))"
fi

# ── Phase 2: crash 5 workers, write during outage, recover ───────────────────

banner "Phase 2: Crash 5 workers → write 20 keys during outage → recover"

CRASHED=$(list_running_workers 5)
if [ -z "$CRASHED" ]; then
    die "No worker containers found — expected project=${COMPOSE_PROJECT} (check COMPOSE_PROJECT env)"
fi
printf '  Stopping: %s\n' "$(echo "$CRASHED" | tr '\n' ' ')"
echo "$CRASHED" | xargs docker stop > /dev/null

# Write crash-window keys to seed immediately — before the TTL wait below.
# seed:PORT has /kv PUT routes and a live conntrack entry maintained by the
# background seed_keepalive started in Phase 0.
echo "  Writing 20 crash-window keys to seed (resilience/crash/0000–0019)…"
write_keys "$SEED_HOST" "resilience/crash" 20 0

# Now wait for the capability advertisement TTL to expire so mgmt drops the
# stopped workers from its view — diagnostic, not a hard failure.
count_dropped() { [ "$(mgmt_node_count)" -le "$(( TOTAL_NODES - 3 ))" ]; }
if poll_until 90 count_dropped; then
    echo "  Capability TTLs expired — mgmt now sees $(mgmt_node_count) nodes"
else
    echo "  WARN: node count did not drop within 90 s (capability TTL may be extended)"
fi

if poll_until 60 kv_at_least 120; then
    ok "Crash window: 20 keys propagated within the live cluster (120 total)"
else
    die "Crash-window keys not visible on mgmt within 60 s (saw $(kv_count_on_mgmt))"
fi

echo "  Restarting crashed workers…"
echo "$CRASHED" | xargs docker start > /dev/null

if poll_until 120 cluster_at_least "$TOTAL_NODES"; then
    ok "Recovery: all ${TOTAL_NODES} nodes rejoined after crash (mgmt sees $(mgmt_node_count))"
else
    die "Cluster did not recover to ${TOTAL_NODES} nodes within 120 s (saw $(mgmt_node_count))"
fi

if poll_until 30 kv_at_least 120; then
    ok "Anti-entropy: all 120 keys intact after crash and recovery"
else
    die "Key loss after crash recovery: only $(kv_count_on_mgmt) of 120 keys on mgmt"
fi

# ── Phase 3: late joiner receives full KV history via anti-entropy ────────────

banner "Phase 3: Late-joiner probe — fresh node receives full KV history via anti-entropy"

# Remove any stale probe from a previous (failed) run
docker rm -f "$PROBE_NAME" > /dev/null 2>&1 || true

# The entrypoint mirrors the worker service definition in the compose file.
# \$(...) and \$1 prevent expansion by this shell; sh -c expands them inside
# the started container.
_DEMO_CMD="MYCELIUM_HOSTNAME=\$(hostname -I | awk '{print \$1}') exec /usr/local/bin/mycelium-demo"

echo "  Starting late-joiner probe (${PROBE_NAME}) on network ${DOCKER_NETWORK}…"
docker run -d \
    --name "$PROBE_NAME" \
    --network "$DOCKER_NETWORK" \
    -e MYCELIUM_ROLE=node \
    -e MYCELIUM_PORT=57000 \
    -e MYCELIUM_HTTP_PORT=8300 \
    -e MYCELIUM_PEERS=seed:57000 \
    -e GOSSIP_WRITER_CHANNEL_DEPTH=512 \
    -e RUST_LOG="warn,mycelium=info" \
    --entrypoint sh \
    "$WORKER_IMAGE" \
    -c "$_DEMO_CMD" \
    > /dev/null
PROBE_STARTED=1

# Check liveness via Docker socket (avoids the iptables FORWARD saturation
# that blocks new TCP connections at 50-node scale — see CLAUDE.md §iptables).
probe_healthy() {
    docker inspect --format='{{.State.Running}}' "$PROBE_NAME" 2>/dev/null | grep -q true
}
if ! poll_until 60 probe_healthy; then
    die "Late-joiner probe container did not reach running state within 60 s"
fi

probe_in_cluster() { [ "$(mgmt_node_count)" -ge "$(( TOTAL_NODES + 1 ))" ]; }
if poll_until 120 probe_in_cluster; then
    ok "Late joiner: probe joined cluster (mgmt sees $(mgmt_node_count) nodes)"
else
    die "Probe did not appear in mgmt node list within 120 s"
fi

# Anti-entropy inbound: probe must receive a key that was written before it
# started.  resilience/baseline/0000 was written in Phase 1.
# Use docker exec so the curl runs inside the probe (localhost — no iptables).
probe_has_baseline_key() {
    local val
    val=$(docker exec "$PROBE_NAME" \
        curl -sf --max-time 5 "http://localhost:${PORT}/kv/resilience/baseline/0000" \
        2>/dev/null || echo "")
    [ "$val" = "v0" ]
}
if poll_until 120 probe_has_baseline_key; then
    ok "Anti-entropy inbound: probe received pre-existing key resilience/baseline/0000"
else
    die "Probe did not receive pre-existing keys via anti-entropy within 120 s"
fi

# Gossip outbound: write a key to the probe and verify it reaches mgmt.
# Use docker exec so the PUT runs inside the probe (localhost — no iptables).
PROBE_KEY="resilience/probe/$(date +%s)"
docker exec "$PROBE_NAME" \
    curl -sf --max-time 5 -X PUT -d "probe-write" \
    "http://localhost:${PORT}/kv/${PROBE_KEY}" > /dev/null 2>&1 \
    || die "Failed to write key to probe HTTP endpoint"

# Verify the probe-written key reached seed (seed has /kv GET via init_node_routes;
# mgmt:PORT has no /kv routes).  This proves the probe gossiped outbound.
probe_key_on_seed() {
    local val
    val=$(curl -sf --max-time 5 \
        "http://${SEED_HOST}:${PORT}/kv/${PROBE_KEY}" 2>/dev/null || echo "")
    [ "$val" = "probe-write" ]
}
if poll_until 30 probe_key_on_seed; then
    ok "Gossip outbound: probe-written key propagated to seed"
else
    die "Key written to probe did not reach seed within 30 s"
fi

# Remove probe before Phase 4 to keep the cluster size stable.
# The probe's KV key persists in the cluster (LWW; remains in all nodes' stores).
docker rm -f "$PROBE_NAME" > /dev/null 2>&1 || true
PROBE_STARTED=0

# ── Phase 4: churn — 3× stop/write/start on 10 workers ──────────────────────

banner "Phase 4: Churn — 3 cycles of stop/write/start on 10 workers"

# Key totals at start of Phase 4:
#   100 (baseline) + 20 (crash) + 1 (probe key) = 121
# Each cycle adds 10 keys:  cycle 1 → 131, cycle 2 → 141, cycle 3 → 151

CHURN_WORKERS=$(list_running_workers 10)
CHURN_COUNT=$(echo "$CHURN_WORKERS" | grep -c . 2>/dev/null || echo 0)
if [ "$CHURN_COUNT" -lt 10 ]; then
    die "Need 10 running workers for churn test; found ${CHURN_COUNT} (expected ${RESILIENCE_WORKERS} running)"
fi
printf '  Churn targets: %s\n' "$(echo "$CHURN_WORKERS" | tr '\n' ' ')"

CHURN_PASS=0
for cycle in 1 2 3; do
    echo "  Cycle ${cycle}/3: stopping 10 workers…"
    echo "$CHURN_WORKERS" | xargs docker stop > /dev/null

    CYCLE_OFFSET=$(( 120 + (cycle - 1) * 10 ))
    echo "  Cycle ${cycle}/3: writing 10 keys at offset ${CYCLE_OFFSET} (resilience/churn/…)…"
    write_keys "$SEED_HOST" "resilience/churn" 10 "$CYCLE_OFFSET"

    EXPECTED_MIN=$(( 121 + cycle * 10 ))
    if ! poll_until 60 kv_at_least "$EXPECTED_MIN"; then
        fail "Cycle ${cycle}/3: only $(kv_count_on_mgmt) keys on mgmt (expected ${EXPECTED_MIN}+)"
        echo "$CHURN_WORKERS" | xargs docker start > /dev/null 2>&1 || true
        die "Key propagation failed in churn cycle ${cycle} — aborting"
    fi
    echo "  Cycle ${cycle}/3: ${EXPECTED_MIN}+ keys confirmed on mgmt"

    echo "  Cycle ${cycle}/3: restarting 10 workers…"
    echo "$CHURN_WORKERS" | xargs docker start > /dev/null

    if poll_until 120 cluster_at_least "$TOTAL_NODES"; then
        echo "  Cycle ${cycle}/3: cluster recovered to $(mgmt_node_count) nodes"
        CHURN_PASS=$(( CHURN_PASS + 1 ))
    else
        fail "Cycle ${cycle}/3: cluster did not recover to ${TOTAL_NODES} nodes within 120 s (saw $(mgmt_node_count))"
    fi
done

if [ "$CHURN_PASS" -eq 3 ]; then
    ok "Churn: 3/3 cycles — cluster recovered after each stop/start burst"
else
    fail "Churn: only ${CHURN_PASS}/3 cycles recovered cleanly"
fi

# Final key-count sanity check: 121 + 30 churn keys = 151
if poll_until 30 kv_at_least 151; then
    ok "Churn: all 151+ keys intact after 3 churn cycles"
else
    fail "Key count after churn: only $(kv_count_on_mgmt) (expected 151+)"
fi

# ── Summary ───────────────────────────────────────────────────────────────────

banner "Scale resilience test results"
printf '  Nodes: %d   Phases: 4   Pass: %d   Fail: %d\n' \
    "$TOTAL_NODES" "$PASS" "$FAIL"

if [ "$FAIL" -gt 0 ]; then
    printf '\033[0;31mFAIL\033[0m\n'
    exit 1
fi
printf '\033[0;32mPASS\033[0m — %d-node resilience test complete (crash/rejoin/anti-entropy/churn verified)\n' \
    "$TOTAL_NODES"
