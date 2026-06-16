#!/usr/bin/env bash
# WS-B Phase 0 — scale baseline instrumentation (HOST-SIDE).
#
# Records the "before" curve the rest of WS-B is measured against (see
# docs/plans/v2-wsb-scale-transport.md §Phase 0). For each worker count it brings
# up the scale cluster, waits for convergence, and captures the two metrics that
# characterise the O(N²) connection ceiling:
#
#   - seed ESTABLISHED count   — persistent TCP connections terminating on seed.
#                                Grows ~linearly with N today (G1 baseline).
#   - host conntrack count     — the Docker-VM netfilter conntrack table size; the
#                                quantity that actually saturates the bridge.
#   - FORWARD-chain rule count — best-effort iptables -S FORWARD | wc -l (na if the
#                                privileged probe image / iptables is unavailable).
#
# It also records seed task_count (per-peer-writer fan-out proxy), store_entries,
# and dropped_frames from /stats.
#
# This script runs on the DOCKER HOST (not inside a container) — the test-runner
# container cannot see the seed's netns or the host FORWARD chain. It uses
# `docker exec` against the fixed container names in docker-compose.scale.yml.
#
# Usage:
#   tests/integration/measure_scale_baseline.sh
#   BASELINE_WORKERS="30 50 70 100" tests/integration/measure_scale_baseline.sh
#   BASELINE_WORKERS="30" CONVERGE_TIMEOUT=300 tests/integration/measure_scale_baseline.sh
#
# Output: appends one CSV row per worker count to
#   tests/integration/baseline/scale-baseline.csv
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

COMPOSE=(docker compose -f tests/integration/docker-compose.scale.yml)
SEED=mycelium-scale-seed
MGMT=mycelium-scale-mgmt

BASELINE_WORKERS="${BASELINE_WORKERS:-30 50 70 100}"
CONVERGE_TIMEOUT="${CONVERGE_TIMEOUT:-240}"   # seconds to wait for cluster convergence
SETTLE_SECS="${SETTLE_SECS:-20}"              # let peer-exchange connections finish forming
PROBE_IMAGE="${PROBE_IMAGE:-alpine:3.19}"     # privileged --net=host probe for VM metrics

OUT_DIR="tests/integration/baseline"
OUT_CSV="${OUT_CSV:-$OUT_DIR/scale-baseline.csv}"
mkdir -p "$OUT_DIR"

log() { printf '\033[1;34m[baseline]\033[0m %s\n' "$*" >&2; }

if [ ! -f "$OUT_CSV" ]; then
    echo "timestamp,git_sha,n_workers,total_nodes,converged,seed_established,host_conntrack_count,host_conntrack_max,forward_rules,seed_task_count,seed_store_entries,seed_dropped_frames" > "$OUT_CSV"
fi

# NOTE: `docker exec` takes the container NAME (mycelium-scale-seed); `docker
# compose exec` takes the SERVICE name (seed). We use plain `docker exec` here.

# Count ESTABLISHED (state hex 01) sockets in the seed container's netns.
seed_established() {
    docker exec -i "$SEED" sh -c \
        'cat /proc/net/tcp /proc/net/tcp6 2>/dev/null | awk "NR>1 && \$4==\"01\"" | wc -l' \
        2>/dev/null | tr -d '[:space:]' || echo na
}

# Pull a numeric field from seed /stats (served on the in-container HTTP port 8300).
seed_stat() {
    local field="$1"
    docker exec -i "$SEED" sh -c \
        "curl -sf --max-time 5 http://localhost:8300/stats 2>/dev/null" 2>/dev/null \
        | sed -n "s/.*\"${field}\"[: ]*\([0-9][0-9]*\).*/\1/p" | head -1 || true
}

# Host-VM netfilter metrics via a privileged --net=host probe container.
# Reads conntrack count/max from procfs (no package needed); FORWARD rule count
# best-effort (na if iptables is not present in the probe image).
host_vm_metrics() {
    docker run --rm --privileged --network host "$PROBE_IMAGE" sh -c '
        cc=$(cat /proc/sys/net/netfilter/nf_conntrack_count 2>/dev/null || echo na)
        cm=$(cat /proc/sys/net/netfilter/nf_conntrack_max   2>/dev/null || echo na)
        command -v iptables >/dev/null 2>&1 || apk add --no-cache iptables >/dev/null 2>&1 || true
        fr=$(iptables -S FORWARD 2>/dev/null | wc -l); [ "$fr" = "0" ] && fr=na
        echo "$cc $cm $fr"
    ' 2>/dev/null || echo "na na na"
}

# Count nodes mgmt currently sees. /api/state returns {"nodes":[{…,"is_self":…},…]};
# each node object has exactly one "is_self" key, so counting them is jq-free and
# unambiguous (the node id key is "id", which would collide with other "id"s).
mgmt_node_count() {
    docker exec -i "$MGMT" sh -c \
        'curl -sf --max-time 5 http://localhost:8090/api/state 2>/dev/null' 2>/dev/null \
        | grep -o '"is_self"' | wc -l | tr -d '[:space:]'
}

measure_one() {
    local n="$1"
    local total=$(( n + 1 ))
    log "=== N=${n} workers (total ${total} nodes incl. seed) ==="

    "${COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
    log "bringing up seed + mgmt + ${n} workers…"
    "${COMPOSE[@]}" up -d --build --scale worker="$n" seed mgmt worker >/dev/null 2>&1

    # Wait for convergence: mgmt must see >= total nodes (it also sees itself).
    local converged=no deadline=$(( SECONDS + CONVERGE_TIMEOUT )) seen=0
    while [ "$SECONDS" -lt "$deadline" ]; do
        seen=$(mgmt_node_count 2>/dev/null || echo 0)
        [ -z "$seen" ] && seen=0
        if [ "$seen" -ge "$total" ]; then converged=yes; break; fi
        sleep 5
    done
    log "convergence=${converged} (mgmt sees ${seen}/${total}); settling ${SETTLE_SECS}s…"
    sleep "$SETTLE_SECS"

    local est tc se df cc cm fr vm
    est=$(seed_established);            est=${est:-na}
    tc=$(seed_stat task_count);         tc=${tc:-na}
    se=$(seed_stat store_entries);      se=${se:-na}
    df=$(seed_stat dropped_frames);     df=${df:-na}
    vm=$(host_vm_metrics)
    cc=$(echo "$vm" | awk '{print $1}')
    cm=$(echo "$vm" | awk '{print $2}')
    fr=$(echo "$vm" | awk '{print $3}')

    local sha; sha=$(git rev-parse --short HEAD 2>/dev/null || echo unknown)
    local row="$(date -u +%FT%TZ),${sha},${n},${total},${converged},${est},${cc},${cm},${fr},${tc},${se},${df}"
    echo "$row" >> "$OUT_CSV"
    log "recorded: seed_established=${est} conntrack=${cc}/${cm} forward_rules=${fr} task_count=${tc} dropped=${df}"

    "${COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
}

log "writing to ${OUT_CSV}"
for n in $BASELINE_WORKERS; do
    measure_one "$n"
done
log "done. Curve:"
column -t -s, "$OUT_CSV" >&2
