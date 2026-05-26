#!/usr/bin/env bash
# Scenario 11: Agentic Flow Networks — KV ring as distributed pipeline buffer.
#
# Tests the three structural properties of an AFN against the existing 4-node cluster:
#
#   (I)  Substrate Unity — a KV write on node-a is immediately readable
#        on node-b and node-c (the buffer IS the cluster; no external queue)
#   (II) Topology Emergence — capability advertisements on node-b/c propagate
#        to node-a's resolver within seconds (no registry service)
#   (III) Stage Transitions — the write-to-next + delete-from-current pattern
#        moves 5 work items through stage-a → stage-b → done across the cluster,
#        with all-node visibility checked after each transition
#
# Uses the public /gateway/kv and /gateway/capability/resolve HTTP endpoints
# directly — the same surface the Python SDK calls.
set -euo pipefail
source /tests/lib/helpers.sh

H_A="http://${NODE_A_HOST:-node-a}:${NODE_HTTP_PORT:-8300}"
H_B="http://${NODE_B_HOST:-node-b}:${NODE_HTTP_PORT:-8300}"
H_C="http://${NODE_C_HOST:-node-c}:${NODE_HTTP_PORT:-8300}"

ITEMS=5
PFX="pipeline/afn11"  # unique prefix — no collision with other scenarios

# ── Encoding helpers ────────────────────────────────────────────────────────
urlencode() { printf '%s' "$1" | jq -sRr @uri; }
b64enc()    { printf '%s' "$1" | base64 | tr -d '\n'; }

# ── Gateway wrappers ────────────────────────────────────────────────────────
gw_kv_set() {   # gw_kv_set BASE_URL KEY VALUE_STR
    curl -sf --max-time 5 -X POST \
        -H "Content-Type: application/json" \
        -d "{\"key\":\"$2\",\"value_b64\":\"$(b64enc "$3")\"}" \
        "$1/gateway/kv" > /dev/null
}

gw_kv_del() {   # gw_kv_del BASE_URL KEY
    curl -sf --max-time 5 -X DELETE \
        "$1/gateway/kv?key=$(urlencode "$2")" > /dev/null
}

gw_kv_count() {   # gw_kv_count BASE_URL PREFIX → integer
    curl -sf --max-time 5 \
        "$1/gateway/kv/keys?prefix=$(urlencode "$2")" 2>/dev/null \
        | jq '.keys | length' 2>/dev/null || echo 0
}

gw_cap_count() {   # gw_cap_count BASE_URL NS NAME → integer
    curl -sf --max-time 5 \
        "$1/gateway/capability/resolve?ns=$2&name=$3" 2>/dev/null \
        | jq '.providers | length' 2>/dev/null || echo 0
}

# ── Phase 1: Seed 5 work items on node-a ────────────────────────────────────
for i in $(seq 0 $((ITEMS - 1))); do
    gw_kv_set "$H_A" "${PFX}/stage-a/item-${i}" \
        "{\"id\":\"item-${i}\",\"raw\":\"article body $i\"}"
done

# ── Phase 2 (I — Substrate Unity): buffer replicated to node-b and node-c ───
# The KV ring propagates writes epidemically. Every node already holds the buffer.
buffer_replicated() {
    local b_count c_count
    b_count=$(gw_kv_count "$H_B" "${PFX}/stage-a/")
    c_count=$(gw_kv_count "$H_C" "${PFX}/stage-a/")
    [ "$b_count" -ge "$ITEMS" ] && [ "$c_count" -ge "$ITEMS" ]
}
poll_until 30 buffer_replicated || {
    printf 'FAIL: stage-a buffer not replicated — node-b: %d  node-c: %d  (expected %d)\n' \
        "$(gw_kv_count "$H_B" "${PFX}/stage-a/")" \
        "$(gw_kv_count "$H_C" "${PFX}/stage-a/")" \
        "$ITEMS" >&2
    false
}

# ── Phase 3 (II — Topology Emergence): cap advertisement propagates ──────────
# Advertise stage_a/afn11worker on node-b and node-c; must appear in node-a's resolver.
curl -sf --max-time 5 -X POST \
    -H "Content-Type: application/json" \
    -d '{"ns":"stage_a","name":"afn11worker","interval_secs":30}' \
    "$H_B/gateway/capability/advertise" > /dev/null

curl -sf --max-time 5 -X POST \
    -H "Content-Type: application/json" \
    -d '{"ns":"stage_a","name":"afn11worker","interval_secs":30}' \
    "$H_C/gateway/capability/advertise" > /dev/null

workers_visible() {
    local count
    count=$(gw_cap_count "$H_A" "stage_a" "afn11worker")
    [ "$count" -ge 2 ]
}
poll_until 30 workers_visible || {
    printf 'FAIL: stage_a/afn11worker not visible from node-a: %d providers (expected ≥2)\n' \
        "$(gw_cap_count "$H_A" "stage_a" "afn11worker")" >&2
    false
}

# ── Phase 4 (III — Stage Transition A→B): node-b acts as worker ─────────────
# Simulates: worker reads item, writes enriched result to stage-b, deletes from stage-a.
for i in $(seq 0 $((ITEMS - 1))); do
    gw_kv_set "$H_B" "${PFX}/stage-b/item-${i}" \
        "{\"id\":\"item-${i}\",\"title\":\"parsed $i\",\"keywords\":[\"climate\",\"food\"]}"
    gw_kv_del "$H_B" "${PFX}/stage-a/item-${i}"
done

# Verify from node-a: stage-a empty, stage-b full
stage_a_drained() {
    local a_count b_count
    a_count=$(gw_kv_count "$H_A" "${PFX}/stage-a/")
    b_count=$(gw_kv_count "$H_A" "${PFX}/stage-b/")
    [ "$a_count" -eq 0 ] && [ "$b_count" -ge "$ITEMS" ]
}
poll_until 20 stage_a_drained || {
    printf 'FAIL: A→B transition — node-a stage-a: %d  stage-b: %d  (expected 0/%d)\n' \
        "$(gw_kv_count "$H_A" "${PFX}/stage-a/")" \
        "$(gw_kv_count "$H_A" "${PFX}/stage-b/")" \
        "$ITEMS" >&2
    false
}

# ── Phase 5 (III — Stage Transition B→Done): node-c acts as worker ──────────
for i in $(seq 0 $((ITEMS - 1))); do
    gw_kv_set "$H_C" "${PFX}/done/item-${i}" \
        "{\"id\":\"item-${i}\",\"composite_score\":0.87}"
    gw_kv_del "$H_C" "${PFX}/stage-b/item-${i}"
done

# Verify from node-a: stage-b empty, all items in done/
pipeline_complete() {
    local done_count b_count
    done_count=$(gw_kv_count "$H_A" "${PFX}/done/")
    b_count=$(gw_kv_count "$H_A" "${PFX}/stage-b/")
    [ "$done_count" -ge "$ITEMS" ] && [ "$b_count" -eq 0 ]
}
poll_until 20 pipeline_complete || {
    printf 'FAIL: B→Done transition — done: %d  stage-b remaining: %d  (expected %d/0)\n' \
        "$(gw_kv_count "$H_A" "${PFX}/done/")" \
        "$(gw_kv_count "$H_A" "${PFX}/stage-b/")" \
        "$ITEMS" >&2
    false
}

# ── Cleanup ──────────────────────────────────────────────────────────────────
for i in $(seq 0 $((ITEMS - 1))); do
    gw_kv_del "$H_A" "${PFX}/done/item-${i}" 2>/dev/null || true
done
