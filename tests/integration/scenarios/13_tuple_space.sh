#!/usr/bin/env bash
# Scenario 13: TupleSpace pipeline — pull-based work distribution.
#
# node-a runs the ts13 tuple space as primary, node-b as secondary mirror.
# Tests the cross-node properties unit tests cannot cover:
#
#   (I)   Primary capability is discoverable cluster-wide (/api/tuple role)
#   (II)  put on node-a / take on node-b — client ops route via RPC to the
#         primary, ids preserved, payloads round-trip through base64
#   (III) In-flight accounting — taken-not-acked items show as inflight in
#         depth, and terminal acks clear them
#   (IV)  Monitoring aggregation — /api/tuple reports both roles and the
#         put/take counters
#   (V)   Empty take times out with 408 (the blocking-pull contract)
#
# Timing note (#150): this runs *after* scenario 04's full-cluster restart, so node-b's
# secondary role + monitoring counters must RE-converge and re-gossip into node-a's /api/tuple
# view. The poll windows below (45/30/60s) are sized for that post-restart re-convergence under
# Docker load — the roles are correct (S13 passes clean on a fresh cluster); the old 30/15/30s
# windows sat right at the edge and flaked ~50%.
set -euo pipefail
source /tests/lib/helpers.sh

H_A="http://${NODE_A_HOST:-node-a}:${NODE_HTTP_PORT:-8300}"
H_B="http://${NODE_B_HOST:-node-b}:${NODE_HTTP_PORT:-8300}"
NS="ts13"

b64() { printf '%s' "$1" | base64 | tr -d '\n'; }

# ── Phase I: primary discoverable ────────────────────────────────────────────
primary_visible() {
    local role
    role=$(curl -s --max-time 3 "${H_A}/api/tuple" 2>/dev/null \
        | jq -r '.nodes[] | select(.ns=="'"$NS"'") | select(.role=="primary") | .role' \
        2>/dev/null | head -1)
    [ "$role" = "primary" ]
}
poll_until 45 primary_visible || {
    printf 'FAIL: ts13 primary not visible in /api/tuple within 30s\n' >&2
    false
}

# ── Phase II: put 10 on node-a, take 10 on node-b ────────────────────────────
put_ids=""
for i in $(seq 0 9); do
    id=$(curl -sf --max-time 5 -X POST -H "Content-Type: application/json" \
        -d "{\"ns\":\"$NS\",\"stage\":\"s13-work\",\"payload_b64\":\"$(b64 "item-$i")\"}" \
        "${H_A}/gateway/tuple/put" | jq -r '.id')
    put_ids="$put_ids $id"
done
put_count=$(echo "$put_ids" | tr ' ' '\n' | grep -c '^[0-9]' || true)
assert_eq "$put_count" 10 "10 puts must return 10 ids"

take_ids=""
for _ in $(seq 0 9); do
    body=$(curl -sf --max-time 10 -X POST -H "Content-Type: application/json" \
        -d "{\"ns\":\"$NS\",\"stage\":\"s13-work\",\"timeout_secs\":5}" \
        "${H_B}/gateway/tuple/take")
    take_ids="$take_ids $(echo "$body" | jq -r '.id')"
done
# Every put id was taken exactly once (set equality, order-independent).
sorted_put=$(echo "$put_ids"  | tr ' ' '\n' | grep '^[0-9]' | sort -n | tr '\n' ',')
sorted_take=$(echo "$take_ids" | tr ' ' '\n' | grep '^[0-9]' | sort -n | tr '\n' ',')
assert_eq "$sorted_take" "$sorted_put" "taken ids must equal put ids"

# ── Phase III: in-flight accounting ──────────────────────────────────────────
inflight=$(curl -sf --max-time 5 \
    "${H_B}/gateway/tuple/depth?ns=$NS&stage=s13-work" \
    | jq -r '.stages[0].inflight')
assert_eq "$inflight" 10 "all taken items must be in-flight before ack"

for id in $take_ids; do
    curl -sf --max-time 5 -X POST -H "Content-Type: application/json" \
        -d "{\"ns\":\"$NS\",\"id\":$id}" "${H_B}/gateway/tuple/ack" > /dev/null
done
inflight_after() {
    local n
    n=$(curl -s --max-time 3 "${H_B}/gateway/tuple/depth?ns=$NS&stage=s13-work" \
        | jq -r '.stages[0].inflight' 2>/dev/null || echo -1)
    [ "$n" = "0" ]
}
poll_until 30 inflight_after || {
    printf 'FAIL: inflight count did not return to 0 after acks\n' >&2
    false
}

# ── Phase IV: /api/tuple aggregation shows both roles and the counters ──────
counters_visible() {
    local doc puts takes secondary
    doc=$(curl -s --max-time 3 "${H_A}/api/tuple" 2>/dev/null) || return 1
    puts=$(echo "$doc" | jq -r '[.nodes[] | select(.ns=="'"$NS"'") | select(.role=="primary")
        | .stages[] | select(.stage=="s13-work") | .put_total][0] // 0')
    takes=$(echo "$doc" | jq -r '[.nodes[] | select(.ns=="'"$NS"'") | select(.role=="primary")
        | .stages[] | select(.stage=="s13-work") | .take_total][0] // 0')
    secondary=$(echo "$doc" | jq -r '[.nodes[] | select(.ns=="'"$NS"'")
        | select(.role=="secondary")] | length')
    [ "$puts" -ge 10 ] && [ "$takes" -ge 10 ] && [ "$secondary" -ge 1 ]
}
poll_until 60 counters_visible || {
    printf 'FAIL: /api/tuple never showed primary counters and a secondary\n' >&2
    false
}

# ── Phase V: empty take → 408 (blocking-pull contract) ───────────────────────
status=$(curl -s -o /dev/null -w "%{http_code}" --max-time 10 \
    -X POST -H "Content-Type: application/json" \
    -d "{\"ns\":\"$NS\",\"stage\":\"s13-empty\",\"timeout_secs\":1}" \
    "${H_B}/gateway/tuple/take")
assert_eq "$status" "408" "take on empty stage must time out with 408"
