#!/usr/bin/env bash
# Scenario 07: Capability discovery via the management API.
#   node-a, node-b, node-c each advertise role=node.
#   mgmt advertises role=mgmt.
#   /api/state must converge to show 4+ nodes with at least one mgmt
#   and two node roles.  Uses poll_until so post-restart gossip
#   propagation latency does not cause spurious failures.
set -euo pipefail
source /tests/lib/helpers.sh

all_roles_visible() {
    local state
    state=$(mgmt_state 2>/dev/null) || return 1

    local total
    total=$(echo "$state" | jq '.nodes | length' 2>/dev/null || echo 0)
    [ "$total" -ge 4 ] || return 1

    local roles
    roles=$(echo "$state" | jq -r '.nodes[].role' 2>/dev/null)
    echo "$roles" | grep -q "^mgmt$"  || return 1

    local node_count
    node_count=$(echo "$roles" | grep -c "^node$" || true)
    [ "$node_count" -ge 2 ]
}

poll_until 120 all_roles_visible || {
    echo "=== /api/state at timeout ===" >&2
    mgmt_state 2>&1 | jq . >&2 || mgmt_state >&2
    echo "=== cap/ KV entries on mgmt ===" >&2
    curl -sf --max-time 5 "http://${MGMT_HOST:-mgmt}:${MGMT_HTTP_PORT:-8090}/api/kv-scan?prefix=cap/" 2>&1 | jq . >&2 || true
    false
}
