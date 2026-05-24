#!/usr/bin/env bash
# Scenario 08: Scatter-gather fan-out.
#   POST /scatter on node-a fans out an echo-scatter RPC to all its gossip
#   peers.  At least one peer must respond (min_ok=1).  With node-b and
#   node-c both up and registered, we expect at least 1 responder.
set -euo pipefail
source /tests/lib/helpers.sh

scatter_ok() {
    local resp
    resp=$(curl -sf --max-time 15 -X POST \
        "http://${NODE_A_HOST:-node-a}:${NODE_HTTP_PORT:-8300}/scatter" 2>/dev/null) || return 1
    echo "$resp" | jq -e '.ok == true and .responders >= 1' > /dev/null 2>&1
}

# Wait up to 60s for at least one successful scatter from node-a.
poll_until 60 scatter_ok || {
    echo "=== scatter debug ===" >&2
    curl -sf --max-time 5 \
        "http://${NODE_A_HOST:-node-a}:${NODE_HTTP_PORT:-8300}/scatter" 2>&1 | jq . >&2 || true
    false
}
