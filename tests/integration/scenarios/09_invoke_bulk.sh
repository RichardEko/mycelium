#!/usr/bin/env bash
# Scenario 09: invoke.bulk large-payload RPC.
#   POST /bulk-echo-peer on node-a picks its first gossip peer, stages a
#   4096-byte payload, and bulk-calls the peer.  The peer echoes it back.
#   The response must report ok=true and echoed_size=4096.
set -euo pipefail
source /tests/lib/helpers.sh

bulk_ok() {
    local resp
    resp=$(curl -sf --max-time 20 -X POST \
        "http://${NODE_A_HOST:-node-a}:${NODE_HTTP_PORT:-8300}/bulk-echo-peer" 2>/dev/null) || return 1
    echo "$resp" | jq -e '.ok == true and .echoed_size == 4096' > /dev/null 2>&1
}

# node-a needs at least one peer with bulk_serve registered.
# Wait up to 90s to allow late-joiners to connect.
poll_until 90 bulk_ok || {
    echo "=== bulk-echo-peer debug ===" >&2
    curl -sf --max-time 10 \
        "http://${NODE_A_HOST:-node-a}:${NODE_HTTP_PORT:-8300}/bulk-echo-peer" 2>&1 | jq . >&2 || true
    false
}
