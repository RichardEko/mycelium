#!/usr/bin/env bash
# Scenario 02: Management dashboard returns valid JSON and serves HTML.
set -euo pipefail
source /tests/lib/helpers.sh

MGMT="${MGMT_HOST:-mgmt}"
PORT="${MGMT_HTTP_PORT:-8090}"

# /api/state returns valid JSON with at least 2 nodes visible
state=$(curl -sf --max-time 5 "http://${MGMT}:${PORT}/api/state")
node_count=$(echo "$state" | jq '.nodes | length')
assert_ge "$node_count" 2 "nodes in /api/state"

# self_id must be present
self_id=$(echo "$state" | jq -r '.self_id')
[ -n "$self_id" ] || { echo "FAIL: self_id is empty" >&2; exit 1; }

# tcp_peers must be a number
tcp_peers=$(echo "$state" | jq '.tcp_peers')
[ "$tcp_peers" -ge 0 ] 2>/dev/null || { echo "FAIL: tcp_peers not a number: $tcp_peers" >&2; exit 1; }

# Dashboard HTML is served at /
html=$(curl -sf --max-time 5 "http://${MGMT}:${PORT}/")
echo "$html" | grep -q "Mycelium" || { echo "FAIL: dashboard HTML missing 'Mycelium'" >&2; exit 1; }
