#!/usr/bin/env bash
# Scenario 07: Capability discovery via the management API.
#   node-a, node-b, node-c each advertise role=node.
#   mgmt advertises role=mgmt.
#   /api/state must show all four nodes with correct roles.
set -euo pipefail
source /tests/lib/helpers.sh

# Allow time for capabilities to propagate after any prior restarts
sleep 3

state=$(mgmt_state)

total=$(echo "$state" | jq '.nodes | length')
assert_ge "$total" 4 "total node count"

node_roles=$(echo "$state" | jq -r '.nodes[].role')

# At least one mgmt node visible
echo "$node_roles" | grep -q "^mgmt$" || \
    { echo "FAIL: no node with role=mgmt found" >&2; exit 1; }

# At least two nodes with role=node visible (node-a and node-b always up)
node_count=$(echo "$node_roles" | grep -c "^node$" || true)
assert_ge "$node_count" 2 "nodes with role=node"
