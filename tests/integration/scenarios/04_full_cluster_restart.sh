#!/usr/bin/env bash
# Scenario 04: Full-cluster restart (node-a, node-b, mgmt — not node-c).
#   node-a has persistence — key survives in WAL.
#   node-b has no persistence — recovers via anti-entropy from node-a or node-c.
#
# node-c is intentionally left running; it is exercised by scenario 05.
set -euo pipefail
source /tests/lib/helpers.sh

NODE_A="${NODE_A_HOST:-node-a}"
NODE_B="${NODE_B_HOST:-node-b}"
NODE_C="${NODE_C_HOST:-node-c}"
KEY="test/s04/cluster-restart"
VALUE="full-restart-$$-$(date +%s)"

# Write on node-a and wait for propagation to node-b and node-c.
# Ensuring node-c has the key before the stop guarantees anti-entropy recovery
# if WAL replay on node-a is slow (node-c stays up through the restart).
kv_put "$NODE_A" "$KEY" "$VALUE"
poll_until 20 kv_check "$NODE_B" "$KEY" "$VALUE"
poll_until 20 kv_check "$NODE_C" "$KEY" "$VALUE"

# Brief pause so the async WAL writer has time to flush the pending entry.
sleep 3

# Bring node-a, node-b, and mgmt down; leave node-c running for scenario 05.
docker stop mycelium-test-node-a mycelium-test-node-b mycelium-test-mgmt
sleep 3

docker start mycelium-test-node-a mycelium-test-node-b mycelium-test-mgmt

# Wait for both data nodes to come back
wait_for_health "$NODE_A" "${NODE_HTTP_PORT:-8300}" 60
wait_for_health "$NODE_B" "${NODE_HTTP_PORT:-8300}" 60

# node-a: restored from WAL (primary path), anti-entropy from node-c (fallback)
poll_until 60 kv_check "$NODE_A" "$KEY" "$VALUE"

# node-b: restored via anti-entropy from node-a or node-c
poll_until 60 kv_check "$NODE_B" "$KEY" "$VALUE"
