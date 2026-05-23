#!/usr/bin/env bash
# Scenario 04: Full-cluster restart.
#   node-a has persistence — key survives in WAL.
#   node-b has no persistence — recovers via anti-entropy from node-a.
set -euo pipefail
source /tests/lib/helpers.sh

NODE_A="${NODE_A_HOST:-node-a}"
NODE_B="${NODE_B_HOST:-node-b}"
KEY="test/s04/cluster-restart"
VALUE="full-restart-$$-$(date +%s)"

# Write on node-a and wait for propagation to node-b
kv_put "$NODE_A" "$KEY" "$VALUE"
poll_until 20 kv_check "$NODE_B" "$KEY" "$VALUE"

# Bring all data containers down
docker stop mycelium-test-node-a mycelium-test-node-b mycelium-test-node-c mycelium-test-mgmt
sleep 3

# Restart in order — node-a first so it is a source of truth for anti-entropy
docker start mycelium-test-node-a mycelium-test-node-b mycelium-test-mgmt

# Wait for both nodes to come back
wait_for_health "$NODE_A" "${NODE_HTTP_PORT:-8300}" 60
wait_for_health "$NODE_B" "${NODE_HTTP_PORT:-8300}" 60

# node-a: restored from WAL (no anti-entropy needed)
poll_until 15 kv_check "$NODE_A" "$KEY" "$VALUE"

# node-b: restored via anti-entropy from node-a (may take a few extra seconds)
poll_until 45 kv_check "$NODE_B" "$KEY" "$VALUE"
