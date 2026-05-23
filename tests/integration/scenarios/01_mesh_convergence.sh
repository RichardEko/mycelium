#!/usr/bin/env bash
# Scenario 01: KV write on node-a propagates to node-b via epidemic gossip.
set -euo pipefail
source /tests/lib/helpers.sh

KEY="test/s01/convergence"
VALUE="hello-from-a-$$"

# Write on node-a
kv_put "${NODE_A_HOST:-node-a}" "$KEY" "$VALUE"

# Must appear on node-b within 20 s (gossip TTL propagation)
poll_until 20 kv_check "${NODE_B_HOST:-node-b}" "$KEY" "$VALUE"
