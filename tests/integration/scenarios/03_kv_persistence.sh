#!/usr/bin/env bash
# Scenario 03: KV state on node-a (which has MYCELIUM_DATA_DIR set) survives
# a container restart without needing anti-entropy from peers.
set -euo pipefail
source /tests/lib/helpers.sh

NODE_A="${NODE_A_HOST:-node-a}"
KEY="test/s03/persistent"
VALUE="persist-$$-$(date +%s)"

# Write key and confirm it's readable
kv_put "$NODE_A" "$KEY" "$VALUE"
poll_until 5 kv_check "$NODE_A" "$KEY" "$VALUE"

# Stop and restart node-a
docker stop mycelium-test-node-a
sleep 2
docker start mycelium-test-node-a

# Wait for node-a to become healthy again
wait_for_health "$NODE_A" "${NODE_HTTP_PORT:-8300}" 45

# Key must be present from the WAL replay — before anti-entropy would have
# had a chance to run (node-a connects back to peers a few seconds later).
poll_until 15 kv_check "$NODE_A" "$KEY" "$VALUE"
