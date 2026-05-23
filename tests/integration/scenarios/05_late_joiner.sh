#!/usr/bin/env bash
# Scenario 05: Anti-entropy late joiner.
#   Stop node-c, write keys on node-a and node-b, restart node-c.
#   node-c starts empty (no persistence) and must receive all prior keys
#   via anti-entropy after it rejoins the mesh.
set -euo pipefail
source /tests/lib/helpers.sh

NODE_A="${NODE_A_HOST:-node-a}"
NODE_B="${NODE_B_HOST:-node-b}"
NODE_C="${NODE_C_HOST:-node-c}"
KEY_A="test/s05/late-joiner-a"
KEY_B="test/s05/late-joiner-b"
VALUE_A="from-a-$$"
VALUE_B="from-b-$$"

# Take node-c offline so it misses the writes.
docker stop mycelium-test-node-c
sleep 2

# Write keys on the running nodes.
kv_put "$NODE_A" "$KEY_A" "$VALUE_A"
kv_put "$NODE_B" "$KEY_B" "$VALUE_B"

# Bring node-c back online (starts with empty state — no persistence).
docker start mycelium-test-node-c
wait_for_health "$NODE_C" "${NODE_HTTP_PORT:-8300}" 60

# node-c must receive both keys via anti-entropy from node-a / node-b.
poll_until 40 kv_check "$NODE_C" "$KEY_A" "$VALUE_A"
poll_until 40 kv_check "$NODE_C" "$KEY_B" "$VALUE_B"
