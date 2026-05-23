#!/usr/bin/env bash
# Scenario 05: Anti-entropy late joiner.
#   node-c starts with a 25 s sleep (see docker-compose.test.yml).
#   Keys written on node-a and node-b before node-c joins must reach
#   node-c via anti-entropy after it comes online.
set -euo pipefail
source /tests/lib/helpers.sh

NODE_A="${NODE_A_HOST:-node-a}"
NODE_B="${NODE_B_HOST:-node-b}"
NODE_C="${NODE_C_HOST:-node-c}"
KEY_A="test/s05/late-joiner-a"
KEY_B="test/s05/late-joiner-b"
VALUE_A="from-a-$$"
VALUE_B="from-b-$$"

# Write keys on node-a and node-b while node-c is still sleeping
kv_put "$NODE_A" "$KEY_A" "$VALUE_A"
kv_put "$NODE_B" "$KEY_B" "$VALUE_B"

# Wait for node-c to start (it may need up to 60 s from container launch)
wait_for_health "$NODE_C" "${NODE_HTTP_PORT:-8300}" 90

# node-c must receive both keys via anti-entropy
poll_until 40 kv_check "$NODE_C" "$KEY_A" "$VALUE_A"
poll_until 40 kv_check "$NODE_C" "$KEY_B" "$VALUE_B"
