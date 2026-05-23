#!/usr/bin/env bash
# Scenario 06: Signal emitted on node-a reaches node-b via epidemic propagation.
#   The 'node' role listens for 'test.signal' and writes the payload to
#   KV key "sig-received/{hostname}", making reception observable.
set -euo pipefail
source /tests/lib/helpers.sh

NODE_A="${NODE_A_HOST:-node-a}"
NODE_B="${NODE_B_HOST:-node-b}"
PORT="${NODE_HTTP_PORT:-8300}"
PAYLOAD="sig-test-$$-$(date +%s)"

# Emit test.signal from node-a with a unique payload
curl -sf --max-time 5 -X POST -d "$PAYLOAD" \
    "http://${NODE_A}:${PORT}/emit/test.signal" > /dev/null

# node-b must record the signal payload in sig-received/node-b
poll_until 20 kv_check "$NODE_B" "sig-received/node-b" "$PAYLOAD"
