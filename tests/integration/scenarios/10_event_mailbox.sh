#!/usr/bin/env bash
# Scenario 10: Actor/Event mailbox — KV-backed event delivery.
#   POST /deliver-to-self on node-a writes an event to its own mailbox.
#   The open_mailbox watcher picks it up and increments a counter.
#   GET /mailbox-count must return count >= 1.
set -euo pipefail
source /tests/lib/helpers.sh

# Trigger delivery of one self-addressed event.
curl -sf --max-time 10 -X POST \
    "http://${NODE_A_HOST:-node-a}:${NODE_HTTP_PORT:-8300}/deliver-to-self" > /dev/null

# Poll until the counter is non-zero (watcher has delivered the event).
mailbox_received() {
    local resp
    resp=$(curl -sf --max-time 5 \
        "http://${NODE_A_HOST:-node-a}:${NODE_HTTP_PORT:-8300}/mailbox-count" 2>/dev/null) || return 1
    echo "$resp" | jq -e '.count >= 1' > /dev/null 2>&1
}

poll_until 30 mailbox_received || {
    echo "=== mailbox-count debug ===" >&2
    curl -sf --max-time 5 \
        "http://${NODE_A_HOST:-node-a}:${NODE_HTTP_PORT:-8300}/mailbox-count" 2>&1 | jq . >&2 || true
    false
}
