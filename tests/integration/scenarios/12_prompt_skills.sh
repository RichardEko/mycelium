#!/usr/bin/env bash
# Scenario 12: Prompt Skills — KV propagation and cross-node invocation.
#
# Tests the two properties that unit tests cannot cover:
#
#   (I)  Template propagation — a PromptTemplate PUT on node-a replicates
#        to node-b via the gossip KV substrate within a few seconds
#
#   (II) Cross-node invocation — calling test/echo from node-b routes the
#        llm.invoke RPC to node-a's EchoBackend and returns output
#
# The `test/echo` skill is pre-registered by the `node` role in three_node_demo
# (EchoBackend — no real LLM required).
set -euo pipefail
source /tests/lib/helpers.sh

H_A="http://${NODE_A_HOST:-node-a}:${NODE_HTTP_PORT:-8300}"
H_B="http://${NODE_B_HOST:-node-b}:${NODE_HTTP_PORT:-8300}"

PFX="scenario12"

# ── Phase 1 (I — Template propagation): PUT on node-a, visible on node-b ────
curl -sf --max-time 5 -X PUT \
    -H "Content-Type: application/json" \
    -d "{\"system\":\"Propagation test.\",\"user_template\":\"${PFX}: {{input}}\",\"max_tokens\":64,\"temperature\":0.0,\"metadata\":{}}" \
    "${H_A}/gateway/prompts/demo/${PFX}" > /dev/null

template_visible_on_b() {
    local status
    status=$(curl -s -o /dev/null -w "%{http_code}" --max-time 5 \
        "${H_B}/gateway/prompts/demo/${PFX}" 2>/dev/null || echo 0)
    [ "$status" = "200" ]
}

poll_until 30 template_visible_on_b || {
    printf 'FAIL: demo/%s template not visible on node-b within 30s\n' "$PFX" >&2
    false
}

# ── Phase 2 (II — Cross-node invocation): call test/echo from node-b ─────────
# test/echo is pre-registered on all `node`-role containers by three_node_demo.
# Call it from node-b's gateway — the RPC routes to whichever node advertises it.
# timeout_ms stays below curl's --max-time so the gateway's RPC timeout (now a
# 504 with an {"error":...} body) always beats the curl deadline. No -f: on a
# 404/502/504 we want the error JSON captured and printed, not discarded
# (the original curl -sf flake printed an illegible "{}").
result=$(curl -s --max-time 15 -X POST \
    -H "Content-Type: application/json" \
    -d '{"ns":"test","name":"echo","input":"hello-scenario12","timeout_ms":10000}' \
    "${H_B}/gateway/llm/call" 2>/dev/null || echo '{}')

output=$(echo "$result" | jq -r '.output // ""' 2>/dev/null || echo "")

if [ -z "$output" ]; then
    printf 'FAIL: /gateway/llm/call returned no output — response: %s\n' "$result" >&2
    false
fi

if ! echo "$output" | grep -q "hello-scenario12"; then
    printf 'FAIL: expected output to contain "hello-scenario12", got: %s\n' "$output" >&2
    false
fi

# ── Cleanup ──────────────────────────────────────────────────────────────────
curl -sf --max-time 5 -X DELETE \
    "${H_A}/gateway/prompts/demo/${PFX}" > /dev/null 2>&1 || true
