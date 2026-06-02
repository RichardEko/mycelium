#!/usr/bin/env bash
# three_node_demo Docker integration test — runs inside the runner container.
set -euo pipefail

HOST="${LLM_HOST:-llm}"
CHAT_PORT="${LLM_CHAT_PORT:-8080}"

PASS=0
FAIL=0

banner() { printf '\n\033[1;34m══ %s ══\033[0m\n' "$1"; }
ok()     { printf '\033[0;32mPASS\033[0m  %s\n' "$1"; PASS=$((PASS+1)); }
fail()   { printf '\033[0;31mFAIL\033[0m  %s\n' "$1"; FAIL=$((FAIL+1)); }

run_scenario() {
    local label="$1"; shift
    printf '  %-52s ' "$label"
    if "$@" 2>/tmp/last.err; then
        ok "$label"
    else
        fail "$label"
        sed 's/^/    /' /tmp/last.err >&2
    fi
}

# poll_until TIMEOUT_SECS CMD [ARGS…]
poll_until() {
    local timeout="$1"; shift
    local i=0
    while [ "$i" -lt "$timeout" ]; do
        if "$@" > /dev/null 2>&1; then return 0; fi
        sleep 1
        i=$((i+1))
    done
    echo "TIMEOUT: '$*' did not succeed within ${timeout}s" >&2
    return 1
}

# ── Phase 0: wait for chat server ────────────────────────────────────────────
banner "Waiting for chat server to be ready"

# The llm node waits for model-init (llama3.2 pull, ~2 GB on first run) and
# then TOOL_SETTLE_SECS=8 before binding the chat port.  Allow up to 10 min
# for the first-run model download; cached runs are much faster.
chat_ready() {
    curl -sf --max-time 5 "http://${HOST}:${CHAT_PORT}/mesh" > /dev/null 2>&1
}
poll_until 600 chat_ready
echo "  Chat server ready — starting scenarios"

# ── Scenarios ─────────────────────────────────────────────────────────────────
banner "Running scenarios"

# 01 — tool discovery: all 4 tools visible on /mesh
scenario_01() {
    local resp count i=0
    # Gossip may still be converging; wait up to 30s for all tools to propagate.
    while [ $i -lt 30 ]; do
        resp=$(curl -sf --max-time 5 "http://${HOST}:${CHAT_PORT}/mesh" 2>/dev/null \
               || echo '{"tools":[]}')
        count=$(echo "$resp" | jq '.tools | length')
        [ "${count:-0}" -ge 4 ] && break
        sleep 1
        i=$((i+1))
    done
    [ "${count:-0}" -ge 4 ] || {
        echo "expected ≥4 tools after 30s, got ${count:-0}" >&2; return 1
    }
    echo "  tools ($count): $(echo "$resp" | jq -r '[.tools[].name] | join(", ")')" >&2
}

# 02 — tool nodes healthy: all 4 tool /ready endpoints return 200
scenario_02() {
    local code
    for svc in tool-a tool-b tool-sf tool-book; do
        code=$(curl -s -o /dev/null -w "%{http_code}" --max-time 5 \
                    "http://${svc}:8300/ready")
        [ "$code" = "200" ] || { echo "$svc /ready returned HTTP $code" >&2; return 1; }
    done
}

# 03 — HTML chat UI: GET / returns text/html with >500 bytes of content
scenario_03() {
    local body_file; body_file=$(mktemp)
    local code
    code=$(curl -s --max-time 5 -o "$body_file" -w "%{http_code}" \
           "http://${HOST}:${CHAT_PORT}/")
    if [ "$code" != "200" ]; then
        echo "GET / returned HTTP $code" >&2; rm -f "$body_file"; return 1
    fi
    local size; size=$(wc -c < "$body_file")
    if [ "$size" -lt 500 ]; then
        echo "HTML body suspiciously small: ${size} bytes" >&2
        rm -f "$body_file"; return 1
    fi
    echo "  chat UI: ${size} bytes" >&2
    rm -f "$body_file"
}

# 04 — chat round-trip: POST /chat → SSE stream → Assistant event with content
#
# Flow: subscribe to SSE first (background curl), then POST the message.
# The planning_cycle drives the stub LLM which returns a canned answer with
# no tool_calls — so the cycle resolves in a single LLM step.
scenario_04() {
    local sse_file; sse_file=$(mktemp)

    # Subscribe before posting so we don't miss fast events.
    curl -sN --max-time 300 "http://${HOST}:${CHAT_PORT}/stream" > "$sse_file" &
    local SSE_PID=$!
    sleep 1  # let the SSE connection establish

    # POST the user message; expect 202 Accepted.
    local code
    code=$(curl -s -X POST -H "Content-Type: application/json" \
               -d '{"message":"What is 2 plus 2?"}' \
               -o /dev/null -w "%{http_code}" --max-time 10 \
               "http://${HOST}:${CHAT_PORT}/chat")
    if [ "$code" != "202" ]; then
        echo "POST /chat returned HTTP $code, expected 202" >&2
        kill "$SSE_PID" 2>/dev/null; rm -f "$sse_file"; return 1
    fi

    # Wait up to 180s — real LLM inference can be slow on CPU-only hardware.
    local i=0
    while [ $i -lt 180 ]; do
        if grep -q '"type":"assistant"' "$sse_file" 2>/dev/null; then break; fi
        sleep 1
        i=$((i+1))
    done

    kill "$SSE_PID" 2>/dev/null
    wait "$SSE_PID" 2>/dev/null || true

    if ! grep -q '"type":"assistant"' "$sse_file"; then
        echo "no assistant event after 180s; SSE events received:" >&2
        cat "$sse_file" >&2
        rm -f "$sse_file"; return 1
    fi

    # Extract content from the SSE data line: `data: {...}`
    local content
    content=$(grep '"type":"assistant"' "$sse_file" \
              | head -1 | sed 's/^data: //' | jq -r '.content' 2>/dev/null || true)
    if [ -z "$content" ]; then
        echo "assistant event has empty content" >&2; rm -f "$sse_file"; return 1
    fi

    echo "  assistant reply: $content" >&2
    rm -f "$sse_file"
}

run_scenario "01 mesh tool discovery (≥4 tools on /mesh)"                scenario_01
run_scenario "02 tool nodes healthy (tool-a, tool-b, tool-sf, tool-book)" scenario_02
run_scenario "03 HTML chat UI (GET /)"                         scenario_03
run_scenario "04 chat round-trip (POST /chat → SSE Assistant)" scenario_04

# ── Summary ───────────────────────────────────────────────────────────────────
banner "Results"
printf '  Passed: %d   Failed: %d\n\n' "$PASS" "$FAIL"
[ "$FAIL" -eq 0 ]
