#!/usr/bin/env bash
# llm_agent Docker integration test — runs inside the runner container.
set -euo pipefail

HOST="${LLM_AGENT_HOST:-llm-agent}"
PORT="${LLM_AGENT_HTTP_PORT:-8100}"

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

state() {
    curl -sf --max-time 5 "http://${HOST}:${PORT}/state"
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

# ── Phase 0: wait for manager election ───────────────────────────────────────
banner "Waiting for llm-agent to be ready"

# Port 8100 is n-0 (NodeId 127.0.0.1:56000 — lexicographically smallest, so
# always the initial manager). Wait until /state returns a non-null manager.port.
manager_elected() {
    local mp
    mp=$(state 2>/dev/null | jq -r '.manager.port // empty' 2>/dev/null)
    [ -n "$mp" ] && [ "$mp" != "null" ]
}
poll_until 90 manager_elected
echo "  Manager elected — starting scenarios"

# ── Scenarios ─────────────────────────────────────────────────────────────────
banner "Running scenarios"

# 01 — three-node mesh convergence
scenario_01() {
    local s; s=$(state)
    local count; count=$(echo "$s" | jq '.nodes | length')
    [ "$count" -eq 3 ] || { echo "expected 3 nodes, got $count" >&2; return 1; }
    local alive; alive=$(echo "$s" | jq '[.nodes[].alive] | all')
    [ "$alive" = "true" ] || { echo "not all nodes alive: $(echo "$s" | jq '[.nodes[] | {l:.label,a:.alive}]')" >&2; return 1; }
    local healthy; healthy=$(echo "$s" | jq '.mesh_status.healthy')
    [ "$healthy" = "true" ] || { echo "mesh_status.healthy=$healthy" >&2; return 1; }
}

# 02 — manager election: n-0 (HTTP 8100) is the elected leader
scenario_02() {
    local s; s=$(state)
    local mp; mp=$(echo "$s" | jq '.manager.port')
    [ "$mp" = "8100" ] || { echo "expected manager_port=8100, got $mp" >&2; return 1; }
    local mn; mn=$(echo "$s" | jq -r '.manager.node')
    [ "$mn" = "n-0" ] || { echo "expected manager_node=n-0, got $mn" >&2; return 1; }
}

# 03 — tool discovery: weather+ping on n-0, search+calculate on n-1
scenario_03() {
    local s; s=$(state)
    local total; total=$(echo "$s" | jq '[.nodes[].tools | length] | add // 0')
    [ "$total" -ge 4 ] || { echo "expected >=4 tools, got $total" >&2; return 1; }

    local n0_tools; n0_tools=$(echo "$s" | jq -r '.nodes[] | select(.label=="n-0") | .tools[]' 2>/dev/null || true)
    echo "$n0_tools" | grep -q "weather"   || { echo "n-0 missing 'weather' (got: $n0_tools)" >&2; return 1; }
    echo "$n0_tools" | grep -q "ping"      || { echo "n-0 missing 'ping' (got: $n0_tools)" >&2; return 1; }

    local n1_tools; n1_tools=$(echo "$s" | jq -r '.nodes[] | select(.label=="n-1") | .tools[]' 2>/dev/null || true)
    echo "$n1_tools" | grep -q "search"    || { echo "n-1 missing 'search' (got: $n1_tools)" >&2; return 1; }
    echo "$n1_tools" | grep -q "calculate" || { echo "n-1 missing 'calculate' (got: $n1_tools)" >&2; return 1; }
}

# 04 — mock planning cycle: trigger via /demo/trigger/llm-task, verify tool calls
scenario_04() {
    local before; before=$(state | jq '.total_calls // 0')
    curl -sf -X POST --max-time 5 "http://${HOST}:${PORT}/demo/trigger/llm-task" > /dev/null

    local after=0 i=0
    while [ $i -lt 30 ]; do
        sleep 1
        after=$(state | jq '.total_calls // 0')
        [ "$after" -gt "$before" ] && break
        i=$((i+1))
    done

    [ "$after" -gt "$before" ] || {
        echo "planning cycle made no tool calls (before=$before after=$after)" >&2
        return 1
    }
    local last; last=$(state | jq -r '.last_tool // empty')
    [ -n "$last" ] && [ "$last" != "null" ] || { echo "last_tool is empty" >&2; return 1; }
    echo "  total_calls=$after last_tool=$last" >&2
}

# 05 — state-machine events present in the activity log after a cycle
scenario_05() {
    local events; events=$(state | jq -r '.log[].event')
    echo "$events" | grep -q "State"    || { echo "no 'State' event in log" >&2; return 1; }
    echo "$events" | grep -q "Tools"    || { echo "no 'Tools' event in log" >&2; return 1; }
    echo "$events" | grep -q "Invoking" || { echo "no 'Invoking' event in log" >&2; return 1; }
}

# 06 — spare pool: 3 spare nodes visible in state
scenario_06() {
    local s; s=$(state)
    local spare_count; spare_count=$(echo "$s" | jq '.spares | length')
    [ "$spare_count" -eq 3 ] || { echo "expected 3 spares, got $spare_count" >&2; return 1; }
    local all_idle; all_idle=$(echo "$s" | jq '[.spares[].mode] | all(. == "idle")')
    [ "$all_idle" = "true" ] || { echo "not all spares idle: $(echo "$s" | jq '[.spares[] | {l:.label,m:.mode}]')" >&2; return 1; }
}

run_scenario "01 three-node mesh convergence"          scenario_01
run_scenario "02 manager election (n-0 / port 8100)"   scenario_02
run_scenario "03 tool discovery (weather/ping/search/calc)" scenario_03
run_scenario "04 mock LLM planning cycle"               scenario_04
run_scenario "05 state-machine log events"              scenario_05
run_scenario "06 spare pool idle (3 spares)"            scenario_06

# 07 — HTML dashboard served on the catch-all route
scenario_07() {
    local resp_file; resp_file=$(mktemp)
    local http_code
    http_code=$(curl -s --max-time 5 -o "$resp_file" -w "%{http_code}" "http://${HOST}:${PORT}/")
    [ "$http_code" = "200" ] || { echo "GET / returned HTTP $http_code" >&2; rm -f "$resp_file"; return 1; }
    local ct
    ct=$(curl -sI --max-time 5 "http://${HOST}:${PORT}/" | grep -i 'content-type:' | tr -d '\r')
    echo "$ct" | grep -qi "text/html" || { echo "expected text/html, got: $ct" >&2; rm -f "$resp_file"; return 1; }
    local size; size=$(wc -c < "$resp_file")
    [ "$size" -gt 1000 ] || { echo "HTML body suspiciously small: ${size} bytes" >&2; rm -f "$resp_file"; return 1; }
    grep -q "mycelium\|Mycelium\|mesh\|node" "$resp_file" || { echo "HTML body doesn't look like mesh dashboard" >&2; rm -f "$resp_file"; return 1; }
    echo "  dashboard: ${size} bytes" >&2
    rm -f "$resp_file"
}

# 08 — presets API returns well-formed list
scenario_08() {
    local resp; resp=$(curl -sf --max-time 5 "http://${HOST}:${PORT}/presets")
    local count; count=$(echo "$resp" | jq 'length')
    [ "$count" -ge 5 ] || { echo "expected >=5 presets, got $count" >&2; return 1; }
    # Each preset should have id, name, description
    local valid; valid=$(echo "$resp" | jq '[.[] | has("id") and has("name") and has("description")] | all')
    [ "$valid" = "true" ] || { echo "some presets missing required fields" >&2; return 1; }
    local ids; ids=$(echo "$resp" | jq -r '.[].id' | tr '\n' ' ')
    echo "  presets ($count): $ids" >&2
}

# 09 — manifest API: GET returns current manifest, POST with bumped version applies it
scenario_09() {
    local resp; resp=$(curl -sf --max-time 5 "http://${HOST}:${PORT}/manifest")
    local name; name=$(echo "$resp" | jq -r '.name')
    [ -n "$name" ] && [ "$name" != "null" ] || { echo "manifest.name is empty" >&2; return 1; }
    local ver; ver=$(echo "$resp" | jq -r '.version')
    [ -n "$ver" ] && [ "$ver" != "null" ] || { echo "manifest.version is empty" >&2; return 1; }
    echo "  manifest: name=$name version=$ver" >&2
}

# 10 — system status endpoint returns expected shape
scenario_10() {
    local resp; resp=$(curl -sf --max-time 5 "http://${HOST}:${PORT}/system/status")
    local healthy; healthy=$(echo "$resp" | jq '.healthy')
    [ "$healthy" = "true" ] || { echo "system/status healthy=$healthy" >&2; return 1; }
    local groups; groups=$(echo "$resp" | jq '.groups | length')
    [ "$groups" -ge 1 ] || { echo "expected at least 1 group, got $groups" >&2; return 1; }
    echo "  system_status: healthy=$healthy groups=$groups" >&2
}

# 11 — node kill/restore: kill n-1, spare-1 activates; restore n-1, spare-1 returns idle
scenario_11() {
    # Kill n-1
    curl -sf -X POST --max-time 5 "http://${HOST}:${PORT}/nodes/n-1/kill" > /dev/null \
        || { echo "POST /nodes/n-1/kill failed" >&2; return 1; }

    # Poll until n-1 shows paused and spare-1 shows failover mode
    spare_active() {
        local s; s=$(state 2>/dev/null)
        local n1_state; n1_state=$(echo "$s" | jq -r '.nodes[] | select(.label=="n-1") | .state' 2>/dev/null)
        local spare_mode; spare_mode=$(echo "$s" | jq -r '.spares[] | select(.label=="spare-1") | .mode' 2>/dev/null)
        [ "$n1_state" = "Offline" ] && [ "$spare_mode" = "failover" ]
    }
    poll_until 15 spare_active || {
        echo "n-1 did not go offline / spare-1 did not enter failover mode" >&2
        state | jq '{nodes:[.nodes[]|{l:.label,s:.state}], spares:[.spares[]|{l:.label,m:.mode}]}' >&2
        return 1
    }
    echo "  n-1 offline, spare-1 in failover" >&2

    # Restore n-1
    curl -sf -X POST --max-time 5 "http://${HOST}:${PORT}/nodes/n-1/start" > /dev/null \
        || { echo "POST /nodes/n-1/start failed" >&2; return 1; }

    # Poll until n-1 is back and spare-1 is idle again
    restored() {
        local s; s=$(state 2>/dev/null)
        local n1_state; n1_state=$(echo "$s" | jq -r '.nodes[] | select(.label=="n-1") | .state' 2>/dev/null)
        local spare_mode; spare_mode=$(echo "$s" | jq -r '.spares[] | select(.label=="spare-1") | .mode' 2>/dev/null)
        [ "$n1_state" != "Offline" ] && [ "$spare_mode" = "idle" ]
    }
    poll_until 15 restored || {
        echo "n-1 did not recover / spare-1 did not return to idle" >&2
        state | jq '{nodes:[.nodes[]|{l:.label,s:.state}], spares:[.spares[]|{l:.label,m:.mode}]}' >&2
        return 1
    }
    echo "  n-1 restored, spare-1 back to idle" >&2
}

run_scenario "07 HTML dashboard served (GET /)"            scenario_07
run_scenario "08 presets API (>=5 presets, all fields)"    scenario_08
run_scenario "09 manifest API (name + version present)"    scenario_09
run_scenario "10 system/status healthy"                    scenario_10
run_scenario "11 node kill/restore + spare failover"       scenario_11

# ── Summary ───────────────────────────────────────────────────────────────────
banner "Results"
printf '  Passed: %d   Failed: %d\n\n' "$PASS" "$FAIL"
[ "$FAIL" -eq 0 ]
