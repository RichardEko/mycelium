#!/usr/bin/env bash
# demo.sh — MCP tool dynamic discovery demo.
#
# Shows the dynamic discovery story in a single terminal session:
#   1. Starts the base cluster (tool-a, tool-b, llm, mgmt)
#   2. Waits for chat server readiness
#   3. Prints the initial tool list visible to the LLM
#   4. Starts tool-sf live — the LLM discovers it without any restart
#   5. Starts tool-book live — same
#   6. Prints the final tool list showing all four tools
#
# Then open http://localhost:8080 to try the new tools interactively:
#   "how does Dan Simmons fit into 1990s SF?"   → routed to sf_lookup
#   "what happens in Hyperion?"                 → routed to book_plot
#   "what's the weather in Tokyo?"              → routed to weather
#
# Prerequisites:
#   cargo build --example three_node_demo
#   ollama serve   (in a separate terminal)
#   ollama pull llama3.2

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
BIN="$REPO_ROOT/target/debug/examples/three_node_demo"
LOG_DIR="$SCRIPT_DIR/logs"

# ── helpers ────────────────────────────────────────────────────────────────────
die() { echo "ERROR: $*" >&2; exit 1; }

mesh_tool_count() {
    curl -sf --max-time 3 "http://localhost:8080/mesh" 2>/dev/null \
        | python3 -c "import sys,json; d=json.load(sys.stdin); print(len(d.get('tools',[])))" 2>/dev/null \
        || echo 0
}

mesh_tool_names() {
    curl -sf --max-time 3 "http://localhost:8080/mesh" 2>/dev/null \
        | python3 -c "import sys,json; d=json.load(sys.stdin); print(', '.join(t['name'] for t in d.get('tools',[])))" 2>/dev/null \
        || echo "(none yet)"
}

wait_for_chat() {
    echo "    waiting for chat server (model download may take a moment)..."
    for i in $(seq 1 120); do
        n=$(mesh_tool_count)
        if [[ "$n" -ge 1 ]]; then
            printf '\r    ✓ chat server ready\n'
            return 0
        fi
        printf '\r    … %ds' "$i"
        sleep 1
    done
    printf '\n'
    die "chat server did not become ready within 120s"
}

wait_for_tool() {
    local name="$1" target="$2"
    for i in $(seq 1 30); do
        n=$(mesh_tool_count)
        if [[ "$n" -ge "$target" ]]; then
            printf '\r    ✓ %s discovered (%d tools now visible)\n' "$name" "$n"
            return 0
        fi
        printf '\r    … waiting for %s (%ds)' "$name" "$i"
        sleep 1
    done
    printf '\n'
    echo "    ✗ timed out waiting for $name" >&2
    return 1
}

start_node() {
    local name="$1"; shift
    "$@" > "$LOG_DIR/${name}.log" 2>&1 &
    echo $! > "$LOG_DIR/${name}.pid"
}

# ── stop any previous run ─────────────────────────────────────────────────────
if ls "$LOG_DIR"/*.pid 2>/dev/null | grep -q . 2>/dev/null; then
    echo "Stopping previous cluster..."
    for pid_file in "$LOG_DIR"/*.pid; do
        pid=$(cat "$pid_file")
        kill "$pid" 2>/dev/null || true
        rm -f "$pid_file"
    done
    sleep 1
fi

[[ -x "$BIN" ]] || die "binary not found — run: cargo build --example three_node_demo"
mkdir -p "$LOG_DIR"

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Mycelium MCP Tools — Dynamic Discovery Demo"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# ── 1: start base cluster ─────────────────────────────────────────────────────
echo ""
echo "[1/4] Starting base cluster (tool-a, tool-b, llm, mgmt)..."

start_node tool-a \
    env MYCELIUM_HOSTNAME=127.0.0.1 MYCELIUM_ROLE=tool-a \
        MYCELIUM_PORT=57000 MYCELIUM_HTTP_PORT=8300 \
        MYCELIUM_PEERS="127.0.0.1:57001,127.0.0.1:57002,127.0.0.1:57003,127.0.0.1:57006" \
    "$BIN"

start_node tool-b \
    env MYCELIUM_HOSTNAME=127.0.0.1 MYCELIUM_ROLE=tool-b \
        MYCELIUM_PORT=57001 MYCELIUM_HTTP_PORT=8301 \
        MYCELIUM_PEERS="127.0.0.1:57000,127.0.0.1:57002,127.0.0.1:57003,127.0.0.1:57006" \
    "$BIN"

start_node llm \
    env MYCELIUM_HOSTNAME=127.0.0.1 MYCELIUM_ROLE=llm \
        MYCELIUM_PORT=57002 MYCELIUM_HTTP_PORT=8302 CHAT_PORT=8080 \
        MYCELIUM_PEERS="127.0.0.1:57000,127.0.0.1:57001,127.0.0.1:57003,127.0.0.1:57006" \
        OLLAMA_BASE_URL="${OLLAMA_BASE_URL:-http://localhost:11434/v1}" \
        OLLAMA_MODEL="${OLLAMA_MODEL:-llama3.2}" \
    "$BIN"

start_node mgmt \
    env MYCELIUM_HOSTNAME=127.0.0.1 MYCELIUM_ROLE=mgmt \
        MYCELIUM_PORT=57003 MYCELIUM_HTTP_PORT=8303 MGMT_PORT=8090 \
        MYCELIUM_PEERS="127.0.0.1:57000,127.0.0.1:57001,127.0.0.1:57002,127.0.0.1:57006" \
    "$BIN"

start_node verifier \
    env MYCELIUM_HOSTNAME=127.0.0.1 MYCELIUM_ROLE=verifier \
        MYCELIUM_PORT=57006 MYCELIUM_HTTP_PORT=8306 \
        MYCELIUM_PEERS="127.0.0.1:57000,127.0.0.1:57001,127.0.0.1:57002,127.0.0.1:57003" \
        OLLAMA_BASE_URL="${OLLAMA_BASE_URL:-http://localhost:11434/v1}" \
        VERIFIER_MODEL="${VERIFIER_MODEL:-llama3.1:8b}" \
    "$BIN"

# ── 2: wait for chat server ───────────────────────────────────────────────────
echo ""
echo "[2/4] Waiting for LLM node to be ready..."
wait_for_chat
echo "      Initial tools: $(mesh_tool_names)"
echo "      Chat UI:    http://localhost:8080"
echo "      Dashboard:  http://localhost:8090"

# ── 3: add tool-sf live ───────────────────────────────────────────────────────
echo ""
echo "[3/4] Starting tool-sf (SF Encyclopedia) — the LLM discovers it live..."
echo "      No restart. No config change. The mesh is the registry."

start_node tool-sf \
    env MYCELIUM_HOSTNAME=127.0.0.1 MYCELIUM_ROLE=tool-sf \
        MYCELIUM_PORT=57004 MYCELIUM_HTTP_PORT=8304 \
        MYCELIUM_PEERS="127.0.0.1:57000,127.0.0.1:57001,127.0.0.1:57002,127.0.0.1:57003" \
    "$BIN"

n_before=$(mesh_tool_count)
wait_for_tool "sf_lookup" $((n_before + 1))

# ── 4: add tool-book live ─────────────────────────────────────────────────────
echo ""
echo "[4/4] Starting tool-book (Wikipedia plot) — same live discovery..."

start_node tool-book \
    env MYCELIUM_HOSTNAME=127.0.0.1 MYCELIUM_ROLE=tool-book \
        MYCELIUM_PORT=57005 MYCELIUM_HTTP_PORT=8305 \
        MYCELIUM_PEERS="127.0.0.1:57000,127.0.0.1:57001,127.0.0.1:57002,127.0.0.1:57003" \
    "$BIN"

n_before=$(mesh_tool_count)
wait_for_tool "book_plot" $((n_before + 1))

# ── summary ───────────────────────────────────────────────────────────────────
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  All 4 tools live: $(mesh_tool_names)"
echo ""
echo "  Claims verifier active — answers checked against tool results."
echo ""
echo "  Try these in the chat UI (http://localhost:8080):"
echo "    \"how does Dan Simmons fit into 1990s SF?\""
echo "    \"what happens in Hyperion?\""
echo "    \"what is 330 times 1024?\""
echo "    \"what's the weather in Tokyo?\""
echo ""
echo "  Mesh dashboard:  http://localhost:8090"
echo "  Stop:            ./stop.sh"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
