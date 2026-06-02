#!/usr/bin/env bash
# Start the full MCP chat cluster: tool-a, tool-b, llm, mgmt, tool-sf, tool-book, verifier.
#
# All nodes run in the background. Open http://localhost:8080 to chat.
# Live tool discovery: tool-sf and tool-book appear in the LLM's tool list within
# a few seconds of this script running — no restart required.
#
# The verifier node intercepts LLM answers after tool use, decomposes them into
# atomic claims, and removes any not grounded in tool results.
#
# Prerequisites:
#   cargo build --example three_node_demo
#   ollama serve   (in a separate terminal)
#   ollama pull llama3.2
#   ollama pull llama3.1:8b   # verifier model (optional; falls back to llama3.2)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
BIN="$REPO_ROOT/target/debug/examples/three_node_demo"
LOG_DIR="$SCRIPT_DIR/logs"

[[ -x "$BIN" ]] || { echo "binary not found — run: cargo build --example three_node_demo"; exit 1; }

# Stop any previous run cleanly
if ls "$LOG_DIR"/*.pid 2>/dev/null | grep -q . 2>/dev/null; then
    echo "Stopping previous cluster..."
    for pid_file in "$LOG_DIR"/*.pid; do
        pid=$(cat "$pid_file")
        kill "$pid" 2>/dev/null || true
        rm -f "$pid_file"
    done
    sleep 1
fi

mkdir -p "$LOG_DIR"

# Port assignment:
#   tool-a    gossip 57000  gateway 8300
#   tool-b    gossip 57001  gateway 8301
#   llm       gossip 57002  gateway 8302  chat 8080
#   mgmt      gossip 57003  gateway 8303  dashboard 8090
#   tool-sf   gossip 57004  gateway 8304
#   tool-book gossip 57005  gateway 8305
#   verifier  gossip 57006  gateway 8306

start_node() {
    local name="$1"; shift
    "$@" > "$LOG_DIR/${name}.log" 2>&1 &
    echo $! > "$LOG_DIR/${name}.pid"
}

ALL_PEERS="127.0.0.1:57001,127.0.0.1:57002,127.0.0.1:57003,127.0.0.1:57004,127.0.0.1:57005,127.0.0.1:57006"
start_node tool-a \
    env MYCELIUM_HOSTNAME=127.0.0.1 MYCELIUM_ROLE=tool-a \
        MYCELIUM_PORT=57000 MYCELIUM_HTTP_PORT=8300 \
        MYCELIUM_PEERS="$ALL_PEERS" \
    "$BIN"

start_node tool-b \
    env MYCELIUM_HOSTNAME=127.0.0.1 MYCELIUM_ROLE=tool-b \
        MYCELIUM_PORT=57001 MYCELIUM_HTTP_PORT=8301 \
        MYCELIUM_PEERS="127.0.0.1:57000,127.0.0.1:57002,127.0.0.1:57003,127.0.0.1:57004,127.0.0.1:57005,127.0.0.1:57006" \
    "$BIN"

start_node llm \
    env MYCELIUM_HOSTNAME=127.0.0.1 MYCELIUM_ROLE=llm \
        MYCELIUM_PORT=57002 MYCELIUM_HTTP_PORT=8302 CHAT_PORT=8080 \
        MYCELIUM_PEERS="127.0.0.1:57000,127.0.0.1:57001,127.0.0.1:57003,127.0.0.1:57004,127.0.0.1:57005,127.0.0.1:57006" \
        OLLAMA_BASE_URL="${OLLAMA_BASE_URL:-http://localhost:11434/v1}" \
        OLLAMA_MODEL="${OLLAMA_MODEL:-llama3.2}" \
    "$BIN"

start_node mgmt \
    env MYCELIUM_HOSTNAME=127.0.0.1 MYCELIUM_ROLE=mgmt \
        MYCELIUM_PORT=57003 MYCELIUM_HTTP_PORT=8303 MGMT_PORT=8090 \
        MYCELIUM_PEERS="127.0.0.1:57000,127.0.0.1:57001,127.0.0.1:57002,127.0.0.1:57004,127.0.0.1:57005,127.0.0.1:57006" \
    "$BIN"

start_node tool-sf \
    env MYCELIUM_HOSTNAME=127.0.0.1 MYCELIUM_ROLE=tool-sf \
        MYCELIUM_PORT=57004 MYCELIUM_HTTP_PORT=8304 \
        MYCELIUM_PEERS="127.0.0.1:57000,127.0.0.1:57001,127.0.0.1:57002,127.0.0.1:57003" \
    "$BIN"

start_node tool-book \
    env MYCELIUM_HOSTNAME=127.0.0.1 MYCELIUM_ROLE=tool-book \
        MYCELIUM_PORT=57005 MYCELIUM_HTTP_PORT=8305 \
        MYCELIUM_PEERS="127.0.0.1:57000,127.0.0.1:57001,127.0.0.1:57002,127.0.0.1:57003" \
    "$BIN"

start_node verifier \
    env MYCELIUM_HOSTNAME=127.0.0.1 MYCELIUM_ROLE=verifier \
        MYCELIUM_PORT=57006 MYCELIUM_HTTP_PORT=8306 \
        MYCELIUM_PEERS="127.0.0.1:57000,127.0.0.1:57001,127.0.0.1:57002,127.0.0.1:57003" \
        OLLAMA_BASE_URL="${OLLAMA_BASE_URL:-http://localhost:11434/v1}" \
        VERIFIER_MODEL="${VERIFIER_MODEL:-llama3.1:8b}" \
    "$BIN"

echo ""
echo "Started 7 nodes — gossip converges in ~5s."
echo ""
echo "  Chat UI:      http://localhost:8080"
echo "  Mesh status:  http://localhost:8080/mesh"
echo "  Dashboard:    http://localhost:8090"
echo ""
echo "  Verifier (claims check) active — answers backed by tool results only."
echo ""
echo "Logs: $LOG_DIR/"
echo "Stop: ./stop.sh"
