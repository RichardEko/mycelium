#!/usr/bin/env bash
# Start the 4-skill community: orchestrator, researcher, writer, verifier.
#
# Prerequisites:
#   cargo build --bin skillrunner
#   ollama pull llama3.2
#   ollama pull llama3.1:8b   # verifier uses a separate model for better precision
#
# Usage:
#   cd examples/community
#   ./start.sh
#   ./invoke.sh "gossip protocols"   # invoke from a separate terminal
#   ./stop.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
BIN="$REPO_ROOT/target/debug/skillrunner"

if [[ ! -x "$BIN" ]]; then
    echo "binary not found — run: cargo build --bin skillrunner"
    exit 1
fi

LOG_DIR="$SCRIPT_DIR/logs"
mkdir -p "$LOG_DIR"

echo "Starting orchestrator  (port 7950)..."
"$BIN" --skill "$SCRIPT_DIR/orchestrator.skill.toml" \
    > "$LOG_DIR/orchestrator.log" 2>&1 &
echo $! > "$LOG_DIR/orchestrator.pid"

# Wait until the seed's gossip port actually accepts before the spokes
# bootstrap to it — a fixed sleep raced the bind on cold targets and cost a
# full reconnect backoff (same fix as demo.sh).
for i in $(seq 1 50); do
    if (exec 3<>/dev/tcp/127.0.0.1/7950) 2>/dev/null; then exec 3>&- 3<&-; break; fi
    sleep 0.1
done

echo "Starting researcher    (port 7952)..."
"$BIN" --skill "$SCRIPT_DIR/researcher.skill.toml" \
    > "$LOG_DIR/researcher.log" 2>&1 &
echo $! > "$LOG_DIR/researcher.pid"

echo "Starting writer        (port 7953)..."
"$BIN" --skill "$SCRIPT_DIR/writer.skill.toml" \
    > "$LOG_DIR/writer.log" 2>&1 &
echo $! > "$LOG_DIR/writer.pid"

echo "Starting verifier      (port 7955)..."
"$BIN" --skill "$SCRIPT_DIR/verifier.skill.toml" \
    > "$LOG_DIR/verifier.log" 2>&1 &
echo $! > "$LOG_DIR/verifier.pid"

echo ""
echo "Community started. Logs: $LOG_DIR/"
echo "Wait ~3 s for gossip to converge, then:"
echo "  ./invoke.sh \"your topic here\""
echo ""
echo "Pipeline: researcher → writer → verifier (claims check)"
echo ""
echo "Stop with: ./stop.sh"
