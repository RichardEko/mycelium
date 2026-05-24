#!/usr/bin/env bash
# Start the 3-skill community: orchestrator, researcher, writer.
#
# Prerequisites:
#   cargo build --bin skillrunner
#   ollama pull llama3.2
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

sleep 1   # let the seed node bind before others connect

echo "Starting researcher    (port 7952)..."
"$BIN" --skill "$SCRIPT_DIR/researcher.skill.toml" \
    > "$LOG_DIR/researcher.log" 2>&1 &
echo $! > "$LOG_DIR/researcher.pid"

echo "Starting writer        (port 7953)..."
"$BIN" --skill "$SCRIPT_DIR/writer.skill.toml" \
    > "$LOG_DIR/writer.log" 2>&1 &
echo $! > "$LOG_DIR/writer.pid"

echo ""
echo "Community started. Logs: $LOG_DIR/"
echo "Wait ~2 s for gossip to converge, then:"
echo "  ./invoke.sh \"your topic here\""
echo ""
echo "Stop with: ./stop.sh"
