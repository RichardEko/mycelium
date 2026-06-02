#!/usr/bin/env bash
# Stop all MCP chat cluster nodes started by start.sh or demo.sh.

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LOG_DIR="$SCRIPT_DIR/logs"

for name in tool-a tool-b llm mgmt tool-sf tool-book; do
    pid_file="$LOG_DIR/${name}.pid"
    if [[ -f "$pid_file" ]]; then
        pid=$(cat "$pid_file")
        if kill -0 "$pid" 2>/dev/null; then
            echo "Stopping $name (PID $pid)..."
            kill "$pid"
        fi
        rm -f "$pid_file"
    fi
done

echo "Done."
