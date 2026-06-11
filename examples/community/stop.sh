#!/usr/bin/env bash
# Gracefully stop the community (sends SIGTERM to each SkillRunner process).

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LOG_DIR="$SCRIPT_DIR/logs"

for name in orchestrator researcher researcher2 writer verifier; do
    pid_file="$LOG_DIR/$name.pid"
    if [[ -f "$pid_file" ]]; then
        pid=$(cat "$pid_file")
        if kill -0 "$pid" 2>/dev/null; then
            echo "Stopping $name (PID $pid)..."
            kill "$pid"
        fi
        rm -f "$pid_file"
    fi
done

# Safety sweep: successive demo runs overwrite the pid files, so orphaned
# generations accumulate — and SO_REUSEPORT lets every generation share the
# same ports, silently round-robining requests between old and new binaries
# instead of failing loudly. Kill anything still running this demo's skills.
sleep 1
leftover=$(pgrep -f "skillrunner.*$SCRIPT_DIR" || true)
if [[ -n "$leftover" ]]; then
    echo "Sweeping orphaned skillrunner processes: $leftover"
    echo "$leftover" | xargs kill 2>/dev/null || true
fi

echo "Done."
