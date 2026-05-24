#!/usr/bin/env bash
# Invoke the orchestrator skill with a topic and print the result.
#
# Usage:
#   ./invoke.sh "gossip protocols in distributed systems"
#   ./invoke.sh "the Rust ownership model" technical
#   ./invoke.sh "recent AI developments" casual 8

set -euo pipefail

TOPIC="${1:-gossip protocols}"
STYLE="${2:-technical}"
MAX_POINTS="${3:-5}"

ORCHESTRATOR_PORT=7950
CALLER_PORT=7960

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

echo "Invoking llm/orchestrator: topic=\"$TOPIC\" style=$STYLE"
echo ""

# Use cargo run --example invoke_skill with env vars to customise the call,
# OR invoke via Python if mycelium-py is installed.

# Python path (preferred — no recompile needed)
if command -v python3 &>/dev/null && python3 -c "import mycelium" 2>/dev/null; then
    python3 - <<PYEOF
import json, sys
sys.path.insert(0, "$REPO_ROOT/mycelium-py/src")
from mycelium import MyceliumAgent

agent = MyceliumAgent("127.0.0.1", 9050)   # http_port of orchestrator

providers = agent.resolve_capability("llm", "orchestrator")
if not providers:
    print("ERROR: orchestrator not found on mesh (is start.sh running?)")
    sys.exit(1)

node_id = providers[0].node_id
payload = json.dumps({
    "topic":      "$TOPIC",
    "style":      "$STYLE",
    "max_points": $MAX_POINTS,
}).encode()

result = agent.rpc_call(node_id, "skill.invoke", payload, timeout_secs=120)
data   = json.loads(result)

if "error" in data:
    print(f"ERROR: {data['error']}")
    sys.exit(1)

print(f"Title:   {data.get('title', '(no title)')}")
print(f"TL;DR:   {data.get('tldr',  '(no tldr)')}")
print()
print(data.get("article", "(no article)"))
PYEOF

else
    # Fallback: Rust example (requires recompile if source changed)
    cd "$REPO_ROOT"
    SKILL_TOPIC="$TOPIC" SKILL_NODE_PORT=$ORCHESTRATOR_PORT SKILL_CALLER_PORT=$CALLER_PORT \
        cargo run --example invoke_skill 2>/dev/null
fi
