#!/usr/bin/env bash
# demo.sh — end-to-end Skills dynamic discovery demo.
#
# What you will see:
#   1. Four skills start and gossip converges in ~3s — no coordinator
#   2. First invocation produces a verified article: researcher → writer → verifier
#   3. A second researcher joins the mesh live — zero restarts, zero config changes
#   4. Second invocation routes to either researcher automatically (load-balanced)
#   5. Management dashboard at http://localhost:9050/mgmt shows the live mesh state
#
# Prerequisites:
#   cargo build --bin skillrunner
#   ollama pull llama3.2
#   ollama pull llama3.1:8b   # verifier model

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
BIN="$REPO_ROOT/target/debug/skillrunner"
LOG_DIR="$SCRIPT_DIR/logs"

# ── helpers ────────────────────────────────────────────────────────────────────
die() { echo "ERROR: $*" >&2; exit 1; }

kv_count() {
    curl -sf "http://localhost:9050/gateway/kv/keys?prefix=$1" 2>/dev/null \
        | python3 -c "import sys,json; d=json.load(sys.stdin); print(len(d.get('keys',[])))" 2>/dev/null \
        || echo 0
}

# Count capability keys for one ns/name. Key shape is cap/{node}/{ns}/{name},
# so the node segment sits between the prefix and the capability — a plain
# prefix query cannot express it.
cap_count() {
    curl -sf "http://localhost:9050/gateway/kv/keys?prefix=cap/" 2>/dev/null \
        | python3 -c "import sys,json; d=json.load(sys.stdin); print(sum(1 for k in d.get('keys',[]) if k.endswith('/$1')))" 2>/dev/null \
        || echo 0
}

wait_for_cap() {
    local cap="$1" target="$2" label="$3"
    for i in $(seq 1 30); do
        n=$(cap_count "$cap")
        if [[ "$n" -ge "$target" ]]; then
            printf '\r    ✓ %s (%d providers)\n' "$label" "$n"
            return 0
        fi
        printf '\r    … %d/%d %s (%ds)' "$n" "$target" "$label" "$i"
        sleep 1
    done
    printf '\n'
    echo "    ✗ timed out waiting for $label" >&2
    return 1
}

wait_for_keys() {
    local prefix="$1" target="$2" label="$3"
    for i in $(seq 1 30); do
        n=$(kv_count "$prefix")
        if [[ "$n" -ge "$target" ]]; then
            printf '\r    ✓ %s (%d entries)\n' "$label" "$n"
            return 0
        fi
        printf '\r    … %d/%d %s (%ds)' "$n" "$target" "$label" "$i"
        sleep 1
    done
    printf '\n'
    echo "    ✗ timed out waiting for $label" >&2
    return 1
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

[[ -x "$BIN" ]] || die "binary not found — run: cargo build --bin skillrunner"
mkdir -p "$LOG_DIR"

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Mycelium Skills — Dynamic Discovery Demo"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# ── 1: start 4-skill cluster ──────────────────────────────────────────────────
echo ""
echo "[1/5] Starting orchestrator + researcher + writer + verifier..."
"$BIN" --skill "$SCRIPT_DIR/orchestrator.skill.toml" > "$LOG_DIR/orchestrator.log" 2>&1 &
echo $! > "$LOG_DIR/orchestrator.pid"
# Wait until the orchestrator's gossip port accepts before starting the
# spokes: they bootstrap to :7950, and a connection refused during the bind
# race costs a full reconnect backoff before the mesh heals (cold-start flake
# — a fixed sleep raced exactly this on a cold target/ build).
for i in $(seq 1 50); do
    if (exec 3<>/dev/tcp/127.0.0.1/7950) 2>/dev/null; then exec 3>&- 3<&-; break; fi
    sleep 0.1
done
"$BIN" --skill "$SCRIPT_DIR/researcher.skill.toml"  > "$LOG_DIR/researcher.log"  2>&1 &
echo $! > "$LOG_DIR/researcher.pid"
"$BIN" --skill "$SCRIPT_DIR/writer.skill.toml"      > "$LOG_DIR/writer.log"      2>&1 &
echo $! > "$LOG_DIR/writer.pid"
"$BIN" --skill "$SCRIPT_DIR/verifier.skill.toml"    > "$LOG_DIR/verifier.log"    2>&1 &
echo $! > "$LOG_DIR/verifier.pid"

# ── 2: gossip convergence ─────────────────────────────────────────────────────
echo ""
echo "[2/5] Waiting for gossip convergence..."
echo "      (each skill's capability gossips to every peer within ~3s)"
wait_for_keys "cap/" 4 "skills visible on mesh"
echo "      Management view: http://localhost:9050/mgmt"
echo "      Pipeline: researcher → writer → verifier (claims check)"

# ── 3: first invocation ───────────────────────────────────────────────────────
echo ""
echo "[3/5] Invoking pipeline — topic: \"gossip protocols in distributed systems\""
echo "──────────────────────────────────────────────────────────────────────────"
"$SCRIPT_DIR/invoke.sh" "gossip protocols in distributed systems"

# ── 4: add second researcher live ─────────────────────────────────────────────
echo ""
echo "[4/5] Adding a second researcher to the mesh..."
echo "      (no orchestrator restart, no config change — gossip finds it)"
# Create a port-modified copy without touching the original
sed "s/bind_port *= *7952/bind_port = 7954/" \
    "$SCRIPT_DIR/researcher.skill.toml" > "$LOG_DIR/researcher2.skill.toml"
"$BIN" --skill "$LOG_DIR/researcher2.skill.toml" > "$LOG_DIR/researcher2.log" 2>&1 &
echo $! > "$LOG_DIR/researcher2.pid"
wait_for_cap "llm/researcher" 2 "second researcher on mesh"
echo "      Orchestrator will now distribute research calls across both nodes."

# ── 5: second invocation ──────────────────────────────────────────────────────
echo ""
echo "[5/5] Second invocation — \"the Rust ownership model\" (load-balanced)"
echo "──────────────────────────────────────────────────────────────────────────"
"$SCRIPT_DIR/invoke.sh" "the Rust ownership model" technical 4

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Demo complete. Cluster still running."
echo ""
echo "  Management dashboard:  http://localhost:9050/mgmt"
echo "  Follow logs:           tail -f $LOG_DIR/orchestrator.log"
echo "  Stop cluster:          ./stop.sh"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
