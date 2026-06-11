#!/usr/bin/env bash
# ci_smoke.sh — scripted run of the community demo against the mock LLM.
#
# Promoted into CI because a live run-through (2026-06-11) found four
# substrate bugs that 317 unit tests and the Docker scenarios never touched:
# the gateway router clobber, the A2A tasks/send timeout, tool results sent
# as non-string content, and skillrunner surviving SIGTERM. Each assertion
# below is a regression gate for one of those.
#
# Requires: target/debug/skillrunner built with --features a2a; python3; curl.
# No Ollama, no network: mock_llm.py stands in on :11434.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
LOG_DIR="$SCRIPT_DIR/logs"
GATEWAY="http://localhost:9050"
BIN="$REPO_ROOT/target/debug/skillrunner"
# NOT 11434: a developer machine may have a real Ollama there, and the smoke
# must be hermetic — a live model answering instead of the mock turns every
# deterministic assertion into flake.
MOCK_PORT="${MOCK_LLM_PORT:-18434}"
mkdir -p "$LOG_DIR"

fail() { echo "✗ FAIL: $*" >&2; exit 1; }
pass() { echo "✓ $*"; }

cleanup() {
    "$SCRIPT_DIR/stop.sh" > /dev/null 2>&1 || true
    [[ -n "${MOCK_PID:-}" ]] && kill "$MOCK_PID" 2>/dev/null || true
}
trap cleanup EXIT

# ── 1. Mock LLM up ─────────────────────────────────────────────────────────
MOCK_LLM_PORT="$MOCK_PORT" python3 "$SCRIPT_DIR/mock_llm.py" > "$LOG_DIR/mock_llm.log" 2>&1 &
MOCK_PID=$!
for i in $(seq 1 50); do
    if curl -sf -o /dev/null -X POST "http://127.0.0.1:$MOCK_PORT/v1/chat/completions" \
        -H 'Content-Type: application/json' -d '{"messages":[]}'; then break; fi
    [[ "$i" == 50 ]] && fail "mock LLM did not come up"
    sleep 0.1
done
pass "mock LLM serving on :$MOCK_PORT"

# ── 2. Cluster up, pointed at the mock ─────────────────────────────────────
# Port-rewritten TOML copies (production TOMLs stay aimed at real Ollama).
[[ -x "$BIN" ]] || fail "skillrunner not built — run: cargo build --bin skillrunner --features a2a"
for skill in orchestrator researcher writer verifier; do
    sed "s|http://localhost:11434/v1|http://127.0.0.1:$MOCK_PORT/v1|" \
        "$SCRIPT_DIR/$skill.skill.toml" > "$LOG_DIR/ci_$skill.skill.toml"
done
"$BIN" --skill "$LOG_DIR/ci_orchestrator.skill.toml" > "$LOG_DIR/orchestrator.log" 2>&1 &
echo $! > "$LOG_DIR/orchestrator.pid"
for i in $(seq 1 50); do
    if (exec 3<>/dev/tcp/127.0.0.1/7950) 2>/dev/null; then exec 3>&- 3<&-; break; fi
    sleep 0.1
done
for skill in researcher writer verifier; do
    "$BIN" --skill "$LOG_DIR/ci_$skill.skill.toml" > "$LOG_DIR/$skill.log" 2>&1 &
    echo $! > "$LOG_DIR/$skill.pid"
done
kv_count() {
    curl -sf "$GATEWAY/gateway/kv/keys?prefix=$1" 2>/dev/null \
        | python3 -c "import sys,json; print(len(json.load(sys.stdin).get('keys',[])))" \
        2>/dev/null || echo 0
}
for i in $(seq 1 60); do
    caps=$(kv_count "cap/")
    # The orchestrator resolves its tools from the gossiped skills/ input
    # schemas (4 skills × input+output = 8 keys) — capability convergence
    # alone is not enough, and racing it makes tasks/send flaky.
    schemas=$(kv_count "skills/")
    [[ "$caps" -ge 4 && "$schemas" -ge 8 ]] && break
    if [[ "$i" == 60 ]]; then
        echo "--- diagnostics ---" >&2
        curl -sf "$GATEWAY/gateway/kv/keys?prefix=skills/" 2>/dev/null >&2 || true
        echo "" >&2
        for l in "$LOG_DIR"/{orchestrator,researcher,writer,verifier}.log; do
            echo "== $l ==" >&2; tail -4 "$l" >&2 2>/dev/null || true
        done
        fail "cluster did not converge (caps: $caps/4, schemas: $schemas/8)"
    fi
    sleep 1
done
# Let every node's skill.invoke RPC receiver finish registering.
sleep 2
pass "4 skills + 8 schemas converged on the mesh"

# ── 3. Agent card: A2A routes present AND schema-aware ─────────────────────
# Regression gates: with_http_routes router clobber (the dashboard used to
# erase the A2A routes → 404 here) and the schema-aware card.
CARD=$(curl -sf "$GATEWAY/.well-known/agent.json") \
    || fail "agent card 404 — A2A routes missing (router merge regression?)"
CARD="$CARD" python3 - <<'PYEOF' || exit 1
import json, os
card = json.loads(os.environ["CARD"])
skills = {s["id"]: s for s in card.get("skills", [])}
assert len(skills) == 4, f"expected 4 skills on card, got {list(skills)}"
orch = skills["llm/orchestrator"]
assert "inputSchema" in orch, "card must expose machine-readable inputSchema"
assert "topic" in orch["inputSchema"].get("properties", {}), orch["inputSchema"]
assert "JSON object" in orch["description"], "description must teach the payload shape"
PYEOF
pass "agent card lists 4 skills with inputSchema + descriptions"

# Both routers must serve simultaneously (merge, not replace).
curl -sf -o /dev/null "$GATEWAY/mgmt" || fail "/mgmt gone — router merge regression"
pass "A2A routes and management dashboard coexist"

# ── 4. Full pipeline through A2A: orchestrator → researcher → writer ───────
# Exercises the tool-call loop (the mock 400s if any tool result is sent as
# a non-string content — the Ollama coercion regression) and the tasks/send
# timeout path.
RESP=$(curl -sf -X POST "$GATEWAY/a2a" -H 'Content-Type: application/json' \
    --max-time 150 -d '{
      "jsonrpc":"2.0","id":1,"method":"tasks/send",
      "params":{"skillId":"llm/orchestrator","message":{"role":"user","parts":[
        {"type":"text","text":"{\"topic\":\"ci smoke topic\",\"style\":\"technical\"}"}]}}}') \
    || fail "tasks/send transport failure"
RESP="$RESP" python3 - <<'PYEOF' || exit 1
import json, os
body = json.loads(os.environ["RESP"])
assert "error" not in body, f"A2A error: {body.get('error')}"
task = body["result"]
assert task["status"]["state"] == "completed", task["status"]
text = task["artifacts"][0]["parts"][0]["text"]
assert "CI Article" in text, f"pipeline output missing mock article: {text[:200]}"
assert "error" not in text.lower() or "CI Article" in text, text[:200]
PYEOF
pass "orchestrator pipeline completed through researcher → writer (mock-verified string content)"

# ── 5. Shutdown: SIGTERM must actually terminate every node ────────────────
"$SCRIPT_DIR/stop.sh" > /dev/null
sleep 2
# pgrep exits 1 when nothing matches — the desired outcome — so shield it
# from set -e/pipefail.
LEFT=$( (pgrep -f "skillrunner.*$SCRIPT_DIR" || true) | wc -l | tr -d ' ')
[[ "$LEFT" == "0" ]] || fail "$LEFT skillrunner process(es) survived SIGTERM"
pass "all skillrunners exited on SIGTERM"

echo ""
echo "demo smoke: ALL PASS"
