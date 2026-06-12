#!/usr/bin/env bash
# ci_smoke.sh — end-to-end smoke of the fluid pipeline in BOTH modes, no Docker.
#
# Each mode gets fresh nodes (no persistence, so a clean KV and tuple space):
#
#   pull (canonical AFN): seeder node hosts the tuple primary; two worker
#        nodes run as tuple clients; worker.py flow-loops take() from the
#        deepest stage and complete() into the next; coordinator.py seeds 24
#        items and watches done markers.
#   push (baseline):      the original coordinator drain loops dispatching
#        RPCs to capability-resolved workers over the KV-ring buffer.
#
# Asserts each mode reports "pipeline complete: 24/24". The coordinator
# self-limits (PIPELINE_TIMEOUT_SECS) so a stalled run exits non-zero rather
# than hanging the job.
#
# Requires: target/debug/examples/three_node_demo (cargo build --example
# three_node_demo); python3 with venv. No postgres: the aggregate stage's DB
# write degrades to a logged warning when psycopg2 is absent — deliberate,
# the smoke asserts flow mechanics, not the sink.
#
# Usage: ci_smoke.sh [pull|push|both]   (default: both)

set -euo pipefail

MODES="${1:-both}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
LOG_DIR="$SCRIPT_DIR/logs"
BIN="$REPO_ROOT/target/debug/examples/three_node_demo"
ITEMS=24

# Distinct from every other harness's ports so local runs never collide.
SEED_GOSSIP=57400; SEED_HTTP=58400
W1_GOSSIP=57401;   W1_HTTP=58401
W2_GOSSIP=57402;   W2_HTTP=58402

mkdir -p "$LOG_DIR"

fail() { echo "✗ FAIL: $*" >&2; exit 1; }
pass() { echo "✓ $*"; }

PIDS=()
cleanup() {
    for pid in "${PIDS[@]:-}"; do kill "$pid" 2>/dev/null || true; done
    wait 2>/dev/null || true
}
trap cleanup EXIT

[[ -x "$BIN" ]] || fail "binary missing — run: cargo build --example three_node_demo"

# ── Python env: mycelium-py + httpx (+httpx-sse for push-mode rpc_serve) ────
VENV="$LOG_DIR/venv"
if [[ ! -x "$VENV/bin/python" ]]; then
    python3 -m venv "$VENV"
    "$VENV/bin/pip" install --quiet "$REPO_ROOT/mycelium-py" "httpx>=0.27" "httpx-sse>=0.4"
fi
PY="$VENV/bin/python"
pass "python env ready"

wait_health() {
    local port="$1" name="$2"
    for i in $(seq 1 100); do
        if curl -sf -o /dev/null "http://127.0.0.1:$port/health"; then return 0; fi
        sleep 0.2
    done
    fail "$name did not become healthy on :$port"
}

start_nodes() {
    MYCELIUM_ROLE=node MYCELIUM_PORT=$SEED_GOSSIP MYCELIUM_HTTP_PORT=$SEED_HTTP \
    MYCELIUM_HOSTNAME=127.0.0.1 MYCELIUM_PEERS="" \
    MYCELIUM_TUPLE_ROLE=primary MYCELIUM_TUPLE_NS=pipeline \
    RUST_LOG=warn "$BIN" > "$LOG_DIR/node_seed.log" 2>&1 &
    PIDS+=($!)

    for w in 1 2; do
        local gport hport
        gport=$((SEED_GOSSIP + w)); hport=$((SEED_HTTP + w))
        MYCELIUM_ROLE=node MYCELIUM_PORT=$gport MYCELIUM_HTTP_PORT=$hport \
        MYCELIUM_HOSTNAME=127.0.0.1 MYCELIUM_PEERS="127.0.0.1:$SEED_GOSSIP" \
        MYCELIUM_TUPLE_ROLE=client MYCELIUM_TUPLE_NS=pipeline \
        RUST_LOG=warn "$BIN" > "$LOG_DIR/node_w$w.log" 2>&1 &
        PIDS+=($!)
    done

    wait_health $SEED_HTTP "seeder node"
    wait_health $W1_HTTP   "worker node 1"
    wait_health $W2_HTTP   "worker node 2"
}

run_mode() {
    local mode="$1"
    echo "── mode: $mode ──────────────────────────────────────────────"
    PIDS=()
    start_nodes
    pass "$mode: 3 nodes healthy"

    for w in 1 2; do
        local hport=$((SEED_HTTP + w))
        ( cd "$SCRIPT_DIR/worker" && \
          PIPELINE_MODE="$mode" MYCELIUM_HTTP_PORT=$hport MYCELIUM_TUPLE_NS=pipeline \
          STAGE_C_SLEEP=0.02 POSTGRES_DSN="postgresql://nope@127.0.0.1:1/none" \
          "$PY" worker.py > "$LOG_DIR/worker${w}_${mode}.log" 2>&1 ) &
        PIDS+=($!)
    done

    local coord_log="$LOG_DIR/coordinator_${mode}.log"
    if ( cd "$SCRIPT_DIR/coordinator" && \
         PIPELINE_MODE="$mode" MYCELIUM_HTTP_PORT=$SEED_HTTP MYCELIUM_TUPLE_NS=pipeline \
         ITEM_COUNT=$ITEMS MIN_WORKERS=2 PIPELINE_TIMEOUT_SECS=180 \
         "$PY" coordinator.py > "$coord_log" 2>&1 ); then
        grep -q "pipeline complete: $ITEMS/$ITEMS" "$coord_log" \
            || fail "$mode: completed but item count wrong — $(tail -3 "$coord_log")"
        pass "$mode: pipeline complete $ITEMS/$ITEMS"
    else
        echo "── coordinator log tail ──" >&2; tail -15 "$coord_log" >&2
        fail "$mode: coordinator exited non-zero"
    fi

    cleanup
    PIDS=()
}

case "$MODES" in
    pull) run_mode pull ;;
    push) run_mode push ;;
    both) run_mode pull; run_mode push ;;
    *)    fail "unknown mode '$MODES' (pull|push|both)" ;;
esac

pass "fluid-pipeline smoke complete ($MODES)"
