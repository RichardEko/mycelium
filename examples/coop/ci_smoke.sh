#!/usr/bin/env bash
# Docker-free smoke for the Food-Rescue Co-op suite. Runs each shipped demo and asserts on its
# output. Mirrors examples/fluid_pipeline/ci_smoke.sh. Exits non-zero on any failure.
set -euo pipefail

cd "$(dirname "$0")/../.."

echo "── 01 · mailbox_llm ─────────────────────────────────────────────"
out="$(cargo run -q -p mycelium-coop-examples --bin mailbox_llm 2>/dev/null)"
echo "$out"
echo "$out" | grep -q "All assertions passed" \
  || { echo "FAIL: mailbox_llm did not pass its assertions"; exit 1; }
echo "$out" | grep -q "triage replied" \
  || { echo "FAIL: no triage reply observed"; exit 1; }

echo
echo "── 02 · stigmergy ───────────────────────────────────────────────"
out="$(cargo run -q -p mycelium-coop-examples --bin stigmergy 2>/dev/null)"
echo "$out"
echo "$out" | grep -q "All assertions passed" \
  || { echo "FAIL: stigmergy did not pass its assertions"; exit 1; }

echo
echo "── 03 · elastic_intent ──────────────────────────────────────────"
out="$(cargo run -q -p mycelium-coop-examples --bin elastic_intent 2>/dev/null)"
echo "$out"
echo "$out" | grep -q "All assertions passed" \
  || { echo "FAIL: elastic_intent did not pass its assertions"; exit 1; }

echo
echo "All co-op smokes passed."
