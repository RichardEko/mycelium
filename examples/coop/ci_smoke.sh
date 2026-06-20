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
echo "── 04 · provisioning (flagship; pulls wasmtime on first build) ───"
out="$(cargo run -q -p mycelium-coop-examples --bin provisioning 2>/dev/null)"
echo "$out"
echo "$out" | grep -q "All assertions passed" \
  || { echo "FAIL: provisioning did not pass its assertions"; exit 1; }
echo "$out" | grep -q "self-healed" \
  || { echo "FAIL: provisioning failover phase did not complete"; exit 1; }

echo
echo "── 05 · federation_facts ────────────────────────────────────────"
out="$(cargo run -q -p mycelium-coop-examples --bin federation_facts 2>/dev/null)"
echo "$out"
echo "$out" | grep -q "All assertions passed" \
  || { echo "FAIL: federation_facts did not pass its assertions"; exit 1; }
echo "$out" | grep -q "verified the self-signature" \
  || { echo "FAIL: cross-domain verification did not occur"; exit 1; }

echo
echo "── 06 · rotation ────────────────────────────────────────────────"
out="$(cargo run -q -p mycelium-coop-examples --bin rotation 2>/dev/null)"
echo "$out"
echo "$out" | grep -q "All assertions passed" \
  || { echo "FAIL: rotation did not pass its assertions"; exit 1; }
echo "$out" | grep -q "STILL verifies the old-key-signed field" \
  || { echo "FAIL: retained-key verification across rotation did not occur"; exit 1; }

echo
echo "── 07 · consensus ───────────────────────────────────────────────"
out="$(cargo run -q -p mycelium-coop-examples --bin consensus 2>/dev/null)"
echo "$out"
echo "$out" | grep -q "All assertions passed" \
  || { echo "FAIL: consensus did not pass its assertions"; exit 1; }
echo "$out" | grep -q "reads as reopened" \
  || { echo "FAIL: leased-decision decay did not occur"; exit 1; }

echo
echo "── 08 · llm_pipeline ────────────────────────────────────────────"
out="$(cargo run -q -p mycelium-coop-examples --bin llm_pipeline 2>/dev/null)"
echo "$out"
echo "$out" | grep -q "All assertions passed" \
  || { echo "FAIL: llm_pipeline did not pass its assertions"; exit 1; }
echo "$out" | grep -q "both LLM stages" \
  || { echo "FAIL: chained LLM stages not evidenced"; exit 1; }

echo
echo "── 09 · mcp_toolgrowth ──────────────────────────────────────────"
out="$(cargo run -q -p mycelium-coop-examples --bin mcp_toolgrowth 2>/dev/null)"
echo "$out"
echo "$out" | grep -q "All assertions passed" \
  || { echo "FAIL: mcp_toolgrowth did not pass its assertions"; exit 1; }
echo "$out" | grep -q "loaded the MCP tool and offered it out" \
  || { echo "FAIL: on-demand MCP tool loading did not occur"; exit 1; }

echo
echo "All co-op smokes passed."
