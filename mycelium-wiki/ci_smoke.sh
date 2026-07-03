#!/usr/bin/env bash
# Phase 5 worked-example smoke (Docker-free, deterministic). Proves the end-to-end template:
#   import documents → curator applies them → a reader chat-agent answers grounded in the wiki.
# The `--mock` backend echoes the retrieved context, so we can assert that a fact present ONLY in the
# imported corpus reaches the answer (i.e. the agent is grounded, not hallucinating). Runs BOTH driving
# use cases through the same binary to show the template is corpus-agnostic (UC2 council + UC1 org twin).
set -euo pipefail
cd "$(dirname "$0")"   # mycelium-wiki/

STORE="$(mktemp -d)"
trap 'rm -rf "$STORE"' EXIT
RUN="cargo run --quiet -p mycelium-wiki --example wiki_chat --features llm --"

fail() { echo "FAIL: $1"; exit 1; }

echo "== UC2 · community council: import decisions, then navigate them by chat =="
$RUN import --store "$STORE" --group council --corpus examples/corpus/council
ANS=$($RUN ask --store "$STORE" --group council --mock "what was decided about the Elm Street bike lane?")
echo "$ANS"
echo "$ANS" | grep -q "Resolution 2026-14" || fail "answer not grounded in the imported decision"
echo "$ANS" | grep -q "120,000"            || fail "the funding fact from the wiki did not reach the answer"

echo
echo "== UC1 · organisation twin: SAME binary, different corpus =="
$RUN import --store "$STORE" --group orgtwin --corpus examples/corpus/org-twin
ANS2=$($RUN ask --store "$STORE" --group orgtwin --mock "who leads the platform team?")
echo "$ANS2"
echo "$ANS2" | grep -q "Okafor" || fail "org-twin answer not grounded in the imported charter"

echo
echo "== negative check: a question the corpus does not cover retrieves nothing =="
ANS3=$($RUN ask --store "$STORE" --group council --mock "how do I bake sourdough bread?")
echo "$ANS3" | grep -q "no matching sections" || fail "expected an ungrounded/empty-context signal"

echo
echo "PASS: import + grounded chat over both corpora (UC1 + UC2) from one template"
