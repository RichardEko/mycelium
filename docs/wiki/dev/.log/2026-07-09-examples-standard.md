# 2026-07-09 — example-doc standard adopted (dev section)

Durable-knowledge ingest for the example-documentation rework (PR #146, phases 1–5).

## What changed

The example docs had drifted into three names for the same section (`Run`/`Quick start`,
`What to observe`/`What you'll see`/`What to try`, `How It Works`/`How it works`) and re-typed
toolchain/Ollama/Python setup in every README. There was no `examples/` index.

- **New standard** — [`examples/README.md`](../../../../examples/README.md) is now the front-door
  index of every example, the single **shared-setup** section (Rust / Ollama / Python tier / Docker),
  and the **doc template**: section names `## Objective` · `## How to run` · `## What it
  demonstrates` · `## Dev notes`, two variants (single-example A, suite B) sharing one per-example
  block; walkthroughs live in the README and link out to guide/wiki (concept) + `src/` (mechanism).
  `coop/` is the reference suite shape.
- **Five READMEs normalized** to the template (`chat`, `fluid_pipeline`, `a2a_langchain`,
  `community` — Variant A; `langgraph` — Variant B), duplicated setup replaced by a link to the
  shared section, and each given verified concept + mechanism links (e.g. `chat` →
  `docs/guide/06-tool-discovery.md` + `examples/three_node_demo.rs`; `langgraph` →
  `docs/guide/15-reasoning-and-langgraph.md` + `langgraph-checkpoint-mycelium/…/saver.py`).
- **Root README slimmed** — the ~160-line embedded Demos walkthroughs (which duplicated
  `examples/`) collapsed to a 9-row pointer table (README 1742 → ~1605 lines); the tight #142
  on-ramp is untouched.

## Wiki coupling

`dev/examples.md` points at the new index/standard (added in the phase-1 commit), so the wiki
catalogue and the example-doc contract stay coupled. Any new example README follows the template;
the reviewer check is "four standard section names + a shared-setup link + concept & mechanism
links that resolve."

Pages touched: `dev/examples.md` (phase 1). No code changed.
