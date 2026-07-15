# Mycelium examples

Every example is a real, runnable program built on the **public API** — no private hooks. This page
is the index, the shared setup (so no example re-explains it), and the **doc template** all example
READMEs follow.

**One grid, three ways to read it.** The [capability matrix](#the-capability-matrix) below fingerprints
every example by the stack **layer** it teaches *and* its facets — how deep (*Level*), how you watch it
(*Surface*), whether it needs a model (*LLM*), and which operational surfaces it lights up (*Audit*,
*Metrics*). Scan **down a column** to filter ("show me the browser demos", "the zero-LLM ones"), or read
**across a row** to characterize one example at a glance. New here? Start at the **Intro** rows and climb.

> **Colour-coded, scannable, opens offline:** [`docs/wiki/dev/examples-layer-matrix.html`](../docs/wiki/dev/examples-layer-matrix.html)
> is the same matrix rendered — layer dots + facet chips + a per-layer summary strip.

## The capability matrix

**Layers:** ● primary · ○ also exercises · · none — **I** gossip-KV (state) · **II** signal-mesh
(events, opacity) · **III** consensus · **IV** capability/agent. **Facets:** *Level* Intro/Adv (★ flagship) ·
*Surface* Web (browser UI) / CLI · *LLM* real (needs a model) / mock (echo, no key) / · none · *Audit* ✓
emits a signed tamper-evident trail · *Metrics* ✓ built with the Prometheus recorder (the Ops Console
**Metrics** tab climbs live).

Each example name links to its **run doc** (a README or guide chapter that tells you how to start it) —
not to raw source. The suite READMEs carry the per-example walkthrough + the exact command.

| Example | I | II | III | IV | Level | Surface | LLM | Audit | Metrics |
|---|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|
| **Start here** — the zero-setup ladder, one file each | | | | | | | | | |
| [`hello_mesh`](../docs/guide/01-gossip-kv.md) | ● | · | · | · | Intro | CLI | · | · | · |
| [`hello_capability`](../docs/guide/02-capabilities.md) | · | · | · | ● | Intro | CLI | · | · | · |
| [`conway`](../docs/guide/01-gossip-kv.md) | ● | ○ | · | · | Intro | Web | · | · | ✓ |
| [`distributed_lock`](../docs/guide/04-consensus.md) | · | · | ● | · | Intro | CLI | · | · | · |
| [`invoke_skill`](../docs/guide/05-skills.md) | ○ | · | · | ● | Intro | CLI | · | · | · |
| [`semantic_coordination`](../docs/guide/11-semantic-coordination.md) | · | ● | · | ○ | Intro | CLI | · | · | · |
| **Top-level** — beyond the ladder | | | | | | | | | |
| [`llm_agent`](../docs/guide/02-capabilities.md) | ○ | ○ | · | ● | Adv | Web | mock | · | ✓ |
| [`coordinator_comparison`](../docs/plans/three_arm_workdist.md) | ● | · | · | ● | Adv | CLI | · | · | · |
| [`three_arm_workdist`](../docs/plans/three_arm_workdist.md) | ● | · | · | ● | Adv | CLI | · | · | · |
| [`three_node_demo`](chat/README.md) ★ | ● | ● | ● | ● | Adv | Web | real | · | · |
| [`ops_console`](ops_console/README.md) † | ○ | ○ | ○ | ○ | Adv | Web | · | · | · |
| **Food-Rescue Co-op** — [`coop/README.md`](coop/README.md), one constructive world | | | | | | | | | |
| [`mailbox_llm`](coop/README.md) | ○ | · | · | ● | Adv | CLI | mock | · | · |
| [`stigmergy`](coop/README.md) | · | ● | · | ○ | Adv | CLI | · | · | · |
| [`stigmergy_viz`](coop/README.md) | · | ● | · | ○ | Adv | Web | · | · | ✓ |
| [`elastic_intent`](coop/README.md) | · | · | · | ● | Adv | CLI | · | · | · |
| [`provisioning`](coop/README.md) ★ | · | · | · | ● | Adv | CLI | · | · | · |
| [`federation_facts`](coop/README.md) | · | · | · | ● | Adv | CLI | · | · | · |
| [`rotation`](coop/README.md) | · | · | · | ● | Adv | CLI | · | · | · |
| [`consensus`](coop/README.md) | · | ○ | ● | · | Adv | CLI | · | · | · |
| [`llm_pipeline`](coop/README.md) | · | · | · | ● | Adv | CLI | mock | · | · |
| [`mcp_toolgrowth`](coop/README.md) | ○ | · | · | ● | Adv | CLI | mock | · | · |
| [`llm_council`](coop/README.md) | · | · | · | ● | Adv | CLI | mock | · | · |
| [`llm_council_viz`](coop/README.md) | · | · | · | ● | Adv | Web | mock | · | ✓ |
| [`catalog`](coop/README.md) | ○ | · | · | ● | Adv | CLI | · | · | · |
| [`model_deploy`](coop/README.md) | ○ | · | · | ● | Adv | CLI | real | · | · |
| [`reheal_deploy`](coop/README.md) | ○ | · | · | ● | Adv | CLI | mock | · | · |
| [`diagnostics`](coop/README.md) | · | ● | · | ○ | Adv | CLI | · | · | · |
| **Companions** — blackboard · tuple-space · wiki, atop I/II | | | | | | | | | |
| [`microgrid`](../mycelium-blackboard/examples/README.md) | ○ | · | · | ● | Adv | CLI | · | · | · |
| [`microgrid_viz`](../mycelium-blackboard/examples/README.md) | ○ | · | · | ● | Adv | Web | · | · | ✓ |
| [`redistribution`](../mycelium-tuple-space/examples/README.md) | ○ | · | · | ● | Adv | CLI | · | · | · |
| [`redistribution_viz`](../mycelium-tuple-space/examples/README.md) | ○ | · | · | ● | Adv | Web | · | · | ✓ |
| [`fluid_pipeline`](fluid_pipeline/README.md) | · | · | · | ● | Adv | CLI | · | · | · |
| [`wiki_chat`](../mycelium-wiki/examples/README.md) | ○ | · | · | ● | Adv | CLI | mock | · | · |
| [`wiki_council_viz`](../mycelium-wiki/examples/README.md) ★ | ○ | · | · | ● | Adv | Web | real | · | ✓ |
| **Reasoning** — [`mycelium-reason/examples/README.md`](../mycelium-reason/examples/README.md) | | | | | | | | | |
| [`fleet_reasoning`](../mycelium-reason/examples/README.md) | · | · | · | ● | Adv | CLI | mock | · | · |
| [`reason_node`](../mycelium-reason/examples/README.md) | ○ | · | · | ● | Adv | CLI | mock | · | · |
| [`reheal_node`](../mycelium-reason/examples/README.md) | ○ | · | ○ | ● | Adv | CLI | mock | · | · |
| **Guardrails** — [`mycelium-guardrails/examples/README.md`](../mycelium-guardrails/examples/README.md) | | | | | | | | | |
| [`guardrail_fleet`](../mycelium-guardrails/examples/README.md) | · | · | · | ● | Adv | CLI | · | ✓ | · |
| [`guardrail_wedge`](../mycelium-guardrails/examples/README.md) | · | · | · | ● | Adv | CLI | · | ✓ | · |
| [`guardrail_viz`](../mycelium-guardrails/examples/README.md) ★ | · | · | · | ● | Adv | Web | · | ✓ | ✓ |
| **Python interop** — external agents & skills | | | | | | | | | |
| [`a2a_langchain`](a2a_langchain/README.md) | · | · | · | ● | Adv | CLI | real | · | · |
| [`langgraph`](langgraph/README.md) | ○ | · | ○ | ● | Adv | CLI | mock | · | · |
| [`community`](community/README.md) | · | · | · | ● | Adv | Web | real | ✓ | · |

★ **flagship** — the marquee demo of its world. † `ops_console` *observes* every layer and both ops
surfaces (`/audit`, `/metrics`) rather than emitting them — point it at any node below. Every link above
goes to a **run doc** (README or guide chapter), never raw source; the exact commands live in each suite
README (and, for the visual demos, [Browser showcases](#browser-showcases) below).

## Browser showcases

Open-and-watch demos — the `/state`-feed-behind-a-canvas pattern `conway` established: run continuously
(Ctrl-C to stop; **not** in any CI smoke), open `http://127.0.0.1:80xx/`. All follow the
[UI-example contract](../docs/wiki/dev/ui-example-contract.md) — gateway+metrics on, Ops Console linked,
opt-in audit, a "what you're seeing" concepts box. See [shared setup](#shared-setup) first; each name
links to its walkthrough README.

| Showcase | Port | Run |
|---|:--:|---|
| [`microgrid_viz`](../mycelium-blackboard/examples/README.md) | `:8091` | `cargo run -p mycelium-blackboard --example microgrid_viz --features gateway,metrics` |
| [`stigmergy_viz`](coop/README.md) | `:8092` | `cargo run -p mycelium-coop-examples --bin stigmergy_viz --features metrics` |
| [`redistribution_viz`](../mycelium-tuple-space/examples/README.md) | `:8093` | `cargo run -p mycelium-tuple-space --example redistribution_viz --features gateway,metrics` |
| [`llm_council_viz`](coop/README.md) | `:8094` | `cargo run -p mycelium-coop-examples --bin llm_council_viz --features metrics` |
| [`wiki_council_viz`](../mycelium-wiki/examples/README.md) ★ | `:8095` | `cargo run -p mycelium-wiki --example wiki_council_viz --features gateway,llm,metrics` |
| [`guardrail_viz`](../mycelium-guardrails/examples/README.md) ★ | `:8096` | `cargo run -p mycelium-guardrails --example guardrail_viz --features compliance,gateway,metrics-export` |
| [`conway`](../docs/guide/01-gossip-kv.md) | `:8090` | `cargo run --example conway --features metrics` |
| [`conway-gpu`](conway-gpu/README.md) | — | `cargo run --release -p conway-gpu` (GPU/wgpu; no gateway) |

`wiki_council_viz` phrases each specialist's grounded answer via a **local model served on the mesh**
(Ollama), falling back to grounded extraction if absent — no cloud, no key. `guardrail_viz` fires
invocations at a Tier-C gate and rebuilds the **cryptographic denial proof** live. The four `*_viz` are
visual variants of the batch coop/companion demos, which stay the CI-gated versions.

**Everything else** — the starter ladder, the Food-Rescue Co-op suite, guardrails, the community
skills cluster, reasoning/LangGraph, AFN, A2A, and the interactive chat — is in the
[capability matrix](#the-capability-matrix) above; click any row for its walkthrough + exact command.

## Ops Console

A generic, read-only dashboard over *any* gateway-enabled node's operational endpoints — `/stats` ·
`/gateway/fleet` · `/gateway/diagnose` (the Legible-Emergence fleet narrative) · `/gateway/audit` ·
`/gateway/kv/keys` · `/metrics` — in one place. A *dev/reference* tool, **not** a shipped control plane
(library, not platform); a customer forks it or points Grafana at `/metrics`.

```
cargo run --example ops_console            # → http://127.0.0.1:8099/  (default target 127.0.0.1:9050)
```

Full docs — the tab-by-tab endpoint map, the `ui/viz` two-way linking convention, and which host box to
point at each demo/showcase — are in **[`ops_console/README.md`](ops_console/README.md)**.

## Research artifacts

Paper 1 / 2a experiment runners — reproducible, not tutorials:
[`coordinator_comparison.rs`](coordinator_comparison.rs) (+ [runner](coordinator_comparison_runner.sh)/[plot](coordinator_comparison_plot.py)) ·
[`three_arm_workdist.rs`](three_arm_workdist.rs) (+ [runner](three_arm_runner.sh)/[plot](three_arm_plot.py)) —
complementary, not redundant: `coordinator_comparison` is the two-arm *decision-level* probe (broker vs
gossip prediction, staleness/misroute), `three_arm_workdist` adds the **pull** arm and measures
*outcomes* (latency/throughput/fairness). See each file's header for the experiment design.

## Shared setup

Every cluster below assumes some subset of these. An example's README names which it needs and its
own one-line build; it does **not** re-explain the install — it links here.

**Rust toolchain** (all examples). The pinned toolchain builds automatically:
```bash
cargo build --example hello_mesh     # or the specific --example / --bin an example names
```

**Ollama** (LLM examples — free, no API key). Any OpenAI-compatible endpoint works instead.
```bash
ollama serve                 # in its own terminal
ollama pull llama3.2         # the common default; some examples name a stronger model
```
To use a non-Ollama backend, set `OLLAMA_BASE_URL` + `OLLAMA_MODEL` (or `OPENAI_API_KEY` +
`OPENAI_MODEL` where the README says so). Small models sometimes mis-pick tools — for reliable
tool-calling use a stronger local model (`qwen3:14b` is verified) or `gpt-4o-mini`.

**Python tier** (the A2A + LangGraph examples). Python ≥ 3.11, in a venv:
```bash
python -m venv .venv && source .venv/bin/activate
pip install './mycelium-py[typed]' ./langgraph-checkpoint-mycelium   # + any per-example deps
```

**Docker Compose v2** (only the containerised examples, e.g. `fluid_pipeline`): `docker compose version`.

---

## The doc template (for contributors)

Example READMEs drifted into three names for the same section ("Run"/"Quick start", "What you'll
see"/"What to observe") and re-typed setup. **New and edited example docs follow the layout below.**
There are two variants; both share the same **per-example block**.

### Per-example block (the reusable unit)

1. **Objective** — 1–3 sentences: what this example demonstrates and *why it matters*. Lead with the
   capability, not the plumbing.
2. **How to run** — the exact commands. Link to [shared setup](#shared-setup) for the toolchain;
   show only this example's build + run + expected first output.
3. **What it demonstrates** — the walkthrough: what to watch, tied back to the concept, **with links
   into the guide/wiki for the idea and into `src/` (or the example source) for the mechanism**. This
   is where a reader connects "what I saw" to "how it works" — the section that earns the example.
4. **Dev notes** *(optional)* — gotchas, tuning knobs, "when NOT to use this."

Standard section names: `## Objective` · `## How to run` · `## What it demonstrates` · `## Dev notes`.
Retire the variants (`Concept`, `Quick start`, `What you'll see`, `What to try`).

### Variant A — single example

Title → **Objective** → **How to run** → **What it demonstrates** → *Dev notes*. One block, top to
bottom. (`chat/`, `fluid_pipeline/`, `a2a_langchain/`.)

### Variant B — suite / cluster (many examples under one theme)

Title → **Objective** (the cluster's theme + shared harness) → **How to run** (the one bring-up every
member shares) → a **per-example block** for each demo (Objective · Run · What it demonstrates+links)
→ **CI**. (`coop/` is the reference implementation of this shape; `community/`, `langgraph/`.)

> **Where narrative lives:** walkthroughs stay *in the example README* (a developer running it wants
> them right there); link *out* to the guide/wiki for the concept and to `src/` for the mechanism —
> don't duplicate either.
