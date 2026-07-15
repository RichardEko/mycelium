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

| Example | I | II | III | IV | Level | Surface | LLM | Audit | Metrics |
|---|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|
| **Start here** — the zero-setup ladder, one file each | | | | | | | | | |
| [`hello_mesh`](hello_mesh.rs) | ● | · | · | · | Intro | CLI | · | · | · |
| [`hello_capability`](hello_capability.rs) | · | · | · | ● | Intro | CLI | · | · | · |
| [`conway`](conway.rs) | ● | ○ | · | · | Intro | Web | · | · | ✓ |
| [`distributed_lock`](distributed_lock.rs) | · | · | ● | · | Intro | CLI | · | · | · |
| [`invoke_skill`](invoke_skill.rs) | ○ | · | · | ● | Intro | CLI | · | · | · |
| [`semantic_coordination`](semantic_coordination.rs) | · | ● | · | ○ | Intro | CLI | · | · | · |
| **Top-level** — beyond the ladder | | | | | | | | | |
| [`llm_agent`](llm_agent.rs) | ○ | ○ | · | ● | Adv | Web | mock | · | · |
| [`coordinator_comparison`](coordinator_comparison.rs) | ● | · | · | ● | Adv | CLI | · | · | · |
| [`three_arm_workdist`](three_arm_workdist.rs) | ● | · | · | ● | Adv | CLI | · | · | · |
| [`three_node_demo`](three_node_demo.rs) ★ | ● | ● | ● | ● | Adv | Web | real | · | · |
| [`ops_console`](ops_console.rs) † | ○ | ○ | ○ | ○ | Adv | Web | · | · | · |
| **Food-Rescue Co-op** — [`coop/`](coop/README.md), one constructive world | | | | | | | | | |
| [`mailbox_llm`](coop/src/bin/mailbox_llm.rs) | ○ | · | · | ● | Adv | CLI | mock | · | · |
| [`stigmergy`](coop/src/bin/stigmergy.rs) | · | ● | · | ○ | Adv | CLI | · | · | · |
| [`stigmergy_viz`](coop/src/bin/stigmergy_viz.rs) | · | ● | · | ○ | Adv | Web | · | · | ✓ |
| [`elastic_intent`](coop/src/bin/elastic_intent.rs) | · | · | · | ● | Adv | CLI | · | · | · |
| [`provisioning`](coop/src/bin/provisioning.rs) ★ | · | · | · | ● | Adv | CLI | · | · | · |
| [`federation_facts`](coop/src/bin/federation_facts.rs) | · | · | · | ● | Adv | CLI | · | · | · |
| [`rotation`](coop/src/bin/rotation.rs) | · | · | · | ● | Adv | CLI | · | · | · |
| [`consensus`](coop/src/bin/consensus.rs) | · | ○ | ● | · | Adv | CLI | · | · | · |
| [`llm_pipeline`](coop/src/bin/llm_pipeline.rs) | · | · | · | ● | Adv | CLI | mock | · | · |
| [`mcp_toolgrowth`](coop/src/bin/mcp_toolgrowth.rs) | ○ | · | · | ● | Adv | CLI | mock | · | · |
| [`llm_council`](coop/src/bin/llm_council.rs) | · | · | · | ● | Adv | CLI | mock | · | · |
| [`llm_council_viz`](coop/src/bin/llm_council_viz.rs) | · | · | · | ● | Adv | Web | mock | · | ✓ |
| [`catalog`](coop/src/bin/catalog.rs) | ○ | · | · | ● | Adv | CLI | · | · | · |
| [`model_deploy`](coop/src/bin/model_deploy.rs) | ○ | · | · | ● | Adv | CLI | real | · | · |
| [`reheal_deploy`](coop/src/bin/reheal_deploy.rs) | ○ | · | · | ● | Adv | CLI | mock | · | · |
| [`diagnostics`](coop/src/bin/diagnostics.rs) | · | ● | · | ○ | Adv | CLI | · | · | · |
| **Companions** — blackboard · tuple-space · wiki, atop I/II | | | | | | | | | |
| [`microgrid`](../mycelium-blackboard/examples/microgrid.rs) | ○ | · | · | ● | Adv | CLI | · | · | · |
| [`microgrid_viz`](../mycelium-blackboard/examples/microgrid_viz.rs) | ○ | · | · | ● | Adv | Web | · | · | ✓ |
| [`redistribution`](../mycelium-tuple-space/examples/redistribution.rs) | ○ | · | · | ● | Adv | CLI | · | · | · |
| [`redistribution_viz`](../mycelium-tuple-space/examples/redistribution_viz.rs) | ○ | · | · | ● | Adv | Web | · | · | ✓ |
| [`fluid_pipeline`](fluid_pipeline/README.md) | · | · | · | ● | Adv | CLI | · | · | · |
| [`wiki_chat`](../mycelium-wiki/examples/wiki_chat.rs) | ○ | · | · | ● | Adv | CLI | mock | · | · |
| [`wiki_council_viz`](../mycelium-wiki/examples/wiki_council_viz.rs) ★ | ○ | · | · | ● | Adv | Web | real | · | ✓ |
| **Reasoning** — [`mycelium-reason/examples/`](../mycelium-reason/examples/) | | | | | | | | | |
| [`fleet_reasoning`](../mycelium-reason/examples/fleet_reasoning.rs) | · | · | · | ● | Adv | CLI | mock | · | · |
| [`reason_node`](../mycelium-reason/examples/reason_node.rs) | ○ | · | · | ● | Adv | CLI | mock | · | · |
| [`reheal_node`](../mycelium-reason/examples/reheal_node.rs) | ○ | · | ○ | ● | Adv | CLI | mock | · | · |
| **Guardrails** — [`mycelium-guardrails/examples/`](../mycelium-guardrails/examples/) | | | | | | | | | |
| [`guardrail_fleet`](../mycelium-guardrails/examples/guardrail_fleet.rs) | · | · | · | ● | Adv | CLI | · | ✓ | · |
| [`guardrail_wedge`](../mycelium-guardrails/examples/guardrail_wedge.rs) | · | · | · | ● | Adv | CLI | · | ✓ | · |
| [`guardrail_viz`](../mycelium-guardrails/examples/guardrail_viz.rs) ★ | · | · | · | ● | Adv | Web | · | ✓ | ✓ |
| **Python interop** — external agents & skills | | | | | | | | | |
| [`a2a_langchain`](a2a_langchain/README.md) | · | · | · | ● | Adv | CLI | real | · | · |
| [`langgraph`](langgraph/README.md) | ○ | · | ○ | ● | Adv | CLI | mock | · | · |
| [`community`](community/README.md) | · | · | · | ● | Adv | Web | real | ✓ | · |

★ **flagship** — the marquee demo of its world. † `ops_console` *observes* every layer and both ops
surfaces (`/audit`, `/metrics`) rather than emitting them — point it at any node below.

## The worlds

The suites, in the order a newcomer meets them — each links to its own README (walkthrough + the exact
commands); see [shared setup](#shared-setup) first.

- **The ladder** (Intro rows). One file each, zero LLM, each rung building on the last:
  `hello_mesh` (the substrate in ~25 lines) → `hello_capability` (broker-less discovery + RPC) →
  `conway` (*watch* KV convergence) → `distributed_lock` (consensus + a fencing token). Guide chapters
  01–04 explain them. `cargo run --example hello_mesh`.
- **Food-Rescue Co-op** — [`coop/`](coop/README.md), 14 demos (12 CI + 2 manual real-model) composed in
  one constructive world: depot nodes rescuing surplus food, no dispatcher. The full pattern catalogue —
  stigmergy, elastic intent, the autonomic **provisioning ⭐** loop, federation, consensus, the durable
  **artifact library** (`catalog` · `model_deploy` · `reheal_deploy`), real-model deploy/reheal.
  `ci_smoke.sh` runs the twelve CI demos Docker-free.
- **Browser showcases** — the `/state`-feed-behind-a-canvas pattern `conway` established: pitch/booth
  demos you *open and watch*, run continuously (Ctrl-C to stop; **not** in any CI smoke). All follow the
  [UI-example contract](../docs/wiki/dev/ui-example-contract.md) — gateway+metrics on, Ops Console linked,
  opt-in audit, a "what you're seeing" concepts box. Run reference:

  | Showcase | Port | Run |
  |---|:--:|---|
  | [`microgrid_viz`](../mycelium-blackboard/examples/microgrid_viz.rs) | `:8091` | `cargo run -p mycelium-blackboard --example microgrid_viz --features gateway,metrics` |
  | [`stigmergy_viz`](coop/src/bin/stigmergy_viz.rs) | `:8092` | `cargo run -p mycelium-coop-examples --bin stigmergy_viz --features metrics` |
  | [`redistribution_viz`](../mycelium-tuple-space/examples/redistribution_viz.rs) | `:8093` | `cargo run -p mycelium-tuple-space --example redistribution_viz --features gateway,metrics` |
  | [`llm_council_viz`](coop/src/bin/llm_council_viz.rs) | `:8094` | `cargo run -p mycelium-coop-examples --bin llm_council_viz --features metrics` |
  | [`wiki_council_viz`](../mycelium-wiki/examples/wiki_council_viz.rs) ★ | `:8095` | `cargo run -p mycelium-wiki --example wiki_council_viz --features gateway,llm,metrics` |
  | [`guardrail_viz`](../mycelium-guardrails/examples/guardrail_viz.rs) ★ | `:8096` | `cargo run -p mycelium-guardrails --example guardrail_viz --features compliance,gateway,metrics-export` |
  | [`conway`](conway.rs) | `:8090` | `cargo run --example conway --features metrics` |
  | [`conway-gpu`](conway-gpu/README.md) | — | `cargo run --release -p conway-gpu` (GPU/wgpu; no gateway) |

  `wiki_council_viz` phrases each specialist's grounded answer via a **local model served on the mesh**
  (Ollama — `register`/`call_prompt_skill`), falling back to grounded extraction if Ollama is absent —
  no cloud, no key. `guardrail_viz` lets you fire invocations at a Tier-C gate and watch the
  **cryptographic denial proof** rebuilt live by a neutral observer. The four `*_viz` above are visual
  variants of the batch coop/companion demos, which stay the CI-gated versions.
- **Guardrails** — [`mycelium-guardrails/examples/`](../mycelium-guardrails/examples/): the three policy
  tiers (soft-warn → hard-**prevent**). `guardrail_wedge` stops an off-allowlist caller at a Tier-C gate
  with a cryptographic denial proof; `guardrail_fleet` composes all three in a co-op fleet; `guardrail_viz`
  is the browser showcase. Its `/gateway/audit` **is** the seal — the de-facto **audit** surface alongside
  `community`.
- **Skills / community cluster** — [`community/`](community/README.md), the `skillrunner` at `:9050`:
  LLM agents as first-class mesh citizens (skills = capabilities, live load-balancing). Every invocation
  writes a **signed audit record**; the mgmt UI (`:9050/mgmt`) shows the trail.
- **Reasoning / LangGraph** — [`langgraph/`](langgraph/README.md) (Python) over a Rust reason mesh
  ([`mycelium-reason/examples/`](../mycelium-reason/examples/)): the 7-rung LangGraph-on-Mycelium ladder,
  `reason_node` · `reheal_node` (the deploy/reheal flagship) · `fleet_reasoning`. Guide
  [ch. 15](../docs/guide/15-reasoning-and-langgraph.md).
- **Agentic Flow Networks** — [`fluid_pipeline/`](fluid_pipeline/README.md): a tuple-space **pull**
  pipeline (stigmergic backpressure) vs a **push** baseline, 10 workers. Concept essay: `flow_networks.html`.
- **A2A interop** — [`a2a_langchain/`](a2a_langchain/README.md): external LangChain / AutoGen agents
  auto-discover Mycelium skills via `/.well-known/agent.json`.
- **Interactive chat** — [`chat/`](chat/README.md), `three_node_demo`: live MCP tool discovery — tools
  join a running mesh and the LLM finds them without restart (the same binary drives the Docker
  integration cluster).

## Ops Console

A generic, read-only dashboard over *any* gateway-enabled node's operational endpoints, in one place:
`/stats` (node runtime + tripwires), `/gateway/fleet` (cluster snapshot), `/gateway/diagnose` (the
Legible-Emergence **fleet narrative** — "why is the fleet in this state", in plain English),
`/gateway/audit` (the tamper-evident signed audit trail — nodes built `--features compliance`),
`/gateway/kv/keys`, `/metrics`. It's a *dev/reference* tool — **not** a shipped control plane (library,
not platform); a customer forks it or points Grafana at `/metrics`.

```
cargo run --example ops_console            # → http://127.0.0.1:8099/  (default target 127.0.0.1:9050)
```

Then set the host box to any cluster: the **community** skills cluster (`:9050`), a **coop** demo, or a
**showcase** — `conway` (`:9090`), `stigmergy_viz` / `llm_council_viz` (they print their gateway URL at
startup), or `microgrid_viz` (`:9091`) / `redistribution_viz` (`:9093`) *run with `--features gateway`*
(those companion crates have the gateway off by default). Every browser demo self-advertises its UI via
two KV keys (`ui/viz` = its URL, `ui/label` = a short name) which the console surfaces as a live "↗ label"
link; each dashboard carries the reverse "⚙ Ops Console" button. The proxy sidesteps CORS.

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
