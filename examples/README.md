# Mycelium examples

Every example is a real, runnable program built on the **public API** — no private hooks. This
page is the index, the shared setup (so no example re-explains it), and the **doc template** all
example READMEs follow.

New here? Start with the zero-setup ladder, then pick a cluster below.

## The index

**Starter ladder (zero LLM, one file each — the junior-dev entry point):**

| Example | What it demonstrates | Run |
|---|---|---|
| [`hello_mesh.rs`](hello_mesh.rs) | Two agents share a KV value by gossip — the substrate in ~25 lines | `cargo run --example hello_mesh` |
| [`hello_capability.rs`](hello_capability.rs) | Broker-less discovery + RPC: advertise `math/double`, resolve it *by name*, call it | `cargo run --example hello_capability` |
| [`conway.rs`](conway.rs) | Conway's Life on a 16×16 gossip mesh — *watch* KV convergence (guide ch. 01) | `cargo run --example conway` |
| [`distributed_lock.rs`](distributed_lock.rs) | Mutual exclusion across 3 nodes + a **fencing token** that refuses stale writers (guide ch. 04) | `cargo run --example distributed_lock` |
| [`invoke_skill.rs`](invoke_skill.rs) | Minimal SkillRunner caller — the smallest skills client (pairs with `community/`) | `cargo run --example invoke_skill` |
| [`semantic_coordination.rs`](semantic_coordination.rs) | Agents coordinating via semantic capability matching | `cargo run --example semantic_coordination` |

**Clusters & suites** (each links to its own README; see [shared setup](#shared-setup) first):

| Cluster | What it demonstrates | LLM? | Doc |
|---|---|:--:|---|
| **Food-Rescue Co-op** — 14 demos (12 CI + 2 manual) | The full pattern catalogue in one constructive world (stigmergy, elastic intent, autonomic provisioning ⭐, federation, consensus, the durable artifact library, real-model deploy/reheal) | some | [`coop/`](coop/README.md) |
| **Mesh Control UI** — `llm_agent` | Capability emergence across 3 nodes with a live topology UI | mock ok | [root example](llm_agent.rs) · [README §Demos](../README.md) |
| **Interactive Chat** — `three_node_demo` | Live MCP tool discovery — tools join a running mesh and the LLM finds them without restart (the same binary drives the Docker integration cluster) | yes | [`chat/`](chat/README.md) |
| **Conway on GPU** — `conway-gpu/` | The 01-chapter visual at 256 agents with a Metal/wgpu compute shader | no | [`conway-gpu/`](conway-gpu/README.md) |
| **Skills** — SkillRunner community cluster | LLM agents as first-class mesh citizens (skills as capabilities, live load-balancing) | yes | [`community/`](community/README.md) |
| **Agentic Flow Networks** — fluid pipeline | Tuple-space pull pipeline (stigmergic backpressure) vs a push baseline, 10 workers | no | [`fluid_pipeline/`](fluid_pipeline/README.md) |
| **A2A interop** — LangChain / AutoGen | External agents auto-discover Mycelium skills via `/.well-known/agent.json` | yes | [`a2a_langchain/`](a2a_langchain/README.md) |
| **Reasoning ladder** — LangGraph-on-Mycelium | 7 rungs from a local checkpointer to a cross-node deploy/reheal flagship | echo model | [`langgraph/`](langgraph/README.md) |

**Browser showcases** (a `/state` feed behind an HTML canvas — the `conway` pattern; run continuously, open `http://127.0.0.1:80xx/`, Ctrl-C to stop; **not** in any CI smoke):

| Showcase | What you see | Run |
|---|---|---|
| [`microgrid_viz`](../mycelium-blackboard/examples/microgrid_viz.rs) | Energy co-op: surplus packets, non-destructive reads, competitive **exactly-once** claims (`:8091`) | `cargo run -p mycelium-blackboard --example microgrid_viz` |
| [`stigmergy_viz`](coop/src/bin/stigmergy_viz.rs) | Pheromone reroute: opacity glow + dispatch routing **around** the busy depot (`:8092`) | `cargo run -p mycelium-coop-examples --bin stigmergy_viz` |
| [`redistribution_viz`](../mycelium-tuple-space/examples/redistribution_viz.rs) | Pipeline flow: `intake→sorted→routed→delivered`, competitive take (`:8093`) | `cargo run -p mycelium-tuple-space --example redistribution_viz` |
| [`llm_council_viz`](coop/src/bin/llm_council_viz.rs) | Deliberation DAG: fan-out · synthesis · critic↔reviser refinement, no LLM key (`:8094`) | `cargo run -p mycelium-coop-examples --bin llm_council_viz` |

(`conway`/`conway-gpu` above are the original visual demos — terminal+canvas and GPU. The four `*_viz` are visual variants of the batch demos, which stay the CI-gated versions.)

**Ops Console** — a generic, read-only dashboard over *any* gateway-enabled node's operational
endpoints, in one place: `/stats` (node runtime + tripwires), `/gateway/fleet` (cluster snapshot),
`/gateway/diagnose` (the Legible-Emergence **fleet narrative** — "why is the fleet in this state", in
plain English), `/gateway/kv/keys`, `/metrics`. It's a *dev/reference* tool — **not** a shipped
control plane (library, not platform); a customer forks it or points Grafana at `/metrics`.

```
cargo run --example ops_console            # → http://127.0.0.1:8099/  (default target 127.0.0.1:9050)
```
Then set the host box to any cluster: the **community** skills cluster (`:9050`), a **coop** demo, or a
**showcase** — `conway` (`:9090`), `stigmergy_viz` / `llm_council_viz` (they print their gateway URL at
startup), or `microgrid_viz` (`:9091`) / `redistribution_viz` (`:9093`) *run with `--features gateway`*
(those companion crates have the gateway off by default). The proxy sidesteps CORS, so the browser
just points at the console.

**Research artifacts** (Paper 1 / 2a experiment runners — reproducible, not tutorials):
[`coordinator_comparison.rs`](coordinator_comparison.rs) (+ [runner](coordinator_comparison_runner.sh)/[plot](coordinator_comparison_plot.py)) ·
[`three_arm_workdist.rs`](three_arm_workdist.rs) (+ [runner](three_arm_runner.sh)/[plot](three_arm_plot.py)) —
see each file's header for the experiment design.

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
