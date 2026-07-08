# mycelium-reason — the LangChain→LangGraph example ladder (design)

**Status:** 🟡 **IN PROGRESS — started 2026-07-08.** The substrate (crate + Python tier, PRs
#130/#131) is shipped; this plan is the **pedagogy layer**: a runnable rung-ladder from a simple
LangChain starter to the flagship *deploy/reheal* demo, plus guide chapter 15. Sequencing decision
(2026-07-08): **flagship first** (de-risk the hardest integration), then backfill down the ladder.
Backend fidelity: **both** — an EchoBackend variant guards the wiring in CI, a real-Ollama variant is
the showcase (the `model_deploy` pattern).

## Why this exists

Everything the series needs is shipped and *individually tested*, but there is exactly one runnable
LangChain example today (`examples/a2a_langchain/` — the A2A *tool-calling* layer, LangChain→Mycelium),
and the checkpointer / `call_typed` are proven only in pytest. A prospective adopter cannot **watch**
LangGraph run on Mycelium, and cannot see the one story that beats Postgres/Redis on non-commodity
terms: *a graph whose model dependency follows it across a node failure.* This ladder turns proven
capability into a legible, runnable narrative.

## Two gaps that are real work, not just demo-writing

1. **No Python routing surface for wedge ①.** `/gateway/llm/call` resolves one provider and does a
   single RPC — **no load-ranking, no failover** (`http.rs:gw_llm_call` → `providers.first()`).
   `InferenceRouter` (the real routing layer) is a Rust call-side API with **no gateway route**. So a
   LangGraph author gets durable cross-node state today but *not* capability-routed inference. **Fix:**
   `POST /gateway/reason/route` in `mycelium-reason/src/http.rs` backed by `InferenceRouter`, + a Python
   `ReasonClient.route(...)`. This is rung 4 — and the flagship needs it, which is why flagship-first
   still builds it first.
2. **The install→serve bridge.** `model_deploy` proves a real GGUF streams in and generates tokens —
   but via a **direct local-Ollama `OpenAiBackend`**, not through the mesh; the `llm/{model}` capability
   marks *presence*, not a mesh-invocable skill. For "the graph's routed LLM calls land on the node the
   model arrived at," the resuming node must, after install+activation, **`serve_model(model,
   OpenAiBackend→local Ollama)`** — bridging the installed local model into a mesh-routable prompt
   skill. This seam does not exist yet; the flagship builds it.

## Architecture split — Rust nodes, Python driver

The artifact-library wiring (require_model + provisioner + install + `serve_model` bridge) lives in
**Rust** (a *reheal node*, extending `reason_node`); **Python** stays thin — it drives the LangGraph
graph + `MyceliumCheckpointSaver` + routed LLM calls, all over the gateway. This keeps the Python
surface to gateway clients (checkpointer ✓, `call_typed` ✓, + new `route`/`trace` clients) and puts the
reheal choreography where the machinery already is.

## The flagship (rung 6) — deploy/reheal choreography

1. A LangGraph `StateGraph` whose LLM node calls the mesh via **`POST /gateway/reason/route`** (routed,
   load-aware, failover) — so inference follows `llm/{model}` wherever it lives.
2. **Node A** serves the model + the graph runs (via `MyceliumCheckpointSaver` → A's gateway) partway,
   `interrupt`s, checkpoints (state gossips; payloads in the blob tier).
3. **Node A is killed.**
4. **Node B** — which does *not* serve the model — reheals: `require_model(model)` → the artifact
   library streams the model in (echo: a trivial fixture; Ollama: real GGUF via the `model_deploy`
   `BlobRuntime` path) → on install+activate, **`serve_model` bridges it** → `await_ready` → a Python
   driver pointed at B's gateway resumes the thread → the graph's routed LLM calls now land on B → real
   tokens → the **fleet trace** (`replay`/`narrate`) + the **WS2 audit chain** (`anchor`) show
   resume + route + model-arrival as one causal story.

**Two variants:** `06_deploy_reheal` echo-CI (deterministic, in the smoke job — proves the wiring) and
`06_deploy_reheal` Ollama-manual (real weights, excluded from CI like `model_deploy` — the showcase).

## The rung ladder

Homes: a new `examples/langgraph/` dir (the LangGraph ladder) — distinct from `examples/a2a_langchain/`
(which stays the A2A tool-calling example, a *different layer*). Each Python rung runs against a Rust
`reason_node`; rungs 0–5 EchoBackend → CI; rung 6 both.

| Rung | Teaches | Deliverable | CI |
|---|---|---|---|
| 0 | one LangChain agent calls **one** mesh skill (the minimal starter the a2a demo skips) | `00_hello_skill.py` | ✅ echo |
| 1 | typed output through the mesh | `01_typed.py` (`call_typed`) | ✅ echo |
| 2 | LangGraph **on** Mycelium — state survives restart | `02_durable_state.py` (`MyceliumCheckpointSaver`) | ✅ echo |
| 3 | cross-node resume — kill A, resume on B | `03_cross_node.py` | ✅ echo |
| 4 | **routed inference** — LLM calls fail over to a healthy node | `POST /gateway/reason/route` + `ReasonClient.route`; `04_routed.py` | ✅ echo |
| 5 | **fleet-reasoning traces** — replay/narrate why the graph reasoned | `ReasonClient.trace` (GET `/gateway/reason/trace`); `05_traces.py` | ✅ echo |
| 6 | **deploy/reheal** — model follows the thread across node death | Rust reheal-node + `06_deploy_reheal.py`; the install→serve bridge | ✅ echo · manual Ollama |
| — | teach it | `docs/guide/15-reasoning-and-langgraph.md` (chapter 15) + the `examples/langgraph/README.md` ladder index | — |

## Build sequence (flagship-first, per the 2026-07-08 decision)

1. **PR A — the routing surface + reheal foundation** (unblocks the flagship): `POST
   /gateway/reason/route` (InferenceRouter-backed) + `ReasonClient.{route,trace}` in `mycelium-py` +
   the Rust *reheal node* (require_model → install → `serve_model` bridge) + the echo-CI flagship demo.
2. **PR B — the Ollama-manual flagship** variant (real GGUF; `model_deploy` machinery; manual, not CI)
   + guide chapter 15's flagship section.
3. **PR C — backfill rungs 0–5** (Python demos over proven pieces) + the `examples/langgraph/` README
   ladder index + the rest of chapter 15 + a `langgraph-smoke` CI extension (or fold into `python-sdk`).

## CI vs manual

Rungs 0–5 + the rung-6 echo variant are deterministic → CI (extend the `python-sdk` job or add
`langgraph-smoke`). The rung-6 Ollama variant needs a live model → **manual**, documented (mirrors
`model_deploy`'s exclusion from `ci_smoke.sh`).

## Open questions

- **Routing endpoint shape** — does `/gateway/reason/route` take `{model, input, context, constraints}`
  and return `{output, provider, attempt}`, or should it also stream (SSE, like `/gateway/llm/stream`)?
  Start non-streaming; add SSE if a rung needs it.
- **`require_model` gateway route** — the reheal node does require_model in Rust, so no Python route is
  needed for the flagship. If a *pure-Python* reheal driver is later wanted, add `POST
  /gateway/reason/require` + `await_ready` polling. Deferred until demanded.
- **Echo "model install" fixture** — the echo variant needs a trivial installable artifact to stream in
  (so the reheal choreography is real, not faked). Reuse the artifact-library `BlobRuntime` with a tiny
  blob + an activation hook that just flips `serve_model` on. Keep it honest: label it as a fixture.

## Non-goals

- Re-teaching A2A tool-calling (that's `examples/a2a_langchain/`, kept separate — the anti-scatter map
  in the checkpointer README already distinguishes the layers).
- A production LangGraph deployment guide — this is a teaching ladder, not an ops runbook.
