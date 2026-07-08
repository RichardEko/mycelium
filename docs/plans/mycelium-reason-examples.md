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
| 0 | one LangChain agent calls **one** mesh skill (the minimal starter the a2a demo skips) | `00_hello_skill.py` ✅ shipped | ✅ echo |
| 1 | typed output through the mesh | `01_typed.py` (`call_typed`) ✅ shipped | ✅ echo |
| 2 | LangGraph **on** Mycelium — state survives restart | `02_durable_state.py` (`MyceliumCheckpointSaver`) ✅ shipped | ✅ echo |
| 3 | cross-node resume — kill A, resume on B | `03_cross_node.py` ✅ shipped | ✅ echo |
| 4 | **routed inference** — LLM calls fail over to a healthy node | `POST /gateway/reason/route` + `ReasonClient.route` ✅ shipped; `04_routed.py` | ✅ echo |
| 5 | **fleet-reasoning traces** — replay/narrate why the graph reasoned | `ReasonClient.trace` (GET `/gateway/reason/trace`); `05_traces.py` ✅ shipped | ✅ echo |
| 6 | **deploy/reheal** — model follows the thread across node death | Rust reheal-node + `06_deploy_reheal.py`; the install→serve bridge | ✅ echo (shipped) · ✅ manual Ollama (`examples/coop/src/bin/reheal_deploy.rs`, shipped) |
| — | teach it | `docs/guide/15-reasoning-and-langgraph.md` (chapter 15) + the `examples/langgraph/README.md` ladder index | — |

## Build sequence (flagship-first, per the 2026-07-08 decision)

1. **PR A — the routing surface + reheal foundation** (unblocks the flagship): `POST
   /gateway/reason/route` (InferenceRouter-backed) ✅ + `ReasonClient.{route,trace}` in `mycelium-py` ✅ +
   the Rust *reheal node* (require_model → mesh blob fetch → `serve_model` bridge —
   `mycelium-reason/examples/reheal_node.rs`) ✅ + the echo-CI flagship demo
   (`examples/langgraph/06_deploy_reheal.py`, in the `python-sdk` job) ✅ **shipped**.

   Seam finding the echo flagship surfaced — **found, then fixed in the router** (a real
   improvement, not a demo workaround; the de-risking earned its keep):
   - **The problem.** A killed node lingers ~90s in a peer's *capability freshness* view
     (3× the 30s re-advertise; it stops refreshing but there is no instant tombstone), so
     `resolve` kept returning it. A mesh RPC to a dead peer has no connection-refused
     fast-fail, so routing to it blocked the full 30s per-attempt timeout. With the router
     ranking equal-load providers by ascending node-id, a dead *lower-id* node poisoned
     **every** post-kill route for ~90s. The first cut of the demo hid this by rigging the
     survivor to hold the lower id — a smell.
   - **The fix (`mycelium-reason/src/route.rs`), two principled changes:**
     1. **Liveness filter** — `candidates()` now intersects with live SWIM membership
        (`GossipAgent::peers()`, plus self), from which a departed node drops near-instantly
        on a graceful close and within SWIM's detection window otherwise — an order of
        magnitude faster than freshness. Canary: `liveness_filter_drops_a_non_peer_cap`
        (injects a fresh cap for a ghost non-peer; fails without the filter).
     2. **Fast failover timeout** — a new `RouterConfig::failover_timeout` (default 8s) caps
        every *non-final* attempt; only the last candidate (or a lone one) gets the full
        `call_timeout`. So a candidate that died inside the detection window costs ~8s, not
        30s, and a genuinely-slow *sole* provider is still not cut off.
     Result: the demo needs **no** node-id rigging — the survivor B keeps the *higher* id
     (the case that used to be slow) and post-kill routes land on it in ≤ ~22s across runs.
     Cross-node routing to a *live* remote provider was never the issue (verified: instant).
   - **Honest ordering, enforced structurally.** The checkpoint gossips A→B, and B fetches
     the model artifact from A over the mesh, both *before* A is killed (once A is dead it
     can serve neither). B's reheal task runs from B's startup and prints a marker on its
     own stdout — the pre-kill structural signal the driver waits on (no routing needed).
     The graph interrupts *before* its LLM node, so node A never calls the model; the only
     inference runs on B after A is dead — the cleanest expression of "the model followed
     the thread."
2. **PR B — the Ollama-manual flagship** variant ✅ **shipped**: `examples/coop/src/bin/reheal_deploy.rs`
   (real GGUF; `model_deploy` machinery + the `serve_model` bridge + `InferenceRouter`; manual, not CI)
   + guide chapter 15's flagship section. Two provider depots each run a `Provisioner` that
   `supervise(profile, 1)`s the model; the origin wins the single-provider election first (a
   structural stagger, so the reheal is unambiguous), and when it is killed the survivor elects,
   streams the GGUF afresh, `ollama create`s it under its own node-unique name, and re-serves the
   routable `llm/{model}` — the app routes real tokens from the survivor. Honest single-machine
   caveat: A and B share one local Ollama daemon, so each `ollama create`s under `{model}-{port}`;
   the streamed bytes + the Mycelium capability follow the thread (per-node Ollama for the true
   multi-machine story). Compile-verified; unrun here (no Ollama in the build env).
3. **PR C — backfill rungs 0–5** ✅ **shipped** (2026-07-08): the five Python demos
   (`00_hello_skill`/`01_typed`/`02_durable_state`/`03_cross_node`/`05_traces`) over the
   proven pieces + the `examples/langgraph/` README ladder index + the echo-rung loop folded
   into the `python-sdk` CI job. Rung 5 needed one small enabling surface — an optional
   `run_id` on `POST /gateway/reason/route` (and `ReasonClient.route(run_id=…)`) so a
   Python-driven routed call *records* a trace (`gw_route` now builds a `TraceRecorder` when
   `run_id` is set). Still open: guide chapter 15 + the rung-6 Ollama-manual variant (PR B).

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
