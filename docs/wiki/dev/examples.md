# dev/examples — the runnable suites

↑ [dev/](dev.md)

**Index + doc standard:** [`examples/README.md`](../../../examples/README.md) is the front-door
index of every example, the shared-setup section (toolchain / Ollama / Python tier — so no README
re-explains it), and the **doc template** all example READMEs follow (`## Objective` · `## How to
run` · `## What it demonstrates` · `## Dev notes`; two variants — single-example and suite — sharing
one per-example block). `coop/` is the reference implementation of the suite shape.

- **Starter ladder (zero-LLM, the junior-dev entry point).** `hello_mesh`
  (`examples/hello_mesh.rs`, ~25 lines: two agents on loopback share a KV value by gossip) →
  `hello_capability` (`examples/hello_capability.rs`, ~45 lines: one node advertises `math/double`
  and serves it, another resolves it *by name* and calls it over RPC — broker-less discovery in one
  file) → `llm_agent` (the richer, LLM-driven capability version). The README front-door leads with
  `hello_mesh`; guide chapters 01–02 explain them. Added #142/#143.
- **Food-Rescue Co-op suite** (`examples/coop/`, workspace member
  `mycelium-coop-examples`): **fourteen** demos (twelve CI + two manual real-model) composed in one constructive world (depot
  nodes rescuing surplus food, no dispatcher) — mailbox_llm · stigmergy · elastic_intent ·
  provisioning ⭐ (the autonomic loop) · federation_facts · rotation · consensus ·
  llm_pipeline · mcp_toolgrowth (real **code arrival**, bridged over MCP) · llm_council ·
  catalog (the durable **library**: runtime-read origin, librarian, origin-death →
  peer-cache install) · diagnostics. `ci_smoke.sh` runs the twelve CI demos Docker-free (CI
  `coop-smoke`). Plus one **manual** demo, `model_deploy` — real GGUF weights **and their
  deployment profile** (system prompt + parameters, referencing the weights by content
  address — design §4.3.1) deployed through the artifact library into Ollama, generating
  real tokens under the governed profile (needs the Ollama daemon; deliberately not in
  the smoke). Per-demo docs: `examples/coop/README.md`; plan:
  `docs/plans/example-suite.md`. **The suite anchors the developer docs** (guide 00 /
  14-patterns / cookbook). Domain preference: constructive framings (microgrids, food
  redistribution), never war-room/crisis.
- **Visual showcases** (the `/state`-JSON + polling-canvas pattern `examples/conway.rs` established):
  browser-animated demos for pitch/booth/onboarding, run **continuously** (Ctrl-C to stop; **not** in
  any CI smoke). `conway` (`cargo run --example conway` → `:8090`, a 256-agent gossip-KV Game of Life,
  terminal ANSI **and** an HTML canvas) · `conway-gpu` (`cargo run --release -p conway-gpu`, a 512×512
  GPU/wgpu render) · `microgrid_viz` (`cargo run -p mycelium-blackboard --example microgrid_viz` →
  `:8091`, the blackboard `rd`/`in` energy co-op with a live exactly-once badge) · `stigmergy_viz`
  (`cargo run -p mycelium-coop-examples --bin stigmergy_viz` → `:8092`, pheromone reroute — opacity
  glow + dispatch routing around the busy depot) · `redistribution_viz` (`cargo run -p
  mycelium-tuple-space --example redistribution_viz` → `:8093`, the intake→sorted→routed→delivered
  pipeline flow) · `llm_council_viz` (`cargo run -p mycelium-coop-examples --bin llm_council_viz` →
  `:8094`, the fan-out · synthesis · critic↔reviser-refinement DAG; `EchoBackend`, **no LLM key**).
  Each opens `http://127.0.0.1:80xx/`. The four `*_viz` are **visual variants** of the batch demos
  (`microgrid` / `stigmergy` / `redistribution` / `llm_council`), which remain the CI-gated versions;
  the two coop `*_viz` bins are *additional* to the fourteen above. A showcase paces its loop so the
  emergence is legible (e.g. a `THINK` dwell for the instant `EchoBackend`) — the batch demos race to
  completion, a showcase must be *watchable*.
- **Agentic Flow Networks** (`examples/fluid_pipeline/`): 10-worker pool, 4-stage pipeline,
  `PIPELINE_MODE=pull` (canonical, tuple-space) vs `push` (baseline comparison). CI
  `afn-smoke`. Concept essay: `flow_networks.html`.
- **A2A LangChain/AutoGen** (`examples/a2a_langchain/`): external agents auto-discover
  Mycelium skills via `/.well-known/agent.json`.
- Integration suite: **13 Docker scenarios** (`make test`, 4-node cluster) + the consistency
  overlay (`make test-overlay`, 3-node consensus); scale suites in
  [testing/scale-tests](testing/scale-tests.md).
