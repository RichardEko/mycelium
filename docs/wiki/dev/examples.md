# dev/examples — the runnable suites

↑ [dev/](dev.md)

**Index + doc standard:** [`examples/README.md`](../../../examples/README.md) is the front-door
index of every example, the shared-setup section (toolchain / Ollama / Python tier — so no README
re-explains it), and the **doc template** all example READMEs follow (`## Objective` · `## How to
run` · `## What it demonstrates` · `## Dev notes`; two variants — single-example and suite — sharing
one per-example block). `coop/` is the reference implementation of the suite shape. **Every example
directory now carries a README** following this template — including the companion/reason/guardrails
`examples/` dirs (added 2026-07-15), so the capability-matrix links land on run-docs, never raw source.

**Capability matrix — the single faceted index.** The front-door
[`examples/README.md` § The capability matrix](../../../examples/README.md#the-capability-matrix) is the
**source of truth**: one row per example, fingerprinted by stack layer (I gossip-KV · II signal-mesh ·
III consensus · IV capability/agent) **and** facet — *Level* (Intro/Adv, ★ flagship) · *Surface*
(Web/CLI) · *LLM* (real/mock/none) · *Audit* · *Metrics*. Update it when an example's layer **or** a
facet changes (a demo gains a browser UI, the metrics feature, a signed-audit surface). The same grid
rendered — colour-coded dots + facet chips + a per-layer summary strip, self-contained, opens offline —
is [`examples-layer-matrix.html`](examples-layer-matrix.html).
Layer explainers: gossip-KV [ch01](../../guide/01-gossip-kv.md) · signal-mesh
[ch03](../../guide/03-signals.md) · consensus [ch04](../../guide/04-consensus.md).

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
  peer-cache install) · diagnostics. The five **runtime-loading** demos (`catalog` ·
  `mcp_toolgrowth` · `provisioning` · `model_deploy` · `reheal_deploy`) each print a
  **`dynamically loaded artifact` banner** at startup via `coop::common::announce_loads`
  (a `LOADS: &[Loads]` const, mirrored in a `## Loads` doc block) — the CLI analogue of the
  browser [concepts box](ui-example-contract.md), declaring **content · type · loaded-from**.
  `ci_smoke.sh` runs the twelve CI demos Docker-free (CI
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
  any CI smoke). All browser examples follow the [UI-example contract](ui-example-contract.md):
  gateway+metrics on, Ops Console linked, opt-in audit, and a "what you're seeing" concepts box. `conway` (`cargo run --example conway --features metrics` → `:8090`, a 256-agent gossip-KV Game of Life,
  terminal ANSI **and** an HTML canvas) · `conway-gpu` (`cargo run --release -p conway-gpu`, a 512×512
  GPU/wgpu render) · `microgrid_viz` (`cargo run -p mycelium-blackboard --example microgrid_viz --features gateway,metrics` →
  `:8091`, the blackboard `rd`/`in` energy co-op with a live exactly-once badge) · `stigmergy_viz`
  (`cargo run -p mycelium-coop-examples --bin stigmergy_viz --features metrics` → `:8092`, pheromone reroute — opacity
  glow + dispatch routing around the busy depot) · `redistribution_viz` (`cargo run -p
  mycelium-tuple-space --example redistribution_viz` → `:8093`, the intake→sorted→routed→delivered
  pipeline flow) · `llm_council_viz` (`cargo run -p mycelium-coop-examples --bin llm_council_viz --features metrics` →
  `:8094`, the fan-out · synthesis · critic↔reviser-refinement DAG; `EchoBackend`, **no LLM key**) ·
  `provisioning_viz` (`cargo run -p mycelium-coop-examples --features wasm,metrics --bin provisioning_viz`
  → `:8097`, the **autonomic self-heal** loop — a capability self-provisions then heals onto a standby
  when the active node is killed, no coordinator) · `catalog_viz` (`… --bin catalog_viz` → `:8098`,
  **origin-death survival** — the librarian dies + its library is deleted, yet a late node still installs
  from a verified peer cache). Each opens `http://127.0.0.1:80xx/`. The six `*_viz` are **visual
  variants** of the batch demos (`microgrid` / `stigmergy` / `redistribution` / `llm_council` /
  `provisioning` / `catalog`), which remain the CI-gated versions; the four coop `*_viz` bins are
  *additional* to the fourteen above. A showcase paces its loop so the
  emergence is legible (e.g. a `THINK` dwell for the instant `EchoBackend`) — the batch demos race to
  completion, a showcase must be *watchable*.
- **Ops Console** (`examples/ops_console/` — `main.rs` + `.html` + README, `:8099`): a generic, read-only dashboard over
  **any** gateway-enabled node's operational endpoints — `/stats` (runtime + tripwires),
  `/gateway/fleet` (cluster snapshot), `/gateway/diagnose` (the Legible-Emergence *fleet narrative*),
  `/gateway/audit` (the signed audit trail — `compliance`-built nodes), `/gateway/kv/keys`, `/metrics`
  — with a server-side proxy so the browser skips CORS. `cargo run
  --example ops_console`, then point the host box at the community cluster (`:9050`), a coop demo, or a
  showcase's printed gateway URL (`conway` `:9090`; `stigmergy_viz`/`llm_council_viz` print theirs;
  `microgrid_viz` `:9091` / `redistribution_viz` `:9093` need `--features gateway`, off by default in
  those companion crates). **Dev/reference tool, explicitly *not* a shipped control plane** — the
  library-not-platform line holds; a customer forks it or scrapes `/metrics` into Grafana.
  *Two-way linking (the `ui/viz` convention):* every browser demo self-advertises its UI as two KV
  keys — `ui/viz` = `http://host:port/`, `ui/label` = a short name — which the console reads from the
  target and surfaces as a live "↗ label" link; each dashboard carries the reverse "⚙ Ops Console"
  button pre-targeted at its own gateway. This spans both the visual showcases above **and** the
  browser *operator* demos brought onto the same dark theme — `three_node_demo` (chat `:8080`, mgmt
  `:8090`) and `llm_agent` (Mesh Control `:8100`, gateways `:9100`–`:9102`). One KV convention closes
  the loop both ways, and the console hard-codes no demo (a node that advertises nothing just hides the
  link). Any gateway example with a UI opts in with two `kv().set(...)` writes — reference:
  `redistribution_viz.rs`.
- **Agentic Flow Networks** (`examples/fluid_pipeline/`): 10-worker pool, 4-stage pipeline,
  `PIPELINE_MODE=pull` (canonical, tuple-space) vs `push` (baseline comparison). CI
  `afn-smoke`. Concept essay: `flow_networks.html`.
- **A2A LangChain/AutoGen** (`examples/a2a_langchain/`): external agents auto-discover
  Mycelium skills via `/.well-known/agent.json`.
- **Skills / community cluster** (`examples/community/`, the `skillrunner` bin at `:9050`): LLM agents
  as first-class mesh citizens (skills = capabilities, live load-balancing). The de-facto **audit**
  demo — every invocation writes a signed audit record; the mgmt UI (`:9050/mgmt`) shows the trail
  (also `GET /gateway/audit` on `compliance`-built nodes).
- **Reasoning / LangGraph** (`examples/langgraph/` + `mycelium-reason/examples/`): the 7-rung
  LangGraph-on-Mycelium ladder (Python) over a Rust reason mesh — `reason_node` (the long-running
  gateway node the rungs drive), `reheal_node` (the deploy/reheal flagship, rung 6), `fleet_reasoning`
  (all three Tier-3 wedges in-process). Guide:
  [15-reasoning-and-langgraph](../../guide/15-reasoning-and-langgraph.md).
- **Guardrails** (`mycelium-guardrails/examples/`): `guardrail_wedge` (an off-allowlist caller
  structurally stopped at a Tier-C gate, with a cryptographic denial proof) + `guardrail_fleet` (all
  three policy tiers composed in one co-op fleet) + `guardrail_viz` — the **browser showcase**: fire
  invocations at the gate and watch admit/deny + the tamper-evident denial proof reconstructed live by
  a neutral observer; gateway + `ui/viz` (its `/gateway/audit` *is* the seal — `:8096`,
  `--features compliance,gateway,metrics-export`).
- **Wiki companion** (`mycelium-wiki/examples/`): `wiki_chat` — import documents, then chat grounded in
  the shared wiki (Phase-5 worked example) · `wiki_council_viz` — the browser showcase: a **live chat**
  over a fleet of wiki-grounded specialists (Transport · Energy · Planning · Budget), fan-out +
  synthesis with citations. Each specialist *phrases* its grounded answer via a **local model served on
  the mesh** — a local Ollama registered as the `llm/{model}` capability (`register_prompt_skill`),
  invoked over the mesh (`call_prompt_skill` → resolve + RPC); deterministic grounded-extraction
  fallback when Ollama is absent. Gateway + `ui/viz` (`:8095`, `--features gateway,llm,metrics`).
- Integration suite: **13 Docker scenarios** (`make test`, 4-node cluster) + the consistency
  overlay (`make test-overlay`, 3-node consensus); scale suites in
  [testing/scale-tests](testing/scale-tests.md).
