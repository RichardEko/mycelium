# mycelium-reason — LLM DX strategy (design sketch)

**Status:** 🔵 **PROPOSED — v3.0 candidate, not started.** Build-vs-adopt resolved to a **three-tier
strategy** (build / adopt / interop) with a **Tier-3-first** sequence. The substrate needs no new
capability. Positioning source: [`../wiki/domain/pattern-coverage.md`](../wiki/domain/pattern-coverage.md)
→ the LLM-authoring DX axis.

## The question this settles

"Roll our own LLM DX framework, or map/support a popular one?" — **neither extreme.** The popular DX
(LangGraph, Instructor, Pydantic AI, CrewAI) is almost all **Python** and operates at layers a
*substrate sits under*. So: **adopt** the commodity layers, **be the distributed backend** for the
orchestration layer, and **build** only the differentiators nothing else can offer. Rolling a full
framework would reimplement commodities in the wrong language for a community that won't switch tools
to get them.

## The compelling frame — substrate-native, not a framework port

The same coordinator-free properties that make *coordination* resilient make *reasoning* resilient:
inference routed with no central proxy, tamper-evident causal traces of the whole fleet's thinking,
memory that hands off between agents, graphs that outlive their orchestrator. Additive value a
single-process framework **structurally cannot** offer.

## The three tiers

### Tier 3 — BUILD (our differentiators; un-adoptable because nothing else has the mesh)
- **① Capability-routed inference** — route each call to a healthy model-advertising node
  (`cap/{node}/llm/inference`) via capability resolution + opacity back-pressure. Elastic, load-aware
  inference across the fleet, **no central proxy** (vs a LiteLLM proxy you operate). *New: a
  resilience/routing policy over `LlmBackend`.*
- **② Fleet-reasoning traces** — extend the HLC audit chain + `/gateway/explain` to LLM-run
  granularity: **tamper-evident, causal, replayable traces of why the whole fleet reasoned as it did.**
  Single-process tracers see one process. *New: run-level trace records; replay via the event log.*

Both **mostly compose** from existing substrate. Exposed through `mycelium-py` so they compose *with*
the Tier-1/2 tools, not replace them.

### Tier 1 — ADOPT (commodity library layer; wrap, don't rebuild)
- **Typed output → [Instructor](https://python.useinstructor.com/)** (~3M downloads/mo — a thin client
  patch) or **[Pydantic AI](https://ai.pydantic.dev/)**. `mycelium-py` wraps these for the `call_typed`
  closure — no custom typed-output+retry. (Schemas stay fleet-shared via the registry.)
- **Provider access →** provider SDKs / LiteLLM-*as-library* for the 100+ adapters; drop its central
  *proxy* — Tier 3 ① replaces it.

### Tier 2 — INTEROP / BE-THE-BACKEND (map/support the popular frameworks)
- **`langgraph-checkpoint-mycelium`** — LangGraph's pluggable
  [`BaseCheckpointSaver`/Store protocol](https://docs.langchain.com/oss/python/langgraph/persistence)
  (`get_tuple`/`list`/`put`/`put_writes`) backed by Mycelium KV + the `append`/`scan_log` log overlay.
  One-line swap → LangGraph agent state becomes **coordinator-free, gossip-replicated, resumable across
  nodes** (the `Suspended`/resume + hand-off value, delivered through *their* abstraction). Directly
  answers "why not just LangGraph?" → *"Use it — on Mycelium; now it survives node loss and hands off
  across the fleet."*
- Extends to CrewAI / AutoGen memory backends + the existing MCP + A2A adapters.

**Relationship to the existing `examples/a2a_langchain/` — a different layer, not a duplicate (avoid
scatter).** That example is **A2A interop, direction LangChain → Mycelium**: a LangChain/AutoGen agent
discovers Mycelium *skills* via `/.well-known/agent.json` and calls them as tools (Mycelium is the
*tool provider*). The checkpointer is the **reverse and deeper**: **LangGraph runs *on* Mycelium**, its
graph state backed by the mesh (Mycelium is the *resilient state backend*). These teach different
things — do **not** merge them. Anti-scatter rule for this deliverable: ship **one** *Mycelium ×
LangChain/LangGraph integration map* (interop edge = A2A, exists · state backend = checkpointer ·
reasoning wedges = Tier 3 · typed output = Tier 1) that labels each touchpoint and when to use it, so
there is a single coherent integration story rather than several look-alike "LangChain examples."

## Sequencing — Tier 3 first, then Tier 1 ∥ Tier 2

**Differentiators first, to a *validated wedge*** (one CI-tested example each — the pattern-gallery
bar; not gold-plated). Rationale: the differentiator is what gives the adopt/interop its **pull**. A
Mycelium-backed LangGraph checkpointer that is *only* durable state competes with Postgres/Redis on
commodity terms and loses on maturity; the same checkpointer that *also* surfaces capability-routed
inference + fleet traces is a category of one. Build the reason-to-adopt first, then distribute it.

Then **Tier 1 (Instructor wrap) ∥ Tier 2 (LangGraph checkpointer)** in parallel — independent surfaces
(`mycelium-py` vs a LangGraph package), and **Tier 2 is built to *expose* the Tier-3 wedges** so it
lands differentiated, not commoditised.

**Trade-off, named honestly:** Tier-3-first pushes time-to-first-external-user slightly later than an
adopt-first land-grab would. For a thesis-led, pre-adoption project that is the right call —
*why Mycelium* before *Mycelium everywhere*.

## Concrete deliverables

| Tier | Deliverable | Home | Nature |
|---|---|---|---|
| 3 (first) | capability-routed inference · fleet-reasoning traces | `mycelium-reason` crate + `mycelium-py` | build (mostly composes) |
| 1 (∥) | `call_typed` over Instructor / Pydantic AI | `mycelium-py` | adopt |
| 2 (∥) | `langgraph-checkpoint-mycelium` | new Python package | interop / be-the-backend |

## What already exists (the composition base — mostly packaging)

`PromptTemplate` (KV, fleet-shared) · `LlmBackend`/`OpenAiBackend` + `/gateway/llm/stream` · MCP tool
discovery · `AgentStateMachine` (Planning/Invoking/Reflecting/Suspended/… + `max_turns`/`tool_budget`
+ `watch_mesh_states`) · schema registry (`schemas/`) · HLC audit chain + OTEL + `/gateway/explain` ·
`kv().append`/`scan_log` event log · `mycelium-wiki` durable memory. The Rust core needs **zero
changes** (companion-crate contract); integration is application-layer, much of it in `mycelium-py`.

## Non-goals

- A first-class **orchestrator** (the substrate's deliberate non-goal — `docs/philosophy.html`).
- **Declarative prompt optimization** (DSPy-style compile-from-examples) — research-track, watch-only.
- Reimplementing a provider SDK, or a typed-output library — Tier 1 adopts those.

## Expressible ≠ validated

Every wedge and the checkpointer fit are **hypotheses until tested**. The checkpointer mapping
(versioned KV + log ↔ checkpoints + pending writes) *looks* natural but needs a **one-day spike**
before commitment. Each Tier-3 wedge earns its claim with a `ci_smoke`-bar example — the same bar
blackboard/tuple-space met. This also raises **`mycelium-py` to a first-class citizen** — a deliberate
strategic choice, since the ecosystem the strategy targets is Python.

## Trigger to revisit

A customer building reasoning agents *on the mesh* who hits the DX cliff, or a positioning need to
answer "why not just LangGraph on top?" with an ergonomic story, not only a coordination one.
