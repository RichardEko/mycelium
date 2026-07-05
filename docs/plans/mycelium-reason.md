# mycelium-reason — design sketch

**Status:** 🔵 **PROPOSED — v3.0 candidate, not started.** A demand-driven DX companion; the substrate
needs no new capability for most of it (see *What already exists*). Positioning source:
[`../wiki/domain/pattern-coverage.md`](../wiki/domain/pattern-coverage.md) → the LLM-authoring DX axis.

## What it is

The **LLM-authoring DX layer** for Mycelium — the ergonomic surface for building the *reasoning* part
of an agent (prompt → tool-loop → typed output → memory → traces), sitting beside the coordination
companions (tuple-space / blackboard / wiki) on the **public API only**. It is *not* a LangGraph /
CrewAI port; it is those ergonomics rebuilt so each feature is **the coordinator-free substrate's
differentiator applied to reasoning**.

Axis note: reasoning DX is orthogonal to coordination-pattern coverage. Mycelium is strong on the
latter and already has real pieces of the former (`PromptTemplate`, `LlmBackend`/`OpenAiBackend` +
streaming, MCP tools, the Layer-V `AgentStateMachine` with `max_turns`/`tool_budget`, HLC audit +
OTEL). The gaps are the *reasoning-framework* ergonomics: reasoning-graph authoring, typed output +
retry, model-call resilience, conversation memory, and run-level evals.

## The compelling frame — substrate-native, not a framework port

The same coordinator-free properties that make *coordination* resilient make *reasoning* resilient:
graphs that outlive their orchestrator, inference routing with no central proxy, memory that hands off
between agents, and tamper-evident causal traces of the whole fleet's thinking. That is additive value
a single-process framework **structurally cannot** offer.

## The closures — ranked, leading with the differentiating wedge

**① Capability-routed inference (the wedge — mostly composes; no central proxy).** Model capacity is
already a mesh capability (`cap/{node}/llm/inference`). "Fallback" becomes **capability resolution +
opacity back-pressure**: route each call to whichever node advertises a healthy model with headroom;
shed load via opacity. Elastic, load-aware inference across the fleet — vs a LiteLLM-style central
proxy you operate. *New code: a resilience/routing policy over `LlmBackend`.*

**② Fleet-reasoning traces (the wedge — mostly composes).** Every turn/tool-call already seals to the
HLC audit chain; `/gateway/explain` reconstructs cross-node causality. Extend to LLM-run granularity:
**tamper-evident, causal, replayable traces of why the whole fleet reasoned as it did** — single-process
tracers see one process. *New code: run-level trace records + an explain view; replay via the event log.*

**③ Typed output + auto-retry (cheap, high-value).** `call_typed::<T>(prompt)` binds the **existing
schema registry** to the call: validate → feed the parse error back → retry. Because schemas are
gossiped, typed reasoning contracts are **fleet-wide**, not per-process. *New code: small glue.*

**④ Reasoning-graph authoring (more work).** An authoring veneer where each node is a mesh **skill** and
edges are capability wiring — so the graph auto-scales and **survives node/orchestrator loss** (the
AFN / tuple-space substrate already does the execution; this is the ergonomic layer). *New code: a
graph DSL/builder over skills + tuple-space.*

**⑤ Hand-off memory (more work; = the governed-memory composable, cashed out).** Per-task history on
`kv().append` + wiki curated long-term memory + auto-summarize-to-window. The Layer-V `Suspended` /
resume already lets a task's context **hand off between agents / survive a crash** — impossible for an
in-process buffer. *New code: a memory/window abstraction.*

## What already exists (the composition base — this is mostly packaging)

`PromptTemplate` (KV, fleet-shared) · `LlmBackend`/`OpenAiBackend` + `/gateway/llm/stream` · MCP tool
discovery · `AgentStateMachine` (Planning/Invoking/Reflecting/Suspended/… + `max_turns`/`tool_budget`
+ `watch_mesh_states`) · schema registry (`schemas/`) · HLC audit chain + OTEL + `/gateway/explain` ·
`kv().append`/`scan_log` event log · `mycelium-wiki` durable memory.

## What's genuinely new (small, scoped)

The backend resilience/routing policy (①), run-level trace records (②), the `call_typed` binding (③),
the graph builder (④), the memory/window abstraction (⑤). All **application-layer, on the public API**
— zero core changes (the companion-crate contract), consistent with the v3.0 "packaging, not new
substrate" thesis.

## Non-goals

- A first-class **orchestrator** (the substrate's deliberate non-goal — see `docs/philosophy.html`).
- **Declarative prompt optimization** (DSPy-style compile-from-examples) — research-track, watch-only.
- Reimplementing a provider SDK — `LlmBackend` stays a thin trait; bring your own model.

## Expressible ≠ validated — the deliverable is a tested pattern gallery

Per the pattern-coverage caveat, each closure is a **hypothesis until it has a working, CI-tested
example**. The companion earns its claims the way blackboard/tuple-space did — worked demos at the
`ci_smoke` bar, one per closure. Ship **①③② first** (differentiating + cheap); ④⑤ on demand.

## Trigger to revisit

A customer building reasoning agents *on the mesh* who hits the DX cliff (writes their own
retry/routing/trace glue), or a positioning need to answer "why not just LangGraph on top?" with an
ergonomic story, not only a coordination one.
