# 2026-07-05 — ingest: pattern-coverage positioning artifact + v3.0 candidates

> **⚠ The framing below was corrected the same day — see the Correction section at the bottom.**
> The "Native / Expressible / Gap · five additive gaps" wording is superseded; the canonical page
> `pattern-coverage.md` and `ROADMAP.md` use **Native / Composable / one genuine gap (ANP)**.

New leaf page `domain/pattern-coverage.md` (linked from `domain.md`): the distributed-agentic
pattern landscape (2026-07 scan) mapped against the substrate — Native / Expressible / Gap, the
orchestrator non-goal, and five additive gaps. Coverage claims are code-anchored (verified this
session: mailbox = durable HLC-ordered delivery; `demand()` = pressure not bidding; no app-event
replay log).

The five gaps are also written into `ROADMAP.md` → **v3.0 Candidates** (proposed, demand-driven):
replayable event-sourced log, auction/bidding, DAG self-evolving network, ANP conformance, governed
shared memory. Disposition tags: engineering-ready-if-demanded (event-log, auction) · standards-
tracking (ANP) · watch-only frontier (DAG, governed-memory). No page contradicts the code; all links resolve.

## Correction (same day, after a composability challenge)

Reframed Native/Expressible/Gap → **Native / Composable / one genuine gap**. Two fixes:
- **Event-sourced log was NOT a gap — it's Native:** `KvHandle::{append, scan_log(from,to),
  subscribe_log(since_hlc), compact_log}` (`mycelium-core/src/kv_handle.rs`), a `log/{stream}/{hlc}`
  overlay on gossip KV. My first grep scoped to `src/` and missed `mycelium-core/src/` — the
  exact "assert a gap without grepping the whole workspace" miss the calibration ledger records.
- **Auction, DAG-network, governed-memory are compositions, not gaps** — Contract-Net over
  signals+append+consensus; the `advertise`/`declare_requirement`/`resolve_wiring` dynamic wiring
  graph; access-broker+authz + HLC read-stamps. They belong in *Composable* (packaging candidates).
- **Only ANP is a genuine gap** (external wire-protocol conformance = an edge adapter, like `a2a`).

ROADMAP v3.0 section retitled "packaging companions + one protocol adapter" to match.

## Addendum — LLM-authoring DX axis + mycelium-reason (same day)

Added a distinct-axis section to `pattern-coverage.md`: reasoning-authoring DX is orthogonal to
coordination coverage. Mycelium has real pieces (`PromptTemplate`, `LlmBackend`+streaming, MCP,
Layer-V `AgentStateMachine` max_turns/tool_budget, HLC audit + `/gateway/explain`); the gaps are the
reasoning-framework ergonomics. Proposed the **`mycelium-reason`** DX companion — design sketch
`docs/plans/mycelium-reason.md`, ROADMAP → v3.0. Framed substrate-native (capability-routed inference,
fleet-reasoning traces, hand-off memory, orchestrator-proof graphs), mostly packaging, lead wedges
①③②. Same expressible≠validated caveat: a tested pattern gallery earns the claim.

## Addendum 2 — LLM DX build-vs-adopt resolved (three-tier, Tier-3-first)

`mycelium-reason.md` reworked from "build our own 5 closures" to a **three-tier strategy**:
- **Tier 3 BUILD** (differentiators, un-adoptable): ① capability-routed inference (no central proxy),
  ② fleet-reasoning traces (HLC audit + /gateway/explain).
- **Tier 1 ADOPT**: typed output via Instructor (~3M dl/mo) / Pydantic AI, wrapped in mycelium-py.
- **Tier 2 INTEROP/BE-THE-BACKEND**: `langgraph-checkpoint-mycelium` on LangGraph's pluggable
  BaseCheckpointSaver/Store protocol (verified via LangChain persistence docs) — one-line swap →
  coordinator-free resumable-across-nodes state; the "why not just LangGraph?" rebuttal.
- **Sequence (user-preferred, agreed): Tier 3 first to a CI-tested wedge, then Tier 1 ∥ Tier 2**, with
  Tier 2 exposing the Tier-3 wedges so interop lands differentiated. Trade-off named: slightly later
  first-external-user vs an adopt-first land-grab — correct for a thesis-led pre-adoption project.
- Raises `mycelium-py` to first-class (the target ecosystem is Python). Core needs zero changes.
- Discipline held: checkpointer fit is expressible, not validated — flagged for a 1-day spike.

## Addendum 3 (2026-07-06) — rescoped to coordination patterns; RAG/HITL/guardrails

Critique found the matrix overclaimed "the agentic space." Rescoped to **coordination** patterns +
added the boundary honestly:
- **Use-case functions (not substrate gaps):** RAG/retrieval, HITL/approval, and *content* guardrails
  are external services a group accesses *through* the mesh — the wiki control-plane/data-plane
  precedent (store off-cluster, accessed via capability + access broker). RAG especially: Mycelium is
  not a vector store and needn't be.
- **Structural guardrails = a native strength + differentiator:** what an agent may *do* (receiver-side
  `Boundary` + capability authz + tool_budget + tamper-evident audit) enforced per-receiver with **no
  central chokepoint** — vs the mainstream "guardrail proxy" (itself a coordinator). Flagged as a
  candidate v3.0 wedge; honest caveats kept (action-not-content; promise-strength/legibility; reframe
  until a worked example exists). ROADMAP v3.0 scope-note + domain.md folder-note updated to match.

## Addendum 4 (2026-07-06) — structural guardrails promoted to a v3.0 PRIMARY

Per user direction, structural guardrails is now a **primary v3.0 deliverable alongside the DX
companion** — new sketch `docs/plans/mycelium-guardrails.md`. Value: *what an agent may do*
(receiver-side `Boundary` + capability authz + CT revocation + tool_budget + tamper-evident audit),
enforced per-receiver with **no central chokepoint** vs the mainstream "guardrail proxy" (itself a
coordinator). Mostly packaging; only new code is an ergonomic policy API compiling one declaration
down to the existing mechanisms; lead wedge = agent structurally stopped at a boundary + audit proves
it. Honest limits kept: promise-strength (per-node, legible-not-mandated), eventually-consistent policy
(gossip-speed revocation), reframe-until-tested. ROADMAP v3.0 now lists two primaries (reason +
guardrails); pattern-coverage + domain.md + plans/README updated.
