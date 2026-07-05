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
