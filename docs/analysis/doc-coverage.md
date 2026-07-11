# Documentation coverage audit

> **Living matrix**, maintained by the `/doc-coverage` skill — refresh it as a *diff*, not a
> re-derivation. Seed run: **2026-07-10** (below). On each run, prepend a dated changelog entry, update
> the matrix cells that moved, and append to the calibration section any prior `Clear` cell later
> found thin.

A systematic audit of whether every core Mycelium concept has a clear landing across a
**WHAT · WHY · HOW × Dev · Ops** matrix — the doc analogue of `ratings.md`'s code audit. Run with
four parallel auditors (substrate · coordination primitives · fleet/groups/topology · security/
extensions/companions), each opening the actual docs and returning a per-cell verdict with the
file:section or the named gap. This file is the persisted result and the **re-run target**: a
future pass should diff against it, not start from scratch.

**Method.** Adversarial (a name-drop is not "Clear"). Cells: **Clear** (correct mental model +
actionable next step, in a doc addressed to that persona or cross-linked) · **Thin** (present but
partial / wrong-audience / HOW missing) · **Missing** · **N/A** (legitimately not that persona's
concern). WHY is usually shared Dev+Ops.

## Headline

The architecture holds up: `docs/README.md` assigns every area a document *type* and each doc a
declared *audience*, so the WHAT/WHY/HOW × Dev/Ops matrix is how the tree is actually cut, not a
retrofit. At audit time the large majority of cells were already Clear, **no cell was a black
hole**, and the recently-reworked cluster/group/`cluster_name` corner was the strongest (Clear on
every cell). The gaps clustered in one place: **operational failure-mode runbooks for the
consensus/lock family, and Dev guide-chapters for two shipped features.** All of them are now
closed (Tiers 1–3); the residue is genuinely nothing at ✗ or `~`.

## Final matrix (post-remediation)

Legend: ✓ Clear · — N/A. Every cell that was ✗/`~` at audit time is annotated with the pass that
closed it.

| Concept | WHY | WHAT·Dev | HOW·Dev | WHAT·Ops | HOW·Ops |
|---|:--:|:--:|:--:|:--:|:--:|
| Layer I — Gossip KV | ✓ | ✓ | ✓ ᵀ² | ✓ | ✓ |
| Layer II — Signal mesh | ✓ | ✓ | ✓ | ✓ | ✓ |
| Layer III — Consensus | ✓ | ✓ | ✓ ᵀ² | ✓ ᵀ¹ | ✓ ᵀ¹ |
| Capabilities / groups | ✓ | ✓ | ✓ | ✓ | ✓ |
| Distributed locks | ✓ | ✓ | ✓ | ✓ ᵀ¹ | ✓ ᵀ¹ |
| Services / RPC | — | ✓ | ✓ ᵀ² | ✓ | ✓ |
| Schema lifecycle | ✓ | ✓ | ✓ | ✓ | ✓ ᵀ¹ |
| Scopes (Cluster/Group/Individual) | ✓ ᵀ³ | ✓ | ✓ | ✓ ᵀ³ | — |
| Membership + cluster_name | ✓ | ✓ | ✓ | ✓ | ✓ |
| Groups (three kinds) | ✓ | ✓ ᵀ³ | ✓ | ✓ | ✓ |
| Legible Emergence | ✓ ᵀ³ | ✓ | ✓ ᵀ³ | ✓ | ✓ |
| Security (TLS/RBAC/SSO/audit) | ✓ | ✓ | ✓ | ✓ | ✓ |
| Artifacts / library | ✓ | ✓ | ✓ ᵀ² | ✓ | ✓ |
| Federation / AgentFacts | ✓ | ✓ | ✓ ᵀ¹ | ✓ | ✓ |
| Reasoning / LLM / MCP / guardrails | ✓ | ✓ | ✓ ᵀ² | ✓ | ✓ |
| Companions | ✓ | ✓ | ✓ | ✓ | ✓ ᵀ² |

ᵀ¹ closed in Tier 1 · ᵀ² Tier 2 · ᵀ³ Tier 3.

## What was found, and how it was closed

### Tier 1 — real holes (✗ / weak-pair cells)
- **Locks · HOW·Ops** was ✗ — a shipped feature with no operational recovery story. Fix: a
  `diagnostics.md` "Stuck / contended lock" runbook (`GET /consensus/lock/{name}` inspection,
  lease-expiry self-heal) + the new metric family.
- **Federation · HOW·Dev** was ✗ — the only external-interop standard with no guide chapter. Fix:
  new `guide/17-federation.md` (serve/verify the edge doc + the multi-author domain board).
- **Consensus · HOW·Ops** — diagnostics covered only the *conflict* case, not the common *no-quorum
  stall*, and consensus had no Prometheus surface. Fix: a "Consensus stalled — quorum unavailable"
  runbook + the `mycelium_consensus_*` metric family.
- **Schema · HOW·Ops** — `schema_mismatch` was a `/stats` scalar with no runbook. Fix: a "Schema
  mismatch" runbook + a mirror gauge.

New in Tier 1: the `mycelium_consensus_*` metric family — `mycelium_consensus_timeouts_total{reason}`
(event-emitted; `no_voters`/`quorum_short`/`all_opaque`/`empty_groups`) plus
`mycelium_consensus_commit_conflicts` / `mycelium_schema_mismatch` gauges mirroring the `/stats`
scalars. Deliberately **no per-lock gauge** (cardinality) — locks are consensus slots, inspected via
`GET /consensus/lock/{name}`.

### Tier 2 — Dev-guide HOW trapped elsewhere
- Consensus **leased commits** + the **converged-holder discipline** → added to `04-consensus.md`
  (were `src/lib.rs`/wiki only).
- **MCP external bridge** (`connect_mcp_server`) → new section in `06-tool-discovery.md` so the
  chapter matches its title; bridged tools land in the same `tools/` namespace.
- **Artifacts** Dev routing → `cookbook.md` recipe now points at the Solution/Dev + DevOps anchors.
- **Companions ops runbook** → new `operations/companions.md` (durability/WAL, capability-ring
  failover, the wiki's node-independent store, teardown; **none emit Prometheus metrics**).
- **RPC** discoverability → cookbook recipe links the service-layer reference.

### Tier 3 — WHY / discoverability polish
- **Legible Emergence** got a WHY landing in `philosophy.html` (under Emergent Levels / Anderson).
- **`explain` gateway-only** made intentional in `diagnostics.md` + `src/lib.rs` (it is a cross-node
  `sys.explain` RPC fan-out, not a local read — no in-process accessor by design).
- **System→Cluster** ops note in `observability.md` (the gateway still accepts `"system"`).
- Scope-unification WHY sentence (`13-cluster-topology.md`); three-kinds-vs-API cross-link
  (`00-concepts.md`).

## Bugs the audit surfaced (correctness, not coverage)

The digging turned up defects that reading-for-coverage exposed — the recurring lesson that
verifying-against-code finds real problems:

1. **Fencing-token doc drift** — `lock_service.rs:46` + `04-consensus.md:366` called `LockGuard::token`
   a "consensus ballot", contradicting the guide, the module's own test, and the #164 fix (the token
   is the commit HLC; the ballot regresses under gossip lag). Fixed.
2. **`GossipError` enum was fabricated** — `error-handling.md` documented `Network(String)` /
   `Config(String)` (which do not exist) and omitted five real variants incl. `FrameTooLarge`.
   Confirmed `mycelium::GossipError` is the re-exported mycelium-core enum; rewrote to the real 10.
3. **Broken/inaccurate anchors** — `#gossipError`→`#gossiperror`; the diagnostics verb table's
   "all also available programmatically" was false for `explain`.

## Artifacts created

- `docs/guide/17-federation.md` (new chapter)
- `docs/operations/companions.md` (new runbook)
- `mycelium_consensus_*` + `mycelium_schema_mismatch` metric family (`metrics.md` §Consensus/locks)
- Three new `diagnostics.md` pathologies + a `mycelium-consensus` Prometheus rule group

## Re-run guidance

The audit was a one-time systematic sweep; a re-run should be a **diff**. Re-audit a concept only
when its code/docs changed since this date (`git log --since=2026-07-10 -- docs/ src/`). The matrix
above is the baseline: any cell dropping below ✓ is a regression. The method (four auditors, the
Clear/Thin/Missing rubric, the exact prompts) is reproducible from this session's transcript. New
concepts (a new sub-handle, a new companion, a new external standard) each need a fresh row audited
across all five cells.
