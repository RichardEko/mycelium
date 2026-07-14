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

## Changelog

- **2026-07-14 (run 4)** — diff-gated: **no material diff to the matrix; carried unchanged from run 3.**
  Zero product-core (`src/` · `mycelium-*/src/`) change since run 3. The whole delta is
  examples/tooling/docs: four browser **visual showcases** (`microgrid_viz` · `stigmergy_viz` ·
  `redistribution_viz` · `llm_council_viz`, the `/state`+canvas pattern) plus their discoverability
  across all three surfaces (wiki `dev/examples.md`, `examples/README.md`, the presentation deck), the
  conway bind/URL fixes, and the local scale-nightly runner. This **enriches** the
  Companions (tuple-space/blackboard) and coordination **HOW·Dev** cells — a reader now has runnable
  visual demos — but those cells were already ✓, so **no verdict moves**. No new concept (a visual
  showcase is an example *category*, not a substrate concept warranting a row). Not re-audited; the
  next scored re-audit waits for a concept-cell-moving change (product code, a new sub-handle/companion,
  or a found gap).
- **2026-07-13 (run 3)** — diff-gated re-audit. The only material diff since run 2 is this session's
  two commits: `8456dc4` (wiki-store **section-granular CAS** — the dual-curator lost-update fix) and
  `d316cdf` (the **`coordination-approaches.md`** design note + cross-links). **No concept cell
  regressed.** The wiki-store CAS is an internal correctness fix, documented in `companions/wiki.md`
  and `wiki-concurrent-edit.md §3.5` (agent/WHY-facing — no new persona gap). The design note **closes
  a latent WHY gap** and produced a **calibration hit** (below): runs 1–2 scored **Distributed
  locks · WHY** and **Companions · WHY** as ✓, but the *cross-cutting* decision — *when to reach for
  the distributed lock vs the capability ring, and why all three companions reject it* — had no
  user-facing home. Each primitive's own rationale was covered; the **comparison spanning
  Locks+Companions+Consensus fell between the matrix rows** (a structural blind spot the row-by-row
  scoring cannot see). *Fixed:* `docs/design/coordination-approaches.md` (CP-vs-AP decision matrix +
  the rule + a fourth-companion checklist), cross-linked so **both** personas reach it — Dev via
  `04-consensus.md` / `faq.md`, Ops via `companions.md`, plus `exactly-once-effect.md`,
  `wiki-concurrent-edit.md`, and `docs/README.md`. All rows carry; WHY for Locks/Companions/Consensus
  is now genuinely — not nominally — Clear.
- **2026-07-11 (run 2)** — diff-gated re-audit. Nothing in the existing matrix's *concept* cells
  changed since the seed (the post-seed commits were the wire-compat gate, wiki ingests, the two new
  skills, and persuasion-surface fixes) — those rows **carry**. One **new concept row** was surfaced
  by the wire-compat gate: **Rolling upgrade**. WHY/WHAT/HOW·Dev were covered (`building-on-mycelium`,
  `faq`, `09-security`, `error-handling`); **HOW·Ops was Thin** — only a one-line "supported"
  assurance in `production-readiness`, no procedure. *Fixed:* added `operations/deployment.md §
  Rolling upgrades` (node-by-node procedure + the two-step-gap tripwire) with cross-links from
  `09-security` and `production-readiness`. Also fixed a **staleness the seed missed** —
  `09-security.md` cited wire **v10/v9** as current → **v12/v11** (logged under Calibration).
- **2026-07-10 (run 1, seed)** — the full four-auditor audit + Tier 1–3 remediation (below).

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
| Rolling upgrade (wire compat) | ✓ | ✓ | ✓ | ✓ | ✓ ᴿ² |

ᵀ¹ closed in Tier 1 · ᵀ² Tier 2 · ᵀ³ Tier 3 · ᴿ² closed in run 2 (2026-07-11).

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
- **Run 2:** `operations/deployment.md § Rolling upgrades` (new operator procedure)
- **Run 3:** `docs/design/coordination-approaches.md` (new WHY decision guide — when to use the
  distributed lock vs the capability ring, and why the three companions reject it; cross-linked from
  `04-consensus.md`, `companions.md`, `exactly-once-effect.md`, `wiki-concurrent-edit.md`, the guide
  FAQ, and `docs/README.md`)

## Calibration

Prior `Clear` cells later found Thin/Missing — the ledger that scores this audit's own verdicts (the
doc analogue of `ratings.md`'s calibration ledger). A cell with repeated hits deserves structural
skepticism, not a re-asserted ✓.

- **2026-07-11 — Security · WHAT/HOW·Dev** was `Clear` in run 1 (seed) while `09-security.md` cited
  the wire version as **v10 "(current)"** and framed the rolling-upgrade window as **v10 ↔ v9** —
  both stale (current is v12/v11). Found by run 2's rolling-upgrade diff-audit. Root cause: the seed
  auditor confirmed the *concept* was explained but did not spot-check the *version constant* in the
  prose. Lesson for future runs: a `Clear` verdict on a doc that pins a constant/version must verify
  the value against code, not just its presence.
- **2026-07-13 — Distributed locks · WHY + Companions · WHY (cross-cutting)** were `Clear` in runs 1–2,
  but the decision *"which coordination primitive do I reach for, and why not the lock?"* had **no
  user-facing landing** — each primitive's own rationale existed (`04-consensus.md` for the lock,
  `exactly-once-effect.md` for the companions), yet the *comparison* lived nowhere: `companions.md`
  only **asserted** "no distributed lock" without the why. Found by a user question ("is the
  lock-vs-ring design decision documented anywhere?") + a doc audit that confirmed the absence. Root
  cause is **structural, not an oversight**: the matrix scores each concept's cells independently, so a
  cross-cutting decision guide that spans several concepts (here the CP-vs-AP coordination axis over
  Locks/Companions/Consensus) falls *between* rows and reads as covered when every individual cell is
  ✓. **Sharpening (a method change, not a point patch):** when two or more concepts share a decision
  axis, audit whether the *comparison itself* has a home — add a "cross-cutting decisions" pass that
  asks "if a reader must choose between these N concepts, where do they learn how?", distinct from
  scoring each concept's own WHY. Fixed: `coordination-approaches.md`.

## Re-run guidance

The audit was a one-time systematic sweep; a re-run should be a **diff**. Re-audit a concept only
when its code/docs changed since the last run (run 3: `git log --since=2026-07-13 -- docs/ src/
mycelium-*/src/`). The matrix
above is the baseline: any cell dropping below ✓ is a regression. The method (four auditors, the
Clear/Thin/Missing rubric, the exact prompts) is reproducible from this session's transcript. New
concepts (a new sub-handle, a new companion, a new external standard) each need a fresh row audited
across all five cells.
