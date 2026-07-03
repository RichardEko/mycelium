# `docs/plans/` — index

The front door for this directory. A plan here is one of three kinds: a **shipped
execution record** (what was built and *why* — kept as institutional memory, not a
to-do), a **design sketch** (rationale, possibly superseded by a build plan), or a
**research-track** item. A `✅` status header in the file is authoritative; this
index just routes you to the right one.

**Canonical homes elsewhere:** per-milestone *design* lives in
[`ROADMAP.md`](../../ROADMAP.md); architecture invariants in
[`CLAUDE.md`](../../CLAUDE.md); cross-cutting design decisions in
[`docs/design/`](../design/). These plans are strategy/sequencing + execution
record, not duplicates of those.

> **As of 2026-06-21, all *delivered* engineering plans are shipped.** v2.0's
> acceptance gate is met — every workstream and all 16 milestones (M1–M16) delivered
> ([`v2.0.md`](v2.0.md)). Open items: one **proposed** plan
> ([`legible-emergence.md`](legible-emergence.md) — emergent observability, awaiting
> go-ahead) and one **research experiment** (three-arm work distribution, for Paper 1).

---

## Shipped — execution records (the "what + why")

The reasoning behind decisions — including the **declined-with-reasoning** calls a
future contributor needs so they don't re-litigate a settled choice — lives here.

| Plan | Workstream / scope | PRs |
|---|---|---|
| [`v2.0.md`](v2.0.md) | The v2 master plan + acceptance scorecard (all 16 milestones) | — |
| [`v2-m1-mycelium-core.md`](v2-m1-mycelium-core.md) | WS-A M1 — workspace split → `mycelium-core` | — |
| [`v2-m2-consensus-feature.md`](v2-m2-consensus-feature.md) | WS-A M2 — `consensus` feature gate | — |
| [`v2-m3-core-handles.md`](v2-m3-core-handles.md) | WS-A M3 — core-handle pushdown | #8 |
| [`v2-wsb-scale-transport.md`](v2-wsb-scale-transport.md) | WS-B — M4/M5 SWIM, M11 codec + Merkle, wire v12 | #19/#21/#22 |
| [`v2-wsc-metabolism.md`](v2-wsc-metabolism.md) | WS-C — M8 auto-derivation, M9 hot-reload/tuning governor | #26/#27 |
| [`elastic-sizing-intent-governed.md`](elastic-sizing-intent-governed.md) | WS-C — `IntentReconciler` → `MembershipGovernor` → operator surface | — |
| [`v2-wsc-m7-m10.md`](v2-wsc-m7-m10.md) | WS-C — M7 distributed rate-limiting, M10 live timing reconfig (fence-free) | #105–#107 |
| [`v2-wsd-security.md`](v2-wsd-security.md) | WS-D — M6 capability authz + CT revocation log | #77–#82 |
| [`v2-wsf-schema-evolution.md`](v2-wsf-schema-evolution.md) | WS-F — registered declarative schema migrations | #83–#88 |
| [`v2-wsg-coordination.md`](v2-wsg-coordination.md) | WS-G — M13 keyed `take`, G2 exactly-once contract, G3 blackboard | #89–#100 |
| [`v2-wsg-g3-blackboard.md`](v2-wsg-g3-blackboard.md) | WS-G / G3 — the `mycelium-blackboard` companion crate (6 phases) | #95–#100 |
| [`mycelium-tuple-space.md`](mycelium-tuple-space.md) | The `mycelium-tuple-space` companion crate (5 phases) | — |
| [`v1x-completion.md`](v1x-completion.md) | v1.x Production Readiness Gap → done (RBAC/audit/crown-jewel/OIDC/cert-rotation), `v1.2.0` | #1 |
| [`docs-and-examples-alignment.md`](docs-and-examples-alignment.md) | The two-audience docs + coop example-suite alignment (7 workstreams) | — |
| [`example-suite.md`](example-suite.md) | The 11-demo Food-Rescue Co-op example suite | — |

## Design sketches (rationale)

| Doc | What it is |
|---|---|
| [`mycelium-blackboard.md`](mycelium-blackboard.md) | The blackboard *design rationale* (worked example, `rd`/`in` split, non-goals). ✅ Built — the phased build plan is [`v2-wsg-g3-blackboard.md`](v2-wsg-g3-blackboard.md). |

## Proposed — not yet started

| Doc | Status |
|---|---|
| [`legible-emergence.md`](legible-emergence.md) | 📋 **Proposed** (2026-06-21) — make coordinator-free fleets diagnosable by a *non-designer* (Detect → Localize → Explain → Intervene), closing the debuggability-of-emergence adoption risk. Coordinator-free by construction (fleet diagnostics computed from each node's locally-held KV + HLC causal order + scatter-gather fan-out; no central collector). 6 phases (0 taxonomy → 1 emergent tripwires → 2 fleet snapshot → 3 causal reconstruction → 4 fleet narrative → 5 operator surface). Awaiting go-ahead. |
| [`mycelium-wiki.md`](mycelium-wiki.md) | 🟢 **Approach revised 2026-07-03 — control-plane/data-plane; build not started** — a **group-scoped, LLM-curated wiki** as the fourth coordination primitive: the **maintained-meaning / authoritative-specific canon** that *composes* with an external metrics store (Postgres) and RAG (background), joined by a shared id namespace. **Not in gossiped KV** — the corpus lives in a **node-independent, pluggable store** (shared FS dir / S3 / doc store, which can be *dumb*); a group node runs a **curator service** that serialises writes + runs the LLM ingest/lint + **brokers access**, while group agents **read the store directly, in parallel**. Mycelium is the **control plane** — curator election + ring-failover, the store-location pointer, the small evaporating **proposal queue** in KV, and the MCP tool — never the storage. This is the wiki pattern's native shape (files + LLM curator + direct reads, as `docs/wiki/` itself works), so the concurrent-prose-merge problem dissolves into single-writer-curator + the store. The earlier **KV-native section-CRDT** ([design record](../design/wiki-concurrent-edit.md) §1–§2) is retained as the **disconnected / no-external-store variant**; the identity model ("competence is a capability, knowledge is not") + curator state machine carry over. Two driving use cases (Novus-i2 org twin; Transparency-Platform council decisions) reviewed 2026-07-03. |

## Research track — in progress

| Doc | Status |
|---|---|
| [`three_arm_workdist.md`](three_arm_workdist.md) | ⏳ **In progress** — the three-arm work-distribution experiment Paper 1 §9.5 calls for. The harness code is built (`examples/three_arm_workdist.rs` + `three_arm_runner.sh` + `three_arm_plot.py`); the *run + analysis + write-up* is the pending research deliverable. The one genuinely-open plan in this directory. |

---

*Completed plans are kept, not deleted: a `COMPLETE` header graduates a plan from a
to-do into documentation — it earns its place by recording the reasoning. They are
not moved to an archive subdir because ~13 cross-links from ROADMAP/CLAUDE/the guide
point into this directory; the status headers + this index provide the legibility
without the link churn.*
