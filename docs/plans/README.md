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

> **As of 2026-07-04, every engineering plan is shipped.** v2.0's acceptance gate is met —
> all 16 milestones (M1–M16) ([`v2.0.md`](v2.0.md)) — and both former *proposed* plans have
> since shipped: **Legible Emergence** (complete, phases 0–5, 2026-07-03) and
> **mycelium-wiki** (complete, build phases 1–5 + gateway/SDKs + access broker, 2026-07-04;
> the only remaining slice is the *additive* disconnected KV-native variant). The one open
> non-engineering item is a **research experiment** (three-arm work distribution, for Paper 1).

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
| [`legible-emergence.md`](legible-emergence.md) | Diagnosable coordinator-free fleets (Detect→Localize→Explain→Intervene) — phases 0–5, incl. the operator surface. ✅ complete 2026-07-03 | — |
| [`mycelium-wiki.md`](mycelium-wiki.md) | The `mycelium-wiki` companion — group-scoped LLM-curated wiki (data plane · curator control plane · reconcile + change-driven lint · MCP + HTTP gateway + py/ts SDKs · access broker · worked example). ✅ phases 1–5 shipped 2026-07-03/04; audited Run 32. Remaining: the additive KV-native variant | — |

## Design sketches (rationale)

| Doc | What it is |
|---|---|
| [`mycelium-blackboard.md`](mycelium-blackboard.md) | The blackboard *design rationale* (worked example, `rd`/`in` split, non-goals). ✅ Built — the phased build plan is [`v2-wsg-g3-blackboard.md`](v2-wsg-g3-blackboard.md). |

## Proposed — not yet started

_None._ Both previously-proposed plans (Legible Emergence; mycelium-wiki) have shipped — see the
execution-records table above. The only open engineering slice is the **additive** disconnected
KV-native wiki variant (design record [`../design/wiki-concurrent-edit.md`](../design/wiki-concurrent-edit.md)),
started only if a no-external-store deployment needs it. Research-track work (Paper 1's three-arm
experiment) is tracked in [`docs/wiki/domain/publications.md`](../wiki/domain/publications.md).

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
