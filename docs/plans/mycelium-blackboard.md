# mycelium-blackboard — design sketch (deferred)

**Status:** Sketch, not a build plan. Recorded 2026-06-12 alongside the
boundary-of-validity analysis in Paper 1 §9.4 and `docs/philosophy.html`
("Where associative matching earns its keep"). Promote to a full phased plan
only when the trigger fires.

## What it is

A companion crate rebuilding **blackboard-style shared working memory** —
opportunistic multi-agent reasoning over typed facts — on Mycelium's public
API, the same way `mycelium-tuple-space` rebuilt work distribution. LLM-agent
shared scratchpads are the blackboard reborn (Carvalho's observation: agent
frameworks are rebuilding this ad hoc, badly), which is what makes the
pattern newly relevant.

## Why the tuple space doesn't cover it

The lane-addressed space requires the flow topology to be known per stage.
Blackboard workloads have **no stages**: agents post partial conclusions,
hypotheses, and evidence; any agent whose trigger condition matches
contributes; the topology is emergent per item. This is one of the four
residual workloads at the lane model's boundary (Paper 1 §9.4) — the one
that justifies a companion rather than an extension.

## The decomposition that already exists

Layers I/II already provide most of the blackboard:

| Blackboard concern | Existing mechanism |
|---|---|
| Fact propagation | KV writes under a `bb/{board}/…` prefix, gossip-flooded |
| Trigger predicates | Signal boundaries / capability-style attribute filters — the control-plane associative matchers |
| Fact evaporation | Read-side freshness convention (same as `CapEntry::is_fresh`) |
| Observability | Prefix scans + per-board counters, same posture as `sys/tuple/…` |

## The one missing primitive

**Competitive destructive claim-by-predicate** — Linda's `in(pattern)` with
exactly-once discipline. Two agents whose triggers both match a fact must
not both consume it. Everything else is non-destructive (`rd`-style) and
needs nothing new.

Shape: serve the claim path through a primary with WAL + in-flight deadline,
exactly as the tuple space does (`Claim` as one indivisible record; unacked
claims re-queue). Roles, failover, election, and gateway routing are reused
from the tuple-space pattern verbatim — primary discovered by capability
advertisement, secondary promotes on evaporation.

Predicate language: start with the capability attribute-filter grammar
(equality + presence), not unification. It is already implemented, already
understood by users, and covers trigger conditions; full structural matching
is scope creep until demonstrated otherwise.

## Non-goals

- No matching engine inside `mycelium-tuple-space` — the lane properties
  (O(1) claims, per-lane depth/backpressure, one-record transitions) are
  load-bearing for AFN and must not be un-bought.
- No semantic/embedding matching in the substrate or this crate — similarity
  is a ranking concern for the selection edge.
- Fan-in joins do **not** need this crate: a keyed-exact-match `take` on the
  tuple space (O(1), lane-accounted) covers them; track that as a tuple-space
  extension instead.

## Trigger to revisit

Real demand for opportunistic multi-agent reasoning over shared facts —
i.e. an embedding use case whose flow topology is emergent per item, where
the team would otherwise build an ad-hoc claim/poll/match store on the KV.
Secondary trigger: Paper 1 reviewers or follow-up work wanting the §8
constructive proof extended to associative-claiming workloads (§9.5 names
this as the next constructive test).
