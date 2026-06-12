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

## Worked example — the incident war room

Six agents share a fact pool for incident #417: a log-analyst, a
change-auditor, a db-specialist, a network-specialist, and two remediation
executors. Nobody dispatches. Each agent holds its own trigger declarations
("I react to facts matching X") — boundary predicates, local to each agent.

1. Monitoring posts `(alert, service=checkout, p99=4.2s)`. It gossips to
   everyone; no routing decision exists.
2. Two triggers match simultaneously — fine, diagnosis is non-destructive
   reading. The log-analyst posts `(observation, "connection-pool exhaustion
   to db-7")`; the change-auditor posts `(observation, "deploy at 14:02 cut
   pool size 100→20")`.
3. Those facts trip *other* triggers: the db-specialist correlates both and
   posts `(hypothesis, root-cause="pool misconfig", proposed-action="rollback
   deploy 14:02", confidence=0.8)`.
4. **The missing primitive bites here.** Both remediation executors match
   "actionable task, confidence ≥ 0.7". Reading was safe to share; acting is
   not — two agents rolling back the same deploy concurrently is the failure
   case. They must race for an atomic claim: exactly one consumes the fact
   and executes; the loser's claim returns empty; if the winner crashes
   mid-action, the in-flight deadline re-queues the fact and the other
   executor claims it.

The route alert → {log-analyst, change-auditor} → db-specialist → remediation
was not designed by anyone — a network-partition incident would have routed
through entirely different agents. The topology is a property of each item's
content, discovered as it happens. That is why lanes cannot express it: the
consumer's criterion is a predicate over fact content, and a lane per
(fact-type × interest) combination explodes against each agent's private,
changing declarations.

The clean split the example surfaces is the design insight: **reading facts
is unconditional and concurrent (diagnosis); consuming facts is competitive
and exactly-once (action)** — Linda's `rd` vs `in`. The substrate already
does `rd` perfectly; this companion only adds `in`.

## Worked example — the community microgrid

A neighbourhood energy cooperative runs agents sharing one fact pool: a
solar forecaster, a local tariff agent, several household demand-shifters
(heat pumps, laundry, EV charging), and two storage executors (a community
battery agent and an EV-charger agent). Nobody dispatches. Each agent holds
its own trigger declarations ("I react to facts matching X") — boundary
predicates, local to each agent.

1. A rooftop inverter agent posts `(surplus, feeder=4, kwh=3.2,
   window=14:00–15:00)`. It gossips to everyone; no routing decision exists.
2. Several triggers match simultaneously — fine, this is non-destructive
   reading. The forecaster updates its model and posts `(forecast, feeder-4
   surplus likely +2 kWh through 16:00)`; the tariff agent posts
   `(price-signal, feeder-4 local price low until 15:00)`.
3. Those facts trip *other* triggers: a heat-pump demand-shifter sees the
   price signal and quietly moves its cycle earlier; the battery coordinator
   posts `(task, action="store 3 kWh from feeder 4 before 15:00")`.
4. **The missing primitive bites here.** Both storage executors match
   "storable surplus on my feeder". Reading was safe to share; acting is
   not — the surplus exists *once*, and both charging against it means
   drawing more than the feeder has. They must race for an atomic claim:
   exactly one consumes the task and charges; the loser's claim returns
   empty; if the winner drops offline mid-charge, the in-flight deadline
   re-queues the remainder and the other executor claims it.

The route inverter → {forecaster, tariff} → demand-shifters / battery was
not designed by anyone — a cloudy-evening *deficit* would route through
entirely different agents (discharge offers, demand deferral). The topology
is a property of each item's content, discovered as it happens. That is why
lanes cannot express it: the consumer's criterion is a predicate over fact
content, and a lane per (fact-type × interest) combination explodes against
each agent's private, changing declarations.

The clean split the example surfaces is the design insight: **reading facts
is unconditional and concurrent (forecasting, pricing); consuming facts is
competitive and exactly-once (acting on finite energy)** — Linda's `rd` vs
`in`. The substrate already does `rd` perfectly; this companion only adds
`in`.

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
