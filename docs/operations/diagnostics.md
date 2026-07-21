# Diagnosing a coordinator-free fleet (Legible Emergence)

A Mycelium cluster has no control plane, so there is no central place that "knows" the fleet is
unhealthy. Instead **every node computes the diagnosis itself**, from the gossiped KV it already
holds — no collector, no daemon. Kill any node and you lose nothing; ask any surviving node and it
answers. This is the operator surface for that.

Three verbs, three endpoints (all scope `fleet:read`; `localize` + `diagnose` are also in-process —
`explain` is gateway-only by design, see below):

| Verb | "…" | Endpoint | API |
|---|---|---|---|
| **localize** | *what* is off, and *where* | `GET /gateway/fleet` | `agent.fleet_snapshot()` |
| **explain** | the *sequence* that produced it | `GET /gateway/explain?since=` | *(cross-node event ring)* |
| **diagnose** | *why*, and what to do | `GET /gateway/diagnose` | `agent.fleet_diagnosis()` |

> **`explain` is gateway-scoped by design.** `localize` and `diagnose` are pure node-local reads,
> exposed in-process as `agent.fleet_snapshot()` / `agent.fleet_diagnosis()`. `explain` is
> different: it is a **best-effort cross-node fan-out** (a bounded `sys.explain` RPC to peers) that
> stitches the causal event sequence together fleet-wide, so it lives at the gateway
> (`GET /gateway/explain?since=`) rather than as an in-process call — the temporal fan-out is an
> HTTP-scoped operation, not a local read. In-process you have a node's *own* event ring (the local
> slice); the cross-node stitch is the endpoint's job.

Start with **diagnose**. It runs a rule engine over the snapshot and returns a plain-English,
most-severe-first list of findings — the artifact an on-call engineer who did not build the system
can act on:

```json
{ "observer": "10.0.0.7:9000",
  "summary": "1 condition(s) detected (1 warning).",
  "findings": [
    { "pathology": "governed_group_conflict", "severity": "Warning",
      "cause": "Group 'rush-pool': live membership 4 is outside the governor's band [1, 2] (steady, not flapping). Action: reconcile the governor intent with the actual pool size." }
  ],
  "caveat": null }
```

## Read the `caveat` first (RT1/RT2)

A diagnosis is **one node's best-effort estimate, not fleet ground truth**. The `caveat` field is
that node telling you its own view is partial:

- `partial view — heard 2 of 5 peers …` — the observer is only hearing part of the fleet (it may be
  the partitioned one). Pathologies on the unheard nodes are invisible from here. **Cross-check by
  asking another node.** At convergence, independent nodes produce the *same findings* while each
  keeps its own caveat.
- `this observer is itself opaque/shedding …` — the observer is overloaded, so its own inputs may be
  degraded.

A clean diagnosis from a blind node is **not** a healthy fleet. When a caveat is present, treat the
findings as a floor, not a complete picture.

## The pathologies — how to read each, and what to do

Each detector is a node-local scan of gossiped KV (taxonomy tier (b)); the `/metrics` gauge is the
alertable scalar, the snapshot field is the relational detail, and the diagnosis is the action.

### Governed-group conflict / thrash (the #56 pattern)

- **Means:** a group's live membership is outside the governor's `[min, max]` band. Escalates to
  **thrash** (Critical) when membership is *also* flapping — the governor caps the group while
  auto-join keeps re-adding nodes, so the count oscillates with no steady state.
- **Read:** gauge `mycelium_emergent_governed_group_conflicts` (> 0) and
  `mycelium_emergent_membership_flaps` (> 0 ⇒ thrash); snapshot `governed_groups[]` (`min`/`max` vs
  `observed`); `explain` shows the onset event naming the group and band.
- **Do:** align the governor intent with the intended size, or pause auto-join for that group. A
  *steady* conflict (no flap) is usually a stale intent — reconcile it.

### Fleet-opacity storm

- **Means:** a large fraction of nodes are opaque (overloaded / shedding load), so work pools onto
  the nodes that remain. **≥ 34 %** is a storm (Critical); anything above 0 is worth watching.
- **Read:** gauge `mycelium_emergent_opaque_node_pct`; the diagnosis names the **throttle graph**
  edges (`sender→observer @ N fps`) as the likely *reason* the nodes are shedding.
- **Do:** add capacity, or raise the rate limits that are shedding. The named throttle edges tell
  you *which* senders are being rate-limited.

### Capability-coverage gap

- **Means:** a demand (`req/…`) has no fresh provider visible. Consumers of that capability will
  stall. **NB:** "not visible *from here*" — a partitioned provider looks identical to a crashed one
  (this is why the detector is hysteresis-confirmed past a provider's refresh window).
- **Read:** gauge `mycelium_emergent_capability_coverage_gaps`; snapshot `capability_coverage_gaps[]`.
- **Do:** check whether the providers crashed or were never deployed; (re-)advertise the capability.
  If the observer's `caveat` shows a partial view, confirm from a node that *should* hear the
  provider before concluding it is gone.

### Stale bridged advert (provider shows live but is dead)

- **Means:** the *inverse* of a coverage gap — a `cap/…` advert stays fresh though its real provider
  (a gateway-bridge client: a Python/TS worker behind `POST /gateway/capability/advertise`) has
  crashed. The refresh task runs in the **node**, so evaporation tracks the node's liveness, not the
  client's.
- **Read:** the capability resolves but RPCs to it fail / its queue stalls, while the advertising
  node's own `/health` is fine.
- **Do:** restart the advertising node (kills the refresh task and tombstones on shutdown), or
  `DELETE /gateway/capability/{handle_id}` if the handle survived. **Prevent:** bridge clients
  advertise with `lease_secs` and heartbeat (`POST /gateway/capability/{handle_id}/heartbeat`,
  beat at ~`lease_secs`/3) so a crashed client's advert retracts within one lease window.

### Opacity oscillation

- **Means:** node/kind pairs are flipping in and out of the overload state — unstable back-pressure
  ("pheromone hunting"), the offered load sitting right at a rate threshold.
- **Read:** gauge `mycelium_emergent_opacity_oscillations`.
- **Do:** widen the rate hysteresis or smooth the offered load so nodes settle.

### Consensus commit conflict

- **Means:** two proposals committed for the same slot — a sign of split-brain proposing or a
  partition healing.
- **Read:** gauge `mycelium_consensus_commit_conflicts` (`delta > 0`) or the `commit_conflicts`
  tripwire on `/stats`; snapshot `commit_conflict_slots[]` (the hot slots).
- **Do:** check consensus membership and whether the cluster recently rejoined after a split.

### Consensus stalled — quorum unavailable

- **Means:** proposals are timing out without committing — fewer than `⌊n/2⌋+1` voters of the
  consensus group are contributing. Consensus is **CP**: the overlay *blocks* rather than lie — a
  `propose` returns `ConsensusResult::Timeout` and a `consistent_get` serves the last committed
  value; neither fabricates a commit. This is the common production failure (distinct from a
  *commit conflict*, which is the opposite — two commits, not zero).
- **Read:** counter `mycelium_consensus_timeouts_total` by `reason` — `no_voters` ⇒ likely a
  **partition** (no votes heard at all); `quorum_short` ⇒ members heard but **quorum not met**
  (overloaded members, or the quorum set is larger than live membership); `all_opaque` ⇒ every
  member is shedding load (cross-check [fleet-opacity storm](#fleet-opacity-storm)). Dev-side, the
  returned `ConsensusResult::Timeout { ballots_tried, votes_last_ballot, quorum_required }` carries
  the same distinction per call.
- **Do:** for `no_voters`, restore member reachability / heal the partition (membership is
  peer-exchange + CA admission — check both); for `quorum_short`, add capacity or confirm the
  quorum is `⌊n/2⌋+1` of the **consensus group**, not the whole cluster; for `all_opaque`, relieve
  load. A **leased** commit self-heals on the next round once quorum returns — no manual repair.

### Stuck / contended distributed lock

- **Means:** callers block on `lock(name, …)` or repeatedly get `Superseded` for the same lock.
  Either the holder crashed mid-section (the lock clears at **lease expiry**, not before), the
  critical section is overrunning its `ttl`, or there is genuine high contention on one lock.
- **Read:** inspect the lock's authoritative slot — `GET /consensus/lock/{name}` →
  `{committed, ballot, lease_ms, lease_expired}`. A non-null `committed` with a live `lease_ms` ⇒
  **held**; `lease_expired: true` ⇒ the lease lapsed and the slot has **reopened** — the lock is
  effectively free (a stale holder cannot win). Repeated `Superseded` in caller logs is
  contention, **not** a bug — the converged-holder discipline rejecting a loser. Watch
  `mycelium_consensus_timeouts_total{reason="quorum_short"}` if acquisition itself is timing out
  (a consensus-throughput problem, not lock semantics).
- **Do:** a crashed holder's lock **self-clears at lease expiry** — wait up to the `ttl` you set;
  don't force it. If sections routinely overrun, raise `ttl` above the observed hold time and keep
  sections short. If many callers queue on one lock, that is the wrong shape — a lock is
  coarse-grained mutual exclusion, not a work queue; re-check the
  [when-to-use guidance](../guide/04-consensus.md#which-primitive-dont-reach-for-a-lock-when-you-want-something-else).

### Schema mismatch

- **Means:** a consumer saw a capability whose schema version has **no registered migration path**
  to what it expects (`NoMigrationPath`) — real version skew across nodes, not additive drift. The
  consumer **routes around** the mismatched provider (detection-not-prevention); it never silently
  coerces.
- **Read:** gauge `mycelium_schema_mismatch` (`delta > 0`), or the `schema_mismatch` tripwire on
  `/stats`. Cross-reference registered schemas/migrations across nodes (`list_schemas()`).
- **Do:** publish the missing `vN → vN+1`
  [`SchemaMigration`](../guide/12-schema-lifecycle.md#detection-tier-2-and-registered-migrations-tier-3),
  or roll the lagging producer/consumer to a compatible version. Never force-coerce across an
  unregistered gap.

## Prometheus alert recipes

Requires the `metrics` feature. Every series carries a `cluster` label when `cluster_name` is set,
so these generalise across environments. The `_peers_heard`/`_peers_known` gauges let you **qualify**
an alert by the observer's own view health (the RT1/RT2 caveat, in PromQL form).

```yaml
groups:
- name: mycelium-emergent
  rules:
  # Governed-group thrash — a conflict that is ALSO flapping (the #56 pattern). Critical.
  - alert: MyceliumGovernedGroupThrash
    expr: mycelium_emergent_governed_group_conflicts > 0 and mycelium_emergent_membership_flaps > 0
    for: 2m
    labels: { severity: critical }
    annotations:
      summary: "Governor-vs-auto-join thrash on {{ $labels.cluster }}"
      runbook: "docs/operations/diagnostics.md#governed-group-conflict--thrash-the-56-pattern"

  # Steady governed-group conflict (intent vs reality) — warning.
  - alert: MyceliumGovernedGroupConflict
    expr: mycelium_emergent_governed_group_conflicts > 0 and mycelium_emergent_membership_flaps == 0
    for: 5m
    labels: { severity: warning }

  # Fleet-opacity storm — a third or more of the fleet shedding load. Critical.
  - alert: MyceliumOpacityStorm
    expr: mycelium_emergent_opaque_node_pct >= 34
    for: 2m
    labels: { severity: critical }

  # Capability-coverage gap — a demand with no provider, sustained.
  - alert: MyceliumCoverageGap
    expr: mycelium_emergent_capability_coverage_gaps > 0
    for: 5m
    labels: { severity: warning }

  # Unstable back-pressure — opacity oscillating.
  - alert: MyceliumOpacityOscillation
    expr: mycelium_emergent_opacity_oscillations > 0
    for: 5m
    labels: { severity: warning }

  # RT1/RT2: a node whose own view is badly partial — its diagnoses are a floor, not the whole
  # picture. Not a fleet pathology; a signal to cross-check, or that THIS node is the partitioned one.
  - alert: MyceliumObserverPartialView
    expr: mycelium_emergent_peers_heard < mycelium_emergent_peers_known * 0.5
    for: 5m
    labels: { severity: info }
    annotations:
      summary: "{{ $labels.instance }} hears < half its peers — diagnoses from here are partial"

- name: mycelium-consensus
  rules:
  # Consensus rounds timing out — the CP overlay is blocking (no quorum). Partition if reason=no_voters.
  - alert: MyceliumConsensusStalled
    expr: rate(mycelium_consensus_timeouts_total[5m]) > 0
    for: 3m
    labels: { severity: warning }
    annotations:
      summary: "Consensus timing out on {{ $labels.cluster }} (reason={{ $labels.reason }})"
      runbook: "docs/operations/diagnostics.md#consensus-stalled--quorum-unavailable"

  # Split-brain double-commit (healed partition). Gauge mirrors the /stats scalar → alert on delta.
  - alert: MyceliumCommitConflict
    expr: delta(mycelium_consensus_commit_conflicts[10m]) > 0
    for: 1m
    labels: { severity: warning }
    annotations:
      runbook: "docs/operations/diagnostics.md#consensus-commit-conflict"

  # Schema version skew — a provider is being routed around.
  - alert: MyceliumSchemaMismatch
    expr: delta(mycelium_schema_mismatch[10m]) > 0
    for: 2m
    labels: { severity: warning }
    annotations:
      runbook: "docs/operations/diagnostics.md#schema-mismatch"
```

Because the diagnosis is coordinator-free, **scrape every node** and let the `cluster` label group
them: at convergence the findings agree, and a *disagreement* between nodes is itself the signal that
one of them is partitioned.

## Turning it on

The five detectors and the event ring run only under `GossipConfig::emergent_detectors_enabled`
(env `GOSSIP_EMERGENT_DETECTORS=1`), off by default — zero overhead when off. The **snapshot and
diagnosis** (`fleet_snapshot()` / `fleet_diagnosis()`, `/gateway/fleet` / `/gateway/diagnose`) work
whether or not the loop runs; the flap/oscillation counters simply read 0 without it. Enable the loop
in production so `explain` has history and the temporal detectors (flap/oscillation) fire.

## See it: the induce-and-diagnose demo

`cargo run -p mycelium-coop-examples --bin diagnostics` stands up a two-depot mesh, induces a
governed-group conflict on one depot, and has the **other** depot diagnose it from its own gossiped
KV — the coordinator-free property, end to end, Docker-free. Covered in CI by the coop smoke suite.

---

*Design: [`docs/plans/legible-emergence.md`](../plans/legible-emergence.md); code:
`src/agent/emergent.rs`; developer view (adding a detector): the wiki's
[dev/diagnostics](../wiki/dev/diagnostics.md) page and
[guide/14 · patterns and pitfalls](../guide/14-patterns-and-pitfalls.md).*
