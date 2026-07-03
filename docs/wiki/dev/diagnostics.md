# dev/diagnostics — the emergent-detector layer (Legible Emergence Phases 1–3)

↑ [dev/](dev.md) · design: `docs/design/legible-emergence-taxonomy.md` · plan:
`docs/plans/legible-emergence.md` · code: `src/agent/emergent.rs`

Coordinator-free **emergent-stratum** diagnosability — the sibling of the node-local `/stats`
tripwires (`commit_conflicts`, …), at the cluster/temporal level. The motivating gap: the
governor-vs-autojoin race (#56) was found by the *designer building a scenario*, not by any
tool; an on-call engineer would have seen node-count flapping with no signal *why*.

## The posture (four rules every detector obeys)

- **No collector, no fan-out.** Every detector is a **node-local scan of the gossiped KV this
  node already holds** (KV floods the cluster —
  [runtime-invariants](architecture/runtime-invariants.md)). Any node computes it; killing any
  node loses nothing. Tier-(b) of the taxonomy.
- **A diagnostic is a per-node best-effort *estimate*, not fleet ground truth** (the red-team's
  RT1/RT2). Every result is paired with a `ViewConfidence` — `peers_heard ≪ peers_known` is the
  node telling you its view is partial (it may be the partitioned one). Emitted as the
  `mycelium_emergent_peers_heard`/`_peers_known`/`_max_staleness_ms` gauges so an operator alert
  can *qualify* a diagnostic by the observer's own view health.
- **Detection, not prevention.** A detector *names* a pathology (a `/stats` gauge); it never
  corrects it, emits a signal, or mutates another layer.
- **Zero overhead when off.** The whole loop is spawned only under
  `GossipConfig::emergent_detectors_enabled` (`GOSSIP_EMERGENT_DETECTORS`), off by default.

## The five detectors

All in `src/agent/emergent.rs`, surfaced on `/stats` (a scalar) and `/metrics`
(`mycelium_emergent_*`). Pure detection functions are unit-tested without a cluster; the loop is
covered by a live #56 integration test in `src/lib_tests.rs`.

| Pathology | `/stats` gauge | Source | Shape |
|---|---|---|---|
| **P1** governed-group conflict (#56) | `governed_group_conflicts` | governor intent (`sys/govern/membership/`) vs live `grp/` count | loop + hysteresis |
| **P4** fleet-opacity storm (RT2 flagship) | `opaque_node_pct` | distinct fresh-`is_opaque` nodes ÷ live nodes | stateless on-demand gauge |
| **P6** capability-coverage gap (RT3 flagship) | `capability_coverage_gaps` | fresh `req/` with zero fresh `cap/` providers | loop + hysteresis |
| **P2** failover flap | `membership_flaps` | a `(group,node)` toggling `grp/` membership | loop + sliding-window `FlapTracker` |
| **P3** opacity oscillation | `opacity_oscillations` | a `(node,kind)` toggling `sys/load/` opacity | reuses P2's `FlapTracker` |

## Three design patterns worth reusing

- **Stateful vs stateless surfacing.** Detectors needing history (hysteresis P1/P6, sliding
  window P2/P3) run in the loop and write an atomic gauge; a stateless gauge (P4) is computed
  **on-demand in `/stats`** — no loop state, no atomic.
- **One generic hysteresis.** `confirm_by_key` (a key must recur `CONFIRM_TICKS` consecutive
  ticks before it counts — the false-positive guard) is shared by P1 and P6.
- **The two temporal detectors are one detector.** P2 (membership) and P3 (opacity) are the same
  *presence-set-churn* shape over different sets, so P3 reuses P2's `FlapTracker`
  (`membership_snapshot`/`opacity_pairs` → `flap_transitions`/`set_transitions` → the tracker).

## RT3 evaporation discipline (the false-"gone" trap)

On a gossip substrate "retracted" and "merely unheard / GC-paused / partitioned" are
*identical* in local KV (both = no fresh key). So P6 (and P4) only count *fresh* entries and P6
is hysteresis-confirmed past a provider's refresh, and it names "no provider **visible from
here**," never "no provider exists." This is the same read-side freshness convention the whole
substrate uses (`CapEntry::is_fresh`, 3× window).

## Status & what's next

Phase 1 is **complete** (all five detectors + `/stats`/`/metrics` + the live #56 test;
[history](history.md)). **Phase 2 in progress** — `GET /gateway/fleet` (scope `fleet:read`) ships the relational
snapshot: `compute_fleet_snapshot` assembles governed-group status (`governed_group_statuses`),
coverage gaps, opacity, and the flap/oscillation counters from local KV, each with the RT1/RT2
`view_confidence` header. The acceptance gate is met — `test_fleet_snapshot_agrees_across_three_
nodes_at_convergence` proves three nodes compute the same *diagnosis* from converged KV while
`view_confidence` stays each observer's own; the live endpoint + `fleet:read` scope gate are
covered by `test_gateway_fleet_snapshot_endpoint_scope_gated` (401/403/200). The snapshot also
carries the throttle graph (`sys/rate/` edges), **cross-node store-convergence** (the spread of
`sys/health/{node}` entry-count self-reports — a *count* not a hash, since a hash churns every tick
as soft-state refreshes; each node publishes its report from the detector loop), and
**commit-conflict hot slots** (`commit_conflict_slots` — the consensus tripwire records each
conflicting slot in a lock-free papaya map). Phase 2 is complete. **Phase 3 in progress** — the bounded HLC-stamped `EventRing` (RT4 always-on-when-enabled)
records detector-state transitions + commit conflicts. `GET /gateway/explain?since=` (scope
`fleet:read`) now returns the **cross-node** causal narrative: `assemble_explain` starts from this
node's ring, fans a best-effort `sys.explain` RPC out to every known peer (served by
`run_explain_responder`, spawned alongside the detector loop), merges each node's single-author ring
into one HLC-ordered stream, and — **RT3** — names the peers that did **not** answer
(`non_responders`) rather than silently dropping their events. It is deliberately **not**
`scatter_gather`: that aborts at `min_ok` and discards *all* partial replies on
`InsufficientReplies`, which is the RT3 failure mode (the slow/partitioned nodes you most need
mid-incident are exactly the ones that time out). Gate:
`test_explain_fanout_assembles_cross_node_ring_and_names_non_responders` (A+B assemble each other's
rings; C — a live peer with no responder — is the deterministic named non-responder).

**Phase 3 is complete** with the **reconstruction narrative** (increment 3): `assemble_explain` now
also returns `narrative` — the same HLC-ordered events rendered one line per event by `narrate`,
which glosses each terse `kind` into plain English (`governed_group_conflict` → "a group's live
membership left the governor's [min,max] band"), and the conflict event's `detail` names the
specific group + band ("workers: 4 live vs band [1, 2]"). Together this reconstructs the #56 story —
governor cap exceeded → node flaps → returns to band — with **no code knowledge required to read
it** (the Phase-3 acceptance bar). An unknown `kind` falls back to its raw string, so a new detector
is surfaced, never dropped. Gates: `narrate_renders_the_56_sequence_legibly`,
`narrate_surfaces_unknown_kinds_rather_than_dropping_them`, and the cross-node e2e asserts the
narrative names the `workers` conflict from *real* detector output. Not started: Phase 4 (fleet
narrative), Phase 5 (operator surface). The red-team findings (RT1–RT4) and their Phase-2+
implications are in the plan.
