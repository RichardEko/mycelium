# Legible Emergence — making coordinator-free fleets diagnosable

**Status:** 🟢 **Phases 0–2 done; Phases 3–5 not started** (proposed 2026-06-21;
red-teamed + Phase-0 taxonomy 2026-07-02; all 5 Phase-1 detectors (P1/P2/P3/P4/P6) + `/metrics` shipped 2026-07-02). Phase 0 shipped as
[`docs/design/legible-emergence-taxonomy.md`](../design/legible-emergence-taxonomy.md) (the
pathology taxonomy, with RT1–RT4 baked in). The **Red-team findings** section (below, near the
end) surfaced four load-bearing issues — chiefly that a diagnostic is a *per-node best-effort
estimate, not fleet ground truth* (the `ViewConfidence` header realises this); read it before
extending. This file is strategy + sequencing; the per-mechanism decisions live in the Phase-0
design record.

## Why

The sharpest adoption risk for a coordinator-free substrate is not performance or correctness —
it is **diagnosability**. When a coordinator-free fleet misbehaves, the cause is *distributed*
(symptom on node A, cause an interaction among B/C/D), *emergent* (no single component is buggy —
the pathology lives in the interaction of correct parts), and *temporal* (a feedback loop, an
oscillation, a convergence that won't close). An SRE's real fear of "no coordinator" is not the
absence of a control plane — it is **"I cannot reason about what it will do."**

Today Mycelium has **excellent node-local legibility** (opacity reasons materialised as
`sys/load/{self}/…` keys, the `/stats` tripwire counters, HLC stamps on every write, `RUST_LOG`)
and **essentially zero emergent-level legibility**. There is no detector for flapping, oscillation,
convergence-stall, or governed-group conflict; `/stats` is node-local, present-tense, and scalar.
The proof is our own history: the governor-vs-emergent-autojoin race (#56→#57) was found by *the
designer building a scenario that triggered it* (`elastic_intent`), not by any tool and not by an
operator. An on-call engineer would have seen node count flapping **with no signal that said why.**

**This plan closes that gap — and in doing so converts the scariest part of the philosophy into a
differentiator.** "Coordinator-free *and* here is a fleet-level causal explainer for why it did
that" is the answer to the deepest objection standing between the architecture and adoption. Nobody
can build this as well as we can, because it requires exactly the HLC + reason-key substrate we
already have.

## The central insight — the diagnostic layer is itself coordinator-free

The naïve way to observe a distributed system is a **central collector** — which for *this* project
would be a coordinator, violating the entire thesis. We do not need one:

1. **Every node already holds the complete gossiped fleet soft-state** — capabilities (`cap/`),
   load/opacity (`sys/load/`), membership + governor intents (`grp/`, `sys/govern/`), throttle
   relationships (`sys/rate/`), quorum attestations (`sys/quorum/`), commit state (`consensus/`).
   So **any node can compute a fleet-level *snapshot* diagnostic locally**, from data it already
   has. No collector. The diagnostic is an instance of the same coordinator-free philosophy it
   observes — kill any node and any other still answers.
2. **HLC gives a total causal order without a central clock** — so a cross-node *temporal*
   reconstruction is possible from per-node event streams, assembled by causal stamp, not by a
   central log.
3. **The one thing not in local KV is other nodes' temporal history** (their transitions over
   time). That is supplied by a bounded per-node event ring collected **on demand via
   scatter-gather** — a *query fan-out* (pull), never a central sink.

This is the load-bearing design decision and the thing Phase 0 must validate before any code.

## Invariants (non-negotiable — a Phase fails review if it breaks one)

- **No coordinator, no collector.** Every diagnostic is computed from locally-held KV and/or an
  on-demand pull fan-out. No node is privileged; no central aggregator is introduced. Verify by:
  the diagnostic works from *any* node and survives killing *any* node.
- **Detection, not prevention.** Detectors *name* pathologies; they never auto-correct. (A flap
  detector does not stop the flap; it surfaces "GOVERNED_GROUP_CONFLICT".) This mirrors the
  existing commit-conflict / `sys/`-namespace tripwire posture — do not let a detector mutate
  another layer's state.
- **Zero overhead when off.** Off by default; opt-in like M7 distributed rate-limiting. No detector
  loop, no event ring, no allocation when disabled.
- **False-positive discipline is the product.** A diagnostic that cries wolf is *worse* than none —
  it trains operators to ignore it. Every detector ships with hysteresis / thresholds and a
  "healthy churn does not trip it" regression test. This is the hardest part and it is a
  first-class gate, not an afterthought.
- **Library, not platform.** We ship the *detectors and the computed diagnostics* (data + a
  reference view); heavy visualisation/alerting is the operator's stack (Grafana, Prometheus
  alerts) — consistent with the multi-cluster framing in `operations/observability.md`.

## Phases

### Phase 0 — Taxonomy + detection-source classification (design record, no code) — ✅ DONE

**Delivered:** [`docs/design/legible-emergence-taxonomy.md`](../design/legible-emergence-taxonomy.md).
Classifies P1–P7 by detection tier (a/b/c), gives each a grounded trip condition + evaporation/
partition tolerance, bakes in RT1–RT4 (the `ViewConfidence` header, the always-on-ring decision),
and clears the gate — the KV-view (b) tier is the majority (5 of 7). Downstream phases inherit its
§7. The original scope, for reference:

The discriminator that makes everything after it cheap or expensive. Produce a `docs/design/`
decision record that, for every emergent pathology, classifies its **detection source**:
- **(a) node-local signal** (this node's own atomics/state — flap rate, opacity transitions),
- **(b) locally-held KV fleet-view** (computable from the gossiped state every node already holds —
  membership-vs-intent, capability coverage, fleet-wide opacity, throttle graph),
- **(c) cross-node temporal assembly** (genuinely needs other nodes' event history — sequences).

For each pathology, define **what "pathological" means** (the threshold + hysteresis that separates
it from healthy churn) — this is where false positives are designed out. Catalogue the initial set:
governed-group conflict, failover flapping, opacity/pheromone oscillation & fleet-wide opacity
storm, anti-entropy convergence stall, capability-coverage gap (a capability advertised by zero
live providers), consensus livelock (votes not arriving). **Gate:** the classification is complete,
each pathology has a defined trip condition, and the (b)-tier (KV-computable) set is confirmed to be
the majority — validating that Phases 1–2 are cheap.

### Phase 1 — Emergent-condition tripwires (node-local + KV-view detectors) — ✅ DONE

**Increments 1–5 shipped — all five KV-view detectors + `/metrics` done** in `src/agent/emergent.rs` — config-gated `GOSSIP_EMERGENT_DETECTORS`
(off by default, zero overhead), the `run_emergent_detectors` loop, the `ViewConfidence` header
(RT1/RT2), all surfaced on `/stats`:
- **P1 governed-group conflict** (#56): `detect_governed_group_conflicts` (governor intent vs live
  `grp/` count, RT3-tolerant) + hysteresis; gauge `governed_group_conflicts`.
- **P4 fleet-opacity storm** (RT2 flagship): `opaque_node_pct` (fresh-opaque nodes ÷ live) — a
  *stateless* gauge computed on-demand in `/stats`; raw gauge the operator thresholds.
- **P6 capability-coverage gap** (RT3 flagship): `detect_coverage_gaps` (fresh `req/` with zero
  fresh `cap/` providers, resolved via `resolve_filter_against_kv`) + hysteresis (needed to tell a
  retracted provider from a merely-lapsed one); gauge `capability_coverage_gaps`. Names "no provider
  *visible from here*," never "exists."

Design notes that emerged: **stateful detectors (hysteresis → P1, P6) live in the loop; stateless
gauges (P4) compute on-demand in `/stats`.** The hysteresis is a shared generic `confirm_by_key`.
11 unit tests. **`/metrics` surface shipped** — the loop emits `mycelium_emergent_*` gauges
(the three detectors + the RT1/RT2 `peers_heard`/`peers_known`/`max_staleness_ms` view-health gauges)
when `metrics` is on. **P2 failover flap** shipped too — `membership_snapshot` + `flap_transitions` (pure) + a
sliding-window `FlapTracker` (per (group,node) transition timestamps; ≥4 toggles in 60 s = a flap,
so a single failover doesn't trip it); gauge `membership_flaps`. This is the detector for the plan's
motivating image ("node count flapping with no signal why"). **P3 opacity oscillation** shipped — reuses P2's `FlapTracker` (opacity is the same
presence-set-churn shape): `opacity_pairs` + `set_transitions` feed a second tracker; gauge
`opacity_oscillations`. **Phase 1 is complete:** all five detectors (P1/P2/P3/P4/P6), the `/stats` + `/metrics`
surface, and a **live end-to-end #56 reproduction** (`test_p1_governed_group_conflict_detector_
fires_and_clears_end_to_end` — a started agent with detectors on, the governor-intent publish
path, the spawned loop, and the gauge: the cap-vs-observed conflict fires, then clears when
membership returns in-bounds). Next: Phase 2 (`/gateway/fleet` relational snapshot).

The cheap, high-value layer. New detectors that read node-local state + the locally-held KV,
surfaced on `/stats` and `/metrics`, mirroring the existing tripwire pattern but at the
cluster/temporal stratum:
- **flap counter** — role-change / promotion rate per window (failover churn);
- **convergence-lag gauge** — max HLC skew across observed peers, or "anti-entropy round did not
  reduce divergence in T" (each node already self-reports enough soft-state; may add a small
  `sys/health/{self}` store-size + last-AE soft-state key);
- **oscillation flag** — a watched key whose value bounces above a rate;
- **governed-group-conflict flag** — governor intent (`sys/govern/`) vs observed membership
  (`grp/`) delta — *the #56 detector that would have put it on `/stats`*;
- **opacity-storm flag** — fraction of live nodes currently opaque exceeds a bound (fleet-wide
  shed / pheromone runaway).
Off-cost-free, opt-in loop (`GOSSIP_EMERGENT_DETECTORS`). **Gate:** reproduce each pathology
(starting with the #56 governor-vs-autojoin condition) and assert the flag fires; assert healthy
churn does **not** trip it (the false-positive gate).

### Phase 2 — Fleet snapshot endpoint (the relational "Localize" view, coordinator-free) — ✅ DONE

**Increment 1 shipped:** `GET /gateway/fleet` (scope `fleet:read`) — `compute_fleet_snapshot`
(`src/agent/emergent.rs`) assembles governed-group status + coverage gaps + opacity + counters from
local KV, with the RT1/RT2 `view_confidence` header. **Acceptance gate met** (RT1-restated):
`test_fleet_snapshot_agrees_across_three_nodes_at_convergence` — three nodes agree on the *diagnosis*
at convergence; `view_confidence` is each observer's own. **Increment 2 shipped** — the snapshot now also carries the **throttle graph** (M7
`sys/rate/` observer→sender edges, `throttle_graph`), a **convergence-health self-report**
(`store_entries` + `store_hash` — two nodes at convergence share the hash; an operator diffs
across nodes), and the cumulative `commit_conflicts` count. **Increment 3 shipped — both previously-deferred fields done:** (1) cross-node
store-convergence via a gossiped `sys/health/{node}` self-report (a store-entry **count**, not a
hash — a hash churns every tick as soft-state refreshes, the RT2 observer effect; the *spread* of
counts is the honest divergence signal), resolving taxonomy §8's open item; (2) per-slot
commit-conflict **hot slots** — the consensus tripwire now records each conflicting slot in a
lock-free papaya map, surfaced as `commit_conflict_slots`. The relational view is complete.


A `GET /gateway/fleet` (scope-gated) that, **computed locally from the gossiped KV any node already
holds**, returns the *relational* picture an operator needs to localise: per-node opacity + reason,
capability coverage (and gaps — a `requires` with zero live providers), membership vs each governed
group's intent, the throttle graph (who is throttling whom), store-divergence across self-reporting
nodes (convergence health), commit-conflict hot slots. **Coordinator-free because it is computed
from local KV** — any node answers it, and it survives killing any node. **Gate:** on a seeded
divergent/conflicted fleet, the snapshot from *three different nodes* agrees on the diagnosis; the
endpoint correctly names a synthetic capability-coverage gap and a governed-group conflict.

### Phase 3 — Causal event ring + fan-out reconstruction (the hard "Explain")

Each node keeps a **bounded, fixed-memory, HLC-stamped ring buffer** of *significant* events (role
changes, opacity transitions, governor actions, commit conflicts, throttle decisions, membership
changes). An operator query `GET /gateway/explain?since=…` **fans out via the existing
scatter-gather primitive** to collect ring buffers, assembles them in HLC causal order *locally*,
and renders the cross-node sequence that produced a state. Coordinator-free: a pull fan-out, not a
central log; bounded memory; opt-in. **Gate:** reconstruct the #56 sequence end-to-end
("governor capped at 8 @ hlc_X → auto-join re-added node @ hlc_Y → governor drained @ hlc_Z → …")
from the assembled rings, with no designer knowledge required to read it.

### Phase 4 — Fleet narrative (the "why is the fleet in this state" — the differentiator payoff)

Extend the per-node opacity self-explanation to a **fleet-level narrative** over the Phase 2
snapshot + Phase 3 causal data: *"Work is pooling on node-7 because nodes 3,4,5 are opaque
(reason: rate-limit aggregate) and the membership governor capped the group at 8 while auto-join is
contending — see the conflict @ hlc_Y."* A templated rule engine over the detector/snapshot/ring
outputs, producing a human-readable diagnosis. This is the artifact that directly answers the SRE's
fear. **Gate:** for each Phase-0 pathology, the narrative names the cause in terms an on-call
engineer who did **not** build the system can act on (the acceptance test below).

### Phase 5 — Operator surface, runbook, alerts, docs

Surface the diagnostics as **data** (the library-not-platform line): extend the existing mesh/`/mgmt`
dashboard with a reference diagnostics view; ship `docs/operations/diagnostics.md` ("the fleet is
doing X — here is how to read it", one entry per pathology, each linking the detector + snapshot +
explain query); Prometheus alert recipes for the emergent tripwires; integrate into
`guide/14-patterns-and-pitfalls.md` (each pathology as a pattern) and the `coop` suite (a demo that
*induces* an emergent pathology and shows the tooling diagnosing it — the constructive proof, the
way `provisioning` is for the autonomic loop). **Gate:** the two-audience docs land; the coop demo
induces-and-diagnoses a pathology Docker-free in CI.

## Definition of done (the acceptance gate)

**The headline test — a non-designer can diagnose an emergent failure using only the tooling.**
Take an engineer who did not build Mycelium. Induce the #56 governor-vs-autojoin condition (and each
other Phase-0 pathology). Given only `/stats`, `/gateway/fleet`, `/gateway/explain`, and the
runbook, they must **Detect → Localize → Explain → know how to Intervene** — without reading the
source. That is the bar; node-local legibility (already strong) does not clear it today.

Plus, per phase: every pathology has a reproduction test that asserts the detector fires **and** a
healthy-churn test that asserts it does not (false-positive gate); the coordinator-free invariant is
verified (works from any node, survives killing any node); zero overhead when disabled.

## Risks & mitigations

- **False positives destroy trust (the #1 risk).** A flaky detector is worse than none. Mitigation:
  hysteresis + thresholds designed in Phase 0; a "healthy churn" regression test per detector is a
  hard gate; ship detectors **conservative** (under-alert) and tighten with evidence.
- **Threshold generalisation.** "Pathological flap" vs "healthy churn" may not have a universal
  constant. Mitigation: thresholds are config (auto-derived from cluster size where possible, à la
  M8); the narrative explains *which* threshold tripped so an operator can tune it.
- **Ring-buffer / fan-out cost at scale.** Bounded fixed-memory ring (cap by count + bytes);
  `explain` fan-out is on-demand and operator-initiated, not continuous; respects the partial-mesh
  forwarding rules. Validate cost at the entry-volume + node-count scale tests.
- **Scope creep into an observability platform.** This contradicts library-not-platform. Mitigation:
  the line is firm — we ship detectors + computed diagnostics + a *reference* view; Grafana/alerting
  is the operator's. Phase 5 surfaces data, not a mandatory UI.
- **The detector layer observing itself.** Detectors must not themselves trip detectors (the
  `explain` fan-out is RPC traffic; the event ring writes are events). Mitigation: diagnostic
  traffic is excluded from the detectors' inputs by construction (Phase 0 names the exclusions).

## Non-goals

- Not a central monitoring stack, not a fleet console, not cross-*cluster* aggregation (that stays
  the operator's Prometheus — see `operations/observability.md`).
- Not auto-remediation. Naming a pathology is the job; *acting* on it remains
  management-as-intent (a human or agent posts a governor/timing intent). Detection, not prevention.
- Not full distributed tracing of every message — a *bounded, significant-events* ring, not a
  firehose.

## Open questions (to resolve in Phase 0)

- Does convergence-lag need a new `sys/health/{self}` soft-state key (store-size + last-AE stamp),
  or is enough already inferable from existing gossiped state?
- Is there a `diagnostics` feature gate, or does this fold into `gateway` + an env flag (like M7)?
- What is the minimum significant-event set for the ring that makes the #56 *and* the consensus-
  livelock reconstructions legible, without becoming a firehose?
- Can the fleet narrative (Phase 4) be a small deterministic rule engine, or does it want the
  capability/skill machinery (an LLM "fleet doctor" skill consuming the snapshot) as an *optional*
  layer on top of the deterministic core?

## Red-team findings (pre-Phase-0, 2026-07-02) — Phase 0 must resolve these

An adversarial review, grounded against the code (`src/agent/scatter.rs`,
`src/capability.rs::CapEntry::is_fresh`, the KV-floods-cluster invariant). The architecture is
sound — the KV-view snapshot **is** computable locally with no collector — but the plan currently
oversells **authority, agreement, and completeness**, treating a node's *local estimate* as if it
were fleet *ground truth*. Four findings the phases do not yet confront:

- **RT1 (Major) — eventual consistency means there is no single fleet truth, so "three nodes
  agree" (Phase 2 gate) does not hold during an incident.** Each node's snapshot reads its *local*
  LWW view, which is divergent at any instant (anti-entropy converges only *eventually*; there is no
  consistent read of `cap/`/`sys/load/` — `consistent_get` is the expensive consensus overlay, not
  this path). During the transient the operator is diagnosing, node A may not yet have received that
  C went opaque, so A and B legitimately compute *different* diagnoses. **Reframe (the fix, and it
  is *more* coordinator-free-honest):** a diagnostic is a **per-node best-effort estimate**, never a
  global verdict. Phase 2's gate becomes "agree *at convergence*; during divergence each snapshot
  labels its own staleness," and every snapshot carries a **view-confidence header** (heard-from
  k/N peers this window, max HLC skew, last-AE age). An estimate that admits its partial view is
  the epistemically honest artifact — and it dissolves the "which node is right?" problem instead of
  smuggling in a quorum/coordinator to resolve it.

- **RT2 (Major) — the diagnostic degrades exactly when it is needed most.** A node computes
  "opacity storm / convergence stall / partition" from the *same gossip* the pathology is degrading.
  A partitioned node holds a stale view and computes a confidently-wrong fleet snapshot **with no
  local signal that it is the partitioned one**. The inputs are least reliable for the
  highest-severity pathologies. **Requirement:** every diagnostic must carry the RT1 view-health
  self-caveat, and Phase 0 must state plainly that a lone node's fleet claim during a partition is an
  estimate from one side; corroboration needs the Phase-3 fan-out (which is itself partial — RT3).

- **RT3 (Medium) — evaporation makes "zero live providers" / "node is gone" inherently ambiguous,
  and the `explain` fan-out is incomplete precisely during incidents.** `is_fresh` is a 3×
  `refresh_interval` window, so a provider whose ad merely lapsed (GC pause, slow refresh) or sits
  across a partition is *indistinguishable in local KV* from one that genuinely retracted — both are
  "no fresh key." So the Phase-2 capability-coverage-gap and opacity detectors are false-positive-
  prone by construction. And `scatter_gather` returns `InsufficientReplies` on timeout: the slow/
  partitioned nodes whose ring you most need are the ones that won't answer, so the Phase-3 causal
  reconstruction is *partial* exactly when it matters. **Requirement:** detectors must tolerate the
  evaporation window (confirmation delay ≥ 3× refresh before asserting "gone"), distinguish
  "retracted" from "unheard" where possible, and `explain` must render *what it has* + **name the
  non-responding nodes** rather than imply completeness.

- **RT4 (Medium) — "zero overhead when off" and "explain what already happened" are in direct
  tension.** If the Phase-3 event ring is off until an operator enables it during an incident, there
  is **no history** to explain the incident that just happened — only future ones. Post-hoc
  diagnosis (the common need) requires **always-on** recording. Phase 0 must decide explicitly:
  quantify the bounded ring's standing cost and make it **always-on-cheap** (a fixed-memory ring of
  significant events likely is — measure it), accepting a small constant overhead, *or* declare the
  tool **future-incidents-only** and say so in the runbook. The plan currently straddles both.

These turn into Phase-0 acceptance criteria: (1) the view-confidence header is part of the snapshot
schema; (2) each detector's trip condition names its evaporation/partition tolerance; (3) the
always-on-vs-on-demand ring decision is made with a measured cost number; (4) the Phase-2 "agreement"
gate is restated as convergence-conditional. What *survives* the review unchanged: the no-collector
architecture, detection-not-prevention, the #56 governor-vs-membership detector as the right cheap
first target, and scatter-gather as the correct temporal-history primitive.

## Relationship to existing work

This is the emergent-stratum sibling of the existing node-local legibility (opacity self-reporting,
the `/stats` tripwires) and reuses, rather than adds to, the substrate: gossiped KV (the fleet view
is already replicated), HLC (causal order is already stamped), scatter-gather (the fan-out primitive
already exists), and the tripwire-counter pattern (the detector shape already exists). It is the
direct response to the **debuggability-of-emergence** adoption risk — the highest-leverage thing the
project can build to neutralise the "emergence is a liability to my buyer" objection.
