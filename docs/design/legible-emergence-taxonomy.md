# Legible Emergence — Phase 0: pathology taxonomy + detection-source classification

**Status:** 📐 **design record (Phase 0 of [`legible-emergence`](../plans/legible-emergence.md))**,
2026-07-02. No code. This is the "discriminator that makes everything after cheap or expensive": it
classifies every emergent pathology by **detection source**, defines each **trip condition**
(threshold + hysteresis), and bakes in the four red-team findings (RT1–RT4) the plan review
surfaced. **Gate (met — §5):** classification complete, each pathology has a trip condition, and the
KV-computable tier is confirmed the majority (Phases 1–2 are cheap). Grounded against
`src/agent/mod.rs::SystemStats`, the fleet KV prefixes, `CapEntry::is_fresh`,
`src/agent/{membership,timing,tuning}_governor.rs`, and `src/agent/scatter.rs`.

## 1. Detection-source tiers

Every pathology is detected from one of three sources, in increasing cost:

- **(a) node-local signal** — this node's own atomics/state, already on `SystemStats`
  (`dropped_frames`, `commit_conflicts`, `sys_namespace_violations`, `cap_authz_violations`,
  `schema_mismatch`, `rate_limited_senders`, `task_count`) or its own role/opacity transitions.
  Zero fan-out, zero extra gossip.
- **(b) locally-held KV fleet-view** — computable from the gossiped soft-state **every node already
  holds** (KV floods the cluster — [runtime-invariants](../wiki/dev/architecture/runtime-invariants.md)):
  `cap/`, `req/`, `sys/load/`, `sys/govern/{fleet,timing,membership}`, `sys/rate/`, `sys/quorum/`,
  `grp/`, `gcap/`, `consensus/`. A local scan; no fan-out. **This tier must be the majority** — it
  is what makes Phases 1–2 cheap and collector-free.
- **(c) cross-node temporal assembly** — genuinely needs other nodes' *event history over time*
  (sequences), which is **not** in KV (KV holds only the current LWW value). Supplied on demand by
  the Phase-3 scatter-gather ring fan-out — a pull, never a central sink; the expensive tier, used
  only for "Explain."

## 2. The view-confidence header (RT1 + RT2) — attached to *every* diagnostic

The load-bearing reframe from the red-team: **a diagnostic is a per-node best-effort *estimate*, not
fleet ground truth.** Eventual consistency means two nodes legitimately compute different diagnoses
during an incident (divergent local LWW views; there is no consistent read of the soft-state —
`consistent_get` is the consensus overlay, not this path). And the estimate degrades exactly when
needed (a partitioned node computes a confident, wrong snapshot). Both are dissolved — not
papered — by making every detector output and every `/gateway/fleet` snapshot carry:

```rust
struct ViewConfidence {
    observer:          NodeId, // whose local view this is
    peers_heard:       usize,  // peers this node has FRESH (is_fresh) soft-state for, this window
    peers_known:       usize,  // peers in this node's roster
    max_hlc_skew_ms:   u64,    // newest local HLC − oldest fresh peer stamp (staleness proxy)
    last_anti_entropy_ms: u64, // age of this node's last completed AE round
    self_degraded:     bool,   // is the observer itself opaque / shedding / behind?
}
```

Rule: **no diagnostic is emitted without it.** `peers_heard ≪ peers_known` or a large
`max_hlc_skew_ms` is the node *telling you its own view is partial* — which is what a partitioned or
storm-degraded node needs to say. This is more coordinator-free-honest than a false single verdict,
and it is a Phase-1 schema requirement, not a Phase-4 nicety.

## 3. The pathology catalogue

| # | Pathology | Tier | Source (grounded) | "Pathological" = trip condition (threshold + hysteresis) | Evaporation / partition tolerance (RT3) |
|---|---|---|---|---|---|
| P1 | **Governed-group conflict** (#56) | **b** | `sys/govern/membership/{group}` (`MembershipIntent{min,max,drain}`) vs live `grp/` member count | live count outside `[min,max]` **sustained > T_conflict** (default ≥ 2× reconcile interval) — not a transient during convergence | intent evaporates (MEMBERSHIP window) ⇒ no intent = no conflict; require the delta to persist past one governor tick |
| P2 | **Failover flapping** | **a** (own) / **b** (observed) | own role-change count (atomic) **or** churn of `{tuple,wiki,blackboard}.{ns}.primary\|curator` ads in `cap/` | ≥ N promotions in window W where the inter-promotion gap `< 3× cap_refresh` (a genuine failover can't be faster than ad evaporation — anything faster is flap) | the `3× cap_refresh` floor **is** the discriminator; slower = healthy failover |
| P3 | **Opacity / pheromone oscillation** | **b** | a watched `sys/load/{node}/…` `is_opaque` toggling | value crosses ≥ R times per window W (hysteresis: count crossings, ignore a single flip) | oscillation must be *faster than* the refresh interval to register — a slow legitimate shed/unshed is not it |
| P4 | **Fleet-wide opacity storm** | **b** | fraction of live nodes with any `sys/load/{node}/… is_opaque=true` | `opaque_fraction > B` (default 0.5) **sustained > T_storm** | **RT2 flagship**: the storm degrades the gossip used to count liveness — the `ViewConfidence` header is *mandatory* here; a low `peers_heard` means "I may be undercounting live nodes," so report the estimate *with* its confidence, never as fact |
| P5 | **Anti-entropy convergence stall** | **b/c** | max `max_hlc_skew_ms` across self-reporting nodes; optionally a new `sys/health/{self}` (store-size + last-AE stamp) soft-state key | divergence (skew or store-size delta) **not reducing** over K consecutive AE windows | inherently partition-sensitive; a stalled *peer* and an *unreachable* peer look alike from local KV — assert "stall" only with `peers_heard` context, escalate to Phase-3 fan-out to disambiguate |
| P6 | **Capability-coverage gap** | **b** | a live `req/` requirement (or `CapFilter`) with **zero fresh** `cap/` providers | zero `is_fresh` providers for a required capability **sustained ≥ 3× refresh** | **RT3 flagship**: "retracted" and "merely unheard/GC-paused/partitioned" are *identical in local KV* (both = no fresh key). MUST wait past the full evaporation window before asserting a gap, and label it "no provider *visible from here*," never "no provider exists" |
| P7 | **Consensus livelock** | **a/c** | a `consensus/` ballot open > T without commit; repeated ballot increments without commit; `commit_conflicts` rising | ballot age > T_ballot **and** ≥ M re-ballots without a commit | needs temporal ("did votes arrive?") → tier-c to confirm; locally you see *your* ballot stuck, not why; the Phase-3 ring shows the missing votes |

## 4. Diagnostic traffic must be excluded from detector inputs (observer effect)

The `explain` fan-out is RPC traffic; the event-ring writes are events; a `/gateway/fleet` scan
touches KV. By construction these are **excluded from the detectors' inputs**: the ring does not
record its own fan-out RPCs; the flap/oscillation detectors ignore diagnostic-origin signals; the
rate detectors already exclude `sys/rate/` self-observation. Named here so Phase 1/3 wire it in from
the start (a detector that trips on the act of diagnosing is the worst false positive).

## 5. Gate check — the KV-view tier is the majority

Tier tally across P1–P7: **tier-a** primary = 1 (P2-own; P7 partial), **tier-b** primary = **5**
(P1, P3, P4, P5, P6; P2-observed), **tier-c** required-to-confirm = 2 (P5, P7) + all of "Explain".
**The (b) tier is the clear majority (5 of 7).** This validates the plan's core economic claim:
Phases 1–2 (node-local + KV-view detectors, no fan-out) cover most pathologies cheaply; only the
temporal "Explain" and the confirmation of stall/livelock need the Phase-3 pull. **Gate met.**

## 6. Always-on vs on-demand ring (RT4) — decided

**Decision: the event ring is *always-on when the feature is enabled*, not enabled-on-demand.** RT4:
post-hoc diagnosis (the common need) requires history *before* the operator reacts; a ring switched
on mid-incident can only explain *future* incidents. The "zero overhead when **off**" invariant is
satisfied by the **feature gate** (`GOSSIP_EMERGENT_DETECTORS` off ⇒ no ring, no detector loop, no
allocation); when **on**, the ring records continuously.

Cost that makes always-on acceptable: a fixed-count ring (default **1024 significant events**) at
~**128 B/event** (kind + HLC + a small payload) = **~128 KB/node**, bounded by count *and* bytes,
overwriting oldest. Significant events only (role changes, opacity transitions, governor actions,
commit conflicts, throttle decisions, membership changes — **not** a per-message firehose), so the
write rate is events-per-second, not frames-per-second. This is cheap enough to leave on for any
cluster that opts into diagnostics. (Phase 3 measures the real number against the scale tests and
adjusts the default; the *decision* — always-on-within-the-feature — is fixed here.)

## 7. What each downstream phase inherits from this record

- **Phase 1** (node-local + KV detectors): implement P1–P4, P6 as tier-a/b detectors on `/stats` +
  `/metrics`; **every emission carries `ViewConfidence` (§2)**; each ships with its §3 trip condition
  and a "healthy churn does not trip it" gate; diagnostic-origin signals excluded (§4).
- **Phase 2** (`/gateway/fleet` snapshot): the relational view is a tier-b local scan + the
  `ViewConfidence` header; its acceptance gate is restated per RT1 — three nodes agree **at
  convergence**, and during divergence each snapshot is self-labelled with its staleness (not "three
  nodes agree" unconditionally).
- **Phase 3** (event ring + `explain`): the ring is always-on-when-enabled (§6), fixed-memory; the
  fan-out renders **what it has and names non-responders** (RT3), disambiguates P5/P7; confirms the
  tier-c pathologies.
- **Phase 4** (narrative): consumes the snapshot + rings; the open rule-engine-vs-LLM question stays
  open, but a per-node LLM narrative inherits RT1 (non-deterministic across nodes) — so the
  *deterministic* rule-engine core is the default and any LLM "fleet doctor" is an optional,
  confidence-labelled layer.

## 8. Open items explicitly deferred (not Phase 0)

- The exact `T_conflict / W / R / B / K / T_ballot / M` default values — set in Phase 1 against
  reproduction tests; Phase 0 fixes only that each exists and is config-tunable (auto-derived from
  cluster size where possible, à la M8), and that the narrative names *which* threshold tripped.
- Whether P5 needs the new `sys/health/{self}` key or is inferable from existing state — a Phase-1
  spike decides; this record only classifies P5 as b/c and partition-sensitive.
- The `diagnostics` feature-gate name/shape (`GOSSIP_EMERGENT_DETECTORS` vs folding into `gateway`)
  — Phase 1 mechanics.
