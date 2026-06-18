# Elastic Sizing via Intent-Governed Membership — Delivery Plan

Strategy/sequencing plan (no design duplication). Builds directly on the **WS-C M9 tuning
governor** (PR #27, `src/agent/tuning_governor.rs`) and realizes the project's governing
pattern — **management = intent + local reconcile** (see project memory *management-as-intent*;
ROADMAP/CLAUDE philosophy notes) — first as a *reusable transport primitive*, then applied to
the near-term need: **elastic, coordinator-free sizing of groups and capability providers.**

Spans **WS-C** (self-managing metabolism — the governor/intent layer) and **WS-E** (autonomic
provisioning — the membership/provider engine). The canonical milestone designs stay in
[ROADMAP §v2.0](../../ROADMAP.md); this plan only orders the work.

---

## 1. Why now

Elastic group/provider sizing (min/max via intent) is a **confirmed near-term need** (Richard,
2026-06-18). The stack review found that group behaviours are *already* intent + local-reconcile
at the membership level (emergent self-join, demand-as-observation, opacity) — what they
uniformly **lack is a governance handle**: a way to *bound or bias* the emergent decision. The
tuning governor just proved that handle for tuning; this plan generalizes it to membership.

This is the **Tier-2** coordination tier (desired-state + per-node veto) of the management
gradient — *not* Tier-3 consensus. No coordinator: every node folds the published bounds into
its **own** join/leave/provide decision; local always wins; intents evaporate (management gone ⇒
self-heal — the litmus test).

---

## 2. Three tracks

```
Track 1  IntentReconciler primitive   [extract shared transport from the tuning governor]
Track 2  MembershipGovernor (ENGINE)  [min/max/drain intent + collective self-election]  ← the hard part
Track 3  Operator surface             [/gateway/govern + audit + Prometheus]  ← orthogonal, lands AFTER the engine
```

**Locked design decisions** (converged 2026-06-18):
- **Share transport, keep policy bespoke.** Extract the reconcile/transport plumbing; do **not**
  build a heavy generic `IntentGovernor<T>` trait. Rule of Three pulls a richer abstraction only
  if a 3rd governor appears. The primitive is the *transport*, not the decision.
- **Operator surface is a separate slice, landed after the engine.** The engine works headless via
  the Rust/agent API; the HTTP+metrics door is a clean mechanical follow-up, reviewed on its own.
- Intents evaporate (refreshed soft-state). Local pin beats fleet. Human and agent publishers are
  substrate-identical — intent, never command.

---

## 3. Track 1 — `IntentReconciler` (shared transport primitive)

Extract the behaviour-agnostic ~60–70% currently inline in `tuning_governor.rs`:

- **Publish:** stamp `written_at_ms`, encode (`serde_fixint`), gossip to a `sys/govern/<scope>` key.
- **Reconcile loop:** `subscribe_prefix` + periodic tick (at `TTL/2`); read intent; **freshness /
  evaporation** check; hand the behaviour either `Fresh(T)` or `Absent` via an `apply` / `revert`
  callback.
- Generic over `T: Serialize + Deserialize` (the intent payload). Owns *only* transport +
  lifecycle — **not** apply, gate, or local-pin (those stay in each governor).

**Refactor, not rewrite:** `TuningGovernor` keeps its bespoke policy and registers its
`apply_fleet`/`revert_fleet` with the reconciler. **No behaviour change** — the existing M9 tests
(`tuning_governor::tests`, `test_wsc_m9_governor_fleet_reconcile_and_local_wins`) are the
regression gate.

**DoD G-T1:** existing governor tests stay green through the extraction; the reconciler is unit-
tested for freshness/evaporation in isolation.

---

## 4. Track 2 — `MembershipGovernor` (the engine; the real work)

**Intent shape:** per group (and per capability, the provider analogue) — `min`, `max`, optional
`drain` (exclude named nodes). Published fleet intent + local override, same governor model.

**The hard part — collective self-election.** Unlike tuning (each node clamps its *own* scalar,
zero coupling), membership is a *collective target*: the group/provider count must converge to
`[min, max]` while each node decides whether **it specifically** should join/leave — and they must
**not all act at once** (thundering-herd overshoot). The decision logic, not the intent plumbing,
is where the effort goes:

- **Observe** current count from the existing emergent state (`grp/{group}/...` members /
  `cap`+`gcap` providers — reuse `demand_snapshot`'s counting).
- **Hysteresis bands** so nodes don't flap at the boundary (act below `min` / above `max`, with a
  dead-band).
- **Staggered self-election:** when under `min`, the *most eligible* non-members join first
  (deterministic rank — lowest-id / lowest-load — plus jitter) so the count rises by ~1 per round,
  not N at once; symmetric for over-`max` (least-eligible members leave / drain first).
- Feeds the existing `join_group` / `leave_group` (and the provider provide/withdraw path).
- **Drain** = a targeted intent the named node honours locally (leave + stay out) — a node-sovereign
  self-removal, never a remote eviction.

**Pairs with WS-E M15** (provisioning): an unmet `[min]` for a capability is just demand the
provisioner re-satisfies; `[max]` caps it. Restart and first-time provisioning collapse onto the
same resolve path.

**DoD G-T2:** multi-node test — publish `[min,max]` for a group; the cluster converges the member
count into the band **without overshoot or oscillation**; a `drain` intent removes the named node;
intent evaporation reverts to emergent (un-bounded) membership. Local pin beats fleet.

---

## 5. Track 3 — Operator surface (separate slice, **after** Track 2)

The human/observability door onto the headless engine. All existing patterns, low risk:

- **HTTP:** `/gateway/govern/...` routes (publish/clear fleet intent; set/clear local lock/enable/
  ratchet; `GET` snapshot). Fleet intent ⇒ POST to *any* node (gossips); local intent ⇒ POST to
  *that* node. Gated by a new **deny-by-default `govern:write`** scope in the gateway ACL
  (`required_scope` table) — governance is sensitive.
- **Audit (WS2):** route governance changes through the tamper-evident audit trail (who/what/when)
  — provenance is an audit concern, not a metrics one.
- **Prometheus:** per-node gauges for *effective* state (`auto_enabled`, per-param effective
  floor/ceiling/ratchet, `locally_pinned`, live hot values; group/provider target vs actual). The
  operator aggregates the fleet view in their own Prometheus/Grafana — *library, not platform*
  ([feedback: deployment framing]). Prometheus = effective state only; intent history = KV + audit.

**DoD G-T3:** `/gateway/govern` round-trips an intent end-to-end (scope-gated; unauthorized → 401/
403); a governance change appears in the audit verify endpoint; governor gauges scrape on `/metrics`.

---

## 6. Sequencing & philosophy guardrails

1. **Track 1** (extract reconciler) — small, mechanical, gated by existing governor tests.
2. **Track 2** (membership engine) — the design-heavy PR; reviewed on its own.
3. **Track 3** (operator surface) — mechanical follow-up once the engine is proven.

Litmus test for every step: *if the publishing entity vanishes, does the cluster keep running and
self-heal?* (Intents evaporate ⇒ yes.) Stay at Tier-2 (desired-state + local veto); escalate to
Tier-3 **consensus** only for a genuinely inviolable cluster-wide invariant (not in scope here).

---

## 7. Open questions (resolve at the start of Track 2)

1. **Eligibility rank for self-election** — lowest-id (deterministic, simple) vs lowest-load
   (better placement, needs the load signal) vs trust-weighted (reuse `suggest_leader`)? Start
   simple (lowest-id + jitter); make the rank pluggable.
2. **Hysteresis / round cadence** — tie the convergence-round interval to `health_check_interval`?
   What dead-band width avoids flap at realistic churn?
3. **Group vs provider unification** — one `MembershipGovernor` over both `grp/` members and
   `cap`/`gcap` providers, or two thin governors sharing the self-election helper? (Likely the
   latter — same Track-1 reconciler, same self-election helper, different count source.)
4. **Interaction with `max_peers` / partial-mesh fan-out** — ensure a bounded group target doesn't
   fight the SWIM/partial-mesh connection bounds.
