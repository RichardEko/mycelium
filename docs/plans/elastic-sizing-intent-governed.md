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

> **Track 2a (group membership) ✅ SHIPPED.** `src/agent/membership_governor.rs`:
> `MembershipIntent { group, min, max, drain, target }` (FleetIntent, via Track-1 transport);
> `publish_membership_intent` / `start_membership_governor`; probabilistic self-election
> (`join_probability`/`leave_probability`/`decide`, pure + unit-tested) over the live `grp/`
> count, eligibility from the group's `CapabilityGroupDef` filter, own-load bias, drain, and a
> post-action cooldown. Reuses `emit_membership` + the Track-1 reconcile helpers. Gates: 5 pure
> unit tests + `test_membership_governor_converges_to_min` (3-node cluster converges up to min).
> **Track 2b (provider/replica sizing)** remains, after WS-E M15.

**Intent shape:** per group (and per capability, the provider analogue) — `min`, `max`, optional
`drain` (exclude named nodes). Published fleet intent + local override, same governor model.

**Node-targeting (applies to every governor intent, incl. the shipped tuning `GovernIntent`):**
add an optional `target: Option<NodeId>`. The reconciler applies a targeted intent only when
`target` is `None` (whole fleet) or equals `self`. This is what makes "not all nodes run HTTP" a
non-issue — per-node governance reaches even headless nodes over gossip, addressed by node-id, so
the operator never needs that node's HTTP. (Still local-veto-able; still evaporates.) A small
additive change to the existing tuning governor's intent + reconcile, worth landing alongside Track 1.

> **⚠ Crux — bounds are convergence targets, never guarantees.** `min`/`max` mean "the cluster
> *converges toward* this band," **not** "this count is guaranteed." A sovereign node may decline
> to join even when the group is under `min` (local veto always wins); the intent evaporates if its
> publisher vanishes. This is the same honesty as "namespace ownership is promise-strength" — and
> it is *load-bearing*: the moment we promise an exact/guaranteed count we have implied a
> coordinator. A genuinely inviolable count is a conscious **Tier-3 consensus** escalation for that
> one operation, never smuggled into this Tier-2 governor.

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

The human/observability door onto the headless engine. All existing patterns, low risk.

**HTTP is opt-in on a *subset* of nodes — never required on all.** The `gateway` feature is the
heavy axum/hyper half (the reason for the `mycelium-core` split + the bare-metal/WASM embed story).
The engine is **gateway-free** (reconciler = `subscribe_prefix`+`kv.get`; publish = `kv.set`), so a
**headless node stays first-class**: it reconciles fleet intents and self-heals, it just isn't an
operator entry point. Enable the gateway on whatever nodes you want operator-reachable (one, a few,
or a dedicated edge/management node).

**No consensus, no single-active endpoint, no forwarding.** Publishing a fleet intent is an
idempotent, commutative LWW KV write — *any* gateway node can accept the POST and it gossips and
converges (two operators on two nodes is fine: newest-wins). An elected "active endpoint" would be
a coordinator + SPOF (fails the litmus test: it dies ⇒ operator interaction blocks) solving a
problem the intent model dissolves. Want one URL? That's an **operator-side ingress/LB** pointed at
the gateway nodes — *library, not platform* — not a Mycelium election.

**Per-node control without per-node HTTP** = a **node-targeted fleet intent** (see §4 / §3 intent
shape: optional `target: Option<NodeId>`). It gossips to everyone including headless nodes; the
named node applies it (with local veto). So you govern a specific headless node by POSTing a
targeted intent at *any* gateway node — never by reaching that node's HTTP directly. The
local-direct-API path (sovereign pin) remains for a node's *own* agents, not the operator's primary route.

- **HTTP:** `/gateway/govern/...` routes (publish/clear fleet intent — optionally `target`-ed;
  `GET` snapshot). Gated by a new **deny-by-default `govern:write`** scope in the gateway ACL
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

**Explicitly rejected:** a consensus-elected single "active governance endpoint" with request
forwarding. It re-introduces a coordinator + SPOF for the operator door and fails the litmus test,
to serialize writes that are already idempotent/commutative (LWW). The operator door is "gateway on
a subset of nodes + node-targeted fleet intent over gossip"; one-URL convenience is operator-side
ingress, not a Mycelium election.

### 6.1 Philosophy compliance — must-holds (build gate)

Audited against [ROADMAP §Core Principles — Compliance Gate](../../ROADMAP.md). The plan is
compliant by construction (Principle 1 even names the `ClusterTuner` shape as the reference); the
risks are all *implementation discipline* — drift points to hold the line on while building. **A
PR in this plan that violates one of these is non-compliant by construction — redesign or don't
ship** (ROADMAP's own gate language).

| Must-hold | Core Principle | The drift it prevents |
|---|---|---|
| **Distributed decision, no controller** — convergence is N independent local decisions; no node watches-the-count-and-assigns | 1 (no coordinator; litmus: "watch-and-decide / assign-a-role") | importing the HPA *controller* model |
| **Bounds are convergence targets, never guarantees** — sovereign veto always wins; min/max is "tends toward", documented | 1 + 5 | implying an exact/guaranteed count ⇒ a coordinator |
| **No synchronous barrier** — jittered threshold activation; transient over/undershoot is the accepted price of emergence | 5 (threshold over agreement) | adding a round/agreement to get "clean" convergence |
| **`IntentReconciler` lives in the agent layer**, composing core's KV-subscribe + freshness convention — never in `mycelium-core` | 1 + 3 (mechanism in core, agency above) | leaking the governance abstraction into the substrate |
| **Read-side reconcile + audit, never a write-guard** in `apply_and_notify` | 3 + 4 (detection not prevention) | teaching Layer I a higher-layer law |
| **No consensus** for the operator door or for bounds (stay Tier-2) | 2 (consistency as a service) | folding agreement into the fast path |
| **Local `lock_*` is self-binding, overridable by a newer intent** — not enforcement on others | 4 (promise-strength) | "lock" being read as a hard, un-overridable bound |

Escalation is allowed but must be *conscious*: a genuinely inviolable cluster-wide count/bound is a
per-operation **Tier-3 consensus** decision (Principle 2), never smuggled into the Tier-2 governor.

### 6.2 Composability note (Principle 6)

The engine ships as agent-layer modules on the public KV/capability API (like `cluster_tuner` /
`tuning_governor`). If Track 2 grows large, a **`mycelium-governor` companion crate** (tuple-space
style) is the more Principle-6-aligned home — keeps the main crate lean and re-proves the public
API is sufficient. Decide at the start of Track 2 based on size.

---

## 7. Open questions — RESOLVED (2026-06-18)

1. **Eligibility rank** → **pluggable; default load-then-id** (lowest `sys/load/{node}` fill_ratio,
   tie-break lowest-id). Reuses the opacity load signal; balances placement; and a node acting on
   *its own* load is the local knowledge Principle 1/5 wants. Pluggable so an operator/agent can
   override (e.g. trust-weighted via `suggest_leader`).
2. **Cadence / hysteresis** → **derived (M8-style), not hand-set.** Convergence tick = `health_check_
   interval` (the natural observe beat); a per-node post-action **cooldown** (a few ticks) damps
   boundary flap; the `[min,max]` band is itself the dead-band. Knobs stay internal for v1.
3. **Group vs provider** → **two thin governors sharing one `self_elect` helper**; **group membership
   ships first (Track 2a)** — its count source (`grp/{group}/*`) and `join_group`/`leave_group`
   already exist. **Provider/replica sizing is Track 2b, after WS-E M15** supplies a provide/withdraw
   path (building it here would drag M15 in).
4. **Partial-mesh interaction** → **non-issue.** Membership is a *logical overlay* (`grp/` KV keys),
   not a connection requirement — gossip reaches members via flooding regardless of SWIM/partial-mesh
   fan-out. Only edge: `min` > eligible-node-count is unsatisfiable (best-effort, expected).

### 7.1 Resolved self-election mechanism (Track 2a)

Per node, each tick (`health_check_interval`, jittered) **and** on `grp/`/intent gossip change, for
each governed group:
- compute the **eligible set** (nodes whose gossiped caps match the group's `CapabilityGroupDef`
  filter) and **my rank** within it (load-then-id); read live **N** (`grp/{group}/*` count) and the
  intent `[min,max]`/`drain` (a `MembershipIntent` via the Track-1 transport, fresh + self/fleet-targeted).
- **probabilistic (Option B — chosen 2026-06-18).** *Why not pure position-based:* it is
  veto-fragile — a vetoing top-ranked node leaves its slot unfilled because lower nodes count it
  "ahead in the queue," stalling convergence below `min`. Probabilistic self-election is robust to
  veto and view-skew by construction.
  - **Join** (eligible non-member, `N < min`) with probability `(min − N) / eligible_non_members`,
    **biased by the node's *own* load** (`gossip_shard_fill` — idle ⇒ ×1.5, busy ⇒ ×0.5, clamped);
    using *own* load keeps it purely local (Principle 5) and prefers idle nodes for placement. A
    busy-but-needed node keeps a non-zero floor so convergence still completes.
  - **Leave** (member, `N > max`) with probability `(N − max) / members`, biased toward busy nodes.
  - **Drain** (I'm listed) → leave (highest priority).
  - Re-rolled each tick against the live `N`; a per-group **post-action cooldown** damps flap.
    Converges over *a few* ticks (not one) with bounded-variance overshoot — both within the accepted
    truths. Local veto handled for free: a vetoer contributes 0; others fill over subsequent ticks.

**Three accepted truths** (agreed): (a) partition ⇒ each side self-sizes ⇒ transient ≤2×max on heal,
re-converges (AP/CAP reality; bounds are per-partition); (b) local veto absorbed by re-check; (c)
drain composes with min (drain ⇒ N drops ⇒ next-ranked joins; eligible<min ⇒ stuck under min, expected).

### 7.2 Remaining (Track 2b / later)

- Provider/replica sizing once WS-E M15 provide/withdraw exists.
- Whether the `MembershipIntent` should carry the group filter or rely on the local `CapabilityGroupDef`.
