# v2.0 WS-C — Self-Managing Metabolism — Delivery Plan

**Status:** ✅ **COMPLETE — all four milestones shipped, no deferrals.** M8 startup auto-derivation +
M9 hot-reload/`ClusterTuner` + tuning governor (PRs #26/#27) + elastic `MembershipGovernor` (see
[`elastic-sizing-intent-governed.md`](elastic-sizing-intent-governed.md)); **M7** distributed
rate-limiting (PR #105) + **M10** live timing reconfiguration — fence-free (PRs #106/#107), see
[`v2-wsc-m7-m10.md`](v2-wsc-m7-m10.md).

Per-workstream execution plan. The canonical *design* lives in
[ROADMAP.md §v2.0 Milestones](../../ROADMAP.md) (M7, M8, M9, M10) and the workstream
summary in [`docs/plans/v2.0.md`](v2.0.md) §WS-C. This document is **strategy /
sequencing only** — it does not restate the milestone designs, it orders them into
shippable phases with falsifiable gates, mirroring
[`v2-wsb-scale-transport.md`](v2-wsb-scale-transport.md).

WS-C theme: **the cluster tunes itself to its own size and load — node-locally, with
no config coordinator.**

---

## 1. Why now (the demand signal)

WS-C's ROADMAP trigger is *"recurring ops friction from teams deploying clusters of
varying size."* WS-B produced exactly that signal in-house:

- The Stage-4 SWIM divergence root cause was a **config bug** — the demo never applied
  `GOSSIP_*`, so SWIM was silently off. A self-deriving config removes the whole class
  of "forgot to set the env var for this size" failure.
- We had to **hand-write** [`docs/operations/tuning.md`](../operations/tuning.md) (a
  quick-ref table, 5 hard invariants, and per-size scaling guidance) precisely because
  none of it is automatic. M8 turns that guide into closed-form derivation.
- The 100-node `make test-scale` formation is sensitive to fan-out / health-check /
  anti-entropy cadence that are **not** derived from N; the entries test had to bump
  `writer_channel_depth` to 4096 by hand. Auto-derivation makes the scale suite itself
  more robust (a DoD gate below).

So WS-C is **consolidation of what WS-B shipped**, not a new frontier — the lowest-risk,
highest-"we just felt this" next step. It is also **philosophy-clean**: derivation is a
node-local function of observable N; the optional tuner *advises*, nodes *decide*
(Core Principles 1, 4, 5).

---

## 2. Scope & sequencing

| Milestone | What | Phase | Posture |
|---|---|---|---|
| **M8** Startup auto-derivation | `None`/"auto" sentinels in `GossipConfig`; closed-form values from N at `start()` | **Phase 1** | The keystone — ship first, standalone |
| **M9** Hot-reloadable subset + `ClusterTuner` | `Arc<AtomicU32>` for sampled-per-use params; advisor agent over `sys/config/` | **Phase 2** | Builds on M8's formulas; trigger-gated |
| **M7** Distributed rate-limiting | gossiped `sys/rate/` observations → local budget tightening | **Phase 3 (independent)** | Different trigger (abuse), parks until needed |
| **M10** Fenced live reconfiguration | consensus-fenced restart of timing params | **Deferred** | Heaviest; defer until M8+M9 proven insufficient |

```
Phase 1  M8  startup auto-derivation     [self-contained — clears the ops-friction signal]
Phase 2  M9  hot-reload + ClusterTuner    [needs M8 formulas; gate on elastic-scaling demand]
Phase 3  M7  distributed rate-limit       [independent; gate on a real abuse pattern]
   —     M10 fenced reconfiguration        [deferred until 8+9 demonstrably insufficient]
```

**Sequencing rationale:** M8 is pure node-local derivation (no consensus, no new wire,
no task restarts — it runs once before any task spawns). M9 reuses M8's formulas behind
a gossip/subscribe advisor. M10 is the only one needing a fence; its ROADMAP entry
already says "defer until there is a validated production need 8 and 9 cannot address."
M7 is orthogonal (abuse-triggered, not size-triggered) and parks on its own trigger.

---

## 3. Definition of done (falsifiable gates)

| Gate | Harness | What it proves |
|---|---|---|
| **G-C1 — formulas are correct & invariant-safe** | `config::tests` unit | `derive_from_n(N)` matches the documented table **and** satisfies all 5 `tuning.md` hard invariants for N ∈ {1, 3, 20, 50, 100, 1000}. |
| **G-C2 — zero-tuning convergence** | host integration test `test_wsc_m8_auto_config_cluster_converges` (a `GossipConfig::auto()` cluster forms + propagates a KV write; resolved config is derived & valid) | A cluster deployed with no hand-set knobs converges — the ops-friction the workstream exists to remove. *(Node-count axis only: `writer_channel_depth = N×4` does not cover the orthogonal entry-volume burst, so the entries-test's manual depth bump stays a separate, volume-driven knob — see `tuning.md` §Auto-derivation.)* |
| **G-C3 — override precedence** | `config::tests` unit | An explicit operator value always wins over derivation; an explicit value that *violates* an invariant logs a `warn!` (detection, not silent coercion) but is honoured. |

G-C2 is the load-bearing proof; G-C1/G-C3 are the unit-level correctness net. Phase 2
adds **G-C4** ✅ (`test_wsc_m9_config_policy_accept_vs_reject`: a gossiped recommendation is
applied by an `accept_all` node and ignored by a `reject_all` node — advisor-not-coordinator),
plus `test_wsc_m9_hot_reload_set_api` for the live set_* mechanism.

---

## 4. Phase 1 — M8 startup auto-derivation (the keystone) ✅ SHIPPED

**Outcome:** `GossipConfig::auto()` + `derive_unset(n)` + `audit_invariants()` in
`mycelium-core/src/config.rs`; `GossipAgent::new` derives from `bootstrap_peers + self`
before `validate()` (so a derived field never trips the zero-guard) and a `config()`
accessor exposes the resolved values. `0` is the per-field "auto" sentinel (consistent with
`gossip_fanout`); explicit/env values pass through untouched. Fan-out was already auto via
`resolved_fanout`, so M8 left it alone (resolved open questions #2 & #4). Gates green:
G-C1 (`derive_matches_table_and_invariants` + idempotence + `integer_sqrt`), G-C3
(`explicit_values_override_derivation`, `invariant_violating_explicit_config_is_honoured`),
G-C2 (`test_wsc_m8_auto_config_cluster_converges`). One correction vs. the original plan:
`writer_channel_depth = N×4` is the node-count axis only — entry-volume burst is orthogonal
(documented). Design notes below.



### 4.1 Mechanism
Add an "auto" sentinel to each formula-driven `GossipConfig` field. For numeric fields
the sentinel is a dedicated value already meaning "unset" where one exists
(`max_active_connections = 0`, `max_inbound_frames_per_sec = 0`) or a new
`Option<_>` / `0`-as-auto convention documented per field. At `GossipAgent::start()`,
**before any task spawns**, a single `config.derive_unset(n_estimate)` pass fills every
auto field, where `n_estimate = bootstrap_peers.len().max(1)` (a lower bound on N).

This slots in next to the existing [`apply_env_overrides`](../../mycelium-core/src/config.rs)
and [`resolved_fanout`](../../mycelium-core/src/config.rs) — env overrides apply *first*
(operator intent), then `derive_unset` fills only what remains auto. Order:
**explicit field set in code > `GOSSIP_*` env > auto-derivation > hard default.**

### 4.2 Derivation table (reconciled with SWIM-default)
The ROADMAP M8 table predates the M5 SWIM cutover; under SWIM the persistent-TCP
footprint is the **forwarding set bounded by `resolved_fanout(gossip_fanout,
max_active_connections, N)`**, not a √N connection cap. The derivation therefore targets
the *fan-out k* and feeds the existing `resolved_fanout` knob rather than reintroducing a
separate cap:

| Field | Auto-derivation | Invariant tie-in (`tuning.md`) |
|---|---|---|
| `default_ttl` | `max(5, ceil(log₂(N+1)))` | §4 TTL ≥ gossip diameter (safe upper bound for any fan-out k ≥ 2) |
| fan-out `k` (`gossip_fanout`) | `0` (full mesh) if N ≤ 20, else `max(16, ceil(√N))`; consumed by `resolved_fanout` | the O(N²)→O(N·k) structural fix WS-B shipped |
| `writer_channel_depth` | `max(1024, N × 4)` | §5 writer depth ≥ burst fan-in |
| `max_seen_entries` | `max(100_000, N × 1_000)` | dedup horizon scales with origin count |
| `propagation_window_secs` | `max(60, health_check_interval_secs × peer_eviction_intervals × 2)` | §3 propagation ≥ eviction window |
| `ping_peer_sample_size` *(candidate)* | `min(N, max(20, ceil(√N)))` | bounds Ping fan-in at large N |

SWIM cadence (`swim_suspicion_timeout_ms`, `swim_gossip_updates`) scaling with N is a
**candidate**, listed for evaluation in Phase 1 but only adopted if a size sweep shows the
fixed defaults degrade — we do not derive what isn't demonstrably size-sensitive.

### 4.3 Invariant enforcement
`derive_unset` finishes with a `validate()` pass that enforces the 5 `tuning.md`
invariants on the *resolved* config (whether each field was derived or operator-set):
1. `reconnect_backoff_secs < health_check_interval_secs − 2`
2. eviction window (`health_check_interval × peer_eviction_intervals`) ≥ expected restart gap
3. `propagation_window_secs ≥` eviction window
4. `default_ttl ≥` estimated gossip diameter
5. `writer_channel_depth ≥` burst fan-in floor

Derived values are constructed to satisfy these by formula. An **operator** value that
violates one is honoured but emits a `warn!` (G-C3) — same detection-not-prevention
posture as the consensus/`sys/` tripwires.

### 4.4 Surface & docs
- `system_stats()` (or a new `GET /config` diagnostic) reports the *resolved* values and,
  per field, whether it was `explicit | env | derived | default` — so an operator can see
  what the cluster chose. (M16 edge criterion: keep it under `/gateway` if it exposes
  anything sensitive; the resolved tuning values are not, so `/stats`-adjacent is fine.)
- `tuning.md` gains a short "Auto-derivation" section: the table above + "leave it unset
  and Mycelium sizes it; set it to override." The existing manual guidance stays as the
  override reference.

### 4.5 Tests (Phase 1)
- `config::tests::derive_matches_table_and_invariants` → **G-C1**.
- `config::tests::explicit_overrides_win_and_warn_on_violation` → **G-C3**.
- Re-run `make test-scale-resilience` / `test-scale-entries` with an all-auto compose
  (strip the hand-set `GOSSIP_WRITER_CHANNEL_DEPTH=4096` etc.) → **G-C2**; the entries
  compose's manual bump should become unnecessary, which is itself the proof.

---

## 5. Phase 2 — M9 hot-reload + `ClusterTuner` ✅ SHIPPED

**Outcome:** the three sampled-per-use params (`max_inbound_frames_per_sec`,
`writer_channel_depth`, `max_concurrent_bulk_handlers`) now live in
`mycelium_core::context::HotConfig` (atomics on `CoreCtx::hot`, seeded from the M8-derived
config) and are read live at each use/spawn: the connection inbound-rate check (per frame),
`get_or_spawn_writer` (per writer spawn — threaded through the connection handler **and**
`run_gossip_shard`/`run_health_monitor`), and the bulk admission (the fixed `Semaphore` was
replaced by a dynamic limit over the existing `active_handlers` counter — atomic fetch-add
with back-out, hot-reloadable both ways). `GossipAgent::{set_max_inbound_frames_per_sec,
set_writer_channel_depth, set_max_concurrent_bulk_handlers}` + `hot_tunables()` are the
node-local application/inspection API.

`ClusterTuner` (`src/agent/cluster_tuner.rs`) is two opt-in tasks, no new mechanism:
`start_cluster_tuner(interval, policy)` runs the **advisor** (observe `peers()`+self → recompute
`GossipConfig::auto_writer_channel_depth(N)` — the *same* fn M8's `derive_unset` uses, no drift
— and gossip to `sys/config/{param}` only when it changed) and the **applier**;
`start_config_applier(policy)` runs just the applier (`subscribe_prefix("sys/config/")` →
run the node's [`ConfigPolicy`] → apply accepted values live). `accept_all`/`reject_all`/
`clamped(min,max)` are provided. Fully decentralized: every node computes the same formula and
converges via LWW; `sys/config/` is deliberately outside the `sys/` self-owned tripwire set.

Gates: G-C4 (`test_wsc_m9_config_policy_accept_vs_reject` — a gossiped recommendation is applied
by an `accept_all` node and ignored by a `reject_all` node) + `test_wsc_m9_hot_reload_set_api`
(set_* updates the hot cell live + writer-depth floor). Matrix 266 lib tests + clippy
`-D warnings` (matrix / gateway-free / tuple-space) green. Original design notes below.

**Start trigger (now satisfied for the mechanism):** the hot-reload primitive + tuner are in;
running the advisor in production is still demand-gated on an actual elastic-scaling deployment.

### 5.1 Tuning governor ✅ SHIPPED — first worked instance of *management = intent + local reconcile*

Post-implementation, management needed to **bound/disable** the auto-tuner. Rather than a
control plane, this became the canonical instance of the project pattern (see project memory
*management-as-intent*): a `TuningGovernor` (`src/agent/tuning_governor.rs`) the applier consults
before applying any recommendation, fed by two intent sources:

- **Local (sovereign):** `GossipAgent::{set_dynamic_tuning, lock_tuning_floor, lock_tuning_ceiling,
  set_tuning_ratchet, clear_tuning_locks, clear_all_tuning_locks, tuning_governor}` — set the
  governor directly and mark the param locally pinned.
- **Fleet (advisory, evaporating):** `publish_tuning_intent(GovernIntent)` → gossiped
  `sys/govern/fleet`; `start_governor_reconciler()` applies it only where not locally pinned
  (local-wins) and only while fresh (`GOVERN_INTENT_TTL_MS`; evaporates → self-heal). Human and
  agent publishers are substrate-identical — it is intent, never command.

`gate()` = `enabled? → clamp to [floor, ceiling] → ratchet vs current`. Ratchet is one-way
(`Up` never auto-decreases, `Down` never auto-increases); nothing is permanently locked (a newer
intent always overrides). Gates the **auto-tuner only** — a manual `set_*` is the operator's own
override. Gates: 10 `tuning_governor::tests` (gate / clamp / ratchet / clear / local-wins /
evaporation / fleet-enable / snapshot) + `test_wsc_m9_governor_fleet_reconcile_and_local_wins`
(cross-node). The broader "govern M8 startup + manual `set_*` too" remains the recognized general
application, deferred.

---

## 6. Phase 3 — M7 distributed rate-limiting (independent; gate: abuse)

Shared observation, local decision: gossip per-sender frame-rate evidence via a bounded
short-TTL `sys/rate/{node}/` namespace; each node independently tightens its *own* inbound
budget once aggregate observed rate crosses a threshold; disconnection stays a node-local
self-defense choice — never a consensus-issued cluster eviction. Expose via `system_stats()`.
**Start trigger:** a confirmed intra-cluster abuse pattern (today `max_inbound_frames_per_sec`
suffices for well-behaved deployments). Orthogonal to M8/M9; sequenced last only because its
trigger is independent of cluster size.

---

## 7. Cross-cutting / philosophy compliance

- **No coordinator (CP1).** M8 is a pure node-local function of observable N. M9's tuner
  has *no standing agency* — it writes advisory KV; nodes clamp/ignore/override freely,
  exactly as operator values override auto-derivation. M10 (deferred) is the only piece
  that needs a fence, which is why it waits for proven need.
- **Detection, not prevention (CP4).** Invariant violations on operator values warn; they
  are not silently rewritten.
- **No new substrate.** M8 is config-layer only. M9 reuses `subscribe_prefix` + atomic
  fields. M7 reuses the KV namespace + the existing per-peer limiter. Nothing touches the
  wire format (no `WIRE_VERSION` bump anywhere in WS-C).

---

## 8. Open questions (resolve before Phase 1 build)

1. **`n_estimate` source.** `bootstrap_peers.len()` is a lower bound and can badly
   under-count a large cluster joined via a single seed. Options: (a) accept the
   under-count (derivation is a floor; operators override when they know better);
   (b) re-derive opportunistically once `peers()` reflects the true N — but that crosses
   into M9 territory (re-deriving live). Recommend (a) for M8, (b) as the M9 hook.
2. **Auto sentinel encoding.** `0`-as-auto for fields where `0` isn't a legal operating
   value (`max_active_connections`, `writer_channel_depth`) vs. `Option<_>` for fields
   where `0`/absence is meaningful. Decide per field; document in the `GossipConfig` doc
   comments alongside the existing `GOSSIP_*` notes.
3. **Which SWIM params (if any) are size-derived.** Resolve via a Phase-1 size sweep, not
   by assumption.
4. **`derive` vs `resolved_fanout` overlap.** `resolved_fanout` already turns
   `gossip_fanout`/`max_active_connections`/N into an effective k at runtime. M8 should
   derive the *inputs* and leave `resolved_fanout` as the single resolution point — avoid
   two places computing k.
