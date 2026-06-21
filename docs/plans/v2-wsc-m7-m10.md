# Delivery plan — WS-C · M7 distributed rate-limiting + M10 live reconfiguration

**Status:** ✅ **COMPLETE** (2026-06-21). M7 (PR #105), M10.1 hot-reloadable timing (PR #106), M10.2
intent-governed fence-free reconfiguration (this). WS-C now has **no deferred milestones** — the
Done-when is fully met. The M10 consensus *fence* was **declined-with-reasoning** (it would import a
coordinator to prevent transient variation the self-healing substrate tolerates); the *end* (live
cluster-wide timing reconfig) is delivered via hot-reload + management-as-intent. Design record follows.

**Original status:** proposed (2026-06-21). The two trigger-gated WS-C milestones, now being built. Canonical
design: ROADMAP §v2.0 M7 + M10; workstream context [`v2-wsc-metabolism.md`](v2-wsc-metabolism.md).

Both reuse machinery already on `main` — M7 the per-peer inbound limiter (`connection.rs` +
`HotConfig::inbound_fps`), M10 the management-as-intent transport (`intent.rs` /
`spawn_intent_reconciler`) and the `HotConfig` hot-reload atomics from M9. **No wire-format change**
in either (no `WIRE_VERSION` bump). Both stay **coordinator-free** (CP1) and **detection-not-prevention /
emergent-threshold** (CP4/CP5).

---

## M7 — Cluster-wide distributed rate-limiting (shared observation, local decision)

**The compliant shape (ROADMAP):** shared *evidence*, local *decision* — never a cluster-wide
eviction verdict (which would be a coordinator enforcing a behavioural judgment). A misbehaving
sender that floods many peers at once stays under each receiver's *per-peer* limit but is caught by
the *aggregate*.

- **Observe** — each receiver already counts inbound frames per peer (`connection.rs`). Periodically
  publish this node's observed per-sender rate to a dedicated, **short-TTL** KV namespace
  `sys/rate/{observer}/{sender}` (evaporating soft-state; bounded). This is *shared evidence*.
- **Decide locally** — each node sums `sys/rate/*/{sender}` (the aggregate observed rate across all
  observers) and, once it crosses a cluster threshold, **tightens its own inbound budget** for that
  sender — emergent backpressure on better-informed local evidence, the same posture as the existing
  per-peer limiter. A sustained abuser ends up throttled by every node it touches, with no global
  round.
- **Disconnect stays node-local** — dropping an abused connection is each node's own self-defense, never
  a consensus eviction.
- **Visibility** — `SystemStats::rate_limited_senders` (count of senders this node is tightening) +
  the per-sender aggregate readable from `sys/rate/`.

**Config:** `GossipConfig::rate_observation` (off by default — `0`/unset preserves today's per-peer-only
behaviour). Knobs: observation publish interval, TTL, the aggregate threshold (a multiple of
`max_inbound_frames_per_sec`).

**Gate G-M7:** a 3-node test where one sender's aggregate rate (summed across observers) crosses the
threshold ⇒ every observer's `rate_limited_senders` reflects it and the sender's effective inbound
budget tightens; a well-behaved sender never trips it; `sys/rate/` entries evaporate when sending stops.

---

## M10 — Live reconfiguration of timing parameters (intent-governed, **fence-free**)

**The design reframe (load-bearing).** The ROADMAP's original M10 proposes a *consensus fence*:
agree a config version, drain, restart tasks, confirm cluster-wide-atomic-or-rollback. **Examined
against the shipped substrate, the fence is unnecessary — and importing it would violate CP1.** The
reasoning:

1. **Within a node, apply atomically.** A node swaps its whole timing set at once (an intent carries
   all fields; the reconciler applies them in one pass), so no loop ever observes a half-applied
   config — closing the only *correctness* hazard the ROADMAP names (the internal-invariant window).
2. **Across nodes, transient variation is benign.** Mycelium is eventually-consistent and
   **self-healing by design**. A window where some nodes run the old `health_check_interval` /
   `reconnect_backoff` and some the new is a *transient suboptimality* (slightly different detection
   latency / reconnection cadence), **not** a safety violation — no split-brain, no data loss, no
   stuck state; the cluster converges as each node reconciles. Paying for a consensus fence (a
   coordinator) to prevent a transient state the substrate already tolerates and heals is exactly the
   coordinator-trap CP1 forbids.

So M10's *end* (live cluster-wide timing reconfiguration) is delivered the Mycelium way — **hot-reload
+ management-as-intent** — and the consensus *means* is consciously declined, recorded with this
reasoning (the same disciplined posture as the G2 overlay decision).

### M10.1 — Hot-reloadable timing params

- Add the restart-requiring timing params to `HotConfig` as atomics: `health_check_interval_secs`,
  `reconnect_backoff_secs`, `peer_eviction_intervals` (the ROADMAP's named set).
- Convert the background loops (health monitor, reconnect, eviction in `lifecycle.rs`) to **re-read
  the atomic each tick** instead of capturing `config.X` once at spawn — so a change takes effect on
  the next cycle with **no task restart** (the same technique M9 used for `writer_channel_depth` etc.).
- A node-local `set_timing(...)` setter (mirrors the M9 `set_*` setters).
- **Gate G-M10.1:** change a timing param live on a running node ⇒ the affected loop's cadence
  changes on its next tick, no task respawned, `task_count` unchanged.

### M10.2 — Intent-governed cluster-wide reconfiguration

- A `TimingIntent` (a `FleetIntent`) published to a `sys/config/timing` key and reconciled by every
  node via the existing `spawn_intent_reconciler` — newest-wins, **local-wins** over fleet (an
  operator's node-local `set_timing` is its own override), **evaporating** (TTL → self-heal back to
  baseline). Human and agent publishers are substrate-identical — intent, never command.
- `GET`/`POST /gateway/govern/timing` operator surface (reuses the elastic-governor gateway pattern),
  audited.
- **Gate G-M10.2:** publish a `TimingIntent` on one node ⇒ all nodes apply it within the TTL window
  (their loops adopt the new cadence); a node's local `set_timing` wins over the fleet intent;
  letting the intent evaporate returns nodes to baseline. The rationale (no fence) is documented.

---

## Sequencing & gates

1. **M7** — distributed rate-limiting (independent; self-contained).
2. **M10.1** — hot-reloadable timing params (the mechanism).
3. **M10.2** — intent-governed propagation + operator surface + the fence-free rationale.

Each its own PR. Gates: `cargo test --lib --features tls,metrics,a2a,llm` + clippy `-D warnings`;
the M10.2 gate is a cross-node intent-reconcile test mirroring the M9 governor's
`test_wsc_m9_governor_fleet_reconcile_and_local_wins`. On completion, the WS-C *Done-when* is met with
**no deferred milestones** — update the scorecard + ROADMAP M7/M10 entries to shipped.
