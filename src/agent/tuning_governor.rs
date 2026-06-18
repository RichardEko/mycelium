//! WS-C M9 — the **tuning governor**: the first worked instance of the project's
//! "management = intent + local reconcile" pattern (see project memory
//! *management-as-intent*). It constrains the auto-tuner (the [`ClusterTuner`] applier)
//! without ever becoming a coordinator:
//!
//! - **Local intent (sovereign).** The node's own API (`GossipAgent::set_dynamic_tuning`,
//!   `lock_tuning_{floor,ceiling}`, `set_tuning_ratchet`, `clear_tuning_locks`) sets the
//!   governor directly and marks the param *locally pinned* — the node owns its own config.
//! - **Fleet intent (advisory, evaporating).** Any entity with a concern (human via the
//!   gateway, or an agent) publishes a [`GovernIntent`] to `sys/govern/fleet` via
//!   [`GossipAgent::publish_tuning_intent`]. Every node's reconciler
//!   ([`GossipAgent::start_governor_reconciler`]) reads it and applies it **only where the
//!   param is not locally pinned** (local always wins), and **only while it is fresh** —
//!   a fleet intent is refreshed soft-state; if its publisher vanishes it ages out and the
//!   node reverts to its own derivation (the litmus test: management gone ⇒ self-heal).
//!
//! Nothing is ever permanently locked: a lock/ratchet is just the currently-winning intent,
//! lifted by a newer one. The governor gates the **auto-tuner only** — a deliberate manual
//! `set_*` is the operator's own override.
//!
//! [`ClusterTuner`]: crate::agent::cluster_tuner

use crate::agent::GossipAgent;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;

/// KV key carrying the cluster-wide (fleet) governance intent.
pub const GOVERN_FLEET_KEY: &str = "sys/govern/fleet";
/// How long a published fleet intent stays in force without a refresh (evaporation
/// window). A publisher must re-publish within this window; otherwise the intent ages
/// out and non-pinned params revert to unconstrained — self-healing if management dies.
pub const GOVERN_INTENT_TTL_MS: u64 = 5 * 60 * 1000;

const NO_FLOOR: u64 = 0;
const NO_CEIL: u64 = u64::MAX;

/// Which hot-tunable param a governance directive targets.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub enum HotParam {
    InboundFps,
    WriterDepth,
    BulkHandlers,
}

impl HotParam {
    fn from_key(s: &str) -> Option<Self> {
        match s {
            "inbound_fps" => Some(Self::InboundFps),
            "writer_depth" => Some(Self::WriterDepth),
            "bulk_handlers" => Some(Self::BulkHandlers),
            _ => None,
        }
    }
    /// Stable string key (used inside [`GovernIntent`] directives).
    pub fn key(self) -> &'static str {
        match self {
            Self::InboundFps => "inbound_fps",
            Self::WriterDepth => "writer_depth",
            Self::BulkHandlers => "bulk_handlers",
        }
    }
    fn all() -> [Self; 3] {
        [Self::InboundFps, Self::WriterDepth, Self::BulkHandlers]
    }
}

/// One-way constraint on auto-tuning. `Up` = the auto-tuner may never *decrease* the value
/// (it ratchets up only); `Down` = may never *increase* it. Reversed only by a new intent.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default, Serialize, Deserialize)]
pub enum Ratchet {
    #[default]
    Off,
    Up,
    Down,
}

impl Ratchet {
    fn to_u8(self) -> u8 {
        match self { Self::Off => 0, Self::Up => 1, Self::Down => 2 }
    }
    fn from_u8(v: u8) -> Self {
        match v { 1 => Self::Up, 2 => Self::Down, _ => Self::Off }
    }
}

struct ParamGov {
    floor: AtomicU64,   // NO_FLOOR = none
    ceiling: AtomicU64, // NO_CEIL  = none
    ratchet: AtomicU8,
    /// The node took local control of this param → fleet intents are ignored for it.
    local_set: AtomicBool,
}

impl Default for ParamGov {
    fn default() -> Self {
        Self {
            floor: AtomicU64::new(NO_FLOOR),
            ceiling: AtomicU64::new(NO_CEIL),
            ratchet: AtomicU8::new(0),
            local_set: AtomicBool::new(false),
        }
    }
}

impl ParamGov {
    fn reset(&self) {
        self.floor.store(NO_FLOOR, Ordering::Relaxed);
        self.ceiling.store(NO_CEIL, Ordering::Relaxed);
        self.ratchet.store(0, Ordering::Relaxed);
        self.local_set.store(false, Ordering::Relaxed);
    }
}

/// Per-node tuning governor. Effective state is atomics; the applier consults [`gate`] for
/// every recommendation. See the module docs for the intent/reconcile model.
///
/// [`gate`]: Self::gate
pub struct TuningGovernor {
    /// Master switch for the auto-tuner. `false` ⇒ no gossiped recommendation is applied.
    auto_enabled: AtomicBool,
    /// Whether `auto_enabled` was set locally (sovereign over the fleet `enabled`).
    enabled_local: AtomicBool,
    inbound: ParamGov,
    writer: ParamGov,
    bulk: ParamGov,
}

impl Default for TuningGovernor {
    fn default() -> Self {
        Self {
            auto_enabled: AtomicBool::new(true),
            enabled_local: AtomicBool::new(false),
            inbound: ParamGov::default(),
            writer: ParamGov::default(),
            bulk: ParamGov::default(),
        }
    }
}

impl TuningGovernor {
    fn p(&self, param: HotParam) -> &ParamGov {
        match param {
            HotParam::InboundFps => &self.inbound,
            HotParam::WriterDepth => &self.writer,
            HotParam::BulkHandlers => &self.bulk,
        }
    }

    /// Gate an auto-tuner recommendation `rec` for `param` given the currently-applied
    /// `cur`. Returns the value to apply, or `None` to skip (tuning disabled).
    pub fn gate(&self, param: HotParam, rec: u64, cur: u64) -> Option<u64> {
        if !self.auto_enabled.load(Ordering::Relaxed) {
            return None;
        }
        let p = self.p(param);
        let floor = p.floor.load(Ordering::Relaxed);
        let ceil = p.ceiling.load(Ordering::Relaxed).max(floor); // guard floor ≤ ceil
        let mut v = rec.clamp(floor, ceil);
        match Ratchet::from_u8(p.ratchet.load(Ordering::Relaxed)) {
            Ratchet::Up => v = v.max(cur),   // never auto-decrease
            Ratchet::Down => v = v.min(cur), // never auto-increase
            Ratchet::Off => {}
        }
        Some(v)
    }

    // ── Local intent (sovereign; marks the field locally pinned) ──────────────
    fn set_enabled(&self, on: bool) {
        self.auto_enabled.store(on, Ordering::Relaxed);
        self.enabled_local.store(true, Ordering::Relaxed);
    }
    fn lock_floor(&self, param: HotParam, v: u64) {
        let p = self.p(param);
        p.floor.store(v, Ordering::Relaxed);
        p.local_set.store(true, Ordering::Relaxed);
    }
    fn lock_ceiling(&self, param: HotParam, v: u64) {
        let p = self.p(param);
        p.ceiling.store(v, Ordering::Relaxed);
        p.local_set.store(true, Ordering::Relaxed);
    }
    fn set_ratchet(&self, param: HotParam, r: Ratchet) {
        let p = self.p(param);
        p.ratchet.store(r.to_u8(), Ordering::Relaxed);
        p.local_set.store(true, Ordering::Relaxed);
    }
    fn clear(&self, param: HotParam) {
        self.p(param).reset();
    }
    fn clear_all(&self) {
        for x in HotParam::all() {
            self.clear(x);
        }
        self.auto_enabled.store(true, Ordering::Relaxed);
        self.enabled_local.store(false, Ordering::Relaxed);
    }

    /// Apply a fleet intent — only to fields the node has **not** locally pinned, and
    /// honouring `enabled` only if the node has not locally pinned the master switch.
    fn apply_fleet(&self, intent: &GovernIntent) {
        if !self.enabled_local.load(Ordering::Relaxed)
            && let Some(e) = intent.enabled
        {
            self.auto_enabled.store(e, Ordering::Relaxed);
        }
        for d in &intent.params {
            let Some(param) = HotParam::from_key(&d.param) else { continue };
            let p = self.p(param);
            if p.local_set.load(Ordering::Relaxed) {
                continue; // local wins
            }
            p.floor.store(d.floor.unwrap_or(NO_FLOOR), Ordering::Relaxed);
            p.ceiling.store(d.ceiling.unwrap_or(NO_CEIL), Ordering::Relaxed);
            p.ratchet.store(d.ratchet.to_u8(), Ordering::Relaxed);
        }
    }

    /// Revert every **non-locally-pinned** field to unconstrained — used when the fleet
    /// intent is absent or has evaporated, so the node self-heals to its own derivation.
    fn revert_fleet(&self) {
        if !self.enabled_local.load(Ordering::Relaxed) {
            self.auto_enabled.store(true, Ordering::Relaxed);
        }
        for x in HotParam::all() {
            let p = self.p(x);
            if !p.local_set.load(Ordering::Relaxed) {
                p.floor.store(NO_FLOOR, Ordering::Relaxed);
                p.ceiling.store(NO_CEIL, Ordering::Relaxed);
                p.ratchet.store(0, Ordering::Relaxed);
            }
        }
    }

    /// Effective state, for diagnostics / the gateway.
    pub fn snapshot(&self) -> GovernorSnapshot {
        let snap = |param: HotParam| {
            let p = self.p(param);
            let floor = p.floor.load(Ordering::Relaxed);
            let ceil = p.ceiling.load(Ordering::Relaxed);
            ParamSnapshot {
                param,
                floor: (floor != NO_FLOOR).then_some(floor),
                ceiling: (ceil != NO_CEIL).then_some(ceil),
                ratchet: Ratchet::from_u8(p.ratchet.load(Ordering::Relaxed)),
                locally_pinned: p.local_set.load(Ordering::Relaxed),
            }
        };
        GovernorSnapshot {
            auto_enabled: self.auto_enabled.load(Ordering::Relaxed),
            params: HotParam::all().map(snap),
        }
    }
}

/// Snapshot of the governor's effective state (`GossipAgent::tuning_governor`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GovernorSnapshot {
    pub auto_enabled: bool,
    pub params: [ParamSnapshot; 3],
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ParamSnapshot {
    pub param: HotParam,
    pub floor: Option<u64>,
    pub ceiling: Option<u64>,
    pub ratchet: Ratchet,
    pub locally_pinned: bool,
}

// ── Fleet intent (gossiped) ─────────────────────────────────────────────────────

/// A governance directive for one param within a [`GovernIntent`].
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ParamDirective {
    pub param: String,
    pub floor: Option<u64>,
    pub ceiling: Option<u64>,
    pub ratchet: Ratchet,
}

/// A cluster-wide governance intent published by an entity with a concern (human or
/// agent). Carried at [`GOVERN_FLEET_KEY`] as evaporating soft-state; each node applies it
/// where it has not locally pinned the param, and discards it once stale.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct GovernIntent {
    pub enabled: Option<bool>,
    pub params: Vec<ParamDirective>,
    /// Unix-ms when published; the reconciler discards the intent after [`GOVERN_INTENT_TTL_MS`].
    pub written_at_ms: u64,
    /// `None` = whole fleet; `Some(node)` = only that node applies it. Lets per-node governance
    /// ride the gossip path (even to headless nodes) without reaching that node's HTTP directly.
    pub target: Option<crate::node_id::NodeId>,
}

impl GovernIntent {
    /// Convenience: a directive that bounds one param.
    pub fn bound(param: HotParam, floor: Option<u64>, ceiling: Option<u64>, ratchet: Ratchet) -> Self {
        Self {
            enabled: None,
            params: vec![ParamDirective { param: param.key().to_string(), floor, ceiling, ratchet }],
            written_at_ms: 0,
            target: None,
        }
    }
    /// Convenience: a directive that just flips the auto-tuner on/off cluster-wide.
    pub fn set_enabled(on: bool) -> Self {
        Self { enabled: Some(on), params: vec![], written_at_ms: 0, target: None }
    }
    /// Target this intent at a single node (per-node governance over gossip).
    pub fn for_node(mut self, node: crate::node_id::NodeId) -> Self {
        self.target = Some(node);
        self
    }
}

impl super::intent::FleetIntent for GovernIntent {
    fn written_at_ms(&self) -> u64 { self.written_at_ms }
    fn stamp(&mut self, now_ms: u64) { self.written_at_ms = now_ms; }
    fn target(&self) -> Option<&crate::node_id::NodeId> { self.target.as_ref() }
}

impl GossipAgent {
    // ── Local governor API (the node's sovereign intent) ──────────────────────

    /// Enable/disable the auto-tuner on this node (master switch). Locally pinned —
    /// fleet `enabled` intents are ignored until [`clear_all_tuning_locks`](Self::clear_all_tuning_locks).
    pub fn set_dynamic_tuning(&self, enabled: bool) {
        self.task_ctx.tuning_governor.set_enabled(enabled);
    }
    /// Lock a low watermark (floor): the auto-tuner may never drop `param` below `value`.
    pub fn lock_tuning_floor(&self, param: HotParam, value: u64) {
        self.task_ctx.tuning_governor.lock_floor(param, value);
    }
    /// Lock a high watermark (ceiling): the auto-tuner may never raise `param` above `value`.
    pub fn lock_tuning_ceiling(&self, param: HotParam, value: u64) {
        self.task_ctx.tuning_governor.lock_ceiling(param, value);
    }
    /// Set a one-way ratchet on `param` (see [`Ratchet`]).
    pub fn set_tuning_ratchet(&self, param: HotParam, ratchet: Ratchet) {
        self.task_ctx.tuning_governor.set_ratchet(param, ratchet);
    }
    /// Clear the local floor/ceiling/ratchet on `param` (un-pin it: fleet intents apply again).
    pub fn clear_tuning_locks(&self, param: HotParam) {
        self.task_ctx.tuning_governor.clear(param);
    }
    /// Clear all local governance (re-enable the auto-tuner, drop every lock/ratchet).
    pub fn clear_all_tuning_locks(&self) {
        self.task_ctx.tuning_governor.clear_all();
    }
    /// Effective governor state (for diagnostics / the gateway).
    pub fn tuning_governor(&self) -> GovernorSnapshot {
        self.task_ctx.tuning_governor.snapshot()
    }

    // ── Fleet intent (publish + reconcile) ────────────────────────────────────

    /// Publish a cluster-wide governance intent (WS-C M9). Stamps it `written_at_ms = now`
    /// and gossips it to [`GOVERN_FLEET_KEY`]; every node running
    /// [`start_governor_reconciler`](Self::start_governor_reconciler) applies it where it
    /// has not locally pinned the param. Re-publish within [`GOVERN_INTENT_TTL_MS`] to keep
    /// it in force (it evaporates otherwise — management gone ⇒ self-heal). Any entity with
    /// a concern may call this; it is intent, not command.
    pub fn publish_tuning_intent(&self, intent: GovernIntent) -> bool {
        super::intent::publish_intent(&self.kv(), GOVERN_FLEET_KEY, intent)
    }

    /// Opt this node in to **reconciling** gossiped fleet governance (WS-C M9). Spawns the
    /// shared intent reconciler over `sys/govern/`: it applies a fresh, fleet-or-self-targeted
    /// [`GovernIntent`] (local pins always win, inside `apply_fleet`) on every change and on a
    /// periodic tick, and reverts non-pinned fields once the intent evaporates. Exits on shutdown.
    pub fn start_governor_reconciler(&self) {
        let governor = Arc::clone(&self.task_ctx.tuning_governor);
        let governor_revert = Arc::clone(&governor);
        super::intent::spawn_intent_reconciler::<GovernIntent, _, _>(
            &self.task_ctx,
            "sys/govern/",
            GOVERN_FLEET_KEY,
            GOVERN_INTENT_TTL_MS,
            move |i| governor.apply_fleet(i),
            move || governor_revert.revert_fleet(),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gate_default_is_enabled_and_unconstrained() {
        let g = TuningGovernor::default();
        assert_eq!(g.gate(HotParam::WriterDepth, 4096, 1024), Some(4096));
    }

    #[test]
    fn gate_disabled_returns_none() {
        let g = TuningGovernor::default();
        g.set_enabled(false);
        assert_eq!(g.gate(HotParam::WriterDepth, 4096, 1024), None);
    }

    #[test]
    fn gate_clamps_to_floor_and_ceiling() {
        let g = TuningGovernor::default();
        g.lock_ceiling(HotParam::WriterDepth, 2048);
        assert_eq!(g.gate(HotParam::WriterDepth, 9999, 1024), Some(2048), "ceiling caps");
        g.lock_floor(HotParam::WriterDepth, 1500);
        assert_eq!(g.gate(HotParam::WriterDepth, 100, 1024), Some(1500), "floor lifts");
    }

    #[test]
    fn ratchet_up_never_decreases() {
        let g = TuningGovernor::default();
        g.set_ratchet(HotParam::WriterDepth, Ratchet::Up);
        assert_eq!(g.gate(HotParam::WriterDepth, 500, 1024), Some(1024), "below current → held");
        assert_eq!(g.gate(HotParam::WriterDepth, 4096, 1024), Some(4096), "above current → allowed");
    }

    #[test]
    fn ratchet_down_never_increases() {
        let g = TuningGovernor::default();
        g.set_ratchet(HotParam::WriterDepth, Ratchet::Down);
        assert_eq!(g.gate(HotParam::WriterDepth, 4096, 1024), Some(1024), "above current → held");
        assert_eq!(g.gate(HotParam::WriterDepth, 500, 1024), Some(500), "below current → allowed");
    }

    #[test]
    fn clear_removes_local_bounds() {
        let g = TuningGovernor::default();
        g.lock_ceiling(HotParam::WriterDepth, 100);
        g.clear(HotParam::WriterDepth);
        assert_eq!(g.gate(HotParam::WriterDepth, 9999, 1024), Some(9999));
    }

    #[test]
    fn local_pin_beats_fleet_intent() {
        let g = TuningGovernor::default();
        g.lock_ceiling(HotParam::WriterDepth, 2048); // local intent
        // A fleet intent tries to raise the ceiling — must be ignored (locally pinned).
        g.apply_fleet(&GovernIntent::bound(HotParam::WriterDepth, None, Some(9999), Ratchet::Off));
        assert_eq!(g.gate(HotParam::WriterDepth, 100_000, 1024), Some(2048));
    }

    #[test]
    fn fleet_applies_when_not_pinned_then_evaporates() {
        let g = TuningGovernor::default();
        g.apply_fleet(&GovernIntent::bound(HotParam::WriterDepth, None, Some(3000), Ratchet::Off));
        assert_eq!(g.gate(HotParam::WriterDepth, 100_000, 1024), Some(3000), "fleet ceiling holds");
        g.revert_fleet(); // intent evaporated → self-heal to unconstrained
        assert_eq!(g.gate(HotParam::WriterDepth, 100_000, 1024), Some(100_000));
    }

    #[test]
    fn fleet_enable_respected_unless_locally_pinned() {
        let g = TuningGovernor::default();
        g.apply_fleet(&GovernIntent::set_enabled(false));
        assert_eq!(g.gate(HotParam::WriterDepth, 4096, 1024), None, "fleet disabled");
        // Local re-enable pins the master switch; a later fleet disable is ignored.
        g.set_enabled(true);
        g.apply_fleet(&GovernIntent::set_enabled(false));
        assert_eq!(g.gate(HotParam::WriterDepth, 4096, 1024), Some(4096), "local enable wins");
    }

    #[test]
    fn snapshot_reflects_state() {
        let g = TuningGovernor::default();
        g.lock_floor(HotParam::InboundFps, 50);
        g.set_ratchet(HotParam::InboundFps, Ratchet::Up);
        let snap = g.snapshot();
        assert!(snap.auto_enabled);
        let p = snap.params.iter().find(|p| p.param == HotParam::InboundFps).unwrap();
        assert_eq!(p.floor, Some(50));
        assert_eq!(p.ceiling, None);
        assert_eq!(p.ratchet, Ratchet::Up);
        assert!(p.locally_pinned);
    }
}
