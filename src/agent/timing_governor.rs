//! Cluster-wide live timing reconfiguration (WS-C / M10.2) — **intent-governed, fence-free**.
//!
//! M10.1 made the timing params (`health_check_interval_secs`, `reconnect_backoff_secs`)
//! hot-reloadable (the loops re-read [`HotConfig`] each cycle). This module distributes a change
//! *cluster-wide* the Mycelium way — **management = intent + local reconcile** (the same transport
//! the elastic governor uses, [`super::intent`]) — and **deliberately without a consensus fence**.
//!
//! ## Why no fence (the load-bearing decision)
//!
//! The ROADMAP's original M10 proposed a consensus fence (agree a config version, drain, restart,
//! confirm cluster-wide-atomic-or-rollback). Examined against the shipped substrate, the fence is
//! unnecessary — and importing it would violate Core Principle 1 (no coordinator):
//!
//! 1. **Within a node, apply atomically.** A [`TimingIntent`] carries the whole timing set and is
//!    applied in one reconcile pass, so no loop ever observes a half-applied config — closing the
//!    only *correctness* hazard the ROADMAP names.
//! 2. **Across nodes, transient variation is benign.** Mycelium is eventually-consistent and
//!    self-healing by design. A window where some nodes run the old `health_check_interval` and some
//!    the new is a *transient suboptimality* (slightly different detection latency), **not** a safety
//!    violation — no split-brain, no data loss; the cluster converges as each node reconciles. Paying
//!    for a coordinator to prevent a state the substrate already tolerates and heals is the
//!    coordinator trap.
//!
//! So the *end* (live cluster-wide timing reconfiguration) is delivered; the consensus *means* is
//! consciously declined. Newest-wins, **local-wins** (a node-local `set_*` pins the node — it is
//! sovereign over its own config), **evaporating** (TTL → self-heal to the static baseline). Human
//! and agent publishers are substrate-identical — intent, never command.

use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::atomic::Ordering;

use crate::agent::TaskCtx;
use crate::node_id::NodeId;
use mycelium_core::kv_handle::KvHandle;

/// KV key carrying the fleet timing intent (evaporating soft-state).
pub const TIMING_INTENT_KEY: &str = "sys/govern/timing";
const TIMING_PREFIX: &str = "sys/govern/timing";
/// Intent freshness window. Beyond this a `TimingIntent` evaporates and non-pinned nodes self-heal
/// to their static baseline.
pub const TIMING_INTENT_TTL_MS: u64 = 30_000;

/// A cluster-wide timing intent. `0` for a field means "do not govern this field" (leave the node's
/// own value). `target = None` ⇒ whole fleet; `Some(node)` ⇒ only that node applies it.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TimingIntent {
    pub health_check_interval_secs: u64,
    pub reconnect_backoff_secs: u64,
    #[serde(default)]
    pub target: Option<NodeId>,
    #[serde(default)]
    pub written_at_ms: u64,
}

impl super::intent::FleetIntent for TimingIntent {
    fn written_at_ms(&self) -> u64 { self.written_at_ms }
    fn stamp(&mut self, now_ms: u64) { self.written_at_ms = now_ms; }
    fn target(&self) -> Option<&NodeId> { self.target.as_ref() }
}

/// Apply a fresh fleet intent to this node — **unless** timing is locally pinned (local-wins). Only
/// non-zero fields are governed.
fn apply(ctx: &Arc<TaskCtx>, intent: &TimingIntent) {
    if ctx.hot.timing_locally_pinned.load(Ordering::Relaxed) {
        return; // local override is sovereign
    }
    if intent.health_check_interval_secs > 0 {
        ctx.hot.health_check_interval_secs.store(intent.health_check_interval_secs, Ordering::Relaxed);
    }
    if intent.reconnect_backoff_secs > 0 {
        ctx.hot.reconnect_backoff_secs.store(intent.reconnect_backoff_secs, Ordering::Relaxed);
    }
}

/// Revert fleet-applied timing to the static baseline (`0` = use config) — unless locally pinned.
/// Called when the intent evaporates / is not for this node, so timing self-heals if governance dies.
fn revert(ctx: &Arc<TaskCtx>) {
    if ctx.hot.timing_locally_pinned.load(Ordering::Relaxed) {
        return;
    }
    ctx.hot.health_check_interval_secs.store(0, Ordering::Relaxed);
    ctx.hot.reconnect_backoff_secs.store(0, Ordering::Relaxed);
}

/// Spawn the timing-intent reconcile loop (reconcile on change + on a TTL/2 tick). Started at
/// `agent.start()` regardless of config — it is inert until a `TimingIntent` is published, and an
/// evaporated intent self-heals via `revert`.
pub fn spawn_timing_reconciler(ctx: &Arc<TaskCtx>) {
    let apply_ctx = Arc::clone(ctx);
    let revert_ctx = Arc::clone(ctx);
    super::intent::spawn_intent_reconciler::<TimingIntent, _, _>(
        ctx,
        TIMING_PREFIX,
        TIMING_INTENT_KEY,
        TIMING_INTENT_TTL_MS,
        move |i| apply(&apply_ctx, i),
        move || revert(&revert_ctx),
    );
}

/// Publish a `TimingIntent` to the fleet (or one `target` node). Intent, not command — any node may
/// publish; every node reconciles. Returns whether the write was queued.
pub fn publish_timing_intent(ctx: &Arc<TaskCtx>, intent: TimingIntent) -> bool {
    let kv = KvHandle::from_core(Arc::clone(&ctx.core));
    super::intent::publish_intent(&kv, TIMING_INTENT_KEY, intent)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent::intent::FleetIntent;

    #[test]
    fn timing_intent_round_trips_and_stamps() {
        let mut i = TimingIntent {
            health_check_interval_secs: 3,
            reconnect_backoff_secs: 7,
            target: None,
            written_at_ms: 0,
        };
        i.stamp(123);
        assert_eq!(i.written_at_ms(), 123);
        assert!(i.target().is_none());
        let bytes = mycelium_core::serde_fixint::to_vec(&i).unwrap();
        let back: TimingIntent = mycelium_core::serde_fixint::from_slice(&bytes).unwrap();
        assert_eq!(back, i);
    }
}
