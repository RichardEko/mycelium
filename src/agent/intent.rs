//! Shared *transport* for the "management = intent + local reconcile" pattern
//! (elastic-sizing Track 1; see `docs/plans/elastic-sizing-intent-governed.md` and project
//! memory *management-as-intent*).
//!
//! This is the reusable plumbing **only**: publish + gossip + freshness/evaporation +
//! node-targeting + the subscribe/tick reconcile loop. The *policy* — what an intent means,
//! how to apply it, local-pin / local-wins — stays **bespoke per governor**. Deliberately a
//! few free functions over a trait, **not** a generic `IntentGovernor<T>`: the decision
//! semantics diverge too much across governors (the tuning scalar vs. membership's collective
//! self-election), so we let the Rule of Three pull any richer abstraction later.
//!
//! Lives in the **agent layer**, composing `mycelium-core`'s KV subscribe + the read-side
//! freshness convention — agency above, mechanism in core (Core Principles 1 & 3).

use crate::agent::TaskCtx;
use crate::node_id::NodeId;
use mycelium_core::kv_handle::KvHandle;
use std::sync::Arc;
use std::time::Duration;

/// A gossiped, evaporating, optionally node-targeted governance intent. The transport reads
/// these three facets generically; everything else about the intent is the governor's business.
pub trait FleetIntent:
    serde::Serialize + serde::de::DeserializeOwned + Send + Sync + 'static
{
    /// Unix-ms the intent was published. Used for evaporation.
    fn written_at_ms(&self) -> u64;
    /// Stamp the publish time (called by [`publish_intent`]).
    fn stamp(&mut self, now_ms: u64);
    /// `None` = whole fleet; `Some(node)` = only that node applies it. Lets per-node governance
    /// ride the gossip path (reaching even headless nodes) instead of needing that node's HTTP.
    fn target(&self) -> Option<&NodeId>;
}

/// Unix-ms now (saturating to 0 before the epoch).
pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Publish `intent` (stamped now) to `key`, gossiped via the KV substrate. Any entity with a
/// concern may call this — it is intent, not command. Returns whether the write was queued.
pub fn publish_intent<I: FleetIntent>(kv: &KvHandle, key: &str, mut intent: I) -> bool {
    intent.stamp(now_ms());
    match mycelium_core::serde_fixint::to_vec(&intent) {
        Ok(bytes) => kv.set(key, bytes),
        Err(_) => false,
    }
}

/// Read `key`; if a **fresh** intent **targeted at `me`** (or the whole fleet) is present, call
/// `apply`; otherwise call `revert` (absent / evaporated / not-for-me → self-heal to local
/// derivation). Shared by the subscribe and periodic-tick paths.
pub fn reconcile_intent<I: FleetIntent>(
    kv: &KvHandle,
    key: &str,
    me: &NodeId,
    ttl_ms: u64,
    apply: impl FnOnce(&I),
    revert: impl FnOnce(),
) {
    match kv.get(key).and_then(|b| mycelium_core::serde_fixint::from_slice::<I>(&b).ok()) {
        Some(i)
            if now_ms().saturating_sub(i.written_at_ms()) <= ttl_ms
                && i.target().is_none_or(|t| t == me) =>
        {
            apply(&i)
        }
        _ => revert(),
    }
}

/// Spawn the reconcile loop for one fleet-intent `key`: reconcile on every change under
/// `prefix` **and** on a periodic tick (so an evaporated intent reverts even with no new
/// write). `apply` / `revert` carry the governor's policy. Exits on shutdown.
pub fn spawn_intent_reconciler<I, A, R>(
    ctx: &Arc<TaskCtx>,
    prefix: &'static str,
    key: &'static str,
    ttl_ms: u64,
    apply: A,
    revert: R,
) where
    I: FleetIntent,
    A: Fn(&I) + Send + 'static,
    R: Fn() + Send + 'static,
{
    let kv = KvHandle::from_core(Arc::clone(&ctx.core));
    let me = ctx.node_id.clone();
    let mut shutdown = ctx.shutdown_tx.subscribe();
    ctx.spawn_task(async move {
        let mut rx = kv.subscribe_prefix(prefix);
        // Tick at TTL/2 so an evaporated intent is noticed within the window.
        let mut tick = tokio::time::interval(Duration::from_millis((ttl_ms / 2).max(1)));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            reconcile_intent::<I>(&kv, key, &me, ttl_ms, &apply, &revert);
            tokio::select! {
                r = rx.changed() => { if r.is_err() { break; } }
                _ = tick.tick() => {}
                _ = shutdown.wait_for(|v| *v) => break,
            }
        }
    });
}
