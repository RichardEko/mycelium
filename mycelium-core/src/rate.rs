//! Cluster-wide distributed rate-limiting (WS-C / M7) — **shared observation, local decision**.
//!
//! A misbehaving sender that connects to many peers at once can stay under each receiver's *per-peer*
//! `max_inbound_frames_per_sec` while flooding the network in aggregate. M7 closes that gap without a
//! coordinator: each node publishes its observed per-peer inbound rate as *shared evidence*, and each
//! node independently *decides* to tighten its own budget for a sender whose **aggregate** (summed
//! across all observers) rate is abusive. A sustained abuser ends up throttled by every node it
//! touches, with no global decision round and no cluster-wide eviction verdict (CP1/CP4/CP5).
//!
//! - **Observe + publish** — the connection read loop ([`connection`](crate::connection)) writes its
//!   observed per-peer rate to `sys/rate/{observer}/{sender}` once per 1-second window (short-TTL
//!   evaporating soft-state). Only when `rate_observation_enabled`.
//! - **Decide** — [`run_rate_decider`] sums `sys/rate/*/{sender}` per sender; a sender over
//!   `rate_aggregate_threshold_fps` gets a **fair-share** local budget (`threshold ÷ observers`)
//!   written into [`CoreCtx::rate_throttle`], so each observer clamps the sender independently and the
//!   aggregate converges back to the threshold. Senders below threshold (or whose evidence
//!   evaporated) are cleared.
//! - **Enforce** — the connection loop clamps a sender's effective inbound limit to
//!   `min(global, throttle)`.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::Bytes;

use crate::context::CoreCtx;
use crate::ops::{kv_scan_prefix, kv_set};

/// KV namespace carrying per-observer rate evidence: `sys/rate/{observer}/{sender}` → fps (ASCII u64).
pub const RATE_PREFIX: &str = "sys/rate/";

/// The default aggregate threshold when `rate_aggregate_threshold_fps` is left `0`.
fn effective_threshold(ctx: &CoreCtx) -> u64 {
    let cfg = ctx.config.rate_aggregate_threshold_fps;
    if cfg > 0 {
        return cfg;
    }
    let per_peer = ctx.config.max_inbound_frames_per_sec;
    if per_peer > 0 { per_peer.saturating_mul(8) } else { 8_000 }
}

/// Publish this node's observed `fps` for `sender` (the immediate peer) as shared evidence. Called
/// once per 1-second window from the connection loop; a no-op-cheap KV write that evaporates.
pub fn publish_observation(ctx: &CoreCtx, sender: &str, fps: u64) {
    let key: Arc<str> = Arc::from(format!("{RATE_PREFIX}{}/{sender}", ctx.node_id));
    kv_set(ctx, key, Bytes::from(fps.to_string()));
}

/// The locally-decided throttle budget (fps) for `sender`, or `0` if none (not abusive). Read by the
/// connection loop once per window — a single lock-free map lookup.
pub fn throttle_for(ctx: &CoreCtx, sender: &str) -> u64 {
    ctx.rate_throttle.pin().get(sender).map(|a| a.load(Ordering::Relaxed)).unwrap_or(0)
}

/// Count of senders this node is currently throttling (for `system_stats()`).
pub fn throttled_sender_count(ctx: &CoreCtx) -> u64 {
    ctx.rate_throttle.pin().len() as u64
}

/// The decider loop: periodically aggregate `sys/rate/` evidence and reconcile the local throttle map.
/// Spawned only when `rate_observation_enabled`.
pub async fn run_rate_decider(ctx: Arc<CoreCtx>, mut shutdown: tokio::sync::watch::Receiver<bool>) {
    let mut tick = tokio::time::interval(Duration::from_secs(2));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = tick.tick() => decide_once(&ctx),
            _ = shutdown.wait_for(|v| *v) => break,
        }
    }
}

/// One aggregate-and-reconcile pass: scan the live evidence and update `ctx.rate_throttle`.
fn decide_once(ctx: &CoreCtx) {
    reconcile_throttle(kv_scan_prefix(ctx, RATE_PREFIX), effective_threshold(ctx), &ctx.rate_throttle);
}

/// Pure aggregation + reconciliation (no I/O — unit-tested directly). Sums the per-`(observer, sender)`
/// evidence into a per-sender aggregate + observer count, sets a fair-share throttle
/// (`threshold ÷ observers`) for over-threshold senders, and clears the rest.
fn reconcile_throttle(
    evidence: Vec<(Arc<str>, Bytes)>,
    threshold: u64,
    throttle: &papaya::HashMap<Arc<str>, AtomicU64>,
) {
    let mut total: HashMap<Arc<str>, u64> = HashMap::new();
    let mut observers: HashMap<Arc<str>, u64> = HashMap::new();
    for (key, val) in evidence {
        // key = sys/rate/{observer}/{sender}
        let Some(sender) = key.rsplit('/').next() else { continue };
        if sender.is_empty() {
            continue;
        }
        let fps: u64 = std::str::from_utf8(&val).ok().and_then(|s| s.parse().ok()).unwrap_or(0);
        let s: Arc<str> = Arc::from(sender);
        *total.entry(Arc::clone(&s)).or_default() += fps;
        *observers.entry(s).or_default() += 1;
    }

    let guard = throttle.pin();
    for (sender, agg) in &total {
        if *agg > threshold {
            let obs = (*observers.get(sender).unwrap_or(&1)).max(1);
            let fair_share = (threshold / obs).max(1);
            match guard.get(sender) {
                Some(a) => a.store(fair_share, Ordering::Relaxed),
                None => { guard.insert(Arc::clone(sender), AtomicU64::new(fair_share)); }
            }
        }
    }
    // Clear throttles for senders no longer over threshold (evidence evaporated or rate fell).
    let stale: Vec<Arc<str>> = guard
        .keys()
        .filter(|s| total.get(*s).is_none_or(|agg| *agg <= threshold))
        .cloned()
        .collect();
    for s in stale {
        guard.remove(&s);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ev(observer: &str, sender: &str, fps: &str) -> (Arc<str>, Bytes) {
        (Arc::from(format!("sys/rate/{observer}/{sender}")), Bytes::from(fps.to_string()))
    }
    fn get(map: &papaya::HashMap<Arc<str>, AtomicU64>, sender: &str) -> u64 {
        map.pin().get(sender).map(|a| a.load(Ordering::Relaxed)).unwrap_or(0)
    }

    #[test]
    fn aggregate_over_threshold_throttles_with_fair_share() {
        let map = papaya::HashMap::new();
        let evidence = vec![
            // Three observers each see "noisy" at 500 fps → aggregate 1500 > 900.
            ev("127.0.0.1:1", "noisy", "500"),
            ev("127.0.0.1:2", "noisy", "500"),
            ev("127.0.0.1:3", "noisy", "500"),
            // A well-behaved sender seen once at 100 fps → aggregate 100 < 900.
            ev("127.0.0.1:1", "calm", "100"),
        ];
        reconcile_throttle(evidence, 900, &map);
        // Fair-share budget = 900 / 3 observers = 300; the calm sender is untouched.
        assert_eq!(get(&map, "noisy"), 300);
        assert_eq!(get(&map, "calm"), 0);
        assert_eq!(map.pin().len(), 1);
    }

    #[test]
    fn throttle_clears_when_rate_falls() {
        let map = papaya::HashMap::new();
        reconcile_throttle(vec![ev("127.0.0.1:1", "s", "1000")], 900, &map);
        assert!(get(&map, "s") > 0, "throttled while abusive");
        // Evidence drops below threshold; cleared on the next pass.
        reconcile_throttle(vec![ev("127.0.0.1:1", "s", "10")], 900, &map);
        assert_eq!(get(&map, "s"), 0, "throttle released when no longer abusive");
    }

    // Analysis Run 27 falsification probe (Concurrency/Semantic): the decider must be idempotent
    // (re-running on identical evidence does not drift the throttle) and boundary-correct (a sender
    // *exactly at* the threshold is NOT throttled — strict `>`, no off-by-one).
    #[test]
    fn reconcile_is_idempotent_and_threshold_is_strict() {
        let map = papaya::HashMap::new();
        let evidence = || vec![ev("o1", "s", "600"), ev("o2", "s", "600")]; // aggregate 1200 > 900
        reconcile_throttle(evidence(), 900, &map);
        let first = get(&map, "s");
        reconcile_throttle(evidence(), 900, &map); // re-run, same evidence
        assert_eq!(get(&map, "s"), first, "idempotent: re-running does not drift the throttle");
        assert_eq!(map.pin().len(), 1, "no duplicate throttle entries accumulate");

        // A sender whose aggregate is EXACTLY the threshold is not throttled (strict `>`).
        let map2 = papaya::HashMap::new();
        reconcile_throttle(vec![ev("o1", "s", "900")], 900, &map2); // aggregate == threshold
        assert_eq!(get(&map2, "s"), 0, "exactly-at-threshold is not throttled (no off-by-one)");
    }

    #[test]
    fn evaporated_evidence_clears_throttle() {
        let map = papaya::HashMap::new();
        reconcile_throttle(vec![ev("127.0.0.1:1", "s", "1000")], 900, &map);
        assert!(get(&map, "s") > 0);
        // No evidence at all (all observers' entries evaporated) → throttle cleared.
        reconcile_throttle(vec![], 900, &map);
        assert_eq!(get(&map, "s"), 0);
    }
}
