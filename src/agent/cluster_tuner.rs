//! WS-C M9: `ClusterTuner` — a decentralized, coordinator-free config advisor.
//!
//! Two opt-in pieces, both regular agents over existing primitives (no new mechanism):
//!
//! - **Advisor** ([`GossipAgent::start_cluster_tuner`]): every `interval`, observe N
//!   (peers + self), recompute the M8 size formula for the hot-tunable params, and — only
//!   if it differs — gossip the recommendation to a shared `sys/config/{param}` KV key.
//! - **Applier** ([`GossipAgent::start_config_applier`]): subscribe to `sys/config/`; on
//!   each change run a node-local [`ConfigPolicy`] over every recommendation and apply the
//!   accepted value via the matching live setter.
//!
//! The advisor *advises*, the node *decides* (Core Principle 1). There is no config
//! coordinator: every node computes the same formula and converges via LWW, and any node
//! may clamp, ignore, or override any recommendation through its policy. `sys/config/` is
//! deliberately *not* in the `sys/` self-owned tripwire set, so multi-node writes are
//! legitimate (like `sys/quorum/` / `sys/audit/`).

use crate::agent::{GossipAgent, TaskCtx};
use mycelium_core::config::GossipConfig;
use mycelium_core::kv_handle::KvHandle;
use std::sync::{atomic::Ordering, Arc};
use std::time::Duration;
use tracing::{debug, warn};

/// The shared advisory-config KV namespace.
pub const CONFIG_PREFIX: &str = "sys/config/";

/// Param key for the writer-channel depth recommendation (the one M8 derives from N).
const K_WRITER_DEPTH: &str = "writer_channel_depth";

/// Node-local decision policy (WS-C M9). Given `(param, recommended_value)`, return the
/// value the node will apply, or `None` to reject. This is where "the node decides":
/// accept, clamp, or veto. `Send + Sync` so it runs in the applier task.
pub type ConfigPolicy = Arc<dyn Fn(&str, u64) -> Option<u64> + Send + Sync>;

/// Accept every recommendation as-is.
pub fn accept_all() -> ConfigPolicy {
    Arc::new(|_param, v| Some(v))
}
/// Subscribe but never apply (observe-only).
pub fn reject_all() -> ConfigPolicy {
    Arc::new(|_param, _v| None)
}
/// Accept, but clamp every recommendation into `[min, max]`.
pub fn clamped(min: u64, max: u64) -> ConfigPolicy {
    Arc::new(move |_param, v| Some(v.clamp(min, max)))
}

#[inline]
fn decode_u64(b: &[u8]) -> Option<u64> {
    b.get(..8).map(|s| u64::from_le_bytes(s.try_into().expect("slice is 8 bytes")))
}

/// Scan `sys/config/`, gate each recommendation through the M9 governor (enable/bounds/
/// ratchet) then the custom `policy`, and apply the accepted values to this node's hot cell.
fn apply_all(kv: &KvHandle, ctx: &Arc<TaskCtx>, policy: &ConfigPolicy) {
    use super::tuning_governor::HotParam;
    for (key, val) in kv.scan_prefix(CONFIG_PREFIX) {
        let Some(param) = key.strip_prefix(CONFIG_PREFIX) else { continue };
        let Some(rec) = decode_u64(&val) else { continue };
        // Map the recommendation to a governor param + current applied value.
        let (hot_param, cur) = match param {
            K_WRITER_DEPTH => (HotParam::WriterDepth, ctx.hot.writer_depth() as u64),
            other => { warn!(param = other, "ClusterTuner: unknown sys/config param, ignoring"); continue; }
        };
        // Governor gate first (management intent: disabled / floor / ceiling / ratchet)…
        let Some(gated) = ctx.tuning_governor.gate(hot_param, rec, cur) else {
            debug!(param, rec, "ClusterTuner: governor blocked recommendation (disabled/bounded)");
            continue;
        };
        // …then the custom ConfigPolicy escape hatch.
        let Some(accepted) = policy(param, gated) else {
            debug!(param, gated, "ClusterTuner: policy rejected recommendation");
            continue;
        };
        match hot_param {
            HotParam::WriterDepth =>
                ctx.hot.writer_channel_depth.store((accepted as usize).max(1), Ordering::Relaxed),
            HotParam::InboundFps =>
                ctx.hot.max_inbound_frames_per_sec.store(accepted, Ordering::Relaxed),
            HotParam::BulkHandlers =>
                ctx.hot.max_concurrent_bulk_handlers.store(accepted as usize, Ordering::Relaxed),
        }
        debug!(param, value = accepted, "ClusterTuner: applied recommendation");
    }
}

impl GossipAgent {
    /// Opt this node in to **applying** gossiped `sys/config/` recommendations (WS-C M9).
    ///
    /// Spawns a task that subscribes to `sys/config/` and, on each change, runs `policy`
    /// over every recommendation and applies the accepted values live (no task restart).
    /// Run this on every node you want auto-tuned; pair with [`start_cluster_tuner`] on at
    /// least one node to produce the recommendations. The task exits on shutdown.
    ///
    /// [`start_cluster_tuner`]: Self::start_cluster_tuner
    pub fn start_config_applier(&self, policy: ConfigPolicy) {
        let ctx = Arc::clone(&self.task_ctx);
        let kv = KvHandle::from_core(Arc::clone(&self.task_ctx.core));
        let mut shutdown = self.task_ctx.shutdown_tx.subscribe();
        self.task_ctx.spawn_task(async move {
            let mut rx = kv.subscribe_prefix(CONFIG_PREFIX);
            loop {
                apply_all(&kv, &ctx, &policy);
                tokio::select! {
                    r = rx.changed() => { if r.is_err() { break; } }
                    _ = shutdown.wait_for(|v| *v) => break,
                }
            }
        });
    }

    /// Opt this node in to **advising** the cluster *and* applying recommendations (WS-C M9).
    ///
    /// Spawns the advisor loop (every `interval`: recompute the M8 size formula from the
    /// live peer count and gossip it to `sys/config/{param}` when it changed) and also calls
    /// [`start_config_applier`](Self::start_config_applier) with `policy`. Running it on one
    /// node advises the cluster; running it on every node makes them all advise (converging
    /// via LWW) and apply. Both tasks exit on shutdown.
    pub fn start_cluster_tuner(&self, interval: Duration, policy: ConfigPolicy) {
        let core = Arc::clone(&self.task_ctx.core);
        let kv = KvHandle::from_core(Arc::clone(&core));
        let mut shutdown = self.task_ctx.shutdown_tx.subscribe();
        self.task_ctx.spawn_task(async move {
            let mut tick = tokio::time::interval(interval);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    _ = tick.tick() => {}
                    _ = shutdown.wait_for(|v| *v) => break,
                }
                // N = known peers + self (a live lower bound, same basis as M8 startup).
                let n = core.peers.pin().len() + 1;
                let rec = GossipConfig::auto_writer_channel_depth(n) as u64;
                let key = format!("{CONFIG_PREFIX}{K_WRITER_DEPTH}");
                // Write only when it changed — converged clusters produce no churn.
                if kv.get(&key).and_then(|b| decode_u64(&b)) != Some(rec) {
                    debug!(n, rec, "ClusterTuner: advertising writer_channel_depth recommendation");
                    let _ = kv.set(key.as_str(), rec.to_le_bytes().to_vec());
                }
            }
        });
        self.start_config_applier(policy);
    }
}
