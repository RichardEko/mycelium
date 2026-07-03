use crate::store::intern_pool_len;
use std::sync::{atomic::Ordering, Arc};

use super::{AgentState, GossipAgent, SystemStats};

// The generic soft-state advertisement loop (`run_kv_persist_task`) and its
// closure type aliases moved to `mycelium-core::kv_persist` in v2 M3 — it is pure
// Layer I (writes KV + gossips) and now drives `CoreCtx::soft_state_advertised`.
// Re-exported here so the upper call sites' `super::kv::…` paths are unchanged.
pub(crate) use mycelium_core::kv_persist::{run_kv_persist_task, PersistPayloadFn};

impl GossipAgent {
    /// Returns this node's identifier.
    pub fn node_id(&self) -> &crate::node_id::NodeId {
        &self.node_id
    }

    /// Returns the node's **resolved** configuration — the value after WS-C M8
    /// startup auto-derivation (`GossipConfig::derive_unset`) has filled any `0`
    /// "auto" tuning fields from the cluster-size estimate. Use it to inspect the
    /// effective tuning a node is actually running with (e.g. after `GossipConfig::auto()`).
    pub fn config(&self) -> &crate::config::GossipConfig {
        &self.config
    }

    /// The optional operator-set cluster / environment name (`GossipConfig::cluster_name`), if any.
    /// A label for disambiguating multiple environments — surfaced on `/stats`, as a `/metrics`
    /// `cluster` label, and in AgentFacts. No effect on gossip or identity.
    pub fn cluster_name(&self) -> Option<&str> {
        self.config.cluster_name.as_deref()
    }

    // ── WS-C M9: live ("hot") retuning ───────────────────────────────────────
    // The node-local application point: operators call these directly; the
    // `ClusterTuner` advisor routes its recommendations through them too. Each
    // takes effect immediately (sampled per use / per spawn), no task restart.
    // Inspect current values via `config()` is the *startup* snapshot; these are
    // the live overrides — read back with the matching `hot_*` getter.

    /// Live-set the per-peer inbound frame-rate cap (`0` = unlimited). Sampled per
    /// inbound frame, so every open connection picks it up on its next frame.
    pub fn set_max_inbound_frames_per_sec(&self, fps: u64) {
        self.task_ctx.hot.max_inbound_frames_per_sec
            .store(fps, std::sync::atomic::Ordering::Relaxed);
    }
    /// Live-set the depth for *new* per-peer writer channels (clamped to ≥ 1).
    /// Existing writers keep their channel; new / reconnecting peers use the new depth.
    pub fn set_writer_channel_depth(&self, depth: usize) {
        self.task_ctx.hot.writer_channel_depth
            .store(depth.max(1), std::sync::atomic::Ordering::Relaxed);
    }
    /// Live-set the concurrent bulk-handler cap (`0` = unlimited). Sampled per bulk admission.
    pub fn set_max_concurrent_bulk_handlers(&self, n: usize) {
        self.task_ctx.hot.max_concurrent_bulk_handlers
            .store(n, std::sync::atomic::Ordering::Relaxed);
    }
    /// Live-set the **health-check interval** (secs, WS-C / M10). The health monitor re-reads it each
    /// cycle and retunes its cadence on the next tick — no task restart. `0` ⇒ revert to the static
    /// config value. A node-local set is **sovereign**: it pins timing so a cluster `TimingIntent`
    /// no longer overrides this node (local-wins).
    pub fn set_health_check_interval_secs(&self, secs: u64) {
        self.task_ctx.hot.health_check_interval_secs
            .store(secs, std::sync::atomic::Ordering::Relaxed);
        self.task_ctx.hot.timing_locally_pinned.store(true, std::sync::atomic::Ordering::Relaxed);
    }
    /// Live-set the **reconnect backoff** (secs, WS-C / M10). `0` ⇒ revert to the static config value.
    /// Pins timing locally (local-wins over fleet governance).
    pub fn set_reconnect_backoff_secs(&self, secs: u64) {
        self.task_ctx.hot.reconnect_backoff_secs
            .store(secs, std::sync::atomic::Ordering::Relaxed);
        self.task_ctx.hot.timing_locally_pinned.store(true, std::sync::atomic::Ordering::Relaxed);
    }
    /// Current live timing values (WS-C / M10), as `(health_check_interval_secs, reconnect_backoff_secs)`.
    /// `0` for a field means "using the static config value".
    pub fn timing_tunables(&self) -> (u64, u64) {
        (self.task_ctx.hot.health_check_interval_secs.load(std::sync::atomic::Ordering::Relaxed),
         self.task_ctx.hot.reconnect_backoff_secs.load(std::sync::atomic::Ordering::Relaxed))
    }

    /// **Govern timing cluster-wide** (WS-C / M10.2): publish an evaporating `TimingIntent` that every
    /// node reconciles toward — newest-wins, **local-wins** (a node that called a `set_*` setter is
    /// pinned and ignores the intent), self-healing on evaporation. `0` for a field leaves it
    /// ungoverned; `target = None` ⇒ whole fleet, `Some(node)` ⇒ just that node. Intent, never
    /// command — and **no consensus fence** (see `timing_governor` docs). Returns whether queued.
    pub fn govern_timing(
        &self,
        health_check_interval_secs: u64,
        reconnect_backoff_secs: u64,
        target: Option<crate::node_id::NodeId>,
    ) -> bool {
        super::timing_governor::publish_timing_intent(
            &self.task_ctx,
            super::timing_governor::TimingIntent {
                health_check_interval_secs,
                reconnect_backoff_secs,
                target,
                written_at_ms: 0,
            },
        )
    }

    /// Current live values of the hot-tunable subset (post-M9 overrides), as
    /// `(max_inbound_frames_per_sec, writer_channel_depth, max_concurrent_bulk_handlers)`.
    pub fn hot_tunables(&self) -> (u64, usize, usize) {
        (self.task_ctx.hot.inbound_fps(),
         self.task_ctx.hot.writer_depth(),
         self.task_ctx.hot.bulk_handlers())
    }

    /// Returns a snapshot of all currently live peer `NodeId`s.
    ///
    /// Useful at Layer 3 when a direct connection (e.g. HTTP) must be opened to
    /// a specific peer. The list reflects the peers table at the moment of the call;
    /// it may be stale by the time it is acted on — treat it as advisory.
    pub fn peers(&self) -> Vec<crate::node_id::NodeId> {
        self.peers.pin().iter().map(|(k, _)| k.clone()).collect()
    }

    /// Returns the groups this node has currently joined.
    ///
    /// Reflects the local [`Boundary`] state at the moment of the call. Useful for
    /// diagnostics and Layer 3 routing decisions that depend on group membership.
    pub fn groups(&self) -> Vec<Arc<str>> {
        self.task_ctx.signal_boundary.read().groups.iter().cloned().collect()
    }

    /// Returns per-peer cumulative drop counts (only peers with at least one drop).
    ///
    /// Each entry is the total number of gossip frames dropped to that peer due to
    /// reconnect backoff since the peer writer was last spawned. Useful for identifying
    /// slow or unreachable peers that inflate the global `dropped_frames` counter.
    pub fn peer_drop_counts(&self) -> Vec<(crate::node_id::NodeId, u64)> {
        use std::sync::atomic::Ordering;
        self.peer_writers.pin()
            .iter()
            .map(|(k, v)| (k.clone(), v.dropped.load(Ordering::Relaxed)))
            .filter(|(_, n)| *n > 0)
            .collect()
    }

    /// Returns `true` once the first soft-state advertisement tick has fired
    /// after startup or restart.
    ///
    /// Hard state (WAL replay) completes before `start()` returns, so
    /// `kv().get`/`kv().scan_prefix` are accurate immediately. Soft state — capability
    /// keys, locality, and other periodically re-advertised keys — is only
    /// written after the first advertisement tick. Use this to implement a
    /// readiness probe that distinguishes "process up" from "fully hydrated."
    ///
    /// Returns `false` until the first call to `advertise_capability`,
    /// `advertise_locality`, or any other `run_kv_persist_task`-driven
    /// advertisement has completed its initial tick.
    pub fn is_ready(&self) -> bool {
        self.task_ctx.soft_state_advertised.load(std::sync::atomic::Ordering::Acquire)
    }

    /// Returns a snapshot of live protocol state.
    ///
    /// Note: `dead_shards` may transiently report all shards as dead in the brief
    /// window between `start()` returning and the shard tasks being scheduled by
    /// the tokio runtime. This is normal and resolves on the next call.
    pub fn system_stats(&self) -> SystemStats {
        let running = AgentState::from_u8(self.state.load(Ordering::Relaxed)) == AgentState::Running;
        let gossip_shard_queue_depths: Vec<usize> = self.task_ctx.gossip_txs.iter()
            .map(|tx| tx.max_capacity() - tx.capacity())
            .collect();
        let dead_shards = if running {
            self.shard_alive.iter()
                .filter(|a| !a.load(Ordering::Relaxed))
                .count()
        } else {
            0
        };
        SystemStats {
            peers: self.peers.len(),
            store_entries: if running {
                self.live_entries.load(Ordering::Relaxed)
            } else {
                self.kv_state.store.pin().iter().filter(|(_, v)| v.data.is_some()).count()
            },
            cached_connections: self.peer_writers.pin()
                .iter()
                .filter(|(_, e)| e.is_live())
                .count(),
            gossip_shard_queue_depths,
            dead_shards,
            gc_alive:             !running || self.gc_alive.load(Ordering::Relaxed),
            health_monitor_alive: !running || self.health_monitor_alive.load(Ordering::Relaxed),
            intern_pool_size:     intern_pool_len(),
            dropped_frames:       self.kv_state.dropped_frames.load(Ordering::Relaxed),
            individual_flood_fallbacks:
                self.kv_state.individual_flood_fallbacks.load(Ordering::Relaxed),
            task_count:           self.task_handles_lock().len(),
            active_bulk_handlers: self.task_ctx.bulk_transport.active_handlers.load(Ordering::Relaxed),
            commit_conflicts:     self.task_ctx.commit_conflicts.load(Ordering::Relaxed),
            sys_namespace_violations:
                self.task_ctx.sys_namespace_violations.load(Ordering::Relaxed),
            cap_authz_violations: self.task_ctx.cap_authz_violations.load(Ordering::Relaxed),
            schema_mismatch: self.task_ctx.schema_mismatch.load(Ordering::Relaxed),
            rate_limited_senders: mycelium_core::rate::throttled_sender_count(&self.task_ctx.core),
        }
    }

    /// The Legible-Emergence **fleet snapshot** — the relational "localize" view computed locally
    /// from the gossiped KV this node already holds (no collector; any node answers it). Governed-
    /// group status, capability-coverage gaps, fleet opacity, the throttle graph, and the RT1/RT2
    /// [`ViewConfidence`](super::emergent::ViewConfidence) that qualifies it. Available whether or
    /// not the detector loop runs (the flap/oscillation counters read 0 when it does not).
    pub fn fleet_snapshot(&self) -> super::emergent::FleetSnapshot {
        super::emergent::compute_fleet_snapshot(&self.task_ctx)
    }

    /// The Legible-Emergence **fleet diagnosis** — the "why is the fleet in this state" narrative:
    /// a rule engine over [`fleet_snapshot`](Self::fleet_snapshot) that names each detected pathology
    /// in code-free, actionable terms, most-severe first, qualified by this observer's own view
    /// health. Diagnostics *as data* — the same content the `GET /gateway/diagnose` endpoint serves.
    pub fn fleet_diagnosis(&self) -> super::emergent::FleetDiagnosis {
        super::emergent::compute_fleet_diagnosis(&self.task_ctx)
    }
}

#[cfg(test)]
mod tests {
    use crate::{GossipAgent, GossipConfig, NodeId};

    fn make_agent() -> GossipAgent {
        GossipAgent::new(NodeId::new("127.0.0.1", 0).unwrap(), GossipConfig::default())
    }

    #[test]
    fn create_agent() {
        let agent = make_agent();
        assert_eq!(agent.node_id(), &NodeId::new("127.0.0.1", 0).unwrap());
    }

    #[tokio::test]
    async fn system_stats_reflect_state() {
        let agent = make_agent();
        let _ = agent.kv().set("a", b"1".to_vec());
        let _ = agent.kv().set("b", b"2".to_vec());
        let _ = agent.kv().delete("b");
        let stats = agent.system_stats();
        assert_eq!(stats.peers, 0);
        assert_eq!(stats.store_entries, 1);
        assert_eq!(stats.cached_connections, 0);
    }

    #[test]
    fn gossip_channel_capacity_used_by_agent() {
        let mut cfg = GossipConfig::default();
        cfg.gossip_channel_capacity = 1;
        let agent = GossipAgent::new(NodeId::new("127.0.0.1", 0).unwrap(), cfg);
        assert!(agent.kv().set("k1", b"v1".to_vec()), "first send fits in capacity-1 shard");
        assert!(!agent.kv().set("k1", b"v2".to_vec()), "second send to same shard should fail");
    }
}
