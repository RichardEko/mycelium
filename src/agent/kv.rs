use crate::framing::{dispatch_gossip_send, dispatch_gossip_try_send, ForwardHint, WireMessage};
use crate::store::{apply_and_notify, intern_pool_len};
use bytes::Bytes;
use std::{
    sync::{atomic::Ordering, Arc},
    time::Duration,
};
use tokio::{sync::watch, time};

use super::{AgentState, GossipAgent, SystemStats};

/// Closure that produces the payload bytes for one tick of `run_kv_persist_task`.
pub(crate) type PersistPayloadFn = Arc<dyn Fn() -> Bytes + Send + Sync>;
/// Optional per-tick side-effect (e.g. signal emission) invoked before the KV write.
pub(crate) type PersistOnTickFn  = Arc<dyn Fn(&Arc<super::TaskCtx>, &Bytes) + Send + Sync>;

impl GossipAgent {
    /// Returns this node's identifier.
    pub fn node_id(&self) -> &crate::node_id::NodeId {
        &self.node_id
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
        self.task_ctx.caps_advertised.load(std::sync::atomic::Ordering::Acquire)
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
            task_count:           self.task_handles_lock().len(),
        }
    }
}

/// Shared persist-loop primitive: ticks at `interval` and writes `payload_fn()`
/// to `kv_key` (Layer I) plus gossips it. Optional `on_tick` runs synchronously
/// before the KV write — used by [`GossipAgent::advertise_persistent`] to emit a
/// matching signal, and by capability ops to do nothing.
///
/// Tombstones `kv_key` at exit (cancel, shutdown, or sender drop), awaiting
/// channel capacity so the retraction is never silently dropped.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn run_kv_persist_task(
    ctx:             Arc<super::TaskCtx>,
    mut cancel_rx:   tokio::sync::oneshot::Receiver<()>,
    mut shutdown_rx: watch::Receiver<bool>,
    kv_key:          Arc<str>,
    interval:        Duration,
    payload_fn:      PersistPayloadFn,
    on_tick:         Option<PersistOnTickFn>,
) {
    let mut ticker = time::interval(interval);
    ticker.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
    let mut first_tick = true;
    loop {
        tokio::select! { biased;
            _ = &mut cancel_rx               => break,
            _ = shutdown_rx.wait_for(|v| *v) => break,
            _ = ticker.tick() => {
                let payload = payload_fn();
                if let Some(ref f) = on_tick {
                    f(&ctx, &payload);
                }
                let update = crate::framing::make_gossip_update(
                    &ctx.node_id, ctx.default_ttl, Arc::clone(&kv_key), payload, false, &ctx.hlc,
                );
                apply_and_notify(&ctx.kv_state, &update);
                if first_tick {
                    ctx.caps_advertised.store(true, std::sync::atomic::Ordering::Release);
                    first_tick = false;
                }
                dispatch_gossip_try_send(
                    &ctx.gossip_txs, WireMessage::Data(update),
                    ctx.node_id.id_hash(), ForwardHint::All, &ctx.kv_state.dropped_frames,
                );
            }
        }
    }
    let tombstone = crate::framing::make_gossip_update(
        &ctx.node_id, ctx.default_ttl, Arc::clone(&kv_key), Bytes::new(), true, &ctx.hlc,
    );
    apply_and_notify(&ctx.kv_state, &tombstone);
    dispatch_gossip_send(
        &ctx.gossip_txs, WireMessage::Data(tombstone),
        ctx.node_id.id_hash(), ForwardHint::All,
    ).await;
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
