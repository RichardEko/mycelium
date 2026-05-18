use crate::store::{apply_and_notify, intern_pool_len};
use bytes::Bytes;
use std::sync::{
        atomic::Ordering,
        Arc,
    };
use tokio::sync::watch;

use super::{GossipAgent, SystemStats, STATE_RUNNING};

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
        self.signal_boundary.read().groups.iter().cloned().collect()
    }

    /// Stores `value` under `key` locally and queues it for gossip to peers.
    ///
    /// `key` accepts `&str`, `Arc<str>`, `String`, or anything that converts to
    /// `Arc<str>`. Callers with a hot key set can pre-intern keys as `Arc<str>`
    /// and pass them here to avoid a heap allocation on every write.
    ///
    /// **Each agent should write only its own keys.** Writing all keys from a single
    /// agent floods that agent's peer-writer channels: with N keys and channel depth D,
    /// writes are silently dropped when N > D. Distribute writes across agents so each
    /// agent writes exactly its own key — this produces 1 message per peer-writer
    /// regardless of cluster size.
    ///
    /// The local store is **always** updated regardless of the return value.
    /// Returns `true` if the update was queued for gossip; `false` if the gossip
    /// channel was full (backpressure) or the shard has died — the update was
    /// applied locally but will not propagate to peers.
    #[must_use]
    pub fn set<K: Into<Arc<str>>>(&self, key: K, value: impl Into<Bytes>) -> bool {
        let key: Arc<str> = key.into();
        let update = self.make_update(key, value.into(), false);
        apply_and_notify(&self.store, &self.subscriptions, &update, self.config.max_store_entries, &self.prefix_index);
        self.dispatch_update(update)
    }

    /// Returns the current value for `key`, or `None` if absent or tombstoned.
    pub fn get(&self, key: &str) -> Option<Bytes> {
        self.store.pin().get(key).and_then(|e| e.data.clone())
    }

    /// Removes `key` locally and queues a tombstone for gossip to peers.
    ///
    /// The local store is **always** updated regardless of the return value.
    /// Returns `true` if the tombstone was queued for gossip; `false` if the gossip
    /// channel was full (backpressure) or the shard has died — the tombstone was
    /// applied locally but will not propagate to peers.
    #[must_use]
    pub fn delete<K: Into<Arc<str>>>(&self, key: K) -> bool {
        let key: Arc<str> = key.into();
        let update = self.make_update(key, Bytes::new(), true);
        apply_and_notify(&self.store, &self.subscriptions, &update, self.config.max_store_entries, &self.prefix_index);
        self.dispatch_update(update)
    }

    /// Like [`set`](Self::set), but awaits channel capacity instead of dropping
    /// the frame when the shard channel is full.
    ///
    /// The local store is **always** updated regardless of the return value.
    /// Returns `false` only if the shard task has crashed — the update was applied
    /// locally but will not propagate to peers. Suitable for callers that must not
    /// lose writes under backpressure.
    #[must_use]
    pub async fn set_async<K: Into<Arc<str>>>(&self, key: K, value: impl Into<Bytes>) -> bool {
        let key: Arc<str> = key.into();
        let update = self.make_update(key, value.into(), false);
        apply_and_notify(&self.store, &self.subscriptions, &update, self.config.max_store_entries, &self.prefix_index);
        self.dispatch_update_async(update).await
    }

    /// Like [`delete`](Self::delete), but awaits channel capacity instead of dropping
    /// the frame when the shard channel is full.
    ///
    /// The local store is **always** updated regardless of the return value.
    /// Returns `false` only if the shard task has crashed — the tombstone was applied
    /// locally but will not propagate to peers. Suitable for callers that must not
    /// lose tombstones under backpressure.
    #[must_use]
    pub async fn delete_async<K: Into<Arc<str>>>(&self, key: K) -> bool {
        let key: Arc<str> = key.into();
        let update = self.make_update(key, Bytes::new(), true);
        apply_and_notify(&self.store, &self.subscriptions, &update, self.config.max_store_entries, &self.prefix_index);
        self.dispatch_update_async(update).await
    }

    /// Returns a snapshot of all keys that have a live (non-tombstone) value.
    ///
    /// Keys are returned as `Arc<str>` — clone is O(1). Callers that need `String`
    /// can call `.to_string()` on each element.
    pub fn keys(&self) -> Vec<Arc<str>> {
        let guard = self.store.pin();
        guard.iter()
            .filter(|(_, v)| v.data.is_some())
            .map(|(k, _)| k.clone())
            .collect()
    }

    /// Returns all live (non-tombstone) key-value pairs whose key starts with `prefix`,
    /// in a single store pass.
    ///
    /// More efficient than `keys()` + `get()` per key when reading prefix-namespaced
    /// data such as pheromone trails or group rosters:
    ///
    /// ```ignore
    /// use gossip_protocol::kv_ns;
    /// let trails = agent.scan_prefix(kv_ns::LOAD);
    /// for (key, bytes) in trails {
    ///     // decode bytes into LoadState, check written_at_ms for evaporation
    /// }
    /// ```
    pub fn scan_prefix(&self, prefix: &str) -> Vec<(Arc<str>, Bytes)> {
        let seg = prefix.find('/').map_or(prefix, |i| &prefix[..i]);
        let store_guard = self.store.pin();
        let idx_guard = self.prefix_index.pin();
        if let Some(bucket) = idx_guard.get(seg) {
            // O(|bucket|) fast path: only iterate keys in this segment.
            bucket.pin().iter()
                .filter_map(|(key, _)| {
                    if !key.starts_with(prefix) { return None; }
                    let entry = store_guard.get(key.as_ref())?;
                    let data = entry.data.clone()?;
                    Some((key.clone(), data))
                })
                .collect()
        } else {
            // Unknown prefix — full scan fallback.
            store_guard.iter()
                .filter(|(k, v)| v.data.is_some() && k.starts_with(prefix))
                .map(|(k, v)| (k.clone(), v.data.clone().unwrap()))
                .collect()
        }
    }

    /// Subscribes to changes for `key`.
    ///
    /// `key` accepts `&str`, `Arc<str>`, or anything that converts to `Arc<str>`.
    ///
    /// The receiver's initial value is a snapshot of the store at subscription time.
    /// A concurrent `set` or `delete` between the store read and the channel CAS may
    /// produce a stale initial value; the next write to that key will deliver the
    /// correct value.
    #[must_use]
    pub fn subscribe<K: Into<Arc<str>>>(&self, key: K) -> watch::Receiver<Option<Bytes>> {
        let key_arc: Arc<str> = key.into();
        loop {
            let guard = self.subscriptions.pin();
            if let Some(tx) = guard.get(&key_arc) {
                if !tx.is_closed() {
                    return tx.subscribe();
                }
            }
            let current = self.store.pin().get(&*key_arc).and_then(|e| e.data.clone());
            let (new_tx, rx) = watch::channel(current);
            let mut slot = Some(new_tx);
            let result = guard.compute(key_arc.clone(), |existing| match existing {
                Some((_, tx)) if !tx.is_closed() => papaya::Operation::Abort(()),
                _ => match slot.take() {
                    Some(tx) => papaya::Operation::Insert(tx),
                    None => papaya::Operation::Abort(()),
                },
            });
            if matches!(result, papaya::Compute::Inserted(..) | papaya::Compute::Updated { .. }) {
                return rx;
            }
        }
    }

    /// Returns a snapshot of live protocol state.
    ///
    /// Note: `dead_shards` may transiently report all shards as dead in the brief
    /// window between `start()` returning and the shard tasks being scheduled by
    /// the tokio runtime. This is normal and resolves on the next call.
    pub fn system_stats(&self) -> SystemStats {
        let running = self.state.load(Ordering::Relaxed) == STATE_RUNNING;
        let gossip_shard_queue_depths: Vec<usize> = self.gossip_txs.iter()
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
                self.store.pin().iter().filter(|(_, v)| v.data.is_some()).count()
            },
            cached_connections: self.peer_writers.iter()
                .filter(|e| !e.value().handle.is_finished())
                .count(),
            gossip_shard_queue_depths,
            dead_shards,
            gc_alive:             !running || self.gc_alive.load(Ordering::Relaxed),
            health_monitor_alive: !running || self.health_monitor_alive.load(Ordering::Relaxed),
            intern_pool_size:     intern_pool_len(),
            dropped_frames:       self.dropped_frames.load(Ordering::Relaxed),
        }
    }
}
