use crate::framing::{dispatch_gossip_send, dispatch_gossip_try_send, ForwardHint, WireMessage};
use crate::signal::{AdvertiseHandle, SignalScope};
use crate::store::{apply_and_notify, intern_pool_len};
use bytes::Bytes;
use std::{
    sync::{atomic::Ordering, Arc},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{sync::watch, time};

use super::{emit_signal, AgentState, GossipAgent, SystemStats};

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
        apply_and_notify(&self.kv_state, &update);
        self.dispatch_update(update)
    }

    /// Returns the current value for `key`, or `None` if absent or tombstoned.
    pub fn get(&self, key: &str) -> Option<Bytes> {
        self.kv_state.store.pin().get(key).and_then(|e| e.data.clone())
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
        apply_and_notify(&self.kv_state, &update);
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
        apply_and_notify(&self.kv_state, &update);
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
        apply_and_notify(&self.kv_state, &update);
        self.dispatch_update_async(update).await
    }

    /// Returns a snapshot of all keys that have a live (non-tombstone) value.
    ///
    /// Keys are returned as `Arc<str>` — clone is O(1). Callers that need `String`
    /// can call `.to_string()` on each element.
    pub fn keys(&self) -> Vec<Arc<str>> {
        let guard = self.kv_state.store.pin();
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
    /// use mycelium::kv_ns;
    /// let trails = agent.scan_prefix(kv_ns::LOAD);
    /// for (key, bytes) in trails {
    ///     // decode bytes into LoadState, check written_at_ms for evaporation
    /// }
    /// ```
    pub fn scan_prefix(&self, prefix: &str) -> Vec<(Arc<str>, Bytes)> {
        let seg = prefix.find('/').map_or(prefix, |i| &prefix[..i]);
        let store_guard = self.kv_state.store.pin();
        let idx_guard = self.kv_state.prefix_index.pin();
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

    /// Subscribes to changes under a key `prefix`.
    ///
    /// Returns a `watch::Receiver<u64>` whose value increments every time a key
    /// matching the prefix is written or tombstoned in the local store. Receivers
    /// typically use `changed().await` rather than reading the counter — the counter
    /// is opaque and exists only to convey "something changed."
    ///
    /// Watcher entries are created lazily and shared: multiple subscribers to the
    /// same prefix receive notifications from the same underlying sender. Closed
    /// senders (no live receivers) are evicted by `apply_and_notify` on the next
    /// matching write.
    #[must_use]
    pub fn subscribe_prefix<P: Into<Arc<str>>>(&self, prefix: P) -> watch::Receiver<u64> {
        let prefix_arc: Arc<str> = prefix.into();
        loop {
            let guard = self.kv_state.prefix_watchers.pin();
            if let Some(tx) = guard.get(&prefix_arc) {
                if !tx.is_closed() {
                    return tx.subscribe();
                }
            }
            let (new_tx, rx) = watch::channel(0u64);
            let new_tx_arc = Arc::new(new_tx);
            let mut slot = Some(new_tx_arc);
            let result = guard.compute(prefix_arc.clone(), |existing| match existing {
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
            let guard = self.kv_state.subscriptions.pin();
            if let Some(tx) = guard.get(&key_arc) {
                if !tx.is_closed() {
                    return tx.subscribe();
                }
            }
            let current = self.kv_state.store.pin().get(&*key_arc).and_then(|e| e.data.clone());
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

    /// Like [`advertise`](Self::advertise) but also writes the payload to Layer I on every
    /// tick so late joiners and restarted peers can discover capabilities immediately via
    /// `scan_prefix(kv_ns::ADVERTISE)` without waiting for the next signal tick.
    ///
    /// Key written: `svc/{kind}/{node_id}`. Tombstoned automatically when the returned
    /// [`AdvertiseHandle`] is dropped or the agent shuts down.
    ///
    /// The signal is still emitted epidemically on each tick (same as [`advertise`]);
    /// the Layer I entry is an additional durable anchor.
    #[must_use]
    pub fn advertise_persistent<F>(
        &self,
        kind:       impl Into<Arc<str>>,
        scope:      SignalScope,
        interval:   Duration,
        payload_fn: F,
    ) -> AdvertiseHandle
    where
        F: Fn() -> Bytes + Send + Sync + 'static,
    {
        let (cancel_tx, cancel_rx) = tokio::sync::oneshot::channel::<()>();
        let shutdown_rx = self.shutdown_tx.subscribe();

        let ctx:             Arc<super::TaskCtx> = Arc::clone(&self.task_ctx);
        let kind: Arc<str>   = kind.into();
        let kv_key: Arc<str> = Arc::from(format!("svc/{}/{}", kind, ctx.node_id).as_str());
        let payload_arc: PersistPayloadFn = Arc::new(payload_fn);
        let on_tick: PersistOnTickFn = {
            let kind = kind.clone();
            Arc::new(move |ctx, payload| {
                emit_signal(ctx, kind.clone(), scope.clone(), payload.clone());
            })
        };

        self.spawn_task(run_kv_persist_task(
            ctx, cancel_rx, shutdown_rx, kv_key, interval, payload_arc, Some(on_tick),
        ));

        AdvertiseHandle { _cancel: cancel_tx }
    }

    /// Counts distinct senders of `kind` within `window` using Layer I as evidence.
    ///
    /// Unlike [`quorum`](Self::quorum), which reads the in-memory sender log (lost on restart),
    /// this reads `sys/quorum/{kind}/` from the KV store — durable, anti-entropy synced records
    /// written by the connection handler on every admitted signal delivery.
    ///
    /// Use this when quorum evidence must survive process restarts — for example, to verify
    /// that enough voters participated in a consensus round before acting on a committed value,
    /// even after this node crashed and was restarted mid-ballot.
    ///
    /// **Scope limitation**: evidence is keyed by `(kind, sender)` only — there is no
    /// slot, ballot, or correlation ID in the key. All signals of `kind` from the same
    /// sender collapse into one entry regardless of ballot or slot. `quorum_persistent` is
    /// best suited for application-level quorum queries such as "have K distinct nodes
    /// advertised capability X within window W?" rather than per-round consensus checks.
    /// For per-round quorum, use the in-memory `votes_last_ballot` field in
    /// [`ConsensusResult::Timeout`](crate::consensus::ConsensusResult::Timeout).
    ///
    /// **Prefer [`quorum`](Self::quorum) for latency-sensitive paths.** The in-memory version
    /// is O(window_entries) with no store access; `quorum_persistent` scans the prefix index
    /// (O(quorum_keys)) plus a store lookup per entry.
    ///
    /// **Retention difference vs [`quorum`]**: the in-memory sender-log is retained for
    /// `signal_window_secs` (default 600 s) and is reset on restart. `quorum_persistent`
    /// reads from the Layer I store whose entries are evicted by tombstone GC after
    /// `default_ttl × propagation_window × 10` ms — a typically much longer window.
    /// Use `quorum` for low-latency reads during normal operation; use `quorum_persistent`
    /// only when evidence must survive process restarts.
    pub fn quorum_persistent(&self, kind: &str, window: Duration) -> usize {
        use crate::signal::kv_ns;
        let prefix = format!("{}{}/", kv_ns::QUORUM, kind);
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH).unwrap_or_default()
            .as_millis() as u64;
        let window_ms = window.as_millis() as u64;
        self.scan_prefix(&prefix)
            .into_iter()
            .filter(|(_, v)| {
                v.get(..8)
                    .and_then(|b| b.try_into().ok())
                    .map(|b: [u8; 8]| u64::from_le_bytes(b))
                    .map(|ts| now_ms.saturating_sub(ts) <= window_ms)
                    .unwrap_or(false)
            })
            .count()
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
                    &ctx.node_id, ctx.default_ttl, kv_key.clone(), payload, false,
                );
                apply_and_notify(&ctx.kv_state, &update);
                dispatch_gossip_try_send(
                    &ctx.gossip_txs, WireMessage::Data(update),
                    ctx.node_id.id_hash(), ForwardHint::All, &ctx.kv_state.dropped_frames,
                );
            }
        }
    }
    let tombstone = crate::framing::make_gossip_update(
        &ctx.node_id, ctx.default_ttl, kv_key.clone(), Bytes::new(), true,
    );
    apply_and_notify(&ctx.kv_state, &tombstone);
    dispatch_gossip_send(
        &ctx.gossip_txs, WireMessage::Data(tombstone),
        ctx.node_id.id_hash(), ForwardHint::All,
    ).await;
}
