use crate::framing::{dispatch_gossip_send, ForwardHint, WireMessage};
use crate::signal::{AdvertiseHandle, Signal, SignalScope, WatchHandle};
use bytes::Bytes;
use std::{
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::{sync::mpsc, time};

use super::GossipAgent;
use super::emit_signal;

impl GossipAgent {
    /// Registers a handler for signals of the given `kind`.
    ///
    /// Returns an `mpsc::Receiver<Signal>` with the default channel depth (256). Caller is
    /// responsible for spawning a task to drive it. Multiple calls for the same kind each
    /// return an independent receiver — all receive every admitted signal.
    ///
    /// **Channel sizing**: 256 suits kinds that arrive at a few Hz (health probes, contract
    /// advertisements). For kinds where N agents emit simultaneously — e.g. `INVOKE` to a
    /// group of 256 workers — use [`signal_rx_with_capacity`](Self::signal_rx_with_capacity)
    /// with `N × expected_burst` as the depth. A full channel produces a warning log and
    /// the signal is dropped without retry.
    #[must_use]
    pub fn signal_rx(&self, kind: impl Into<Arc<str>>) -> mpsc::Receiver<Signal> {
        self.signal_handlers.register(kind.into())
    }

    /// Like [`signal_rx`](Self::signal_rx) with an explicit channel depth.
    ///
    /// Use a larger capacity for high-frequency kinds (e.g. health probes from N agents)
    /// or when the handler task cannot drain immediately.
    #[must_use]
    pub fn signal_rx_with_capacity(&self, kind: impl Into<Arc<str>>, cap: usize) -> mpsc::Receiver<Signal> {
        self.signal_handlers.register_with_capacity(kind.into(), cap)
    }

    /// Emits a signal to the cluster.
    ///
    /// The signal is delivered locally first (if admitted by this node's boundary),
    /// then queued for epidemic forwarding to all peers. The same nonce is inserted
    /// into the seen-set so if the signal returns via a peer it is silently dropped.
    ///
    /// Returns `true` if the signal was queued for forwarding; `false` if the gossip
    /// channel was full or the shard has died — local delivery still occurs.
    #[must_use]
    pub fn emit(
        &self,
        kind:    impl Into<Arc<str>>,
        scope:   SignalScope,
        payload: impl Into<Bytes>,
    ) -> bool {
        emit_signal(
            &self.node_id, &self.seen, &self.current_ts,
            &self.signal_boundary, &self.signal_handlers, &self.gossip_txs,
            self.config.default_ttl, &self.dropped_frames,
            kind.into(), scope, payload.into(),
        )
    }

    /// Like [`emit`](Self::emit), but awaits channel capacity instead of dropping
    /// the frame when the shard channel is full.
    ///
    /// Local delivery always occurs regardless of the return value.
    /// Returns `false` only if the shard task has crashed — the signal was delivered
    /// locally but will not propagate to peers. Suitable for `INVOKE` / `INVOKE_RESULT`
    /// flows where dropping a frame is a correctness failure.
    #[must_use]
    pub async fn emit_async(
        &self,
        kind:    impl Into<Arc<str>>,
        scope:   SignalScope,
        payload: impl Into<Bytes>,
    ) -> bool {
        let kind:    Arc<str> = kind.into();
        let payload: Bytes    = payload.into();
        let nonce = fastrand::u64(1..);
        let ts = self.current_ts.load(std::sync::atomic::Ordering::Relaxed);
        let _ = self.seen.is_duplicate(nonce, ts);

        if self.signal_boundary.read().admits(&scope) {
            let admit = match &scope {
                SignalScope::Individual(_) => true,
                _ => {
                    let opacity = self.signal_handlers.fill_ratio(&kind);
                    opacity == 0.0 || fastrand::f32() >= opacity
                }
            };
            if admit {
                self.signal_handlers.deliver(&Signal {
                    kind: kind.clone(), scope: scope.clone(),
                    payload: payload.clone(), sender: self.node_id.clone(), nonce,
                });
            }
        }

        let hint = match &scope {
            SignalScope::System           => ForwardHint::All,
            SignalScope::Group(name)      => ForwardHint::Group(name.clone()),
            SignalScope::Individual(peer) => ForwardHint::Individual(peer.clone()),
        };
        dispatch_gossip_send(
            &self.gossip_txs,
            WireMessage::Signal {
                ttl: self.config.default_ttl, nonce,
                sender: self.node_id.clone(), scope, kind, payload,
            },
            self.node_id.id_hash(), hint,
        ).await
    }

    /// Joins a named boundary group.
    ///
    /// The node immediately begins admitting `Group(name)` signals. Membership is
    /// published into the gossip KV store at `grp/<name>/<node_id>` so peers can
    /// observe it and subscribe to group roster changes.
    pub fn join_group(&self, group: impl Into<Arc<str>>) {
        let group: Arc<str> = group.into();
        let inserted = self.signal_boundary.write().groups.insert(group.clone());
        if inserted {
            let key = format!("grp/{}/{}", &*group, self.node_id);
            let _ = self.set(key, b"1".to_vec());
        }
    }

    /// Leaves a named boundary group.
    ///
    /// The node immediately stops admitting `Group(name)` signals. A tombstone for
    /// `grp/<name>/<node_id>` is published into the gossip store.
    pub fn leave_group(&self, group: impl Into<Arc<str>>) {
        let group: Arc<str> = group.into();
        let removed = self.signal_boundary.write().groups.remove(&group);
        if removed {
            let key = format!("grp/{}/{}", &*group, self.node_id);
            let _ = self.delete(key);
        }
    }

    /// Awaits the first locally-admitted signal of `kind` satisfying `predicate`.
    ///
    /// Returns `None` if `timeout` elapses before a matching signal arrives.
    /// Non-matching signals are discarded; the deadline is fixed across all iterations
    /// so total wait never exceeds `timeout`.
    ///
    /// The handler channel is registered **synchronously** when this function is called
    /// (before the returned future is polled), so no reply can be missed even if
    /// `emit` is called immediately after:
    /// ```ignore
    /// let result_fut = agent.signal_once("invoke.result", Duration::from_secs(5), |s| {
    ///     s.nonce == request_nonce
    /// });
    /// agent.emit("invoke", scope, payload);  // safe — channel already registered
    /// let result = result_fut.await;
    /// ```
    pub fn signal_once<F>(
        &self,
        kind:      impl Into<Arc<str>>,
        timeout:   Duration,
        predicate: F,
    ) -> impl std::future::Future<Output = Option<Signal>>
    where
        F: Fn(&Signal) -> bool,
    {
        let mut rx = self.signal_handlers.register_with_capacity(kind.into(), 256);
        async move {
            let deadline = time::Instant::now() + timeout;
            loop {
                match time::timeout_at(deadline, rx.recv()).await {
                    Ok(Some(sig)) if predicate(&sig) => return Some(sig),
                    Ok(Some(_))                      => continue,
                    _                               => return None,
                }
            }
        }
    }

    /// Periodically emits `kind` on `scope` every `interval`, calling `payload_fn`
    /// each tick to capture fresh state (e.g. current load metrics).
    ///
    /// Returns an [`AdvertiseHandle`] whose drop stops the task. The task also exits
    /// automatically when the agent shuts down.
    ///
    /// Workers call this once at startup to advertise availability:
    /// ```ignore
    /// let _handle = agent.advertise(
    ///     signal_kind::CONTRACT_AVAILABLE,
    ///     SignalScope::Group("nlp".into()),
    ///     Duration::from_secs(5),
    ///     || Bytes::new(),
    /// );
    /// ```
    pub fn advertise<F>(
        &self,
        kind:       impl Into<Arc<str>>,
        scope:      SignalScope,
        interval:   Duration,
        payload_fn: F,
    ) -> AdvertiseHandle
    where
        F: Fn() -> Bytes + Send + Sync + 'static,
    {
        let (cancel_tx, mut cancel_rx) = tokio::sync::oneshot::channel::<()>();
        let mut shutdown_rx = self.shutdown_tx.subscribe();

        let node_id         = self.node_id.clone();
        let seen            = self.seen.clone();
        let current_ts      = self.current_ts.clone();
        let signal_boundary = self.signal_boundary.clone();
        let signal_handlers = self.signal_handlers.clone();
        let gossip_txs      = self.gossip_txs.clone();
        let default_ttl     = self.config.default_ttl;
        let dropped_frames  = self.dropped_frames.clone();
        let kind: Arc<str>  = kind.into();

        let handle = tokio::spawn(async move {
            let mut ticker = time::interval(interval);
            ticker.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
            loop {
                tokio::select! { biased;
                    _ = &mut cancel_rx                   => break,
                    _ = shutdown_rx.wait_for(|v| *v)     => break,
                    _ = ticker.tick() => {
                        emit_signal(
                            &node_id, &seen, &current_ts, &signal_boundary,
                            &signal_handlers, &gossip_txs, default_ttl,
                            &dropped_frames, kind.clone(), scope.clone(), payload_fn(),
                        );
                    }
                }
            }
        });
        {
            let mut handles = self.task_handles.lock().unwrap_or_else(|e| e.into_inner());
            handles.retain(|h| !h.is_finished());
            handles.push(handle);
        }

        AdvertiseHandle { _cancel: cancel_tx }
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
        use crate::framing::{dispatch_gossip_try_send, GossipUpdate};
        use crate::store::apply_and_notify;

        let (cancel_tx, mut cancel_rx) = tokio::sync::oneshot::channel::<()>();
        let mut shutdown_rx = self.shutdown_tx.subscribe();

        let node_id           = self.node_id.clone();
        let seen              = self.seen.clone();
        let current_ts        = self.current_ts.clone();
        let signal_boundary   = self.signal_boundary.clone();
        let signal_handlers   = self.signal_handlers.clone();
        let gossip_txs        = self.gossip_txs.clone();
        let default_ttl       = self.config.default_ttl;
        let max_store_entries = self.config.max_store_entries;
        let dropped_frames    = self.dropped_frames.clone();
        let store             = self.store.clone();
        let subscriptions     = self.subscriptions.clone();
        let prefix_index      = self.prefix_index.clone();
        let kind: Arc<str>    = kind.into();
        let kv_key: Arc<str>  = Arc::from(format!("svc/{}/{}", kind, node_id).as_str());

        let handle = tokio::spawn(async move {
            let mut ticker = time::interval(interval);
            ticker.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
            loop {
                tokio::select! { biased;
                    _ = &mut cancel_rx                   => break,
                    _ = shutdown_rx.wait_for(|v| *v)     => break,
                    _ = ticker.tick() => {
                        let payload = payload_fn();
                        emit_signal(
                            &node_id, &seen, &current_ts, &signal_boundary,
                            &signal_handlers, &gossip_txs, default_ttl,
                            &dropped_frames, kind.clone(), scope.clone(), payload.clone(),
                        );
                        let ts = SystemTime::now()
                            .duration_since(UNIX_EPOCH).unwrap_or_default()
                            .as_millis() as u64;
                        let update = GossipUpdate {
                            nonce: fastrand::u64(1..),
                            sender: node_id.id_hash(),
                            ttl: default_ttl,
                            is_tombstone: false,
                            timestamp: ts,
                            key: kv_key.clone(),
                            value: payload,
                        };
                        apply_and_notify(&store, &subscriptions, &update, max_store_entries, &prefix_index);
                        dispatch_gossip_try_send(
                            &gossip_txs, WireMessage::Data(update),
                            node_id.id_hash(), ForwardHint::All, &dropped_frames,
                        );
                    }
                }
            }
            let ts = SystemTime::now()
                .duration_since(UNIX_EPOCH).unwrap_or_default()
                .as_millis() as u64;
            let tombstone = GossipUpdate {
                nonce: fastrand::u64(1..),
                sender: node_id.id_hash(),
                ttl: default_ttl,
                is_tombstone: true,
                timestamp: ts,
                key: kv_key.clone(),
                value: Bytes::new(),
            };
            apply_and_notify(&store, &subscriptions, &tombstone, max_store_entries, &prefix_index);
            dispatch_gossip_try_send(
                &gossip_txs, WireMessage::Data(tombstone),
                node_id.id_hash(), ForwardHint::All, &dropped_frames,
            );
        });
        {
            let mut handles = self.task_handles.lock().unwrap_or_else(|e| e.into_inner());
            handles.retain(|h| !h.is_finished());
            handles.push(handle);
        }

        AdvertiseHandle { _cancel: cancel_tx }
    }

    /// Returns when this node last admitted a signal of `kind` via local delivery,
    /// or `None` if no signal of that kind has ever been delivered.
    ///
    /// Does not require a registered handler — the timestamp is recorded on every
    /// call to `deliver()` regardless of whether a handler is registered.
    /// Updated even while the kind is suppressed.
    pub fn last_signal(&self, kind: &str) -> Option<Instant> {
        self.signal_handlers.last_signal(kind)
    }

    /// Suppresses local delivery of `kind` signals for `duration`.
    ///
    /// The signal is still forwarded epidemically — propagation is unconditional.
    /// The node simply does not call registered handlers for the suppressed kind.
    /// [`last_signal`](Self::last_signal) continues to update during suppression.
    ///
    /// This is an explicit refractory period. Call it after handling a signal to
    /// prevent re-handling the same kind within a cooldown window:
    ///
    /// ```ignore
    /// while let Some(sig) = invoke_rx.recv().await {
    ///     agent.suppress(signal_kind::INVOKE, Duration::from_millis(500));
    ///     handle_invocation(sig).await;
    /// }
    /// ```
    pub fn suppress(&self, kind: impl Into<Arc<str>>, duration: Duration) {
        self.signal_handlers.suppress(kind.into(), Instant::now() + duration);
    }

    /// Lifts a suppression set by [`suppress`](Self::suppress) before it expires.
    pub fn unsuppress(&self, kind: &str) {
        self.signal_handlers.unsuppress(kind);
    }

    /// Returns `true` if `kind` is currently suppressed on this node.
    pub fn is_suppressed(&self, kind: &str) -> bool {
        self.signal_handlers.is_suppressed(kind)
    }

    /// Watches `kind` for staleness, calling `on_stale` whenever the signal has not
    /// been delivered for longer than `threshold`.
    ///
    /// Spawns a background task that checks every `threshold / 4` (minimum 100 ms).
    /// `on_stale` fires repeatedly while the kind remains silent — callers that want
    /// one-shot behaviour should drop the returned handle or call
    /// [`unsuppress`](Self::unsuppress) after responding.
    ///
    /// A kind that has never been seen counts as stale immediately. Returns a
    /// [`WatchHandle`] whose drop cancels the task; the task also exits automatically
    /// on [`shutdown`](Self::shutdown).
    ///
    /// ```ignore
    /// let _watcher = agent.watch(
    ///     signal_kind::CONTRACT_AVAILABLE,
    ///     Duration::from_secs(30),
    ///     move || { respawn_worker(); },
    /// );
    /// ```
    pub fn watch<F>(
        &self,
        kind:      impl Into<Arc<str>>,
        threshold: Duration,
        on_stale:  F,
    ) -> WatchHandle
    where
        F: Fn() + Send + 'static,
    {
        let (cancel_tx, mut cancel_rx) = tokio::sync::oneshot::channel::<()>();
        let mut shutdown_rx = self.shutdown_tx.subscribe();
        let signal_handlers = self.signal_handlers.clone();
        let kind: Arc<str>  = kind.into();
        let check_interval  = (threshold / 4).max(Duration::from_millis(100));

        let handle = tokio::spawn(async move {
            let mut ticker = time::interval(check_interval);
            ticker.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
            loop {
                tokio::select! { biased;
                    _ = &mut cancel_rx               => break,
                    _ = shutdown_rx.wait_for(|v| *v) => break,
                    _ = ticker.tick() => {
                        let stale = signal_handlers
                            .last_signal(&kind)
                            .map(|t| t.elapsed() > threshold)
                            .unwrap_or(true);
                        if stale { on_stale(); }
                    }
                }
            }
        });
        {
            let mut handles = self.task_handles.lock().unwrap_or_else(|e| e.into_inner());
            handles.retain(|h| !h.is_finished());
            handles.push(handle);
        }

        WatchHandle { _cancel: cancel_tx }
    }

    /// Returns `true` when at least `min_senders` distinct nodes have had a signal of
    /// `kind` delivered to this node within `window`.
    ///
    /// Synchronous read — no background task. Pairs well with
    /// [`advertise`](Self::advertise): peers advertise their heartbeat every N seconds;
    /// the receiver calls `quorum` to act only once K distinct peers have been heard:
    ///
    /// ```ignore
    /// if agent.quorum(signal_kind::CONTRACT_AVAILABLE, 3, Duration::from_secs(10)) {
    ///     dispatch_workload();
    /// }
    /// ```
    pub fn quorum(&self, kind: &str, min_senders: usize, window: Duration) -> bool {
        self.signal_handlers.quorum(kind, min_senders, window)
    }

    /// Like [`quorum`](Self::quorum) but only counts senders that are current members
    /// of `group` according to Layer I (`grp/{group}/`).
    ///
    /// Prevents ex-members from satisfying quorum after they call [`leave_group`](Self::leave_group).
    /// A node is considered a current member if its `grp/{group}/{node_id}` key is live
    /// (not tombstoned) in the store.
    pub fn group_quorum(
        &self,
        group: &str,
        kind: &str,
        min_senders: usize,
        window: Duration,
    ) -> bool {
        use ahash::AHashSet;
        let prefix = format!("grp/{}/", group);
        let member_hashes: AHashSet<u64> = self
            .scan_prefix(&prefix)
            .into_iter()
            .filter_map(|(key, _)| {
                key.strip_prefix(&prefix)
                    .and_then(|s| s.parse::<crate::node_id::NodeId>().ok())
                    .map(|n| n.id_hash())
            })
            .collect();
        self.signal_handlers.quorum_for_group(kind, &member_hashes, min_senders, window)
    }

    /// Counts distinct senders of `kind` within `window` using Layer I as evidence.
    ///
    /// Unlike [`quorum`](Self::quorum), which reads the in-memory sender log (lost on restart),
    /// this reads `quorum/{kind}/` from the KV store — durable, anti-entropy synced records
    /// written by the connection handler on every admitted signal delivery.
    ///
    /// Use this when quorum evidence must survive process restarts — for example, to verify
    /// that enough voters participated in a consensus round before acting on a committed value,
    /// even after this node crashed and was restarted mid-ballot.
    ///
    /// **Prefer [`quorum`](Self::quorum) for latency-sensitive paths.** The in-memory version
    /// is O(window_entries) with no store access; `quorum_persistent` scans the prefix index
    /// (O(quorum_keys)) plus a store lookup per entry.
    pub fn quorum_persistent(&self, kind: &str, window: Duration) -> usize {
        use std::time::{SystemTime, UNIX_EPOCH};
        let prefix = format!("quorum/{}/", kind);
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
}
