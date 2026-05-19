use crate::signal::{kv_ns, AdvertiseHandle, Signal, SignalScope, WatchHandle};
use bytes::{BufMut, Bytes, BytesMut};
use std::{
    sync::Arc,
    time::{Duration, Instant},
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
        self.task_ctx.signal_handlers.register(kind.into())
    }

    /// Like [`signal_rx`](Self::signal_rx) with an explicit channel depth.
    ///
    /// Use a larger capacity for high-frequency kinds (e.g. health probes from N agents)
    /// or when the handler task cannot drain immediately.
    #[must_use]
    pub fn signal_rx_with_capacity(&self, kind: impl Into<Arc<str>>, cap: usize) -> mpsc::Receiver<Signal> {
        self.task_ctx.signal_handlers.register_with_capacity(kind.into(), cap)
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
        emit_signal(&self.task_ctx, kind.into(), scope, payload.into())
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
        super::helpers::emit_signal_async(&self.task_ctx, kind.into(), scope, payload.into()).await
    }

    /// Joins a named boundary group.
    ///
    /// The node immediately begins admitting `Group(name)` signals. Membership is
    /// published into the gossip KV store at `grp/<name>/<node_id>` so peers can
    /// observe it and subscribe to group roster changes.
    pub fn join_group(&self, group: impl Into<Arc<str>>) {
        let group: Arc<str> = group.into();
        let inserted = self.task_ctx.signal_boundary.write().groups.insert(group.clone());
        if inserted {
            self.group_roster_cache.pin().remove(&group);
            let key = crate::signal::grp_member_key(&group, &self.node_id);
            let _ = self.set(key, b"1".to_vec());
        }
    }

    /// Leaves a named boundary group.
    ///
    /// The node immediately stops admitting `Group(name)` signals. A tombstone for
    /// `grp/<name>/<node_id>` is published into the gossip store.
    pub fn leave_group(&self, group: impl Into<Arc<str>>) {
        let group: Arc<str> = group.into();
        let removed = self.task_ctx.signal_boundary.write().groups.remove(&group);
        if removed {
            self.group_roster_cache.pin().remove(&group);
            let key = crate::signal::grp_member_key(&group, &self.node_id);
            let _ = self.delete(key);
        }
    }

    /// Returns the live members of `group` according to the Layer I roster (`grp/{group}/`).
    ///
    /// Nodes that have called [`leave_group`] or whose membership entry has been tombstoned
    /// are excluded. Order is arbitrary.
    pub fn group_members(&self, group: &str) -> Vec<crate::node_id::NodeId> {
        let prefix = crate::signal::grp_prefix(group);
        self.scan_prefix(&prefix)
            .into_iter()
            .filter_map(|(key, _)| {
                key.strip_prefix(&prefix)
                    .and_then(|s| s.parse::<crate::node_id::NodeId>().ok())
            })
            .collect()
    }

    /// Like [`group_members`](Self::group_members) but returns a cached result when
    /// the roster was fetched within `ttl`. The cache is eagerly invalidated whenever
    /// this node calls `join_group` or `leave_group`, so it only goes stale for remote
    /// membership changes — acceptable for consensus ballot setup.
    pub(super) fn cached_group_members(
        &self,
        group: &str,
        ttl: std::time::Duration,
    ) -> Arc<(Vec<crate::node_id::NodeId>, std::time::Instant)> {
        let group_key: Arc<str> = Arc::from(group);
        let guard = self.group_roster_cache.pin();
        if let Some(entry) = guard.get(&group_key) {
            if entry.1.elapsed() < ttl {
                return entry.clone();
            }
        }
        let members = self.group_members(group);
        let fresh = Arc::new((members, std::time::Instant::now()));
        guard.insert(group_key, fresh.clone());
        fresh
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
        let mut rx = self.task_ctx.signal_handlers.register_with_capacity(kind.into(), 256);
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

    /// Emits a request signal and awaits a matching reply.
    ///
    /// Generates a random 8-byte nonce, prepends it to `payload` (little-endian u64),
    /// emits `kind` on `scope`, then awaits the first `result_kind` signal whose payload
    /// starts with the same 8 bytes. Returns `None` if `timeout` elapses.
    ///
    /// The result handler is registered **before** the request is emitted so no reply
    /// can be missed even if the peer responds immediately.
    ///
    /// **Nonce convention**: the first 8 bytes of the payload carry the correlation nonce.
    /// Responders must echo these bytes at the start of their `result_kind` reply payload.
    /// See [`signal_kind::INVOKE_RESULT`](crate::signal::signal_kind::INVOKE_RESULT).
    ///
    /// **Channel capacity**: the result handler ring buffer holds 256 entries. If more
    /// than 256 `result_kind` signals arrive before the matching nonce is found, the
    /// oldest are dropped. For high-fan-in result kinds, register a larger channel with
    /// [`signal_rx_with_capacity`](Self::signal_rx_with_capacity) and drive it with
    /// [`signal_once`](Self::signal_once) manually.
    ///
    /// To cancel early when the target goes opaque, race against a
    /// [`BOUNDARY_OPAQUE`](crate::signal::signal_kind::BOUNDARY_OPAQUE) subscription:
    /// ```ignore
    /// let req = agent.request(INVOKE, scope, payload, INVOKE_RESULT, timeout);
    /// let mut opaque_rx = agent.signal_rx(signal_kind::BOUNDARY_OPAQUE);
    /// tokio::select! {
    ///     result = req        => result,
    ///     _ = opaque_rx.recv() => None,
    /// }
    /// ```
    pub fn request(
        &self,
        kind:        impl Into<Arc<str>>,
        scope:       SignalScope,
        payload:     impl Into<Bytes>,
        result_kind: impl Into<Arc<str>>,
        timeout:     Duration,
    ) -> impl std::future::Future<Output = Option<Signal>> {
        let nonce: u64 = fastrand::u64(1..);
        let payload_bytes: Bytes = payload.into();
        let mut buf = BytesMut::with_capacity(8 + payload_bytes.len());
        buf.put_u64_le(nonce);
        buf.put(payload_bytes);
        let frozen = buf.freeze();
        let mut rx = self.task_ctx.signal_handlers.register_with_capacity(result_kind.into(), 256);
        let _ = self.emit(kind.into(), scope, frozen);
        async move {
            let deadline = time::Instant::now() + timeout;
            loop {
                match time::timeout_at(deadline, rx.recv()).await {
                    Ok(Some(sig)) if sig.payload.get(..8)
                        .and_then(|b| b.try_into().ok())
                        .map(|b: [u8; 8]| u64::from_le_bytes(b) == nonce)
                        .unwrap_or(false) => return Some(sig),
                    Ok(Some(_)) => continue,
                    _ => return None,
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

        let ctx:           Arc<super::TaskCtx> = Arc::clone(&self.task_ctx);
        let kind: Arc<str> = kind.into();

        let handle = tokio::spawn(async move {
            let mut ticker = time::interval(interval);
            ticker.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
            loop {
                tokio::select! { biased;
                    _ = &mut cancel_rx                   => break,
                    _ = shutdown_rx.wait_for(|v| *v)     => break,
                    _ = ticker.tick() => {
                        emit_signal(&ctx, kind.clone(), scope.clone(), payload_fn());
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

    /// Returns when this node last admitted a signal of `kind` via local delivery,
    /// or `None` if no signal of that kind has ever been delivered.
    ///
    /// Does not require a registered handler — the timestamp is recorded on every
    /// call to `deliver()` regardless of whether a handler is registered.
    /// Updated even while the kind is suppressed.
    pub fn last_signal(&self, kind: &str) -> Option<Instant> {
        self.task_ctx.signal_handlers.last_signal(kind)
    }

    /// Returns how long ago `kind` was last observed by *any* peer, reading from
    /// the `sys/quorum/` Layer I evidence written by the connection handler.
    ///
    /// Unlike [`last_signal`](Self::last_signal), which reads the in-memory
    /// `last_seen` map and returns `None` after a process restart, this method
    /// survives restarts: `sys/quorum/` entries are anti-entropy synced and
    /// persist on disk (if a durable store backend is configured).
    ///
    /// Returns the *minimum* (most recent) age across all senders that sent
    /// `kind`. Returns `None` when no `sys/quorum/{kind}/` entries exist.
    pub fn last_signal_persistent(&self, kind: &str) -> Option<std::time::Duration> {
        use std::time::{SystemTime, UNIX_EPOCH};
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let prefix = format!("{}{}/", kv_ns::QUORUM, kind);
        self.scan_prefix(&prefix)
            .into_iter()
            .filter_map(|(_, bytes)| {
                if bytes.len() < 8 { return None; }
                let written_at = u64::from_le_bytes(bytes[..8].try_into().ok()?);
                now_ms.checked_sub(written_at)
            })
            .min()
            .map(std::time::Duration::from_millis)
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
        self.task_ctx.signal_handlers.suppress(kind.into(), Instant::now() + duration);
    }

    /// Lifts a suppression set by [`suppress`](Self::suppress) before it expires.
    pub fn unsuppress(&self, kind: &str) {
        self.task_ctx.signal_handlers.unsuppress(kind);
    }

    /// Returns `true` if `kind` is currently suppressed on this node.
    pub fn is_suppressed(&self, kind: &str) -> bool {
        self.task_ctx.signal_handlers.is_suppressed(kind)
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
        let signal_handlers = Arc::clone(&self.task_ctx.signal_handlers);
        let kv_watch        = Arc::clone(&self.kv_state);
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
                        let elapsed = signal_handlers.last_signal(&kind).map(|t| t.elapsed());
                        let stale = match elapsed {
                            Some(dur) if dur <= threshold => false,
                            Some(_) => true,
                            // No in-memory record: check Layer I quorum evidence so that a
                            // recently-restarted node doesn't fire on_stale spuriously when
                            // sys/quorum/ entries prove the kind was active before the restart.
                            None => {
                                use std::time::{SystemTime, UNIX_EPOCH};
                                let now_ms = SystemTime::now()
                                    .duration_since(UNIX_EPOCH).unwrap_or_default()
                                    .as_millis() as u64;
                                let threshold_ms = threshold.as_millis() as u64;
                                let prefix = format!("{}{}/", kv_ns::QUORUM, &*kind);
                                let store = kv_watch.store.pin();
                                let idx   = kv_watch.prefix_index.pin();
                                let found = if let Some(bucket) = idx.get(kv_ns::QUORUM.split_once('/').map_or(kv_ns::QUORUM, |(s, _)| s)) {
                                    bucket.pin().iter().any(|(k, _)| {
                                        if !k.starts_with(&*prefix) { return false; }
                                        store.get(k.as_ref())
                                            .and_then(|e| e.data.as_ref())
                                            .and_then(|b| b.get(..8))
                                            .and_then(|b| b.try_into().ok())
                                            .map(|b: [u8; 8]| u64::from_le_bytes(b))
                                            .map(|ts| now_ms.saturating_sub(ts) <= threshold_ms)
                                            .unwrap_or(false)
                                    })
                                } else {
                                    false
                                };
                                !found
                            }
                        };
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

    /// Routes a request to the group member best suited to handle `kind`.
    ///
    /// Selects the target using [`suggest_leader`](Self::suggest_leader) — the member
    /// with the lowest `sys/load/{member}/{kind}` fill ratio within `max_age`. Ties are
    /// broken by `id_hash()`. Emits the request as [`SignalScope::Individual`] so only that
    /// member's handler fires, then awaits the first `result_kind` signal whose first 8
    /// payload bytes match the correlation nonce. Returns `None` on timeout.
    ///
    /// This mirrors what service meshes like Envoy + xDS do (pick the lowest-load endpoint,
    /// forward) but is built entirely in the gossip layer without a control plane.
    ///
    /// Use [`signal_window`](Self::signal_window) as `max_age` to respect the configured
    /// pheromone evaporation window.
    pub fn route_to(
        &self,
        group:       &str,
        kind:        impl Into<Arc<str>>,
        payload:     impl Into<Bytes>,
        result_kind: impl Into<Arc<str>>,
        max_age:     Duration,
        timeout:     Duration,
    ) -> impl std::future::Future<Output = Option<Signal>> {
        let kind_arc: Arc<str> = kind.into();
        let target = self.suggest_leader(group, &kind_arc, max_age);
        self.request(kind_arc, SignalScope::Individual(target), payload, result_kind, timeout)
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
        self.task_ctx.signal_handlers.quorum(kind, min_senders, window)
    }

    /// Like [`quorum`](Self::quorum) but only counts senders that are current members
    /// of `group` according to Layer I (`grp/{group}/`).
    ///
    /// Prevents ex-members from satisfying quorum after they call [`leave_group`](Self::leave_group).
    /// A node is considered a current member if its `grp/{group}/{node_id}` key is live
    /// (not tombstoned) in the store.
    ///
    /// For hot-path callers that already hold a pre-built member hash set (e.g. during
    /// ballot collection), prefer [`group_quorum_prehashed`](Self::group_quorum_prehashed)
    /// to avoid recomputing `id_hash()` on every call.
    pub fn group_quorum(
        &self,
        group: &str,
        kind: &str,
        min_senders: usize,
        window: Duration,
    ) -> bool {
        use ahash::AHashSet;
        let member_hashes: AHashSet<u64> = self.group_members(group)
            .iter()
            .map(|n| n.id_hash())
            .collect();
        self.task_ctx.signal_handlers.quorum_for_group(kind, &member_hashes, min_senders, window)
    }

    /// Like [`group_quorum`](Self::group_quorum) but accepts a pre-built member hash set.
    ///
    /// Avoids recomputing `id_hash()` on every call when the caller already holds a
    /// stable `AHashSet<u64>` for the group's current membership (e.g. during a ballot
    /// collection loop where the member list doesn't change mid-round).
    ///
    /// Build the set once with:
    /// ```ignore
    /// let member_hashes: AHashSet<u64> = agent.group_members(group)
    ///     .iter().map(|n| n.id_hash()).collect();
    /// ```
    pub fn group_quorum_prehashed(
        &self,
        member_hashes: &ahash::AHashSet<u64>,
        kind: &str,
        min_senders: usize,
        window: Duration,
    ) -> bool {
        self.task_ctx.signal_handlers.quorum_for_group(kind, member_hashes, min_senders, window)
    }

}
