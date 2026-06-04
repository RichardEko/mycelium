use crate::signal::{kv_ns, AdvertiseHandle, Signal, SignalScope, WatchHandle};
use bytes::{BufMut, Bytes, BytesMut};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{sync::mpsc, time};

use super::{TaskCtx, emit_signal};
use super::helpers::{group_members_ctx, kv_delete, kv_scan_prefix, kv_set};
use super::kv::run_kv_persist_task;
use super::kv::{PersistPayloadFn, PersistOnTickFn};

/// Typed handle for signal mesh operations (Layer II).
///
/// Zero-cost: wraps one `Arc<TaskCtx>` clone. `Clone + Send + Sync`.
/// Acquire via [`GossipAgent::mesh`](super::GossipAgent::mesh).
#[derive(Clone)]
pub struct MeshHandle {
    pub(crate) ctx: Arc<TaskCtx>,
}

impl MeshHandle {
    /// Registers a handler for signals of the given `kind`.
    ///
    /// Returns an `mpsc::Receiver<Signal>` with the default channel depth (256).
    #[must_use]
    pub fn signal_rx(&self, kind: impl Into<Arc<str>>) -> mpsc::Receiver<Signal> {
        self.ctx.signal_handlers.register(kind.into())
    }

    /// Like [`signal_rx`](Self::signal_rx) with an explicit channel depth.
    #[must_use]
    pub fn signal_rx_with_capacity(&self, kind: impl Into<Arc<str>>, cap: usize) -> mpsc::Receiver<Signal> {
        self.ctx.signal_handlers.register_with_capacity(kind.into(), cap)
    }

    /// Like [`signal_rx`](Self::signal_rx) but only delivers signals from `trusted` senders.
    #[must_use]
    pub fn signal_rx_from(
        &self,
        kind:    impl Into<Arc<str>>,
        trusted: Vec<crate::node_id::NodeId>,
    ) -> mpsc::Receiver<Signal> {
        self.ctx.signal_handlers.register_from(kind.into(), trusted)
    }

    /// Emits a signal to the cluster.
    #[must_use]
    pub fn emit(
        &self,
        kind:    impl Into<Arc<str>>,
        scope:   SignalScope,
        payload: impl Into<Bytes>,
    ) -> bool {
        emit_signal(&self.ctx, kind.into(), scope, payload.into())
    }

    /// Like [`emit`](Self::emit) but stamps an HLC sequence number for causal ordering.
    #[must_use]
    pub fn emit_ordered(
        &self,
        kind:    impl Into<Arc<str>>,
        scope:   SignalScope,
        payload: impl Into<Bytes>,
    ) -> bool {
        super::helpers::emit_signal_ordered(&self.ctx, kind.into(), scope, payload.into())
    }

    /// Like [`emit`](Self::emit), but awaits channel capacity instead of dropping on full.
    #[must_use]
    pub async fn emit_async(
        &self,
        kind:    impl Into<Arc<str>>,
        scope:   SignalScope,
        payload: impl Into<Bytes>,
    ) -> bool {
        super::helpers::emit_signal_async(&self.ctx, kind.into(), scope, payload.into()).await
    }

    /// Joins a named **signal boundary group**, publishing membership to `grp/{group}/{node}`.
    ///
    /// Signal groups control **routing**: `SignalScope::Group(name)` delivers only to members of
    /// this group. Membership is managed explicitly by the caller via `join_group`/`leave_group`.
    ///
    /// This is distinct from **capability groups** (`CapabilitiesHandle::define_capability_group`),
    /// which control **emergent discovery**: nodes self-join based on matching their own capability
    /// set against a `CapabilityGroupDef` filter — no explicit `join_group` call is needed.
    pub fn join_group(&self, group: impl Into<Arc<str>>) {
        let group: Arc<str> = group.into();
        let inserted = self.ctx.signal_boundary.write().groups.insert(group.clone());
        if inserted {
            let key = crate::signal::grp_member_key(&group, &self.ctx.node_id);
            let _ = kv_set(&self.ctx, Arc::from(key.as_str()), Bytes::from_static(b"1"));
        }
    }

    /// Leaves a named boundary group, tombstoning the KV membership entry.
    pub fn leave_group(&self, group: impl Into<Arc<str>>) {
        let group: Arc<str> = group.into();
        let removed = self.ctx.signal_boundary.write().groups.remove(&group);
        if removed {
            let key = crate::signal::grp_member_key(&group, &self.ctx.node_id);
            let _ = kv_delete(&self.ctx, Arc::from(key.as_str()));
        }
    }

    /// Returns live members of `group` from Layer I (`grp/{group}/`).
    pub fn group_members(&self, group: &str) -> Vec<crate::node_id::NodeId> {
        group_members_ctx(&self.ctx, group)
    }

    /// Awaits the first locally-admitted signal of `kind` satisfying `predicate`.
    pub fn signal_once<F>(
        &self,
        kind:      impl Into<Arc<str>>,
        timeout:   Duration,
        predicate: F,
    ) -> impl std::future::Future<Output = Option<Signal>>
    where
        F: Fn(&Signal) -> bool,
    {
        let mut rx = self.ctx.signal_handlers.register_with_capacity(kind.into(), 256);
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
        let mut rx = self.ctx.signal_handlers.register_with_capacity(result_kind.into(), 256);
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

    /// Periodically emits `kind` on `scope` every `interval`.
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
        let mut shutdown_rx = self.ctx.shutdown_tx.subscribe();
        let ctx: Arc<TaskCtx> = Arc::clone(&self.ctx);
        let kind: Arc<str>    = kind.into();

        self.ctx.spawn_task(async move {
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

        AdvertiseHandle { _cancel: cancel_tx }
    }

    /// Like [`advertise`](Self::advertise) but also writes payload to Layer I on every tick.
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
        let shutdown_rx = self.ctx.shutdown_tx.subscribe();

        let ctx:             Arc<TaskCtx> = Arc::clone(&self.ctx);
        let kind: Arc<str>   = kind.into();
        let kv_key: Arc<str> = Arc::from(format!("svc/{}/{}", kind, ctx.node_id).as_str());
        let payload_arc: PersistPayloadFn = Arc::new(payload_fn);
        let on_tick: PersistOnTickFn = {
            let kind = kind.clone();
            Arc::new(move |ctx, payload| {
                emit_signal(ctx, kind.clone(), scope.clone(), payload.clone());
            })
        };

        self.ctx.spawn_task(run_kv_persist_task(
            ctx, cancel_rx, shutdown_rx, kv_key, interval, payload_arc, Some(on_tick),
        ));

        AdvertiseHandle { _cancel: cancel_tx }
    }

    /// Returns when this node last admitted a signal of `kind`.
    pub fn last_signal(&self, kind: &str) -> Option<Instant> {
        self.ctx.signal_handlers.last_signal(kind)
    }

    /// Returns the age of the most recently seen evidence of `kind` in `sys/quorum/`.
    pub fn last_signal_persistent(&self, kind: &str) -> Option<Duration> {
        use std::time::{SystemTime, UNIX_EPOCH};
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let prefix = format!("{}{}/", kv_ns::QUORUM, kind);
        kv_scan_prefix(&self.ctx, &prefix)
            .into_iter()
            .filter_map(|(_, bytes)| {
                if bytes.len() < 8 { return None; }
                let written_at = u64::from_le_bytes(bytes[..8].try_into().ok()?);
                now_ms.checked_sub(written_at)
            })
            .min()
            .map(Duration::from_millis)
    }

    /// Suppresses local delivery of `kind` signals for `duration`.
    pub fn suppress(&self, kind: impl Into<Arc<str>>, duration: Duration) {
        self.ctx.signal_handlers.suppress(kind.into(), Instant::now() + duration);
    }

    /// Lifts a suppression set by [`suppress`](Self::suppress) before it expires.
    pub fn unsuppress(&self, kind: &str) {
        self.ctx.signal_handlers.unsuppress(kind);
    }

    /// Returns `true` if `kind` is currently suppressed on this node.
    pub fn is_suppressed(&self, kind: &str) -> bool {
        self.ctx.signal_handlers.is_suppressed(kind)
    }

    /// Watches `kind` for staleness, calling `on_stale` whenever the signal has not
    /// been delivered for longer than `threshold`.
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
        let mut shutdown_rx    = self.ctx.shutdown_tx.subscribe();
        let signal_handlers    = Arc::clone(&self.ctx.signal_handlers);
        let kv_watch           = Arc::clone(&self.ctx.kv_state);
        let kind: Arc<str>     = kind.into();
        let check_interval     = (threshold / 4).max(Duration::from_millis(100));

        self.ctx.spawn_task(async move {
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

        WatchHandle { _cancel: cancel_tx }
    }

    /// Returns `true` when at least `min_senders` distinct nodes have had a signal of
    /// `kind` delivered within `window`.
    pub fn quorum(&self, kind: &str, min_senders: usize, window: Duration) -> bool {
        self.ctx.signal_handlers.quorum(kind, min_senders, window)
    }

    /// Like [`quorum`](Self::quorum) but only counts current members of `group`.
    pub fn group_quorum(
        &self,
        group:       &str,
        kind:        &str,
        min_senders: usize,
        window:      Duration,
    ) -> bool {
        use ahash::AHashSet;
        let member_hashes: AHashSet<u64> = group_members_ctx(&self.ctx, group)
            .iter()
            .map(|n| n.id_hash())
            .collect();
        self.ctx.signal_handlers.quorum_for_group(kind, &member_hashes, min_senders, window)
    }

    /// Like [`group_quorum`](Self::group_quorum) but accepts a pre-built member hash set.
    pub fn group_quorum_prehashed(
        &self,
        member_hashes: &ahash::AHashSet<u64>,
        kind:          &str,
        min_senders:   usize,
        window:        Duration,
    ) -> bool {
        self.ctx.signal_handlers.quorum_for_group(kind, member_hashes, min_senders, window)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::signal::{Signal, SignalHandlers, SignalScope};
    use crate::{GossipAgent, GossipConfig, NodeId};
    use bytes::Bytes;
    use std::{sync::Arc, time::Duration};

    fn make_agent() -> GossipAgent {
        GossipAgent::new(NodeId::new("127.0.0.1", 0).unwrap(), GossipConfig::default())
    }

    // ── quorum ────────────────────────────────────────────────────────────

    #[test]
    fn quorum_false_initially() {
        let agent = make_agent();
        assert!(!agent.mesh().quorum("contract.available", 1, Duration::from_secs(10)));
    }

    #[test]
    fn quorum_true_after_delivery() {
        let agent = make_agent();
        let _ = agent.mesh().emit("contract.available", SignalScope::System, Bytes::new());
        assert!(
            agent.mesh().quorum("contract.available", 1, Duration::from_secs(10)),
            "quorum(k, 1, 10s) must be true after one delivery",
        );
    }

    #[test]
    fn quorum_distinct_senders() {
        let handlers = SignalHandlers::new(Duration::from_secs(600));
        let kind: Arc<str> = Arc::from("test.quorum.distinct");
        let sender_a = NodeId::new("127.0.0.1", 1001).unwrap();
        let sender_b = NodeId::new("127.0.0.1", 1002).unwrap();
        let sig = |sender: NodeId, nonce: u64| Signal {
            kind: kind.clone(), scope: SignalScope::System,
            payload: Bytes::new(), sender, nonce,
        };
        handlers.deliver(&sig(sender_a.clone(), 1));
        handlers.deliver(&sig(sender_a.clone(), 2));
        assert!(
            !handlers.quorum(&kind, 2, Duration::from_secs(10)),
            "two signals from the same sender must not satisfy quorum(k, 2)",
        );
        handlers.deliver(&sig(sender_b, 3));
        assert!(
            handlers.quorum(&kind, 2, Duration::from_secs(10)),
            "two distinct senders must satisfy quorum(k, 2)",
        );
    }

    // ── pheromone trail ───────────────────────────────────────────────────

    #[test]
    fn pheromone_trail_write_read_and_evaporate() {
        let agent = make_agent();
        let load_key = format!("{}worker-1", kv_ns::LOAD);
        let _ = agent.kv().set(load_key.clone(), b"queue=0".to_vec());
        let trails = agent.kv().scan_prefix(kv_ns::LOAD);
        assert_eq!(trails.len(), 1);
        assert_eq!(trails[0].1, Bytes::from_static(b"queue=0"));
        let _ = agent.kv().set(load_key.clone(), b"queue=3".to_vec());
        assert_eq!(agent.kv().scan_prefix(kv_ns::LOAD).len(), 1,
                   "update overwrites in place — store has one entry per worker");
        let _ = agent.kv().delete(load_key);
        assert_eq!(agent.kv().scan_prefix(kv_ns::LOAD).len(), 0,
                   "tombstone evaporates pheromone trail");
    }

    // ── group join/leave ──────────────────────────────────────────────────

    #[test]
    fn join_group_idempotent() {
        let agent = make_agent();
        agent.mesh().join_group("nlp");
        agent.mesh().join_group("nlp");
        let _rx = agent.mesh().signal_rx("t");
        let _ = agent.mesh().emit("t", SignalScope::Group(Arc::from("nlp")), b"ok".to_vec());
        let key = format!("grp/nlp/{}", agent.node_id());
        assert_eq!(agent.kv().get(&key), Some(Bytes::from_static(b"1")), "join is still reflected in store");
    }

    #[test]
    fn leave_group_idempotent() {
        let agent = make_agent();
        agent.mesh().join_group("compute");
        agent.mesh().leave_group("compute");
        agent.mesh().leave_group("compute");
        let key = format!("grp/compute/{}", agent.node_id());
        assert_eq!(agent.kv().get(&key), None, "tombstone stands after double leave");
    }

    #[tokio::test]
    async fn join_group_published_to_store() {
        let agent = make_agent();
        agent.mesh().join_group("compute");
        let key = format!("grp/compute/{}", agent.node_id());
        assert_eq!(agent.kv().get(&key), Some(Bytes::from_static(b"1")));
    }

    #[tokio::test]
    async fn leave_group_tombstones_store_entry() {
        let agent = make_agent();
        agent.mesh().join_group("compute");
        agent.mesh().leave_group("compute");
        let key = format!("grp/compute/{}", agent.node_id());
        assert_eq!(agent.kv().get(&key), None, "leave_group should tombstone the membership key");
    }
}
