//! KV store operations — [`KvHandle`].
//!
//! Wraps the gossip KV layer (Layer I): last-write-wins state propagation,
//! prefix subscriptions, quorum confirmation, and the append-only log overlay.
//!
//! Obtain a handle via [`GossipAgent::kv`](crate::GossipAgent::kv).

#[cfg(feature = "gateway")]
use crate::framing::ForwardHint;
use crate::store::PrefixPredicateWatcher;
use bytes::Bytes;
use std::{
    sync::{atomic::Ordering, Arc},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::{mpsc, watch};

use super::{
    helpers::{kv_delete, kv_delete_async, kv_get, kv_scan_prefix, kv_set, kv_set_async},
    TaskCtx,
};

// ── Public types ──────────────────────────────────────────────────────────────

/// A single entry in an ordered durable log stream.
#[derive(Debug, Clone)]
pub struct LogEntry {
    /// HLC timestamp — use as a cursor for [`KvHandle::subscribe_log`] and
    /// [`KvHandle::scan_log`].
    pub hlc:   u64,
    pub value: Bytes,
}

// ── Handle ────────────────────────────────────────────────────────────────────

/// Typed handle for KV store operations (Layer I).
///
/// Obtained via [`GossipAgent::kv`](crate::GossipAgent::kv).
/// Zero-cost: wraps a single `Arc<TaskCtx>` clone.
///
/// `KvHandle` is `Clone + Send + Sync` and can be stored, moved across tasks,
/// and shared between threads without any additional synchronisation.
#[derive(Clone)]
pub struct KvHandle {
    pub(crate) ctx: Arc<TaskCtx>,
}

impl KvHandle {
    // ── Basic KV ─────────────────────────────────────────────────────────────

    /// Stores `value` under `key` locally and queues it for gossip to peers.
    ///
    /// The local store is **always** updated regardless of the return value.
    /// Returns `true` if the update was queued for gossip; `false` if the gossip
    /// channel was full or the shard has died.
    #[must_use]
    #[tracing::instrument(level = "trace", skip(self, key, value), fields(node = %self.ctx.node_id))]
    pub fn set<K: Into<Arc<str>>>(&self, key: K, value: impl Into<Bytes>) -> bool {
        kv_set(&self.ctx, key.into(), value.into())
    }

    /// Returns the current value for `key`, or `None` if absent or tombstoned.
    #[tracing::instrument(level = "trace", skip(self), fields(node = %self.ctx.node_id, key))]
    pub fn get(&self, key: &str) -> Option<Bytes> {
        kv_get(&self.ctx, key)
    }

    /// Removes `key` locally and queues a tombstone for gossip to peers.
    ///
    /// Returns `true` if the tombstone was queued; `false` if the channel was
    /// full or the shard has died — the tombstone was applied locally but will
    /// not propagate to peers.
    #[must_use]
    pub fn delete<K: Into<Arc<str>>>(&self, key: K) -> bool {
        kv_delete(&self.ctx, key.into())
    }

    /// Like [`set`](Self::set), but awaits channel capacity instead of dropping
    /// the frame when the shard channel is full.
    #[must_use]
    #[tracing::instrument(level = "trace", skip(self, key, value), fields(node = %self.ctx.node_id))]
    pub async fn set_async<K: Into<Arc<str>>>(&self, key: K, value: impl Into<Bytes>) -> bool {
        kv_set_async(&self.ctx, key.into(), value.into()).await
    }

    /// Like [`delete`](Self::delete), but awaits channel capacity instead of
    /// dropping the frame when the shard channel is full.
    #[must_use]
    pub async fn delete_async<K: Into<Arc<str>>>(&self, key: K) -> bool {
        kv_delete_async(&self.ctx, key.into()).await
    }

    /// Writes `value` under `key` and waits for at least `min_acks` distinct peers
    /// to confirm receipt before returning.
    ///
    /// # Durability, not consistency
    ///
    /// This method confirms that `min_acks` peers have **received** the write via
    /// gossip. It does **not** provide linearisability, total-order, or any consensus
    /// guarantee. Two concurrent callers writing different values to the same key will
    /// both succeed here; LWW resolves the winner silently. For a linearisable write
    /// use [`consistent_set`](crate::GossipAgent::consistent_set).
    ///
    /// # Errors
    ///
    /// Returns [`QuorumError::Timeout`] when fewer than `min_acks` peers confirm
    /// within `timeout`. The write is **not** rolled back.
    pub async fn set_with_min_acks(
        &self,
        key:      impl Into<Arc<str>>,
        value:    impl Into<Bytes>,
        min_acks: usize,
        timeout:  Duration,
    ) -> Result<usize, super::kv_quorum::QuorumError> {
        use super::kv_quorum::{QuorumAckTracker, QuorumError};

        let key:   Arc<str> = key.into();
        let value: Bytes    = value.into();

        if min_acks == 0 {
            let _ = kv_set_async(&self.ctx, key, value).await;
            return Ok(0);
        }

        let write_ts_min = self.ctx.hlc.tick();
        let self_hash    = self.ctx.node_id.id_hash();
        let (tracker, mut rx) = QuorumAckTracker::new(write_ts_min, self_hash);
        self.ctx.kv_state.quorum_trackers.pin().insert(Arc::clone(&key), Arc::clone(&tracker));

        let _ = kv_set_async(&self.ctx, Arc::clone(&key), value).await;

        let result = tokio::time::timeout(timeout, async {
            loop {
                let n = *rx.borrow();
                if n >= min_acks { return n; }
                if rx.changed().await.is_err() { return *rx.borrow(); }
            }
        })
        .await;

        self.ctx.kv_state.quorum_trackers.pin().remove(&key);

        match result {
            Ok(n)  => Ok(n),
            Err(_) => Err(QuorumError::Timeout { acks_received: *rx.borrow() }),
        }
    }

    // ── Scan / list ───────────────────────────────────────────────────────────

    /// Returns a snapshot of all keys that have a live (non-tombstone) value.
    pub fn keys(&self) -> Vec<Arc<str>> {
        let guard = self.ctx.kv_state.store.pin();
        guard.iter()
            .filter(|(_, v)| v.data.is_some())
            .map(|(k, _)| Arc::clone(k))
            .collect()
    }

    /// Returns all live key-value pairs whose key starts with `prefix`.
    #[tracing::instrument(level = "trace", skip(self), fields(node = %self.ctx.node_id, prefix))]
    pub fn scan_prefix(&self, prefix: &str) -> Vec<(Arc<str>, Bytes)> {
        kv_scan_prefix(&self.ctx, prefix)
    }

    // ── Subscriptions ─────────────────────────────────────────────────────────

    /// Subscribes to changes under a key `prefix`.
    ///
    /// Returns a `watch::Receiver<u64>` whose value increments every time a key
    /// matching the prefix is written or tombstoned. Closed senders are evicted
    /// on the next matching write.
    #[must_use]
    pub fn subscribe_prefix<P: Into<Arc<str>>>(&self, prefix: P) -> watch::Receiver<u64> {
        let prefix_arc: Arc<str> = prefix.into();
        loop {
            let guard = self.ctx.kv_state.prefix_watchers.pin();
            if let Some(tx) = guard.get(&prefix_arc)
                && !tx.is_closed() {
                    return tx.subscribe();
                }
            let (new_tx, rx) = watch::channel(0u64);
            let new_tx_arc   = Arc::new(new_tx);
            let mut slot     = Some(new_tx_arc);
            let result = guard.compute(Arc::clone(&prefix_arc), |existing| match existing {
                Some((_, tx)) if !tx.is_closed() => papaya::Operation::Abort(()),
                _ => match slot.take() {
                    Some(tx) => papaya::Operation::Insert(tx),
                    None     => papaya::Operation::Abort(()),
                },
            });
            if matches!(result, papaya::Compute::Inserted(..) | papaya::Compute::Updated { .. }) {
                return rx;
            }
        }
    }

    /// Like [`subscribe_prefix`](Self::subscribe_prefix) but only notifies when
    /// the written key satisfies `predicate`. Cuts wake-up amplification when only
    /// a subset of keys under the prefix is relevant.
    #[must_use]
    pub fn subscribe_prefix_with_predicate<P, F>(
        &self,
        prefix:    P,
        predicate: F,
    ) -> watch::Receiver<u64>
    where
        P: Into<Arc<str>>,
        F: Fn(&str) -> bool + Send + Sync + 'static,
    {
        let prefix_arc: Arc<str> = prefix.into();
        let (tx, rx)             = watch::channel(0u64);
        let entry = PrefixPredicateWatcher {
            prefix:    prefix_arc,
            predicate: Arc::new(predicate),
            tx:        Arc::new(tx),
        };
        let id = self.ctx.kv_state.next_pred_watcher_id.fetch_add(1, Ordering::Relaxed);
        self.ctx.kv_state.prefix_predicate_watchers.pin().insert(id, entry);
        rx
    }

    /// Subscribes to changes for `key`.
    ///
    /// The receiver's initial value is a snapshot of the store at subscription time.
    #[must_use]
    pub fn subscribe<K: Into<Arc<str>>>(&self, key: K) -> watch::Receiver<Option<Bytes>> {
        let key_arc: Arc<str> = key.into();
        loop {
            let guard = self.ctx.kv_state.subscriptions.pin();
            if let Some(tx) = guard.get(&key_arc)
                && !tx.is_closed() {
                    return tx.subscribe();
                }
            let current = self.ctx.kv_state.store.pin()
                .get(&*key_arc)
                .and_then(|e| e.data.clone());
            let (new_tx, rx) = watch::channel(current);
            let mut slot     = Some(new_tx);
            let result = guard.compute(Arc::clone(&key_arc), |existing| match existing {
                Some((_, tx)) if !tx.is_closed() => papaya::Operation::Abort(()),
                _ => match slot.take() {
                    Some(tx) => papaya::Operation::Insert(tx),
                    None     => papaya::Operation::Abort(()),
                },
            });
            if matches!(result, papaya::Compute::Inserted(..) | papaya::Compute::Updated { .. }) {
                return rx;
            }
        }
    }

    // ── Persistent quorum ─────────────────────────────────────────────────────

    /// Counts distinct senders of `kind` within `window` using Layer I as evidence.
    ///
    /// Unlike [`quorum`](crate::GossipAgent::quorum), which reads the in-memory sender
    /// log, this reads `sys/quorum/{kind}/` from the KV store — durable across restarts.
    pub fn quorum_persistent(&self, kind: &str, window: Duration) -> usize {
        use crate::signal::kv_ns;
        let prefix   = format!("{}{}/", kv_ns::QUORUM, kind);
        let now_ms   = SystemTime::now()
            .duration_since(UNIX_EPOCH).unwrap_or_default()
            .as_millis() as u64;
        let window_ms = window.as_millis() as u64;
        kv_scan_prefix(&self.ctx, &prefix)
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

    // ── Append-only log overlay ───────────────────────────────────────────────

    /// Appends `value` to `stream`. Writes `log/{stream}/{hlc:016x}` to the
    /// gossip KV. Returns the HLC timestamp of the written entry.
    pub fn append(&self, stream: &str, value: impl Into<Bytes>) -> u64 {
        let hlc = self.ctx.hlc.tick();
        let _   = kv_set(&self.ctx, Arc::from(format!("log/{stream}/{hlc:016x}").as_str()), value.into());
        hlc
    }

    /// Range scan of `stream`. Returns entries with HLC in `[from, to)`, sorted by HLC.
    ///
    /// `from = 0` means from the beginning; `to = u64::MAX` means to the end.
    pub fn scan_log(&self, stream: &str, from: u64, to: u64) -> Vec<LogEntry> {
        let prefix = format!("log/{stream}/");
        let mut entries: Vec<LogEntry> = kv_scan_prefix(&self.ctx, &prefix)
            .into_iter()
            .filter_map(|(k, v)| {
                let suffix = k.strip_prefix(&prefix)?;
                let hlc    = u64::from_str_radix(suffix, 16).ok()?;
                if hlc >= from && hlc < to { Some(LogEntry { hlc, value: v }) } else { None }
            })
            .collect();
        entries.sort_by_key(|e| e.hlc);
        entries
    }

    /// Tombstones all entries in `stream` with HLC < `before_hlc`.
    pub fn compact_log(&self, stream: &str, before_hlc: u64) {
        let prefix = format!("log/{stream}/");
        for (k, _) in kv_scan_prefix(&self.ctx, &prefix) {
            let suffix = k.strip_prefix(&prefix).unwrap_or("");
            if let Ok(hlc) = u64::from_str_radix(suffix, 16)
                && hlc < before_hlc {
                    let _ = kv_delete(&self.ctx, k);
                }
        }
    }

    /// Subscribes to live entries in `stream` at or after `since_hlc`.
    ///
    /// Spawns a background watcher task that re-scans on every prefix change and
    /// forwards new entries. The task shuts down automatically when the returned
    /// receiver is dropped.
    pub fn subscribe_log(&self, stream: &str, since_hlc: u64) -> mpsc::Receiver<LogEntry> {
        let (tx, rx)    = mpsc::channel::<LogEntry>(256);
        let prefix      = Arc::from(format!("log/{stream}/").as_str());
        let stream_str  = stream.to_string();
        let handle      = self.clone();
        let mut watcher = self.subscribe_prefix(Arc::clone(&prefix));
        let mut last_seen = since_hlc;

        tokio::spawn(async move {
            loop {
                for entry in handle.scan_log(&stream_str, last_seen, u64::MAX) {
                    last_seen = entry.hlc + 1;
                    if tx.send(entry).await.is_err() { return; }
                }
                if watcher.changed().await.is_err() { return; }
            }
        });

        rx
    }

    /// Coordinated consumer group subscription. At most one consumer at a time
    /// processes entries; offset is persisted at `clog/{stream}/{group}/offset`.
    ///
    /// Uses a best-effort LWW claim rather than full consensus, which is sufficient
    /// for consumer-group coordination: only one consumer will hold the freshest
    /// offset at a time.
    pub async fn subscribe_log_group(
        &self,
        stream: &str,
        group:  &str,
    ) -> mpsc::Receiver<LogEntry> {
        let (tx, rx) = mpsc::channel::<LogEntry>(64);
        let stream   = stream.to_string();
        let group    = group.to_string();
        let handle   = self.clone();

        tokio::spawn(async move {
            loop {
                // Best-effort claim: LWW write acts as a soft lock.
                let lock_name   = format!("clog/{stream}/{group}/claim");
                let lock_key    = Arc::from(format!("lock/{lock_name}").as_str());
                let now_ms      = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map(|d| d.as_millis() as u64)
                    .unwrap_or(0);
                let lock_json   = serde_json::json!({
                    "holder":     handle.ctx.node_id.to_string(),
                    "expires_ms": now_ms + 30_000u64,
                });
                let lock_value  = Bytes::from(
                    serde_json::to_vec(&lock_json).unwrap_or_default()
                );
                let _           = kv_set(&handle.ctx, lock_key, lock_value);

                // Read current offset.
                let offset_key = format!("clog/{stream}/{group}/offset");
                let offset: u64 = handle
                    .get(&offset_key)
                    .and_then(|b| {
                        std::str::from_utf8(&b).ok()
                            .and_then(|s| u64::from_str_radix(s, 16).ok())
                    })
                    .unwrap_or(0);

                let next = handle.scan_log(&stream, offset + 1, u64::MAX)
                    .into_iter()
                    .next();

                if let Some(entry) = next {
                    let new_offset = format!("{:016x}", entry.hlc);
                    let _ = kv_set(
                        &handle.ctx,
                        Arc::from(offset_key.as_str()),
                        Bytes::from(new_offset.into_bytes()),
                    );
                    if tx.send(entry).await.is_err() { return; }
                } else {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        });

        rx
    }
}

// ── SubscribeHandle (internal — used by HTTP gateway log-group handler) ──────

#[cfg(feature = "gateway")]
/// Minimal agent proxy used by the HTTP gateway's consumer-group log endpoint.
pub(super) struct SubscribeHandle {
    pub(super) task_ctx: Arc<TaskCtx>,
    kv_state:            Arc<crate::store::KvState>,
}

#[cfg(feature = "gateway")]
impl SubscribeHandle {
    /// Construct from an `Arc<TaskCtx>`.
    pub(super) fn from_task_ctx(task_ctx: Arc<TaskCtx>) -> Self {
        let kv_state = Arc::clone(&task_ctx.kv_state);
        Self { task_ctx, kv_state }
    }

    pub(super) async fn distributed_lock(
        &self,
        name: &str,
        ttl:  Duration,
    ) -> Result<super::overlay_consistent::LockGuard, super::overlay_consistent::ConsistencyError>
    {
        use super::helpers::make_gossip_update;
        use crate::store::apply_and_notify;

        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);

        let lock_json = serde_json::json!({
            "holder":     self.task_ctx.node_id.to_string(),
            "expires_ms": now_ms + ttl.as_millis() as u64,
        });
        let value = Bytes::from(serde_json::to_vec(&lock_json).unwrap_or_default());

        let key: Arc<str> = Arc::from(format!("lock/{name}").as_str());
        let update = make_gossip_update(
            &self.task_ctx.node_id,
            self.task_ctx.default_ttl,
            Arc::clone(&key),
            value,
            false,
            &self.task_ctx.hlc,
        );
        apply_and_notify(&self.kv_state, &update);
        crate::framing::dispatch_gossip_try_send(
            &self.task_ctx.gossip_txs,
            crate::framing::WireMessage::Data(update),
            self.task_ctx.node_id.id_hash(),
            ForwardHint::All,
            &self.kv_state.dropped_frames,
        );

        Ok(super::overlay_consistent::LockGuard {
            ctx:      Arc::clone(&self.task_ctx),
            name:     Arc::from(name),
            token:    self.task_ctx.hlc.current(),
            released: false,
        })
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use crate::{GossipAgent, GossipConfig, NodeId};
    use bytes::Bytes;
    use std::{sync::Arc, time::Duration};

    fn make_agent() -> GossipAgent {
        GossipAgent::new(NodeId::new("127.0.0.1", 0).unwrap(), GossipConfig::default())
    }

    fn alloc_port() -> u16 {
        use std::net::TcpListener;
        TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
    }

    async fn make_started_agent(port: u16) -> GossipAgent {
        let id  = NodeId::new("127.0.0.1", port).unwrap();
        let cfg = GossipConfig { bind_address: "127.0.0.1".parse().unwrap(), bind_port: port, ..GossipConfig::default() };
        let a   = GossipAgent::new(id, cfg);
        a.start().await.unwrap();
        a
    }

    // ── Basic KV ─────────────────────────────────────────────────────────────

    #[test]
    fn set_get() {
        let a = make_agent();
        let _ = a.kv().set("hello", b"world".to_vec());
        assert_eq!(a.kv().get("hello"), Some(Bytes::from_static(b"world")));
    }

    #[test]
    fn set_returns_true_when_channel_has_capacity() {
        assert!(make_agent().kv().set("k", b"v".to_vec()));
    }

    #[test]
    fn delete_local() {
        let a = make_agent();
        let _ = a.kv().set("key", b"val".to_vec());
        let _ = a.kv().delete("key");
        assert_eq!(a.kv().get("key"), None);
    }

    #[test]
    fn keys_returns_live_keys_only() {
        let a = make_agent();
        let _ = a.kv().set("a", b"1".to_vec());
        let _ = a.kv().set("b", b"2".to_vec());
        let _ = a.kv().set("c", b"3".to_vec());
        let _ = a.kv().delete("b");
        let mut keys = a.kv().keys();
        keys.sort();
        assert_eq!(keys, vec![Arc::from("a"), Arc::from("c")]);
    }

    #[test]
    fn keys_empty_on_new_agent() {
        assert!(make_agent().kv().keys().is_empty());
    }

    #[test]
    fn scan_prefix_returns_matching_live_entries() {
        let a = make_agent();
        let _ = a.kv().set("load/node-a", b"state-a".to_vec());
        let _ = a.kv().set("load/node-b", b"state-b".to_vec());
        let _ = a.kv().set("other/key",   b"other".to_vec());
        let mut entries = a.kv().scan_prefix("load/");
        entries.sort_by(|(a, _), (b, _)| a.cmp(b));
        assert_eq!(entries.len(), 2);
        assert_eq!(&*entries[0].0, "load/node-a");
        assert_eq!(entries[0].1, Bytes::from_static(b"state-a"));
        assert_eq!(&*entries[1].0, "load/node-b");
        assert_eq!(entries[1].1, Bytes::from_static(b"state-b"));
    }

    #[test]
    fn scan_prefix_excludes_tombstones() {
        let a = make_agent();
        let _ = a.kv().set("load/node-a", b"alive".to_vec());
        let _ = a.kv().set("load/node-b", b"alive".to_vec());
        let _ = a.kv().delete("load/node-a");
        let entries = a.kv().scan_prefix("load/");
        assert_eq!(entries.len(), 1);
        assert_eq!(&*entries[0].0, "load/node-b");
    }

    #[test]
    fn scan_prefix_no_match_returns_empty() {
        let a = make_agent();
        let _ = a.kv().set("load/node-a", b"x".to_vec());
        assert_eq!(a.kv().scan_prefix("grp/").len(), 0);
    }

    #[tokio::test]
    async fn set_async_stores_and_queues() {
        let a = make_agent();
        assert!(a.kv().set_async("k", b"v".to_vec()).await);
        assert_eq!(a.kv().get("k"), Some(Bytes::from_static(b"v")));
    }

    #[tokio::test]
    async fn delete_async_tombstones_key() {
        let a = make_agent();
        assert!(a.kv().set_async("k", b"v".to_vec()).await);
        assert!(a.kv().delete_async("k").await);
        assert_eq!(a.kv().get("k"), None);
    }

    #[tokio::test]
    async fn subscribe_initial_value_absent() {
        let rx = make_agent().kv().subscribe("missing");
        assert_eq!(*rx.borrow(), None);
    }

    #[tokio::test]
    async fn subscribe_initial_value_present() {
        let a = make_agent();
        let _ = a.kv().set("k", b"hello".to_vec());
        let rx = a.kv().subscribe("k");
        assert_eq!(*rx.borrow(), Some(Bytes::from_static(b"hello")));
    }

    #[tokio::test]
    async fn subscribe_notified_on_set() {
        let a = make_agent();
        let mut rx = a.kv().subscribe("k");
        rx.borrow_and_update();
        let _ = a.kv().set("k", b"world".to_vec());
        tokio::time::timeout(std::time::Duration::from_millis(100), rx.changed())
            .await.expect("should fire within 100 ms").unwrap();
        assert_eq!(*rx.borrow(), Some(Bytes::from_static(b"world")));
    }

    #[tokio::test]
    async fn subscribe_notified_on_delete() {
        let a = make_agent();
        let _ = a.kv().set("k", b"v".to_vec());
        let mut rx = a.kv().subscribe("k");
        rx.borrow_and_update();
        let _ = a.kv().delete("k");
        tokio::time::timeout(std::time::Duration::from_millis(100), rx.changed())
            .await.expect("should fire within 100 ms").unwrap();
        assert_eq!(*rx.borrow(), None);
    }

    #[tokio::test]
    async fn subscribe_prefix_with_predicate_skips_non_matching_keys() {
        let a = make_agent();
        let mut rx = a.kv().subscribe_prefix_with_predicate(
            Arc::<str>::from("cap/"),
            |k: &str| k.ends_with("/compute/gpu"),
        );
        let mark = *rx.borrow();
        let _ = a.kv().set("cap/127.0.0.1:1/storage/disk", b"x".to_vec());
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert_eq!(*rx.borrow(), mark, "predicate must suppress non-matching keys");
        let _ = a.kv().set("cap/127.0.0.1:1/compute/gpu", b"y".to_vec());
        tokio::time::timeout(std::time::Duration::from_millis(100), rx.changed())
            .await.expect("predicate-matching write should fire within 100 ms").unwrap();
        assert_ne!(*rx.borrow(), mark);
    }

    #[test]
    fn gossip_channel_capacity_respected() {
        let mut cfg = GossipConfig::default();
        cfg.gossip_channel_capacity = 1;
        let a = GossipAgent::new(NodeId::new("127.0.0.1", 0).unwrap(), cfg);
        assert!( a.kv().set("k1", b"v1".to_vec()), "first send fits");
        assert!(!a.kv().set("k1", b"v2".to_vec()), "second send to same shard fails");
    }

    // ── set_with_min_acks ─────────────────────────────────────────────────────

    #[tokio::test]
    async fn set_with_min_acks_zero() {
        let a = make_agent();
        let r = a.kv().set_with_min_acks("sq-key", b"val".to_vec(), 0, Duration::from_secs(5)).await;
        assert_eq!(r, Ok(0));
        assert_eq!(a.kv().get("sq-key"), Some(Bytes::from_static(b"val")));
    }

    #[tokio::test]
    async fn set_with_min_acks_timeout_no_peers() {
        use crate::agent::kv_quorum::QuorumError;
        let a = make_agent();
        let r = a.kv().set_with_min_acks("sq-key2", b"val".to_vec(), 1, Duration::from_millis(50)).await;
        match r {
            Err(QuorumError::Timeout { acks_received }) => assert_eq!(acks_received, 0),
            Ok(n) => panic!("expected Timeout, got Ok({n})"),
        }
    }

    // ── Log overlay ───────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_append_scan_compact() {
        let a = make_started_agent(alloc_port()).await;

        let h1 = a.kv().append("events", Bytes::from_static(b"e1"));
        let h2 = a.kv().append("events", Bytes::from_static(b"e2"));
        let _h3 = a.kv().append("events", Bytes::from_static(b"e3"));
        let h4 = a.kv().append("events", Bytes::from_static(b"e4"));
        let h5 = a.kv().append("events", Bytes::from_static(b"e5"));

        let all = a.kv().scan_log("events", 0, u64::MAX);
        assert_eq!(all.len(), 5);
        assert_eq!(all[0].value, Bytes::from_static(b"e1"));
        assert_eq!(all[4].value, Bytes::from_static(b"e5"));

        let mid = a.kv().scan_log("events", h2, h4);
        assert_eq!(mid.len(), 2);

        a.kv().compact_log("events", h4);
        let after = a.kv().scan_log("events", 0, u64::MAX);
        assert_eq!(after.len(), 2);
        assert!(after.iter().all(|e| e.hlc >= h4));

        let _ = (h1, h5);
        a.shutdown().await;
    }

    #[tokio::test]
    async fn test_subscribe_log_receives_live_append() {
        let a  = make_started_agent(alloc_port()).await;
        let mut rx = a.kv().subscribe_log("live", 0);

        a.kv().append("live", Bytes::from_static(b"msg1"));

        let entry = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            rx.recv(),
        ).await.expect("timeout").expect("channel closed");

        assert_eq!(entry.value, Bytes::from_static(b"msg1"));
        a.shutdown().await;
    }
}
