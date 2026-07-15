//! KV store operations — [`KvHandle`].
//!
//! Wraps the gossip KV layer (Layer I): last-write-wins state propagation,
//! prefix subscriptions, and the append-only log overlay.
//!
//! Obtain a handle via `GossipAgent::kv`. The quorum-durability overlay
//! (`set_with_min_acks`) is a Layer-III-flavoured *extension* and lives in the
//! full `mycelium` crate as `KvQuorumExt`, not here — "consistency as a service,
//! not a foundation".

use crate::context::CoreCtx;
use crate::ops::{kv_delete, kv_delete_async, kv_get, kv_scan_prefix, kv_set, kv_set_async};
use crate::store::PrefixPredicateWatcher;
use bytes::Bytes;
use std::{
    sync::{atomic::Ordering, Arc},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::{mpsc, watch};

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
/// Obtained via `GossipAgent::kv`.
/// Zero-cost: wraps a single `Arc<CoreCtx>` clone.
///
/// `KvHandle` is `Clone + Send + Sync` and can be stored, moved across tasks,
/// and shared between threads without any additional synchronisation.
#[derive(Clone)]
pub struct KvHandle {
    pub(crate) ctx: Arc<CoreCtx>,
}

impl KvHandle {
    /// Builds a handle directly from the substrate context. Used by the full
    /// `mycelium` crate's `GossipAgent::kv()` accessor.
    #[doc(hidden)]
    pub fn from_core(ctx: Arc<CoreCtx>) -> Self {
        Self { ctx }
    }

    /// The underlying substrate context. Lets the full crate's `KvQuorumExt`
    /// build the quorum-durability overlay off a core handle.
    #[doc(hidden)]
    pub fn core(&self) -> &Arc<CoreCtx> {
        &self.ctx
    }

    // ── Basic KV ─────────────────────────────────────────────────────────────

    /// Stores `value` under `key` locally and queues it for gossip to peers.
    ///
    /// Returns `true` if the update was queued for gossip. Returns `false` if the
    /// gossip channel was full or the shard has died (the local store is still
    /// updated in those cases — anti-entropy propagates the entry later), **or** if
    /// `key.len() + value.len()` exceeds
    /// [`MAX_KV_WRITE_BYTES`](crate::framing::MAX_KV_WRITE_BYTES), in which case the
    /// write is rejected outright (nothing applied anywhere, a `warn!` is logged):
    /// an entry that large cannot be encoded into a gossip frame and would silently
    /// never propagate. Use the bulk transport for payloads that large.
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

    // The quorum-durability overlay `set_with_min_acks` is an upper-crate
    // extension (`mycelium::KvQuorumExt`): durability-by-ACK-count is a guarantee
    // layered *on* the substrate, not part of it. See src/agent/kv_quorum_ext.rs.

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
        // Salt the key with the node id. Two nodes appending in the same wall-ms both stamp
        // `pack(ms, 0)`; without the salt they collide on ONE key and LWW silently drops one
        // append (and the loss is anti-entropy-digest-invisible) — audit 2026-07-15. The HLC is
        // still the first key segment, so ordering + range scans are unchanged.
        let node = &self.ctx.node_id;
        let _ = kv_set(&self.ctx, Arc::from(format!("log/{stream}/{hlc:016x}/{node}").as_str()), value.into());
        hlc
    }

    /// Range scan of `stream`. Returns entries with HLC in `[from, to)`, sorted by HLC.
    ///
    /// `from = 0` means from the beginning; `to = u64::MAX` means to the end.
    pub fn scan_log(&self, stream: &str, from: u64, to: u64) -> Vec<LogEntry> {
        let prefix = format!("log/{stream}/");
        let mut rows: Vec<(u64, Arc<str>, Bytes)> = kv_scan_prefix(&self.ctx, &prefix)
            .into_iter()
            .filter_map(|(k, v)| {
                let suffix = k.strip_prefix(&prefix)?;
                // HLC is the first key segment; a `/{node}` salt may follow it (see `append`).
                let hlc = u64::from_str_radix(suffix.split('/').next()?, 16).ok()?;
                if hlc >= from && hlc < to { Some((hlc, Arc::from(k.as_ref()), v)) } else { None }
            })
            .collect();
        // Stable total order every node agrees on: (hlc, key). The key carries the node salt, so
        // two same-HLC appends from different nodes get a deterministic order (not scan order).
        rows.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
        rows.into_iter().map(|(hlc, _, value)| LogEntry { hlc, value }).collect()
    }

    /// Tombstones all entries in `stream` with HLC < `before_hlc`.
    pub fn compact_log(&self, stream: &str, before_hlc: u64) {
        let prefix = format!("log/{stream}/");
        for (k, _) in kv_scan_prefix(&self.ctx, &prefix) {
            let suffix = k.strip_prefix(&prefix).unwrap_or("");
            if let Some(hlc) = suffix.split('/').next().and_then(|s| u64::from_str_radix(s, 16).ok())
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

    /// **Single-active** log-group subscription — the contract is *at most one consumer
    /// processes a stream at a time* (an exact-once ordered consumer with failover), **not**
    /// load-balanced work-sharing across the group. For competitive, load-balanced
    /// exactly-once *work distribution* (each item claimed by exactly one of many workers),
    /// use a work queue — the [`mycelium-tuple-space`] companion, which claims each item
    /// atomically. A single shared, advancing offset (this API) fundamentally cannot do
    /// competitive per-item consumption; that is a different pattern. See the
    /// `log-group vs work-queue` architecture note and issue #149.
    ///
    /// **Consistency here is best-effort.** This core path coordinates via an LWW claim, which
    /// is *not* mutually exclusive: under concurrent cross-node consumers it can briefly admit
    /// two active consumers, so an entry may be delivered more than once. For a *consensus*-backed
    /// single-active claim (true exact-once), use the gateway endpoint
    /// `GET /gateway/overlay/log/group/subscribe`. Offset is persisted at `clog/{stream}/{group}/offset`.
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
