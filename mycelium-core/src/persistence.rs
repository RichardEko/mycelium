//! Local KV persistence: append-only WAL + periodic snapshot.
//!
//! Each node writes under `{base_path}/{node_id}/kv/`:
//! - `wal.bin`       — length-prefixed [`SyncEntry`] records
//! - `snapshot.bin`  — last compacted full store snapshot
//! - `snapshot.tmp`  — in-progress write; atomically renamed on completion
//!
//! [`WalHandle`] is stored in `TaskCtx` and cloned into `ConnContext`.
//! `store.rs` and `framing.rs` are not modified — no circular imports.

use crate::config::SyncMode;
use crate::framing::SyncEntry;
use crate::node_id::NodeId;
use crate::serde_fixint as codec;
use crate::store::{apply_and_notify, KvState};
use bytes::{BufMut, BytesMut};
use serde::{Deserialize, Serialize};
use std::{
    io::{self},
    path::PathBuf,
    sync::Arc,
    time::Duration,
};
use tokio::{
    fs as tfs,
    io::{AsyncSeekExt, AsyncWriteExt},
    sync::{mpsc, oneshot},
    time,
};
use tracing::{error, warn};

// ── Data-at-rest encryption hook (WS3 crown-jewel) ────────────────────────────

/// Operator-supplied envelope cipher for KV data **at rest** — the WAL records
/// and snapshot blobs this node writes to disk.
///
/// The substrate stays deliberately neutral on key custody: implement this trait
/// over your own KMS / keyring / HSM and attach it with
/// `GossipAgent::with_data_at_rest_cipher`.
/// When no cipher is attached, bytes are written in the clear (unchanged
/// behaviour, zero overhead).
///
/// Scope: this protects the **on-disk** persistence surface only. Data in transit
/// is protected separately by the `tls` feature (mTLS); data in memory is not
/// encrypted. A node must use a cipher whose key is stable across restarts, or it
/// cannot replay its own WAL/snapshot — key rotation is the operator's concern.
pub trait DataAtRestCipher: Send + Sync {
    /// Encrypt a plaintext blob for storage. Called once per WAL record and once
    /// per snapshot. The returned ciphertext is length-framed verbatim on disk.
    fn encrypt(&self, plaintext: &[u8]) -> Vec<u8>;
    /// Decrypt a blob read from disk. Return `None` on authentication or format
    /// failure — the record is then treated as corrupt and skipped, exactly as a
    /// truncated/garbled plaintext record would be.
    fn decrypt(&self, ciphertext: &[u8]) -> Option<Vec<u8>>;
}

/// Optional reference passed through the persistence paths.
type Cipher<'a> = Option<&'a Arc<dyn DataAtRestCipher>>;

// ── On-disk snapshot format ──────────────────────────────────────────────────

#[derive(Serialize, Deserialize)]
pub struct KvSnapshot {
    /// HLC timestamp of the most-recent entry included in this snapshot.
    /// WAL replay skips entries with `timestamp <= snapshot_hlc`.
    pub snapshot_hlc: u64,
    pub entries: Vec<SyncEntry>,
}

// ── WAL record size cap ──────────────────────────────────────────────────────

const MAX_RECORD_BYTES: usize = 64 * 1024 * 1024;

// ── Channel messages ─────────────────────────────────────────────────────────

pub enum WalMsg {
    Append {
        entry: SyncEntry,
        /// `Some` → caller awaits fsync result.
        /// `None` → fire-and-forget.
        ack: Option<oneshot::Sender<io::Result<()>>>,
    },
    TriggerSnapshot {
        ack: oneshot::Sender<io::Result<()>>,
    },
    #[allow(dead_code)]
    Shutdown,
}

// ── Public handle ────────────────────────────────────────────────────────────

pub struct WalHandle {
    tx:        mpsc::Sender<WalMsg>,
    sync_mode: SyncMode,
}

impl WalHandle {
    /// Append and — in `Flush` mode — await the `fdatasync` ack.
    pub async fn append(&self, entry: SyncEntry) -> io::Result<()> {
        match self.sync_mode {
            SyncMode::Flush => {
                let (tx, rx) = oneshot::channel();
                let _ = self.tx.send(WalMsg::Append { entry, ack: Some(tx) }).await;
                rx.await.unwrap_or(Ok(()))
            }
            SyncMode::Async | SyncMode::Os => {
                let _ = self.tx.try_send(WalMsg::Append { entry, ack: None });
                Ok(())
            }
        }
    }

    /// Fire-and-forget for synchronous callers (`set` / `delete`).
    /// Never awaits fsync. Silently drops if the channel is full —
    /// consistent with `GossipAgent::set`'s existing try_send semantics.
    pub fn append_try(&self, entry: SyncEntry) {
        let _ = self.tx.try_send(WalMsg::Append { entry, ack: None });
    }

    /// Always awaits `fdatasync` regardless of `sync_mode`.
    /// Used for consensus committed-slot writes.
    pub async fn append_sync(&self, entry: SyncEntry) -> io::Result<()> {
        let (tx, rx) = oneshot::channel();
        let _ = self.tx.send(WalMsg::Append { entry, ack: Some(tx) }).await;
        rx.await.unwrap_or(Ok(()))
    }

    /// Ask the writer to snapshot immediately. Awaits completion.
    pub async fn trigger_snapshot(&self) -> io::Result<()> {
        let (tx, rx) = oneshot::channel();
        let _ = self.tx.send(WalMsg::TriggerSnapshot { ack: tx }).await;
        rx.await.unwrap_or(Ok(()))
    }

    #[allow(dead_code)]
    pub async fn shutdown(&self) {
        let _ = self.tx.send(WalMsg::Shutdown).await;
    }
}

// ── Startup replay ───────────────────────────────────────────────────────────

/// Reads `snapshot.bin` and `wal.bin` from `dir`, calls `apply_fn` for each
/// entry, and returns the highest HLC timestamp seen.
///
/// `apply_fn` is responsible for `intern_key` (if configured) and
/// `apply_and_notify` — keeping `persistence.rs` free of agent-layer imports.
pub async fn replay<F>(
    dir: &std::path::Path,
    cipher: Cipher<'_>,
    mut apply_fn: F,
) -> io::Result<u64>
where
    F: FnMut(SyncEntry),
{
    let mut max_ts: u64 = 0;
    let snapshot_path = dir.join("snapshot.bin");
    let wal_path      = dir.join("wal.bin");

    // 1. Snapshot ─────────────────────────────────────────────────────────────
    let snapshot_hlc = if snapshot_path.exists() {
        match tfs::read(&snapshot_path).await {
            Ok(raw) => {
                // Decrypt the snapshot blob if a cipher is configured; a decrypt
                // failure is treated like a corrupt snapshot (skipped).
                let decrypted = match cipher {
                    Some(c) => c.decrypt(&raw),
                    None    => Some(raw.to_vec()),
                };
                let bytes = match decrypted {
                    Some(b) => b,
                    None => {
                        warn!("persistence: snapshot.bin failed to decrypt, skipping");
                        Vec::new()
                    }
                };
                match codec::from_slice::<KvSnapshot>(&bytes) {
                    Ok(snap) => {
                        let hlc = snap.snapshot_hlc;
                        for entry in snap.entries {
                            if entry.timestamp > max_ts { max_ts = entry.timestamp; }
                            apply_fn(entry);
                        }
                        hlc
                    }
                    Err(e) => {
                        warn!("persistence: corrupt snapshot.bin, skipping: {e}");
                        0
                    }
                }
            }
            Err(e) => {
                warn!("persistence: failed to read snapshot.bin: {e}");
                0
            }
        }
    } else {
        0
    };

    // 2. WAL ──────────────────────────────────────────────────────────────────
    if wal_path.exists() {
        match tfs::read(&wal_path).await {
            Ok(bytes) => {
                let mut pos = 0usize;
                while pos + 4 <= bytes.len() {
                    let len = u32::from_le_bytes([
                        bytes[pos], bytes[pos+1], bytes[pos+2], bytes[pos+3],
                    ]) as usize;
                    pos += 4;
                    if len == 0 || len > MAX_RECORD_BYTES { break; }
                    if pos + len > bytes.len()            { break; } // truncated tail
                    let record_bytes = &bytes[pos..pos + len];
                    pos += len;
                    // Decrypt the record if a cipher is configured; a decrypt
                    // failure is a corrupt tail — stop, matching the decode-Err path.
                    let decrypted = match cipher {
                        Some(c) => match c.decrypt(record_bytes) {
                            Some(b) => b,
                            None    => break,
                        },
                        None => record_bytes.to_vec(),
                    };
                    match codec::from_slice::<SyncEntry>(&decrypted) {
                        Ok(entry) if entry.timestamp > snapshot_hlc => {
                            if entry.timestamp > max_ts { max_ts = entry.timestamp; }
                            apply_fn(entry);
                        }
                        Ok(_)    => {} // already covered by snapshot
                        Err(_)   => break, // corrupt tail — stop
                    }
                }
            }
            Err(e) => warn!("persistence: failed to read wal.bin: {e}"),
        }
    }

    Ok(max_ts)
}

// ── WalWriter task ───────────────────────────────────────────────────────────

#[allow(clippy::too_many_arguments)]
/// Hook the WAL snapshot loop consults each interval tick to decide whether to
/// defer a scheduled snapshot (e.g. when the node is already opaque for load
/// reasons, to avoid piling snapshot opacity on top). `None` — pure-core embeds —
/// never defers. Core provides the mechanism; the opacity policy is supplied by the
/// upper layer, so core stays unaware of `sys/load/` semantics (Layer II).
pub type SnapshotDeferHook = Arc<dyn Fn() -> bool + Send + Sync>;

#[allow(clippy::too_many_arguments)]
pub fn spawn_wal_writer(
    dir:                    PathBuf,
    sync_mode:              SyncMode,
    snapshot_wal_threshold: usize,
    snapshot_interval_secs: u64,
    kv_state:               Arc<KvState>,
    node_id:                NodeId,
    hlc:                    Arc<crate::hlc::Hlc>,
    default_ttl:            u8,
    cipher:                 Option<Arc<dyn DataAtRestCipher>>,
    defer_snapshot:         Option<SnapshotDeferHook>,
) -> WalHandle {
    let channel_depth = (snapshot_wal_threshold * 4).max(1024);
    let (tx, rx) = mpsc::channel::<WalMsg>(channel_depth);
    let handle = WalHandle { tx, sync_mode };

    tokio::spawn(wal_writer_task(
        rx,
        dir,
        sync_mode,
        snapshot_wal_threshold,
        snapshot_interval_secs,
        kv_state,
        node_id,
        hlc,
        default_ttl,
        cipher,
        defer_snapshot,
    ));

    handle
}

#[allow(clippy::too_many_arguments)]
async fn wal_writer_task(
    mut rx:                 mpsc::Receiver<WalMsg>,
    dir:                    PathBuf,
    sync_mode:              SyncMode,
    snapshot_wal_threshold: usize,
    snapshot_interval_secs: u64,
    kv_state:               Arc<KvState>,
    node_id:                NodeId,
    hlc:                    Arc<crate::hlc::Hlc>,
    default_ttl:            u8,
    cipher:                 Option<Arc<dyn DataAtRestCipher>>,
    defer_snapshot:         Option<SnapshotDeferHook>,
) {
    let wal_path = dir.join("wal.bin");
    let mut wal_file = match open_wal(&wal_path).await {
        Ok(f)  => f,
        Err(e) => { error!("persistence: failed to open wal.bin: {e}"); return; }
    };
    let mut wal_entry_count: usize = 0;

    let interval = Duration::from_secs(snapshot_interval_secs);
    let mut snap_timer = time::interval(interval);
    snap_timer.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
    snap_timer.tick().await; // consume immediate first tick

    loop {
        tokio::select! {
            biased;

            msg = rx.recv() => {
                match msg {
                    // Channel closed (WalHandle dropped) or explicit Shutdown:
                    // snapshot and exit.
                    None | Some(WalMsg::Shutdown) => {
                        let _ = do_snapshot(&dir, &kv_state, &node_id, &hlc, default_ttl, &mut wal_file, cipher.as_ref()).await;
                        break;
                    }
                    Some(WalMsg::Append { entry, ack }) => {
                        let result = wal_append(&mut wal_file, &entry, sync_mode, cipher.as_ref()).await;
                        wal_entry_count += 1;
                        if let Some(ack) = ack { let _ = ack.send(result); }
                        if wal_entry_count >= snapshot_wal_threshold {
                            let _ = do_snapshot(&dir, &kv_state, &node_id, &hlc, default_ttl, &mut wal_file, cipher.as_ref()).await;
                            wal_entry_count = 0;
                        }
                    }
                    Some(WalMsg::TriggerSnapshot { ack }) => {
                        let result = do_snapshot(&dir, &kv_state, &node_id, &hlc, default_ttl, &mut wal_file, cipher.as_ref()).await;
                        wal_entry_count = 0;
                        let _ = ack.send(result);
                    }
                }
            }

            _ = snap_timer.tick() => {
                // Defer if already opaque for another reason to avoid piling
                // snapshot opacity on top of existing load-based opacity. The
                // opacity check is injected (Layer II policy); core stays neutral.
                if defer_snapshot.as_ref().is_some_and(|f| f()) {
                    snap_timer.reset_after(Duration::from_secs(30));
                    continue;
                }
                let _ = do_snapshot(&dir, &kv_state, &node_id, &hlc, default_ttl, &mut wal_file, cipher.as_ref()).await;
                wal_entry_count = 0;
            }
        }
    }
}

// ── WAL I/O ──────────────────────────────────────────────────────────────────

async fn open_wal(path: &std::path::Path) -> io::Result<tfs::File> {
    tfs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .await
}

async fn wal_append(
    file:      &mut tfs::File,
    entry:     &SyncEntry,
    sync_mode: SyncMode,
    cipher:    Cipher<'_>,
) -> io::Result<()> {
    // Encode the record, then optionally encrypt the payload. The length prefix
    // frames whatever lands on disk (ciphertext when a cipher is configured).
    let mut payload: Vec<u8> = codec::to_vec(entry)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    if let Some(c) = cipher {
        payload = c.encrypt(&payload);
    }

    // Build [u32 LE length][payload] in one buffer.
    let mut buf = BytesMut::with_capacity(payload.len() + 4);
    buf.put_u32_le(payload.len() as u32);
    buf.extend_from_slice(&payload);

    file.write_all(&buf).await?;
    if sync_mode == SyncMode::Flush {
        file.sync_data().await?;
    }
    Ok(())
}

// ── Snapshot ─────────────────────────────────────────────────────────────────

#[allow(clippy::too_many_arguments)]
async fn do_snapshot(
    dir:         &std::path::Path,
    kv_state:    &Arc<KvState>,
    node_id:     &NodeId,
    hlc:         &Arc<crate::hlc::Hlc>,
    default_ttl: u8,
    wal_file:    &mut tfs::File,
    cipher:      Cipher<'_>,
) -> io::Result<()> {
    let opacity_key: Arc<str> = Arc::from(format!(
        "{}{}{}",
        crate::signal::kv_ns::LOAD,
        node_id,
        "/persistence",
    ));

    // 1. Raise opacity.
    let opaque_val = crate::signal::encode_load_state(&crate::signal::LoadState {
        fill_ratio:    1.0,
        is_opaque:     true,
        written_at_ms: crate::hlc::physical_ms(hlc.current()),
    });
    let raise_upd = crate::framing::make_gossip_update(
        node_id, default_ttl, Arc::clone(&opacity_key), opaque_val, false, hlc,
    );
    apply_and_notify(kv_state, &raise_upd);

    // 2. Scan store.
    let snapshot_hlc = hlc.current();
    let entries: Vec<SyncEntry> = {
        let guard = kv_state.store.pin();
        guard.iter()
            // Include TOMBSTONES, not just live entries. The in-memory store retains a tombstone for
            // a propagation window (the GC sweeps only older ones, tasks.rs), so a delete is remembered
            // long enough to reach every peer. The old `filter_map` on `v.data` dropped every tombstone
            // from the snapshot and then truncated the WAL, so after a restart the deleted key existed
            // NOWHERE on disk — and a stale peer that missed the delete resurrected it via anti-entropy
            // (no tombstone to win the LWW tie). Persist what the store holds: the GC has already
            // bounded the tombstone set, so this is exactly the in-window anti-resurrection set. Replay
            // re-applies `is_tombstone` (lifecycle.rs apply_fn). Audit 2026-07-15 pass 3.
            .map(|(k, v)| SyncEntry {
                key:          Arc::clone(k),
                value:        v.data.clone().unwrap_or_default(),
                timestamp:    v.timestamp,
                is_tombstone: v.data.is_none(),
            })
            .collect()
    };

    // 3. Write snapshot.tmp → fdatasync → rename to snapshot.bin.
    let tmp_path  = dir.join("snapshot.tmp");
    let snap_path = dir.join("snapshot.bin");
    let snap = KvSnapshot { snapshot_hlc, entries };
    let encoded = {
        let buf = codec::to_vec(&snap)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        match cipher {
            Some(c) => c.encrypt(&buf),
            None    => buf,
        }
    };
    tfs::write(&tmp_path, &encoded).await?;
    {
        let f = tfs::File::open(&tmp_path).await?;
        f.sync_data().await?;
    }
    tfs::rename(&tmp_path, &snap_path).await?;

    // 4. Truncate WAL.
    wal_file.seek(std::io::SeekFrom::Start(0)).await?;
    wal_file.set_len(0).await?;
    wal_file.sync_data().await?;

    // 5. Lower opacity — tombstone the persistence key.
    let lower_upd = crate::framing::make_gossip_update(
        node_id, default_ttl, opacity_key, bytes::Bytes::new(), true, hlc,
    );
    apply_and_notify(kv_state, &lower_upd);

    Ok(())
}

#[cfg(test)]
mod persist_tests {
    use super::*;
    use crate::framing::{make_gossip_update, GossipUpdate};
    use crate::store::KvState;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn unique_dir() -> std::path::PathBuf {
        static N: AtomicU64 = AtomicU64::new(0);
        let d = std::env::temp_dir().join(format!(
            "myc-persist-{}-{}", std::process::id(), N.fetch_add(1, Ordering::Relaxed),
        ));
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    #[tokio::test]
    async fn regression_snapshot_retains_tombstone_no_resurrection_across_restart() {
        // Audit 2026-07-15 pass 3: do_snapshot dropped ALL tombstones then truncated the WAL, so a
        // deleted key existed NOWHERE on disk — after a restart a stale peer's ancient value
        // resurrected it (no tombstone to win the LWW tie). The snapshot must retain the tombstone.
        let dir  = unique_dir();
        let node = NodeId::new("127.0.0.1", 1).unwrap();
        let hlc  = Arc::new(crate::hlc::Hlc::new());

        // Source store: write "k", then delete it → a fresh-HLC tombstone.
        let src = KvState::new(0);
        apply_and_notify(&src, &make_gossip_update(&node, 1, Arc::from("k"), bytes::Bytes::from_static(b"v1"), false, &hlc));
        apply_and_notify(&src, &make_gossip_update(&node, 1, Arc::from("k"), bytes::Bytes::new(), true, &hlc));

        // Snapshot to disk (truncates the WAL), then replay into a FRESH store — a restart.
        let mut wal = tfs::OpenOptions::new().create(true).read(true).write(true)
            .open(dir.join("wal.log")).await.unwrap();
        do_snapshot(&dir, &src, &node, &hlc, 1, &mut wal, None).await.unwrap();

        let restored = KvState::new(0);
        {
            let r = Arc::clone(&restored);
            let apply = move |e: SyncEntry| {
                apply_and_notify(&r, &GossipUpdate {
                    nonce: crate::framing::ANTI_ENTROPY_NONCE, sender: 0, ttl: 1,
                    is_tombstone: e.is_tombstone, timestamp: e.timestamp, key: e.key, value: e.value,
                });
            };
            replay(&dir, None, apply).await.unwrap();
        }

        // The tombstone survived: "k" present as a tombstone (data None), NOT absent.
        let after = restored.store.pin().get("k").map(|e| e.data.is_none());
        assert_eq!(after, Some(true), "snapshot+replay must retain the tombstone, not drop it");

        // A stale peer re-delivers the ancient value → must NOT resurrect (tombstone wins LWW).
        apply_and_notify(&restored, &GossipUpdate {
            nonce: 7, sender: 0, ttl: 1, is_tombstone: false,
            timestamp: crate::hlc::pack(1, 0), key: Arc::from("k"), value: bytes::Bytes::from_static(b"v1"),
        });
        assert!(restored.store.pin().get("k").unwrap().data.is_none(),
            "an ancient replayed write must not resurrect the deleted key");

        std::fs::remove_dir_all(&dir).ok();
    }
}
