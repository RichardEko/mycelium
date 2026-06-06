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
use crate::framing::{bincode_cfg, SyncEntry};
use crate::node_id::NodeId;
use crate::store::{apply_and_notify, KvState};
use bincode::serde as bcode;
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

// ── On-disk snapshot format ──────────────────────────────────────────────────

#[derive(Serialize, Deserialize)]
pub(crate) struct KvSnapshot {
    /// HLC timestamp of the most-recent entry included in this snapshot.
    /// WAL replay skips entries with `timestamp <= snapshot_hlc`.
    pub(crate) snapshot_hlc: u64,
    pub(crate) entries: Vec<SyncEntry>,
}

// ── WAL record size cap ──────────────────────────────────────────────────────

const MAX_RECORD_BYTES: usize = 64 * 1024 * 1024;

// ── Channel messages ─────────────────────────────────────────────────────────

pub(crate) enum WalMsg {
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

pub(crate) struct WalHandle {
    tx:        mpsc::Sender<WalMsg>,
    sync_mode: SyncMode,
}

impl WalHandle {
    /// Append and — in `Flush` mode — await the `fdatasync` ack.
    pub(crate) async fn append(&self, entry: SyncEntry) -> io::Result<()> {
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
    pub(crate) fn append_try(&self, entry: SyncEntry) {
        let _ = self.tx.try_send(WalMsg::Append { entry, ack: None });
    }

    /// Always awaits `fdatasync` regardless of `sync_mode`.
    /// Used for consensus committed-slot writes.
    pub(crate) async fn append_sync(&self, entry: SyncEntry) -> io::Result<()> {
        let (tx, rx) = oneshot::channel();
        let _ = self.tx.send(WalMsg::Append { entry, ack: Some(tx) }).await;
        rx.await.unwrap_or(Ok(()))
    }

    /// Ask the writer to snapshot immediately. Awaits completion.
    pub(crate) async fn trigger_snapshot(&self) -> io::Result<()> {
        let (tx, rx) = oneshot::channel();
        let _ = self.tx.send(WalMsg::TriggerSnapshot { ack: tx }).await;
        rx.await.unwrap_or(Ok(()))
    }

    #[allow(dead_code)]
    pub(crate) async fn shutdown(&self) {
        let _ = self.tx.send(WalMsg::Shutdown).await;
    }
}

// ── Startup replay ───────────────────────────────────────────────────────────

/// Reads `snapshot.bin` and `wal.bin` from `dir`, calls `apply_fn` for each
/// entry, and returns the highest HLC timestamp seen.
///
/// `apply_fn` is responsible for `intern_key` (if configured) and
/// `apply_and_notify` — keeping `persistence.rs` free of agent-layer imports.
pub(crate) async fn replay<F>(dir: &std::path::Path, mut apply_fn: F) -> io::Result<u64>
where
    F: FnMut(SyncEntry),
{
    let mut max_ts: u64 = 0;
    let snapshot_path = dir.join("snapshot.bin");
    let wal_path      = dir.join("wal.bin");

    // 1. Snapshot ─────────────────────────────────────────────────────────────
    let snapshot_hlc = if snapshot_path.exists() {
        match tfs::read(&snapshot_path).await {
            Ok(bytes) => {
                match bcode::decode_from_slice::<KvSnapshot, _>(&bytes, bincode_cfg()) {
                    Ok((snap, _)) => {
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
                    match bcode::decode_from_slice::<SyncEntry, _>(record_bytes, bincode_cfg()) {
                        Ok((entry, _)) if entry.timestamp > snapshot_hlc => {
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
pub(crate) fn spawn_wal_writer(
    dir:                    PathBuf,
    sync_mode:              SyncMode,
    snapshot_wal_threshold: usize,
    snapshot_interval_secs: u64,
    kv_state:               Arc<KvState>,
    node_id:                NodeId,
    hlc:                    Arc<crate::hlc::Hlc>,
    default_ttl:            u8,
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
                        let _ = do_snapshot(&dir, &kv_state, &node_id, &hlc, default_ttl, &mut wal_file).await;
                        break;
                    }
                    Some(WalMsg::Append { entry, ack }) => {
                        let result = wal_append(&mut wal_file, &entry, sync_mode).await;
                        wal_entry_count += 1;
                        if let Some(ack) = ack { let _ = ack.send(result); }
                        if wal_entry_count >= snapshot_wal_threshold {
                            let _ = do_snapshot(&dir, &kv_state, &node_id, &hlc, default_ttl, &mut wal_file).await;
                            wal_entry_count = 0;
                        }
                    }
                    Some(WalMsg::TriggerSnapshot { ack }) => {
                        let result = do_snapshot(&dir, &kv_state, &node_id, &hlc, default_ttl, &mut wal_file).await;
                        wal_entry_count = 0;
                        let _ = ack.send(result);
                    }
                }
            }

            _ = snap_timer.tick() => {
                // Defer if already opaque for another reason to avoid piling
                // snapshot opacity on top of existing load-based opacity.
                if crate::agent::is_self_opaque(&kv_state, &node_id) {
                    snap_timer.reset_after(Duration::from_secs(30));
                    continue;
                }
                let _ = do_snapshot(&dir, &kv_state, &node_id, &hlc, default_ttl, &mut wal_file).await;
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
) -> io::Result<()> {
    // Build [u32 LE length][bincode payload] in one buffer.
    let mut buf = BytesMut::with_capacity(256);
    buf.put_u32_le(0); // placeholder
    let payload_start = buf.len();
    {
        let mut w = (&mut buf).writer();
        bcode::encode_into_std_write(entry, &mut w, bincode_cfg())
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    }
    let payload_len = (buf.len() - payload_start) as u32;
    buf[..4].copy_from_slice(&payload_len.to_le_bytes());

    file.write_all(&buf).await?;
    if sync_mode == SyncMode::Flush {
        file.sync_data().await?;
    }
    Ok(())
}

// ── Snapshot ─────────────────────────────────────────────────────────────────

async fn do_snapshot(
    dir:         &std::path::Path,
    kv_state:    &Arc<KvState>,
    node_id:     &NodeId,
    hlc:         &Arc<crate::hlc::Hlc>,
    default_ttl: u8,
    wal_file:    &mut tfs::File,
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
            .filter_map(|(k, v)| {
                v.data.as_ref().map(|data| SyncEntry {
                    key:          Arc::clone(k),
                    value:        data.clone(),
                    timestamp:    v.timestamp,
                    is_tombstone: false,
                })
            })
            .collect()
    };

    // 3. Write snapshot.tmp → fdatasync → rename to snapshot.bin.
    let tmp_path  = dir.join("snapshot.tmp");
    let snap_path = dir.join("snapshot.bin");
    let snap = KvSnapshot { snapshot_hlc, entries };
    let encoded = {
        let mut buf = Vec::new();
        bcode::encode_into_std_write(&snap, &mut buf, bincode_cfg())
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        buf
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
