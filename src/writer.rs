use crate::framing::{bincode_cfg, write_frame, WireMessage};
use crate::node_id::NodeId;
use crate::store::{store_hash, StoreEntry};
use bytes::{BufMut, Bytes, BytesMut};
use dashmap::DashMap;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncWriteExt, BufWriter},
    net::TcpStream,
    sync::{mpsc, mpsc::error::TrySendError, watch},
    task::JoinHandle,
    time as ttime,
};
use tracing::{debug, warn};

/// Returns a jittered backoff in `[backoff/2, backoff*3/2]`.
fn jittered(backoff: Duration) -> Duration {
    let half = backoff.as_millis() as u64 / 2;
    backoff / 2 + Duration::from_millis(fastrand::u64(0..=half * 2))
}

/// Long-lived task that owns the TCP connection to one peer.
///
/// Receives pre-serialized frames over `rx` and writes them in order.
/// After the first frame is written, the task drains any additional queued frames
/// into the `BufWriter` before flushing — coalescing multiple small gossip messages
/// into a single (or fewer) kernel write calls. Reconnects transparently after write
/// failures; backs off for `backoff` after each connect failure so a dead peer
/// doesn't cause a connect storm. Exits on global shutdown, per-peer eviction
/// signal, or when all senders drop.
pub(crate) async fn run_peer_writer(
    peer: NodeId,
    mut rx: mpsc::Receiver<Bytes>,
    backoff: Duration,
    idle_timeout: Duration,
    mut shutdown_rx: watch::Receiver<bool>,
    mut peer_shutdown_rx: watch::Receiver<bool>,
) {
    let mut conn: Option<BufWriter<TcpStream>> = None;
    // Stores (fail_time, actual_backoff) where actual_backoff is jittered so
    // simultaneous reconnects after a partition don't all fire at the same instant.
    let mut last_fail: Option<(Instant, Duration)> = None;
    // Idle eviction: track when we last sent a frame. None = no timeout configured.
    let mut idle_deadline: Option<ttime::Instant> = if idle_timeout.is_zero() {
        None
    } else {
        Some(ttime::Instant::now() + idle_timeout)
    };

    loop {
        // biased: data path checked first so a burst of frames drains before shutdown.
        // The idle arm uses pending() when no timeout is configured so it never fires.
        let data: Bytes = tokio::select! { biased;
            msg = rx.recv() => match msg {
                Some(d) => d,
                None => break, // all senders dropped
            },
            _ = shutdown_rx.wait_for(|v| *v) => break,
            _ = peer_shutdown_rx.wait_for(|v| *v) => break,
            _ = async {
                match idle_deadline {
                    Some(d) => ttime::sleep_until(d).await,
                    None    => std::future::pending().await,
                }
            } => break,
        };
        if let Some(ref mut d) = idle_deadline {
            *d = ttime::Instant::now() + idle_timeout;
        }

        if let Some((fail_time, fail_backoff)) = last_fail {
            if fail_time.elapsed() < fail_backoff {
                debug!("Dropping frame to {} during reconnect backoff", peer);
                continue;
            }
        }

        // Lazily establish (or re-establish) the connection.
        if conn.is_none() {
            match TcpStream::connect(peer.to_socket_addr()).await {
                Ok(s) => {
                    let _ = s.set_nodelay(true);
                    // 16 KB buffer coalesces a full burst of small gossip frames into
                    // one or two kernel write calls; explicit flush sends after drain.
                    conn = Some(BufWriter::with_capacity(16_384, s));
                    last_fail = None;
                }
                Err(e) => {
                    last_fail = Some((Instant::now(), jittered(backoff)));
                    warn!("Connect to {} failed: {}", peer, e);
                    continue;
                }
            }
        }

        // Write this frame and any others already queued, then flush once.
        let write_ok = {
            let c = conn.as_mut().unwrap();
            let mut ok = write_frame(c, &data).await.is_ok();
            if ok {
                while let Ok(more) = rx.try_recv() {
                    if write_frame(c, &more).await.is_err() {
                        ok = false;
                        break;
                    }
                }
            }
            if ok { ok = c.flush().await.is_ok(); }
            ok
        };

        if !write_ok {
            conn = None;
            // +1 for the frame that caused the write failure (already dequeued, never sent).
            let dropped = rx.len() + 1;
            last_fail = Some((Instant::now(), jittered(backoff)));
            warn!("Write to {} failed; {} frame(s) will be dropped during backoff", peer, dropped);
        }
    }
}

/// Peer writer map entry. Keeps writer lifecycle co-located with peer state, bounding the
/// global task_handles vec to the small fixed set of system tasks (listener, shards, health).
pub(crate) struct WriterEntry {
    pub(crate) tx:            mpsc::Sender<Bytes>,
    pub(crate) peer_shutdown: Arc<watch::Sender<bool>>,
    pub(crate) handle:        JoinHandle<()>,
}

/// Returns the frame sender for `peer`'s writer task, spawning a new task on first use.
///
/// If the existing writer task has finished (idle timeout, disconnect, or eviction),
/// the stale map entry is replaced atomically using `and_modify` so the next caller
/// gets a fresh sender rather than a closed one.
pub(crate) fn get_or_spawn_writer(
    peer: &NodeId,
    writers: &DashMap<NodeId, WriterEntry>,
    chan_depth: usize,
    backoff: Duration,
    idle_timeout: Duration,
    shutdown_tx: &Arc<watch::Sender<bool>>,
) -> mpsc::Sender<Bytes> {
    writers
        .entry(peer.clone())
        .and_modify(|e| {
            // Replace in-place when the writer task has finished so callers don't
            // get a closed sender. This covers idle timeout, write failure, and eviction.
            if e.handle.is_finished() {
                let (tx, rx) = mpsc::channel(chan_depth);
                let (peer_shutdown_tx, peer_shutdown_rx) = watch::channel(false);
                let handle = tokio::spawn(run_peer_writer(
                    peer.clone(),
                    rx,
                    backoff,
                    idle_timeout,
                    shutdown_tx.subscribe(),
                    peer_shutdown_rx,
                ));
                *e = WriterEntry { tx, peer_shutdown: Arc::new(peer_shutdown_tx), handle };
            }
        })
        .or_insert_with(|| {
            let (tx, rx) = mpsc::channel(chan_depth);
            let (peer_shutdown_tx, peer_shutdown_rx) = watch::channel(false);
            let handle = tokio::spawn(run_peer_writer(
                peer.clone(),
                rx,
                backoff,
                idle_timeout,
                shutdown_tx.subscribe(),
                peer_shutdown_rx,
            ));
            WriterEntry { tx, peer_shutdown: Arc::new(peer_shutdown_tx), handle }
        })
        .tx
        .clone()
}

/// Removes `peer`'s writer from the map and signals its task to exit.
/// Dropping the JoinHandle detaches the task; it exits shortly via peer_shutdown signal.
pub(crate) fn evict_peer_writer(writers: &DashMap<NodeId, WriterEntry>, peer: &NodeId) {
    if let Some((_, entry)) = writers.remove(peer) {
        let _ = entry.peer_shutdown.send(true);
        // handle is dropped here; the task exits shortly via peer_shutdown signal.
    }
}

/// Serialises and enqueues a `StateRequest` into `peer`'s writer channel,
/// spawning the writer task if needed.
///
/// Computes `store_hash` of the local store so the receiver can skip sending a full
/// snapshot if its store is already in sync (anti-entropy fast-path).
#[allow(clippy::too_many_arguments)]
pub(crate) fn request_state(
    peer: &NodeId,
    peer_writers: &Arc<DashMap<NodeId, WriterEntry>>,
    store: &Arc<papaya::HashMap<Arc<str>, StoreEntry>>,
    writer_depth: usize,
    backoff: Duration,
    idle_timeout: Duration,
    shutdown_tx: &Arc<watch::Sender<bool>>,
    sender: &NodeId,
) {
    let hash = store_hash(store);
    let mut buf = BytesMut::with_capacity(64);
    if let Err(e) = bincode::serde::encode_into_std_write(
        WireMessage::StateRequest { sender: sender.clone(), store_hash: hash },
        &mut (&mut buf).writer(),
        bincode_cfg(),
    ) {
        warn!("Failed to encode StateRequest to {}: {}", peer, e);
        return;
    }
    let data: Bytes = buf.freeze();
    let tx = get_or_spawn_writer(peer, peer_writers, writer_depth, backoff, idle_timeout, shutdown_tx);
    match tx.try_send(data) {
        Ok(()) => {}
        Err(TrySendError::Full(_)) => warn!("StateRequest channel full for {}", peer),
        Err(TrySendError::Closed(_)) => warn!("StateRequest writer for {} has exited", peer),
    }
}
