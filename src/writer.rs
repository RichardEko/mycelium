use crate::framing::{bincode_cfg, write_frame, WireMessage};
use crate::node_id::NodeId;
use crate::store::store_hash_acc;
use bytes::{BufMut, Bytes, BytesMut};
use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncWriteExt, BufWriter},
    net::TcpStream,
    sync::{mpsc, watch},
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
#[allow(clippy::too_many_arguments)]
pub(crate) async fn run_peer_writer(
    peer: NodeId,
    mut rx: mpsc::Receiver<Bytes>,
    backoff: Duration,
    idle_timeout: Duration,
    mut shutdown_rx: watch::Receiver<bool>,
    mut peer_shutdown_rx: watch::Receiver<bool>,
    dropped_frames: Arc<AtomicU64>,
    peer_dropped: Arc<AtomicU64>,
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
                dropped_frames.fetch_add(1, Ordering::Relaxed);
                peer_dropped.fetch_add(1, Ordering::Relaxed);
                debug!("Dropping frame to {} during reconnect backoff", peer);
                continue;
            }
        }

        // Lazily establish (or re-establish) the connection.
        if conn.is_none() {
            match TcpStream::connect(peer.to_socket_addr()).await {
                Ok(s) => {
                    let _ = s.set_nodelay(true);
                    #[cfg(unix)]
                    {
                        use socket2::{SockRef, TcpKeepalive};
                        let ka = TcpKeepalive::new()
                            .with_time(Duration::from_secs(30))
                            .with_interval(Duration::from_secs(10));
                        let _ = SockRef::from(&s).set_tcp_keepalive(&ka);
                    }
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
///
/// `abort_handle` is `Clone` (unlike `JoinHandle`), satisfying papaya's `V: Clone` bound
/// for `compute()`. The task runs as a detached tokio task; it exits via `peer_shutdown`
/// or the global shutdown signal. `abort_handle.is_finished()` tracks liveness.
#[derive(Clone)]
pub(crate) struct WriterEntry {
    pub(crate) tx:            mpsc::Sender<Bytes>,
    pub(crate) peer_shutdown: Arc<watch::Sender<bool>>,
    pub(crate) abort_handle:  tokio::task::AbortHandle,
    /// Cumulative frames dropped to this peer during reconnect backoff.
    /// Subset of the global `dropped_frames` counter; useful for identifying slow peers.
    pub(crate) dropped:       Arc<AtomicU64>,
}

/// Returns the frame sender for `peer`'s writer task, spawning a new task on first use.
///
/// If the existing writer task has finished (idle timeout, disconnect, or eviction),
/// the stale map entry is replaced. A new task is spawned OUTSIDE the papaya `compute()`
/// callback to avoid spawning multiple tasks when the callback retries due to concurrent
/// map mutations. A CAS-style `compute()` then installs the new entry, aborting if
/// another racing caller already inserted a live writer — in which case the just-spawned
/// task is immediately shut down.
pub(crate) fn get_or_spawn_writer(
    peer: &NodeId,
    writers: &papaya::HashMap<NodeId, WriterEntry>,
    chan_depth: usize,
    backoff: Duration,
    idle_timeout: Duration,
    shutdown_tx: &Arc<watch::Sender<bool>>,
    dropped_frames: &Arc<AtomicU64>,
) -> mpsc::Sender<Bytes> {
    let guard = writers.pin();

    // Fast path: live writer already exists.
    if let Some(entry) = guard.get(peer) {
        if !entry.abort_handle.is_finished() {
            return entry.tx.clone();
        }
    }

    // Slow path: spawn outside compute() so retries don't create duplicate tasks.
    let (tx, rx) = mpsc::channel(chan_depth);
    let (peer_shutdown_tx, peer_shutdown_rx) = watch::channel(false);
    let peer_shutdown = Arc::new(peer_shutdown_tx);
    let dropped = Arc::new(AtomicU64::new(0));
    let join_handle = tokio::spawn(run_peer_writer(
        peer.clone(),
        rx,
        backoff,
        idle_timeout,
        shutdown_tx.subscribe(),
        peer_shutdown_rx,
        dropped_frames.clone(),
        dropped.clone(),
    ));
    let abort_handle = join_handle.abort_handle();
    drop(join_handle); // detach — task exits via peer_shutdown or global shutdown signal
    let new_entry = WriterEntry { tx: tx.clone(), peer_shutdown: peer_shutdown.clone(), abort_handle, dropped };

    // CAS insert: if a racing caller already installed a live writer, use theirs and
    // abort the task we just spawned.
    match guard.compute(peer.clone(), |existing| match existing {
        Some((_, e)) if !e.abort_handle.is_finished() => papaya::Operation::Abort(e.tx.clone()),
        _ => papaya::Operation::Insert(new_entry.clone()),
    }) {
        papaya::Compute::Aborted(winner_tx) => {
            new_entry.abort_handle.abort();
            winner_tx
        }
        _ => tx,
    }
}

/// Removes `peer`'s writer from the map and signals its task to exit.
pub(crate) fn evict_peer_writer(writers: &papaya::HashMap<NodeId, WriterEntry>, peer: &NodeId) {
    let guard = writers.pin();
    if let Some(entry) = guard.get(peer) {
        let _ = entry.peer_shutdown.send(true);
    }
    guard.remove(peer);
}

/// Serialises and enqueues a `StateRequest` into `peer`'s writer channel,
/// spawning the writer task if needed.
///
/// `key_timestamps` is the sender's full (key, timestamp) index, used by the receiver
/// to compute a delta response (v8+ delta sync). Pass `vec![]` for a full-dump request
/// (e.g. health monitor calls, or when the local store is empty).
#[allow(clippy::too_many_arguments)]
pub(crate) fn request_state(
    peer: &NodeId,
    peer_writers: &papaya::HashMap<NodeId, WriterEntry>,
    writer_depth: usize,
    backoff: Duration,
    idle_timeout: Duration,
    shutdown_tx: &Arc<watch::Sender<bool>>,
    sender: &NodeId,
    hash_acc: &AtomicU64,
    dropped_frames: &Arc<AtomicU64>,
    key_timestamps: Vec<(std::sync::Arc<str>, u64)>,
) {
    let hash = store_hash_acc(hash_acc);
    let mut buf = BytesMut::with_capacity(64);
    if let Err(e) = bincode::serde::encode_into_std_write(
        WireMessage::StateRequest { sender: sender.clone(), store_hash: hash, key_timestamps },
        &mut (&mut buf).writer(),
        bincode_cfg(),
    ) {
        warn!("Failed to encode StateRequest to {}: {}", peer, e);
        return;
    }
    let data: Bytes = buf.freeze();
    let tx = get_or_spawn_writer(peer, peer_writers, writer_depth, backoff, idle_timeout, shutdown_tx, dropped_frames);
    if tx.try_send(data).is_err() {
        warn!("StateRequest writer for {}: channel full or closed; state sync skipped", peer);
    }
}
