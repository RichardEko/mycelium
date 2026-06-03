use crate::framing::{bincode_cfg, write_frame, WireMessage};
use crate::node_id::NodeId;
use crate::store::store_hash_acc;
use crate::stream::GossipStream;
use crate::tls::NodeTls;
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
    tls: Option<Arc<NodeTls>>,
) {
    let mut conn: Option<BufWriter<GossipStream>> = None;
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
                #[cfg(feature = "metrics")]
                metrics::counter!("gossip_frames_dropped_total").increment(1);
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
                    // Optional TLS upgrade before buffering.
                    let stream = tls_connect(s, &peer, &tls).await;
                    match stream {
                        Ok(gs) => {
                            // 16 KB buffer coalesces a full burst of small gossip frames into
                            // one or two kernel write calls; explicit flush sends after drain.
                            conn = Some(BufWriter::with_capacity(16_384, gs));
                            last_fail = None;
                        }
                        Err(e) => {
                            last_fail = Some((Instant::now(), jittered(backoff)));
                            warn!("TLS handshake to {} failed: {}", peer, e);
                            continue;
                        }
                    }
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
/// or the global shutdown signal.
///
/// `abort_handle = None` is the *pending* sentinel: a caller has claimed the spawn slot
/// and installed the channel, but the writer task has not been spawned yet. Concurrent
/// callers that see `None` return the pre-installed `tx` directly — they share the same
/// channel and their frames will be drained once the task starts.
#[derive(Clone)]
pub(crate) struct WriterEntry {
    pub(crate) tx:            mpsc::Sender<Bytes>,
    pub(crate) peer_shutdown: Arc<watch::Sender<bool>>,
    /// `None` = spawn in progress (pending sentinel); `Some` = task running or finished.
    pub(crate) abort_handle:  Option<tokio::task::AbortHandle>,
    /// Cumulative frames dropped to this peer during reconnect backoff.
    /// Subset of the global `dropped_frames` counter; useful for identifying slow peers.
    pub(crate) dropped:       Arc<AtomicU64>,
}

impl WriterEntry {
    /// Returns `true` if the writer task is alive or its spawn is still pending.
    pub(crate) fn is_live(&self) -> bool {
        self.abort_handle.as_ref().is_none_or(|h| !h.is_finished())
    }
}

/// Returns the frame sender for `peer`'s writer task, spawning a new task on first use.
///
/// Uses a *claim-then-spawn* protocol to ensure exactly one task is spawned per peer:
///
/// 1. **Fast path** — if a live entry (or a pending spawn) exists, return its `tx`.
/// 2. **Claim** — atomically insert a pending sentinel (`abort_handle = None`) with a
///    pre-created channel. Concurrent callers that lose the CAS return the winner's `tx`.
/// 3. **Spawn** — the claim winner spawns the writer task outside `compute()` (so papaya
///    retry loops don't create duplicate tasks), then updates the entry with the real handle.
#[allow(clippy::too_many_arguments)]
pub(crate) fn get_or_spawn_writer(
    peer: &NodeId,
    writers: &papaya::HashMap<NodeId, WriterEntry>,
    chan_depth: usize,
    backoff: Duration,
    idle_timeout: Duration,
    shutdown_tx: &Arc<watch::Sender<bool>>,
    dropped_frames: &Arc<AtomicU64>,
    tls: Option<Arc<NodeTls>>,
) -> Option<mpsc::Sender<Bytes>> {
    // Guard: refuse to spawn during shutdown.
    if *shutdown_tx.borrow() {
        return None;
    }

    let guard = writers.pin();

    // Fast path: live writer or pending spawn already exists.
    if let Some(entry) = guard.get(peer) {
        if entry.is_live() {
            return Some(entry.tx.clone());
        }
    }

    // Claim the spawn slot by installing a pending sentinel atomically.
    // Creating the channel here is O(1) (no OS resources); the task only runs if we win.
    let (tx, rx) = mpsc::channel(chan_depth);
    let (peer_shutdown_tx, peer_shutdown_rx) = watch::channel(false);
    let peer_shutdown = Arc::new(peer_shutdown_tx);
    let dropped = Arc::new(AtomicU64::new(0));
    let pending = WriterEntry {
        tx: tx.clone(),
        peer_shutdown: peer_shutdown.clone(),
        abort_handle: None,
        dropped: dropped.clone(),
    };

    let claim = guard.compute(peer.clone(), |existing| match existing {
        Some((_, e)) if e.is_live() => papaya::Operation::Abort(e.tx.clone()),
        _                           => papaya::Operation::Insert(pending.clone()),
    });

    if let papaya::Compute::Aborted(winner_tx) = claim {
        // Another caller already holds the slot (live writer or pending spawn). Use theirs.
        return Some(winner_tx);
    }

    // We won the claim. Spawn the task (outside compute so retries don't duplicate it).
    let join_handle = tokio::spawn(run_peer_writer(
        peer.clone(),
        rx,
        backoff,
        idle_timeout,
        shutdown_tx.subscribe(),
        peer_shutdown_rx,
        dropped_frames.clone(),
        dropped,
        tls,
    ));
    let abort_handle = join_handle.abort_handle();
    drop(join_handle); // detach — task exits via peer_shutdown or global shutdown signal

    // Upgrade the pending entry to a live entry with the real abort handle.
    guard.compute(peer.clone(), |existing| match existing {
        Some((_, e)) if e.abort_handle.is_none() => papaya::Operation::Insert(WriterEntry {
            tx: tx.clone(),
            peer_shutdown: peer_shutdown.clone(),
            abort_handle: Some(abort_handle.clone()),
            dropped: e.dropped.clone(),
        }),
        _ => papaya::Operation::Abort(()),
    });

    Some(tx)
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
    tls: Option<Arc<NodeTls>>,
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
    let Some(tx) = get_or_spawn_writer(peer, peer_writers, writer_depth, backoff, idle_timeout, shutdown_tx, dropped_frames, tls) else { return; };
    if tx.try_send(data).is_err() {
        warn!("StateRequest writer for {}: channel full or closed; state sync skipped", peer);
    }
}

/// Upgrades a plain `TcpStream` to a `GossipStream`, performing a TLS client
/// handshake when `tls` is `Some`. Returns the plain stream unchanged otherwise.
async fn tls_connect(
    stream: TcpStream,
    #[allow(unused_variables)] peer: &NodeId,
    tls: &Option<Arc<NodeTls>>,
) -> Result<GossipStream, std::io::Error> {
    #[cfg(feature = "tls")]
    if let Some(ref node_tls) = tls {
        use rustls::pki_types::ServerName;
        let ip = peer.to_socket_addr().ip();
        let server_name = ServerName::try_from(ip.to_string().as_str())
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e.to_string()))?
            .to_owned();
        let connector = tokio_rustls::TlsConnector::from(Arc::clone(&node_tls.client_config));
        let tls_stream = connector.connect(server_name, stream).await?;
        return Ok(GossipStream::TlsClient(tls_stream));
    }
    let _ = tls; // suppress unused warning when feature is disabled
    Ok(GossipStream::Plain(stream))
}
