use crate::connection::{handle_connection, ConnContext};
use crate::error::GossipError;
use crate::framing::{bincode_cfg, ForwardHint, WireMessage};
use crate::node_id::NodeId;
use crate::seen::ShardedSeen;
use crate::signal::{Boundary, SignalHandlers};
use crate::store::{intern_pool_len, StoreEntry};
use crate::writer::{evict_peer_writer, get_or_spawn_writer, request_state, WriterEntry};
use ahash::{AHashMap, AHashSet};
use bytes::{BufMut, Bytes, BytesMut};
use dashmap::DashMap;
use parking_lot::RwLock;
use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::{
    net::TcpListener,
    sync::{mpsc, mpsc::error::TrySendError, watch, Semaphore},
    task::JoinSet,
    time,
};
use tracing::{debug, error, warn};

use super::EPIDEMIC_K;

// ── Liveness guards ────────────────────────────────────────────────────────────

/// Clears an `AtomicBool` liveness flag on drop — handles both clean exit and panics.
pub(super) struct AliveGuard(pub(super) Arc<AtomicBool>);
impl Drop for AliveGuard {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Relaxed);
    }
}

/// Decrements the listener count on drop and logs an error if the exit was unexpected.
/// Ensures the count is always balanced even on panics.
pub(super) struct ListenerGuard {
    pub(super) count:       Arc<AtomicUsize>,
    pub(super) shutdown_tx: Arc<watch::Sender<bool>>,
}
impl Drop for ListenerGuard {
    fn drop(&mut self) {
        let prev = self.count.fetch_sub(1, Ordering::Relaxed);
        if !*self.shutdown_tx.borrow() {
            if prev == 1 {
                error!("All listener tasks exited unexpectedly; node can no longer accept inbound connections");
            } else {
                error!("Listener task exited unexpectedly; {} listener task(s) remain", prev - 1);
            }
        }
    }
}

// ── Listener context ───────────────────────────────────────────────────────────

/// Bundled context cloned into every listener task.
#[derive(Clone)]
pub(super) struct ListenerContext {
    pub(super) node_id:        NodeId,
    pub(super) store:          Arc<papaya::HashMap<Arc<str>, StoreEntry>>,
    pub(super) peers:          Arc<papaya::HashMap<NodeId, Instant>>,
    pub(super) gossip_txs:     Arc<[mpsc::Sender<(Bytes, u64, ForwardHint)>]>,
    pub(super) seen:           Arc<ShardedSeen>,
    pub(super) shutdown_tx:    Arc<watch::Sender<bool>>,
    pub(super) subscriptions:  Arc<papaya::HashMap<Arc<str>, watch::Sender<Option<Bytes>>>>,
    pub(super) current_ts:     Arc<AtomicU64>,
    pub(super) peer_writers:   Arc<DashMap<NodeId, WriterEntry>>,
    pub(super) conn_sem:       Arc<Semaphore>,
    pub(super) listener_alive: Arc<AtomicUsize>,
    pub(super) max_conn:       usize,
    pub(super) max_ttl:        u8,
    pub(super) writer_depth:   usize,
    pub(super) backoff:        Duration,
    pub(super) n_shards:        usize,
    pub(super) intern_keys:     bool,
    pub(super) intern_max_keys: usize,
    pub(super) signal_boundary: Arc<RwLock<Boundary>>,
    pub(super) signal_handlers: Arc<SignalHandlers>,
    pub(super) max_peers:           usize,
    pub(super) writer_idle_timeout: Duration,
    pub(super) max_store_entries:   usize,
    pub(super) prefix_index:        Arc<crate::store::PrefixIndex>,
    pub(super) dropped_frames:      Arc<AtomicU64>,
    pub(super) hash_acc:            Arc<AtomicU64>,
}

// ── Task implementations ───────────────────────────────────────────────────────

pub(super) async fn run_listener_task(listener: TcpListener, lctx: ListenerContext) {
    let ListenerContext {
        node_id, store, peers, gossip_txs, seen, shutdown_tx, subscriptions,
        current_ts, peer_writers, conn_sem, listener_alive,
        max_conn, max_ttl, writer_depth, backoff, n_shards, intern_keys, intern_max_keys,
        signal_boundary, signal_handlers, max_peers, writer_idle_timeout, max_store_entries,
        prefix_index, dropped_frames, hash_acc,
    } = lctx;
    let mut shutdown_rx = shutdown_tx.subscribe();
    let mut conn_set: JoinSet<()> = JoinSet::new();
    let mut retry_delay = false;
    listener_alive.fetch_add(1, Ordering::Relaxed);
    let _guard = ListenerGuard { count: listener_alive.clone(), shutdown_tx: shutdown_tx.clone() };

    loop {
        if retry_delay {
            retry_delay = false;
            tokio::select! {
                _ = time::sleep(Duration::from_millis(50)) => {}
                _ = shutdown_rx.wait_for(|v| *v) => break,
            }
        }

        tokio::select! {
            result = listener.accept() => {
                match result {
                    Ok((socket, addr)) => {
                        if let Err(e) = socket.set_nodelay(true) {
                            warn!("set_nodelay failed for {}: {}", addr, e);
                        }
                        match conn_sem.clone().try_acquire_owned() {
                            Ok(permit) => {
                                let node_id          = node_id.clone();
                                let store            = store.clone();
                                let peers            = peers.clone();
                                let gossip_txs       = gossip_txs.clone();
                                let seen             = seen.clone();
                                let shutdown         = shutdown_tx.clone();
                                let subscriptions    = subscriptions.clone();
                                let current_ts       = current_ts.clone();
                                let peer_writers     = peer_writers.clone();
                                let signal_boundary  = signal_boundary.clone();
                                let signal_handlers  = signal_handlers.clone();
                                let prefix_index     = prefix_index.clone();
                                let dropped_frames   = dropped_frames.clone();
                                let hash_acc         = hash_acc.clone();
                                conn_set.spawn(async move {
                                    let _permit = permit;
                                    let ctx = ConnContext {
                                        node_id, store, peers, gossip_txs,
                                        seen, shutdown, max_ttl, subscriptions,
                                        current_ts, peer_writers,
                                        writer_depth, backoff, n_shards,
                                        intern_keys, intern_max_keys, signal_boundary,
                                        signal_handlers, max_peers, writer_idle_timeout,
                                        max_store_entries, prefix_index, dropped_frames,
                                        hash_acc,
                                    };
                                    if let Err(e) = handle_connection(socket, addr, ctx).await {
                                        warn!("Connection error from {}: {}", addr, e);
                                    }
                                });
                            }
                            Err(_) => {
                                warn!("Connection limit ({}) reached, dropping {}", max_conn, addr);
                            }
                        }
                    }
                    Err(e) => {
                        use std::io::ErrorKind;
                        if matches!(e.kind(), ErrorKind::InvalidInput | ErrorKind::InvalidData) {
                            error!("Fatal accept error: {}; listener stopping", e);
                            break;
                        }
                        warn!("Accept error (transient, retrying in 50ms): {}", e);
                        retry_delay = true;
                    }
                }
                while conn_set.try_join_next().is_some() {}
            }
            _ = shutdown_rx.wait_for(|v| *v) => break,
        }
    }

    while conn_set.join_next().await.is_some() {}
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn run_gossip_shard(
    shard_idx:             usize,
    mut gossip_rx:         mpsc::Receiver<(Bytes, u64, ForwardHint)>,
    bootstrap_peers:       Arc<[NodeId]>,
    peer_writers:          Arc<DashMap<NodeId, WriterEntry>>,
    shutdown_tx:           Arc<watch::Sender<bool>>,
    mut peer_list_rx:      watch::Receiver<Arc<[NodeId]>>,
    alive:                 Arc<AtomicBool>,
    writer_depth:          usize,
    backoff:               Duration,
    idle_timeout:          Duration,
    max_forwarding_peers:  usize,
    dropped_frames:        Arc<AtomicU64>,
    group_aware_forwarding: bool,
    prefix_index:          Arc<crate::store::PrefixIndex>,
) {
    alive.store(true, Ordering::Relaxed);
    let _alive_guard = AliveGuard(alive.clone());
    let mut shutdown_rx = shutdown_tx.subscribe();
    let mut cached_peer_list: Arc<[NodeId]> = peer_list_rx.borrow().clone();
    let mut targets: AHashSet<NodeId> = AHashSet::new();
    targets.extend(bootstrap_peers.iter().cloned());
    let remaining = max_forwarding_peers.saturating_sub(targets.len());
    targets.extend(cached_peer_list.iter().take(remaining).cloned());
    let mut sender_cache: AHashMap<NodeId, mpsc::Sender<Bytes>> = AHashMap::new();

    macro_rules! send_to_peer {
        ($peer:expr, $data:expr) => {{
            let peer: &NodeId = $peer;
            let tx = if let Some(t) = sender_cache.get(peer) {
                t.clone()
            } else {
                let t = get_or_spawn_writer(
                    peer, &peer_writers, writer_depth, backoff, idle_timeout, &shutdown_tx,
                );
                sender_cache.insert(peer.clone(), t.clone());
                t
            };
            match tx.try_send($data.clone()) {
                Ok(()) => {}
                Err(TrySendError::Full(_)) => {
                    dropped_frames.fetch_add(1, Ordering::Relaxed);
                    warn!("Peer writer channel full, dropping forward to {}", peer);
                }
                Err(TrySendError::Closed(_)) => {
                    debug!("Peer writer for {} closed; respawning and retrying", peer);
                    sender_cache.remove(peer);
                    let new_tx = get_or_spawn_writer(
                        peer, &peer_writers, writer_depth, backoff, idle_timeout, &shutdown_tx,
                    );
                    sender_cache.insert(peer.clone(), new_tx.clone());
                    match new_tx.try_send($data.clone()) {
                        Ok(()) => {}
                        Err(TrySendError::Full(_)) => {
                            dropped_frames.fetch_add(1, Ordering::Relaxed);
                            warn!("Respawned writer for {} is full; frame dropped", peer);
                        }
                        Err(TrySendError::Closed(_)) => {
                            debug!("Respawned writer for {} closed immediately", peer);
                        }
                    }
                }
            }
        }};
    }

    loop {
        tokio::select! { biased;
            result = gossip_rx.recv() => {
                let (data, sender_hash, hint) = match result {
                    Some(u) => u,
                    None => break,
                };

                if peer_list_rx.has_changed().unwrap_or(false) {
                    cached_peer_list = peer_list_rx.borrow_and_update().clone();
                    targets.clear();
                    targets.extend(bootstrap_peers.iter().cloned());
                    let remaining = max_forwarding_peers.saturating_sub(targets.len());
                    targets.extend(cached_peer_list.iter().take(remaining).cloned());
                    sender_cache.retain(|k, _| targets.contains(k));
                }

                if targets.is_empty() { continue; }

                if !group_aware_forwarding {
                    for peer in targets.iter().filter(|p| p.id_hash() != sender_hash) {
                        send_to_peer!(peer, data);
                    }
                } else {
                    match &hint {
                        ForwardHint::All => {
                            for peer in targets.iter().filter(|p| p.id_hash() != sender_hash) {
                                send_to_peer!(peer, data);
                            }
                        }
                        ForwardHint::Individual(target) => {
                            if target.id_hash() != sender_hash && targets.contains(target) {
                                send_to_peer!(target, data);
                            }
                        }
                        ForwardHint::Group(name) => {
                            let prefix = format!("grp/{}/", name);
                            let idx_guard = prefix_index.pin();
                            let members: AHashSet<NodeId> = idx_guard.get("grp")
                                .map(|bucket| {
                                    bucket.pin().iter()
                                        .filter_map(|(key, _)| {
                                            if !key.starts_with(&*prefix) { return None; }
                                            key[prefix.len()..].parse::<NodeId>().ok()
                                        })
                                        .collect()
                                })
                                .unwrap_or_default();

                            for peer in targets.iter()
                                .filter(|p| p.id_hash() != sender_hash && members.contains(*p))
                            {
                                send_to_peer!(peer, data);
                            }

                            let non_members: Vec<&NodeId> = targets.iter()
                                .filter(|p| p.id_hash() != sender_hash && !members.contains(*p))
                                .collect();
                            if !non_members.is_empty() {
                                let k = EPIDEMIC_K.min(non_members.len());
                                let start = fastrand::usize(0..non_members.len());
                                for i in 0..k {
                                    let peer = non_members[(start + i) % non_members.len()];
                                    send_to_peer!(peer, data);
                                }
                            }
                        }
                    }
                }
            }
            _ = shutdown_rx.wait_for(|v| *v) => break,
        }
    }

    if !*shutdown_rx.borrow() {
        error!("Gossip shard {} exited unexpectedly; set/delete on this shard will be dropped", shard_idx);
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn run_health_monitor(
    node_id:               NodeId,
    bootstrap_peers:       Arc<[NodeId]>,
    peers:                 Arc<papaya::HashMap<NodeId, Instant>>,
    peer_writers:          Arc<DashMap<NodeId, WriterEntry>>,
    store:                 Arc<papaya::HashMap<Arc<str>, StoreEntry>>,
    peer_list_tx:          watch::Sender<Arc<[NodeId]>>,
    shutdown_tx:           Arc<watch::Sender<bool>>,
    current_ts:            Arc<AtomicU64>,
    interval_secs:         u64,
    writer_depth:          usize,
    backoff:               Duration,
    idle_timeout:          Duration,
    peer_eviction_intervals: u64,
    health_monitor_alive:    Arc<AtomicBool>,
    ping_peer_sample_size:   usize,
    health_check_max_jitter: u64,
    hash_acc:                Arc<AtomicU64>,
) {
    let bootstrap_set: AHashSet<NodeId> = bootstrap_peers.iter().cloned().collect();
    let mut shutdown_rx = shutdown_tx.subscribe();
    health_monitor_alive.store(true, Ordering::Relaxed);
    let _alive_guard = AliveGuard(health_monitor_alive.clone());

    let max_jitter = if health_check_max_jitter > 0 { health_check_max_jitter } else { interval_secs * 500 };
    let jitter_ms = fastrand::u64(0..max_jitter.max(1));
    tokio::select! {
        _ = time::sleep(Duration::from_millis(jitter_ms)) => {}
        _ = shutdown_rx.wait_for(|v| *v) => return,
    }

    for peer in &bootstrap_set {
        request_state(peer, &peer_writers, &store, writer_depth, backoff, idle_timeout, &shutdown_tx, &node_id, &hash_acc);
    }

    let mut ticker = time::interval(Duration::from_secs(interval_secs));
    ticker.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
    let mut ping_buf: BytesMut = BytesMut::with_capacity(1024);
    let mut cached_ping_targets: AHashSet<NodeId> = bootstrap_set.clone();
    cached_ping_targets.remove(&node_id);
    let mut last_peer_set: AHashSet<NodeId> = AHashSet::new();
    let mut ping_sender_cache: AHashMap<NodeId, mpsc::Sender<Bytes>> = AHashMap::new();

    loop {
        tokio::select! { biased;
            _ = ticker.tick() => {
                let mut current_peer_set: AHashSet<NodeId> = AHashSet::new();
                let mut known: Vec<NodeId> = Vec::new();
                {
                    let guard = peers.pin();
                    for (id, _) in guard.iter() {
                        known.push(id.clone());
                        current_peer_set.insert(id.clone());
                    }
                }
                for i in 0..ping_peer_sample_size.min(known.len()) {
                    let j = i + fastrand::usize(..known.len() - i);
                    known.swap(i, j);
                }
                known.truncate(ping_peer_sample_size);

                if let Err(e) = bincode::serde::encode_into_std_write(
                    WireMessage::Ping { sender: node_id.clone(), known_peers: known },
                    &mut (&mut ping_buf).writer(),
                    bincode_cfg(),
                ) {
                    warn!("Ping serialize failed: {}", e);
                    continue;
                }
                let ping_data: Bytes = ping_buf.split().freeze();

                if current_peer_set != last_peer_set {
                    let peer_list: Arc<[NodeId]> = current_peer_set.iter().cloned().collect();
                    let _ = peer_list_tx.send(peer_list);
                    cached_ping_targets.clear();
                    cached_ping_targets.extend(bootstrap_set.iter().cloned());
                    cached_ping_targets.extend(current_peer_set.iter().cloned());
                    cached_ping_targets.remove(&node_id);
                    ping_sender_cache.retain(|k, _| cached_ping_targets.contains(k));
                    last_peer_set = current_peer_set;
                }
                for peer in &cached_ping_targets {
                    let tx = if let Some(t) = ping_sender_cache.get(peer) {
                        t.clone()
                    } else {
                        let t = get_or_spawn_writer(
                            peer, &peer_writers, writer_depth, backoff, idle_timeout, &shutdown_tx,
                        );
                        ping_sender_cache.insert(peer.clone(), t.clone());
                        t
                    };
                    match tx.try_send(ping_data.clone()) {
                        Ok(()) => {}
                        Err(TrySendError::Full(_)) => {
                            warn!("Peer writer channel full, dropping ping to {}", peer);
                        }
                        Err(TrySendError::Closed(_)) => {
                            debug!("Peer writer for {} closed; respawning for ping retry", peer);
                            ping_sender_cache.remove(peer);
                            let new_tx = get_or_spawn_writer(
                                peer, &peer_writers, writer_depth, backoff, idle_timeout, &shutdown_tx,
                            );
                            ping_sender_cache.insert(peer.clone(), new_tx.clone());
                            match new_tx.try_send(ping_data.clone()) {
                                Ok(()) => {}
                                Err(_) => {
                                    debug!("Respawned writer for {} could not accept ping", peer);
                                }
                            }
                        }
                    }
                }

                let wall_ts = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64;
                current_ts.store(wall_ts, Ordering::Relaxed);

                let eviction_window = Duration::from_secs(interval_secs.saturating_mul(peer_eviction_intervals));
                let maybe_peer_cutoff = Instant::now().checked_sub(eviction_window);

                if let Some(peer_cutoff) = maybe_peer_cutoff {
                    let guard = peers.pin();
                    let stale_peers: Vec<NodeId> = guard
                        .iter()
                        .filter(|(_, t)| **t < peer_cutoff)
                        .map(|(id, _)| id.clone())
                        .collect();
                    for id in &stale_peers {
                        let removed = matches!(
                            guard.compute(id.clone(), |existing| match existing {
                                Some((_, t)) if *t < peer_cutoff => papaya::Operation::Remove,
                                _ => papaya::Operation::Abort(()),
                            }),
                            papaya::Compute::Removed(..)
                        );
                        if removed {
                            warn!("Peer {} timed out, removing", id);
                            evict_peer_writer(&peer_writers, id);
                        }
                    }
                }
            }
            _ = shutdown_rx.wait_for(|v| *v) => break,
        }
    }

    if !*shutdown_rx.borrow() {
        error!("Health monitor task exited unexpectedly; peer eviction and pings have stopped");
    }
}

/// On unix, create a TCP listener with `SO_REUSEPORT` so multiple listener tasks
/// can be bound to the same address.
#[cfg(unix)]
pub(super) async fn new_listener(addr: SocketAddr, backlog: u32) -> Result<TcpListener, GossipError> {
    let sock = if addr.is_ipv6() {
        tokio::net::TcpSocket::new_v6()
    } else {
        tokio::net::TcpSocket::new_v4()
    }.map_err(GossipError::Io)?;
    sock.set_reuseport(true).map_err(GossipError::Io)?;
    sock.set_reuseaddr(true).map_err(GossipError::Io)?;
    sock.bind(addr).map_err(GossipError::Io)?;
    sock.listen(backlog).map_err(GossipError::Io)
}

#[cfg(not(unix))]
pub(super) async fn new_listener(addr: SocketAddr, backlog: u32) -> Result<TcpListener, GossipError> {
    let sock = if addr.is_ipv6() {
        tokio::net::TcpSocket::new_v6()
    } else {
        tokio::net::TcpSocket::new_v4()
    }.map_err(GossipError::Io)?;
    sock.set_reuseaddr(true).map_err(GossipError::Io)?;
    sock.bind(addr).map_err(GossipError::Io)?;
    sock.listen(backlog).map_err(GossipError::Io)
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn run_gc_task(
    store:             Arc<papaya::HashMap<Arc<str>, StoreEntry>>,
    subscriptions:     Arc<papaya::HashMap<Arc<str>, watch::Sender<Option<Bytes>>>>,
    shutdown_tx:       Arc<watch::Sender<bool>>,
    interval_secs:     u64,
    default_ttl:       u8,
    propagation_window: u64,
    live_entries:      Arc<AtomicUsize>,
    seen:              Arc<ShardedSeen>,
    max_seen_entries:  usize,
    gc_alive:          Arc<AtomicBool>,
    signal_handlers:   Arc<SignalHandlers>,
    peer_writers:      Arc<DashMap<NodeId, WriterEntry>>,
    intern_max_keys:   usize,
    signal_boundary:   Arc<RwLock<crate::signal::Boundary>>,
    node_id:           NodeId,
) {
    gc_alive.store(true, Ordering::Relaxed);
    let _alive_guard = AliveGuard(gc_alive.clone());
    let mut shutdown_rx = shutdown_tx.subscribe();
    let initial = store.pin().iter().filter(|(_, v)| v.data.is_some()).count();
    live_entries.store(initial, Ordering::Relaxed);

    let gc_interval = Duration::from_secs(interval_secs.saturating_mul(10).max(60));
    let mut ticker = time::interval(gc_interval);
    ticker.set_missed_tick_behavior(time::MissedTickBehavior::Skip);

    loop {
        tokio::select! { biased;
            _ = ticker.tick() => {
                let wall_ts = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64;
                let tombstone_cutoff = wall_ts.saturating_sub(
                    (default_ttl as u64)
                        .saturating_mul(propagation_window)
                        .saturating_mul(10)
                        .saturating_mul(1_000),
                );

                let mut live: usize = 0;
                {
                    let guard = store.pin();
                    let stale_keys: Vec<Arc<str>> = guard
                        .iter()
                        .filter(|(_, v)| {
                            if v.data.is_some() { live += 1; }
                            v.data.is_none() && v.timestamp < tombstone_cutoff
                        })
                        .map(|(k, _)| k.clone())
                        .collect();
                    for key in &stale_keys { guard.remove(key); }
                }
                live_entries.store(live, Ordering::Relaxed);

                let pool_len = intern_pool_len();
                let pool_warn = if intern_max_keys > 0 {
                    intern_max_keys.saturating_mul(2)
                } else {
                    100_000
                };
                if pool_len > pool_warn {
                    warn!(
                        "Key intern pool has {} entries (warn threshold {}). \
                         High key churn detected — set intern_max_keys or intern_keys=false \
                         to prevent unbounded memory growth.",
                        pool_len, pool_warn
                    );
                }

                signal_handlers.trim_sender_log();

                {
                    let sub_guard = subscriptions.pin();
                    let stale: Vec<Arc<str>> = sub_guard
                        .iter()
                        .filter_map(|(k, tx)| if tx.is_closed() { Some(k.clone()) } else { None })
                        .collect();
                    for key in &stale {
                        sub_guard.compute(key.clone(), |existing| match existing {
                            Some((_, tx)) if tx.is_closed() => papaya::Operation::Remove,
                            _ => papaya::Operation::Abort(()),
                        });
                    }
                }

                let half_window = wall_ts.saturating_sub(
                    (default_ttl as u64)
                        .saturating_mul(propagation_window)
                        .saturating_mul(2)
                        .saturating_mul(1_000),
                );
                let seen_cutoff = wall_ts.saturating_sub(
                    (default_ttl as u64)
                        .saturating_mul(propagation_window)
                        .saturating_mul(4)
                        .saturating_mul(1_000),
                );
                if seen.evict(max_seen_entries, seen_cutoff, half_window) {
                    seen.emergency_trim(wall_ts.saturating_sub(60_000));
                    warn!("Seen-set emergency trim; {} entries remain", seen.len());
                }

                peer_writers.retain(|_, entry| !entry.handle.is_finished());

                // Boundary reconcile — catch-all for updates missed by the push-based path.
                {
                    let suffix = format!("/{}", node_id);
                    let mut to_insert: Vec<Arc<str>> = Vec::new();
                    let mut to_remove: Vec<Arc<str>> = Vec::new();
                    {
                        let guard = store.pin();
                        for (key, entry) in guard.iter() {
                            let Some(inner) = key.strip_prefix("grp/") else { continue };
                            let Some(slash) = inner.rfind('/') else { continue };
                            if inner[slash..] != suffix { continue }
                            let group = &inner[..slash];
                            if entry.data.is_some() {
                                to_insert.push(Arc::from(group));
                            } else {
                                to_remove.push(Arc::from(group));
                            }
                        }
                    }
                    let mut bnd = signal_boundary.write();
                    for g in to_insert { bnd.groups.insert(g); }
                    for g in &to_remove { bnd.groups.remove(g.as_ref()); }
                }
            }
            _ = shutdown_rx.wait_for(|v| *v) => break,
        }
    }

    if !*shutdown_rx.borrow() {
        error!("GC task exited unexpectedly; tombstone expiry and subscription eviction have stopped");
    }
}
