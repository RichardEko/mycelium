use crate::connection::{handle_connection, ConnContext};
use crate::stream::GossipStream;
use crate::tls::NodeTls;
use crate::error::GossipError;
use crate::framing::{bincode_cfg, ForwardHint, WireMessage};
use crate::locality::LocalityPath;
use crate::node_id::NodeId;
use crate::seen::ShardedSeen;
use crate::signal::SignalHandlers;
use crate::store::intern_pool_len;
use crate::writer::{evict_peer_writer, get_or_spawn_writer, request_state, WriterEntry};
use ahash::{AHashMap, AHashSet};
use bytes::{BufMut, Bytes, BytesMut};
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


// ── Liveness guards ────────────────────────────────────────────────────────────

/// Clears an `AtomicBool` liveness flag on drop — handles both clean exit and panics.
pub(super) struct AliveGuard(pub(super) Arc<AtomicBool>);
impl Drop for AliveGuard {
    fn drop(&mut self) {
        // Relaxed: liveness flags are purely diagnostic (read by system_stats()).
        // No ordering invariant depends on them — a brief observation lag is acceptable.
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
        // Relaxed: diagnostic counter read by system_stats(); no ordering dependency.
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
///
/// Embeds a [`ConnContext`] for the 17 fields shared with connection handlers,
/// plus 4 listener-only fields. This eliminates the structural duplication that
/// existed when both structs listed the same Arcs independently.
#[derive(Clone)]
pub(super) struct ListenerContext {
    pub(super) conn:           ConnContext,
    pub(super) conn_sem:       Arc<Semaphore>,
    pub(super) listener_alive: Arc<AtomicUsize>,
    pub(super) max_conn:       usize,
    /// Bind address for listener restart on fatal accept error.
    pub(super) addr:           SocketAddr,
    /// TCP accept-queue depth used when recreating the listener socket.
    pub(super) tcp_backlog:    u32,
    /// Optional TLS server config for mTLS peer connections.
    pub(super) tls:            Option<Arc<NodeTls>>,
}

// ── Task implementations ───────────────────────────────────────────────────────

pub(super) async fn run_listener_task(mut listener: TcpListener, lctx: ListenerContext) {
    let ListenerContext { conn, conn_sem, listener_alive, max_conn, addr, tcp_backlog, tls } = lctx;
    let mut shutdown_rx = conn.shutdown.subscribe();
    let mut conn_set: JoinSet<()> = JoinSet::new();
    let mut retry_delay = false;
    let mut need_restart = false;
    listener_alive.fetch_add(1, Ordering::Relaxed);
    let _guard = ListenerGuard { count: Arc::clone(&listener_alive), shutdown_tx: Arc::clone(&conn.shutdown) };
    let mut restart_backoff = Duration::from_millis(100);

    loop {
        if retry_delay {
            retry_delay = false;
            tokio::select! {
                _ = time::sleep(Duration::from_millis(50)) => {}
                _ = shutdown_rx.wait_for(|v| *v) => break,
            }
        }

        // Restart path: backoff sleep then rebind the socket.
        // Handled at the top of the loop (outside any select!) to avoid nested borrows.
        if need_restart {
            need_restart = false;
            if *shutdown_rx.borrow() { break; }
            time::sleep(restart_backoff).await;
            if *shutdown_rx.borrow() { break; }
            restart_backoff = (restart_backoff * 2).min(Duration::from_secs(30));
            match new_listener(addr, tcp_backlog).await {
                Ok(new) => {
                    listener = new;
                    error!("Listener on {} restarted successfully", addr);
                }
                Err(e) => {
                    error!("Listener restart failed for {}: {}; retrying", addr, e);
                    need_restart = true;
                }
            }
            continue;
        }

        tokio::select! {
            result = listener.accept() => {
                match result {
                    Ok((socket, peer_addr)) => {
                        restart_backoff = Duration::from_millis(100);
                        if let Err(e) = socket.set_nodelay(true) {
                            warn!("set_nodelay failed for {}: {}", peer_addr, e);
                        }
                        match Arc::clone(&conn_sem).try_acquire_owned() {
                            Ok(permit) => {
                                let ctx = conn.clone();
                                let tls = tls.clone();
                                conn_set.spawn(async move {
                                    let _permit = permit;
                                    let gs = tls_accept(socket, &tls).await;
                                    match gs {
                                        Ok(gs) => {
                                            if let Err(e) = handle_connection(gs, peer_addr, ctx).await {
                                                warn!("Connection error from {}: {}", peer_addr, e);
                                            }
                                        }
                                        Err(e) => {
                                            warn!("TLS accept from {}: {}", peer_addr, e);
                                        }
                                    }
                                });
                            }
                            Err(_) => {
                                warn!("Connection limit ({}) reached, dropping {}", max_conn, peer_addr);
                            }
                        }
                    }
                    Err(e) => {
                        use std::io::ErrorKind;
                        if matches!(e.kind(), ErrorKind::InvalidInput | ErrorKind::InvalidData) {
                            error!("Fatal accept error on {}: {}; restarting in {:?}", addr, e, restart_backoff);
                            need_restart = true;
                        } else {
                            warn!("Accept error (transient, retrying in 50ms): {}", e);
                            retry_delay = true;
                        }
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
    shard_idx:              usize,
    mut gossip_rx:          mpsc::Receiver<(Bytes, u64, ForwardHint)>,
    bootstrap_peers:        Arc<[NodeId]>,
    peer_writers:           Arc<papaya::HashMap<NodeId, WriterEntry>>,
    shutdown_tx:            Arc<watch::Sender<bool>>,
    mut peer_list_rx:       watch::Receiver<Arc<[NodeId]>>,
    alive:                  Arc<AtomicBool>,
    writer_depth:           usize,
    backoff:                Duration,
    idle_timeout:           Duration,
    max_forwarding_peers:   usize,
    dropped_frames:         Arc<AtomicU64>,
    individual_flood_fallbacks: Arc<AtomicU64>,
    group_aware_forwarding: bool,
    epidemic_extra_peers:   usize,
    prefix_index:           Arc<crate::store::PrefixIndex>,
    grp_generation:         Arc<AtomicU64>,
    self_locality:          Option<LocalityPath>,
    peer_localities:        Arc<papaya::HashMap<NodeId, LocalityPath>>,
    tls:                    Option<Arc<NodeTls>>,
) {
    alive.store(true, Ordering::Relaxed);
    let _alive_guard = AliveGuard(Arc::clone(&alive));
    let mut shutdown_rx = shutdown_tx.subscribe();
    let mut cached_peer_list: Arc<[NodeId]> = peer_list_rx.borrow().clone();
    let mut targets: AHashSet<NodeId> = AHashSet::new();
    targets.extend(bootstrap_peers.iter().cloned());
    let remaining = max_forwarding_peers.saturating_sub(targets.len());
    targets.extend(cached_peer_list.iter().take(remaining).cloned());
    let mut sender_cache: AHashMap<NodeId, mpsc::Sender<Bytes>> = AHashMap::new();
    // Per-group member set cache. Keyed by group name; value is (generation, member set).
    // Invalidated whenever grp_generation advances (any grp/ KV change).
    let mut group_member_cache: AHashMap<Arc<str>, (u64, AHashSet<NodeId>)> = AHashMap::new();

    macro_rules! send_to_peer {
        ($peer:expr, $data:expr) => {{
            let peer: &NodeId = $peer;
            let tx = if let Some(t) = sender_cache.get(peer) {
                t.clone()
            } else {
                let Some(t) = get_or_spawn_writer(
                    peer, &peer_writers, writer_depth, backoff, idle_timeout, &shutdown_tx, &dropped_frames, tls.clone(),
                ) else { continue; };
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
                    let Some(new_tx) = get_or_spawn_writer(
                        peer, &peer_writers, writer_depth, backoff, idle_timeout, &shutdown_tx, &dropped_frames, tls.clone(),
                    ) else { continue; };
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

                if targets.is_empty() {
                    // A flood frame with no peers is normal during startup;
                    // an Individual frame (RPC/vote) dropped with zero peers
                    // is a delivery failure the caller only sees as a timeout
                    // — make it legible.
                    if let ForwardHint::Individual(target) = &hint {
                        let c = individual_flood_fallbacks.fetch_add(1, Ordering::Relaxed);
                        if c == 0 || c.is_multiple_of(256) {
                            warn!(target_node = %target,
                                "Individual-scoped frame dropped: no peers at all \
                                 (RPC/vote cannot be delivered; will surface as timeout)");
                        }
                    }
                    continue;
                }

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
                            if target.id_hash() != sender_hash {
                                if targets.contains(target) {
                                    // Direct route: the targeted-send optimization.
                                    send_to_peer!(target, data);
                                } else {
                                    // No direct route: fall back to unconditional
                                    // flooding so the signal still reaches the
                                    // target via intermediate hops (each applies
                                    // this same rule; seen-set dedups, hop-TTL
                                    // bounds it). Dropping here silently breaks
                                    // RPC requests, RPC responses, and consensus
                                    // votes between non-peered pairs — exactly
                                    // what partial meshes built with
                                    // max_active_connections produce. Forwarding
                                    // must stay unconditional; only *admission*
                                    // is scoped (Boundary::admits).
                                    let c = individual_flood_fallbacks.fetch_add(1, Ordering::Relaxed);
                                    if c == 0 || c.is_multiple_of(256) {
                                        warn!(target_node = %target, count = c + 1,
                                            "Individual-scoped frame has no direct route; \
                                             flooding via relay (topology pressure — consider \
                                             peering RPC-heavy pairs directly)");
                                    }
                                    for peer in targets.iter().filter(|p| p.id_hash() != sender_hash) {
                                        send_to_peer!(peer, data);
                                    }
                                }
                            }
                        }
                        ForwardHint::Group(name) => {
                            // Acquire: pairs with Release in store.rs grp_generation.fetch_add.
                            // Ensures that a new gen value is only observed after the grp/ KV
                            // write that caused it is also visible, preventing a stale roster
                            // from being used immediately after a membership change.
                            let current_gen = grp_generation.load(Ordering::Acquire);
                            let members: &AHashSet<NodeId> = {
                                let entry = group_member_cache.entry(Arc::clone(name));
                                let (roster_gen, set) = entry.or_insert((u64::MAX, AHashSet::new()));
                                if *roster_gen != current_gen {
                                    let prefix = crate::signal::grp_prefix(name);
                                    let idx_guard = prefix_index.pin();
                                    *set = idx_guard.get("grp")
                                        .map(|bucket| {
                                            bucket.pin().iter()
                                                .filter_map(|(key, _)| {
                                                    if !key.starts_with(&*prefix) { return None; }
                                                    key[prefix.len()..].parse::<NodeId>().ok()
                                                })
                                                .collect()
                                        })
                                        .unwrap_or_default();
                                    *roster_gen = current_gen;
                                }
                                set
                            };

                            for peer in targets.iter()
                                .filter(|p| p.id_hash() != sender_hash && members.contains(*p))
                            {
                                send_to_peer!(peer, data);
                            }

                            let mut non_members: Vec<&NodeId> = targets.iter()
                                .filter(|p| p.id_hash() != sender_hash && !members.contains(*p))
                                .collect();
                            if !non_members.is_empty() {
                                let k = epidemic_extra_peers.min(non_members.len());
                                // Shuffle first so that peers with equal locality scores
                                // (or no known locality) are picked uniformly across
                                // emissions. When a self_locality is configured, a stable
                                // sort by shared_prefix_len then biases toward topology-
                                // closer peers while preserving random tie-breaking.
                                fastrand::shuffle(&mut non_members);
                                if let Some(self_loc) = self_locality.as_ref() {
                                    let pl_guard = peer_localities.pin();
                                    non_members.sort_by_key(|p| {
                                        let score = pl_guard.get(*p)
                                            .map(|loc| loc.shared_prefix_len(self_loc))
                                            .unwrap_or(0);
                                        std::cmp::Reverse(score)
                                    });
                                }
                                for peer in non_members.iter().take(k) {
                                    send_to_peer!(*peer, data);
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
    peer_writers:          Arc<papaya::HashMap<NodeId, WriterEntry>>,
    peer_list_tx:          watch::Sender<Arc<[NodeId]>>,
    shutdown_tx:           Arc<watch::Sender<bool>>,
    hlc:                   Arc<crate::hlc::Hlc>,
    interval_secs:         u64,
    writer_depth:          usize,
    backoff:               Duration,
    idle_timeout:          Duration,
    peer_eviction_intervals: u64,
    health_monitor_alive:    Arc<AtomicBool>,
    ping_peer_sample_size:    usize,
    max_active_connections:   usize,
    health_check_max_jitter:  u64,
    hash_acc:                 Arc<AtomicU64>,
    dropped_frames:          Arc<AtomicU64>,
    signal_handlers:         Arc<SignalHandlers>,
    signal_window_secs:      u64,
    tls:                     Option<Arc<NodeTls>>,
) {
    let bootstrap_set: AHashSet<NodeId> = bootstrap_peers.iter().cloned().collect();
    let mut shutdown_rx = shutdown_tx.subscribe();
    health_monitor_alive.store(true, Ordering::Relaxed);
    let _alive_guard = AliveGuard(Arc::clone(&health_monitor_alive));

    let max_jitter = if health_check_max_jitter > 0 { health_check_max_jitter } else { interval_secs * 500 };
    let jitter_ms = fastrand::u64(0..max_jitter.max(1));
    tokio::select! {
        _ = time::sleep(Duration::from_millis(jitter_ms)) => {}
        _ = shutdown_rx.wait_for(|v| *v) => return,
    }

    for peer in &bootstrap_set {
        request_state(peer, &peer_writers, writer_depth, backoff, idle_timeout, &shutdown_tx, &node_id, &hash_acc, &dropped_frames, vec![], tls.clone());
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
                    // peer_list_tx feeds signal fan-out; always send the full known set.
                    let peer_list: Arc<[NodeId]> = current_peer_set.iter().cloned().collect();
                    let _ = peer_list_tx.send(peer_list);

                    // Build the new active-connection set: always include bootstrap peers,
                    // then randomly sample from the rest up to max_active_connections.
                    // When max_active_connections == 0 the cap is disabled (full mesh).
                    let mut new_targets: AHashSet<NodeId> = bootstrap_set.clone();
                    new_targets.extend(current_peer_set.iter().cloned());
                    new_targets.remove(&node_id);

                    if max_active_connections > 0 && new_targets.len() > max_active_connections {
                        let slots = max_active_connections.saturating_sub(
                            bootstrap_set.iter().filter(|p| **p != node_id).count()
                        );
                        let mut non_bootstrap: Vec<NodeId> = new_targets.iter()
                            .filter(|p| !bootstrap_set.contains(*p))
                            .cloned()
                            .collect();
                        // Fisher-Yates partial shuffle to select `slots` random peers.
                        for i in 0..slots.min(non_bootstrap.len()) {
                            let j = i + fastrand::usize(..non_bootstrap.len() - i);
                            non_bootstrap.swap(i, j);
                        }
                        non_bootstrap.truncate(slots);
                        new_targets = bootstrap_set.iter()
                            .filter(|p| **p != node_id)
                            .cloned()
                            .collect();
                        new_targets.extend(non_bootstrap);
                    }

                    // Trigger anti-entropy with every peer newly entering the active set so
                    // soft-state keys (capabilities, locality) propagate on reconnection
                    // without waiting for the next advertisement tick.
                    for peer in new_targets.difference(&cached_ping_targets) {
                        request_state(peer, &peer_writers, writer_depth, backoff,
                            idle_timeout, &shutdown_tx, &node_id, &hash_acc,
                            &dropped_frames, vec![], tls.clone());
                    }
                    // Bootstrap peers are pre-loaded into cached_ping_targets, so they never
                    // appear in the difference above. Fire a separate StateRequest for each
                    // bootstrap peer that just appeared in the active peer set (sent us a Ping)
                    // for the first time. This handles the reconnect case where the startup
                    // StateRequest's response was dropped by writer backoff: the peer is not
                    // "new" to cached_ping_targets but is "new" to last_peer_set, so we
                    // re-trigger once the cooldown has cleared (cooldown = interval - 1 s).
                    for peer in current_peer_set.iter()
                        .filter(|p| bootstrap_set.contains(*p) && !last_peer_set.contains(*p))
                    {
                        request_state(peer, &peer_writers, writer_depth, backoff,
                            idle_timeout, &shutdown_tx, &node_id, &hash_acc,
                            &dropped_frames, vec![], tls.clone());
                    }

                    ping_sender_cache.retain(|k, _| new_targets.contains(k));
                    cached_ping_targets = new_targets;
                    last_peer_set = current_peer_set;
                }
                for peer in &cached_ping_targets {
                    let tx = if let Some(t) = ping_sender_cache.get(peer) {
                        t.clone()
                    } else {
                        let Some(t) = get_or_spawn_writer(
                            peer, &peer_writers, writer_depth, backoff, idle_timeout, &shutdown_tx, &dropped_frames, tls.clone(),
                        ) else { continue; };
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
                            let Some(new_tx) = get_or_spawn_writer(
                                peer, &peer_writers, writer_depth, backoff, idle_timeout, &shutdown_tx, &dropped_frames, tls.clone(),
                            ) else { continue; };
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

                // Advance the HLC's physical floor by ticking it; if wall time
                // has moved past the previous physical, the logical resets to
                // 0. This keeps the clock fresh for seen-set TTL eviction
                // even when this node has no outbound traffic and would
                // otherwise let its HLC drift behind real time.
                let _ = hlc.tick();

                signal_handlers.trim_sender_log(Duration::from_secs(signal_window_secs));

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
    kv_state:          Arc<crate::store::KvState>,
    shutdown_tx:       Arc<watch::Sender<bool>>,
    interval_secs:     u64,
    default_ttl:       u8,
    propagation_window: u64,
    live_entries:      Arc<AtomicUsize>,
    seen:              Arc<ShardedSeen>,
    max_seen_entries:  usize,
    gc_alive:          Arc<AtomicBool>,
    peer_writers:      Arc<papaya::HashMap<NodeId, WriterEntry>>,
    intern_max_keys:   usize,
    signal_boundary:   Arc<parking_lot::RwLock<crate::signal::Boundary>>,
    node_id:           NodeId,
) {
    let store         = &kv_state.store;
    let subscriptions = &kv_state.subscriptions;
    gc_alive.store(true, Ordering::Relaxed);
    let _alive_guard = AliveGuard(Arc::clone(&gc_alive));
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

                let live = crate::store::sweep_stale_tombstones(store, tombstone_cutoff);
                live_entries.store(live, Ordering::Relaxed);

                let pool_len = intern_pool_len();
                if intern_max_keys > 0 && pool_len > intern_max_keys {
                    // Pool exceeded cap: evict entries with no external holders so new keys
                    // can be interned rather than falling back to unshared allocations.
                    crate::store::shrink_intern_pool(intern_max_keys);
                    debug!(
                        "Intern pool shrunk from {} to {} entries (cap {})",
                        pool_len, intern_pool_len(), intern_max_keys
                    );
                } else {
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
                }

                {
                    let sub_guard = subscriptions.pin();
                    let stale: Vec<Arc<str>> = sub_guard
                        .iter()
                        .filter_map(|(k, tx)| if tx.is_closed() { Some(Arc::clone(k)) } else { None })
                        .collect();
                    for key in &stale {
                        sub_guard.compute(Arc::clone(key), |existing| match existing {
                            Some((_, tx)) if tx.is_closed() => papaya::Operation::Remove,
                            _ => papaya::Operation::Abort(()),
                        });
                    }
                }

                // Evict closed prefix_watchers and prefix_predicate_watchers whose
                // subscribers have been dropped without a matching write ever hitting
                // their prefix (the lazy write-path eviction doesn't fire in that case).
                {
                    let pw_guard = kv_state.prefix_watchers.pin();
                    let stale: Vec<Arc<str>> = pw_guard
                        .iter()
                        .filter_map(|(k, tx)| if tx.is_closed() { Some(Arc::clone(k)) } else { None })
                        .collect();
                    for key in stale {
                        pw_guard.compute(key, |existing| match existing {
                            Some((_, tx)) if tx.is_closed() => papaya::Operation::Remove,
                            _ => papaya::Operation::Abort(()),
                        });
                    }
                }
                {
                    let ppw_guard = kv_state.prefix_predicate_watchers.pin();
                    let stale: Vec<u64> = ppw_guard
                        .iter()
                        .filter_map(|(id, w)| if w.tx.is_closed() { Some(*id) } else { None })
                        .collect();
                    for id in stale {
                        ppw_guard.compute(id, |existing| match existing {
                            Some((_, w)) if w.tx.is_closed() => papaya::Operation::Remove,
                            _ => papaya::Operation::Abort(()),
                        });
                    }
                }

                // Evict quorum trackers whose caller future was dropped mid-wait
                // (Arc::strong_count == 1 means only the map holds the reference).
                {
                    let qt_guard = kv_state.quorum_trackers.pin();
                    let orphaned: Vec<Arc<str>> = qt_guard
                        .iter()
                        .filter_map(|(k, v)| {
                            if Arc::strong_count(v) == 1 { Some(Arc::clone(k)) } else { None }
                        })
                        .collect();
                    for key in orphaned {
                        qt_guard.compute(key, |existing| match existing {
                            Some((_, v)) if Arc::strong_count(v) == 1 => papaya::Operation::Remove,
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

                {
                    let finished: Vec<NodeId> = {
                        let guard = peer_writers.pin();
                        guard.iter()
                            .filter(|(_, e)| !e.is_live())
                            .map(|(k, _)| k.clone())
                            .collect()
                    };
                    let guard = peer_writers.pin();
                    for id in finished { guard.remove(&id); }
                }

                // Boundary reconcile — catch-all for updates missed by the push-based path.
                {
                    let node_id_str = node_id.to_string();
                    let mut bnd = signal_boundary.write();
                    crate::signal::reconcile_boundary_from_store(store, &mut bnd, &node_id_str);
                }
            }
            _ = shutdown_rx.wait_for(|v| *v) => break,
        }
    }

    if !*shutdown_rx.borrow() {
        error!("GC task exited unexpectedly; tombstone expiry and subscription eviction have stopped");
    }
}

/// Upgrades a plain `TcpStream` to a `GossipStream` by performing a TLS server
/// handshake when `tls` is `Some`. Returns the plain stream unchanged otherwise.
async fn tls_accept(
    stream: tokio::net::TcpStream,
    tls: &Option<Arc<NodeTls>>,
) -> Result<GossipStream, std::io::Error> {
    #[cfg(feature = "tls")]
    if let Some(node_tls) = tls {
        let acceptor = tokio_rustls::TlsAcceptor::from(node_tls.server_config());
        let tls_stream = acceptor.accept(stream).await?;
        return Ok(GossipStream::TlsServer(tls_stream));
    }
    let _ = tls;
    Ok(GossipStream::Plain(stream))
}
