use crate::connection::{handle_connection, ConnContext};
use crate::stream::GossipStream;
use crate::tls::NodeTls;
use crate::error::GossipError;
use crate::framing::{ForwardHint, WireMessage};
use crate::locality::LocalityPath;
use crate::node_id::NodeId;
use crate::seen::ShardedSeen;
use crate::signal::SignalHandlers;
use crate::store::intern_pool_len;
use crate::config::resolved_fanout;
use crate::writer::{evict_peer_writer, get_or_spawn_writer, request_state, WriterEntry};
use ahash::{AHashMap, AHashSet};
use bytes::Bytes;
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
    self_id:                NodeId,
    mut gossip_rx:          mpsc::Receiver<(Bytes, u64, ForwardHint)>,
    bootstrap_peers:        Arc<[NodeId]>,
    peer_writers:           Arc<papaya::HashMap<NodeId, WriterEntry>>,
    pinned_peers:           Arc<papaya::HashMap<NodeId, ()>>,
    shutdown_tx:            Arc<watch::Sender<bool>>,
    mut peer_list_rx:       watch::Receiver<Arc<[NodeId]>>,
    alive:                  Arc<AtomicBool>,
    hot:                    Arc<mycelium_core::context::HotConfig>,
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
                    peer, &peer_writers, hot.writer_depth(), backoff, idle_timeout, &shutdown_tx, &dropped_frames, tls.clone(),
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
                        peer, &peer_writers, hot.writer_depth(), backoff, idle_timeout, &shutdown_tx, &dropped_frames, tls.clone(),
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
                    // The published list IS the bounded active set (WS-B M4): use it directly
                    // as the forwarding fan-out. Bootstrap peers are NOT re-pinned here — the
                    // health monitor keeps them in the active set only while still discovering,
                    // then de-pins them, so a well-known seed stops being a forwarding target
                    // for every node (which would pin O(N) inbound connections on it).
                    targets.clear();
                    targets.extend(cached_peer_list.iter().take(max_forwarding_peers).cloned());
                    sender_cache.retain(|k, _| targets.contains(k));
                }

                // An Individual frame whose target is THIS node terminated here: both a local
                // self-emit (e.g. mailbox deliver-to-self) and a relayed frame arriving at its
                // destination traverse this queue, because forwarding is unconditional.
                // Forwarding past the target is pure waste — no other node can terminate it, so
                // it floods until seen-set/TTL kill it, and it counts/warns as topology pressure
                // against ourselves (observed in the #161 hosted run, from scenario 10's
                // deliver-to-self). Terminating an *arrived* frame is not a scope-admission
                // decision — admission already delivered it locally; there is nowhere to route.
                if let ForwardHint::Individual(target) = &hint
                    && *target == self_id {
                        continue;
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
                            // (self-targeted frames already terminated via the early continue above)
                            if target.id_hash() != sender_hash {
                                // A direct route exists if the target is an active forwarding target
                                // OR was explicitly pinned via `connect_peer` (RPC-heavy pairs, e.g.
                                // a tuple-space secondary → primary — #150). The pin survives the
                                // seed-de-pinning target rebuild, so those RPCs keep a direct route
                                // instead of degrading to flood-relay latency.
                                if targets.contains(target) || pinned_peers.pin().contains_key(target) {
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
/// Sticky reconciliation of the active-connection (ping) target set toward fan-out
/// `k` (WS-B M4 partial mesh). Prefers keeping existing live members — when the set
/// is already at `k` live peers it is a no-op, so there is no per-tick churn. Only
/// the deficit is filled (uniform-random from known peers) or the surplus trimmed.
///
/// Bootstrap peers are retained/added as a *discovery aid* while `retain_bootstrap`
/// is set (the caller passes `known.len() < AUTO_FANOUT_FLOOR` — an absolute floor,
/// so small clusters always keep their seed and only at scale, once a node has
/// discovered enough peers, is the seed de-pinned and sampled like any other peer,
/// stopping it accumulating O(N) inbound connections). Returns `(added, removed)` —
/// the caller fires anti-entropy for newly active peers and evicts the writers of
/// dropped ones.
fn reconcile_active_targets(
    active:           &mut AHashSet<NodeId>,
    known:            &AHashSet<NodeId>,   // current live peers (excludes self)
    bootstrap:        &AHashSet<NodeId>,   // excludes self
    k:                usize,
    retain_bootstrap: bool,
) -> (Vec<NodeId>, Vec<NodeId>) {
    // 1. Retention: keep members still alive; while still discovering also keep
    //    bootstrap peers even if they have not pinged back yet (our only route in).
    let mut removed: Vec<NodeId> = active
        .iter()
        .filter(|p| !(known.contains(*p) || retain_bootstrap && bootstrap.contains(*p)))
        .cloned()
        .collect();
    for p in &removed { active.remove(p); }

    // 2. Candidate pool: known peers not already active; while discovering, bootstrap too.
    let mut pool: Vec<NodeId> = known.iter().filter(|p| !active.contains(*p)).cloned().collect();
    if retain_bootstrap {
        for b in bootstrap {
            if !active.contains(b) && !known.contains(b) { pool.push(b.clone()); }
        }
    }

    // 3. Fill the deficit up to k with uniform-random picks.
    let mut added = Vec::new();
    while active.len() < k && !pool.is_empty() {
        let pick = pool.swap_remove(fastrand::usize(..pool.len()));
        if active.insert(pick.clone()) { added.push(pick); }
    }

    // 4. Trim the surplus (k shrank) — drop random members, never a bootstrap peer
    //    while still discovering.
    if active.len() > k {
        let drop_n = active.len() - k;
        let mut victims: Vec<NodeId> = active.iter()
            .filter(|p| !(retain_bootstrap && bootstrap.contains(*p)))
            .cloned()
            .collect();
        for i in 0..drop_n.min(victims.len()) {
            let j = i + fastrand::usize(..victims.len() - i);
            victims.swap(i, j);
            active.remove(&victims[i]);
            removed.push(victims[i].clone());
        }
    }

    (added, removed)
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
    kv_state:              Arc<crate::store::KvState>,
    interval_secs:         u64,
    hot:                   Arc<mycelium_core::context::HotConfig>,
    backoff:               Duration,
    idle_timeout:          Duration,
    peer_eviction_intervals: u64,
    health_monitor_alive:    Arc<AtomicBool>,
    ping_peer_sample_size:    usize,
    max_active_connections:   usize,
    gossip_fanout:            usize,
    swim_enabled:             bool,
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

    // Startup anti-entropy pull from the bootstrap peers. Skipped under SWIM: it would
    // make every node open a TCP connection to the (shared) seed at once — the dominant
    // source of the seed's startup connection spike — and under SWIM the initial KV state
    // is instead pulled via anti-entropy on the first forwarding-set members (uniform
    // random), so the seed is not a universal anti-entropy target.
    if !swim_enabled {
        // Send our current Merkle digest so any state restored from WAL/snapshot is not
        // re-pulled; an empty store yields an all-zero digest ⇒ responder full-dumps.
        let digest = crate::store::store_bucket_hashes(&kv_state);
        for peer in &bootstrap_set {
            request_state(peer, &peer_writers, hot.writer_depth(), backoff, idle_timeout, &shutdown_tx, &node_id, &hash_acc, &dropped_frames, digest.clone(), tls.clone());
        }
    }

    // M10 (WS-C): the tick cadence is live-reconfigurable. We re-read the hot value each loop and
    // recreate the interval only when it changes, so a `TimingIntent` (or local `set_timing`) retunes
    // the health-check cadence with no task restart — preserving the immediate-first-tick + Skip
    // semantics. `0` ⇒ keep the static config value.
    let mut current_health_secs = hot.health_interval_secs(interval_secs);
    let mut ticker = time::interval(Duration::from_secs(current_health_secs.max(1)));
    ticker.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
    // With SWIM (M5 cutover) the forwarding set starts empty and is NOT seeded with the
    // bootstrap peers: liveness + discovery ride UDP probing/gossip, so the forwarding
    // fan-out is a pure uniform-random sample of the membership — no node permanently
    // pins the seed (the residual M4 left). Without SWIM, keep the v1 behaviour (seed the
    // ping set with bootstrap so the TCP ping path bootstraps discovery).
    let mut cached_ping_targets: AHashSet<NodeId> =
        if swim_enabled { AHashSet::new() } else { bootstrap_set.clone() };
    cached_ping_targets.remove(&node_id);
    // Bootstrap set excluding self — the discovery-aid set for fan-out reconciliation.
    let bootstrap_no_self: AHashSet<NodeId> =
        bootstrap_set.iter().filter(|p| **p != node_id).cloned().collect();
    let mut last_peer_set: AHashSet<NodeId> = AHashSet::new();
    let mut ping_sender_cache: AHashMap<NodeId, mpsc::Sender<Bytes>> = AHashMap::new();
    // SWIM anti-entropy de-churning state. `last_anti_entropy` records when we last fired a
    // StateRequest to each peer, so a churning forwarding set cannot re-trigger a sync storm
    // (each re-add used to re-request state, and the responder spawns a persistent idle-timeout
    // reply writer — so a popular peer like the seed accreted O(N) warm writers; Stage-4
    // divergence). The resync cooldown reuses the liveness window: re-sync a forwarding member
    // about once per it.
    let mut last_anti_entropy: AHashMap<NodeId, Instant> = AHashMap::new();
    let resync_cooldown =
        Duration::from_secs(interval_secs.saturating_mul(peer_eviction_intervals).max(1));

    loop {
        // M10: adopt a live cadence change on the next cycle (recreate only on change).
        let want_health_secs = hot.health_interval_secs(interval_secs);
        if want_health_secs != current_health_secs {
            current_health_secs = want_health_secs;
            ticker = time::interval(Duration::from_secs(current_health_secs.max(1)));
            ticker.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
            tracing::info!(secs = current_health_secs, "M10: health-check interval retuned live");
        }
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
                // Build the TCP heartbeat ping only when SWIM is off (it owns liveness +
                // discovery otherwise). A serialize failure skips just this tick's ping —
                // the reconcile/eviction below still runs.
                let ping_data: Option<Bytes> = if swim_enabled {
                    None
                } else {
                    for i in 0..ping_peer_sample_size.min(known.len()) {
                        let j = i + fastrand::usize(..known.len() - i);
                        known.swap(i, j);
                    }
                    known.truncate(ping_peer_sample_size);
                    Some(mycelium_core::codec::wire_to_bytes(
                        &WireMessage::Ping { sender: node_id.clone(), known_peers: known },
                    ))
                };

                if current_peer_set != last_peer_set {
                    // Fire a StateRequest for each bootstrap peer that just appeared in the
                    // active peer set (sent us a Ping) for the first time. Handles the reconnect
                    // case where the startup StateRequest's response was dropped by writer
                    // backoff: the peer may already be an active target but is "new" to
                    // last_peer_set, so we re-trigger once the cooldown has cleared.
                    // Skipped under SWIM (same reason as the startup pull): it would re-pin the
                    // shared seed as a universal anti-entropy target.
                    if !swim_enabled {
                        let reconnect_iter = current_peer_set.iter()
                            .filter(|p| bootstrap_set.contains(*p) && !last_peer_set.contains(*p));
                        let mut reconnect_peers = reconnect_iter.peekable();
                        if reconnect_peers.peek().is_some() {
                            let digest = crate::store::store_bucket_hashes(&kv_state);
                            for peer in reconnect_peers {
                                request_state(peer, &peer_writers, hot.writer_depth(), backoff,
                                    idle_timeout, &shutdown_tx, &node_id, &hash_acc,
                                    &dropped_frames, digest.clone(), tls.clone());
                            }
                        }
                    }
                    last_peer_set = current_peer_set.clone();
                }

                // WS-B M4 partial mesh: reconcile the active-connection set toward the
                // resolved fan-out every tick. Sticky — a no-op once converged — so it does
                // not churn; it only fills the deficit or trims the surplus. Newly active
                // peers get anti-entropy; dropped peers have their writers evicted so the
                // TCP connection actually closes (the connection count is bounded by k, not N).
                let k = resolved_fanout(gossip_fanout, max_active_connections, current_peer_set.len());
                // Without SWIM: keep the seed pinned until we've discovered a healthy number
                // of peers (absolute floor), then de-pin — small clusters stay full mesh.
                // With SWIM: never pin the bootstrap in the forwarding set; discovery is
                // handled by UDP gossip, so the seed is just one uniform-random candidate.
                let retain_bootstrap =
                    !swim_enabled && current_peer_set.len() < crate::config::AUTO_FANOUT_FLOOR;
                // SWIM cutover, part 1 — CONTINUOUS bootstrap de-pin. Every node discovers the
                // seed first and the early greedy fill pins it into all N forwarding sets. The
                // seed is also the one peer every node reliably reaches (it IS the bootstrap), so
                // under membership churn — e.g. UDP-loss-induced false failures transiently
                // shrinking the known-peer pool over the Docker bridge — it keeps getting
                // re-sampled into the forwarding set, and a *one-time* de-pin can never undo that
                // (Stage-4 Docker re-validation: 43/50 workers stayed pinned to the seed even with
                // fully-converged membership). So re-evict the bootstrap every tick once we know
                // more than ~1.33× our forwarding slots. The threshold is `> k + k/3` (not `2k`):
                // SWIM's UDP gossip only converges membership to a sparse value at scale over a
                // lossy bridge (~24 known at N=100), which barely reached the old `2k` so the
                // de-pin never engaged; `k + k/3` engages at that sparse membership while keeping
                // a floor of `> k` (a node needs ≥1 non-bootstrap peer to swap the seed for).
                // Below the threshold (small clusters / a node still discovering) the seed is kept
                // — connectivity over flatness.
                let depin_threshold = k + k / 3;
                let depin_active = swim_enabled && k > 0 && current_peer_set.len() > depin_threshold;
                if depin_active {
                    for b in &bootstrap_no_self {
                        if cached_ping_targets.remove(b) {
                            ping_sender_cache.remove(b);
                            last_anti_entropy.remove(b);
                            evict_peer_writer(&peer_writers, b);
                        }
                    }
                }
                // SWIM cutover, part 2 — slow uniform shuffle. Drop ONE random member per tick
                // once at capacity so the active set drifts into a moving uniform sample (no
                // member stays permanently retained) WITHOUT the churn storm of the old bulk
                // ~k/3 eviction: every freed slot is re-filled by `reconcile` and each fill used
                // to re-fire anti-entropy, re-warming reply writers cluster-wide. One swap/tick
                // is enough to keep the sample fresh; the decoupled anti-entropy below makes the
                // cost of the swap a single bounded StateRequest, not an O(N) ripple.
                if swim_enabled && cached_ping_targets.len() >= k && k > 1
                    && let Some(v) = cached_ping_targets
                        .iter()
                        .nth(fastrand::usize(..cached_ping_targets.len()))
                        .cloned()
                {
                    cached_ping_targets.remove(&v);
                    ping_sender_cache.remove(&v);
                    last_anti_entropy.remove(&v);
                    evict_peer_writer(&peer_writers, &v);
                }
                // Reconcile toward `k`. Once de-pinning, exclude the bootstrap from the candidate
                // POOL too — not just the active set — otherwise reconcile re-adds it at `k/known`,
                // which at the sparse scale-membership SWIM reaches (known ≪ N) wildly
                // over-represents the seed (e.g. k=12 of known=24 ⇒ a 50% re-add rate, so the seed
                // stays pinned on ~half the cluster despite the de-pin). Excluding it from the pool
                // keeps the seed near-zero inbound — it stays current via its own outbound
                // anti-entropy pulls — so the seed's fan-in is flat (independent of N), not `k/known`.
                let (added, removed) = if depin_active {
                    let mut pool = current_peer_set.clone();
                    for b in &bootstrap_no_self { pool.remove(b); }
                    reconcile_active_targets(&mut cached_ping_targets, &pool, &bootstrap_no_self, k, retain_bootstrap)
                } else {
                    reconcile_active_targets(
                        &mut cached_ping_targets, &current_peer_set, &bootstrap_no_self, k, retain_bootstrap)
                };
                // Publish the bounded active set to the gossip shards: it is BOTH the ping
                // set and the forwarding fan-out, so a node opens persistent writers to only
                // ~k peers (not N). Cluster-wide delivery still holds via multi-hop epidemic
                // flooding + the seen-set; Individual frames to a non-active target fall back
                // to flooding (regression-gated). Republish only when the set changes.
                if !added.is_empty() || !removed.is_empty() {
                    let active: Arc<[NodeId]> = cached_ping_targets.iter().cloned().collect();
                    let _ = peer_list_tx.send(active);
                }
                // Anti-entropy. Without SWIM: sync each newly-added forwarding peer once (the v1
                // on-add trigger). With SWIM: decouple anti-entropy from the add/remove churn —
                // sync each CURRENT forwarding member at most once per `resync_cooldown`. The
                // forwarding set is bounded by k, so a popular peer (the seed) is
                // anti-entropy-targeted by only ~k nodes per window, not by every node that
                // briefly churned it into its set — which is what made it accrete O(N) warm
                // reply writers (Stage-4 seed_out divergence). As a bonus this re-syncs stable
                // members periodically (the old on-add trigger never re-synced them, so a frame
                // dropped between two settled peers could only heal via some third peer); when
                // stores already agree the responder's hash fast-path makes it an empty exchange.
                if swim_enabled {
                    let now = Instant::now();
                    let due_peers: Vec<&NodeId> = cached_ping_targets.iter()
                        .filter(|peer| last_anti_entropy
                            .get(*peer)
                            .is_none_or(|t| now.duration_since(*t) >= resync_cooldown))
                        .collect();
                    // One digest per tick (not per peer) — a full store scan amortised
                    // across every due peer; the Merkle delta keeps each response small.
                    if !due_peers.is_empty() {
                        let digest = crate::store::store_bucket_hashes(&kv_state);
                        for peer in due_peers {
                            request_state(peer, &peer_writers, hot.writer_depth(), backoff,
                                idle_timeout, &shutdown_tx, &node_id, &hash_acc,
                                &dropped_frames, digest.clone(), tls.clone());
                            last_anti_entropy.insert(peer.clone(), now);
                        }
                    }
                    last_anti_entropy.retain(|p, _| current_peer_set.contains(p));
                } else if !added.is_empty() {
                    let digest = crate::store::store_bucket_hashes(&kv_state);
                    for peer in &added {
                        request_state(peer, &peer_writers, hot.writer_depth(), backoff,
                            idle_timeout, &shutdown_tx, &node_id, &hash_acc,
                            &dropped_frames, digest.clone(), tls.clone());
                    }
                }
                for peer in &removed {
                    ping_sender_cache.remove(peer);
                    evict_peer_writer(&peer_writers, peer);
                }
                // TCP heartbeat ping. With SWIM (M5 cutover) liveness + discovery ride UDP
                // probing/gossip, so the TCP ping is gone: forwarding writers stay warm via
                // actual gossip traffic and idle-close otherwise — which is precisely the
                // "no persistent heartbeat connections" goal (a closed idle writer leaves no
                // iptables FORWARD / conntrack entry). Without SWIM, ping as before.
                if let Some(ping_data) = ping_data.as_ref() {
                    for peer in &cached_ping_targets {
                        let tx = if let Some(t) = ping_sender_cache.get(peer) {
                            t.clone()
                        } else {
                            let Some(t) = get_or_spawn_writer(
                                peer, &peer_writers, hot.writer_depth(), backoff, idle_timeout, &shutdown_tx, &dropped_frames, tls.clone(),
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
                                    peer, &peer_writers, hot.writer_depth(), backoff, idle_timeout, &shutdown_tx, &dropped_frames, tls.clone(),
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
                }

                // Advance the HLC's physical floor by ticking it; if wall time
                // has moved past the previous physical, the logical resets to
                // 0. This keeps the clock fresh for seen-set TTL eviction
                // even when this node has no outbound traffic and would
                // otherwise let its HLC drift behind real time.
                let _ = hlc.tick();

                signal_handlers.trim_sender_log(Duration::from_secs(signal_window_secs));

                // Staleness eviction is the TCP-ping liveness model: a peer not heard from
                // within the window is dropped. Under SWIM this is WRONG — the prober
                // refreshes only one peer per period, so each peer is touched every
                // ~N×period, which is slower than any sane window; the health monitor would
                // evict live peers faster than SWIM refreshes them and collapse the
                // membership. With SWIM, liveness + eviction are owned entirely by the
                // failure detector (a confirmed-Dead member is removed via apply_effect), so
                // skip staleness eviction here.
                let eviction_window = Duration::from_secs(interval_secs.saturating_mul(peer_eviction_intervals));
                let maybe_peer_cutoff = if swim_enabled { None } else { Instant::now().checked_sub(eviction_window) };

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

#[cfg(test)]
mod reconcile_tests {
    use super::reconcile_active_targets;
    use crate::node_id::NodeId;
    use ahash::AHashSet;

    fn id(p: u16) -> NodeId { NodeId::new("127.0.0.1", p).unwrap() }
    fn set(ps: &[u16]) -> AHashSet<NodeId> { ps.iter().map(|p| id(*p)).collect() }

    #[test]
    fn fills_up_to_k_from_known() {
        let mut active = AHashSet::new();
        let known = set(&[10, 11, 12, 13, 14]);
        let (added, removed) = reconcile_active_targets(&mut active, &known, &AHashSet::new(), 3, false);
        assert_eq!(active.len(), 3);
        assert_eq!(added.len(), 3);
        assert!(removed.is_empty());
        assert!(active.iter().all(|p| known.contains(p)));
    }

    #[test]
    fn is_sticky_no_churn_once_converged() {
        let known = set(&[10, 11, 12, 13, 14, 15, 16, 17, 18, 19]);
        let mut active = AHashSet::new();
        reconcile_active_targets(&mut active, &known, &AHashSet::new(), 4, false);
        let snapshot = active.clone();
        // Re-run with the same inputs repeatedly: no member changes.
        for _ in 0..20 {
            let (added, removed) = reconcile_active_targets(&mut active, &known, &AHashSet::new(), 4, false);
            assert!(added.is_empty(), "stable set should add nothing");
            assert!(removed.is_empty(), "stable set should remove nothing");
            assert_eq!(active, snapshot, "active set must not churn");
        }
    }

    #[test]
    fn drops_dead_peers_and_refills() {
        let mut active = set(&[10, 11, 12]);
        // 11 died (no longer known); 13/14 are fresh.
        let known = set(&[10, 12, 13, 14]);
        let (added, removed) = reconcile_active_targets(&mut active, &known, &AHashSet::new(), 3, false);
        assert!(removed.contains(&id(11)));
        assert!(!active.contains(&id(11)));
        assert_eq!(active.len(), 3);
        assert_eq!(added.len(), 1); // refilled the slot freed by 11
        assert!(active.iter().all(|p| known.contains(p)));
    }

    #[test]
    fn retains_bootstrap_while_discovering_then_depins() {
        let boot = set(&[1]); // the seed
        // Still discovering (retain_bootstrap = true): seed kept even if not yet pinged back.
        let mut active = set(&[1]);
        let known_small = set(&[2, 3]);
        let (_a, removed) = reconcile_active_targets(&mut active, &known_small, &boot, 8, true);
        assert!(active.contains(&id(1)), "bootstrap kept while discovering");
        assert!(!removed.contains(&id(1)));

        // Discovered enough (retain_bootstrap = false): seed no longer force-held, and
        // since it has left the live set it is dropped and sampled like any other peer.
        let mut active2 = set(&[1, 20, 21, 22]);
        let known_big = set(&[20, 21, 22, 23, 24, 25, 26, 27, 28, 29]); // seed not among them
        let (_a2, removed2) = reconcile_active_targets(&mut active2, &known_big, &boot, 4, false);
        assert!(!active2.contains(&id(1)), "seed de-pinned once discovery floor crossed");
        assert!(removed2.contains(&id(1)));
        assert_eq!(active2.len(), 4);
    }

    #[test]
    fn small_cluster_keeps_full_connectivity_with_bootstrap_retained() {
        // Regression for the n=7 partial-mesh gate: with k == known and retain_bootstrap
        // true, every known peer (incl. the bootstrap edge) stays active — no partition.
        let boot = set(&[1]);
        let known = set(&[1, 2, 3, 4, 5, 6]); // 6 known incl. seed
        let mut active = set(&[1]);
        let (_added, removed) = reconcile_active_targets(&mut active, &known, &boot, 6, true);
        assert_eq!(active.len(), 6, "all known peers stay connected in a small cluster");
        assert!(active.contains(&id(1)), "bootstrap edge retained");
        assert!(removed.is_empty());
    }

    #[test]
    fn trims_surplus_when_k_shrinks() {
        let known = set(&[10, 11, 12, 13, 14, 15]);
        let mut active = known.clone(); // 6 active
        let (added, removed) = reconcile_active_targets(&mut active, &known, &AHashSet::new(), 2, false);
        assert!(added.is_empty());
        assert_eq!(active.len(), 2);
        assert_eq!(removed.len(), 4);
    }
}
