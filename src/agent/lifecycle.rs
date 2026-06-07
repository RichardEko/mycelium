use crate::error::GossipError;
use crate::signal::reconcile_boundary_from_store;
#[cfg(feature = "tls")] use crate::signal::kv_ns;
use crate::store::{apply_and_notify, intern_key};
use crate::framing::GossipUpdate;
#[cfg(feature = "tls")] use bytes::Bytes;
use std::{
    net::{IpAddr, SocketAddr},
    sync::{
        atomic::Ordering,
        Arc,
    },
    time::Duration,
};
use tokio::{
    fs as tfs,
    sync::Semaphore,
    time,
};
use tracing::{info, warn};

use crate::connection::ConnContext;
use super::{GossipAgent, AgentState};
use super::helpers::{kv_delete, kv_scan_prefix, kv_set, kv_subscribe_prefix};
use super::tasks::{
    ListenerContext, run_gossip_shard, run_health_monitor, run_gc_task, run_listener_task, new_listener,
};

impl GossipAgent {
    /// Binds the TCP listener(s) and launches background loops.
    pub async fn start(&self) -> Result<(), GossipError> {
        self.config.validate()?;
        let bind_ip: IpAddr = self.config.bind_address.parse().map_err(|e| {
            GossipError::InvalidField {
                field:  "bind_address",
                reason: format!("'{}': {}", self.config.bind_address, e),
            }
        })?;
        let bind_addr = SocketAddr::new(bind_ip, self.config.bind_port);
        if self.node_id.to_socket_addr() != bind_addr {
            return Err(GossipError::NodeIdMismatch {
                node_id:   self.node_id.to_string(),
                bind_addr: bind_addr.to_string(),
            });
        }

        match self.state.compare_exchange(
            AgentState::Idle as u8, AgentState::Running as u8,
            Ordering::AcqRel, Ordering::Acquire,
        ) {
            Ok(_) => {}
            Err(v) if v == AgentState::Running as u8 => {
                return Err(GossipError::AlreadyRunning);
            }
            Err(_) => {
                return Err(GossipError::Shutdown);
            }
        }
        // Replay WAL + snapshot before any boundary/quorum warm-up reads the store.
        if let Some(ref pcfg) = self.config.persistence {
            let dir = pcfg.base_path.join(self.node_id.to_string()).join("kv");
            if let Err(e) = tfs::create_dir_all(&dir).await {
                warn!("persistence: failed to create data dir {:?}: {e}", dir);
            } else {
                let kv_state     = Arc::clone(&self.kv_state);
                let intern_keys  = self.config.intern_keys;
                let intern_max   = self.config.intern_max_keys;
                let hlc          = Arc::clone(&self.task_ctx.hlc);
                let node_id      = self.node_id.clone();
                let default_ttl  = self.config.default_ttl;
                let sync_mode    = pcfg.sync_mode;
                let threshold    = pcfg.snapshot_wal_threshold;
                let interval     = pcfg.snapshot_interval_secs;

                let apply_fn = {
                    let kv_state = Arc::clone(&kv_state);
                    move |entry: crate::framing::SyncEntry| {
                        let key = if intern_keys {
                            intern_key(entry.key, intern_max)
                        } else {
                            entry.key
                        };
                        let upd = GossipUpdate {
                            nonce:        crate::framing::ANTI_ENTROPY_NONCE,
                            sender:       0,
                            ttl:          1,
                            is_tombstone: entry.is_tombstone,
                            timestamp:    entry.timestamp,
                            key,
                            value:        entry.value,
                        };
                        apply_and_notify(&kv_state, &upd);
                    }
                };

                match crate::persistence::replay(&dir, apply_fn).await {
                    Ok(max_ts) => {
                        if max_ts > 0 { hlc.observe(max_ts); }
                    }
                    Err(e) => warn!("persistence: replay failed: {e}"),
                }

                let handle = crate::persistence::spawn_wal_writer(
                    dir.clone(),
                    sync_mode,
                    threshold,
                    interval,
                    Arc::clone(&kv_state),
                    node_id,
                    Arc::clone(&hlc),
                    default_ttl,
                );
                let handle = Arc::new(handle);
                // Compact immediately so next restart has a bounded replay window.
                let _ = handle.trigger_snapshot().await;
                let _ = self.task_ctx.wal.set(handle);
            }
        }

        self.rehydrate_boundary_from_kv();
        self.warm_quorum_from_layer1();
        self.prewarm_peer_localities();
        self.advertise_locality();

        // Initialise TLS context if configured.
        #[cfg(feature = "tls")]
        if let Some(ref tls_cfg) = self.config.tls {
            match crate::tls::load_or_generate(tls_cfg, &self.node_id) {
                Ok(node_tls) => {
                    let arc_tls = Arc::new(node_tls);
                    // Publish Ed25519 verifying key so peers can verify signed consensus messages.
                    let vk = arc_tls.signing_key.verifying_key().to_bytes();
                    let id_key = format!("sys/identity/{}", self.node_id);
                    let _ = kv_set(&self.task_ctx, Arc::from(id_key.as_str()), Bytes::copy_from_slice(&vk));
                    let _ = self.task_ctx.tls.set(arc_tls);
                    // Seed peer_keys from any sys/identity/ entries already in the local store.
                    self.prewarm_peer_keys();
                    // Watch for future identity publications from peers.
                    self.start_identity_watcher();
                }
                Err(e) => return Err(e),
            }
        }

        self.start_listener(bind_addr).await.inspect_err(|_| {
            self.state.store(AgentState::Idle as u8, Ordering::Release);
        })?;
        self.start_gossip_loop();
        self.start_health_monitor();
        self.start_gc_task();
        #[cfg(feature = "gateway")]
        if let Some(port) = self.config.http_port {
            let http_addr: std::net::IpAddr = self.config.http_addr.parse().map_err(|_| {
                GossipError::InvalidField {
                    field:  "http_addr",
                    reason: format!("'{}' is not a valid IP address", self.config.http_addr),
                }
            })?;
            let http_bind = SocketAddr::new(http_addr, port);
            let ctx   = Arc::clone(&self.task_ctx);
            let srx   = self.shutdown_tx.subscribe();
            let extra = self.extra_routes.lock().unwrap_or_else(|e| e.into_inner()).take();
            self.spawn_task(async move {
                if let Err(e) = super::http::run_http_server(http_bind, ctx, srx, extra).await {
                    tracing::error!("HTTP server exited: {e}");
                }
            });
        }
        self.start_capability_group_watcher();
        info!("Gossip agent started: {}", self.node_id);
        Ok(())
    }

    /// Spawns the background task that keeps this node's `grp/` membership in
    /// sync with the `cap-group/` definitions whose filter matches its own
    /// capabilities (Phase 3h dual-subscription watcher).
    fn start_capability_group_watcher(&self) {
        let def_rx      = kv_subscribe_prefix(&self.task_ctx, Arc::<str>::from("cap-group/"));
        let own_prefix  = Arc::<str>::from(format!("cap/{}/", self.node_id).as_str());
        let own_rx      = kv_subscribe_prefix(&self.task_ctx, own_prefix);
        let shutdown_rx = self.shutdown_tx.subscribe();
        let ctx         = Arc::clone(&self.task_ctx);
        let own_node_id = self.node_id.clone();
        self.spawn_task(super::emergent_groups::watch_capability_group_definitions(
            ctx, own_node_id, def_rx, own_rx, shutdown_rx,
        ));
    }

    async fn start_listener(&self, addr: SocketAddr) -> Result<(), GossipError> {
        #[cfg(unix)]
        let n_listeners = self.task_ctx.gossip_txs.len().clamp(1, 4);
        #[cfg(not(unix))]
        let n_listeners: usize = 1;

        info!("Listening on {} ({} accept task{})",
            addr, n_listeners, if n_listeners == 1 { "" } else { "s" });

        let mut listeners: Vec<tokio::net::TcpListener> = Vec::with_capacity(n_listeners);
        for _ in 0..n_listeners {
            listeners.push(new_listener(addr, self.config.tcp_accept_backlog).await?);
        }

        let conn = ConnContext {
            task_ctx:        Arc::clone(&self.task_ctx),
            peers:           Arc::clone(&self.peers),
            shutdown:        Arc::clone(&self.shutdown_tx),
            peer_writers:    Arc::clone(&self.peer_writers),
            writer_depth:    self.config.writer_channel_depth,
            backoff:         Duration::from_secs(self.config.reconnect_backoff_secs),
            n_shards:        self.task_ctx.gossip_txs.len(),
            intern_keys:     self.config.intern_keys,
            intern_max_keys: self.config.intern_max_keys,
            max_peers:           self.config.max_peers,
            writer_idle_timeout: Duration::from_secs(self.config.writer_idle_timeout_secs),
        };
        let lctx = ListenerContext {
            conn,
            conn_sem:       Arc::new(Semaphore::new(self.config.max_connections)),
            listener_alive: Arc::clone(&self.listener_alive),
            max_conn:       self.config.max_connections,
            addr,
            tcp_backlog:    self.config.tcp_accept_backlog,
            tls:            self.task_ctx.tls.get().cloned(),
        };

        let mut handles = self.task_handles_lock();
        for listener in listeners {
            handles.spawn(run_listener_task(listener, lctx.clone()));
        }
        Ok(())
    }

    fn start_gossip_loop(&self) {
        let bootstrap_peers        = Arc::clone(&self.bootstrap_peers);
        let peer_writers           = Arc::clone(&self.peer_writers);
        let shutdown_tx            = Arc::clone(&self.shutdown_tx);
        let writer_depth           = self.config.writer_channel_depth;
        let backoff                = Duration::from_secs(self.config.reconnect_backoff_secs);
        let idle_timeout           = Duration::from_secs(self.config.writer_idle_timeout_secs);
        let group_aware_forwarding = self.config.group_aware_forwarding;
        let epidemic_extra_peers   = self.config.epidemic_extra_peers;
        let prefix_index           = std::sync::Arc::<papaya::HashMap<std::sync::Arc<str>, std::sync::Arc<papaya::HashMap<std::sync::Arc<str>, ()>>>>::clone(&self.kv_state.prefix_index);
        let grp_generation         = Arc::clone(&self.kv_state.grp_generation);
        let peer_localities        = Arc::clone(&self.kv_state.peer_localities);
        let self_locality          = self.self_locality();

        let rxs = self.gossip_rxs
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
            .expect("gossip loop started twice");
        for (shard_idx, gossip_rx) in rxs.into_iter().enumerate() {
            self.spawn_task(run_gossip_shard(
                shard_idx,
                gossip_rx,
                Arc::clone(&bootstrap_peers),
                Arc::clone(&peer_writers),
                Arc::clone(&shutdown_tx),
                self.peer_list_tx.subscribe(),
                Arc::clone(&self.shard_alive[shard_idx]),
                writer_depth,
                backoff,
                idle_timeout,
                self.config.max_forwarding_peers,
                Arc::clone(&self.kv_state.dropped_frames),
                group_aware_forwarding,
                epidemic_extra_peers,
                std::sync::Arc::<papaya::HashMap<std::sync::Arc<str>, std::sync::Arc<papaya::HashMap<std::sync::Arc<str>, ()>>>>::clone(&prefix_index),
                Arc::clone(&grp_generation),
                self_locality.clone(),
                Arc::clone(&peer_localities),
                self.task_ctx.tls.get().cloned(),
            ));
        }
    }

    fn start_health_monitor(&self) {
        self.spawn_task(run_health_monitor(
            self.node_id.clone(),
            Arc::clone(&self.bootstrap_peers),
            Arc::clone(&self.peers),
            Arc::clone(&self.peer_writers),
            self.peer_list_tx.clone(),
            Arc::clone(&self.shutdown_tx),
            Arc::clone(&self.task_ctx.hlc),
            self.config.health_check_interval_secs,
            self.config.writer_channel_depth,
            Duration::from_secs(self.config.reconnect_backoff_secs),
            Duration::from_secs(self.config.writer_idle_timeout_secs),
            self.config.peer_eviction_intervals,
            Arc::clone(&self.health_monitor_alive),
            self.config.ping_peer_sample_size,
            self.config.max_active_connections,
            self.config.health_check_max_jitter_ms,
            Arc::clone(&self.kv_state.hash_acc),
            Arc::clone(&self.kv_state.dropped_frames),
            Arc::clone(&self.task_ctx.signal_handlers),
            self.config.signal_window_secs,
            self.task_ctx.tls.get().cloned(),
        ));
    }

    fn start_gc_task(&self) {
        self.spawn_task(run_gc_task(
            Arc::clone(&self.kv_state),
            Arc::clone(&self.shutdown_tx),
            self.config.health_check_interval_secs,
            self.config.default_ttl,
            self.config.propagation_window_secs,
            Arc::clone(&self.live_entries),
            Arc::clone(&self.task_ctx.seen),
            self.config.max_seen_entries,
            Arc::clone(&self.gc_alive),
            Arc::clone(&self.peer_writers),
            self.config.intern_max_keys,
            Arc::clone(&self.task_ctx.signal_boundary),
            self.node_id.clone(),
        ));
    }

    /// Seeds `signal_handlers.sender_log` from `sys/quorum/` Layer I entries written
    /// during the previous run. Called once in `start()` so `quorum()` returns a correct
    /// result on the first call after a process restart rather than always returning false.
    pub(crate) fn warm_quorum_from_layer1(&self) {
        use std::time::{SystemTime, UNIX_EPOCH};
        use crate::node_id::NodeId;
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let window_ms = self.config.signal_window_secs * 1000;
        use crate::signal::kv_ns;
        let prefix = kv_ns::QUORUM;
        for (key, bytes) in kv_scan_prefix(&self.task_ctx, prefix) {
            let Some(tail) = key.strip_prefix(prefix) else { continue };
            // Key format: sys/quorum/{kind}/{sender_id}
            // rfind('/') splits on the last segment, which is always the sender_id.
            let Some(slash) = tail.rfind('/') else { continue };
            let kind: std::sync::Arc<str> = std::sync::Arc::from(&tail[..slash]);
            let sender_str = &tail[slash + 1..];
            let Ok(sender) = sender_str.parse::<NodeId>() else { continue };
            if bytes.len() < 8 { continue; }
            let written_at_ms = u64::from_le_bytes(bytes[..8].try_into().expect("infallible: WAL quorum entries are always exactly 8-byte timestamps"));
            let age_ms = now_ms.saturating_sub(written_at_ms);
            if age_ms > window_ms { continue; }
            self.task_ctx.signal_handlers.seed_sender_log(kind, sender, age_ms);
        }
    }

    pub(crate) fn rehydrate_boundary_from_kv(&self) {
        let node_id_str = self.node_id.to_string();
        let mut bnd = self.task_ctx.signal_boundary.write();
        reconcile_boundary_from_store(&self.kv_state.store, &mut bnd, &node_id_str);
    }

    /// Publishes this node's [`LocalityPath`](crate::locality::LocalityPath) under
    /// `cap/{node_id}/locality/self` so peers can discover its topology address.
    ///
    /// Skipped when `config.locality_path` is empty — there is no point gossiping
    /// "unspecified locality." Tombstoned at shutdown via [`Self::shutdown_with_timeout`].
    pub(crate) fn advertise_locality(&self) {
        if self.config.locality_path.is_empty() { return; }
        let loc = crate::locality::LocalityPath::new(self.config.locality_path.iter().cloned());
        let key = format!("cap/{}/locality/self", self.node_id);
        let _ = kv_set(&self.task_ctx, Arc::from(key.as_str()), loc.encode());
    }

    /// Walks `cap/*/locality/self` in the local KV view once at startup and
    /// populates `kv_state.peer_localities` from any entries already present.
    /// Without this, the `peer_localities` cache is cold until the first
    /// fresh write or anti-entropy round, which means locality-aware
    /// resolution (`resolve_with_locality`, `signal_wired_via_locality`)
    /// silently scores every provider at depth 0 in the warm-up window.
    pub(crate) fn prewarm_peer_localities(&self) {
        let prefix = "cap/";
        let suffix = "/locality/self";
        let guard = self.kv_state.peer_localities.pin();
        for (key, bytes) in kv_scan_prefix(&self.task_ctx, prefix) {
            // Same shape-check that apply_and_notify does for live writes.
            let Some(rest) = key.strip_prefix(prefix) else { continue };
            let Some(node_id_str) = rest.strip_suffix(suffix) else { continue };
            if node_id_str.contains('/') { continue; }
            let Ok(node_id) = node_id_str.parse::<crate::node_id::NodeId>() else { continue };
            let Some(loc) = crate::locality::LocalityPath::decode(&bytes) else { continue };
            guard.insert(node_id, loc);
        }
    }

    /// Reads all `sys/identity/{node_id}` KV entries already in the local store
    /// and inserts their 32-byte public keys into `task_ctx.peer_keys`.
    /// Called at startup after TLS is initialised, before listeners are spawned.
    #[cfg(feature = "tls")]
    fn prewarm_peer_keys(&self) {
        let prefix = kv_ns::IDENTITY;
        let guard  = self.task_ctx.peer_keys.pin();
        for (key, bytes) in kv_scan_prefix(&self.task_ctx, prefix) {
            let Some(node_id_str) = key.strip_prefix(prefix) else { continue };
            let Ok(node_id) = node_id_str.parse::<crate::node_id::NodeId>() else { continue };
            if bytes.len() == 32 {
                let mut arr = [0u8; 32];
                arr.copy_from_slice(&bytes);
                guard.insert(node_id, arr);
            }
        }
    }

    /// Subscribes to `sys/identity/` prefix changes and mirrors verifying keys
    /// into `task_ctx.peer_keys` as peers publish them via anti-entropy gossip.
    /// On each notification, re-scans the full prefix so removals (tombstones) are
    /// also caught; the prefix is small (one entry per cluster node).
    #[cfg(feature = "tls")]
    fn start_identity_watcher(&self) {
        let mut rx      = kv_subscribe_prefix(&self.task_ctx, Arc::<str>::from(kv_ns::IDENTITY));
        let shutdown_rx = self.shutdown_tx.subscribe();
        let peer_keys   = Arc::clone(&self.task_ctx.peer_keys);
        let kv_state    = Arc::clone(&self.kv_state);
        self.spawn_task(async move {
            let mut shutdown_rx = shutdown_rx;
            loop {
                tokio::select! { biased;
                    _ = shutdown_rx.wait_for(|v| *v) => break,
                    res = rx.changed() => { if res.is_err() { break; } }
                }
                // Re-sync peer_keys from the current store snapshot.
                let guard       = peer_keys.pin();
                let store_guard = kv_state.store.pin();
                for (key, entry) in store_guard.iter() {
                    let Some(suffix) = key.strip_prefix(kv_ns::IDENTITY) else { continue };
                    let Ok(node_id) = suffix.parse::<crate::node_id::NodeId>() else { continue };
                    match &entry.data {
                        Some(b) if b.len() == 32 => {
                            let mut arr = [0u8; 32];
                            arr.copy_from_slice(b);
                            guard.insert(node_id, arr);
                        }
                        _ => { guard.remove(&node_id); }
                    }
                }
            }
        });
    }

    /// Signals all background tasks to stop and waits up to `timeout` for them to exit.
    ///
    /// Tasks that have not exited within the timeout are logged as warnings and abandoned.
    /// Passing `Duration::MAX` replicates the old unbounded-wait behaviour.
    pub async fn shutdown_with_timeout(&self, timeout: Duration) {
        let my_load_prefix = format!("sys/load/{}/", self.node_id);
        let load_keys: Vec<String> = self.kv_state.store.pin()
            .iter()
            .filter(|(k, v)| k.starts_with(&*my_load_prefix) && v.data.is_some())
            .map(|(k, _)| k.to_string())
            .collect();
        for key in load_keys {
            let _ = kv_delete(&self.task_ctx, Arc::from(key.as_str()));
        }
        // Retract our locality advertisement so peers stop using a stale entry
        // for topology-aware fan-out and quorum diversity.
        if !self.config.locality_path.is_empty() {
            let _ = kv_delete(&self.task_ctx, Arc::from(format!("cap/{}/locality/self", self.node_id).as_str()));
        }

        // Retract our capability/requirement/group-def advertisements so peers
        // do not see stale providers, requirers, or niche definitions. The
        // per-handle persist tasks already tombstone individual keys on drop,
        // but the agent may shut down before user handles are dropped — the
        // sweep here is a safety net.
        let cap_prefix            = format!("cap/{}/",             self.node_id);
        let req_prefix            = format!("req/{}/",             self.node_id);
        let req_load_prefix       = format!("sys/load/{}/req/",       self.node_id);
        let group_req_load_prefix = format!("sys/load/{}/group-req/", self.node_id);
        let agent_owned_keys: Vec<String> = self.kv_state.store.pin()
            .iter()
            .filter(|(k, v)| {
                v.data.is_some() && (
                    k.starts_with(&*cap_prefix) ||
                    k.starts_with(&*req_prefix) ||
                    k.starts_with(&*req_load_prefix) ||
                    k.starts_with(&*group_req_load_prefix)
                )
            })
            .map(|(k, _)| k.to_string())
            .collect();
        for key in agent_owned_keys {
            let _ = kv_delete(&self.task_ctx, Arc::from(key.as_str()));
        }
        // `cap-group/{name}` definitions are gossiped by whichever node
        // currently owns the CapabilityGroupHandle. We cannot tell ownership
        // from the key alone; rely on the persist task's own tombstone path
        // for graceful retract. On crash the def persists until TTL — this
        // is intentional (the niche outlives one individual).
        let _ = self.state.compare_exchange(
            AgentState::Running as u8, AgentState::Stopped as u8,
            Ordering::AcqRel, Ordering::Acquire,
        );
        let _ = self.shutdown_tx.send(true);
        // Signal all peer writer tasks to exit. They run as detached tasks and exit
        // via peer_shutdown or the global shutdown signal already sent above.
        {
            let guard = self.peer_writers.pin();
            let keys: Vec<crate::node_id::NodeId> = guard.iter()
                .map(|(k, v)| { let _ = v.peer_shutdown.send(true); k.clone() })
                .collect();
            for key in keys { guard.remove(&key); }
        }
        // Drain the JoinSet. Swap it out first so the std::sync::Mutex is not held
        // across any await point (clippy::await_holding_lock).
        let drain = async {
            let mut owned = {
                let mut handles = self.task_handles_lock();
                let mut empty = tokio::task::JoinSet::new();
                std::mem::swap(&mut *handles, &mut empty);
                empty
            };
            while owned.join_next().await.is_some() {}
        };
        if time::timeout(timeout, drain).await.is_err() {
            let mut handles = self.task_handles_lock();
            let remaining = handles.len();
            warn!("shutdown: {} task(s) did not exit within {:?}; abandoning", remaining, timeout);
            handles.abort_all();
        }
    }

    /// Signals all background tasks to stop and waits for them to exit.
    /// Uses a 5-second timeout; tasks that do not exit are aborted and logged.
    pub async fn shutdown(&self) {
        self.shutdown_with_timeout(Duration::from_secs(5)).await;
    }
}
