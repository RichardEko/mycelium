use crate::error::GossipError;
use crate::signal::reconcile_boundary_from_store;
use std::{
    net::{IpAddr, SocketAddr},
    sync::{
        atomic::Ordering,
        Arc,
    },
    time::Duration,
};
use tokio::{
    sync::Semaphore,
    time,
};
use tracing::{info, warn};

use crate::connection::ConnContext;
use super::{GossipAgent, AgentState};
use super::tasks::{
    ListenerContext, run_gossip_shard, run_health_monitor, run_gc_task, run_listener_task, new_listener,
};

impl GossipAgent {
    /// Binds the TCP listener(s) and launches background loops.
    pub async fn start(&self) -> Result<(), GossipError> {
        self.config.validate()?;
        let bind_ip: IpAddr = self.config.bind_address.parse().map_err(|e| {
            GossipError::Config(format!("bind_address '{}': {}", self.config.bind_address, e))
        })?;
        let bind_addr = SocketAddr::new(bind_ip, self.config.bind_port);
        if self.node_id.to_socket_addr() != bind_addr {
            return Err(GossipError::Config(format!(
                "node_id '{}' does not match bind address '{}'",
                self.node_id, bind_addr
            )));
        }

        match self.state.compare_exchange(
            AgentState::Idle as u8, AgentState::Running as u8,
            Ordering::AcqRel, Ordering::Acquire,
        ) {
            Ok(_) => {}
            Err(v) if v == AgentState::Running as u8 => {
                return Err(GossipError::State("Agent already running".into()));
            }
            Err(_) => {
                return Err(GossipError::State(
                    "Agent has been shut down and cannot be restarted".into(),
                ));
            }
        }
        self.rehydrate_boundary_from_kv();
        self.warm_quorum_from_layer1();
        self.advertise_locality();
        self.start_listener(bind_addr).await.inspect_err(|_| {
            self.state.store(AgentState::Idle as u8, Ordering::Release);
        })?;
        self.start_gossip_loop();
        self.start_health_monitor();
        self.start_gc_task();
        info!("Gossip agent started: {}", self.node_id);
        Ok(())
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
            node_id:         self.node_id.clone(),
            peers:           self.peers.clone(),
            gossip_txs:      self.task_ctx.gossip_txs.clone(),
            seen:            self.task_ctx.seen.clone(),
            shutdown:        self.shutdown_tx.clone(),
            max_ttl:         self.config.default_ttl,
            current_ts:      self.task_ctx.current_ts.clone(),
            peer_writers:    self.peer_writers.clone(),
            writer_depth:    self.config.writer_channel_depth,
            backoff:         Duration::from_secs(self.config.reconnect_backoff_secs),
            n_shards:        self.task_ctx.gossip_txs.len(),
            intern_keys:     self.config.intern_keys,
            intern_max_keys: self.config.intern_max_keys,
            signal_boundary: self.task_ctx.signal_boundary.clone(),
            signal_handlers: self.task_ctx.signal_handlers.clone(),
            max_peers:           self.config.max_peers,
            writer_idle_timeout: Duration::from_secs(self.config.writer_idle_timeout_secs),
            kv_state:            self.kv_state.clone(),
        };
        let lctx = ListenerContext {
            conn,
            conn_sem:       Arc::new(Semaphore::new(self.config.max_connections)),
            listener_alive: self.listener_alive.clone(),
            max_conn:       self.config.max_connections,
            addr,
            tcp_backlog:    self.config.tcp_accept_backlog,
        };

        let mut handles = self.task_handles_lock();
        for listener in listeners {
            handles.spawn(run_listener_task(listener, lctx.clone()));
        }
        Ok(())
    }

    fn start_gossip_loop(&self) {
        let bootstrap_peers        = self.bootstrap_peers.clone();
        let peer_writers           = self.peer_writers.clone();
        let shutdown_tx            = self.shutdown_tx.clone();
        let writer_depth           = self.config.writer_channel_depth;
        let backoff                = Duration::from_secs(self.config.reconnect_backoff_secs);
        let idle_timeout           = Duration::from_secs(self.config.writer_idle_timeout_secs);
        let group_aware_forwarding = self.config.group_aware_forwarding;
        let epidemic_extra_peers   = self.config.epidemic_extra_peers;
        let prefix_index           = self.kv_state.prefix_index.clone();
        let grp_generation         = self.kv_state.grp_generation.clone();
        let peer_localities        = self.kv_state.peer_localities.clone();
        let self_locality          = if self.config.locality_path.is_empty() {
            None
        } else {
            Some(crate::locality::LocalityPath::new(self.config.locality_path.iter().cloned()))
        };

        let rxs = self.gossip_rxs
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
            .expect("gossip loop started twice");
        for (shard_idx, gossip_rx) in rxs.into_iter().enumerate() {
            self.spawn_task(run_gossip_shard(
                shard_idx,
                gossip_rx,
                bootstrap_peers.clone(),
                peer_writers.clone(),
                shutdown_tx.clone(),
                self.peer_list_tx.subscribe(),
                self.shard_alive[shard_idx].clone(),
                writer_depth,
                backoff,
                idle_timeout,
                self.config.max_forwarding_peers,
                self.kv_state.dropped_frames.clone(),
                group_aware_forwarding,
                epidemic_extra_peers,
                prefix_index.clone(),
                grp_generation.clone(),
                self_locality.clone(),
                peer_localities.clone(),
            ));
        }
    }

    fn start_health_monitor(&self) {
        self.spawn_task(run_health_monitor(
            self.node_id.clone(),
            self.bootstrap_peers.clone(),
            self.peers.clone(),
            self.peer_writers.clone(),
            self.peer_list_tx.clone(),
            self.shutdown_tx.clone(),
            self.task_ctx.current_ts.clone(),
            self.config.health_check_interval_secs,
            self.config.writer_channel_depth,
            Duration::from_secs(self.config.reconnect_backoff_secs),
            Duration::from_secs(self.config.writer_idle_timeout_secs),
            self.config.peer_eviction_intervals,
            self.health_monitor_alive.clone(),
            self.config.ping_peer_sample_size,
            self.config.health_check_max_jitter_ms,
            self.kv_state.hash_acc.clone(),
            self.kv_state.dropped_frames.clone(),
            self.task_ctx.signal_handlers.clone(),
            self.config.signal_window_secs,
        ));
    }

    fn start_gc_task(&self) {
        self.spawn_task(run_gc_task(
            self.kv_state.clone(),
            self.shutdown_tx.clone(),
            self.config.health_check_interval_secs,
            self.config.default_ttl,
            self.config.propagation_window_secs,
            self.live_entries.clone(),
            self.task_ctx.seen.clone(),
            self.config.max_seen_entries,
            self.gc_alive.clone(),
            self.peer_writers.clone(),
            self.config.intern_max_keys,
            self.task_ctx.signal_boundary.clone(),
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
        for (key, bytes) in self.scan_prefix(prefix) {
            let Some(tail) = key.strip_prefix(prefix) else { continue };
            // Key format: sys/quorum/{kind}/{sender_id}
            // rfind('/') splits on the last segment, which is always the sender_id.
            let Some(slash) = tail.rfind('/') else { continue };
            let kind: std::sync::Arc<str> = std::sync::Arc::from(&tail[..slash]);
            let sender_str = &tail[slash + 1..];
            let Ok(sender) = sender_str.parse::<NodeId>() else { continue };
            if bytes.len() < 8 { continue; }
            let written_at_ms = u64::from_le_bytes(bytes[..8].try_into().unwrap());
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
        let _ = self.set(key, loc.encode());
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
            let _ = self.delete(key);
        }
        // Retract our locality advertisement so peers stop using a stale entry
        // for topology-aware fan-out and quorum diversity.
        if !self.config.locality_path.is_empty() {
            let _ = self.delete(format!("cap/{}/locality/self", self.node_id));
        }
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
