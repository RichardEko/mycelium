use crate::error::GossipError;
use crate::signal::parse_own_grp_key;
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
    task::JoinSet,
    time,
};
use tracing::{info, warn};

use crate::connection::ConnContext;
use super::{GossipAgent, STATE_IDLE, STATE_RUNNING, STATE_STOPPED};
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
            STATE_IDLE, STATE_RUNNING,
            Ordering::AcqRel, Ordering::Acquire,
        ) {
            Ok(_) => {}
            Err(STATE_RUNNING) => {
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
        self.start_listener(bind_addr).await.inspect_err(|_| {
            self.state.store(STATE_IDLE, Ordering::Release);
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
        };

        let mut new_handles = Vec::with_capacity(n_listeners);
        for listener in listeners {
            new_handles.push(tokio::spawn(run_listener_task(listener, lctx.clone())));
        }
        self.task_handles.lock().unwrap_or_else(|e| e.into_inner()).extend(new_handles);
        Ok(())
    }

    fn start_gossip_loop(&self) {
        let bootstrap_peers       = self.bootstrap_peers.clone();
        let peer_writers          = self.peer_writers.clone();
        let shutdown_tx           = self.shutdown_tx.clone();
        let writer_depth          = self.config.writer_channel_depth;
        let backoff               = Duration::from_secs(self.config.reconnect_backoff_secs);
        let idle_timeout          = Duration::from_secs(self.config.writer_idle_timeout_secs);
        let group_aware_forwarding = self.config.group_aware_forwarding;
        let prefix_index          = self.kv_state.prefix_index.clone();

        let rxs = self.gossip_rxs
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
            .expect("gossip loop started twice");
        for (shard_idx, gossip_rx) in rxs.into_iter().enumerate() {
            let handle = tokio::spawn(run_gossip_shard(
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
                prefix_index.clone(),
            ));
            self.task_handles.lock().unwrap_or_else(|e| e.into_inner()).push(handle);
        }
    }

    fn start_health_monitor(&self) {
        let handle = tokio::spawn(run_health_monitor(
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
        self.task_handles.lock().unwrap_or_else(|e| e.into_inner()).push(handle);
    }

    fn start_gc_task(&self) {
        let handle = tokio::spawn(run_gc_task(
            self.kv_state.store.clone(),
            self.kv_state.subscriptions.clone(),
            self.shutdown_tx.clone(),
            self.config.health_check_interval_secs,
            self.config.default_ttl,
            self.config.propagation_window_secs,
            self.live_entries.clone(),
            self.task_ctx.seen.clone(),
            self.config.max_seen_entries,
            self.gc_alive.clone(),
            self.task_ctx.signal_handlers.clone(),
            self.peer_writers.clone(),
            self.config.intern_max_keys,
            self.task_ctx.signal_boundary.clone(),
            self.node_id.clone(),
            self.config.signal_window_secs,
        ));
        self.task_handles.lock().unwrap_or_else(|e| e.into_inner()).push(handle);
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
        let mut to_insert: Vec<Arc<str>> = Vec::new();
        let mut to_remove: Vec<Arc<str>> = Vec::new();
        {
            let guard = self.kv_state.store.pin();
            for (key, entry) in guard.iter() {
                let Some(group) = parse_own_grp_key(key, &node_id_str) else { continue };
                if entry.data.is_some() {
                    to_insert.push(Arc::from(group));
                } else {
                    to_remove.push(Arc::from(group));
                }
            }
        }
        let mut bnd = self.task_ctx.signal_boundary.write();
        for g in to_insert { bnd.groups.insert(g); }
        for g in &to_remove { bnd.groups.remove(g.as_ref()); }
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
        let _ = self.state.compare_exchange(
            STATE_RUNNING, STATE_STOPPED,
            Ordering::AcqRel, Ordering::Acquire,
        );
        let _ = self.shutdown_tx.send(true);
        let mut set: JoinSet<()> = JoinSet::new();
        for h in self.task_handles.lock().unwrap_or_else(|e| e.into_inner()).drain(..) {
            set.spawn(async move { let _ = h.await; });
        }
        // Signal all peer writer tasks to exit. They run as detached tasks and exit
        // via peer_shutdown or the global shutdown signal already sent above.
        {
            let guard = self.peer_writers.pin();
            let keys: Vec<crate::node_id::NodeId> = guard.iter()
                .map(|(k, v)| { let _ = v.peer_shutdown.send(true); k.clone() })
                .collect();
            for key in keys { guard.remove(&key); }
        }
        let drain = async { while set.join_next().await.is_some() {} };
        if time::timeout(timeout, drain).await.is_err() {
            let remaining = set.len();
            warn!("shutdown: {} task(s) did not exit within {:?}; abandoning", remaining, timeout);
            set.abort_all();
        }
    }

    /// Signals all background tasks to stop and waits for them to exit.
    /// Uses a 5-second timeout; tasks that do not exit are aborted and logged.
    pub async fn shutdown(&self) {
        self.shutdown_with_timeout(Duration::from_secs(5)).await;
    }
}
