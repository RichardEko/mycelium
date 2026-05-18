use crate::config::GossipConfig;
use crate::connection::{handle_connection, ConnContext};
use crate::error::GossipError;
use crate::framing::{
    bincode_cfg, shard_for_key, ForwardHint, GossipUpdate, WireMessage,
};
use crate::node_id::NodeId;
use crate::seen::ShardedSeen;
use crate::signal::{
    decode_load_state, encode_load_state, kv_ns, AdvertiseHandle, Boundary,
    LoadState, OpacityHandle, OpacityHint, OpacityState, Signal, SignalHandlers,
    SignalScope, WatchHandle,
};
use crate::store::{apply_and_notify, intern_pool_len, StoreEntry};
use crate::consensus::{
    consensus_kind, ConsensusConfig, ConsensusEngine,
    ConsensusHandle, ConsensusResult, consensus_ns,
};
use crate::writer::{evict_peer_writer, get_or_spawn_writer, request_state, WriterEntry};
use ahash::{AHashMap, AHashSet};
use bytes::{BufMut, Bytes, BytesMut};
use dashmap::DashMap;
use parking_lot::RwLock;
use std::{
    net::{IpAddr, SocketAddr},
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicU8, AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::{
    net::TcpListener,
    sync::{mpsc, mpsc::error::TrySendError, watch, Semaphore},
    task::{JoinHandle, JoinSet},
    time,
};
use tracing::{debug, error, info, warn};

#[rustfmt::skip]
const STATE_IDLE:    u8 = 0;
#[rustfmt::skip]
const STATE_RUNNING: u8 = 1;
#[rustfmt::skip]
const STATE_STOPPED: u8 = 2;

/// Number of random non-member peers added to Group-scoped signal fan-out
/// for epidemic coverage even when `group_aware_forwarding = true`.
const EPIDEMIC_K: usize = 3;

/// Snapshot of live protocol state.
#[derive(Debug)]
pub struct SystemStats {
    pub peers: usize,
    pub store_entries: usize,
    pub cached_connections: usize,
    /// Pending message count per gossip shard (index matches shard number).
    pub gossip_shard_queue_depths: Vec<usize>,
    /// Number of gossip shards that have crashed while the agent is running.
    pub dead_shards: usize,
    /// `true` when the agent is not running (pre-`start` or post-`shutdown`),
    /// or when the GC task is alive. `false` only when the agent is running and
    /// the GC task has exited unexpectedly; tombstone expiry and subscription
    /// cleanup will have stopped.
    pub gc_alive: bool,
    /// `true` when the agent is not running (pre-`start` or post-`shutdown`),
    /// or when the health monitor task is alive. `false` only when the agent is
    /// running and the health monitor has exited unexpectedly; peer pings and
    /// peer eviction will have stopped.
    pub health_monitor_alive: bool,
    /// Number of entries in the process-wide key intern pool.
    /// Grows with distinct keys ever observed; never trimmed.
    /// Zero when `intern_keys = false` and no key has been interned.
    pub intern_pool_size: usize,
    /// Cumulative gossip frames dropped since agent creation due to full channels.
    ///
    /// Incremented by `set`, `delete`, `emit`, and internal forwarding whenever
    /// `try_send` returns `Err(Full)`. A non-zero value means the agent lost
    /// writes — raise `writer_channel_depth` or `gossip_channel_capacity`.
    pub dropped_frames: u64,
}

/// Core gossip agent.
///
/// All fields are private. Use the public methods to interact with the agent.
///
/// ## Interface patterns
///
/// ### Direct methods
/// Most methods (`set`, `get`, `emit`, `subscribe`, …) are synchronous or
/// `async fn`. They complete in the caller's task and return their result immediately.
///
/// ### Task helpers
/// A smaller subset of methods (`advertise`, `signal_once`, `watch`,
/// `manage_opacity`, `manage_opacity_gated`) spawn a background tokio task and return
/// a typed *handle* ([`AdvertiseHandle`](crate::signal::AdvertiseHandle),
/// [`WatchHandle`](crate::signal::WatchHandle),
/// [`OpacityHandle`](crate::signal::OpacityHandle), …). Dropping the handle cancels
/// the task; keeping it alive keeps the task running. These are for standing,
/// event-driven behaviours — periodic beacons, adaptive opacity controllers, reacting
/// to incoming signals — that must outlive a single `await` call without blocking the
/// caller.
///
/// All task-helper tasks exit automatically when [`shutdown`](Self::shutdown) is
/// called, even if the handle is still live.
pub struct GossipAgent {
    node_id: NodeId,
    config: GossipConfig,
    store: Arc<papaya::HashMap<Arc<str>, StoreEntry>>,
    peers: Arc<papaya::HashMap<NodeId, Instant>>,
    peer_list_tx: watch::Sender<Arc<[NodeId]>>,
    bootstrap_peers: Arc<[NodeId]>,
    /// Pre-encoded frame bytes + sender id_hash + forwarding hint; shards fan out without re-encoding.
    gossip_txs: Arc<[mpsc::Sender<(Bytes, u64, ForwardHint)>]>,
    #[allow(clippy::type_complexity)]
    gossip_rxs: std::sync::Mutex<Option<Vec<mpsc::Receiver<(Bytes, u64, ForwardHint)>>>>,
    seen: Arc<ShardedSeen>,
    current_ts: Arc<AtomicU64>,
    peer_writers: Arc<DashMap<NodeId, WriterEntry>>,
    subscriptions: Arc<papaya::HashMap<Arc<str>, watch::Sender<Option<Bytes>>>>,
    /// Cached count of live (non-tombstone) store entries. Updated by the GC task;
    /// up to one GC interval stale but O(1) to read via system_stats().
    live_entries: Arc<AtomicUsize>,
    state: AtomicU8,
    shutdown_tx: Arc<watch::Sender<bool>>,
    shard_alive: Vec<Arc<AtomicBool>>,
    /// Counts live listener tasks; error is logged when this reaches zero unexpectedly.
    listener_alive: Arc<AtomicUsize>,
    health_monitor_alive: Arc<AtomicBool>,
    gc_alive: Arc<AtomicBool>,
    task_handles: std::sync::Mutex<Vec<JoinHandle<()>>>,
    /// Local boundary filter — which scopes this node acts on.
    signal_boundary: Arc<RwLock<Boundary>>,
    /// Fan-out registry for local signal delivery.
    signal_handlers: Arc<SignalHandlers>,
    /// Cumulative count of gossip frames silently dropped due to full channels.
    dropped_frames: Arc<AtomicU64>,
}

impl GossipAgent {
    // ── Lifecycle ─────────────────────────────────────────────────────────────

    /// Creates a new agent. Call [`start`](Self::start) to begin listening.
    pub fn new(node_id: NodeId, mut config: GossipConfig) -> Self {
        let cap = config.gossip_channel_capacity;
        let n_shards = config.gossip_shards.next_power_of_two();
        // Write the normalised value back so config reflects the actual shard count.
        config.gossip_shards = n_shards;
        let mut gossip_txs_vec: Vec<mpsc::Sender<(Bytes, u64, ForwardHint)>> = Vec::with_capacity(n_shards);
        let mut gossip_rxs_inner: Vec<mpsc::Receiver<(Bytes, u64, ForwardHint)>> = Vec::with_capacity(n_shards);
        for _ in 0..n_shards {
            let (tx, rx) = mpsc::channel::<(Bytes, u64, ForwardHint)>(cap);
            gossip_txs_vec.push(tx);
            gossip_rxs_inner.push(rx);
        }
        let (shutdown_tx, _) = watch::channel(false);
        let mut bootstrap_peers = config.bootstrap_peers.clone();
        bootstrap_peers.retain(|p| p != &node_id);
        let bootstrap_peers: Arc<[NodeId]> = bootstrap_peers.into();
        let (peer_list_tx, _) = watch::channel(bootstrap_peers.clone());
        let init_ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let shard_alive = (0..n_shards)
            .map(|_| Arc::new(AtomicBool::new(false)))
            .collect();
        let seen_shards = n_shards.max(16); // at least 16 seen shards for good CAS distribution

        Self {
            node_id: node_id.clone(),
            config,
            store: Arc::new(papaya::HashMap::new()),
            peers: Arc::new(papaya::HashMap::new()),
            bootstrap_peers,
            peer_list_tx,
            gossip_txs: gossip_txs_vec.into(),
            gossip_rxs: std::sync::Mutex::new(Some(gossip_rxs_inner)),
            seen: Arc::new(ShardedSeen::new(seen_shards)),
            current_ts: Arc::new(AtomicU64::new(init_ts)),
            peer_writers: Arc::new(DashMap::new()),
            subscriptions: Arc::new(papaya::HashMap::new()),
            live_entries: Arc::new(AtomicUsize::new(0)),
            state: AtomicU8::new(STATE_IDLE),
            shutdown_tx: Arc::new(shutdown_tx),
            shard_alive,
            listener_alive: Arc::new(AtomicUsize::new(0)),
            health_monitor_alive: Arc::new(AtomicBool::new(false)),
            gc_alive: Arc::new(AtomicBool::new(false)),
            task_handles: std::sync::Mutex::new(Vec::new()),
            signal_boundary: Arc::new(RwLock::new(Boundary::new(node_id))),
            signal_handlers: Arc::new(SignalHandlers::new()),
            dropped_frames: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Binds the TCP listener(s) and launches background loops.
    pub async fn start(&self) -> Result<(), GossipError> {
        self.config.validate()?;
        // Parse the bind address structurally so IPv6 addresses (e.g. "::1") are
        // handled correctly. String formatting ("::1:8080") produces an ambiguous
        // form that SocketAddr::from_str rejects.
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
        self.start_listener(bind_addr).await.inspect_err(|_| {
            self.state.store(STATE_IDLE, Ordering::Release);
        })?;
        self.start_gossip_loop();
        self.start_health_monitor();
        self.start_gc_task();
        self.rehydrate_boundary_from_kv();
        info!("Gossip agent started: {}", self.node_id);
        Ok(())
    }

    async fn start_listener(&self, addr: SocketAddr) -> Result<(), GossipError> {
        // On unix, bind min(gossip_shards, 4) SO_REUSEPORT sockets so the kernel
        // load-balances accepted connections across tasks — removes the single-accept-loop
        // bottleneck at high connection rates.
        #[cfg(unix)]
        let n_listeners = self.gossip_txs.len().clamp(1, 4);
        #[cfg(not(unix))]
        let n_listeners: usize = 1;

        info!("Listening on {} ({} accept task{})",
            addr, n_listeners, if n_listeners == 1 { "" } else { "s" });

        // Phase 1: bind all sockets before spawning any tasks.
        // If any bind fails, no tasks have been spawned, so start() can be safely
        // retried without leaving orphaned listener tasks behind.
        let mut listeners: Vec<tokio::net::TcpListener> = Vec::with_capacity(n_listeners);
        for _ in 0..n_listeners {
            listeners.push(new_listener(addr, self.config.tcp_accept_backlog).await?);
        }

        // Phase 2: all binds succeeded — build the shared context and spawn one task per listener.
        let lctx = ListenerContext {
            node_id:         self.node_id.clone(),
            store:           self.store.clone(),
            peers:           self.peers.clone(),
            gossip_txs:      self.gossip_txs.clone(),
            seen:            self.seen.clone(),
            shutdown_tx:     self.shutdown_tx.clone(),
            subscriptions:   self.subscriptions.clone(),
            current_ts:      self.current_ts.clone(),
            peer_writers:    self.peer_writers.clone(),
            conn_sem:        Arc::new(Semaphore::new(self.config.max_connections)),
            listener_alive:  self.listener_alive.clone(),
            max_conn:        self.config.max_connections,
            max_ttl:         self.config.default_ttl,
            writer_depth:    self.config.writer_channel_depth,
            backoff:         Duration::from_secs(self.config.reconnect_backoff_secs),
            n_shards:        self.gossip_txs.len(),
            intern_keys:     self.config.intern_keys,
            intern_max_keys: self.config.intern_max_keys,
            signal_boundary: self.signal_boundary.clone(),
            signal_handlers: self.signal_handlers.clone(),
            max_peers:       self.config.max_peers,
            writer_idle_timeout: Duration::from_secs(self.config.writer_idle_timeout_secs),
        };

        let mut new_handles: Vec<JoinHandle<()>> = Vec::with_capacity(n_listeners);
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
        let store                 = self.store.clone();
        let group_aware_forwarding = self.config.group_aware_forwarding;

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
                self.dropped_frames.clone(),
                store.clone(),
                group_aware_forwarding,
            ));
            self.task_handles.lock().unwrap_or_else(|e| e.into_inner()).push(handle);
        }
    }

    fn start_health_monitor(&self) {
        let node_id              = self.node_id.clone();
        let bootstrap_peers      = self.bootstrap_peers.clone();
        let peers                = self.peers.clone();
        let peer_writers         = self.peer_writers.clone();
        let store                = self.store.clone();
        let peer_list_tx         = self.peer_list_tx.clone();
        let shutdown_tx          = self.shutdown_tx.clone();
        let current_ts           = self.current_ts.clone();
        let interval_secs        = self.config.health_check_interval_secs;
        let writer_depth         = self.config.writer_channel_depth;
        let backoff              = Duration::from_secs(self.config.reconnect_backoff_secs);
        let idle_timeout         = Duration::from_secs(self.config.writer_idle_timeout_secs);
        let peer_eviction_intervals = self.config.peer_eviction_intervals;
        let health_monitor_alive = self.health_monitor_alive.clone();
        let ping_peer_sample_size    = self.config.ping_peer_sample_size;
        let health_check_max_jitter  = self.config.health_check_max_jitter_ms;

        let bnd_handle = tokio::spawn(run_boundary_reconcile(
            self.node_id.clone(),
            self.store.clone(),
            self.signal_boundary.clone(),
            self.shutdown_tx.clone(),
            interval_secs,
        ));
        self.task_handles.lock().unwrap_or_else(|e| e.into_inner()).push(bnd_handle);

        let handle = tokio::spawn(run_health_monitor(
            node_id,
            bootstrap_peers,
            peers,
            peer_writers,
            store,
            peer_list_tx,
            shutdown_tx,
            current_ts,
            interval_secs,
            writer_depth,
            backoff,
            idle_timeout,
            peer_eviction_intervals,
            health_monitor_alive,
            ping_peer_sample_size,
            health_check_max_jitter,
        ));
        self.task_handles.lock().unwrap_or_else(|e| e.into_inner()).push(handle);
    }

    fn start_gc_task(&self) {
        let store             = self.store.clone();
        let subscriptions     = self.subscriptions.clone();
        let shutdown_tx       = self.shutdown_tx.clone();
        let interval_secs     = self.config.health_check_interval_secs;
        let default_ttl       = self.config.default_ttl;
        let propagation       = self.config.propagation_window_secs;
        let live_entries      = self.live_entries.clone();
        let seen              = self.seen.clone();
        let max_seen_entries  = self.config.max_seen_entries;
        let peer_writers      = self.peer_writers.clone();
        let gc_alive          = self.gc_alive.clone();
        let signal_handlers   = self.signal_handlers.clone();
        let intern_max_keys   = self.config.intern_max_keys;

        let handle = tokio::spawn(run_gc_task(
            store, subscriptions, shutdown_tx,
            interval_secs, default_ttl, propagation, live_entries,
            seen, max_seen_entries, gc_alive, signal_handlers, peer_writers,
            intern_max_keys,
        ));
        self.task_handles.lock().unwrap_or_else(|e| e.into_inner()).push(handle);
    }

    pub(crate) fn rehydrate_boundary_from_kv(&self) {
        let suffix = format!("/{}", self.node_id);
        let mut to_insert: Vec<Arc<str>> = Vec::new();
        let mut to_remove: Vec<Arc<str>> = Vec::new();
        {
            let guard = self.store.pin();
            for (key, entry) in guard.iter() {
                if !key.starts_with("grp/") || !key.ends_with(suffix.as_str()) { continue; }
                let Some(tail) = key.strip_prefix("grp/") else { continue };
                let Some(group) = tail.strip_suffix(suffix.as_str()) else { continue };
                if entry.data.is_some() {
                    to_insert.push(Arc::from(group));
                } else {
                    to_remove.push(Arc::from(group));
                }
            }
        }
        let mut bnd = self.signal_boundary.write();
        for g in to_insert { bnd.groups.insert(g); }
        for g in &to_remove { bnd.groups.remove(g.as_ref()); }
    }

    // ── Node state ────────────────────────────────────────────────────────────

    /// Returns this node's identifier.
    pub fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    /// Returns a snapshot of all currently live peer `NodeId`s.
    ///
    /// Useful at Layer 3 when a direct connection (e.g. HTTP) must be opened to
    /// a specific peer. The list reflects the peers table at the moment of the call;
    /// it may be stale by the time it is acted on — treat it as advisory.
    pub fn peers(&self) -> Vec<NodeId> {
        self.peers.pin().iter().map(|(k, _)| k.clone()).collect()
    }

    /// Returns the groups this node has currently joined.
    ///
    /// Reflects the local [`Boundary`] state at the moment of the call. Useful for
    /// diagnostics and Layer 3 routing decisions that depend on group membership.
    pub fn groups(&self) -> Vec<Arc<str>> {
        self.signal_boundary.read().groups.iter().cloned().collect()
    }

    // ── KV ────────────────────────────────────────────────────────────────────

    /// Stores `value` under `key` locally and queues it for gossip to peers.
    ///
    /// `key` accepts `&str`, `Arc<str>`, `String`, or anything that converts to
    /// `Arc<str>`. Callers with a hot key set can pre-intern keys as `Arc<str>`
    /// and pass them here to avoid a heap allocation on every write.
    ///
    /// **Each agent should write only its own keys.** Writing all keys from a single
    /// agent floods that agent's peer-writer channels: with N keys and channel depth D,
    /// writes are silently dropped when N > D. Distribute writes across agents so each
    /// agent writes exactly its own key — this produces 1 message per peer-writer
    /// regardless of cluster size.
    ///
    /// The local store is **always** updated regardless of the return value.
    /// Returns `true` if the update was queued for gossip; `false` if the gossip
    /// channel was full (backpressure) or the shard has died — the update was
    /// applied locally but will not propagate to peers.
    #[must_use]
    pub fn set<K: Into<Arc<str>>>(&self, key: K, value: impl Into<Bytes>) -> bool {
        let key: Arc<str> = key.into();
        let update = self.make_update(key, value.into(), false);
        apply_and_notify(&self.store, &self.subscriptions, &update);
        self.dispatch_update(update)
    }

    /// Returns the current value for `key`, or `None` if absent or tombstoned.
    pub fn get(&self, key: &str) -> Option<Bytes> {
        self.store.pin().get(key).and_then(|e| e.data.clone())
    }

    /// Removes `key` locally and queues a tombstone for gossip to peers.
    ///
    /// The local store is **always** updated regardless of the return value.
    /// Returns `true` if the tombstone was queued for gossip; `false` if the gossip
    /// channel was full (backpressure) or the shard has died — the tombstone was
    /// applied locally but will not propagate to peers.
    #[must_use]
    pub fn delete<K: Into<Arc<str>>>(&self, key: K) -> bool {
        let key: Arc<str> = key.into();
        let update = self.make_update(key, Bytes::new(), true);
        apply_and_notify(&self.store, &self.subscriptions, &update);
        self.dispatch_update(update)
    }

    /// Like [`set`](Self::set), but awaits channel capacity instead of dropping
    /// the frame when the shard channel is full.
    ///
    /// The local store is **always** updated regardless of the return value.
    /// Returns `false` only if the shard task has crashed — the update was applied
    /// locally but will not propagate to peers. Suitable for callers that must not
    /// lose writes under backpressure.
    #[must_use]
    pub async fn set_async<K: Into<Arc<str>>>(&self, key: K, value: impl Into<Bytes>) -> bool {
        let key: Arc<str> = key.into();
        let update = self.make_update(key, value.into(), false);
        apply_and_notify(&self.store, &self.subscriptions, &update);
        self.dispatch_update_async(update).await
    }

    /// Like [`delete`](Self::delete), but awaits channel capacity instead of dropping
    /// the frame when the shard channel is full.
    ///
    /// The local store is **always** updated regardless of the return value.
    /// Returns `false` only if the shard task has crashed — the tombstone was applied
    /// locally but will not propagate to peers. Suitable for callers that must not
    /// lose tombstones under backpressure.
    #[must_use]
    pub async fn delete_async<K: Into<Arc<str>>>(&self, key: K) -> bool {
        let key: Arc<str> = key.into();
        let update = self.make_update(key, Bytes::new(), true);
        apply_and_notify(&self.store, &self.subscriptions, &update);
        self.dispatch_update_async(update).await
    }

    /// Returns a snapshot of all keys that have a live (non-tombstone) value.
    ///
    /// Keys are returned as `Arc<str>` — clone is O(1). Callers that need `String`
    /// can call `.to_string()` on each element.
    pub fn keys(&self) -> Vec<Arc<str>> {
        let guard = self.store.pin();
        guard.iter()
            .filter(|(_, v)| v.data.is_some())
            .map(|(k, _)| k.clone())
            .collect()
    }

    /// Returns all live (non-tombstone) key-value pairs whose key starts with `prefix`,
    /// in a single store pass.
    ///
    /// More efficient than `keys()` + `get()` per key when reading prefix-namespaced
    /// data such as pheromone trails or group rosters:
    ///
    /// ```ignore
    /// use gossip_protocol::kv_ns;
    /// let trails = agent.scan_prefix(kv_ns::LOAD);
    /// for (key, bytes) in trails {
    ///     // decode bytes into LoadState, check written_at_ms for evaporation
    /// }
    /// ```
    pub fn scan_prefix(&self, prefix: &str) -> Vec<(Arc<str>, Bytes)> {
        let guard = self.store.pin();
        guard.iter()
            .filter(|(k, v)| v.data.is_some() && k.starts_with(prefix))
            .map(|(k, v)| (k.clone(), v.data.clone().unwrap()))
            .collect()
    }

    /// Subscribes to changes for `key`.
    ///
    /// `key` accepts `&str`, `Arc<str>`, or anything that converts to `Arc<str>`.
    ///
    /// The receiver's initial value is a snapshot of the store at subscription time.
    /// A concurrent `set` or `delete` between the store read and the channel CAS may
    /// produce a stale initial value; the next write to that key will deliver the
    /// correct value.
    #[must_use]
    pub fn subscribe<K: Into<Arc<str>>>(&self, key: K) -> watch::Receiver<Option<Bytes>> {
        let key_arc: Arc<str> = key.into();
        loop {
            let guard = self.subscriptions.pin();
            if let Some(tx) = guard.get(&key_arc) {
                if !tx.is_closed() {
                    return tx.subscribe();
                }
            }
            let current = self.store.pin().get(&*key_arc).and_then(|e| e.data.clone());
            let (new_tx, rx) = watch::channel(current);
            let mut slot = Some(new_tx);
            let result = guard.compute(key_arc.clone(), |existing| match existing {
                Some((_, tx)) if !tx.is_closed() => papaya::Operation::Abort(()),
                _ => match slot.take() {
                    Some(tx) => papaya::Operation::Insert(tx),
                    None => papaya::Operation::Abort(()),
                },
            });
            if matches!(result, papaya::Compute::Inserted(..) | papaya::Compute::Updated { .. }) {
                return rx;
            }
            // A concurrent subscriber won the CAS; loop to borrow its sender.
        }
    }

    /// Returns all pheromone trail entries not older than `max_age`.
    ///
    /// Reads [`kv_ns::LOAD`] from Layer I. Each entry was written by a peer's
    /// [`manage_opacity`](Self::manage_opacity) governor when it crossed the
    /// opacity threshold. An absent entry means the peer is transparent (not
    /// overloaded) for that kind.
    ///
    /// Returns `(node_id_str, kind_str, LoadState)` triples sorted by
    /// `fill_ratio` descending (most-loaded first). Entries whose
    /// `written_at_ms` is older than `max_age` are excluded (evaporation).
    pub fn peer_load(&self, max_age: Duration) -> Vec<(Arc<str>, Arc<str>, LoadState)> {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH).unwrap_or_default()
            .as_millis() as u64;
        let max_age_ms = max_age.as_millis() as u64;
        let mut results: Vec<(Arc<str>, Arc<str>, LoadState)> = self
            .scan_prefix(kv_ns::LOAD)
            .into_iter()
            .filter_map(|(key, bytes)| {
                // Key format: "load/{node_id}/{kind}"
                let tail = key.strip_prefix("load/")?;
                let slash = tail.find('/')?;
                let node_str: Arc<str> = Arc::from(&tail[..slash]);
                let kind_str: Arc<str> = Arc::from(&tail[slash + 1..]);
                let state = decode_load_state(&bytes)?;
                if now_ms.saturating_sub(state.written_at_ms) > max_age_ms {
                    return None;
                }
                Some((node_str, kind_str, state))
            })
            .collect();
        results.sort_by(|a, b| b.2.fill_ratio.partial_cmp(&a.2.fill_ratio).unwrap_or(std::cmp::Ordering::Equal));
        results
    }

    /// Returns a `watch::Receiver` that fires whenever `load/{node_id}/{kind}` changes.
    ///
    /// Unlike [`subscribe`](Self::subscribe), the receiver yields decoded [`LoadState`]
    /// values instead of raw bytes — symmetric with [`peer_load`](Self::peer_load).
    /// Fires once on registration with the current value, then on every update from
    /// anti-entropy or a peer's opacity transition. `None` means absent or tombstoned.
    ///
    /// The forwarding task exits automatically when either the underlying store channel
    /// closes (agent shutdown) or all receivers drop (caller abandoned the watch).
    #[must_use]
    pub fn peer_load_rx(&self, node_id: &NodeId, kind: &str) -> watch::Receiver<Option<LoadState>> {
        let mut raw_rx = self.subscribe(format!("load/{}/{}", node_id, kind));
        let initial = raw_rx.borrow().as_ref().and_then(decode_load_state);
        let (tx, rx) = watch::channel(initial);
        tokio::spawn(async move {
            loop {
                if raw_rx.changed().await.is_err() { break; }
                let decoded = raw_rx.borrow().as_ref().and_then(decode_load_state);
                if tx.send(decoded).is_err() { break; }
            }
        });
        rx
    }

    // ── Signal / Boundary API (Layer 2) ──────────────────────────────────────

    /// Registers a handler for signals of the given `kind`.
    ///
    /// Returns an `mpsc::Receiver<Signal>` with the default channel depth (256). Caller is
    /// responsible for spawning a task to drive it. Multiple calls for the same kind each
    /// return an independent receiver — all receive every admitted signal.
    ///
    /// **Channel sizing**: 256 suits kinds that arrive at a few Hz (health probes, contract
    /// advertisements). For kinds where N agents emit simultaneously — e.g. `INVOKE` to a
    /// group of 256 workers — use [`signal_rx_with_capacity`](Self::signal_rx_with_capacity)
    /// with `N × expected_burst` as the depth. A full channel produces a warning log and
    /// the signal is dropped without retry.
    #[must_use]
    pub fn signal_rx(&self, kind: impl Into<Arc<str>>) -> mpsc::Receiver<Signal> {
        self.signal_handlers.register(kind.into())
    }

    /// Like [`signal_rx`](Self::signal_rx) with an explicit channel depth.
    ///
    /// Use a larger capacity for high-frequency kinds (e.g. health probes from N agents)
    /// or when the handler task cannot drain immediately.
    #[must_use]
    pub fn signal_rx_with_capacity(&self, kind: impl Into<Arc<str>>, cap: usize) -> mpsc::Receiver<Signal> {
        self.signal_handlers.register_with_capacity(kind.into(), cap)
    }

    /// Emits a signal to the cluster.
    ///
    /// The signal is delivered locally first (if admitted by this node's boundary),
    /// then queued for epidemic forwarding to all peers. The same nonce is inserted
    /// into the seen-set so if the signal returns via a peer it is silently dropped.
    ///
    /// Returns `true` if the signal was queued for forwarding; `false` if the gossip
    /// channel was full or the shard has died — local delivery still occurs.
    #[must_use]
    pub fn emit(
        &self,
        kind:    impl Into<Arc<str>>,
        scope:   SignalScope,
        payload: impl Into<Bytes>,
    ) -> bool {
        emit_signal(
            &self.node_id, &self.seen, &self.current_ts,
            &self.signal_boundary, &self.signal_handlers, &self.gossip_txs,
            self.config.default_ttl, &self.dropped_frames,
            kind.into(), scope, payload.into(),
        )
    }

    /// Like [`emit`](Self::emit), but awaits channel capacity instead of dropping
    /// the frame when the shard channel is full.
    ///
    /// Local delivery always occurs regardless of the return value.
    /// Returns `false` only if the shard task has crashed — the signal was delivered
    /// locally but will not propagate to peers. Suitable for `INVOKE` / `INVOKE_RESULT`
    /// flows where dropping a frame is a correctness failure.
    #[must_use]
    pub async fn emit_async(
        &self,
        kind:    impl Into<Arc<str>>,
        scope:   SignalScope,
        payload: impl Into<Bytes>,
    ) -> bool {
        let kind:    Arc<str> = kind.into();
        let payload: Bytes    = payload.into();
        let nonce = fastrand::u64(1..);  // 0 is reserved as ANTI_ENTROPY_NONCE
        let ts = self.current_ts.load(Ordering::Relaxed);
        let _ = self.seen.is_duplicate(nonce, ts);

        if self.signal_boundary.read().admits(&scope) {
            let admit = match &scope {
                SignalScope::Individual(_) => true,
                _ => {
                    let opacity = self.signal_handlers.fill_ratio(&kind);
                    opacity == 0.0 || fastrand::f32() >= opacity
                }
            };
            if admit {
                self.signal_handlers.deliver(&Signal {
                    kind: kind.clone(), scope: scope.clone(),
                    payload: payload.clone(), sender: self.node_id.clone(), nonce,
                });
            }
        }

        let hint = match &scope {
            SignalScope::System           => ForwardHint::All,
            SignalScope::Group(name)      => ForwardHint::Group(name.clone()),
            SignalScope::Individual(peer) => ForwardHint::Individual(peer.clone()),
        };
        let shard = shard_for_key(&kind, self.gossip_txs.len());
        let sender_hash = self.node_id.id_hash();
        let mut buf = BytesMut::with_capacity(256);
        if bincode::serde::encode_into_std_write(
            WireMessage::Signal {
                ttl: self.config.default_ttl, nonce,
                sender: self.node_id.clone(), scope, kind, payload,
            },
            &mut (&mut buf).writer(),
            bincode_cfg(),
        ).is_err() {
            error!("Signal encode failed");
            return false;
        }
        match self.gossip_txs[shard].send((buf.freeze(), sender_hash, hint)).await {
            Ok(()) => true,
            Err(_) => {
                if self.state.load(Ordering::Relaxed) == STATE_RUNNING {
                    error!("Gossip shard {} is dead; signal will not propagate", shard);
                }
                false
            }
        }
    }

    /// Joins a named boundary group.
    ///
    /// The node immediately begins admitting `Group(name)` signals. Membership is
    /// published into the gossip KV store at `grp/<name>/<node_id>` so peers can
    /// observe it and subscribe to group roster changes.
    pub fn join_group(&self, group: impl Into<Arc<str>>) {
        let group: Arc<str> = group.into();
        let inserted = self.signal_boundary.write().groups.insert(group.clone());
        // Only gossip if this is a new membership — suppress redundant LWW churn.
        if inserted {
            let key = format!("grp/{}/{}", &*group, self.node_id);
            let _ = self.set(key, b"1".to_vec());
        }
    }

    /// Leaves a named boundary group.
    ///
    /// The node immediately stops admitting `Group(name)` signals. A tombstone for
    /// `grp/<name>/<node_id>` is published into the gossip store.
    pub fn leave_group(&self, group: impl Into<Arc<str>>) {
        let group: Arc<str> = group.into();
        let removed = self.signal_boundary.write().groups.remove(&group);
        // Only tombstone if we were actually a member — suppress redundant LWW churn.
        if removed {
            let key = format!("grp/{}/{}", &*group, self.node_id);
            let _ = self.delete(key);
        }
    }

    /// Awaits the first locally-admitted signal of `kind` satisfying `predicate`.
    ///
    /// Returns `None` if `timeout` elapses before a matching signal arrives.
    /// Non-matching signals are discarded; the deadline is fixed across all iterations
    /// so total wait never exceeds `timeout`.
    ///
    /// The handler channel is registered **synchronously** when this function is called
    /// (before the returned future is polled), so no reply can be missed even if
    /// `emit` is called immediately after:
    /// ```ignore
    /// let result_fut = agent.signal_once("invoke.result", Duration::from_secs(5), |s| {
    ///     s.nonce == request_nonce
    /// });
    /// agent.emit("invoke", scope, payload);  // safe — channel already registered
    /// let result = result_fut.await;
    /// ```
    pub fn signal_once<F>(
        &self,
        kind:      impl Into<Arc<str>>,
        timeout:   Duration,
        predicate: F,
    ) -> impl std::future::Future<Output = Option<Signal>>
    where
        F: Fn(&Signal) -> bool,
    {
        let mut rx = self.signal_handlers.register_with_capacity(kind.into(), 256);
        async move {
            let deadline = time::Instant::now() + timeout;
            loop {
                match time::timeout_at(deadline, rx.recv()).await {
                    Ok(Some(sig)) if predicate(&sig) => return Some(sig),
                    Ok(Some(_))                      => continue,
                    _                               => return None,
                }
            }
        }
    }

    // ── Task helpers ─────────────────────────────────────────────────────────────────
    // Methods below spawn a background tokio task and return a typed handle.
    // Dropping the handle cancels the task.  See the "Interface patterns" section in
    // the GossipAgent doc comment for a full explanation of when to prefer these over
    // direct-call methods.

    /// Periodically emits `kind` on `scope` every `interval`, calling `payload_fn`
    /// each tick to capture fresh state (e.g. current load metrics).
    ///
    /// Returns an [`AdvertiseHandle`] whose drop stops the task. The task also exits
    /// automatically when the agent shuts down.
    ///
    /// Workers call this once at startup to advertise availability:
    /// ```ignore
    /// let _handle = agent.advertise(
    ///     signal_kind::CONTRACT_AVAILABLE,
    ///     SignalScope::Group("nlp".into()),
    ///     Duration::from_secs(5),
    ///     || Bytes::new(),
    /// );
    /// ```
    pub fn advertise<F>(
        &self,
        kind:       impl Into<Arc<str>>,
        scope:      SignalScope,
        interval:   Duration,
        payload_fn: F,
    ) -> AdvertiseHandle
    where
        F: Fn() -> Bytes + Send + Sync + 'static,
    {
        let (cancel_tx, mut cancel_rx) = tokio::sync::oneshot::channel::<()>();
        let mut shutdown_rx = self.shutdown_tx.subscribe();

        let node_id         = self.node_id.clone();
        let seen            = self.seen.clone();
        let current_ts      = self.current_ts.clone();
        let signal_boundary = self.signal_boundary.clone();
        let signal_handlers = self.signal_handlers.clone();
        let gossip_txs      = self.gossip_txs.clone();
        let default_ttl     = self.config.default_ttl;
        let dropped_frames  = self.dropped_frames.clone();
        let kind: Arc<str>  = kind.into();

        let handle = tokio::spawn(async move {
            let mut ticker = time::interval(interval);
            ticker.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
            loop {
                tokio::select! { biased;
                    _ = &mut cancel_rx                   => break,
                    _ = shutdown_rx.wait_for(|v| *v)     => break,
                    _ = ticker.tick() => {
                        emit_signal(
                            &node_id, &seen, &current_ts, &signal_boundary,
                            &signal_handlers, &gossip_txs, default_ttl,
                            &dropped_frames, kind.clone(), scope.clone(), payload_fn(),
                        );
                    }
                }
            }
        });
        {
            let mut handles = self.task_handles.lock().unwrap_or_else(|e| e.into_inner());
            handles.retain(|h| !h.is_finished());
            handles.push(handle);
        }

        AdvertiseHandle { _cancel: cancel_tx }
    }

    /// Returns when this node last admitted a signal of `kind` via local delivery,
    /// or `None` if no signal of that kind has ever been delivered.
    ///
    /// Does not require a registered handler — the timestamp is recorded on every
    /// call to `deliver()` regardless of whether a handler is registered.
    /// Updated even while the kind is suppressed.
    pub fn last_signal(&self, kind: &str) -> Option<Instant> {
        self.signal_handlers.last_signal(kind)
    }

    /// Suppresses local delivery of `kind` signals for `duration`.
    ///
    /// The signal is still forwarded epidemically — propagation is unconditional.
    /// The node simply does not call registered handlers for the suppressed kind.
    /// [`last_signal`](Self::last_signal) continues to update during suppression.
    ///
    /// This is an explicit refractory period. Call it after handling a signal to
    /// prevent re-handling the same kind within a cooldown window:
    ///
    /// ```ignore
    /// while let Some(sig) = invoke_rx.recv().await {
    ///     agent.suppress(signal_kind::INVOKE, Duration::from_millis(500));
    ///     handle_invocation(sig).await;
    /// }
    /// ```
    pub fn suppress(&self, kind: impl Into<Arc<str>>, duration: Duration) {
        self.signal_handlers.suppress(kind.into(), Instant::now() + duration);
    }

    /// Lifts a suppression set by [`suppress`](Self::suppress) before it expires.
    pub fn unsuppress(&self, kind: &str) {
        self.signal_handlers.unsuppress(kind);
    }

    /// Returns `true` if `kind` is currently suppressed on this node.
    pub fn is_suppressed(&self, kind: &str) -> bool {
        self.signal_handlers.is_suppressed(kind)
    }

    /// Watches `kind` for staleness, calling `on_stale` whenever the signal has not
    /// been delivered for longer than `threshold`.
    ///
    /// Spawns a background task that checks every `threshold / 4` (minimum 100 ms).
    /// `on_stale` fires repeatedly while the kind remains silent — callers that want
    /// one-shot behaviour should drop the returned handle or call
    /// [`unsuppress`](Self::unsuppress) after responding.
    ///
    /// A kind that has never been seen counts as stale immediately. Returns a
    /// [`WatchHandle`] whose drop cancels the task; the task also exits automatically
    /// on [`shutdown`](Self::shutdown).
    ///
    /// ```ignore
    /// let _watcher = agent.watch(
    ///     signal_kind::CONTRACT_AVAILABLE,
    ///     Duration::from_secs(30),
    ///     move || { respawn_worker(); },
    /// );
    /// ```
    pub fn watch<F>(
        &self,
        kind:      impl Into<Arc<str>>,
        threshold: Duration,
        on_stale:  F,
    ) -> WatchHandle
    where
        F: Fn() + Send + 'static,
    {
        let (cancel_tx, mut cancel_rx) = tokio::sync::oneshot::channel::<()>();
        let mut shutdown_rx = self.shutdown_tx.subscribe();
        let signal_handlers = self.signal_handlers.clone();
        let kind: Arc<str>  = kind.into();
        let check_interval  = (threshold / 4).max(Duration::from_millis(100));

        let handle = tokio::spawn(async move {
            let mut ticker = time::interval(check_interval);
            ticker.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
            loop {
                tokio::select! { biased;
                    _ = &mut cancel_rx               => break,
                    _ = shutdown_rx.wait_for(|v| *v) => break,
                    _ = ticker.tick() => {
                        let stale = signal_handlers
                            .last_signal(&kind)
                            .map(|t| t.elapsed() > threshold)
                            .unwrap_or(true);
                        if stale { on_stale(); }
                    }
                }
            }
        });
        {
            let mut handles = self.task_handles.lock().unwrap_or_else(|e| e.into_inner());
            handles.retain(|h| !h.is_finished());
            handles.push(handle);
        }

        WatchHandle { _cancel: cancel_tx }
    }

    /// Returns `true` when at least `min_senders` distinct nodes have had a signal of
    /// `kind` delivered to this node within `window`.
    ///
    /// Synchronous read — no background task. Pairs well with
    /// [`advertise`](Self::advertise): peers advertise their heartbeat every N seconds;
    /// the receiver calls `quorum` to act only once K distinct peers have been heard:
    ///
    /// ```ignore
    /// if agent.quorum(signal_kind::CONTRACT_AVAILABLE, 3, Duration::from_secs(10)) {
    ///     dispatch_workload();
    /// }
    /// ```
    pub fn quorum(&self, kind: &str, min_senders: usize, window: Duration) -> bool {
        self.signal_handlers.quorum(kind, min_senders, window)
    }

    /// Like [`quorum`](Self::quorum) but only counts senders that are current members
    /// of `group` according to Layer I (`grp/{group}/`).
    ///
    /// Prevents ex-members from satisfying quorum after they call [`leave_group`](Self::leave_group).
    /// A node is considered a current member if its `grp/{group}/{node_id}` key is live
    /// (not tombstoned) in the store.
    pub fn group_quorum(
        &self,
        group: &str,
        kind: &str,
        min_senders: usize,
        window: Duration,
    ) -> bool {
        let prefix = format!("grp/{}/", group);
        let member_hashes: AHashSet<u64> = self
            .scan_prefix(&prefix)
            .into_iter()
            .filter_map(|(key, _)| {
                key.strip_prefix(&prefix)
                    .and_then(|s| s.parse::<NodeId>().ok())
                    .map(|n| n.id_hash())
            })
            .collect();
        self.signal_handlers.quorum_for_group(kind, &member_hashes, min_senders, window)
    }

    /// Starts an adaptive opacity governor for `kind`.
    ///
    /// The governor samples `kind`'s handler-channel fill ratio every 100 ms and
    /// automatically emits [`BOUNDARY_OPAQUE`](crate::signal_kind::BOUNDARY_OPAQUE) /
    /// [`BOUNDARY_TRANSPARENT`](crate::signal_kind::BOUNDARY_TRANSPARENT) on `scope`
    /// when the fill ratio crosses the adaptive threshold derived from `hint`.
    ///
    /// **Threshold adaptation** — the library clamps `hint.threshold` to `[0.4, 0.95]`
    /// and reduces it by a `trend_factor` when the channel is filling quickly, so the
    /// signal is emitted before the channel saturates rather than after.
    ///
    /// **Hysteresis** — `BOUNDARY_TRANSPARENT` is only emitted once the fill ratio
    /// drops below `effective_threshold − hint.hysteresis`, preventing oscillation at
    /// the boundary.
    ///
    /// Returns an [`OpacityHandle`] whose drop stops the governor. The task also
    /// exits automatically on [`shutdown`](Self::shutdown).
    ///
    /// ```ignore
    /// let _gov = agent.manage_opacity(
    ///     signal_kind::INVOKE,
    ///     SignalScope::System,
    ///     OpacityHint::default(),
    /// );
    /// ```
    pub fn manage_opacity(
        &self,
        kind:  impl Into<Arc<str>>,
        scope: SignalScope,
        hint:  OpacityHint,
    ) -> OpacityHandle {
        self.manage_opacity_impl(kind.into(), scope, hint, None)
    }

    /// Like [`manage_opacity`](Self::manage_opacity) but with an application gate.
    ///
    /// The gate is called with an [`OpacityState`] snapshot on every tick where the
    /// library wants to emit `BOUNDARY_OPAQUE`. Returning `false` defers emission
    /// until the next tick; the library re-asks every tick so the gate stays stateless.
    ///
    /// **Override**: if `fill_ratio == 1.0` (channel completely full) the library
    /// emits regardless of the gate's return value, so a vetoing gate cannot hold the
    /// cluster permanently uninformed about a saturated node.
    ///
    /// ```ignore
    /// let _gov = agent.manage_opacity_gated(
    ///     signal_kind::INVOKE,
    ///     SignalScope::System,
    ///     OpacityHint { threshold: 0.8, ..Default::default() },
    ///     |state| state.fill_ratio >= 0.9 || !has_inflight.load(Ordering::Relaxed),
    /// );
    /// ```
    pub fn manage_opacity_gated<F>(
        &self,
        kind:  impl Into<Arc<str>>,
        scope: SignalScope,
        hint:  OpacityHint,
        gate:  F,
    ) -> OpacityHandle
    where
        F: Fn(&OpacityState) -> bool + Send + 'static,
    {
        self.manage_opacity_impl(kind.into(), scope, hint, Some(Box::new(gate)))
    }

    #[allow(clippy::type_complexity)]
    fn manage_opacity_impl(
        &self,
        kind:  Arc<str>,
        scope: SignalScope,
        hint:  OpacityHint,
        gate:  Option<Box<dyn Fn(&OpacityState) -> bool + Send + 'static>>,
    ) -> OpacityHandle {
        let (cancel_tx, mut cancel_rx) = tokio::sync::oneshot::channel::<()>();
        let mut shutdown_rx = self.shutdown_tx.subscribe();

        let signal_handlers = self.signal_handlers.clone();
        let node_id         = self.node_id.clone();
        let seen            = self.seen.clone();
        let current_ts      = self.current_ts.clone();
        let signal_boundary = self.signal_boundary.clone();
        let gossip_txs      = self.gossip_txs.clone();
        let default_ttl     = self.config.default_ttl;
        let dropped_frames  = self.dropped_frames.clone();
        let store           = self.store.clone();
        let subscriptions   = self.subscriptions.clone();

        let clamped_threshold = hint.threshold.clamp(0.4, 0.95);

        let handle = tokio::spawn(async move {
            let mut ticker = time::interval(Duration::from_millis(100));
            ticker.set_missed_tick_behavior(time::MissedTickBehavior::Skip);

            // Seed prev_fill from current state so the first-tick trend is meaningful.
            let mut prev_fill  = signal_handlers.fill_ratio(&kind);
            let mut is_opaque  = false;

            loop {
                tokio::select! { biased;
                    _ = &mut cancel_rx               => break,
                    _ = shutdown_rx.wait_for(|v| *v) => break,
                    _ = ticker.tick() => {
                        let fill_ratio = signal_handlers.fill_ratio(&kind);
                        // trend_factor in [0, 0.4]: rising 0.2/tick reduces threshold by 40%.
                        let trend        = fill_ratio - prev_fill;
                        let trend_factor = (trend.max(0.0) * 2.0).min(0.4);
                        let eff          = clamped_threshold * (1.0 - trend_factor);
                        prev_fill        = fill_ratio;

                        let state = OpacityState {
                            fill_ratio,
                            effective_threshold: eff,
                            trend,
                            is_opaque,
                        };

                        if !is_opaque && fill_ratio >= eff {
                            let gate_ok = gate.as_ref()
                                .map(|g| g(&state))
                                .unwrap_or(true);
                            if gate_ok || fill_ratio >= 1.0 {
                                emit_signal(
                                    &node_id, &seen, &current_ts, &signal_boundary,
                                    &signal_handlers, &gossip_txs, default_ttl,
                                    &dropped_frames,
                                    Arc::from(crate::signal::signal_kind::BOUNDARY_OPAQUE),
                                    scope.clone(), hint.payload.clone(),
                                );
                                is_opaque = true;
                                // Write pheromone trail to Layer I so peers observe this
                                // node's load state via `peer_load` / anti-entropy.
                                let written_at_ms = SystemTime::now()
                                    .duration_since(UNIX_EPOCH).unwrap_or_default()
                                    .as_millis() as u64;
                                let load_key: Arc<str> =
                                    Arc::from(format!("load/{}/{}", node_id, kind).as_str());
                                let pheromone_update = GossipUpdate {
                                    nonce:        fastrand::u64(1..),
                                    sender:       node_id.id_hash(),
                                    ttl:          default_ttl,
                                    is_tombstone: false,
                                    timestamp:    written_at_ms,
                                    key:          load_key.clone(),
                                    value:        encode_load_state(&LoadState {
                                        fill_ratio,
                                        is_opaque: true,
                                        written_at_ms,
                                    }),
                                };
                                apply_and_notify(&store, &subscriptions, &pheromone_update);
                                let shard = shard_for_key(&load_key, gossip_txs.len());
                                let mut buf = BytesMut::with_capacity(64);
                                if bincode::serde::encode_into_std_write(
                                    WireMessage::Data(pheromone_update),
                                    &mut (&mut buf).writer(), bincode_cfg(),
                                ).is_ok() {
                                    let _ = gossip_txs[shard].try_send((
                                        buf.freeze(), node_id.id_hash(), ForwardHint::All,
                                    ));
                                }
                            }
                        } else if is_opaque && fill_ratio < eff - hint.hysteresis {
                            emit_signal(
                                &node_id, &seen, &current_ts, &signal_boundary,
                                &signal_handlers, &gossip_txs, default_ttl,
                                &dropped_frames,
                                Arc::from(crate::signal::signal_kind::BOUNDARY_TRANSPARENT),
                                scope.clone(), Bytes::new(),
                            );
                            is_opaque = false;
                            // Tombstone the pheromone trail — immediate evaporation on recovery.
                            let written_at_ms = SystemTime::now()
                                .duration_since(UNIX_EPOCH).unwrap_or_default()
                                .as_millis() as u64;
                            let load_key: Arc<str> =
                                Arc::from(format!("load/{}/{}", node_id, kind).as_str());
                            let tombstone_update = GossipUpdate {
                                nonce:        fastrand::u64(1..),
                                sender:       node_id.id_hash(),
                                ttl:          default_ttl,
                                is_tombstone: true,
                                timestamp:    written_at_ms,
                                key:          load_key.clone(),
                                value:        Bytes::new(),
                            };
                            apply_and_notify(&store, &subscriptions, &tombstone_update);
                            let shard = shard_for_key(&load_key, gossip_txs.len());
                            let mut buf = BytesMut::with_capacity(64);
                            if bincode::serde::encode_into_std_write(
                                WireMessage::Data(tombstone_update),
                                &mut (&mut buf).writer(), bincode_cfg(),
                            ).is_ok() {
                                let _ = gossip_txs[shard].try_send((
                                    buf.freeze(), node_id.id_hash(), ForwardHint::All,
                                ));
                            }
                        }
                    }
                }
            }
        });
        {
            let mut handles = self.task_handles.lock().unwrap_or_else(|e| e.into_inner());
            handles.retain(|h| !h.is_finished());
            handles.push(handle);
        }

        OpacityHandle { _cancel: cancel_tx }
    }

    /// Returns the current fill ratio of handler channels for `kind`.
    ///
    /// `0.0` = all channels empty (boundary fully transparent for this kind).
    /// `1.0` = at least one channel full (boundary fully opaque — signals being shed).
    /// Returns `0.0` when no handlers are registered.
    ///
    /// The value reflects the **most-loaded** registered handler. If any one handler
    /// is saturated, this returns 1.0 even if others still have capacity — consistent
    /// with the opacity shedding model where a fully saturated handler would drop signals.
    ///
    /// Workers can poll this to decide when to emit a
    /// [`BOUNDARY_OPAQUE`](crate::signal_kind::BOUNDARY_OPAQUE) transition signal,
    /// giving upstream nodes time to drain connections before the boundary closes.
    pub fn opacity(&self, kind: &str) -> f32 {
        self.signal_handlers.fill_ratio(&Arc::from(kind))
    }

    // ── Node diagnostics ─────────────────────────────────────────────────────────────

    /// Returns a snapshot of live protocol state.
    ///
    /// Note: `dead_shards` may transiently report all shards as dead in the brief
    /// window between `start()` returning and the shard tasks being scheduled by
    /// the tokio runtime. This is normal and resolves on the next call.
    pub fn system_stats(&self) -> SystemStats {
        let running = self.state.load(Ordering::Relaxed) == STATE_RUNNING;
        let gossip_shard_queue_depths: Vec<usize> = self.gossip_txs.iter()
            .map(|tx| tx.max_capacity() - tx.capacity())
            .collect();
        let dead_shards = if running {
            self.shard_alive.iter()
                .filter(|a| !a.load(Ordering::Relaxed))
                .count()
        } else {
            0
        };
        SystemStats {
            peers: self.peers.len(),
            // Running: use the GC-maintained atomic (O(1)).
            // Idle/stopped: fall back to an exact scan (pre-start inspection and tests).
            store_entries: if running {
                self.live_entries.load(Ordering::Relaxed)
            } else {
                self.store.pin().iter().filter(|(_, v)| v.data.is_some()).count()
            },
            // Filter out writers whose tasks have finished (idle-timed-out, peer evicted,
            // or disconnected) so the count reflects currently-active connections.
            // The GC pass lazily removes the finished entries from the map; this filter
            // makes the metric accurate without waiting for the next GC cycle.
            cached_connections: self.peer_writers.iter()
                .filter(|e| !e.value().handle.is_finished())
                .count(),
            gossip_shard_queue_depths,
            dead_shards,
            // When not running, gate to `true` so callers don't mistake a clean
            // shutdown (or pre-start state) for a task crash.
            gc_alive:             !running || self.gc_alive.load(Ordering::Relaxed),
            health_monitor_alive: !running || self.health_monitor_alive.load(Ordering::Relaxed),
            intern_pool_size:     intern_pool_len(),
            dropped_frames:       self.dropped_frames.load(Ordering::Relaxed),
        }
    }

    // ── Lifecycle (shutdown) ──────────────────────────────────────────────────────────

    /// Signals all background tasks to stop and waits up to `timeout` for them to exit.
    ///
    /// Tasks that have not exited within the timeout are logged as warnings and abandoned.
    /// Passing `Duration::MAX` replicates the old unbounded-wait behaviour.
    pub async fn shutdown_with_timeout(&self, timeout: Duration) {
        // Tombstone this node's pheromone trails before stopping gossip shards so
        // peers see the evaporation immediately rather than waiting for max_age expiry.
        let my_load_prefix = format!("load/{}/", self.node_id);
        let load_keys: Vec<String> = self.store.pin()
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
        let writer_keys: Vec<NodeId> = self.peer_writers.iter().map(|e| e.key().clone()).collect();
        for key in writer_keys {
            if let Some((_, entry)) = self.peer_writers.remove(&key) {
                set.spawn(async move { let _ = entry.handle.await; });
            }
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

    // ── Consensus API (Layer 2 Extension) ───────────────────────────────────

    /// Subscribes to committed values for a consensus slot.
    ///
    /// Returns a `watch::Receiver` that fires whenever the slot is committed or
    /// overwritten. Initial value is the current committed state (or `None`).
    #[must_use]
    pub fn consensus_rx(&self, slot: &str) -> watch::Receiver<Option<Bytes>> {
        self.subscribe(format!("{}{}", consensus_ns::COMMITTED, slot))
    }

    /// Returns the last committed value for a consensus slot, or `None`.
    pub fn consensus_get(&self, slot: &str) -> Option<Bytes> {
        self.get(&format!("{}{}", consensus_ns::COMMITTED, slot))
    }

    /// Declares this node's quorum trust slice for `group` (SCP §3.1).
    ///
    /// Stored at `consensus/trust/{group}/{node_id}` and gossip-synced to all
    /// peers. The current protocol uses simple majority regardless of slices;
    /// this stores intent for future slice-aware quorum extensions.
    pub fn declare_trust(&self, group: &str, trusted_peers: &[NodeId]) {
        let key = format!("{}{}/{}", consensus_ns::TRUST, group, self.node_id);
        let mut buf = BytesMut::new();
        if bincode::serde::encode_into_std_write(
            trusted_peers, &mut (&mut buf).writer(), bincode_cfg(),
        ).is_ok() {
            let _ = self.set(key, buf.freeze());
        }
    }

    /// Returns all declared trust slices for `group`, keyed by declaring node.
    pub fn group_trust(&self, group: &str) -> Vec<(NodeId, Vec<NodeId>)> {
        let prefix = format!("{}{}/", consensus_ns::TRUST, group);
        self.scan_prefix(&prefix)
            .into_iter()
            .filter_map(|(key, bytes)| {
                let node_str = key.strip_prefix(&prefix)?;
                let node_id: NodeId = node_str.parse().ok()?;
                let (peers, _) = bincode::serde::decode_from_slice::<Vec<NodeId>, _>(
                    &bytes, bincode_cfg(),
                ).ok()?;
                Some((node_id, peers))
            })
            .collect()
    }

    /// Returns the group member with the lowest observed load for `kind`.
    ///
    /// Iterates `grp/{group}/` for member NodeIds, then reads `load/{member}/{kind}`
    /// from Layer I (written by [`manage_opacity`](Self::manage_opacity)) for each.
    /// Members with no load entry are ranked lowest (transparent). Returns the
    /// lowest-load member, or `self.node_id().clone()` when the group is empty or
    /// no members have load data within `max_age`.
    ///
    /// `max_age` is used for pheromone evaporation — entries older than this are
    /// treated as transparent.
    pub fn suggest_leader(&self, group: &str, kind: &str, max_age: Duration) -> NodeId {
        let prefix = format!("grp/{}/", group);
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH).unwrap_or_default()
            .as_millis() as u64;
        let max_age_ms = max_age.as_millis() as u64;

        let members: Vec<NodeId> = self
            .scan_prefix(&prefix)
            .into_iter()
            .filter_map(|(key, _)| {
                let node_str = key.strip_prefix(&prefix)?;
                node_str.parse::<NodeId>().ok()
            })
            .collect();

        if members.is_empty() {
            return self.node_id.clone();
        }

        let best = members
            .iter()
            .min_by(|a, b| {
                let load_a = self.get(&format!("load/{}/{}", a, kind))
                    .and_then(|b| decode_load_state(&b))
                    .filter(|s| now_ms.saturating_sub(s.written_at_ms) <= max_age_ms)
                    .map(|s| s.fill_ratio)
                    .unwrap_or(0.0);
                let load_b = self.get(&format!("load/{}/{}", b, kind))
                    .and_then(|b| decode_load_state(&b))
                    .filter(|s| now_ms.saturating_sub(s.written_at_ms) <= max_age_ms)
                    .map(|s| s.fill_ratio)
                    .unwrap_or(0.0);
                load_a.partial_cmp(&load_b).unwrap_or(std::cmp::Ordering::Equal)
            });

        best.cloned().unwrap_or_else(|| self.node_id.clone())
    }

    /// Proposes `value` for a named `slot` within a group.
    ///
    /// Blocks until quorum commits, another node commits first, or all ballot
    /// attempts are exhausted. All group members that called
    /// [`start_consensus_listener`](Self::start_consensus_listener) participate
    /// as voters.
    ///
    /// Quorum defaults to `floor(N/2)+1` where N is the current group member
    /// count. Set `config.quorum_size > 0` to override.
    pub async fn group_propose(
        &self,
        group:  &str,
        slot:   &str,
        value:  Bytes,
        config: ConsensusConfig,
    ) -> ConsensusResult {
        // Defer if overloaded: prefer the durable pheromone trail (Layer I) as the primary
        // signal since that is the same source voters use to abstain; fall back to the
        // in-memory channel fill for the case where manage_opacity hasn't written a trail yet.
        let pheromone_fill = self
            .get(&format!("load/{}/{}", self.node_id, consensus_kind::PROPOSE))
            .and_then(|b| decode_load_state(&b))
            .map(|s| s.fill_ratio)
            .unwrap_or(0.0);
        let local_opacity = pheromone_fill.max(self.opacity(consensus_kind::PROPOSE));
        if local_opacity > 0.0 && config.ballot_retry_jitter_ms > 0 {
            let defer_ms = (local_opacity * config.ballot_retry_jitter_ms as f32 * 2.0) as u64;
            tokio::time::sleep(Duration::from_millis(defer_ms)).await;
        }
        // Collect member ids once; used for both the count and the opaque filter.
        let grp_prefix = format!("grp/{}/", group);
        let member_ids: AHashSet<String> = self
            .scan_prefix(&grp_prefix)
            .into_iter()
            .filter_map(|(key, _)| key.strip_prefix(&grp_prefix).map(|s| s.to_string()))
            .collect();
        let raw_members = member_ids.len().max(1);
        let active_members = if config.count_opaque_as_absent {
            let now_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64;
            let freshness_ms = self.config.health_check_interval_secs * 2 * 1000;
            // Only count nodes that are actually members of this group as opaque.
            let opaque_count = member_ids.iter().filter(|node_str| {
                self.scan_prefix(&format!("load/{}/", node_str))
                    .into_iter()
                    .any(|(_, bytes)| {
                        decode_load_state(&bytes)
                            .map(|s| s.is_opaque && now_ms.saturating_sub(s.written_at_ms) <= freshness_ms)
                            .unwrap_or(false)
                    })
            }).count();
            raw_members.saturating_sub(opaque_count).max(1)
        } else {
            raw_members
        };
        let quorum  = compute_quorum_size(config.quorum_size, active_members);
        self.make_consensus_engine(config.abstain_when_opaque, config.use_trust_slices)
            .propose(SignalScope::Group(Arc::from(group)), Arc::from(slot), value, quorum, config)
            .await
    }

    /// Proposes `value` for system-wide consensus (all known peers vote).
    ///
    /// Quorum defaults to `floor(N/2)+1` where N is `peers + 1` (including self).
    /// Set `config.quorum_size > 0` to override.
    pub async fn system_propose(
        &self,
        slot:   &str,
        value:  Bytes,
        config: ConsensusConfig,
    ) -> ConsensusResult {
        // Defer if overloaded: prefer the durable pheromone trail (Layer I) as the primary
        // signal since that is the same source voters use to abstain; fall back to the
        // in-memory channel fill for the case where manage_opacity hasn't written a trail yet.
        let pheromone_fill = self
            .get(&format!("load/{}/{}", self.node_id, consensus_kind::PROPOSE))
            .and_then(|b| decode_load_state(&b))
            .map(|s| s.fill_ratio)
            .unwrap_or(0.0);
        let local_opacity = pheromone_fill.max(self.opacity(consensus_kind::PROPOSE));
        if local_opacity > 0.0 && config.ballot_retry_jitter_ms > 0 {
            let defer_ms = (local_opacity * config.ballot_retry_jitter_ms as f32 * 2.0) as u64;
            tokio::time::sleep(Duration::from_millis(defer_ms)).await;
        }
        let n_nodes = (self.system_stats().peers + 1).max(1);
        let active_n = if config.count_opaque_as_absent {
            let now_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64;
            let freshness_ms = self.config.health_check_interval_secs * 2 * 1000;
            let opaque_count = self.scan_prefix(kv_ns::LOAD)
                .into_iter()
                .filter(|(_, bytes)| {
                    decode_load_state(bytes)
                        .map(|s| s.is_opaque && now_ms.saturating_sub(s.written_at_ms) <= freshness_ms)
                        .unwrap_or(false)
                })
                .count();
            n_nodes.saturating_sub(opaque_count).max(1)
        } else {
            n_nodes
        };
        let quorum  = compute_quorum_size(config.quorum_size, active_n);
        self.make_consensus_engine(config.abstain_when_opaque, config.use_trust_slices)
            .propose(SignalScope::System, Arc::from(slot), value, quorum, config)
            .await
    }

    /// Starts the consensus voter/listener task.
    ///
    /// Nodes that call this participate as voters in all consensus rounds.
    /// Nodes that do not call this still receive committed values via anti-entropy
    /// KV sync but their votes will not be counted.
    ///
    /// `config.abstain_when_opaque` controls whether this voter silently drops
    /// PROPOSE messages while its pheromone trail shows `is_opaque: true`.
    ///
    /// Returns a [`ConsensusHandle`] whose drop stops the task. The task also
    /// exits on [`shutdown`](Self::shutdown).
    pub fn start_consensus_listener(&self, config: ConsensusConfig) -> ConsensusHandle {
        let (cancel_tx, cancel_rx) = tokio::sync::oneshot::channel::<()>();
        let shutdown_rx = self.shutdown_tx.subscribe();

        let handle = self.make_consensus_engine(config.abstain_when_opaque, config.use_trust_slices).spawn_listener(cancel_rx, shutdown_rx);
        {
            let mut handles = self.task_handles.lock().unwrap_or_else(|e| e.into_inner());
            handles.retain(|h| !h.is_finished());
            handles.push(handle);
        }
        ConsensusHandle { _cancel: cancel_tx }
    }

    // ── Private helpers ───────────────────────────────────────────────────────

    /// Encodes `update` and delivers it to the correct gossip shard via `try_send`.
    /// Returns `false` if the channel is full or the shard has died.
    fn dispatch_update(&self, update: GossipUpdate) -> bool {
        let shard  = shard_for_key(&update.key, self.gossip_txs.len());
        let sender = update.sender;
        let mut buf = BytesMut::with_capacity(256);
        if bincode::serde::encode_into_std_write(
            WireMessage::Data(update), &mut (&mut buf).writer(), bincode_cfg(),
        ).is_err() {
            error!("Gossip shard {}: encode failed", shard);
            return false;
        }
        match self.gossip_txs[shard].try_send((buf.freeze(), sender, ForwardHint::All)) {
            Ok(()) => true,
            Err(TrySendError::Full(_)) => {
                self.dropped_frames.fetch_add(1, Ordering::Relaxed);
                warn!("Gossip channel full for shard {}; frame dropped", shard);
                false
            }
            Err(TrySendError::Closed(_)) => {
                if self.state.load(Ordering::Relaxed) == STATE_RUNNING {
                    error!("Gossip shard {} is dead; update will not propagate", shard);
                } else {
                    warn!("Gossip shard {} not started; update stored locally only", shard);
                }
                false
            }
        }
    }

    /// Like `dispatch_update` but awaits channel capacity rather than dropping.
    async fn dispatch_update_async(&self, update: GossipUpdate) -> bool {
        let shard  = shard_for_key(&update.key, self.gossip_txs.len());
        let sender = update.sender;
        let mut buf = BytesMut::with_capacity(256);
        if bincode::serde::encode_into_std_write(
            WireMessage::Data(update), &mut (&mut buf).writer(), bincode_cfg(),
        ).is_err() {
            error!("Gossip shard {}: encode failed", shard);
            return false;
        }
        match self.gossip_txs[shard].send((buf.freeze(), sender, ForwardHint::All)).await {
            Ok(()) => true,
            Err(_) => {
                if self.state.load(Ordering::Relaxed) == STATE_RUNNING {
                    error!("Gossip shard {} is dead; update will not propagate", shard);
                }
                false
            }
        }
    }

    fn make_update(&self, key: Arc<str>, value: Bytes, is_tombstone: bool) -> GossipUpdate {
        // Use SystemTime::now() here (not the cached current_ts) so that each
        // locally-originated write gets a fresh timestamp. Two set() calls in
        // the same health-monitor tick interval would otherwise share a timestamp
        // and lose LWW determinism for concurrent cross-node writes to the same key.
        //
        // Known limitation: SystemTime is not monotonic. An NTP backward step can
        // produce a timestamp smaller than a previous write, causing LWW to silently
        // discard the new write on remote nodes that already hold the higher timestamp.
        // Workloads that require strict monotonicity should use a logical clock or a
        // hybrid logical clock on top of this layer.
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        GossipUpdate {
            nonce: fastrand::u64(1..),  // 0 is reserved as ANTI_ENTROPY_NONCE
            sender: self.node_id.id_hash(),
            ttl: self.config.default_ttl,
            is_tombstone,
            timestamp,
            key,
            value,
        }
    }

    // ── Consensus internals ───────────────────────────────────────────────────

    fn make_consensus_engine(&self, abstain_when_opaque: bool, use_trust_slices: bool) -> ConsensusEngine {
        ConsensusEngine {
            node_id:             self.node_id.clone(),
            seen:                self.seen.clone(),
            current_ts:          self.current_ts.clone(),
            signal_boundary:     self.signal_boundary.clone(),
            signal_handlers:     self.signal_handlers.clone(),
            gossip_txs:          self.gossip_txs.clone(),
            default_ttl:         self.config.default_ttl,
            dropped_frames:      self.dropped_frames.clone(),
            store:                self.store.clone(),
            subscriptions:       self.subscriptions.clone(),
            abstain_when_opaque,
            use_trust_slices,
        }
    }


}

/// Sends the shutdown signal on drop — best-effort only. Does not wait for
/// background tasks to exit. Call [`shutdown`](GossipAgent::shutdown) or
/// [`shutdown_with_timeout`](GossipAgent::shutdown_with_timeout) before
/// dropping for a clean drain.
impl Drop for GossipAgent {
    fn drop(&mut self) {
        let _ = self.shutdown_tx.send(true);
    }
}

impl std::fmt::Debug for GossipAgent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GossipAgent")
            .field("node_id", &self.node_id)
            .field("state", &self.state.load(Ordering::Relaxed))
            .field("peers", &self.peers.len())
            .field("store_entries", &self.store.len())
            .finish_non_exhaustive()
    }
}

// ── Free helpers ─────────────────────────────────────────────────────────────

/// Generates a nonce, marks it seen, delivers locally (with boundary + opacity checks),
/// encodes the wire frame, and routes to the correct gossip shard via `try_send`.
///
/// Shared by [`GossipAgent::emit`] and the [`advertise`](GossipAgent::advertise) task.
#[allow(clippy::too_many_arguments)]
pub(crate) fn emit_signal(
    node_id:         &NodeId,
    seen:            &ShardedSeen,
    current_ts:      &AtomicU64,
    signal_boundary: &RwLock<Boundary>,
    signal_handlers: &SignalHandlers,
    gossip_txs:      &[mpsc::Sender<(Bytes, u64, ForwardHint)>],
    default_ttl:     u8,
    dropped_frames:  &AtomicU64,
    kind:            Arc<str>,
    scope:           SignalScope,
    payload:         Bytes,
) -> bool {
    let nonce = fastrand::u64(1..);  // 0 is reserved as ANTI_ENTROPY_NONCE
    let ts = current_ts.load(Ordering::Relaxed);
    let _ = seen.is_duplicate(nonce, ts);

    if signal_boundary.read().admits(&scope) {
        let admit = match &scope {
            SignalScope::Individual(_) => true,
            _ => {
                let opacity = signal_handlers.fill_ratio(&kind);
                opacity == 0.0 || fastrand::f32() >= opacity
            }
        };
        if admit {
            signal_handlers.deliver(&Signal {
                kind: kind.clone(), scope: scope.clone(),
                payload: payload.clone(), sender: node_id.clone(), nonce,
            });
        }
    }

    let hint = match &scope {
        SignalScope::System           => ForwardHint::All,
        SignalScope::Group(name)      => ForwardHint::Group(name.clone()),
        SignalScope::Individual(peer) => ForwardHint::Individual(peer.clone()),
    };
    let shard = shard_for_key(&kind, gossip_txs.len());
    let sender_hash = node_id.id_hash();
    let mut buf = BytesMut::with_capacity(256);
    if bincode::serde::encode_into_std_write(
        WireMessage::Signal { ttl: default_ttl, nonce, sender: node_id.clone(), scope, kind, payload },
        &mut (&mut buf).writer(),
        bincode_cfg(),
    ).is_err() {
        error!("Signal encode failed");
        return false;
    }
    match gossip_txs[shard].try_send((buf.freeze(), sender_hash, hint)) {
        Ok(()) => true,
        Err(TrySendError::Full(_)) => {
            dropped_frames.fetch_add(1, Ordering::Relaxed);
            warn!("Gossip channel full for shard {}; signal dropped", shard);
            false
        }
        Err(TrySendError::Closed(_)) => {
            warn!("Gossip shard {} not available; signal will not propagate", shard);
            false
        }
    }
}

// ── Background task implementations ──────────────────────────────────────────

/// Clears an `AtomicBool` liveness flag on drop — handles both clean exit and panics.
struct AliveGuard(Arc<AtomicBool>);
impl Drop for AliveGuard {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Relaxed);
    }
}

/// Decrements the listener count on drop and logs an error if the exit was unexpected.
/// Ensures the count is always balanced even on panics.
struct ListenerGuard {
    count:       Arc<AtomicUsize>,
    shutdown_tx: Arc<watch::Sender<bool>>,
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

/// Bundled context cloned into every listener task — replaces the 18-argument
/// function signature and removes the `#[allow(clippy::too_many_arguments)]` suppressor.
#[derive(Clone)]
struct ListenerContext {
    node_id:        NodeId,
    store:          Arc<papaya::HashMap<Arc<str>, StoreEntry>>,
    peers:          Arc<papaya::HashMap<NodeId, Instant>>,
    gossip_txs:     Arc<[mpsc::Sender<(Bytes, u64, ForwardHint)>]>,
    seen:           Arc<ShardedSeen>,
    shutdown_tx:    Arc<watch::Sender<bool>>,
    subscriptions:  Arc<papaya::HashMap<Arc<str>, watch::Sender<Option<Bytes>>>>,
    current_ts:     Arc<AtomicU64>,
    peer_writers:   Arc<DashMap<NodeId, WriterEntry>>,
    conn_sem:       Arc<Semaphore>,
    listener_alive: Arc<AtomicUsize>,
    max_conn:       usize,
    max_ttl:        u8,
    writer_depth:   usize,
    backoff:        Duration,
    n_shards:        usize,
    intern_keys:     bool,
    intern_max_keys: usize,
    signal_boundary: Arc<RwLock<Boundary>>,
    signal_handlers: Arc<SignalHandlers>,
    max_peers:       usize,
    writer_idle_timeout: Duration,
}

async fn run_listener_task(listener: TcpListener, lctx: ListenerContext) {
    let ListenerContext {
        node_id, store, peers, gossip_txs, seen, shutdown_tx, subscriptions,
        current_ts, peer_writers, conn_sem, listener_alive,
        max_conn, max_ttl, writer_depth, backoff, n_shards, intern_keys, intern_max_keys,
        signal_boundary, signal_handlers, max_peers, writer_idle_timeout,
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
                                conn_set.spawn(async move {
                                    let _permit = permit;
                                    let ctx = ConnContext {
                                        node_id, store, peers, gossip_txs,
                                        seen, shutdown, max_ttl, subscriptions,
                                        current_ts, peer_writers,
                                        writer_depth, backoff, n_shards,
                                        intern_keys, intern_max_keys, signal_boundary,
                                        signal_handlers, max_peers, writer_idle_timeout,
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
    // _guard drops here, decrementing listener_alive and logging an error if
    // the exit was unexpected (including on panic).
}

#[allow(clippy::too_many_arguments)]
async fn run_gossip_shard(
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
    store:                 Arc<papaya::HashMap<Arc<str>, StoreEntry>>,
    group_aware_forwarding: bool,
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

    // Send `data` to `peer`, retrying with a fresh writer if the cached sender is closed.
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

                // Bytes is pre-encoded; no re-encoding per hop (zero-copy forwarding).
                if !group_aware_forwarding {
                    // Default: broadcast to all targets (pre-Fix-2 behaviour).
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
                            // Collect known group members from the KV store (grp/<name>/ prefix).
                            let prefix = format!("grp/{}/", name);
                            let guard = store.pin();
                            let members: AHashSet<NodeId> = guard.iter()
                                .filter(|(k, v)| k.starts_with(&*prefix) && v.data.is_some())
                                .filter_map(|(k, _)| k[prefix.len()..].parse::<NodeId>().ok())
                                .collect();
                            drop(guard);

                            // Forward to all known members present in our target set.
                            for peer in targets.iter()
                                .filter(|p| p.id_hash() != sender_hash && members.contains(*p))
                            {
                                send_to_peer!(peer, data);
                            }

                            // Plus up to EPIDEMIC_K random non-members for epidemic coverage.
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
    // _alive_guard drops here, clearing alive to false on both clean exit and panic.
}

#[allow(clippy::too_many_arguments)]
async fn run_health_monitor(
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
        request_state(peer, &peer_writers, &store, writer_depth, backoff, idle_timeout, &shutdown_tx, &node_id);
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
                // Single pass: collect peer list, build set, and sample for ping.
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
                    // Collect directly into Arc<[NodeId]> — avoids an intermediate Vec allocation.
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
                            // Writer exited; evict stale entry and retry with a fresh writer.
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
                // Skip peer eviction when the node has been running for less time than
                // the eviction window; evicting nothing is safer than evicting all peers.
                let maybe_peer_cutoff = Instant::now().checked_sub(eviction_window);

                if let Some(peer_cutoff) = maybe_peer_cutoff {
                    // Single guard scope: collect stale keys then evict in the same pin.
                    // papaya's lock-free compute is safe to call while iterating because
                    // pin() establishes an epoch guard that keeps snapshot consistency.
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
    // _alive_guard drops here, clearing health_monitor_alive on both clean exit and panic.
}

// ── Listener construction ─────────────────────────────────────────────────────

/// On unix, create a TCP listener with `SO_REUSEPORT` so multiple listener tasks
/// can be bound to the same address. The kernel load-balances accepted connections
/// across all bound sockets.
#[cfg(unix)]
async fn new_listener(addr: SocketAddr, backlog: u32) -> Result<TcpListener, GossipError> {
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

/// On non-unix, use a single TcpSocket listener with the configured accept backlog.
#[cfg(not(unix))]
async fn new_listener(addr: SocketAddr, backlog: u32) -> Result<TcpListener, GossipError> {
    let sock = if addr.is_ipv6() {
        tokio::net::TcpSocket::new_v6()
    } else {
        tokio::net::TcpSocket::new_v4()
    }.map_err(GossipError::Io)?;
    sock.set_reuseaddr(true).map_err(GossipError::Io)?;
    sock.bind(addr).map_err(GossipError::Io)?;
    sock.listen(backlog).map_err(GossipError::Io)
}

// ── GC task ───────────────────────────────────────────────────────────────────

/// Long-running background task that performs slow housekeeping on a 10× interval:
/// tombstone expiry, closed-subscription eviction, seen-set eviction, sender-log
/// trimming, intern-pool monitoring, and live-entry count refresh.
///
/// Separating this from the health monitor lets the latency-sensitive ping loop
/// run at full frequency without being delayed by O(n) store scans.
#[allow(clippy::too_many_arguments)]
async fn run_gc_task(
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
    signal_handlers:   Arc<crate::signal::SignalHandlers>,
    peer_writers:      Arc<DashMap<NodeId, WriterEntry>>,
    intern_max_keys:   usize,
) {
    gc_alive.store(true, Ordering::Relaxed);
    let _alive_guard = AliveGuard(gc_alive.clone());
    let mut shutdown_rx = shutdown_tx.subscribe();
    // Run once at startup so system_stats() is accurate from the first call.
    let initial = store.pin().iter().filter(|(_, v)| v.data.is_some()).count();
    live_entries.store(initial, Ordering::Relaxed);

    // GC does not need to run at the same frequency as pings; 10× slower is fine.
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
                // Retain tombstones for 10× the propagation window so every node has ample
                // time to receive a tombstone before it is GC'd. With defaults (TTL=5,
                // window=60s): 5 × 60 × 10 = 50 minutes.
                let tombstone_cutoff = wall_ts.saturating_sub(
                    (default_ttl as u64)
                        .saturating_mul(propagation_window)
                        .saturating_mul(10)
                        .saturating_mul(1_000),
                );

                // Tombstone GC — count live entries in the same pass.
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

                // Key intern pool monitoring.
                // Threshold: 2× the configured cap (if set) or 100 000 (if uncapped).
                // Fires once per GC interval so operators have a proactive signal without
                // needing to poll system_stats().
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

                // Sender-log GC — removes entries older than 10 min across all signal kinds.
                // Prevents unbounded growth when dynamic kind strings are used or when
                // deliver() is not called for a kind (e.g. suppressed, no handler registered).
                signal_handlers.trim_sender_log();

                // Closed-subscription eviction.
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

                // Seen-set eviction: nonces older than 4× propagation windows are safe
                // to forget (a TTL-1 message reaches every node within 1× window; 4× is
                // a generous safety margin). When the set is over the size limit the more
                // aggressive 2× cutoff is used to shed load faster.
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

                // Evict writer map entries whose tasks have finished (idle timeout,
                // peer eviction, disconnect). This keeps cached_connections accurate
                // and bounds the DashMap to currently-active peers.
                peer_writers.retain(|_, entry| !entry.handle.is_finished());
            }
            _ = shutdown_rx.wait_for(|v| *v) => break,
        }
    }

    if !*shutdown_rx.borrow() {
        error!("GC task exited unexpectedly; tombstone expiry and subscription eviction have stopped");
    }
    // _alive_guard drops here, clearing gc_alive on both clean exit and panic.
}

async fn run_boundary_reconcile(
    node_id:         NodeId,
    store:           Arc<papaya::HashMap<Arc<str>, StoreEntry>>,
    signal_boundary: Arc<RwLock<Boundary>>,
    shutdown_tx:     Arc<watch::Sender<bool>>,
    interval_secs:   u64,
) {
    let mut shutdown_rx = shutdown_tx.subscribe();
    let mut ticker = time::interval(Duration::from_secs(interval_secs));
    ticker.set_missed_tick_behavior(time::MissedTickBehavior::Skip);

    loop {
        tokio::select! { biased;
            _ = shutdown_rx.wait_for(|v| *v) => return,
            _ = ticker.tick() => {
                let suffix = format!("/{}", node_id);
                let mut to_insert: Vec<Arc<str>> = Vec::new();
                let mut to_remove: Vec<Arc<str>> = Vec::new();
                {
                    let guard = store.pin();
                    for (key, entry) in guard.iter() {
                        if !key.starts_with("grp/") || !key.ends_with(suffix.as_str()) { continue; }
                        let Some(tail) = key.strip_prefix("grp/") else { continue };
                        let Some(group) = tail.strip_suffix(suffix.as_str()) else { continue };
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
    }
}

// ── Consensus helpers ─────────────────────────────────────────────────────────

fn compute_quorum_size(config_size: usize, member_count: usize) -> usize {
    if config_size > 0 { config_size } else { member_count / 2 + 1 }
}


