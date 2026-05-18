use crate::config::GossipConfig;
use crate::framing::ForwardHint;
use crate::node_id::NodeId;
use crate::seen::ShardedSeen;
use crate::signal::{Boundary, SignalHandlers};
use crate::store::{PrefixIndex, StoreEntry};
use crate::writer::WriterEntry;
use bytes::Bytes;
use dashmap::DashMap;
use parking_lot::RwLock;
use std::{
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicU8, AtomicUsize, Ordering},
        Arc,
    },
    time::Instant,
};
use tokio::{sync::{mpsc, watch}, task::JoinHandle};

mod lifecycle;
mod kv;
mod signal_ops;
mod opacity;
mod consensus_ops;
mod helpers;
mod tasks;

pub(crate) use helpers::emit_signal;

pub(super) const STATE_IDLE:    u8 = 0;
pub(super) const STATE_RUNNING: u8 = 1;
pub(super) const STATE_STOPPED: u8 = 2;

/// Number of random non-member peers added to Group-scoped signal fan-out
/// for epidemic coverage even when `group_aware_forwarding = true`.
pub(super) const EPIDEMIC_K: usize = 3;

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
    pub(super) node_id: NodeId,
    pub(super) config: GossipConfig,
    pub(super) store: Arc<papaya::HashMap<Arc<str>, StoreEntry>>,
    pub(super) peers: Arc<papaya::HashMap<NodeId, Instant>>,
    pub(super) peer_list_tx: watch::Sender<Arc<[NodeId]>>,
    pub(super) bootstrap_peers: Arc<[NodeId]>,
    /// Pre-encoded frame bytes + sender id_hash + forwarding hint; shards fan out without re-encoding.
    pub(super) gossip_txs: Arc<[mpsc::Sender<(Bytes, u64, ForwardHint)>]>,
    #[allow(clippy::type_complexity)]
    pub(super) gossip_rxs: std::sync::Mutex<Option<Vec<mpsc::Receiver<(Bytes, u64, ForwardHint)>>>>,
    pub(super) seen: Arc<ShardedSeen>,
    pub(super) current_ts: Arc<AtomicU64>,
    pub(super) peer_writers: Arc<DashMap<NodeId, WriterEntry>>,
    pub(super) subscriptions: Arc<papaya::HashMap<Arc<str>, watch::Sender<Option<Bytes>>>>,
    /// Cached count of live (non-tombstone) store entries. Updated by the GC task;
    /// up to one GC interval stale but O(1) to read via system_stats().
    pub(super) live_entries: Arc<AtomicUsize>,
    pub(super) state: AtomicU8,
    pub(super) shutdown_tx: Arc<watch::Sender<bool>>,
    pub(super) shard_alive: Vec<Arc<AtomicBool>>,
    /// Counts live listener tasks; error is logged when this reaches zero unexpectedly.
    pub(super) listener_alive: Arc<AtomicUsize>,
    pub(super) health_monitor_alive: Arc<AtomicBool>,
    pub(super) gc_alive: Arc<AtomicBool>,
    pub(super) task_handles: std::sync::Mutex<Vec<JoinHandle<()>>>,
    /// Local boundary filter — which scopes this node acts on.
    pub(super) signal_boundary: Arc<RwLock<Boundary>>,
    /// Fan-out registry for local signal delivery.
    pub(super) signal_handlers: Arc<SignalHandlers>,
    /// Cumulative count of gossip frames silently dropped due to full channels.
    pub(super) dropped_frames: Arc<AtomicU64>,
    /// Secondary index: first path segment → live key set.
    /// Maintained by `apply_and_notify`; used by `scan_prefix` for O(bucket) scans.
    pub(super) prefix_index: Arc<PrefixIndex>,
}

impl GossipAgent {
    /// Creates a new agent. Call [`start`](Self::start) to begin listening.
    pub fn new(node_id: NodeId, mut config: GossipConfig) -> Self {
        let cap = config.gossip_channel_capacity;
        let n_shards = config.gossip_shards.next_power_of_two();
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
        use std::time::{SystemTime, UNIX_EPOCH};
        let init_ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let shard_alive = (0..n_shards)
            .map(|_| Arc::new(AtomicBool::new(false)))
            .collect();
        let seen_shards = n_shards.max(16);

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
            prefix_index: Arc::new(PrefixIndex::new()),
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
