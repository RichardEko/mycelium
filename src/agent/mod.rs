use crate::config::GossipConfig;
use crate::framing::ForwardHint;
use crate::node_id::NodeId;
use crate::seen::ShardedSeen;
use crate::signal::{Boundary, SignalHandlers};
use crate::store::KvState;
use crate::writer::WriterEntry;
use bytes::Bytes;
use parking_lot::RwLock;
use std::{
    sync::{
        atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering},
        Arc,
    },
    time::Instant,
};
use tokio::{sync::{mpsc, watch}, task::JoinSet};

mod lifecycle;
mod kv;
mod signal_ops;
mod rpc;
mod http;
mod mcp;
mod opacity;
mod consensus_ops;
mod capability_ops;
mod demand;
mod emergent_groups;
mod wiring;
pub(crate) mod helpers;
mod tasks;
mod state_machine;
mod scatter;
mod bulk;
mod mailbox;

pub(crate) use helpers::emit_signal;
pub(crate) use helpers::emit_signal_async;
pub(crate) use helpers::make_gossip_update;
pub(crate) use opacity::is_self_opaque;
pub use mcp::{McpClientHandle, McpError, McpToolHandle};
pub use rpc::RpcError;
pub use state_machine::{AgentPolicy, ExecutionState, AgentStateMachine, PolicyViolation};
pub use scatter::{ScatterError, ScatterResult};
pub use bulk::{BulkError, BulkServeHandle};
pub use mailbox::{MailboxHandle, MeshEvent};

/// Cached roster entry for a single group, held in the short-lived `group_roster_cache`.
pub(super) struct RosterEntry {
    pub(super) members:    Vec<NodeId>,
    pub(super) fetched_at: Instant,
    /// Value of `KvState::grp_generation` at the time this entry was fetched.
    /// If the generation counter has advanced, the roster is stale and must be re-fetched.
    pub(super) grp_gen:    u64,
}

type RosterCache = Arc<papaya::HashMap<Arc<str>, Arc<RosterEntry>>>;

/// Gossip shard channel receivers, taken once by `start_gossip_loop`.
type GossipRxs = std::sync::Mutex<Option<Vec<mpsc::Receiver<(Bytes, u64, ForwardHint)>>>>;

/// Agent lifecycle state stored in an `AtomicU8`.
#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) enum AgentState {
    Idle    = 0,
    Running = 1,
    Stopped = 2,
}

impl AgentState {
    pub(super) fn from_u8(v: u8) -> Self {
        match v {
            0 => Self::Idle,
            1 => Self::Running,
            _ => Self::Stopped,
        }
    }
}

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

/// Shared infrastructure fields extracted from `GossipAgent` so they can be
/// bundled into a single `Arc` and handed to `ConsensusEngine` and long-lived
/// task helpers without requiring each to clone 8 individual fields.
///
/// `GossipAgent` cannot be passed directly to these helpers because doing so
/// would create a reference cycle: the agent holds task handles, and those
/// tasks would hold a reference back to the agent.
pub(crate) struct TaskCtx {
    pub(crate) node_id:          NodeId,
    pub(crate) seen:             Arc<ShardedSeen>,
    /// Hybrid Logical Clock for causal LWW ordering. `make_gossip_update`
    /// calls `tick()` for every locally-originated write; the connection
    /// handler calls `observe()` for every incoming timestamp so the local
    /// clock dominates any remote stamp it has seen.
    pub(crate) hlc:              Arc<crate::hlc::Hlc>,
    pub(crate) signal_boundary:  Arc<RwLock<Boundary>>,
    pub(crate) signal_handlers:  Arc<SignalHandlers>,
    pub(crate) gossip_txs:       Arc<[mpsc::Sender<(Bytes, u64, ForwardHint)>]>,
    pub(crate) default_ttl:      u8,
    pub(crate) kv_state:         Arc<KvState>,
    /// WAL handle for durable KV writes. Unset when persistence is disabled.
    /// Written once by `start()` after replay; read-only afterwards.
    pub(crate) wal: std::sync::OnceLock<Arc<crate::persistence::WalHandle>>,
    /// Set to `true` by the first tick of any `run_kv_persist_task` (capability
    /// or locality advertisement). Until this is `true`, soft-state keys have
    /// not yet been written to the local store after a restart, so `/ready`
    /// returns 503.
    pub(crate) caps_advertised: Arc<std::sync::atomic::AtomicBool>,
    /// Bulk-transport adapter: staging map, HTTP port, pooled HTTP client.
    pub(crate) bulk_transport: Arc<bulk::BulkTransport>,
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
///
/// ## Topical method index
///
/// `GossipAgent` exposes a wide surface; the methods cluster as follows.
/// Use this index to find the family of methods you want.
///
/// - **Lifecycle**: `new`, `start`, `shutdown`, `shutdown_with_timeout`,
///   `system_stats`, `peers`, `groups`, `peer_drop_counts`.
/// - **KV (Layer I)**: `set`, `set_async`, `get`, `delete`, `delete_async`,
///   `keys`, `scan_prefix`, `subscribe`, `subscribe_prefix`.
/// - **Signals (Layer II) — emit/receive**: `emit`, `emit_async`, `signal_rx`,
///   `signal_rx_with_capacity`, `signal_once`, `request`, `advertise`, `advertise_persistent`.
/// - **RPC (Layer III)**: `rpc_call`, `rpc_respond`.
/// - **Groups (static)**: `join_group`, `leave_group`, `group_members`,
///   `cached_group_members`, `group_quorum`, `group_quorum_prehashed`.
/// - **Opacity & load (Layer II)**: `manage_opacity`, `manage_opacity_gated`,
///   `opacity`, `effective_opacity`, `is_opaque`, `is_self_opaque`,
///   `is_node_opaque`, `peer_load`, `peer_load_rx`, `count_opaque_system`,
///   `count_opaque_members`.
/// - **Signal log / quorum**: `last_signal`, `last_signal_persistent`,
///   `quorum`, `quorum_persistent`, `signal_window`, `signal_window_secs`,
///   `suppress`, `unsuppress`, `is_suppressed`.
/// - **Consensus (Layer III)**: `group_propose`, `system_propose`,
///   `start_consensus_listener`, `consensus_get`, `consensus_rx`,
///   `declare_trust`, `suggest_leader`.
/// - **Capability / requirement** (Phase 3): `advertise_capability`, `resolve`,
///   `watch_capabilities`, `declare_requirement`, `watch_requirement`,
///   `suggest_leader_with_requirements`.
/// - **Emergent groups** (Phase 3g/3h): `define_capability_group`. (The
///   per-agent watcher task that drives membership is started automatically
///   by `start()`.)
/// - **Inter-group wiring** (Phase 4 + Phase 5 + Phase 6): `resolve_wiring`,
///   `watch_wiring`, `signal_wired_via`, `resolve_with_locality`,
///   `resolve_wiring_with_locality`, `signal_wired_via_locality`. Ranking is
///   applied automatically when the `CapFilter` carries a `CapRanking`.
/// - **Demand pressure** (Phase 9): `demand`, `watch_demand`.
///
/// Items not in this index are private implementation details (methods like
/// `make_update`, `dispatch_update`, `spawn_task`, etc. are `pub(super)` or
/// `pub(crate)` only).
pub struct GossipAgent {
    pub(super) node_id: NodeId,
    pub(super) config: GossipConfig,
    pub(super) peers: Arc<papaya::HashMap<NodeId, Instant>>,
    pub(super) peer_list_tx: watch::Sender<Arc<[NodeId]>>,
    pub(super) bootstrap_peers: Arc<[NodeId]>,
    pub(super) gossip_rxs: GossipRxs,
    pub(super) peer_writers: Arc<papaya::HashMap<NodeId, WriterEntry>>,
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
    pub(super) task_handles: std::sync::Mutex<JoinSet<()>>,
    /// Bundled KV-path state (store + subscriptions + prefix_index + hash_acc +
    /// dropped_frames + max_store_entries). Access fields via `self.kv_state.x`.
    pub(super) kv_state: Arc<KvState>,
    /// Infrastructure bundle shared with `ConsensusEngine` and long-lived task helpers.
    /// Access fields via `self.task_ctx.x`.
    pub(super) task_ctx: Arc<TaskCtx>,
    /// Short-lived cache of group membership lists, keyed by group name.
    /// Entries expire after `health_check_interval_secs` and are eagerly invalidated
    /// by `join_group`/`leave_group`. Avoids a full prefix-scan per `group_propose` call.
    pub(super) group_roster_cache: RosterCache,
}

impl GossipAgent {
    /// Returns the configured pheromone evaporation window as a `Duration`.
    ///
    /// Use this in calls to [`suggest_leader`](Self::suggest_leader),
    /// [`peer_load`](Self::peer_load), and [`route_to`](Self::route_to) instead of
    /// the compile-time [`SENDER_LOG_WINDOW`](crate::signal::SENDER_LOG_WINDOW) constant,
    /// so the evaporation window respects the operator's [`GossipConfig::signal_window_secs`].
    pub fn signal_window(&self) -> std::time::Duration {
        std::time::Duration::from_secs(self.config.signal_window_secs)
    }

    /// Acquires the task-handles lock, recovering from poison.
    pub(super) fn task_handles_lock(&self) -> std::sync::MutexGuard<JoinSet<()>> {
        self.task_handles.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Spawns `fut` onto the Tokio runtime and tracks it in the task-handles `JoinSet`.
    /// Replaces the `tokio::spawn` + `task_handles_lock().push(handle)` pattern so
    /// completed tasks are automatically reaped by the `JoinSet` rather than accumulating.
    pub(super) fn spawn_task<F>(&self, fut: F)
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        self.task_handles_lock().spawn(fut);
    }

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
        let shard_alive = (0..n_shards)
            .map(|_| Arc::new(AtomicBool::new(false)))
            .collect();
        let seen_shards = n_shards.max(16);

        let signal_window = std::time::Duration::from_secs(config.signal_window_secs);
        let kv_state      = KvState::new(config.max_store_entries);
        let default_ttl   = config.default_ttl;
        let gossip_txs: Arc<[mpsc::Sender<(Bytes, u64, ForwardHint)>]> = gossip_txs_vec.into();
        let task_ctx = Arc::new(TaskCtx {
            node_id:         node_id.clone(),
            seen:            Arc::new(ShardedSeen::new(seen_shards)),
            hlc:             Arc::new(crate::hlc::Hlc::new()),
            signal_boundary: Arc::new(RwLock::new(Boundary::new(node_id.clone()))),
            signal_handlers: Arc::new(SignalHandlers::new(signal_window)),
            gossip_txs,
            default_ttl,
            kv_state:        kv_state.clone(),
            wal:             std::sync::OnceLock::new(),
            caps_advertised: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            bulk_transport:  Arc::new(bulk::BulkTransport::new(
                config.http_port.unwrap_or(0),
                std::time::Duration::from_secs(config.bulk_fetch_timeout_secs),
            )),
        });

        Self {
            node_id,
            config,
            peers: Arc::new(papaya::HashMap::new()),
            bootstrap_peers,
            peer_list_tx,
            gossip_rxs: std::sync::Mutex::new(Some(gossip_rxs_inner)),
            peer_writers: Arc::new(papaya::HashMap::new()),
            live_entries: Arc::new(AtomicUsize::new(0)),
            state: AtomicU8::new(AgentState::Idle as u8),
            shutdown_tx: Arc::new(shutdown_tx),
            shard_alive,
            listener_alive: Arc::new(AtomicUsize::new(0)),
            health_monitor_alive: Arc::new(AtomicBool::new(false)),
            gc_alive: Arc::new(AtomicBool::new(false)),
            task_handles: std::sync::Mutex::new(JoinSet::new()),
            kv_state,
            task_ctx,
            group_roster_cache: Arc::new(papaya::HashMap::new()),
        }
    }
}

impl GossipAgent {
    /// Creates an [`AgentStateMachine`] bound to this node.
    ///
    /// The state machine writes every committed transition to `agent/{node}/state`
    /// in the gossip KV store (visible to the whole mesh) and emits an
    /// `agent.state` signal. Policy guards in `policy` are checked synchronously
    /// (and asynchronously for approval flows) before each transition.
    ///
    /// Turn and tool-call counters are reset when the state machine enters
    /// `Idle`, `Done`, or `Failed` — i.e., at the start of each new task.
    pub fn agent_state_machine(&self, policy: AgentPolicy) -> Arc<AgentStateMachine> {
        AgentStateMachine::new(Arc::clone(&self.task_ctx), policy)
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
            .field("store_entries", &self.kv_state.store.len())
            .finish_non_exhaustive()
    }
}
