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
pub(crate) mod kv_quorum;
mod kv_handle;
mod mesh_handle;
mod consensus_handle;
mod overlay_consistent;
mod overlay_reliable;
mod rpc;
#[cfg(feature = "gateway")]
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
mod sharding;
mod shard_ops;
mod service_handle;
mod capability_handle;
mod schema_handle;
#[cfg(feature = "a2a")]
pub(crate) mod a2a;
#[cfg(feature = "llm")]
pub(crate) mod prompt;
#[cfg(feature = "llm")]
pub(crate) mod llm;

#[allow(unused_imports)]
pub(crate) use bulk::BulkTransport;
#[allow(unused_imports)]
pub(crate) use capability_ops::FilterOpacityRegistry;
pub(crate) use helpers::emit_signal;
pub(crate) use helpers::emit_signal_async;
pub(crate) use helpers::make_gossip_update;
#[cfg(feature = "llm")]
use helpers::{kv_delete, kv_scan_prefix, kv_set};
pub(crate) use opacity::is_self_opaque;
pub use mcp::{McpClientHandle, McpError, McpToolHandle};
pub use rpc::{RpcError, RpcRequest, RpcRequestRx};
pub use state_machine::{AgentPolicy, ExecutionState, AgentStateMachine, PolicyViolation};
pub use scatter::{ScatterError, ScatterResult};
pub use bulk::{BulkError, BulkServeHandle};
pub use mailbox::{MailboxHandle, MeshEvent};
pub use overlay_consistent::{ConsistencyError, LockGuard};
pub use consensus_handle::ConsensusHandle;
pub use service_handle::ServiceHandle;
pub use capability_handle::CapabilitiesHandle;
pub use kv_quorum::QuorumError;
pub use kv_handle::{KvHandle, LogEntry};
pub use mesh_handle::MeshHandle;
pub use overlay_reliable::AckResult;
pub use sharding::ShardError;
pub use schema_handle::{SchemaError, SchemaHandle, SchemaPublishResult};
#[cfg(feature = "llm")]
pub use prompt::{PromptTemplate, PromptSkillError, PromptSkillHandle};
#[cfg(feature = "llm")]
pub use llm::{LlmBackend, LlmResult, LlmError, OpenAiBackend, EchoBackend};

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

/// Shared infrastructure extracted from `GossipAgent` into a single `Arc` so that
/// `ConsensusEngine`, connection handlers, and long-lived task helpers can each hold
/// a clone without creating a reference cycle back to the agent.
///
/// ## Why this exists
///
/// `GossipAgent` spawns background tasks that need access to the same infrastructure
/// the agent uses. If those tasks held an `Arc<GossipAgent>`, the agent could never be
/// dropped (cycle: agent holds `JoinSet`, tasks hold agent). `TaskCtx` breaks the cycle
/// by holding all the shared infrastructure in a separate struct; `GossipAgent` holds
/// `Arc<TaskCtx>`, and so do the tasks — but `GossipAgent` is NOT in `TaskCtx`.
///
/// ## Field groups
///
/// Fields are grouped by layer. The six typed handles (`KvHandle`, `MeshHandle`, etc.)
/// each hold `Arc<TaskCtx>` and access only their relevant subset.
///
/// | Group | Fields |
/// |---|---|
/// | Identity + config | `node_id`, `config`, `default_ttl` |
/// | Layer I — KV | `seen`, `hlc`, `gossip_txs`, `kv_state`, `wal` |
/// | Layer II — Signals | `signal_boundary`, `signal_handlers`, `reorder_buf` |
/// | Capability subsystem | `caps_advertised`, `filter_opacity_registry`, `group_roster_cache` |
/// | Service layer | `bulk_transport`, `rpc_pending` |
/// | Security | `tls`, `peer_keys` |
/// | Networking | `peers` |
/// | Lifecycle | `shutdown_tx`, `task_handles` |
///
/// ## v2 roadmap
///
/// `TaskCtx` is a known God Object — see `CLAUDE.md § Layer I/II entanglement`. The
/// planned fix is a workspace split (`mycelium-core` carrying Layers I+II, `mycelium`
/// adding Layers III+). `TaskCtx` would split into `CoreCtx` (Layers I+II only) and a
/// richer context that wraps it. That refactor is deferred until there is a real use
/// case for embedding the core without the capability / consensus layers.
pub(crate) struct TaskCtx {
    // ── Identity + config ────────────────────────────────────────────────────────
    pub(crate) node_id:          NodeId,
    /// Shared copy of the agent configuration. Available to typed handles so they
    /// can access `signal_window_secs`, `health_check_interval_secs`, `locality_path`,
    /// and `topology_policies` without borrowing `GossipAgent`.
    pub(crate) config:           Arc<GossipConfig>,
    pub(crate) default_ttl:      u8,

    // ── Layer I — KV substrate ───────────────────────────────────────────────────
    pub(crate) seen:             Arc<ShardedSeen>,
    /// Hybrid Logical Clock for causal LWW ordering. `make_gossip_update`
    /// calls `tick()` for every locally-originated write; the connection
    /// handler calls `observe()` for every incoming timestamp so the local
    /// clock dominates any remote stamp it has seen.
    pub(crate) hlc:              Arc<crate::hlc::Hlc>,
    pub(crate) gossip_txs:       Arc<[mpsc::Sender<(Bytes, u64, ForwardHint)>]>,
    pub(crate) kv_state:         Arc<KvState>,
    /// WAL handle for durable KV writes. Unset when persistence is disabled.
    /// Written once by `start()` after replay; read-only afterwards.
    pub(crate) wal: std::sync::OnceLock<Arc<crate::persistence::WalHandle>>,

    // ── Layer II — Signal mesh ───────────────────────────────────────────────────
    pub(crate) signal_boundary:  Arc<RwLock<Boundary>>,
    pub(crate) signal_handlers:  Arc<SignalHandlers>,
    /// Receiver-side causal reorder buffer for `emit_ordered` signals.
    /// `None` when `config.signal_ordered_delivery = false` (the default).
    pub(crate) reorder_buf: Option<Arc<std::sync::Mutex<crate::signal::SignalReorderBuffer>>>,

    // ── Capability subsystem ─────────────────────────────────────────────────────
    /// Set to `true` by the first tick of any `run_kv_persist_task` (capability
    /// or locality advertisement). Until this is `true`, soft-state keys have
    /// not yet been written to the local store after a restart, so `/ready`
    /// returns 503. Stored with Release; loaded with Acquire.
    pub(crate) caps_advertised: Arc<std::sync::atomic::AtomicBool>,
    /// Shared registry for the consolidated `declare_requirement` opacity watcher.
    /// A single background task reads from this instead of one task per requirement.
    pub(crate) filter_opacity_registry: Arc<capability_ops::FilterOpacityRegistry>,
    /// Short-lived cache of group membership lists keyed by group name.
    /// Invalidated generation-based: `KvState::grp_generation` is bumped (Release)
    /// whenever a `grp/` key changes; the cache reader loads it with Acquire so it
    /// never sees a stale roster after observing the new generation value.
    pub(crate) group_roster_cache: RosterCache,

    // ── Service layer ────────────────────────────────────────────────────────────
    /// Bulk-transport adapter: staging map, HTTP port, pooled HTTP client.
    pub(crate) bulk_transport: Arc<bulk::BulkTransport>,
    /// In-flight RPC/bulk correlation map for O(1) reply dispatch.
    /// Key: correlation nonce (first 8 bytes of result payload, LE).
    /// The connection handler's fast-path removes the entry and fires the
    /// oneshot instead of fanning out through signal_handlers.
    pub(crate) rpc_pending: Arc<std::sync::Mutex<std::collections::HashMap<u64, tokio::sync::oneshot::Sender<crate::signal::Signal>>>>,

    // ── Security ─────────────────────────────────────────────────────────────────
    /// TLS context (server + client configs + signing key). Unset when the
    /// `tls` feature is disabled or when `GossipConfig::tls` is `None`.
    /// Written once by `start()` before any task is spawned; read-only afterwards.
    pub(crate) tls: std::sync::OnceLock<Arc<crate::tls::NodeTls>>,
    /// Map from peer NodeId → 32-byte Ed25519 public key, populated from two
    /// sources: (a) the mTLS handshake cert, (b) `sys/identity/` KV entries
    /// gossiped by peers. Used to verify signed consensus messages.
    #[cfg_attr(not(feature = "tls"), allow(dead_code))]
    pub(crate) peer_keys: Arc<papaya::HashMap<NodeId, [u8; 32]>>,

    // ── Networking ───────────────────────────────────────────────────────────────
    /// Live peer table shared with the HTTP gateway for peer-count-based quorum sizing.
    pub(crate) peers: Arc<papaya::HashMap<NodeId, std::time::Instant>>,

    // ── Lifecycle ────────────────────────────────────────────────────────────────
    /// Shutdown broadcast — sending `true` cancels all background tasks.
    pub(crate) shutdown_tx: Arc<watch::Sender<bool>>,
    /// All spawned background tasks. Reaping is automatic via `JoinSet`.
    pub(crate) task_handles: Arc<std::sync::Mutex<JoinSet<()>>>,
}

impl TaskCtx {
    pub(crate) fn spawn_task<F>(&self, fut: F)
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        self.task_handles.lock().unwrap_or_else(|e| e.into_inner()).spawn(fut);
    }
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
/// ## Typed sub-handles
///
/// All domain operations are accessed through typed handles returned by the
/// six accessor methods below. Each handle is `Clone + Send + Sync` and can be
/// stored, moved across tasks, or captured in closures.
///
/// | Accessor | Handle | Domain |
/// |---|---|---|
/// | `agent.kv()` | [`KvHandle`] | KV store — Layer I |
/// | `agent.mesh()` | [`MeshHandle`] | Signal mesh — Layer II |
/// | `agent.consensus()` | [`ConsensusHandle`] | Consensus — Layer III |
/// | `agent.service()` | [`ServiceHandle`] | RPC / bulk / scatter / mailbox / sharding |
/// | `agent.capabilities()` | [`CapabilitiesHandle`] | Capability / requirement / wiring / demand |
/// | `agent.schemas()` | [`SchemaHandle`] | Schema registry |
///
/// ## Lifecycle methods (directly on `GossipAgent`)
///
/// `new`, `with_http_routes`, `start`, `shutdown`, `shutdown_with_timeout`,
/// `node_id`, `peers`, `groups`, `signal_window`, `system_stats`, `is_ready`,
/// `peer_drop_counts`, `agent_state_machine`.
///
/// Items not in this index are private implementation details (`pub(super)` or
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
    /// Bundled KV-path state (store + subscriptions + prefix_index + hash_acc +
    /// dropped_frames + max_store_entries). Access fields via `self.kv_state.x`.
    pub(super) kv_state: Arc<KvState>,
    /// Infrastructure bundle shared with `ConsensusEngine` and long-lived task helpers.
    /// Access fields via `self.task_ctx.x`.
    pub(super) task_ctx: Arc<TaskCtx>,
    /// Application-supplied axum routes merged into the embedded gateway at [`start`](Self::start) time.
    /// Taken once by the HTTP server task; subsequent calls to `with_http_routes` are no-ops after start.
    #[cfg(feature = "gateway")]
    pub(super) extra_routes: std::sync::Mutex<Option<axum::Router>>,
    /// LLM skill registry: maps `"{ns}/{name}"` → backend.
    /// Template is read from KV on every invocation (not cached here).
    #[cfg(feature = "llm")]
    pub(crate) llm_skills: llm::LlmSkillRegistry,
}

impl GossipAgent {
    // ── Sub-handle accessors ──────────────────────────────────────────────────

    /// Returns a typed handle for KV store operations (Layer I).
    ///
    /// Zero-cost: clones one `Arc` per call. The handle is `Clone + Send + Sync`
    /// and can be stored, moved across tasks, or captured in closures.
    ///
    /// ```ignore
    /// let kv = agent.kv();
    /// kv.set("load/self", Bytes::from_static(b"queue=0"));
    /// let val = kv.get("load/self");
    /// ```
    pub fn kv(&self) -> KvHandle {
        KvHandle { ctx: Arc::clone(&self.task_ctx) }
    }

    /// Returns a typed handle for signal mesh operations (Layer II).
    ///
    /// Zero-cost: clones one `Arc` per call. The handle is `Clone + Send + Sync`
    /// and can be stored, moved across tasks, or captured in closures.
    ///
    /// ```ignore
    /// let mesh = agent.mesh();
    /// mesh.join_group("nlp");
    /// mesh.emit(signal_kind::INVOKE, SignalScope::Group("nlp".into()), Bytes::new());
    /// let mut rx = mesh.signal_rx(signal_kind::INVOKE);
    /// ```
    pub fn mesh(&self) -> MeshHandle {
        MeshHandle { ctx: Arc::clone(&self.task_ctx) }
    }

    /// Returns a typed handle for schema registry operations.
    ///
    /// Zero-cost: clones one `Arc` per call. The handle can be stored and moved
    /// across tasks independently of the agent.
    ///
    /// ```ignore
    /// let schemas = agent.schemas();
    /// schemas.publish_schema("acme/v1", MY_SCHEMA_JSON).await?;
    /// let bytes = schemas.get_schema("acme/v1");
    /// ```
    pub fn schemas(&self) -> SchemaHandle {
        SchemaHandle { ctx: Arc::clone(&self.task_ctx) }
    }

    /// Returns a typed handle for consensus operations (Layer III).
    ///
    /// Zero-cost: clones one `Arc` per call. The handle is `Clone + Send + Sync`
    /// and can be stored, moved across tasks, or captured in closures.
    ///
    /// ```ignore
    /// let c = agent.consensus();
    /// c.consistent_set("cfg/x", val).await?;
    /// let _listener = c.start_consensus_listener(ConsensusConfig::default());
    /// ```
    pub fn consensus(&self) -> ConsensusHandle {
        ConsensusHandle { ctx: Arc::clone(&self.task_ctx) }
    }

    /// Returns a typed handle for service / communication operations.
    ///
    /// Covers RPC, bulk transfer, scatter-gather, reliable delivery,
    /// persistent mailboxes, and consistent-hash sharding.
    ///
    /// Zero-cost: clones one `Arc` per call.
    pub fn service(&self) -> ServiceHandle {
        ServiceHandle { ctx: Arc::clone(&self.task_ctx) }
    }

    /// Returns a typed handle for capability, opacity, wiring, and demand operations.
    ///
    /// Covers capability advertisement, requirement declaration, wiring resolution,
    /// demand tracking, emergent group definitions, and the load pheromone trail API.
    ///
    /// Zero-cost: clones one `Arc` per call.
    pub fn capabilities(&self) -> CapabilitiesHandle {
        CapabilitiesHandle { ctx: Arc::clone(&self.task_ctx) }
    }

    // ── Signal window helper ──────────────────────────────────────────────────

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
    pub(super) fn task_handles_lock(&self) -> std::sync::MutexGuard<'_, JoinSet<()>> {
        self.task_ctx.task_handles.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Spawns `fut` onto the Tokio runtime and tracks it in the task-handles `JoinSet`.
    /// Replaces the `tokio::spawn` + `task_handles_lock().push(handle)` pattern so
    /// completed tasks are automatically reaped by the `JoinSet` rather than accumulating.
    pub(super) fn spawn_task<F>(&self, fut: F)
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        self.task_ctx.spawn_task(fut);
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
        let (shutdown_tx_inner, _) = watch::channel(false);
        let shutdown_tx_arc = Arc::new(shutdown_tx_inner);
        let task_handles_arc: Arc<std::sync::Mutex<JoinSet<()>>> =
            Arc::new(std::sync::Mutex::new(JoinSet::new()));
        let group_roster_cache: RosterCache = Arc::new(papaya::HashMap::new());
        let mut bootstrap_peers = config.bootstrap_peers.clone();
        bootstrap_peers.retain(|p| p != &node_id);
        let bootstrap_peers: Arc<[NodeId]> = bootstrap_peers.into();
        let (peer_list_tx, _) = watch::channel(Arc::clone(&bootstrap_peers));
        let shard_alive = (0..n_shards)
            .map(|_| Arc::new(AtomicBool::new(false)))
            .collect();
        let seen_shards = n_shards.max(16);

        let signal_window = std::time::Duration::from_secs(config.signal_window_secs);
        let kv_state      = KvState::new(config.max_store_entries);
        let default_ttl   = config.default_ttl;
        let gossip_txs: Arc<[mpsc::Sender<(Bytes, u64, ForwardHint)>]> = gossip_txs_vec.into();
        let peers_arc: Arc<papaya::HashMap<NodeId, std::time::Instant>> = Arc::new(papaya::HashMap::new());
        let config_arc = Arc::new(config.clone());
        let task_ctx = Arc::new(TaskCtx {
            node_id:         node_id.clone(),
            config:          Arc::clone(&config_arc),
            seen:            Arc::new(ShardedSeen::new(seen_shards)),
            hlc:             Arc::new(crate::hlc::Hlc::new()),
            signal_boundary: Arc::new(RwLock::new(Boundary::new(node_id.clone()))),
            signal_handlers: Arc::new(SignalHandlers::new(signal_window)),
            gossip_txs,
            default_ttl,
            kv_state:        Arc::clone(&kv_state),
            wal:             std::sync::OnceLock::new(),
            caps_advertised: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            bulk_transport:  Arc::new(bulk::BulkTransport::new(
                config.http_port.unwrap_or(0),
                std::time::Duration::from_secs(config.bulk_fetch_timeout_secs),
            )),
            rpc_pending: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
            tls: std::sync::OnceLock::new(),
            peer_keys: Arc::new(papaya::HashMap::new()),
            peers: Arc::clone(&peers_arc),
            filter_opacity_registry: Arc::new(capability_ops::FilterOpacityRegistry::new()),
            reorder_buf: if config.signal_ordered_delivery {
                Some(Arc::new(std::sync::Mutex::new(
                    crate::signal::SignalReorderBuffer::new(
                        std::time::Duration::from_millis(config.signal_reorder_max_hold_ms),
                        config.signal_reorder_max_depth,
                    )
                )))
            } else {
                None
            },
            shutdown_tx:         Arc::clone(&shutdown_tx_arc),
            task_handles:        Arc::clone(&task_handles_arc),
            group_roster_cache:  Arc::clone(&group_roster_cache),
        });

        Self {
            node_id,
            config,
            peers: peers_arc,
            bootstrap_peers,
            peer_list_tx,
            gossip_rxs: std::sync::Mutex::new(Some(gossip_rxs_inner)),
            peer_writers: Arc::new(papaya::HashMap::new()),
            live_entries: Arc::new(AtomicUsize::new(0)),
            state: AtomicU8::new(AgentState::Idle as u8),
            shutdown_tx: shutdown_tx_arc,
            shard_alive,
            listener_alive: Arc::new(AtomicUsize::new(0)),
            health_monitor_alive: Arc::new(AtomicBool::new(false)),
            gc_alive: Arc::new(AtomicBool::new(false)),
            kv_state,
            task_ctx,
            #[cfg(feature = "gateway")]
            extra_routes: std::sync::Mutex::new(None),
            #[cfg(feature = "llm")]
            llm_skills: std::sync::Arc::new(dashmap::DashMap::new()),
        }
    }
}

#[cfg(feature = "gateway")]
impl GossipAgent {
    /// Registers application-level axum routes to be merged into the embedded HTTP gateway.
    ///
    /// Call this after [`new`](Self::new) and before [`start`](Self::start).
    /// The supplied `routes` must already have their state attached (call `.with_state(…)`
    /// on them before passing here so they are `Router<()>`). Routes registered after
    /// `start` is called are silently ignored.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let extra = axum::Router::new()
    ///     .route("/my-endpoint", axum::routing::get(my_handler))
    ///     .with_state(my_state);
    /// agent.with_http_routes(extra);
    /// agent.start().await?;
    /// ```
    pub fn with_http_routes(&self, routes: axum::Router) {
        *self.extra_routes.lock().unwrap_or_else(|e| e.into_inner()) = Some(routes);
    }

    /// Registers the A2A (Agent-to-Agent protocol) endpoints on this node's HTTP gateway.
    ///
    /// Adds `GET /.well-known/agent.json` (discovery) and `POST /a2a` (JSON-RPC) to the
    /// embedded HTTP server. The AgentCard is built dynamically from the live `cap/` KV
    /// prefix so skills become visible as capabilities are advertised.
    ///
    /// Must be called before [`start`](Self::start).
    ///
    /// Requires the `a2a` cargo feature.
    #[cfg(feature = "a2a")]
    pub fn with_a2a(self) -> Self {
        let ctx   = Arc::clone(&self.task_ctx);
        let tasks = Arc::new(papaya::HashMap::<String, a2a::A2aTask>::new());
        a2a::spawn_cleanup(Arc::clone(&tasks));
        let router = a2a::a2a_router_full(ctx, tasks);
        self.with_http_routes(router);
        self
    }
}

#[cfg(feature = "llm")]
impl GossipAgent {
    /// Publish a prompt template to the cluster KV and register this node as a
    /// provider. The skill is discoverable via the capability ring immediately.
    /// Dropping the returned handle retracts the capability and removes the backend.
    pub async fn register_prompt_skill(
        &self,
        ns:       &str,
        name:     &str,
        template: prompt::PromptTemplate,
        backend:  std::sync::Arc<dyn llm::LlmBackend>,
    ) -> Result<prompt::PromptSkillHandle, prompt::PromptSkillError> {
        use crate::capability::Capability;
        use crate::signal::kv_ns;
        use std::time::Duration;

        // 1. Write template to KV — configuration, not heartbeat.
        let kv_key = format!("{}{}/{}", kv_ns::PROMPTS, ns, name);
        let bytes  = serde_json::to_vec(&template)
            .map_err(|e| prompt::PromptSkillError::LlmError(e.to_string()))?;
        kv_set(&self.task_ctx, Arc::from(kv_key.as_str()), bytes::Bytes::from(bytes));

        // 2. Advertise capability — presence heartbeat, evaporates when node dies.
        let cap_handle = self.capabilities().advertise_capability(
            Capability::new(ns, name),
            Duration::from_secs(30),
        );

        // 3. Register backend in the shared registry.
        let skill_id = format!("{}/{}", ns, name);
        let was_empty = self.llm_skills.is_empty();
        self.llm_skills.insert(skill_id.clone(), backend);

        // 4. Spawn dispatch loop on first registration.
        if was_empty {
            llm::spawn_llm_dispatch_loop(self, std::sync::Arc::clone(&self.llm_skills));
        }

        // 5. Create cancellation channel for this skill's registry entry.
        let (cancel_tx, cancel_rx) = tokio::sync::oneshot::channel::<()>();
        let registry  = std::sync::Arc::clone(&self.llm_skills);
        let skill_id2 = skill_id.clone();
        tokio::spawn(async move {
            let _ = cancel_rx.await;
            registry.remove(&skill_id2);
        });

        Ok(prompt::PromptSkillHandle {
            _cap:            cap_handle,
            _handler_cancel: cancel_tx,
        })
    }

    /// Call a prompt skill. Resolves a provider via the capability ring,
    /// sends an RPC `llm.invoke` call, returns the LLM's output string.
    pub async fn call_prompt_skill(
        &self,
        ns:      &str,
        name:    &str,
        input:   &str,
        context: std::collections::HashMap<String, String>,
        timeout: std::time::Duration,
    ) -> Result<String, prompt::PromptSkillError> {
        use crate::capability::CapFilter;
        use crate::signal::signal_kind;

        let providers = self.capabilities().resolve(&CapFilter::new(ns, name));
        let (target, _) = providers.into_iter().next()
            .ok_or_else(|| prompt::PromptSkillError::NoProvider {
                ns: ns.into(), name: name.into(),
            })?;

        let req = serde_json::json!({
            "prompt":  format!("{}/{}", ns, name),
            "input":   input,
            "context": context,
        });
        let payload = bytes::Bytes::from(req.to_string().into_bytes());

        let reply = rpc::rpc_call_ctx(
            &self.task_ctx,
            target,
            std::sync::Arc::from(signal_kind::LLM_INVOKE),
            payload,
            timeout,
        ).await?;

        // Parse response — may be success or error
        let v: serde_json::Value = serde_json::from_slice(&reply)
            .map_err(|e| prompt::PromptSkillError::LlmError(e.to_string()))?;
        if let Some(err) = v.get("error").and_then(|e| e.as_str()) {
            let detail = v.get("detail").and_then(|d| d.as_str()).unwrap_or("");
            return Err(prompt::PromptSkillError::LlmError(format!("{}: {}", err, detail)));
        }
        v["output"].as_str()
            .map(|s| s.to_owned())
            .ok_or_else(|| prompt::PromptSkillError::LlmError("missing output field".into()))
    }

    /// Update a prompt template in the cluster KV. All serving nodes pick up
    /// the change on their next invocation (they read from KV, not a local cache).
    /// Does not require holding the original `PromptSkillHandle`.
    pub fn update_prompt(
        &self,
        ns:       &str,
        name:     &str,
        template: prompt::PromptTemplate,
    ) -> Result<(), prompt::PromptSkillError> {
        use crate::signal::kv_ns;
        let kv_key = format!("{}{}/{}", kv_ns::PROMPTS, ns, name);
        let bytes  = serde_json::to_vec(&template)
            .map_err(|e| prompt::PromptSkillError::LlmError(e.to_string()))?;
        kv_set(&self.task_ctx, Arc::from(kv_key.as_str()), bytes::Bytes::from(bytes));
        Ok(())
    }

    /// Retrieve the current prompt template from the local KV snapshot.
    /// Synchronous — reads in-memory state, same as `resolve()`.
    pub fn get_prompt(&self, ns: &str, name: &str) -> Option<prompt::PromptTemplate> {
        use crate::signal::kv_ns;
        let key = format!("{}{}/{}", kv_ns::PROMPTS, ns, name);
        let bytes = self.kv_state.store.pin().get(key.as_str())
            .and_then(|e| e.data.clone())?;
        serde_json::from_slice(&bytes).ok()
    }

    /// List all prompt skills currently visible in the local KV snapshot.
    pub fn list_prompts(&self) -> Vec<(String, String)> {
        use crate::signal::kv_ns;
        kv_scan_prefix(&self.task_ctx, kv_ns::PROMPTS)
            .into_iter()
            .filter_map(|(k, _)| {
                let rest = k.strip_prefix(kv_ns::PROMPTS)?;
                let mut parts = rest.splitn(2, '/');
                let ns   = parts.next()?.to_owned();
                let name = parts.next()?.to_owned();
                if name.is_empty() { return None; }
                Some((ns, name))
            })
            .collect()
    }

    /// Tombstone the prompt template KV entry. The skill becomes unreachable
    /// once all serving nodes' capability entries expire (≤30s). Use when
    /// permanently retiring a skill; for a graceful drain, drop all
    /// `PromptSkillHandle`s first so capability entries evaporate naturally.
    pub fn delete_prompt(&self, ns: &str, name: &str) {
        use crate::signal::kv_ns;
        let key = format!("{}{}/{}", kv_ns::PROMPTS, ns, name);
        kv_delete(&self.task_ctx, Arc::from(key.as_str()));
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
