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
        atomic::{AtomicBool, AtomicU8, AtomicU64, AtomicUsize, Ordering},
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
#[cfg(feature = "llm")]
mod llm_handle;
mod mcp_handle;
#[cfg(feature = "compliance")]
mod rbac;

#[allow(unused_imports)]
pub(crate) use bulk::BulkTransport;
#[allow(unused_imports)]
pub(crate) use capability_ops::FilterOpacityRegistry;
pub(crate) use helpers::emit_signal;
pub(crate) use helpers::emit_signal_async;
pub(crate) use helpers::make_gossip_update;
pub(crate) use opacity::is_self_opaque;
#[cfg(feature = "gateway")]
pub use mcp::McpClientHandle;
pub use mcp::{McpError, McpToolHandle};
pub use rpc::{RpcError, RpcRequest, RpcRequestRx};
pub use state_machine::{AgentPolicy, ExecutionState, AgentStateMachine, PolicyViolation};
pub use scatter::{ScatterError, ScatterResult};
pub use bulk::{BulkError, BulkServeHandle};
pub use mailbox::{MailboxHandle, MeshEvent};
pub use overlay_consistent::{ConsistencyError, LockGuard};
pub use consensus_handle::ConsensusHandle;
pub use service_handle::ServiceHandle;
pub use capability_handle::CapabilitiesHandle;
#[cfg(feature = "compliance")]
pub use rbac::{role_key, RoleClaim, SignedRoleClaim, ROLE_PREFIX};
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
#[cfg(feature = "llm")]
pub use llm_handle::LlmHandle;
pub use mcp_handle::McpHandle;

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
    /// Times an Individual-scoped frame (RPC request/response, consensus vote)
    /// had no direct route and fell back to flooding — or, with zero peers,
    /// was dropped outright. Correct behaviour, but non-zero under steady
    /// state means RPC-heavy pairs lack direct peering and pay relay latency.
    pub individual_flood_fallbacks: u64,
    /// Number of background tasks currently tracked in the agent's `JoinSet`.
    ///
    /// Steady-state expected count (after `start()` completes):
    /// - **Core** (always): GC, health-monitor, anti-entropy, WAL-flush, signal-
    ///   reorder-buffer, capability-heartbeat, group-member-sync = **7**
    /// - **+N per gossip shard** (default 4 shards): writer + listener pair = **+8**
    /// - **+1 gateway** (when `gateway` feature enabled): Axum HTTP server = **+1**
    /// - **+1 per connected peer**: per-peer writer task
    /// - **+1 per live RPC/bulk call** while in flight
    ///
    /// Typical baseline on a 3-node cluster: ~17–20. A value growing unboundedly
    /// indicates task leaks; consult `task_handles` diagnostics.
    ///
    /// Note: per-request `bulk_serve` handler tasks are NOT included here because
    /// they are spawned outside the `JoinSet`; their count is in `active_bulk_handlers`.
    pub task_count: usize,
    /// Number of `bulk_serve` per-request handler tasks currently executing.
    ///
    /// These tasks are spawned outside the tracked `JoinSet` and are bounded by
    /// `GossipConfig::max_concurrent_bulk_handlers` (default 64). A value
    /// at the configured ceiling means the semaphore is dropping requests — raise
    /// `max_concurrent_bulk_handlers` or reduce the bulk call rate.
    pub active_bulk_handlers: u64,
    /// Cumulative commit-conflict detections by this node's consensus listener.
    ///
    /// Incremented when a `COMMIT` arrives carrying a **different** value for a
    /// slot whose existing commitment is still live (slots are commit-once;
    /// epoch-leased slots reopen only after lease expiry). Each detection means
    /// a raced double-commit, a buggy proposer, or a forged commit message —
    /// the listener refuses to endorse the conflicting value and logs a `warn!`.
    ///
    /// **Any non-zero value warrants investigation.** Namespace ownership of
    /// `consensus/` is promise-strength (convention, not mechanism): this
    /// counter is the tripwire that makes violations legible. Requires
    /// `start_consensus_listener` — nodes without a listener do not detect.
    pub commit_conflicts: u64,
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

    // ── LLM skill registry ───────────────────────────────────────────────────────
    /// Maps `"{ns}/{name}"` → LLM backend. Template is read from KV on every
    /// invocation; only the backend reference is cached here.
    #[cfg(feature = "llm")]
    pub(crate) llm_skills: llm::LlmSkillRegistry,
    /// First-registration gate for the `llm.invoke` dispatch loop. `swap(true)`
    /// so exactly one loop spawns even when two `register_prompt_skill` calls
    /// race (a `was_empty` check-then-act could spawn two loops, each receiving
    /// every invoke signal → duplicate RPC responses).
    #[cfg(feature = "llm")]
    pub(crate) llm_dispatch_spawned: std::sync::atomic::AtomicBool,

    // ── Service layer ────────────────────────────────────────────────────────────
    /// Bulk-transport adapter: staging map, HTTP port, pooled HTTP client.
    pub(crate) bulk_transport: Arc<bulk::BulkTransport>,
    /// In-flight RPC/bulk correlation map for O(1) reply dispatch.
    /// Key: correlation nonce (first 8 bytes of result payload, LE).
    /// The connection handler's fast-path removes the entry and fires the
    /// oneshot instead of fanning out through signal_handlers.
    pub(crate) rpc_pending: Arc<std::sync::Mutex<std::collections::HashMap<u64, tokio::sync::oneshot::Sender<crate::signal::Signal>>>>,

    // ── Layer III — Consensus ────────────────────────────────────────────────────
    /// Cumulative commit-conflict detections (see `SystemStats::commit_conflicts`).
    /// Incremented by the consensus listener's tripwire; Relaxed ordering —
    /// purely diagnostic, surfaced via `system_stats()` and `/stats`.
    pub(crate) commit_conflicts: Arc<AtomicU64>,

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
/// accessor methods below. Each handle is `Clone + Send + Sync` and can be
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
/// | `agent.mcp()` | [`McpHandle`] | MCP tool bridge (server + client roles) |
/// | `agent.llm()` | [`LlmHandle`] | LLM prompt skills (`llm` feature) |
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
}

impl GossipAgent {
    // ── Sub-handle accessors ──────────────────────────────────────────────────

    /// Returns a typed handle for KV store operations (Layer I).
    ///
    /// Zero-cost: clones one `Arc` per call. The handle is `Clone + Send + Sync`
    /// and can be stored, moved across tasks, or captured in closures.
    ///
    /// ```no_run
    /// # use mycelium::{GossipAgent, GossipConfig, NodeId};
    /// # use bytes::Bytes;
    /// # let agent = GossipAgent::new(NodeId::new("127.0.0.1", 7000).unwrap(), GossipConfig::default());
    /// let kv = agent.kv();
    /// let _ = kv.set("load/self", Bytes::from_static(b"queue=0"));
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
    /// ```no_run
    /// # use mycelium::{GossipAgent, GossipConfig, NodeId, SignalScope, signal_kind};
    /// # use bytes::Bytes;
    /// # let agent = GossipAgent::new(NodeId::new("127.0.0.1", 7000).unwrap(), GossipConfig::default());
    /// let mesh = agent.mesh();
    /// mesh.join_group("nlp");
    /// let _ = mesh.emit(signal_kind::INVOKE, SignalScope::Group("nlp".into()), Bytes::new());
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
    /// ```no_run
    /// # use mycelium::{GossipAgent, GossipConfig, NodeId};
    /// # async fn example(agent: &GossipAgent) -> Result<(), Box<dyn std::error::Error>> {
    /// let schemas = agent.schemas();
    /// schemas.publish_schema("acme/v1", br#"{"type":"object"}"#).await?;
    /// let bytes = schemas.get_schema("acme/v1");
    /// # Ok(()) }
    /// ```
    pub fn schemas(&self) -> SchemaHandle {
        SchemaHandle { ctx: Arc::clone(&self.task_ctx) }
    }

    /// Returns a typed handle for consensus operations (Layer III).
    ///
    /// Zero-cost: clones one `Arc` per call. The handle is `Clone + Send + Sync`
    /// and can be stored, moved across tasks, or captured in closures.
    ///
    /// ```no_run
    /// # use mycelium::{GossipAgent, GossipConfig, NodeId, ConsensusConfig};
    /// # use bytes::Bytes;
    /// # async fn example(agent: &GossipAgent) -> Result<(), Box<dyn std::error::Error>> {
    /// let c = agent.consensus();
    /// c.consistent_set("cfg/x", Bytes::from_static(b"v")).await?;
    /// let _listener = c.start_consensus_listener(ConsensusConfig::default());
    /// # Ok(()) }
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

    /// Returns a typed handle for MCP tool registration and client bridging.
    ///
    /// Covers server-role tool registration (`register_mcp_tool`) and client-role
    /// bridging of external MCP servers into the Mycelium mesh (`connect_mcp_server`).
    ///
    /// Zero-cost: clones one `Arc` per call.
    pub fn mcp(&self) -> McpHandle {
        McpHandle { ctx: Arc::clone(&self.task_ctx) }
    }

    /// Returns a typed handle for LLM prompt-skill operations.
    ///
    /// Covers prompt skill registration, invocation, template management,
    /// and the node-local LLM backend registry.
    ///
    /// Zero-cost: clones one `Arc` per call.
    #[cfg(feature = "llm")]
    pub fn llm(&self) -> LlmHandle {
        LlmHandle { ctx: Arc::clone(&self.task_ctx) }
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
            hlc:             Arc::new(crate::hlc::Hlc::with_max_drift(config.max_clock_drift_ms)),
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
                config.max_concurrent_bulk_handlers,
            )),
            rpc_pending: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
            commit_conflicts: Arc::new(AtomicU64::new(0)),
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
            #[cfg(feature = "llm")]
            llm_skills: std::sync::Arc::new(papaya::HashMap::new()),
            #[cfg(feature = "llm")]
            llm_dispatch_spawned: std::sync::atomic::AtomicBool::new(false),
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
    /// May be called multiple times; routers are **merged**, so application
    /// routes compose with [`with_a2a`](Self::with_a2a) and other adapters
    /// rather than replacing them.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use mycelium::{GossipAgent, GossipConfig, NodeId};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let agent = GossipAgent::new(NodeId::new("127.0.0.1", 7000)?, GossipConfig::default());
    /// async fn my_handler() -> &'static str { "ok" }
    /// // Attach state with `.with_state(…)` before passing so the router is `Router<()>`.
    /// let extra = axum::Router::new()
    ///     .route("/my-endpoint", axum::routing::get(my_handler));
    /// agent.with_http_routes(extra);
    /// agent.start().await?;
    /// # Ok(()) }
    /// ```
    pub fn with_http_routes(&self, routes: axum::Router) {
        // Merge, don't replace: callers compose routers (`with_a2a()` +
        // application routes) and a last-caller-wins slot silently dropped
        // every earlier registration — skillrunner's management dashboard
        // erased the A2A endpoints for as long as both were enabled.
        let mut slot = self.extra_routes.lock().unwrap_or_else(|e| e.into_inner());
        *slot = Some(match slot.take() {
            Some(existing) => existing.merge(routes),
            None           => routes,
        });
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

/// RBAC — signed node-role advertisement + verified read (WS1; `compliance` feature).
#[cfg(feature = "compliance")]
impl GossipAgent {
    /// Advertise this node's roles + data-classification clearance as a signed
    /// claim at `sys/role/{node}`. Requires the `tls` identity (roles are
    /// Ed25519-signed); returns [`GossipError::InvalidField`] if `GossipConfig::tls`
    /// was not set.
    ///
    /// One-shot write — the signed claim persists and anti-entropy-syncs like any
    /// KV entry; re-call to update. (Periodic re-advertisement / evaporation is a
    /// later refinement.)
    pub fn advertise_roles(
        &self,
        roles: impl IntoIterator<Item = Arc<str>>,
        clearance: u8,
    ) -> Result<(), crate::error::GossipError> {
        let tls = self.task_ctx.tls.get().ok_or(crate::error::GossipError::InvalidField {
            field:  "tls",
            reason: "role advertisement requires the tls identity (set GossipConfig::tls)".into(),
        })?;
        let issued_at_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let claim  = rbac::RoleClaim::new(self.node_id.clone(), roles, clearance, issued_at_ms);
        let signed = rbac::SignedRoleClaim::sign(claim, &tls.signing_key);
        // Local + WAL write is guaranteed; a `false` here only means this gossip
        // tick's channel was full — the entry still anti-entropy-syncs and a
        // re-advertise retries, so a dropped dispatch is not an error.
        let _ = self.kv().set(rbac::role_key(&self.node_id), signed.encode());
        Ok(())
    }

    /// Read and **verify** `node`'s role claim. Returns the claim only if its
    /// signature checks out against `node`'s identity key (from `peer_keys`, or
    /// this node's own key when `node` is self). A forged or mis-attributed
    /// `sys/role/` write reads back as `None` — detection, not prevention.
    pub fn roles_of(&self, node: &NodeId) -> Option<rbac::RoleClaim> {
        let bytes = self.kv().get(&rbac::role_key(node))?;
        let vk    = self.verifying_key_for(node)?;
        rbac::verified_roles(&bytes, node, &vk)
    }

    /// 32-byte Ed25519 verifying key for `node`: this node's own key when `node`
    /// is self, otherwise the key gossiped to `peer_keys` (from `sys/identity/`).
    fn verifying_key_for(&self, node: &NodeId) -> Option<[u8; 32]> {
        if node == &self.node_id {
            return self.task_ctx.tls.get().map(|t| t.signing_key.verifying_key().to_bytes());
        }
        self.task_ctx.peer_keys.pin().get(node).copied()
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

#[cfg(all(test, feature = "compliance"))]
mod rbac_agent_tests {
    use super::*;
    use crate::config::GossipConfig;

    #[test]
    fn advertise_roles_requires_tls_identity() {
        let node = NodeId::new("127.0.0.1", 7400).unwrap();
        let agent = GossipAgent::new(node.clone(), GossipConfig::default());
        // No tls configured → cannot sign → typed error, never a panic.
        let err = agent.advertise_roles(["admin".into()], 3).unwrap_err();
        assert!(matches!(err, crate::error::GossipError::InvalidField { field: "tls", .. }));
        // Nothing written, so nothing verifies back.
        assert!(agent.roles_of(&node).is_none());
    }
}
