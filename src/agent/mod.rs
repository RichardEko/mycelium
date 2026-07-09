use crate::config::GossipConfig;
use crate::framing::ForwardHint;
use crate::node_id::NodeId;
use crate::seen::ShardedSeen;
use crate::signal::{Boundary, Signal, SignalHandlers, signal_kind};
use crate::store::KvState;
use crate::writer::WriterEntry;
use mycelium_core::CoreCtx;
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
mod kv_quorum_ext;
// kv_handle + mesh_handle moved to mycelium-core (v2 M3); the GossipAgent-driven
// tests stay here.
#[cfg(test)]
mod kv_handle_tests;
#[cfg(test)]
mod mesh_handle_tests;
#[cfg(feature = "consensus")]
mod consensus_handle;
#[cfg(feature = "consensus")]
mod overlay_consistent;
mod overlay_reliable;
mod rpc;
#[cfg(feature = "gateway")]
mod http;
mod mcp;
mod opacity;
#[cfg(feature = "consensus")]
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
pub(crate) mod intent;
mod cluster_tuner;
mod tuning_governor;
pub(crate) mod timing_governor;
mod membership_governor;
// The diagnostics module's *snapshot/view* surface (fleet snapshot, ViewConfidence, the `explain`
// reader) is consumed only by the `gateway` HTTP handlers and the `metrics` emitter; its detectors
// are used unconditionally by the loop. In a build with neither feature (e.g. `--no-default-features`
// or the wasm-host's minimal embed) that surface is legitimately dead — allow it there only, so
// genuine dead code is still caught under normal features.
#[cfg_attr(not(any(feature = "gateway", feature = "metrics")), allow(dead_code))]
pub(crate) mod emergent;
mod sharding;
mod shard_ops;
mod service_handle;
mod capability_handle;
// schema_handle moved to mycelium-core (v2 M3); the GossipAgent-driven tests stay here.
#[cfg(test)]
mod schema_handle_tests;
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
#[cfg(feature = "compliance")]
pub(crate) mod audit;
#[cfg(feature = "compliance")]
pub(crate) mod revocation;
#[cfg(feature = "compliance")]
pub(crate) mod transparency;
#[cfg(feature = "compliance")]
pub(crate) mod capauthz;
#[cfg(feature = "compliance")]
pub(crate) mod oidc;

#[allow(unused_imports)]
pub(crate) use bulk::BulkTransport;
#[allow(unused_imports)]
pub(crate) use capability_ops::FilterOpacityRegistry;
pub(crate) use helpers::emit_signal;
#[cfg(feature = "consensus")]
pub(crate) use helpers::emit_signal_async;
#[cfg(feature = "consensus")]
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
pub use cluster_tuner::{accept_all, clamped, reject_all, ConfigPolicy, CONFIG_PREFIX};
pub use tuning_governor::{
    GovernIntent, GovernorSnapshot, HotParam, ParamDirective, ParamSnapshot, Ratchet,
    GOVERN_FLEET_KEY, GOVERN_INTENT_TTL_MS,
};
#[cfg(test)]
pub(crate) use tuning_governor::TuningGovernor;
pub use membership_governor::{MembershipAction, MembershipIntent, MEMBERSHIP_INTENT_TTL_MS, MEMBERSHIP_PREFIX};
// Legible Emergence — fleet diagnostics as data (localize · explain · diagnose). `GroupStatus` is
// reached via `FleetSnapshot.governed_groups` (the bare name is already the mesh-dashboard type).
pub use emergent::{
    FleetDiagnosis, FleetSnapshot, Finding, Severity, StoreConvergence, ThrottleEdge, ViewConfidence,
};
#[cfg(feature = "consensus")]
pub use overlay_consistent::{ConsistencyError, LockGuard};
#[cfg(feature = "consensus")]
pub use consensus_handle::ConsensusHandle;
pub use service_handle::ServiceHandle;
pub use capability_handle::CapabilitiesHandle;
#[cfg(feature = "compliance")]
pub use rbac::{role_key, RoleClaim, SignedRoleClaim, ROLE_PREFIX};
#[cfg(feature = "compliance")]
pub use audit::{
    audit_key, audit_stream_prefix, verify_chain, verify_chain_keys, verify_stream_from_genesis,
    AuditAction, AuditOutcome, AuditRecord, AuditVerifyError, SignedAuditRecord, AUDIT_PREFIX,
};
#[cfg(feature = "compliance")]
pub use revocation::{RevocationEvent, SignedRevocation, REVOCATION_PREFIX};
// WS-D / D2: the client-side revocation inclusion-proof verifier + Merkle primitives, so an SDK or
// external auditor can check a `/gateway/transparency` proof without trusting the server.
#[cfg(feature = "compliance")]
pub use transparency::{leaf_hash, merkle_root, verify_inclusion, ProofStep};
#[cfg(feature = "compliance")]
pub use oidc::OidcConfig;
pub use kv_quorum::QuorumError;
pub use kv_quorum_ext::KvQuorumExt;
pub use mycelium_core::{KvHandle, LogEntry, MeshHandle};
pub use overlay_reliable::AckResult;
pub use sharding::ShardError;
pub use mycelium_core::{SchemaError, SchemaHandle, SchemaPublishResult};
#[cfg(feature = "llm")]
pub use prompt::{PromptTemplate, PromptSkillError, PromptSkillHandle};
#[cfg(feature = "llm")]
pub use llm::{LlmBackend, LlmResult, LlmError, OpenAiBackend, EchoBackend};
#[cfg(feature = "llm")]
pub use llm_handle::LlmHandle;
pub use mcp_handle::McpHandle;

/// Cached roster entry for a single group, held in the short-lived `group_roster_cache`.
/// Only read by the consensus path's `cached_group_members_ctx`; the field/struct
/// remain populated unconditionally (cheap) but are dead without the `consensus` feature.
#[cfg_attr(not(feature = "consensus"), allow(dead_code))]
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

    /// Cumulative count of inbound (remote) writes to a `sys/` key this node
    /// owns — `sys/identity/{self}`, `sys/load/{self}`, `sys/role/{self}`,
    /// `sys/tuple/{self}/…`. Only the named node should ever originate these;
    /// a remote write to one is a namespace-ownership violation.
    ///
    /// **Detection, not prevention** (mirrors [`commit_conflicts`](Self::commit_conflicts)):
    /// the offending write is still applied per LWW — Layer I stays ignorant of
    /// the namespace convention — and a `warn!` is logged. `sys/` ownership is
    /// promise-strength; this counter is the tripwire that makes a clobber
    /// legible. Signed keys (`identity`, `role`) additionally fail verification
    /// at read; unsigned keys (`load`, `tuple`) rely on this signal alone.
    pub sys_namespace_violations: u64,

    /// Cumulative count of capability advertisements this node **declined to
    /// resolve** because the advertiser did not hold a role required by the
    /// `sys/capauthz/{ns}/{name}` policy (WS-D / M6 · D5).
    ///
    /// **Detection, not prevention** (mirrors the tripwires above): the
    /// advertisement still propagates per LWW; this node simply routes around the
    /// unauthorized provider at resolve time and logs a `warn!`. A non-zero value
    /// means a provider is advertising a governed capability without the required
    /// signed role — investigate that node. Only counts under the `compliance`
    /// feature with a published policy.
    pub cap_authz_violations: u64,

    /// Cumulative count of **schema-version mismatches** detected at resolve
    /// (WS-F / Schema-Evo · E2): a provider matched the requested `(ns, name)`
    /// (and attributes) but advertised a different `schema_id` than the
    /// `CapFilter::with_schema(expected)` asked for.
    ///
    /// **Detection, not prevention** (the tripwire idiom): the mismatched
    /// provider is routed around (it never satisfies the schema-strict filter),
    /// but the drift is made legible instead of silently invisible. A non-zero
    /// value means producers and consumers in the cluster disagree on a schema
    /// version — register a migration (tier 3) or reconcile the versions.
    pub schema_mismatch: u64,

    /// Number of senders this node is currently **distributed-rate-limiting** (WS-C / M7): a sender
    /// whose *aggregate* observed inbound rate (summed across all observers via `sys/rate/`) crossed
    /// `rate_aggregate_threshold_fps`, so this node tightened its own inbound budget for it. `0`
    /// unless `rate_observation_enabled`. A node-local decision on shared evidence — never a cluster
    /// eviction. A sustained non-zero value names an abusive sender to investigate.
    pub rate_limited_senders: u64,
}

// The old 22-field `TaskCtx` God Object has been split (ROADMAP §v2.0 M1): `CoreCtx`
// (the Layers I+II infrastructure bundle) and `ReplyInterceptor` now live in the
// `mycelium-core` crate, imported at the top of this module. `TaskCtx` below wraps
// `Arc<CoreCtx>` and adds the Layer III+ fields; `GossipAgent` holds `Arc<TaskCtx>`
// (and so do the tasks), breaking the agent↔task reference cycle.

/// The full infrastructure bundle: [`CoreCtx`] (Layers I+II) plus the Layer III+ fields
/// (capability / service / consensus / compliance). Held in a single `Arc`, cloned into
/// every background task and typed handle. `Deref`s to `CoreCtx`, so the ~380 existing
/// `ctx.<core-field>` access sites are unchanged.
pub(crate) struct TaskCtx {
    /// Layers I + II substrate context. Will live in `mycelium-core` (M1).
    pub(crate) core: Arc<CoreCtx>,

    // ── Capability subsystem ─────────────────────────────────────────────────────
    // The soft-state readiness flag (formerly `caps_advertised`) moved to
    // `CoreCtx::soft_state_advertised` in v2 M3 — the persist loop that flips it is
    // pure Layer I. Read it via `self.soft_state_advertised` (Deref) as before.
    /// Shared registry for the consolidated `declare_requirement` opacity watcher.
    /// A single background task reads from this instead of one task per requirement.
    pub(crate) filter_opacity_registry: Arc<capability_ops::FilterOpacityRegistry>,
    /// Short-lived cache of group membership lists keyed by group name.
    /// Invalidated generation-based: `KvState::grp_generation` is bumped (Release)
    /// whenever a `grp/` key changes; the cache reader loads it with Acquire so it
    /// never sees a stale roster after observing the new generation value.
    #[cfg_attr(not(feature = "consensus"), allow(dead_code))]
    pub(crate) group_roster_cache: RosterCache,
    /// WS-C M9 tuning governor — the auto-tuner gate fed by local API (sovereign) +
    /// gossiped `sys/govern/` fleet intents (reconciled, local-wins, evaporating).
    pub(crate) tuning_governor: Arc<tuning_governor::TuningGovernor>,

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
    /// Per-slot commit-conflict counts (Legible-Emergence Phase 2 "hot slots"): the consensus
    /// listener's tripwire records each conflicting slot here (lock-free papaya, incremented via
    /// `compute`). Read by the fleet snapshot; empty when there are no conflicts / no consensus.
    // Read only by the `gateway` fleet snapshot; written only by the `consensus` tripwire — so no
    // single feature makes it both, and it is legitimately unread in a gateway-free build.
    #[cfg_attr(not(feature = "gateway"), allow(dead_code))]
    pub(crate) commit_conflict_slots: Arc<papaya::HashMap<Arc<str>, u64>>,

    /// Legible-Emergence Phase 3: the bounded, HLC-stamped **event ring** — the per-node source the
    /// `explain` fan-out assembles in causal order. Always allocated (tiny); recorded to only when
    /// `emergent_detectors_enabled` (RT4 always-on-when-enabled). Lock #: leaf, short critical
    /// section, never across `await`.
    pub(crate) event_ring: Arc<emergent::EventRing>,

    /// Legible-Emergence Phase-1 gauge: count of governed groups currently in a **confirmed**
    /// membership conflict (observed `grp/` count outside the governor's `[min,max]`, sustained
    /// past hysteresis). Set each tick by [`emergent::run_emergent_detectors`]; `0` unless
    /// `emergent_detectors_enabled`. Relaxed — diagnostic; surfaced on `/stats`.
    pub(crate) governed_group_conflicts: Arc<AtomicU64>,

    /// Legible-Emergence Phase-1 gauge (P6): count of required capabilities currently with **zero
    /// fresh providers visible from this node**, confirmed past hysteresis. Set by the detector
    /// loop; `0` unless `emergent_detectors_enabled`. Relaxed — diagnostic; on `/stats`.
    pub(crate) capability_coverage_gaps: Arc<AtomicU64>,

    /// Legible-Emergence Phase-1 gauge (P2): count of (group, node) pairs whose membership is
    /// currently **flapping** (≥ threshold join/leave transitions within the flap window). Set by
    /// the detector loop; `0` unless `emergent_detectors_enabled`. Relaxed — diagnostic; on `/stats`.
    pub(crate) membership_flaps: Arc<AtomicU64>,

    /// Legible-Emergence Phase-1 gauge (P3): count of (node, kind) pairs whose opacity is currently
    /// **oscillating** (≥ threshold opaque/transparent toggles within the window — pheromone
    /// hunting). Set by the detector loop; `0` unless `emergent_detectors_enabled`. On `/stats`.
    pub(crate) opacity_oscillations: Arc<AtomicU64>,

    /// Cumulative capability-authorization rejections at resolve (WS-D / M6 · D5;
    /// see `SystemStats::cap_authz_violations`). Relaxed — diagnostic.
    pub(crate) cap_authz_violations: Arc<AtomicU64>,

    /// Cumulative schema-version mismatches at resolve (WS-F / Schema-Evo · E2;
    /// see `SystemStats::schema_mismatch`). Relaxed — diagnostic.
    pub(crate) schema_mismatch: Arc<AtomicU64>,

    /// Head of this node's tamper-evident audit chain (WS2). `audit()` seals a
    /// record under this lock so the per-node chain stays linear, then releases
    /// it before writing to KV. Lock #8 in the lock-order table (leaf).
    #[cfg(feature = "compliance")]
    pub(crate) audit_chain: Arc<std::sync::Mutex<audit::AuditChainState>>,
}

impl std::ops::Deref for TaskCtx {
    type Target = CoreCtx;
    #[inline]
    fn deref(&self) -> &CoreCtx {
        &self.core
    }
}

// `TaskCtx::spawn_task` was removed in v2 M3 — `spawn_task` now lives on `CoreCtx`
// (the `task_handles` JoinSet it drives is a core field). `Arc<TaskCtx>` call sites
// Deref-coerce to it unchanged.

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
    /// Optional operator-supplied data-at-rest cipher (WS3). Set once via
    /// [`with_data_at_rest_cipher`](Self::with_data_at_rest_cipher) before `start`;
    /// read at `start` and threaded into the WAL/snapshot persistence paths.
    pub(super) data_at_rest_cipher:
        std::sync::OnceLock<Arc<dyn crate::persistence::DataAtRestCipher>>,
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
        KvHandle::from_core(Arc::clone(&self.task_ctx.core))
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
        MeshHandle::from_core(Arc::clone(&self.task_ctx.core))
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
        SchemaHandle::from_core(Arc::clone(&self.task_ctx.core))
    }

    /// Publish a **schema migration** into the registry (WS-F / Schema-Evo · E3) — a declarative
    /// `from → to` transform gossiped alongside the schemas. Any node may publish; every node then
    /// resolves migration paths over the union (see [`migrate_payload`](Self::migrate_payload)).
    /// Registered + explicit — never silent coercion. Returns whether the write was queued.
    pub fn publish_migration(&self, migration: &crate::schema_evolution::SchemaMigration) -> bool {
        crate::schema_evolution::publish_migration(&self.kv(), migration)
    }

    /// The registered `from → to` migration from the local gossip view, if any.
    pub fn get_migration(&self, from: &str, to: &str) -> Option<crate::schema_evolution::SchemaMigration> {
        crate::schema_evolution::get_migration(&self.kv(), from, to)
    }

    /// Every registered migration in the local gossip view.
    pub fn list_migrations(&self) -> Vec<crate::schema_evolution::SchemaMigration> {
        crate::schema_evolution::list_migrations(&self.kv())
    }

    /// **Migrate** a received JSON `payload` from schema `from` to `to` by composing the registered
    /// migration chain (WS-F / Schema-Evo · E3b). This is the **explicit** application a consumer
    /// makes before parsing a cross-version payload — never an automatic silent coercion on the hot
    /// path. `Err(NoMigrationPath)` when no registered chain connects the versions (and the
    /// `schema_mismatch` tripwire fires so the drift is legible) — **detect, don't guess**.
    pub fn migrate_payload(
        &self,
        from: &str,
        to: &str,
        payload: &[u8],
    ) -> Result<serde_json::Value, crate::schema_evolution::MigrationError> {
        use crate::schema_evolution::MigrationError;
        let value: serde_json::Value = serde_json::from_slice(payload)
            .map_err(|e| MigrationError::InvalidJson(e.to_string()))?;
        let migrations = self.list_migrations();
        match crate::schema_evolution::migrate_value(&migrations, from, to, value) {
            Ok(v) => Ok(v),
            Err(e @ MigrationError::NoMigrationPath { .. }) => {
                self.task_ctx.schema_mismatch.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                Err(e)
            }
            Err(e) => Err(e),
        }
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
    ///
    /// Requires the `consensus` feature (default-on; v2 M2 gate).
    #[cfg(feature = "consensus")]
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
        // WS-C M8: fill any "auto" (0-sentinel) tuning fields from a cluster-size estimate
        // before anything reads the config. `bootstrap_peers` (excluding self) + this node is
        // a lower bound on N; explicit/env values are non-zero and pass through untouched.
        // Runs before `start()`'s `validate()`, so a derived field never trips the zero guard.
        let n_estimate = config.bootstrap_peers.iter().filter(|p| *p != &node_id).count() + 1;
        config.derive_unset(n_estimate);
        config.audit_invariants();
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
        // RPC/bulk reply correlation map. Created here so the core-level reply
        // interceptor can capture it; the same Arc is shared into `TaskCtx`
        // (Layer III) where `rpc_call` registers and awaits oneshots.
        let rpc_pending: Arc<std::sync::Mutex<std::collections::HashMap<u64, tokio::sync::oneshot::Sender<crate::signal::Signal>>>> =
            Arc::new(std::sync::Mutex::new(std::collections::HashMap::new()));
        let core_ctx = Arc::new(CoreCtx {
            node_id:         node_id.clone(),
            // WS-C M9: snapshot the hot-tunable subset from the (M8-derived) config.
            hot:             Arc::new(mycelium_core::context::HotConfig::from_config(&config)),
            config:          Arc::clone(&config_arc),
            seen:            Arc::new(ShardedSeen::new(seen_shards)),
            hlc:             Arc::new(crate::hlc::Hlc::with_max_drift(config.max_clock_drift_ms)),
            signal_boundary: Arc::new(RwLock::new(Boundary::new(node_id.clone()))),
            signal_handlers: Arc::new(SignalHandlers::new(signal_window)),
            gossip_txs,
            default_ttl,
            kv_state:        Arc::clone(&kv_state),
            wal:             std::sync::OnceLock::new(),
            sys_namespace_violations: Arc::new(AtomicU64::new(0)),
            tls: std::sync::OnceLock::new(),
            peer_keys: Arc::new(papaya::HashMap::new()),
            peers: Arc::clone(&peers_arc),
            rate_throttle: Arc::new(papaya::HashMap::new()),
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
            reply_interceptor: Some({
                let rp = Arc::clone(&rpc_pending);
                Arc::new(move |sig: &Signal| -> bool {
                    // Claim correlated rpc.result / bulk.result: the nonce is the
                    // first 8 LE bytes of the payload. On hit, fire the oneshot and
                    // signal "claimed" so the fan-out is skipped.
                    if sig.payload.len() >= 8
                        && (sig.kind.as_ref() == signal_kind::RPC_RESULT
                            || sig.kind.as_ref() == signal_kind::BULK_RESULT)
                    {
                        let call_nonce = u64::from_le_bytes(
                            sig.payload[..8].try_into()
                                .expect("RPC/bulk result nonce occupies first 8 bytes; payload length checked"),
                        );
                        if let Some(tx) = rp.lock().unwrap_or_else(|e| e.into_inner()).remove(&call_nonce) {
                            let _ = tx.send(sig.clone());
                            return true;
                        }
                    }
                    false
                })
            }),
            soft_state_advertised: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            shutdown_tx:         Arc::clone(&shutdown_tx_arc),
            task_handles:        Arc::clone(&task_handles_arc),
        });
        let task_ctx = Arc::new(TaskCtx {
            core: Arc::clone(&core_ctx),
            bulk_transport:  Arc::new(bulk::BulkTransport::new(
                config.http_port.unwrap_or(0),
                std::time::Duration::from_secs(config.bulk_fetch_timeout_secs),
                config.max_concurrent_bulk_handlers,
            )),
            rpc_pending: Arc::clone(&rpc_pending),
            commit_conflicts: Arc::new(AtomicU64::new(0)),
            commit_conflict_slots: Arc::new(papaya::HashMap::new()),
            event_ring: Arc::new(emergent::EventRing::default()),
            governed_group_conflicts: Arc::new(AtomicU64::new(0)),
            capability_coverage_gaps: Arc::new(AtomicU64::new(0)),
            membership_flaps: Arc::new(AtomicU64::new(0)),
            opacity_oscillations: Arc::new(AtomicU64::new(0)),
            cap_authz_violations: Arc::new(AtomicU64::new(0)),
            schema_mismatch: Arc::new(AtomicU64::new(0)),
            #[cfg(feature = "compliance")]
            audit_chain: Arc::new(std::sync::Mutex::new(audit::AuditChainState::new())),
            filter_opacity_registry: Arc::new(capability_ops::FilterOpacityRegistry::new()),
            group_roster_cache:  Arc::clone(&group_roster_cache),
            tuning_governor:     Arc::new(tuning_governor::TuningGovernor::default()),
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
            data_at_rest_cipher: std::sync::OnceLock::new(),
        }
    }

    /// Attach an operator-supplied [`DataAtRestCipher`](crate::DataAtRestCipher)
    /// (WS3 crown-jewel) that envelope-encrypts this node's WAL records and
    /// snapshots before they hit disk, and decrypts them on replay. Opt-in: with
    /// no cipher attached, persistence bytes are written in the clear (unchanged).
    ///
    /// Call **before** [`start`](Self::start); it is read once at startup. Calling
    /// it twice keeps the first cipher (a `warn!` is logged). The substrate stays
    /// neutral on key custody — your impl wraps your KMS/keyring; the same key must
    /// be available across restarts or the node cannot replay its own data.
    ///
    /// Only affects data **at rest**; the gossip wire is secured separately by the
    /// `tls` feature.
    pub fn with_data_at_rest_cipher(
        &self,
        cipher: Arc<dyn crate::persistence::DataAtRestCipher>,
    ) {
        if self.data_at_rest_cipher.set(cipher).is_err() {
            tracing::warn!("with_data_at_rest_cipher called more than once; keeping the first cipher");
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

    /// This node's outbound [`EgressPolicy`](crate::EgressPolicy) (WS3). Consulted
    /// before the substrate makes outbound HTTP requests it *chooses* to make —
    /// the MCP client bridge, capability probes, and LLM-backend calls. Empty
    /// `allow_hosts` (default) permits all.
    pub fn egress_policy(&self) -> &crate::config::EgressPolicy {
        &self.config.egress
    }
}

/// Hot certificate / identity rotation (WS5; `tls` feature).
#[cfg(feature = "tls")]
impl GossipAgent {
    /// Sign `msg` with this node's Ed25519 **identity** key — the same key behind TLS and gossip
    /// `SignedData`. `None` if no `tls` identity is configured. Public so companion crates (e.g.
    /// WS-F AgentFacts emission) can **self-certify** documents under the node identity without
    /// reaching into the substrate; the matching key is [`identity_public_key`](Self::identity_public_key).
    pub fn sign_with_identity(&self, msg: &[u8]) -> Option<[u8; 64]> {
        let tls = self.task_ctx.tls.get()?;
        Some(crate::tls::sign_bytes(&tls.signing_key(), msg))
    }

    /// This node's Ed25519 identity **public** (verifying) key, or `None` without the `tls`
    /// identity. A fetcher verifies a self-signed document against this; trust is the caller's to
    /// decide (self-certified — no issuer authority).
    pub fn identity_public_key(&self) -> Option<[u8; 32]> {
        let tls = self.task_ctx.tls.get()?;
        Some(tls.signing_key().verifying_key().to_bytes())
    }

    /// Rotate this node's TLS / identity key **without cluster disruption**.
    ///
    /// 1. Generate a new key + CA-signed cert (reusing the cluster CA), persisted
    ///    to disk — but do not activate it yet.
    /// 2. Publish `sys/identity/{self}` = `new ‖ old`, signed by the **old** key
    ///    (which peers still trust), so peers' retained key sets accept both.
    /// 3. Wait `propagation` for that to gossip.
    /// 4. Cut over: atomically swap the active key + cert ([`tls::NodeTls::activate`]).
    ///    New gossip signatures and new TLS handshakes use the new key (configs are
    ///    read per connection); existing connections keep their CA-trusted session —
    ///    no listener restart. The `new‖old` identity entry is retained so the prior
    ///    key survives one restart (historical-record verification).
    ///
    /// Requires `GossipConfig::tls`. Returns the new 32-byte verifying key.
    ///
    /// **Compromise caveat:** the old key remains accepted (retained-key
    /// verification, WS5 option B) so historical signatures stay valid; rotating
    /// away from a *compromised* key needs explicit revocation on top.
    pub async fn rotate_identity(
        &self,
        propagation: std::time::Duration,
    ) -> Result<[u8; 32], crate::error::GossipError> {
        use crate::error::GossipError;
        let tls = self.task_ctx.tls.get().ok_or(GossipError::InvalidField {
            field: "tls", reason: "rotate_identity requires the tls identity (set GossipConfig::tls)".into(),
        })?;
        let tls_cfg = self.config.tls.as_ref().ok_or(GossipError::InvalidField {
            field: "tls", reason: "rotate_identity requires GossipConfig::tls".into(),
        })?;

        // 1. Generate the new material (persisted, not yet active).
        let material = crate::tls::generate_rotation(tls_cfg, &self.node_id)?;
        let new_vk = material.verifying_key;

        // 2. Publish new ‖ (every previously-published key) — the full rotation
        //    history, so historical signatures stay verifiable across any number
        //    of rotations. Signed by the still-active OLD key (peers trust it), so
        //    their retained sets accept the new key before the cutover.
        let id_key = format!("sys/identity/{}", self.node_id);
        let existing = self
            .kv()
            .get(&id_key)
            .map(|b| helpers::parse_identity_keys(&b))
            .unwrap_or_default();
        let value = helpers::encode_identity_history(new_vk, &existing);
        let _ = self.kv().set(id_key, Bytes::from(value));

        // 3. Let it propagate.
        tokio::time::sleep(propagation).await;

        // 4. Cut over to the new key/cert.
        tls.activate(material);
        Ok(new_vk)
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
        let signed = rbac::SignedRoleClaim::sign(claim, &tls.signing_key());
        // Local + WAL write is guaranteed; a `false` here only means this gossip
        // tick's channel was full — the entry still anti-entropy-syncs and a
        // re-advertise retries, so a dropped dispatch is not an error.
        let _ = self.kv().set(rbac::role_key(&self.node_id), signed.encode());
        Ok(())
    }

    /// Read and **verify** `node`'s role claim. Returns the claim only if its
    /// signature checks out against **any** verifying key known for `node` (the
    /// WS5 retained set, plus this node's own current key when `node` is self), so
    /// a claim signed before a key rotation still verifies. A forged or
    /// mis-attributed `sys/role/` write reads back as `None`.
    pub fn roles_of(&self, node: &NodeId) -> Option<rbac::RoleClaim> {
        let bytes = self.kv().get(&rbac::role_key(node))?;
        helpers::known_verifying_keys(&self.task_ctx, node)
            .iter()
            .find_map(|vk| rbac::verified_roles(&bytes, node, vk))
    }

    /// **Revoke** one of this node's own verifying keys cluster-wide (WS-D / D1) — for a key known
    /// to be *compromised*, where WS5 rotation alone is insufficient (a rotated-away key stays
    /// trusted for verification). Writes a signed revocation to `sys/revocation/{self}/{key}`,
    /// signed by the **current** identity key; once it gossips, every node's retained-key verify
    /// paths (role claims, audit chain) **exclude** `revoked_key`, so anything it signed fails
    /// verification — "sub-second revocation", bounded by gossip latency.
    ///
    /// Typical flow: detect compromise → [`rotate_identity`](Self::rotate_identity) to a fresh key
    /// → `revoke_identity_key(old_key)` (signed by the new current key). Requires the `tls` identity.
    pub fn revoke_identity_key(&self, revoked_key: [u8; 32]) -> Result<(), crate::error::GossipError> {
        revocation::revoke_key(&self.task_ctx, revoked_key, None)
    }

    /// This node's **revocation-log head** (WS-D / D3): `(merkle_root_hex, count)` over its validated
    /// revocations — the same root `/gateway/transparency` serves. Surfacing it in the federation
    /// edge (e.g. the `mycelium-agentfacts` AgentFacts `certification`) lets a NANDA-quilt fetcher
    /// see "is this domain's key set current?" and check inclusion proofs against it — no new trust
    /// authority. `None` if there are no revocations yet.
    pub fn revocation_head(&self) -> Option<(String, usize)> {
        let (root, count) = transparency::revocation_head(&self.task_ctx, &self.task_ctx.node_id);
        (count > 0).then(|| {
            let hex: String = root.iter().map(|b| format!("{b:02x}")).collect();
            (hex, count)
        })
    }

    /// Set the **gossip-level capability-authorization policy** for `ns/name` (WS-D / M6 · D4): an
    /// any-of list of roles an advertiser must hold (a *signature-verified* `sys/role/` claim) for
    /// resolvers to admit it. Writes `sys/capauthz/{ns}/{name}`; every node then **routes around**
    /// unauthorized advertisers of `ns/name` at resolve time and counts the rejection on `/stats`
    /// (`cap_authz_violations`). An empty `roles` opens the capability again. Enforce-at-resolve,
    /// detect-at-write — never a Layer I write guard. Returns whether the write was queued.
    pub fn set_capability_authz(&self, ns: &str, name: &str, roles: Vec<String>) -> bool {
        capauthz::set_policy(&self.task_ctx, ns, name, roles)
    }

    /// Set the capability-authorization policy for `ns/name` **through consensus** (WS-D / M6 · D6),
    /// so the cluster *agrees* the policy rather than accepting a unilateral LWW write — a single
    /// rogue or buggy operator cannot impose a cluster-wide authz policy alone. Proposes the policy
    /// on the consensus slot `capauthz/{ns}/{name}`; on commit, the agreed value is applied to
    /// `sys/capauthz/{ns}/{name}` (the key resolvers read, unchanged from D4) so enforcement stays
    /// at each resolver — **consensus carries the policy, it is not an admission coordinator**.
    ///
    /// Requires `start_consensus_listener` on every voter (multi-node quorum). Returns the
    /// [`ConsensusResult`]; the policy is applied only on `Committed`.
    #[cfg(feature = "consensus")]
    pub async fn set_capability_authz_via_consensus(
        &self,
        ns: &str,
        name: &str,
        roles: Vec<String>,
        config: crate::consensus::ConsensusConfig,
    ) -> crate::consensus::ConsensusResult {
        let policy = capauthz::CapAuthzPolicy { required_roles: roles }.encode();
        let slot = format!("capauthz/{ns}/{name}");
        let result = self.consensus().system_propose(&slot, policy.clone(), config).await;
        if matches!(result, crate::consensus::ConsensusResult::Committed { .. }) {
            // Apply the agreed policy to the key resolvers enforce on (D4). Idempotent under LWW.
            let _ = self.kv().set(capauthz::capauthz_key(ns, name), policy);
        }
        result
    }

    /// Provider-side authorization check: may the (verified) `sender` invoke a
    /// capability whose `authorized_callers` allowlist is `allow`? Empty `allow`
    /// ⇒ unrestricted; otherwise admits if `sender`'s NodeId is listed or it holds
    /// a listed role (verified via [`roles_of`](Self::roles_of)).
    ///
    /// **Identity-bound:** under the `tls` identity, an incoming RPC/invoke frame's
    /// sender is signature-verified at the connection layer, so `request.sender()`
    /// is a trustworthy input. Call this in a provider's `rpc_rx` serve loop before
    /// honoring an invocation — `authorized_callers` is only *enforced* where the
    /// provider serves, never at the caller's resolve (which the caller controls).
    pub fn caller_authorized(&self, sender: &NodeId, allow: &[Arc<str>]) -> bool {
        if allow.is_empty() {
            return true;
        }
        let roles = self.roles_of(sender).map(|c| c.roles).unwrap_or_default();
        rbac::caller_admitted(allow, sender, &roles)
    }

    /// Seal one event into this node's tamper-evident audit chain (WS2) and write
    /// it to `sys/audit/{self}/{seq}`. Returns the record's content hash — the
    /// stable, citable identifier of the sealed event.
    ///
    /// Requires the `tls` identity (records are Ed25519-signed); a node without
    /// `GossipConfig::tls` returns [`GossipError::InvalidField`]. The chain head is
    /// advanced under a short lock that is released **before** the KV write, so no
    /// two lock-order-table locks are ever held together (the write itself takes
    /// the leaf index-stripe lock inside `apply_and_notify`).
    ///
    /// Detection-not-prevention: the record is a normal signed KV entry that
    /// gossips to the cluster; tampering is caught by [`verify_chain`](crate::verify_chain),
    /// never blocked at the store.
    pub fn audit(
        &self,
        action: audit::AuditAction,
        principal: impl Into<String>,
        target: impl Into<String>,
        outcome: audit::AuditOutcome,
        detail: Option<String>,
    ) -> Result<[u8; 32], crate::error::GossipError> {
        audit::seal_and_write(&self.task_ctx, action, principal, target, outcome, detail)
    }

    /// Read `node`'s audit stream from KV, decoded and ordered by sequence. This
    /// is the content-hash slice primitive the M16 consumer builds on: filter the
    /// returned records and cite each `record.content_hash()`.
    pub fn audit_stream(&self, node: &NodeId) -> Vec<audit::SignedAuditRecord> {
        audit::read_stream(&self.task_ctx, node)
    }

    /// Verify `node`'s full audit stream against its identity key. `Ok(())` means
    /// the stream is intact from genesis; an [`AuditVerifyError`](crate::AuditVerifyError)
    /// names the first violation, or `UnknownSigner` if `node`'s key is not known.
    pub fn audit_verify(&self, node: &NodeId) -> Result<(), audit::AuditVerifyError> {
        audit::verify_stream(&self.task_ctx, node)
    }

    /// Distinct node ids that have an audit stream in the local KV view
    /// (parsed from `sys/audit/{node}/…` keys).
    pub fn audit_stream_nodes(&self) -> Vec<NodeId> {
        audit::stream_nodes(&self.task_ctx)
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

    #[test]
    fn caller_authorized_open_nodeid_and_roleless() {
        let node = NodeId::new("127.0.0.1", 7401).unwrap();
        let agent = GossipAgent::new(node, GossipConfig::default());
        let caller = NodeId::new("10.0.0.5", 8000).unwrap();
        let caller_str: Arc<str> = caller.to_string().into();
        // Empty allowlist → open.
        assert!(agent.caller_authorized(&caller, &[]));
        // Caller has no advertised/verified roles → role entries do not admit.
        assert!(!agent.caller_authorized(&caller, &["orchestrator".into()]));
        // Explicit NodeId entry admits.
        assert!(agent.caller_authorized(&caller, &[caller_str]));
    }
}
