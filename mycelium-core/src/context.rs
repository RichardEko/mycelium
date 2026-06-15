//! [`CoreCtx`] — the shared Layers I + II infrastructure bundle.
//!
//! Carries identity/config, the Layer I KV substrate, the Layer II signal mesh,
//! transport security, networking, and lifecycle handles — everything the gossip
//! connection handler and writer need. The full `mycelium` crate wraps this in its
//! `TaskCtx` (adding Layer III+) and derefs to it, so existing `ctx.<core-field>`
//! sites are unchanged.
//!
//! **Invariant:** `CoreCtx` never references a higher-layer type (philosophy §5a —
//! the substrate is never aware of the layers above it).

use crate::config::GossipConfig;
use crate::framing::ForwardHint;
use crate::node_id::NodeId;
use crate::seen::ShardedSeen;
use crate::signal::{Boundary, Signal, SignalHandlers};
use crate::store::KvState;
use bytes::Bytes;
use parking_lot::RwLock;
use std::sync::{atomic::AtomicU64, Arc};
use tokio::{sync::{mpsc, watch}, task::JoinSet};

/// Opt-in pre-delivery signal interceptor registered by the upper service layer.
/// Given a delivered [`Signal`], it claims correlated `rpc.result` / `bulk.result`
/// replies — firing the waiting oneshot — and returns `true` to skip the
/// `signal_handlers` fan-out. Core knows nothing about RPC: the connection handler
/// only asks "did anything claim this signal?" The RPC correlation law lives in the
/// closure the service layer registers (mechanism in core; agency above).
pub type ReplyInterceptor = Arc<dyn Fn(&Signal) -> bool + Send + Sync>;

/// The shared Layers I + II infrastructure bundle. The full `mycelium` crate's
/// `TaskCtx` holds this as `core: Arc<CoreCtx>` and `Deref`s to it.
pub struct CoreCtx {
    // ── Identity + config ────────────────────────────────────────────────────────
    pub node_id:          NodeId,
    /// Shared copy of the agent configuration. Available to typed handles so they
    /// can access `signal_window_secs`, `health_check_interval_secs`, `locality_path`,
    /// and `topology_policies` without borrowing `GossipAgent`.
    pub config:           Arc<GossipConfig>,
    pub default_ttl:      u8,

    // ── Layer I — KV substrate ───────────────────────────────────────────────────
    pub seen:             Arc<ShardedSeen>,
    /// Hybrid Logical Clock for causal LWW ordering. `make_gossip_update`
    /// calls `tick()` for every locally-originated write; the connection
    /// handler calls `observe()` for every incoming timestamp so the local
    /// clock dominates any remote stamp it has seen.
    pub hlc:              Arc<crate::hlc::Hlc>,
    pub gossip_txs:       Arc<[mpsc::Sender<(Bytes, u64, ForwardHint)>]>,
    pub kv_state:         Arc<KvState>,
    /// WAL handle for durable KV writes. Unset when persistence is disabled.
    /// Written once by `start()` after replay; read-only afterwards.
    pub wal: std::sync::OnceLock<Arc<crate::persistence::WalHandle>>,

    // ── Layer II — Signal mesh ───────────────────────────────────────────────────
    pub signal_boundary:  Arc<RwLock<Boundary>>,
    pub signal_handlers:  Arc<SignalHandlers>,
    /// Receiver-side causal reorder buffer for `emit_ordered` signals.
    /// `None` when `config.signal_ordered_delivery = false` (the default).
    pub reorder_buf: Option<Arc<std::sync::Mutex<crate::signal::SignalReorderBuffer>>>,
    /// Opt-in pre-delivery signal interceptor (see [`ReplyInterceptor`]). `None`
    /// for pure KV/signal embeds (zero overhead); the upper service layer sets it
    /// to claim correlated RPC/bulk replies. Core stays RPC-agnostic.
    pub reply_interceptor: Option<ReplyInterceptor>,

    // ── Security (transport) ─────────────────────────────────────────────────────
    /// TLS context (server + client configs + signing key). Unset when the
    /// `tls` feature is disabled or when `GossipConfig::tls` is `None`.
    /// Written once by `start()` before any task is spawned; read-only afterwards.
    pub tls: std::sync::OnceLock<Arc<crate::tls::NodeTls>>,
    /// Retained verifying-key **set** per node (WS5 option B): every key a node
    /// has published at `sys/identity/{node}`, accumulated across rotations so
    /// historical signatures (audit, consensus, roles) keep verifying. Verify
    /// paths try all keys.
    #[cfg_attr(not(feature = "tls"), allow(dead_code))]
    pub peer_keys: Arc<papaya::HashMap<NodeId, Vec<[u8; 32]>>>,

    /// Cumulative `sys/` namespace-ownership violations (see
    /// `SystemStats::sys_namespace_violations`). Incremented by the connection
    /// handler's inbound-apply tripwire when a remote write targets a `sys/`
    /// key this node owns; Relaxed ordering — purely diagnostic. Core because the
    /// connection handler (Layer I transport) is the sole writer.
    pub sys_namespace_violations: Arc<AtomicU64>,

    // ── Networking ───────────────────────────────────────────────────────────────
    /// Live peer table shared with the HTTP gateway for peer-count-based quorum sizing.
    pub peers: Arc<papaya::HashMap<NodeId, std::time::Instant>>,

    // ── Lifecycle ────────────────────────────────────────────────────────────────
    /// Shutdown broadcast — sending `true` cancels all background tasks.
    pub shutdown_tx: Arc<watch::Sender<bool>>,
    /// All spawned background tasks. Reaping is automatic via `JoinSet`.
    pub task_handles: Arc<std::sync::Mutex<JoinSet<()>>>,
}
