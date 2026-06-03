//! Configuration for all gossip protocol components.
//!
//! The primary type is [`GossipConfig`], which is passed to [`GossipAgent::new`](crate::GossipAgent::new).
//! All fields have documented defaults. Use [`GossipConfig::default()`] as a starting point and
//! override only the fields that matter for your deployment.
//!
//! Config can also be loaded from a TOML file via [`GossipConfig::load_from_file`] and overridden
//! at runtime via `GOSSIP_*` environment variables.

use serde::{Deserialize, Serialize};
use std::{collections::HashMap, env, fs, path::{Path, PathBuf}};
use crate::error::GossipError;
use crate::NodeId;

/// TLS configuration for mTLS peer connections and node identity signing.
///
/// All fields are optional: when `cert_pem` / `key_pem` are absent, certificates
/// are auto-generated into `auto_cert_dir`. The auto-generated CA cert
/// (`ca-cert.pem`) must be copied to all peers so they can verify each other.
///
/// Only meaningful when the `tls` crate feature is enabled. Has no effect when
/// the feature is disabled.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct TlsConfig {
    /// Path to a PEM-encoded node certificate. `None` = auto-generate on startup.
    pub cert_pem: Option<PathBuf>,
    /// Path to a PEM-encoded (PKCS8) node private key. `None` = auto-generate.
    pub key_pem: Option<PathBuf>,
    /// Path to the PEM-encoded cluster CA certificate used to verify peers.
    /// `None` = look for `{auto_cert_dir}/ca-cert.pem`; generate if absent.
    pub ca_cert_pem: Option<PathBuf>,
    /// Directory where auto-generated cert/key/CA files are stored.
    /// Defaults to `"./mycelium-tls/"` relative to the working directory.
    pub auto_cert_dir: PathBuf,
}

impl Default for TlsConfig {
    fn default() -> Self {
        Self {
            cert_pem:     None,
            key_pem:      None,
            ca_cert_pem:  None,
            auto_cert_dir: PathBuf::from("./mycelium-tls/"),
        }
    }
}

/// Controls if and how a node persists its KV store to local disk.
///
/// Set `GossipConfig::persistence` to `Some(PersistenceConfig { .. })` to opt in.
/// `None` (the default) keeps the current in-memory-only behaviour.
///
/// Data is stored under `{base_path}/{node_id}/kv/`, giving collision-free layout
/// when multiple nodes run on the same machine.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PersistenceConfig {
    /// Root directory for persistence files.
    /// Actual data lives under `{base_path}/{node_id}/kv/`.
    pub base_path: PathBuf,

    /// Controls `fdatasync` behaviour on WAL appends.
    ///
    /// Does not affect snapshot writes — snapshots always `fdatasync` before
    /// the atomic rename regardless of this setting.
    #[serde(default)]
    pub sync_mode: SyncMode,

    /// Trigger a snapshot when the WAL reaches this many entries.
    /// Prevents unbounded WAL growth. Default: `10_000`.
    #[serde(default = "default_snapshot_wal_threshold")]
    pub snapshot_wal_threshold: usize,

    /// Also trigger a snapshot on this timer interval (seconds). Default: `300`.
    #[serde(default = "default_snapshot_interval_secs")]
    pub snapshot_interval_secs: u64,
}

fn default_snapshot_wal_threshold() -> usize { 10_000 }
fn default_snapshot_interval_secs()  -> u64  { 300 }

/// Controls how aggressively WAL appends are flushed to durable storage.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum SyncMode {
    /// `fdatasync` after every WAL append. Safest; ~1 ms overhead per write on SSD.
    Flush,
    /// OS-buffered writes. Fast; last few writes may be lost on power failure.
    #[default]
    Async,
    /// No explicit sync. For development and tests only.
    Os,
}

/// Enforcement strength for a [`GroupTopologyPolicy`].
///
/// **Soft**: topology is a preference for fan-out scoring and leader selection
/// but never gates a quorum. Quorums commit as long as the active-member count
/// is met — diversity, if any, emerges from preference.
///
/// **Hard**: quorum commit requires the policy's diversity condition to be met.
/// When the condition is not met, [`ConsensusEngine::propose`] returns
/// `ConsensusResult::TopologyUnsatisfied` — never silently degrades. The caller
/// decides whether to wait, retry, or surface an error.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum TopologyEnforcement {
    #[default]
    Soft,
    Hard,
}

/// How a group's quorum must be distributed across [`LocalityPath`](crate::locality::LocalityPath) levels.
///
/// `prefer_shared_depth` biases fan-out and leader selection toward nodes sharing
/// locality at the named depth. `spread_depth` + `spread_min_distinct` define the
/// diversity gate evaluated when `enforcement = Hard`.
///
/// Validation: when `enforcement = Hard`, `spread_depth` must be `Some` and
/// `spread_min_distinct` must be `>= 2`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct GroupTopologyPolicy {
    pub prefer_shared_depth: usize,
    pub spread_depth:        Option<usize>,
    pub spread_min_distinct: usize,
    pub enforcement:         TopologyEnforcement,
}

impl Default for GroupTopologyPolicy {
    fn default() -> Self {
        Self {
            prefer_shared_depth: 0,
            spread_depth:        None,
            spread_min_distinct: 1,
            enforcement:         TopologyEnforcement::Soft,
        }
    }
}

/// Unified configuration for all protocol components.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct GossipConfig {
    /// IP address the node binds its TCP listener to.
    pub bind_address: String,
    /// TCP port the node listens on. Must be non-zero.
    pub bind_port: u16,
    /// Peers to connect to on startup for initial cluster discovery.
    pub bootstrap_peers: Vec<NodeId>,
    /// Window (seconds) used to compute seen-set and tombstone retention cutoffs.
    /// Raise this if your network can be partitioned for longer than the default.
    pub propagation_window_secs: u64,
    /// How often (seconds) the health monitor sends pings and evicts silent peers.
    pub health_check_interval_secs: u64,
    /// Initial TTL applied to locally-originated gossip messages.
    /// Each hop decrements this by one; a message with TTL 1 is not forwarded.
    pub default_ttl: u8,
    /// Maximum number of concurrent inbound TCP connections.
    pub max_connections: usize,
    /// Depth of each per-peer outbound MPSC channel (a ring buffer, not a semaphore).
    /// **When full, gossip frames are silently dropped** — the failure is indistinguishable
    /// from network packet loss unless `system_stats().dropped_frames` is monitored.
    ///
    /// Size this to handle the maximum burst that can arrive before a slow peer drains
    /// its channel. For a cluster of N agents writing one KV entry per tick with epidemic
    /// fan-out F, budget at least `N × F` per peer-writer (the intermediate node that
    /// happens to forward the most messages in one generation). At the default fan-out of
    /// 4 and N = 256 agents that is 1 024; the default of 64 is correct only for small
    /// clusters (N ≤ 16).
    pub writer_channel_depth: usize,
    /// Maximum number of peers each gossip shard will forward to simultaneously.
    ///
    /// Bootstrap peers are always included regardless of this limit. Peers discovered
    /// via the health monitor are added up to this cap. Setting this to
    /// `bootstrap_peers.len()` keeps the gossip topology fixed at the bootstrap mesh
    /// while still allowing the health monitor to run at its natural interval for failure
    /// detection.
    ///
    /// Default: `i64::MAX as usize` (no effective cap — all discovered peers are forwarded to).
    pub max_forwarding_peers: usize,
    /// How long (seconds) to wait before retrying after a failed connection attempt or
    /// write error to a peer. Must be in `[1, 300]`. At the minimum, one reconnect attempt
    /// is made per second per dead peer. Increase on large clusters to avoid a connect
    /// storm after a network partition. Frames to the peer are silently dropped while the
    /// backoff is active, so values above a few seconds can impair convergence.
    pub reconnect_backoff_secs: u64,
    /// Capacity of each sharded gossip worker channel. There are `gossip_shards`
    /// independent channels, each with this depth. `set`/`delete` return `false`
    /// when the target shard's channel is full. Increase if your workload produces
    /// bursts of writes faster than the gossip workers can drain them.
    pub gossip_channel_capacity: usize,
    /// Maximum number of nonce entries in the seen-set before graduated eviction
    /// kicks in. Each entry is ~24 bytes. At the default of 100 000 that is ~2.4 MB.
    ///
    /// Both Data (`set`/`delete`) and Signal (`emit`) nonces share this budget.
    /// High signal rates (e.g. health probes from many agents) can compete with Data
    /// nonces for capacity. Raise proportionally when both layers are active.
    pub max_seen_entries: usize,
    /// Number of consecutive health-check intervals a peer can be silent before eviction.
    /// Default: 3 (evict after 3 missed pings).
    pub peer_eviction_intervals: u64,
    /// Number of independent gossip worker shards (each is one tokio task + one channel).
    /// Defaults to logical CPU count, capped at 16. Raise for very high write rates on
    /// many-core machines.
    pub gossip_shards: usize,
    /// When `true`, received gossip keys are interned in a process-wide pool so all
    /// inbound connection handlers share a single `Arc<str>` allocation per distinct key.
    /// Effective for workloads with a bounded key set; set to `false` for workloads with
    /// an unbounded key space (e.g. per-request UUIDs as keys) to prevent pool growth.
    pub intern_keys: bool,
    /// Maximum number of distinct keys held in the process-wide intern pool.
    /// When the pool reaches this limit, new keys bypass interning — they are returned as
    /// their own `Arc<str>` without being inserted into the pool. This bounds pool memory
    /// without disabling interning entirely for the keys already present.
    ///
    /// `0` = no limit (default). Only meaningful when `intern_keys = true`.
    pub intern_max_keys: usize,
    /// Number of peer addresses sampled into each outbound Ping's `known_peers` list.
    /// Controls both topology-discovery speed and Ping message size. In clusters with
    /// more than `ping_peer_sample_size` peers, each Ping carries a random subset so
    /// every node gradually learns the full topology over several rounds. Raise on
    /// large clusters (> 100 nodes) where convergence speed matters.
    pub ping_peer_sample_size: usize,
    /// TCP accept-queue (backlog) depth for inbound listener sockets. The OS silently
    /// drops SYN packets when the queue is full. Raise if you observe connection
    /// timeouts during cluster restarts or large fan-in connection bursts.
    pub tcp_accept_backlog: u32,
    /// Maximum number of peers this node will track in its peer table.
    ///
    /// Bootstrap peers are always retained. Peers discovered via piggybacked `known_peers`
    /// lists in Ping messages are only added when the table has fewer than `max_peers`
    /// entries. Raise when running large dynamic clusters; lower when the gossip topology
    /// should be fixed (e.g. grid demos where unbounded discovery causes O(N²) connections).
    ///
    /// Default: `i64::MAX as usize` (no effective cap — all discovered peers are tracked).
    pub max_peers: usize,
    /// Seconds of inactivity after which a peer writer closes its TCP connection.
    ///
    /// The connection is re-established transparently on the next frame destined for that
    /// peer, so this is invisible to callers. Idle writer tasks consume a file descriptor
    /// and a tokio task for every peer ever contacted; setting a timeout bounds that cost
    /// in clusters that churn or where many peers are only occasionally active.
    ///
    /// `0` = no timeout (default, existing behaviour — writers stay connected indefinitely).
    pub writer_idle_timeout_secs: u64,
    /// When `true`, gossip shards apply scope-aware forwarding for Signal frames:
    /// Group-scoped signals are forwarded only to known group members (plus up to
    /// `epidemic_extra_peers` random non-members for epidemic coverage), and
    /// Individual-scoped signals are forwarded only to the target peer. System signals
    /// and Data frames are always broadcast to all targets regardless of this setting.
    ///
    /// Requires that group membership be published to the KV store (via `join_group`)
    /// so shards can determine the member set. Defaults to `true`. Set to `false` to
    /// revert to pre-v0.2 broadcast forwarding (all signals fan out to all peers
    /// regardless of group scope), which may be useful for debugging topology issues.
    pub group_aware_forwarding: bool,

    /// Number of random non-member peers added to Group-scoped signal fan-out when
    /// `group_aware_forwarding = true`. These extra hops give epidemic coverage to
    /// nodes that are not in the group's member set, ensuring signals reach the full
    /// cluster over time even for narrow groups.
    ///
    /// Tune to cluster size: 3 is appropriate for clusters of up to ~100 nodes.
    /// Raise to 5–7 on very large clusters (> 1 000 nodes) where 3 hops may be
    /// insufficient for timely convergence; lower to 1–2 on small clusters (< 10
    /// nodes) where 3 random peers may flood non-members unnecessarily.
    ///
    /// Default: `3`.
    pub epidemic_extra_peers: usize,
    /// Hard cap on the one-shot startup jitter before the first health-check ping (ms).
    ///
    /// Jitter prevents a thundering herd when many nodes start simultaneously. The default
    /// (`0`) uses `[0, health_check_interval_secs × 500)` ms — up to half the interval,
    /// spreading a cluster's first pings across a full period. Set to a small value (e.g.
    /// `50`) in test configurations to reduce stabilisation delays without removing jitter
    /// entirely.
    pub health_check_max_jitter_ms: u64,

    /// Evaporation window (seconds) for pheromone trail entries written by
    /// [`manage_opacity`](crate::GossipAgent::manage_opacity).
    ///
    /// [`suggest_leader`](crate::GossipAgent::suggest_leader), [`peer_load`](crate::GossipAgent::peer_load),
    /// and [`peer_load_rx`](crate::GossipAgent::peer_load_rx) treat entries older than this as
    /// transparent (unloaded). Raise when nodes can be unreachable for longer than the default
    /// before their pheromone entries should be considered stale.
    ///
    /// Use [`GossipAgent::signal_window`] to read this as a `Duration` — prefer that over
    /// the [`SENDER_LOG_WINDOW`](crate::signal::SENDER_LOG_WINDOW) compile-time constant in
    /// application code.
    ///
    /// Default: 600 (10 minutes).
    pub signal_window_secs: u64,

    /// Enable causal delivery ordering for signals emitted via
    /// [`emit_ordered`](crate::GossipAgent::emit_ordered).
    ///
    /// When `true`, received signals that carry an `hlc_seq` timestamp are
    /// buffered in a per-`(sender, kind)` min-heap and delivered in ascending
    /// HLC order. Signals without `hlc_seq` (unordered `emit`) bypass the
    /// buffer entirely — zero cost to existing callers.
    ///
    /// When `false` (the default), `hlc_seq` is ignored and all signals are
    /// delivered immediately in arrival order.
    pub signal_ordered_delivery: bool,

    /// Maximum time (ms) a signal may be held in the reorder buffer waiting
    /// for earlier signals to arrive. After this deadline the signal is
    /// delivered regardless of gaps. Only relevant when
    /// `signal_ordered_delivery = true`.
    ///
    /// Default: 500.
    pub signal_reorder_max_hold_ms: u64,

    /// Maximum number of buffered signals per `(sender, kind)` pair before a
    /// forced flush (delivered in HLC order). Prevents unbounded growth if a
    /// sender emits many ordered signals faster than the buffer can drain.
    /// Only relevant when `signal_ordered_delivery = true`.
    ///
    /// Default: 64.
    pub signal_reorder_max_depth: usize,

    /// Maximum number of **live** (non-tombstone) entries in the KV store.
    ///
    /// When the live count reaches this limit, new live writes are silently dropped.
    /// Tombstone writes (deletes) are always accepted — they reduce the live count.
    /// `0` = unlimited (default).
    ///
    /// **Trade-off**: a cap prevents unbounded memory growth in workloads with high key
    /// cardinality, but silently discards writes once the limit is hit. Monitor
    /// `system_stats().store_entries` to detect saturation and raise the limit before
    /// it becomes active in production.
    pub max_store_entries: usize,

    /// Hierarchical locality address for this node, coarse → fine.
    ///
    /// Typical: `["eu-west-1", "az-1a", "rack-14"]`. Segments are opaque strings;
    /// the protocol never interprets values, only equality at each level. Empty
    /// (the default) means "unspecified" — the node shares zero locality with any
    /// peer, and topology-aware features degrade to a no-op for that node.
    ///
    /// Written to `cap/{node_id}/locality/self` at startup; tombstoned at shutdown.
    /// Other nodes see this via gossip and use it for [`topology_policies`](Self::topology_policies)
    /// scoring and Hard-enforcement quorum gates.
    pub locality_path: Vec<String>,

    /// Per-group topology policy. Looked up by group name when constructing a
    /// consensus engine. Absent entries default to no policy (Soft enforcement,
    /// no diversity gate).
    ///
    /// **Precedence over `CapabilityGroupDef.topology_policy`**: config entries here
    /// always win. Emergent-group definitions can suggest a policy, but operator
    /// config in this map overrides it.
    pub topology_policies: HashMap<String, GroupTopologyPolicy>,

    /// TCP port for the embedded HTTP server (Layer 3 gateway).
    ///
    /// `None` (the default) disables the HTTP server entirely — existing deployments
    /// are unaffected unless this is set. When set, the server binds on startup and
    /// provides `/health`, `/stats`, and `/signals/{kind}` (SSE) endpoints, and will
    /// serve the MCP bridge and language gateway in Layer 4.
    ///
    /// Must be non-zero and must differ from `bind_port`.
    pub http_port: Option<u16>,

    /// Bind address for the embedded HTTP server.
    ///
    /// Defaults to `"127.0.0.1"` (loopback only). Set to `"0.0.0.0"` to accept
    /// connections on all interfaces. Only meaningful when `http_port` is `Some`.
    pub http_addr: String,

    /// Local KV persistence configuration.
    ///
    /// `None` (the default) keeps the current in-memory-only behaviour — no files
    /// are written and a restart loses all KV state. Set to `Some(PersistenceConfig
    /// { base_path, .. })` to enable an append-only WAL and periodic snapshots.
    ///
    /// Each node writes under `{base_path}/{node_id}/kv/`, so multiple nodes on
    /// the same machine never collide. If `base_path` is not writable at startup,
    /// a warning is logged and the node falls back to in-memory-only mode.
    pub persistence: Option<PersistenceConfig>,

    /// Timeout (seconds) for the HTTP fetch issued by `bulk_serve` when a target
    /// node retrieves a staged bulk payload from the caller's HTTP endpoint.
    ///
    /// If the caller's HTTP server does not respond within this window the
    /// `bulk_serve` handler logs a warning and discards the in-flight call,
    /// preventing orphaned tasks from accumulating when a caller is unreachable.
    ///
    /// Default: `30`.
    pub bulk_fetch_timeout_secs: u64,

    /// Mutual TLS configuration.
    ///
    /// `None` (the default) disables TLS — the gossip TCP port accepts plain
    /// connections with no authentication. Set to `Some(TlsConfig { .. })` to
    /// require mTLS: all peers must present a certificate signed by the cluster CA.
    ///
    /// Requires the `tls` crate feature. Has no effect when the feature is
    /// disabled even if set to `Some(...)`.
    pub tls: Option<TlsConfig>,
}

impl Default for GossipConfig {
    fn default() -> Self {
        Self {
            bind_address: "127.0.0.1".to_string(),
            bind_port: 8080,
            bootstrap_peers: Vec::new(),
            propagation_window_secs: 60,
            health_check_interval_secs: 10,
            default_ttl: 5,
            max_connections: 1024,
            writer_channel_depth: 256,
            max_forwarding_peers: i64::MAX as usize,
            reconnect_backoff_secs: 5,
            gossip_channel_capacity: 1024,
            max_seen_entries: 100_000,
            peer_eviction_intervals: 3,
            gossip_shards: std::thread::available_parallelism()
                .map_or(4, |n| n.get())
                .min(16),
            intern_keys: true,
            intern_max_keys: 0,
            ping_peer_sample_size: 20,
            tcp_accept_backlog: 1024,
            max_peers: i64::MAX as usize,
            writer_idle_timeout_secs: 0,
            group_aware_forwarding: true,
            epidemic_extra_peers:   3,
            health_check_max_jitter_ms: 0,
            signal_window_secs: 600,
            signal_ordered_delivery:     false,
            signal_reorder_max_hold_ms:  500,
            signal_reorder_max_depth:    64,
            max_store_entries: 0,
            locality_path:     Vec::new(),
            topology_policies: HashMap::new(),
            http_port:               None,
            http_addr:               "127.0.0.1".to_string(),
            persistence:             None,
            bulk_fetch_timeout_secs: 30,
            tls:                     None,
        }
    }
}

impl GossipConfig {
    /// Validates all numeric constraints.
    ///
    /// Called automatically by [`GossipAgent::start`] and [`load_from_file`](Self::load_from_file).
    /// Call manually after mutating fields directly to catch errors early.
    pub fn validate(&self) -> Result<(), GossipError> {
        if self.bind_address.is_empty() {
            return Err(GossipError::Config("bind_address cannot be empty".into()));
        }
        self.bind_address.parse::<std::net::IpAddr>().map_err(|_| {
            GossipError::Config(format!(
                "bind_address '{}' is not a valid IP address", self.bind_address
            ))
        })?;
        if self.bind_port == 0 {
            return Err(GossipError::Config("Bind port cannot be zero".into()));
        }
        if self.max_connections == 0 {
            return Err(GossipError::Config("max_connections cannot be zero".into()));
        }
        if self.max_connections > 65535 {
            return Err(GossipError::Config(
                "max_connections cannot exceed 65535 (practical file-descriptor budget \
                 per process; each inbound connection consumes one fd)".into(),
            ));
        }
        if self.health_check_interval_secs == 0 {
            return Err(GossipError::Config(
                "health_check_interval_secs cannot be zero".into(),
            ));
        }
        if self.health_check_interval_secs > 3600 {
            return Err(GossipError::Config(
                "health_check_interval_secs cannot exceed 3600 seconds (1 hour)".into(),
            ));
        }
        if self.default_ttl == 0 {
            return Err(GossipError::Config("default_ttl cannot be zero".into()));
        }
        if self.propagation_window_secs == 0 {
            return Err(GossipError::Config(
                "propagation_window_secs cannot be zero".into(),
            ));
        }
        if self.writer_channel_depth == 0 {
            return Err(GossipError::Config(
                "writer_channel_depth cannot be zero".into(),
            ));
        }
        if self.writer_channel_depth < 64 {
            tracing::warn!(
                "writer_channel_depth {} is below 64; frame drops are likely in clusters with more than 16 nodes",
                self.writer_channel_depth,
            );
        }
        if self.gossip_channel_capacity == 0 {
            return Err(GossipError::Config(
                "gossip_channel_capacity cannot be zero".into(),
            ));
        }
        if self.max_seen_entries == 0 {
            return Err(GossipError::Config(
                "max_seen_entries cannot be zero".into(),
            ));
        }
        if self.peer_eviction_intervals == 0 {
            return Err(GossipError::Config(
                "peer_eviction_intervals cannot be zero".into(),
            ));
        }
        if self.gossip_shards == 0 {
            return Err(GossipError::Config("gossip_shards cannot be zero".into()));
        }
        if self.reconnect_backoff_secs == 0 {
            return Err(GossipError::Config(
                "reconnect_backoff_secs must be at least 1; \
                 set to 1 to retry as aggressively as possible".into(),
            ));
        }
        if self.reconnect_backoff_secs > 300 {
            return Err(GossipError::Config(
                "reconnect_backoff_secs cannot exceed 300 seconds; \
                 frames to unreachable peers are dropped during backoff, so large values \
                 impair convergence — increase health_check_interval_secs instead".into(),
            ));
        }
        if self.ping_peer_sample_size == 0 {
            return Err(GossipError::Config(
                "ping_peer_sample_size cannot be zero".into(),
            ));
        }
        if self.tcp_accept_backlog == 0 {
            return Err(GossipError::Config(
                "tcp_accept_backlog cannot be zero".into(),
            ));
        }
        if self.max_peers == 0 {
            return Err(GossipError::Config(
                "max_peers cannot be zero".into(),
            ));
        }
        if self.max_forwarding_peers > 100 && self.bootstrap_peers.len() > 20 {
            tracing::warn!(
                bootstrap_peers = self.bootstrap_peers.len(),
                max_forwarding_peers = self.max_forwarding_peers,
                "max_forwarding_peers is unlimited with a large bootstrap set; \
                 consider capping it (e.g. 32) to avoid O(N²) gossip traffic",
            );
        }
        if let Some(p) = self.http_port {
            if p == 0 {
                return Err(GossipError::Config("http_port cannot be zero".into()));
            }
            if p == self.bind_port {
                return Err(GossipError::Config(
                    "http_port must differ from bind_port".into(),
                ));
            }
            if self.http_addr.is_empty() {
                return Err(GossipError::Config("http_addr cannot be empty".into()));
            }
            self.http_addr.parse::<std::net::IpAddr>().map_err(|_| {
                GossipError::Config(format!(
                    "http_addr '{}' is not a valid IP address", self.http_addr
                ))
            })?;
        }
        for seg in &self.locality_path {
            if seg.is_empty() {
                return Err(GossipError::Config(
                    "locality_path segments must be non-empty".into(),
                ));
            }
        }
        for (group, policy) in &self.topology_policies {
            if policy.enforcement == TopologyEnforcement::Hard {
                if policy.spread_depth.is_none() {
                    return Err(GossipError::Config(format!(
                        "topology_policies[{}] requires spread_depth when enforcement = Hard",
                        group,
                    )));
                }
                if policy.spread_min_distinct < 2 {
                    return Err(GossipError::Config(format!(
                        "topology_policies[{}] requires spread_min_distinct >= 2 when enforcement = Hard",
                        group,
                    )));
                }
            }
        }
        if let Some(p) = &self.persistence {
            if p.snapshot_wal_threshold == 0 {
                return Err(GossipError::Config(
                    "persistence.snapshot_wal_threshold cannot be zero".into(),
                ));
            }
            if p.snapshot_interval_secs == 0 {
                return Err(GossipError::Config(
                    "persistence.snapshot_interval_secs cannot be zero".into(),
                ));
            }
            // Writability check: warn and the caller falls back to None at startup.
            // We don't hard-fail here because validate() is also called in load_from_file
            // before the node_id is known, so we can only check the base path itself.
            if !p.base_path.as_os_str().is_empty() {
                if let Err(e) = fs::create_dir_all(&p.base_path) {
                    tracing::warn!(
                        path = %p.base_path.display(),
                        error = %e,
                        "persistence.base_path is not writable; node will run in-memory-only mode",
                    );
                }
            }
        }
        Ok(())
    }

    /// Applies `GOSSIP_*` environment variable overrides to this config in-place.
    ///
    /// Called automatically by [`load_from_file`](Self::load_from_file). Call
    /// manually when constructing a `GossipConfig` programmatically and env var
    /// overrides should take effect — e.g. container deployments that configure
    /// entirely through environment variables and have no config file.
    ///
    /// **Note:** this method does _not_ call [`validate`](Self::validate). Callers
    /// must invoke `validate()` separately after all overrides are applied.
    ///
    /// All 24 fields can be overridden: `GOSSIP_BIND_ADDRESS`, `GOSSIP_BIND_PORT`,
    /// `GOSSIP_PROPAGATION_WINDOW_SECS`, `GOSSIP_HEALTH_CHECK_INTERVAL_SECS`,
    /// `GOSSIP_DEFAULT_TTL`, `GOSSIP_MAX_CONNECTIONS`, `GOSSIP_WRITER_CHANNEL_DEPTH`,
    /// `GOSSIP_MAX_FORWARDING_PEERS`, `GOSSIP_RECONNECT_BACKOFF_SECS`,
    /// `GOSSIP_GOSSIP_CHANNEL_CAPACITY`, `GOSSIP_MAX_SEEN_ENTRIES`,
    /// `GOSSIP_PEER_EVICTION_INTERVALS`, `GOSSIP_GOSSIP_SHARDS`,
    /// `GOSSIP_INTERN_KEYS` (`true`/`false`/`1`/`0`), `GOSSIP_INTERN_MAX_KEYS`,
    /// `GOSSIP_BOOTSTRAP_PEERS` (comma-separated
    /// `ip:port` list), `GOSSIP_PING_PEER_SAMPLE_SIZE`, `GOSSIP_TCP_ACCEPT_BACKLOG`,
    /// `GOSSIP_MAX_PEERS`, `GOSSIP_WRITER_IDLE_TIMEOUT_SECS`,
    /// `GOSSIP_GROUP_AWARE_FORWARDING` (`true`/`false`/`1`/`0`),
    /// `GOSSIP_HEALTH_CHECK_MAX_JITTER_MS`, `GOSSIP_SIGNAL_WINDOW_SECS`,
    /// `GOSSIP_MAX_STORE_ENTRIES`, `GOSSIP_EPIDEMIC_EXTRA_PEERS`.
    pub fn apply_env_overrides(&mut self) -> Result<(), GossipError> {
        if let Ok(v) = env::var("GOSSIP_BIND_ADDRESS") {
            self.bind_address = v;
        }
        if let Ok(v) = env::var("GOSSIP_BIND_PORT") {
            self.bind_port = v.parse().map_err(GossipError::Parse)?;
        }
        if let Ok(v) = env::var("GOSSIP_PROPAGATION_WINDOW_SECS") {
            self.propagation_window_secs = v.parse().map_err(GossipError::Parse)?;
        }
        if let Ok(v) = env::var("GOSSIP_HEALTH_CHECK_INTERVAL_SECS") {
            self.health_check_interval_secs = v.parse().map_err(GossipError::Parse)?;
        }
        if let Ok(v) = env::var("GOSSIP_DEFAULT_TTL") {
            self.default_ttl = v.parse().map_err(GossipError::Parse)?;
        }
        if let Ok(v) = env::var("GOSSIP_MAX_CONNECTIONS") {
            self.max_connections = v.parse().map_err(GossipError::Parse)?;
        }
        if let Ok(v) = env::var("GOSSIP_WRITER_CHANNEL_DEPTH") {
            self.writer_channel_depth = v.parse().map_err(GossipError::Parse)?;
        }
        if let Ok(v) = env::var("GOSSIP_MAX_FORWARDING_PEERS") {
            self.max_forwarding_peers = v.parse().map_err(GossipError::Parse)?;
        }
        if let Ok(v) = env::var("GOSSIP_RECONNECT_BACKOFF_SECS") {
            self.reconnect_backoff_secs = v.parse().map_err(GossipError::Parse)?;
        }
        if let Ok(v) = env::var("GOSSIP_GOSSIP_CHANNEL_CAPACITY") {
            self.gossip_channel_capacity = v.parse().map_err(GossipError::Parse)?;
        }
        if let Ok(v) = env::var("GOSSIP_MAX_SEEN_ENTRIES") {
            self.max_seen_entries = v.parse().map_err(GossipError::Parse)?;
        }
        if let Ok(v) = env::var("GOSSIP_PEER_EVICTION_INTERVALS") {
            self.peer_eviction_intervals = v.parse().map_err(GossipError::Parse)?;
        }
        if let Ok(v) = env::var("GOSSIP_GOSSIP_SHARDS") {
            self.gossip_shards = v.parse().map_err(GossipError::Parse)?;
        }
        if let Ok(v) = env::var("GOSSIP_INTERN_KEYS") {
            self.intern_keys = match v.as_str() {
                "true"  | "1" => true,
                "false" | "0" => false,
                _ => return Err(GossipError::Config(format!(
                    "GOSSIP_INTERN_KEYS must be 'true', 'false', '1', or '0', got '{}'", v
                ))),
            };
        }
        if let Ok(v) = env::var("GOSSIP_INTERN_MAX_KEYS") {
            self.intern_max_keys = v.parse().map_err(GossipError::Parse)?;
        }
        if let Ok(v) = env::var("GOSSIP_BOOTSTRAP_PEERS") {
            let peers: Result<Vec<NodeId>, _> = v
                .split(',')
                .map(|s| s.trim().parse::<NodeId>())
                .collect();
            self.bootstrap_peers = peers.map_err(|e| GossipError::Config(e.to_string()))?;
        }
        if let Ok(v) = env::var("GOSSIP_PING_PEER_SAMPLE_SIZE") {
            self.ping_peer_sample_size = v.parse().map_err(GossipError::Parse)?;
        }
        if let Ok(v) = env::var("GOSSIP_TCP_ACCEPT_BACKLOG") {
            self.tcp_accept_backlog = v.parse().map_err(GossipError::Parse)?;
        }
        if let Ok(v) = env::var("GOSSIP_MAX_PEERS") {
            self.max_peers = v.parse().map_err(GossipError::Parse)?;
        }
        if let Ok(v) = env::var("GOSSIP_WRITER_IDLE_TIMEOUT_SECS") {
            self.writer_idle_timeout_secs = v.parse().map_err(GossipError::Parse)?;
        }
        if let Ok(v) = env::var("GOSSIP_GROUP_AWARE_FORWARDING") {
            self.group_aware_forwarding = match v.as_str() {
                "true" | "1" => true,
                "false" | "0" => false,
                _ => return Err(GossipError::Config(
                    "GOSSIP_GROUP_AWARE_FORWARDING must be true/false/1/0".into(),
                )),
            };
        }
        if let Ok(v) = env::var("GOSSIP_HEALTH_CHECK_MAX_JITTER_MS") {
            self.health_check_max_jitter_ms = v.parse().map_err(GossipError::Parse)?;
        }
        if let Ok(v) = env::var("GOSSIP_SIGNAL_WINDOW_SECS") {
            self.signal_window_secs = v.parse().map_err(GossipError::Parse)?;
        }
        if let Ok(v) = env::var("GOSSIP_MAX_STORE_ENTRIES") {
            self.max_store_entries = v.parse().map_err(GossipError::Parse)?;
        }
        if let Ok(v) = env::var("GOSSIP_EPIDEMIC_EXTRA_PEERS") {
            self.epidemic_extra_peers = v.parse().map_err(GossipError::Parse)?;
        }
        if let Ok(v) = env::var("GOSSIP_LOCALITY_PATH") {
            self.locality_path = v
                .split('/')
                .filter(|s| !s.is_empty())
                .map(|s| s.to_string())
                .collect();
        }
        if let Ok(v) = env::var("GOSSIP_HTTP_PORT") {
            self.http_port = Some(v.parse().map_err(GossipError::Parse)?);
        }
        if let Ok(v) = env::var("GOSSIP_HTTP_ADDR") {
            self.http_addr = v;
        }
        Ok(())
    }

    /// Loads configuration from a TOML file, then applies environment variable
    /// overrides via [`apply_env_overrides`](Self::apply_env_overrides).
    ///
    /// Bootstrap peer addresses in the TOML file are validated at deserialise time
    /// via [`NodeId`]'s custom `Deserialize` implementation.
    pub fn load_from_file<P: AsRef<Path>>(path: P) -> Result<Self, GossipError> {
        let config_str = fs::read_to_string(path).map_err(GossipError::Io)?;
        let mut config: Self = toml::from_str(&config_str).map_err(GossipError::Toml)?;
        config.apply_env_overrides()?;
        config.validate()?;
        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node_id::NodeId;

    #[test]
    fn validate_rejects_empty_bind_address() {
        let mut cfg = GossipConfig::default();
        cfg.bind_address = String::new();
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_zero_port() {
        let mut cfg = GossipConfig::default();
        cfg.bind_port = 0;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_zero_ttl() {
        let mut cfg = GossipConfig::default();
        cfg.default_ttl = 0;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_zero_writer_channel_depth() {
        let mut cfg = GossipConfig::default();
        cfg.writer_channel_depth = 0;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_default_is_valid() {
        assert!(GossipConfig::default().validate().is_ok());
    }

    #[test]
    fn validate_rejects_zero_gossip_channel_capacity() {
        let mut cfg = GossipConfig::default();
        cfg.gossip_channel_capacity = 0;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_zero_max_seen_entries() {
        let mut cfg = GossipConfig::default();
        cfg.max_seen_entries = 0;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_zero_peer_eviction_intervals() {
        let mut cfg = GossipConfig::default();
        cfg.peer_eviction_intervals = 0;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_zero_gossip_shards() {
        let mut cfg = GossipConfig::default();
        cfg.gossip_shards = 0;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn roundtrip_toml() {
        let mut original = GossipConfig::default();
        original.bind_port = 9100;
        original.bind_address = "0.0.0.0".to_string();
        original.default_ttl = 7;
        original.health_check_interval_secs = 3;
        original.bootstrap_peers = vec![
            NodeId::new("127.0.0.1", 9101).unwrap(),
            NodeId::new("127.0.0.1", 9102).unwrap(),
        ];
        let toml_str = toml::to_string(&original).expect("serialise to TOML");
        let roundtripped: GossipConfig = toml::from_str(&toml_str).expect("deserialise from TOML");
        assert_eq!(roundtripped, original, "all fields must survive a TOML round-trip");
    }

    #[test]
    fn apply_env_overrides_sets_field() {
        struct EnvGuard(&'static str, Option<String>);
        impl Drop for EnvGuard {
            fn drop(&mut self) {
                match &self.1 {
                    Some(v) => std::env::set_var(self.0, v),
                    None    => std::env::remove_var(self.0),
                }
            }
        }
        let var = "GOSSIP_MAX_SEEN_ENTRIES";
        let _guard = EnvGuard(var, std::env::var(var).ok());
        std::env::set_var(var, "12345");
        let mut cfg = GossipConfig::default();
        cfg.apply_env_overrides().expect("apply_env_overrides must not fail");
        assert_eq!(cfg.max_seen_entries, 12345);
    }

    #[test]
    fn validate_rejects_max_connections_above_limit() {
        let mut cfg = GossipConfig::default();
        cfg.max_connections = 65536;
        assert!(cfg.validate().is_err(), "max_connections = 65536 should fail validation");
    }

    #[test]
    fn validate_rejects_reconnect_backoff_above_limit() {
        let mut cfg = GossipConfig::default();
        cfg.reconnect_backoff_secs = 301;
        assert!(cfg.validate().is_err(), "reconnect_backoff_secs = 301 should fail validation");
    }

    #[test]
    fn validate_rejects_zero_ping_peer_sample_size() {
        let mut cfg = GossipConfig::default();
        cfg.ping_peer_sample_size = 0;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_zero_tcp_accept_backlog() {
        let mut cfg = GossipConfig::default();
        cfg.tcp_accept_backlog = 0;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_excessive_health_check_interval() {
        let mut cfg = GossipConfig::default();
        cfg.health_check_interval_secs = 3601;
        assert!(cfg.validate().is_err(), "health_check_interval_secs = 3601 should fail validation");
    }
}
