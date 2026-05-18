//! Configuration for all gossip protocol components.
//!
//! The primary type is [`GossipConfig`], which is passed to [`GossipAgent::new`](crate::GossipAgent::new).
//! All fields have documented defaults. Use [`GossipConfig::default()`] as a starting point and
//! override only the fields that matter for your deployment.
//!
//! Config can also be loaded from a TOML file via [`GossipConfig::load_from_file`] and overridden
//! at runtime via `GOSSIP_*` environment variables.

use serde::{Deserialize, Serialize};
use std::{env, fs, path::Path};
use crate::error::GossipError;
use crate::NodeId;

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
    /// Group-scoped signals are forwarded only to known group members (plus up to 3
    /// random peers for epidemic coverage), and Individual-scoped signals are forwarded
    /// only to the target peer. System signals and Data frames are always broadcast to
    /// all targets regardless of this setting.
    ///
    /// Requires that group membership be published to the KV store (via `join_group`)
    /// so shards can determine the member set. Defaults to `true`. Set to `false` to
    /// revert to pre-v0.2 broadcast forwarding (all signals fan out to all peers
    /// regardless of group scope), which may be useful for debugging topology issues.
    pub group_aware_forwarding: bool,
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
            health_check_max_jitter_ms: 0,
            signal_window_secs: 600,
            max_store_entries: 0,
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
    /// All 21 fields can be overridden: `GOSSIP_BIND_ADDRESS`, `GOSSIP_BIND_PORT`,
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
    /// `GOSSIP_HEALTH_CHECK_MAX_JITTER_MS`.
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
