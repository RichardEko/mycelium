//! Configuration for all gossip protocol components.
//!
//! The primary type is [`GossipConfig`], which is passed to `GossipAgent::new`.
//! All fields have documented defaults. Use [`GossipConfig::default()`] as a starting point and
//! override only the fields that matter for your deployment.
//!
//! Config can also be loaded from a TOML file via [`GossipConfig::load_from_file`] and overridden
//! at runtime via `GOSSIP_*` environment variables.

use serde::{Deserialize, Serialize};
use std::{collections::HashMap, env, fs, path::{Path, PathBuf}};
use crate::error::GossipError;
use crate::NodeId;

/// OIDC SSO configuration (WS4). Plain configuration data — it lives here in core
/// `config` (not the upper `oidc` verifier module) so `GossipConfig` never names an
/// upper-layer type. The verifier logic (`agent::oidc::OidcVerifier`, JWKS fetch,
/// `validate_token`) stays in the upper crate and imports this struct. The IdP's
/// signing keys are not configured here — they are fetched from the JWKS at runtime,
/// so vendor differences stay configuration.
#[cfg(feature = "compliance")]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OidcConfig {
    /// Expected `iss` claim — the IdP issuer URL. Also the discovery base
    /// (`{issuer}/.well-known/openid-configuration`) when `jwks_uri` is absent.
    pub issuer: String,
    /// Expected `aud` claim — this deployment's client/application id.
    pub audience: String,
    /// JWT claim holding the user's groups/roles (default `"groups"`; Entra often
    /// uses `"roles"`, Keycloak a nested path — config, not code).
    #[serde(default = "default_group_claim")]
    pub group_claim: String,
    /// Maps an IdP group name to the gateway scopes it grants. A principal's
    /// scopes are the union over its groups.
    #[serde(default)]
    pub group_scopes: HashMap<String, Vec<String>>,
    /// Explicit JWKS URI. If `None`, derive from `issuer` via discovery.
    #[serde(default)]
    pub jwks_uri: Option<String>,
}

#[cfg(feature = "compliance")]
fn default_group_claim() -> String { "groups".to_string() }

#[cfg(feature = "compliance")]
impl OidcConfig {
    /// Gateway scopes granted to a principal in `groups` (union, deduplicated).
    pub fn scopes_for_groups(&self, groups: &[String]) -> Vec<String> {
        let mut out: Vec<String> = Vec::new();
        for g in groups {
            if let Some(scopes) = self.group_scopes.get(g) {
                for s in scopes {
                    if !out.contains(s) {
                        out.push(s.clone());
                    }
                }
            }
        }
        out
    }
}

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

/// A gateway bearer token paired with its OAuth2-style scope grants.
///
/// Scopes follow the `resource:verb` convention (`kv:read`, `kv:write`,
/// `mesh:write`, `consensus:read`, …). A route admits a token when the token
/// holds the route's required scope, or the superuser wildcard `"*"`.
///
/// Only enforced under the `compliance` crate feature; without it the field is
/// inert and the legacy single-token model (`gateway_auth_token`) applies. The
/// legacy token is equivalent to a scoped token holding `"*"`, so existing
/// deployments upgrade with no behaviour change.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GatewayToken {
    /// The bearer token presented as `Authorization: Bearer <token>`.
    pub token: String,
    /// Scopes granted to this token. `["*"]` is full access.
    #[serde(default)]
    pub scopes: Vec<String>,
}

/// Outbound egress allow-policy (WS3 crown-jewel — blast-radius containment).
///
/// A node-local allowlist of hosts the substrate may reach *outbound*. It is a
/// **documented posture, not a coordinator**: each node enforces its own policy;
/// nothing is centrally assigned. An empty `allow_hosts` (the default) permits
/// all egress — behaviour is unchanged until an operator opts in.
///
/// Enforced at the MCP client bridge (`connect_mcp_server`) — the canonical
/// "twin reaches an external tool server" egress. Other outbound paths (LLM
/// backends, capability probes) are operator-responsibility; see the egress
/// runbook and threat model for the full posture.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct EgressPolicy {
    /// Permitted outbound hosts. Empty = allow all. An entry matches a host if it
    /// is equal, or — when the entry starts with `.` — if the host equals or is a
    /// subdomain of it (`.internal` matches `internal` and `api.internal`).
    #[serde(default)]
    pub allow_hosts: Vec<String>,
}

impl EgressPolicy {
    /// True if outbound to `host` is permitted. Empty allowlist ⇒ permit all.
    pub fn permits_host(&self, host: &str) -> bool {
        if self.allow_hosts.is_empty() {
            return true;
        }
        let host = host.to_ascii_lowercase();
        self.allow_hosts.iter().any(|p| {
            let p = p.to_ascii_lowercase();
            match p.strip_prefix('.') {
                Some(suffix) => host == suffix || host.ends_with(&format!(".{suffix}")),
                None => host == p,
            }
        })
    }

    /// True if outbound to `url`'s host is permitted. A URL whose host cannot be
    /// parsed is **denied** when a non-empty allowlist is in force (fail closed).
    pub fn permits_url(&self, url: &str) -> bool {
        if self.allow_hosts.is_empty() {
            return true;
        }
        match host_of_url(url) {
            Some(h) => self.permits_host(&h),
            None => false,
        }
    }
}

/// Extract the host from a URL without pulling in a URL crate: drop the scheme,
/// any `userinfo@`, then take up to the first `/?#` and strip a `:port`. IPv6
/// literals in brackets are returned without the brackets.
pub fn host_of_url(url: &str) -> Option<String> {
    let after_scheme = url.split_once("://").map(|(_, r)| r).unwrap_or(url);
    let authority = after_scheme
        .split(['/', '?', '#'])
        .next()
        .unwrap_or(after_scheme);
    let authority = authority.rsplit_once('@').map(|(_, h)| h).unwrap_or(authority);
    if authority.is_empty() {
        return None;
    }
    // IPv6 literal: [::1]:port
    if let Some(rest) = authority.strip_prefix('[') {
        return rest.split(']').next().filter(|s| !s.is_empty()).map(|s| s.to_string());
    }
    let host = authority.split(':').next().unwrap_or(authority);
    if host.is_empty() { None } else { Some(host.to_string()) }
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
/// When the condition is not met, `ConsensusEngine::propose` returns
/// `ConsensusResult::TopologyUnsatisfied` — never silently degrades. The caller
/// decides whether to wait, retry, or surface an error.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum TopologyEnforcement {
    #[default]
    Soft,
    Hard,
}

/// How a group's quorum must be distributed across `LocalityPath` levels.
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

/// Integer floor-sqrt (`⌊√n⌋`), used by `GossipConfig::derive_unset` to size
/// `ping_peer_sample_size`. No float cast — exact and clippy-clean.
pub(crate) fn integer_sqrt(n: usize) -> usize {
    if n < 2 { return n; }
    let mut x = n;
    let mut y = x.div_ceil(2);
    while y < x {
        x = y;
        y = (x + n / x) / 2;
    }
    x
}

/// Floor for auto-resolved [`GossipConfig::gossip_fanout`]: clusters of this many
/// known peers or fewer keep a connection to every peer (effectively full mesh),
/// so small clusters and unit tests are unaffected by partial-mesh bounding.
pub const AUTO_FANOUT_FLOOR: usize = 8;

/// Resolve the effective outbound fan-out for a node that currently knows
/// `known_count` peers (excluding itself), given the configured `gossip_fanout`
/// (`0` = auto) and an optional `max_active_connections` hard ceiling (`0` = none).
///
/// Auto fan-out is `2·⌈log2(known_count)⌉` floored at [`AUTO_FANOUT_FLOOR`]; the
/// result is always capped at `known_count` (can't connect to peers you don't know)
/// and at `max_active_connections` when that is non-zero.
pub fn resolved_fanout(gossip_fanout: usize, max_active_connections: usize, known_count: usize) -> usize {
    let mut k = if gossip_fanout > 0 {
        gossip_fanout
    } else {
        // ⌈log2(n)⌉ via bit length; n.max(1) keeps log defined at 0/1.
        let n = known_count.max(1);
        let ceil_log2 = (usize::BITS - (n - 1).leading_zeros()) as usize; // 0 for n=1
        (2 * ceil_log2).max(AUTO_FANOUT_FLOOR)
    };
    if max_active_connections > 0 {
        k = k.min(max_active_connections);
    }
    k.min(known_count).max(known_count.min(1)) // ≥1 when any peer is known
}

/// Unified configuration for all protocol components.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct GossipConfig {
    /// IP address the node binds its TCP listener to.
    pub bind_address: String,
    /// TCP port the node listens on. Must be non-zero.
    pub bind_port: u16,
    /// **Optional cluster / environment name** (e.g. `"prod-eu"`, `"staging"`). Purely a label for
    /// *operator* disambiguation across multiple Mycelium environments — it has **no effect on
    /// gossip, identity, or membership** (a node is still identified by its `node_id`). When set it
    /// is surfaced on `GET /stats` (`cluster_name`), as a `cluster` label on every `/metrics` series,
    /// and (when the AgentFacts lens is mounted) in the federation document — so one Prometheus /
    /// Grafana can tell `prod-eu` from `staging` without external bookkeeping. `None` (default) =
    /// unlabelled. Set via `GOSSIP_CLUSTER_NAME`.
    pub cluster_name: Option<String>,
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
    /// happens to forward the most messages in one generation). The default of 1024
    /// covers that budget at the default fan-out of 4 up to N = 256 agents; bulk-write
    /// bursts (thousands of keys in one window) still want 4096+ — both scale tests
    /// recorded drops at smaller depths (M2 Run-21). Memory is allocated per in-flight
    /// frame, not up front, so a deeper channel costs nothing while idle.
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
    /// Maximum number of simultaneous outbound gossip connections this node maintains.
    ///
    /// Without a cap every node connects to every known peer, creating O(N²) TCP
    /// connections cluster-wide. On a Docker bridge network this saturates the Linux
    /// iptables FORWARD chain at around 50 nodes and causes new connections to time out.
    ///
    /// Bootstrap peers are always included; the remaining slots are filled with a random
    /// selection from the full known-peer set, rotated on each topology change. Gossip
    /// still propagates cluster-wide because Ping messages carry piggybacked peer lists
    /// and inbound connections are not limited. For a cluster of N nodes with K active
    /// connections per node, gossip diameter ≈ log(N)/log(K), which stays below 4 for
    /// K=12 up to thousands of nodes.
    ///
    /// Recommended: 12–20 for clusters of 20+ nodes.
    /// `0` = unlimited (default — full mesh, suitable for small clusters and unit tests).
    ///
    /// Set via `GOSSIP_MAX_ACTIVE_CONNECTIONS`.
    pub max_active_connections: usize,
    /// Target number of *outbound* gossip connections each node actively maintains —
    /// the partial-mesh degree (WS-B M4). The health monitor pings only this many
    /// peers, so only their writers stay warm; the rest idle-close (see
    /// `writer_idle_timeout_secs`). Gossip still reaches the whole cluster because
    /// Ping frames piggyback peer lists, inbound connections are unrestricted, and
    /// Individual/Signal/consensus frames fall back to multi-hop flooding (only
    /// *admission* is scoped). For N nodes at fan-out `k`, the gossip diameter is
    /// ≈ log(N)/log(k), which stays small.
    ///
    /// - `0` = **auto** (default): `k ≈ 2·⌈log2 N⌉`, floored at [`AUTO_FANOUT_FLOOR`]
    ///   and capped at the known-peer count — so clusters of ≤ `AUTO_FANOUT_FLOOR`
    ///   nodes stay effectively full-mesh and only larger clusters are bounded.
    /// - `> 0` = a fixed fan-out `k`.
    ///
    /// `max_active_connections`, when set, is an additional hard ceiling on top of this.
    /// Set via `GOSSIP_FANOUT`.
    pub gossip_fanout: usize,
    /// Enable the SWIM-style UDP failure detector (WS-B M5). **Default `true`** (WS-B M5
    /// Stage-4 cutover — G1 flat `seed_established` + G3 50-worker resilience both green over
    /// Docker). The node binds a UDP socket (see `swim_udp_port`) and runs the SWIM transport
    /// for liveness/heartbeats — connection-free probing that does not consume Docker-bridge
    /// iptables FORWARD / conntrack entries, lifting the O(N²) connection ceiling. TCP is
    /// retained for anti-entropy and Data/Signal delivery. Set `false` (via
    /// `GOSSIP_SWIM_FAILURE_DETECTOR=0`) to fall back to the legacy TCP-ping liveness path.
    ///
    /// **Rolling-upgrade caveat:** SWIM owns liveness when on (the failure detector evicts a
    /// peer that fails direct+indirect UDP probes). A SWIM-on node probing a SWIM-*off* node
    /// (no UDP listener) will mark it Dead. So do **not** mix SWIM-on and SWIM-off nodes in one
    /// cluster — flip the whole cluster together (or pin `=0` during a staged upgrade until all
    /// nodes are on the new binary, then restart into SWIM-on).
    pub swim_failure_detector: bool,
    /// UDP port for the SWIM failure detector. `None` (default) binds the **same port
    /// number as `bind_port`** on a separate UDP socket — the SWIM/`memberlist`
    /// convention (one port to open in firewalls for both protocols). Set an explicit
    /// port only when TCP/UDP must be separated. Ignored unless `swim_failure_detector`
    /// is set. Set via `GOSSIP_SWIM_UDP_PORT`.
    pub swim_udp_port: Option<u16>,
    /// SWIM protocol period in milliseconds — how often the prober picks a random peer
    /// to probe (WS-B M5 Stage 2). Default `500`. Set via `GOSSIP_SWIM_PROBE_INTERVAL_MS`.
    pub swim_probe_interval_ms: u64,
    /// SWIM direct-probe timeout in milliseconds: how long to wait for a direct `Ack`
    /// before falling back to indirect probing. Default `300`. Set via
    /// `GOSSIP_SWIM_PROBE_TIMEOUT_MS`.
    pub swim_probe_timeout_ms: u64,
    /// Number of random relay peers asked to probe the target on our behalf when a direct
    /// probe times out (SWIM indirect probe `k`). Default `3`. Set via `GOSSIP_SWIM_INDIRECT_PROBES`.
    pub swim_indirect_probes: usize,
    /// Number of membership updates piggybacked on each `Ping`/`Ack` (SWIM gossip fan-out,
    /// WS-B M5 Stage 3) — bounded so datagrams stay under the MTU. Default `12`. Set via
    /// `GOSSIP_SWIM_GOSSIP_UPDATES`.
    pub swim_gossip_updates: usize,
    /// Milliseconds a member may stay `Suspect` before being promoted to `Dead` and evicted
    /// if no refutation arrives (SWIM suspicion timeout). Default `4000`. Set via
    /// `GOSSIP_SWIM_SUSPICION_TIMEOUT_MS`.
    pub swim_suspicion_timeout_ms: u64,
    /// Seconds of inactivity after which a peer writer closes its TCP connection.
    ///
    /// The connection is re-established transparently on the next frame destined for that
    /// peer, so this is invisible to callers. Idle writer tasks consume a file descriptor
    /// and a tokio task for every peer ever contacted; setting a timeout bounds that cost
    /// in clusters that churn or where many peers are only occasionally active.
    ///
    /// `0` = no timeout (writers stay connected indefinitely). Default `30` s
    /// (WS-B M4): with partial-mesh fan-out, writers to peers a node no longer
    /// actively pings must close for the connection count to actually drop —
    /// otherwise an anti-entropy one-shot or a dropped-fan-out peer would pin a TCP
    /// socket forever. 30 s comfortably exceeds the 10 s default health-check
    /// interval, so actively-pinged (fan-out) writers stay warm while idle ones close.
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
    /// `manage_opacity`.
    ///
    /// `suggest_leader`, `peer_load`,
    /// and `peer_load_rx` treat entries older than this as
    /// transparent (unloaded). Raise when nodes can be unreachable for longer than the default
    /// before their pheromone entries should be considered stale.
    ///
    /// Use `GossipAgent::signal_window` to read this as a `Duration` — prefer that over
    /// the `SENDER_LOG_WINDOW` compile-time constant in
    /// application code.
    ///
    /// Default: 600 (10 minutes).
    pub signal_window_secs: u64,

    /// Enable causal delivery ordering for signals emitted via
    /// `emit_ordered`.
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
    ///
    /// **Capability gossip overhead:** each call to
    /// `advertise_capability` writes one KV
    /// entry that is re-asserted on every `interval` tick and gossiped to all peers on
    /// each reassertion. With many capabilities per node, gossip bandwidth scales as
    /// `capabilities_per_node × peers × reassertion_rate`. As a practical guideline,
    /// keep per-node capability count below **200** for clusters of up to ~100 peers
    /// on a default 30 s interval; above this threshold consider increasing the interval
    /// or grouping fine-grained capabilities under a single coarser entry.
    pub max_store_entries: usize,

    /// Bound on how far ahead of the local wall clock a *remote* HLC stamp
    /// may pull this node's clock, in milliseconds. `0` disables the bound.
    ///
    /// Set via `GOSSIP_MAX_CLOCK_DRIFT_MS`. Default **300 000** (5 minutes).
    ///
    /// `Hlc::observe` clamps remote physical time to `wall_now + this bound`
    /// (rate-limited `warn!` when it engages). Without the bound, one peer
    /// with a far-future clock drags every node's HLC forward irrecoverably
    /// and read-side evaporation — the substrate's failure detector — is
    /// silently suspended for the full drift duration. Keep the bound well
    /// above your real clock-sync error (NTP keeps clusters within
    /// milliseconds; 5 minutes is generous) but far below any capability
    /// `refresh_interval` you rely on for failover latency.
    pub max_clock_drift_ms: u64,

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

    /// Maximum number of concurrent per-request handler tasks spawned by `bulk_serve`.
    ///
    /// Each incoming bulk signal spawns one background task to fetch-and-respond.
    /// Without a cap, a flood of bulk signals could exhaust Tokio's task budget.
    /// When the concurrency limit is reached, new bulk signals are dropped and a
    /// warning is logged.
    ///
    /// `0` = unlimited (not recommended in production).
    ///
    /// Default: `64`.
    pub max_concurrent_bulk_handlers: usize,

    /// Maximum number of gossip frames accepted per second from a single peer.
    ///
    /// Frames that exceed this rate are silently dropped and a warning is logged.
    /// The counter resets every second on a per-connection basis, so a brief burst
    /// is allowed once per window.
    ///
    /// This is a first-line defence against a malicious or misbehaving peer
    /// flooding the inbound processing pipeline. Set conservatively high enough
    /// that legitimate traffic is never dropped; a small cluster under normal load
    /// generates fewer than 200 fps per connection.
    ///
    /// `0` = unlimited (default — use only in trusted-network environments or tests).
    ///
    /// Set via `GOSSIP_MAX_INBOUND_FRAMES_PER_SEC`.
    pub max_inbound_frames_per_sec: u64,

    /// **Cluster-wide distributed rate-limiting** (WS-C / M7) — *shared observation, local
    /// decision*. When `true`, each node publishes its observed per-peer inbound frame rate to a
    /// short-TTL `sys/rate/{observer}/{sender}` namespace, and a decider task sums the aggregate
    /// across all observers; a sender whose aggregate exceeds `rate_aggregate_threshold_fps` is
    /// **locally** throttled by every node it touches (a fair-share budget = threshold ÷ observers),
    /// never a cluster-wide eviction verdict. Off (`false`) by default — pure per-peer limiting,
    /// zero overhead. Catches a sender that floods many peers at once (each under its per-peer
    /// limit, but caught by the aggregate). Set via `GOSSIP_RATE_OBSERVATION`.
    pub rate_observation_enabled: bool,

    /// The aggregate (summed-across-observers) inbound frame-rate threshold above which M7 throttles
    /// a sender. `0` with `rate_observation_enabled` picks a default of `8 × max_inbound_frames_per_sec`
    /// (or `8000` if that is unset). Set via `GOSSIP_RATE_AGGREGATE_THRESHOLD_FPS`.
    pub rate_aggregate_threshold_fps: u64,

    /// Optional bearer token that protects the language-bridge gateway endpoints.
    ///
    /// When set, every request to a `/gateway/**` path must include the header:
    /// ```text
    /// Authorization: Bearer <token>
    /// ```
    /// Requests without this header — or with the wrong token — receive `401 Unauthorized`.
    ///
    /// Health, readiness, stats, and metrics endpoints (`/health`, `/ready`,
    /// `/stats`, `/metrics`, `/signals/{kind}`) are always public regardless of
    /// this setting, so load-balancer probes keep working without credentials.
    ///
    /// `None` (the default) leaves the gateway unauthenticated — suitable for
    /// loopback-only deployments (`http_addr = "127.0.0.1"`). Set to `Some(token)`
    /// when the HTTP port is exposed beyond localhost.
    ///
    /// Can also be set via the `GOSSIP_GATEWAY_AUTH_TOKEN` environment variable.
    pub gateway_auth_token: Option<String>,

    /// OAuth2-style scoped gateway tokens (`compliance` feature).
    ///
    /// Each [`GatewayToken`] maps a bearer token to a set of `resource:verb`
    /// scopes; gateway routes require a scope and admit a token holding it (or
    /// the `"*"` wildcard). Enforced only under the `compliance` feature — when
    /// the feature is off this list is ignored and the legacy single-token model
    /// (`gateway_auth_token`) applies unchanged. Public routes (`/health`,
    /// `/ready`, `/stats`, `/metrics`, the descriptor path) are never scope-gated.
    ///
    /// Default: empty. A `gateway_auth_token` set alongside an empty list keeps
    /// today's behaviour (one token, full access).
    #[serde(default)]
    pub gateway_scoped_tokens: Vec<GatewayToken>,

    /// Outbound egress allow-policy (WS3). Default: empty = allow all. Set
    /// `allow_hosts` to constrain which external hosts the substrate may reach
    /// (enforced at the MCP client bridge). A node-local posture, not a coordinator.
    #[serde(default)]
    pub egress: EgressPolicy,

    /// Generic-OIDC SSO config for the gateway (WS4, `compliance` feature).
    /// `None` (default) = no OIDC; gateway auth uses bearer tokens / scoped
    /// tokens only. When set, a presented JWT is validated against the IdP's
    /// JWKS and its groups mapped to gateway scopes. Human-operator auth, not
    /// agent identity.
    #[cfg(feature = "compliance")]
    #[serde(default)]
    pub oidc: Option<OidcConfig>,

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
            cluster_name: None,
            bootstrap_peers: Vec::new(),
            propagation_window_secs: 60,
            health_check_interval_secs: 10,
            default_ttl: 5,
            max_connections: 1024,
            writer_channel_depth: 1024,
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
            max_active_connections: 0,
            gossip_fanout: 0,
            swim_failure_detector: true,
            swim_udp_port: None,
            // Membership-gossip rate. Raised from the original 1000 ms / 6 updates: at scale
            // (100 nodes over a lossy bridge) that converged membership to only ~14 known peers
            // — below the de-pin threshold (2k≈24) — so the seed stayed pinned (WS-B M5 Stage-4
            // Docker re-validation). 500 ms / 12 updates ≈ 4× the gossip throughput, lifting
            // N=100 membership over the threshold; one datagram of 13 updates is ~340 B, well
            // under the 512 B MTU budget. Small clusters are unaffected (their sample is tiny).
            swim_probe_interval_ms: 500,
            swim_probe_timeout_ms: 300,
            swim_indirect_probes: 3,
            swim_gossip_updates: 12,
            swim_suspicion_timeout_ms: 4000,
            writer_idle_timeout_secs: 30,
            group_aware_forwarding: true,
            epidemic_extra_peers:   3,
            health_check_max_jitter_ms: 0,
            signal_window_secs: 600,
            signal_ordered_delivery:     false,
            signal_reorder_max_hold_ms:  500,
            signal_reorder_max_depth:    64,
            max_store_entries: 0,
            max_clock_drift_ms: crate::hlc::DEFAULT_MAX_CLOCK_DRIFT_MS,
            locality_path:     Vec::new(),
            topology_policies: HashMap::new(),
            http_port:               None,
            http_addr:               "127.0.0.1".to_string(),
            persistence:             None,
            bulk_fetch_timeout_secs:       30,
            max_concurrent_bulk_handlers:  64,
            max_inbound_frames_per_sec:    0,
            rate_observation_enabled:      false,
            rate_aggregate_threshold_fps:  0,
            gateway_auth_token:            None,
            gateway_scoped_tokens:         Vec::new(),
            egress:                        EgressPolicy::default(),
            #[cfg(feature = "compliance")]
            oidc:                          None,
            tls:                           None,
        }
    }
}

impl GossipConfig {
    /// A self-sizing configuration (WS-C M8): [`Default`] with every formula-driven
    /// tuning field set to its `0` "auto" sentinel, so `GossipAgent::new` derives them
    /// from the cluster-size estimate. Use this to deploy a cluster of any size without
    /// hand-computing tuning values; override any individual field afterwards (an explicit
    /// non-zero value always wins). Fan-out (`gossip_fanout`/`max_active_connections`) is
    /// already auto by default — it is resolved live by [`resolved_fanout`].
    ///
    /// See [`derive_unset`](Self::derive_unset), `docs/operations/tuning.md` §Auto-derivation,
    /// and `docs/plans/v2-wsc-metabolism.md`.
    pub fn auto() -> Self {
        Self {
            default_ttl:             0,
            writer_channel_depth:    0,
            max_seen_entries:        0,
            ping_peer_sample_size:   0,
            propagation_window_secs: 0,
            ..Self::default()
        }
    }

    /// Fills size-derived tuning fields left at their `0` "auto" sentinel with closed-form
    /// values from `n` (an estimate of cluster size N; callers pass a lower bound such as
    /// `bootstrap_peers.len() + 1`). Explicit non-zero values — set in code or via
    /// `GOSSIP_*` env — are left untouched, so operator intent always wins.
    ///
    /// Called by `GossipAgent::new` **before** [`validate`](Self::validate), so a derived
    /// field never trips validate's zero-rejection guards. Idempotent: a second call with
    /// no `0` fields left is a no-op. Fan-out is intentionally *not* handled here — it is
    /// resolved live per known-peer count by [`resolved_fanout`] (the single k-resolution
    /// point), which already treats `gossip_fanout = 0` as auto.
    ///
    /// Formulas (see `docs/operations/tuning.md` §Auto-derivation):
    /// - `default_ttl            = max(5, ⌈log₂(N+1)⌉)`  — covers the gossip diameter (invariant 4)
    /// - `writer_channel_depth   = max(1024, N×4)`       — per-peer burst fan-in floor (invariant 5)
    /// - `max_seen_entries       = max(100_000, N×1000)` — dedup horizon scales with origin count
    /// - `ping_peer_sample_size  = min(N, max(20, ⌊√N⌋))` — bounds Ping fan-in at large N
    /// - `propagation_window_secs= max(60, health×eviction×2)` — ≥ eviction window (invariant 3)
    pub fn derive_unset(&mut self, n: usize) {
        let n = n.max(1);
        // ⌈log₂(m)⌉: smallest k with 2^k ≥ m. 0 for m ≤ 1.
        let ceil_log2 = |m: usize| -> usize {
            if m <= 1 { 0 } else { (usize::BITS - (m - 1).leading_zeros()) as usize }
        };
        if self.default_ttl == 0 {
            self.default_ttl = (ceil_log2(n + 1).max(5)).min(u8::MAX as usize) as u8;
        }
        if self.writer_channel_depth == 0 {
            self.writer_channel_depth = Self::auto_writer_channel_depth(n);
        }
        if self.max_seen_entries == 0 {
            self.max_seen_entries = (n.saturating_mul(1_000)).max(100_000);
        }
        if self.ping_peer_sample_size == 0 {
            self.ping_peer_sample_size = integer_sqrt(n).max(20).min(n);
        }
        if self.propagation_window_secs == 0 {
            self.propagation_window_secs = self
                .health_check_interval_secs
                .saturating_mul(self.peer_eviction_intervals)
                .saturating_mul(2)
                .max(60);
        }
    }

    /// The M8 size-derived `writer_channel_depth` for cluster size `n` (`max(1024, N×4)`).
    /// Exposed so the WS-C M9 `ClusterTuner` recommends the *same* value `derive_unset`
    /// would, with no formula drift between startup derivation and live retuning.
    #[inline]
    pub fn auto_writer_channel_depth(n: usize) -> usize {
        n.max(1).saturating_mul(4).max(1024)
    }

    /// Logs a `warn!` for each soft tuning invariant the **resolved** config violates
    /// (`docs/operations/tuning.md` §Hard invariants). Detection, not prevention: the
    /// values are honoured (an operator may know better), exactly like the consensus /
    /// `sys/` tripwires. `validate()` still hard-rejects the structurally-invalid cases
    /// (zero fields, out-of-range bounds); this covers the cross-field relationships
    /// validate does not. Called by `GossipAgent::new` after [`derive_unset`](Self::derive_unset).
    pub fn audit_invariants(&self) {
        // Invariant 1 — backoff must be shorter than the health-check interval (−2 s margin).
        if self.reconnect_backoff_secs + 2 >= self.health_check_interval_secs {
            tracing::warn!(
                reconnect_backoff_secs = self.reconnect_backoff_secs,
                health_check_interval_secs = self.health_check_interval_secs,
                "tuning invariant 1: reconnect_backoff_secs should be < health_check_interval_secs − 2 \
                 (a peer can otherwise be evicted mid-backoff and never reconnect)"
            );
        }
        // Invariant 3 — propagation window must cover the eviction window.
        let eviction_window = self.health_check_interval_secs.saturating_mul(self.peer_eviction_intervals);
        if self.propagation_window_secs < eviction_window {
            tracing::warn!(
                propagation_window_secs = self.propagation_window_secs,
                eviction_window,
                "tuning invariant 3: propagation_window_secs should be ≥ the eviction window \
                 (health_check_interval_secs × peer_eviction_intervals), else a peer can evaporate \
                 from the seen-set before a slow partition heals"
            );
        }
    }

    /// Validates all numeric constraints.
    ///
    /// Called automatically by `GossipAgent::start` and [`load_from_file`](Self::load_from_file).
    /// Call manually after mutating fields directly to catch errors early.
    pub fn validate(&self) -> Result<(), GossipError> {
        if self.bind_address.is_empty() {
            return Err(GossipError::InvalidField { field: "bind_address", reason: "cannot be empty".into() });
        }
        self.bind_address.parse::<std::net::IpAddr>().map_err(|_| {
            GossipError::InvalidField {
                field: "bind_address",
                reason: format!("'{}' is not a valid IP address", self.bind_address),
            }
        })?;
        if self.bind_port == 0 {
            return Err(GossipError::InvalidField { field: "bind_port", reason: "cannot be zero".into() });
        }
        if self.max_connections == 0 {
            return Err(GossipError::InvalidField { field: "max_connections", reason: "cannot be zero".into() });
        }
        if self.max_connections > 65535 {
            return Err(GossipError::InvalidField {
                field: "max_connections",
                reason: "cannot exceed 65535 (practical file-descriptor budget \
                         per process; each inbound connection consumes one fd)".into(),
            });
        }
        if self.health_check_interval_secs == 0 {
            return Err(GossipError::InvalidField {
                field: "health_check_interval_secs",
                reason: "cannot be zero".into(),
            });
        }
        if self.health_check_interval_secs > 3600 {
            return Err(GossipError::InvalidField {
                field: "health_check_interval_secs",
                reason: "cannot exceed 3600 seconds (1 hour)".into(),
            });
        }
        if self.default_ttl == 0 {
            return Err(GossipError::InvalidField { field: "default_ttl", reason: "cannot be zero".into() });
        }
        if self.propagation_window_secs == 0 {
            return Err(GossipError::InvalidField {
                field: "propagation_window_secs",
                reason: "cannot be zero".into(),
            });
        }
        if self.writer_channel_depth == 0 {
            return Err(GossipError::InvalidField {
                field: "writer_channel_depth",
                reason: "cannot be zero".into(),
            });
        }
        if self.writer_channel_depth < 64 {
            tracing::warn!(
                "writer_channel_depth {} is below 64; frame drops are likely in clusters with more than 16 nodes",
                self.writer_channel_depth,
            );
        }
        if self.gossip_channel_capacity == 0 {
            return Err(GossipError::InvalidField {
                field: "gossip_channel_capacity",
                reason: "cannot be zero".into(),
            });
        }
        if self.max_seen_entries == 0 {
            return Err(GossipError::InvalidField {
                field: "max_seen_entries",
                reason: "cannot be zero".into(),
            });
        }
        if self.peer_eviction_intervals == 0 {
            return Err(GossipError::InvalidField {
                field: "peer_eviction_intervals",
                reason: "cannot be zero".into(),
            });
        }
        if self.gossip_shards == 0 {
            return Err(GossipError::InvalidField { field: "gossip_shards", reason: "cannot be zero".into() });
        }
        if self.reconnect_backoff_secs == 0 {
            return Err(GossipError::InvalidField {
                field: "reconnect_backoff_secs",
                reason: "must be at least 1; set to 1 to retry as aggressively as possible".into(),
            });
        }
        if self.reconnect_backoff_secs > 300 {
            return Err(GossipError::InvalidField {
                field: "reconnect_backoff_secs",
                reason: "cannot exceed 300 seconds; frames to unreachable peers are dropped \
                         during backoff, so large values impair convergence — increase \
                         health_check_interval_secs instead".into(),
            });
        }
        if self.ping_peer_sample_size == 0 {
            return Err(GossipError::InvalidField {
                field: "ping_peer_sample_size",
                reason: "cannot be zero".into(),
            });
        }
        if self.tcp_accept_backlog == 0 {
            return Err(GossipError::InvalidField {
                field: "tcp_accept_backlog",
                reason: "cannot be zero".into(),
            });
        }
        if self.max_peers == 0 {
            return Err(GossipError::InvalidField {
                field: "max_peers",
                reason: "cannot be zero".into(),
            });
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
                return Err(GossipError::InvalidField { field: "http_port", reason: "cannot be zero".into() });
            }
            if p == self.bind_port {
                return Err(GossipError::FieldConflict {
                    field_a: "http_port",
                    field_b: "bind_port",
                    reason:  "must differ".into(),
                });
            }
            if self.http_addr.is_empty() {
                return Err(GossipError::InvalidField { field: "http_addr", reason: "cannot be empty".into() });
            }
            self.http_addr.parse::<std::net::IpAddr>().map_err(|_| {
                GossipError::InvalidField {
                    field:  "http_addr",
                    reason: format!("'{}' is not a valid IP address", self.http_addr),
                }
            })?;
        }
        for seg in &self.locality_path {
            if seg.is_empty() {
                return Err(GossipError::InvalidField {
                    field:  "locality_path",
                    reason: "segments must be non-empty".into(),
                });
            }
        }
        for (group, policy) in &self.topology_policies {
            if policy.enforcement == TopologyEnforcement::Hard {
                if policy.spread_depth.is_none() {
                    return Err(GossipError::InvalidField {
                        field:  "topology_policies",
                        reason: format!("[{group}] requires spread_depth when enforcement = Hard"),
                    });
                }
                if policy.spread_min_distinct < 2 {
                    return Err(GossipError::InvalidField {
                        field:  "topology_policies",
                        reason: format!("[{group}] requires spread_min_distinct >= 2 when enforcement = Hard"),
                    });
                }
            }
        }
        if let Some(p) = &self.persistence {
            if p.snapshot_wal_threshold == 0 {
                return Err(GossipError::InvalidField {
                    field:  "persistence.snapshot_wal_threshold",
                    reason: "cannot be zero".into(),
                });
            }
            if p.snapshot_interval_secs == 0 {
                return Err(GossipError::InvalidField {
                    field:  "persistence.snapshot_interval_secs",
                    reason: "cannot be zero".into(),
                });
            }
            // Writability check: warn and the caller falls back to None at startup.
            // We don't hard-fail here because validate() is also called in load_from_file
            // before the node_id is known, so we can only check the base path itself.
            if !p.base_path.as_os_str().is_empty()
                && let Err(e) = fs::create_dir_all(&p.base_path) {
                    tracing::warn!(
                        path = %p.base_path.display(),
                        error = %e,
                        "persistence.base_path is not writable; node will run in-memory-only mode",
                    );
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
    /// `GOSSIP_MAX_PEERS`, `GOSSIP_MAX_ACTIVE_CONNECTIONS`, `GOSSIP_WRITER_IDLE_TIMEOUT_SECS`,
    /// `GOSSIP_GROUP_AWARE_FORWARDING` (`true`/`false`/`1`/`0`),
    /// `GOSSIP_HEALTH_CHECK_MAX_JITTER_MS`, `GOSSIP_SIGNAL_WINDOW_SECS`,
    /// `GOSSIP_MAX_STORE_ENTRIES`, `GOSSIP_MAX_CLOCK_DRIFT_MS`,
    /// `GOSSIP_EPIDEMIC_EXTRA_PEERS`,
    /// `GOSSIP_GATEWAY_AUTH_TOKEN`.
    pub fn apply_env_overrides(&mut self) -> Result<(), GossipError> {
        if let Ok(v) = env::var("GOSSIP_BIND_ADDRESS") {
            self.bind_address = v;
        }
        if let Ok(v) = env::var("GOSSIP_BIND_PORT") {
            self.bind_port = v.parse().map_err(GossipError::Parse)?;
        }
        if let Ok(v) = env::var("GOSSIP_CLUSTER_NAME") {
            self.cluster_name = if v.trim().is_empty() { None } else { Some(v) };
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
                _ => return Err(GossipError::InvalidField {
                    field:  "GOSSIP_INTERN_KEYS",
                    reason: format!("must be 'true', 'false', '1', or '0', got '{v}'"),
                }),
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
            self.bootstrap_peers = peers.map_err(|e| GossipError::InvalidField {
                field:  "GOSSIP_BOOTSTRAP_PEERS",
                reason: e.to_string(),
            })?;
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
        if let Ok(v) = env::var("GOSSIP_MAX_ACTIVE_CONNECTIONS") {
            self.max_active_connections = v.parse().map_err(GossipError::Parse)?;
        }
        if let Ok(v) = env::var("GOSSIP_FANOUT") {
            self.gossip_fanout = v.parse().map_err(GossipError::Parse)?;
        }
        if let Ok(v) = env::var("GOSSIP_SWIM_FAILURE_DETECTOR") {
            self.swim_failure_detector = match v.as_str() {
                "true" | "1" => true,
                "false" | "0" => false,
                _ => return Err(GossipError::InvalidField {
                    field:  "GOSSIP_SWIM_FAILURE_DETECTOR",
                    reason: "must be true/false/1/0".into(),
                }),
            };
        }
        if let Ok(v) = env::var("GOSSIP_SWIM_UDP_PORT") {
            self.swim_udp_port = Some(v.parse().map_err(GossipError::Parse)?);
        }
        if let Ok(v) = env::var("GOSSIP_SWIM_PROBE_INTERVAL_MS") {
            self.swim_probe_interval_ms = v.parse().map_err(GossipError::Parse)?;
        }
        if let Ok(v) = env::var("GOSSIP_SWIM_PROBE_TIMEOUT_MS") {
            self.swim_probe_timeout_ms = v.parse().map_err(GossipError::Parse)?;
        }
        if let Ok(v) = env::var("GOSSIP_SWIM_INDIRECT_PROBES") {
            self.swim_indirect_probes = v.parse().map_err(GossipError::Parse)?;
        }
        if let Ok(v) = env::var("GOSSIP_SWIM_GOSSIP_UPDATES") {
            self.swim_gossip_updates = v.parse().map_err(GossipError::Parse)?;
        }
        if let Ok(v) = env::var("GOSSIP_SWIM_SUSPICION_TIMEOUT_MS") {
            self.swim_suspicion_timeout_ms = v.parse().map_err(GossipError::Parse)?;
        }
        if let Ok(v) = env::var("GOSSIP_GROUP_AWARE_FORWARDING") {
            self.group_aware_forwarding = match v.as_str() {
                "true" | "1" => true,
                "false" | "0" => false,
                _ => return Err(GossipError::InvalidField {
                    field:  "GOSSIP_GROUP_AWARE_FORWARDING",
                    reason: "must be true/false/1/0".into(),
                }),
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
        if let Ok(v) = env::var("GOSSIP_MAX_CLOCK_DRIFT_MS") {
            self.max_clock_drift_ms = v.parse().map_err(GossipError::Parse)?;
        }
        if let Ok(v) = env::var("GOSSIP_EPIDEMIC_EXTRA_PEERS") {
            self.epidemic_extra_peers = v.parse().map_err(GossipError::Parse)?;
        }
        if let Ok(v) = env::var("GOSSIP_MAX_CONCURRENT_BULK_HANDLERS") {
            self.max_concurrent_bulk_handlers = v.parse().map_err(GossipError::Parse)?;
        }
        if let Ok(v) = env::var("GOSSIP_MAX_INBOUND_FRAMES_PER_SEC") {
            self.max_inbound_frames_per_sec = v.parse().map_err(GossipError::Parse)?;
        }
        if let Ok(v) = env::var("GOSSIP_RATE_OBSERVATION") {
            self.rate_observation_enabled = matches!(v.as_str(), "1" | "true" | "TRUE" | "yes");
        }
        if let Ok(v) = env::var("GOSSIP_RATE_AGGREGATE_THRESHOLD_FPS") {
            self.rate_aggregate_threshold_fps = v.parse().map_err(GossipError::Parse)?;
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
        if let Ok(v) = env::var("GOSSIP_GATEWAY_AUTH_TOKEN") {
            self.gateway_auth_token = Some(v);
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

    // ── WS-B M4 fan-out resolution ────────────────────────────────────────

    #[test]
    fn auto_fanout_keeps_small_clusters_full_mesh() {
        // known ≤ AUTO_FANOUT_FLOOR → keep every known peer (no bounding).
        for known in 0..=AUTO_FANOUT_FLOOR {
            assert_eq!(resolved_fanout(0, 0, known), known, "known={known} should be full mesh");
        }
    }

    #[test]
    fn auto_fanout_grows_logarithmically_for_large_clusters() {
        // 2·⌈log2 N⌉, floored at 8, capped at N.
        assert_eq!(resolved_fanout(0, 0, 16), 8);   // 2·4 = 8
        assert_eq!(resolved_fanout(0, 0, 100), 14); // 2·7 = 14
        assert_eq!(resolved_fanout(0, 0, 1000), 20);// 2·10 = 20
        // Always far below N — this is the O(N²)→O(N·k) win.
        assert!(resolved_fanout(0, 0, 100) < 100);
        assert!(resolved_fanout(0, 0, 1000) < 1000);
    }

    #[test]
    fn explicit_fanout_overrides_auto_but_caps_at_known() {
        assert_eq!(resolved_fanout(5, 0, 100), 5);
        assert_eq!(resolved_fanout(50, 0, 12), 12); // can't exceed known peers
    }

    #[test]
    fn max_active_connections_is_a_hard_ceiling() {
        assert_eq!(resolved_fanout(0, 10, 100), 10); // auto would be 14, ceiling wins
        assert_eq!(resolved_fanout(30, 10, 100), 10);// explicit 30, ceiling wins
        assert_eq!(resolved_fanout(0, 100, 100), 14);// ceiling above auto → auto wins
    }

    #[test]
    fn fanout_is_zero_only_with_no_known_peers() {
        assert_eq!(resolved_fanout(0, 0, 0), 0);
        assert_eq!(resolved_fanout(0, 0, 1), 1);
        assert_eq!(resolved_fanout(8, 0, 0), 0);
    }

    // ── WS3 egress policy gate ────────────────────────────────────────────

    #[test]
    fn egress_empty_allowlist_permits_all() {
        let p = EgressPolicy::default();
        assert!(p.permits_host("anything.example.com"));
        assert!(p.permits_url("http://10.0.0.1:8080/x"));
    }

    #[test]
    fn egress_exact_and_suffix_matching() {
        let p = EgressPolicy { allow_hosts: vec!["api.example.com".into(), ".internal".into()] };
        // exact
        assert!(p.permits_host("api.example.com"));
        assert!(!p.permits_host("evil.example.com"));
        // suffix / subdomain
        assert!(p.permits_host("internal"));
        assert!(p.permits_host("svc.internal"));
        assert!(p.permits_host("a.b.internal"));
        assert!(!p.permits_host("internal.evil.com"));
        // case-insensitive
        assert!(p.permits_host("API.EXAMPLE.COM"));
    }

    #[test]
    fn egress_url_host_extraction_and_fail_closed() {
        let p = EgressPolicy { allow_hosts: vec!["allowed.host".into()] };
        assert!(p.permits_url("https://allowed.host/path?q=1"));
        assert!(p.permits_url("http://user:pw@allowed.host:8443/x"));
        assert!(!p.permits_url("http://blocked.host/x"));
        // Unparseable host under a non-empty allowlist → denied (fail closed).
        assert!(!p.permits_url("not a url"));
        assert!(!p.permits_url("http:///nohost"));
    }

    #[test]
    fn egress_host_of_url_parses_forms() {
        assert_eq!(host_of_url("http://h.example/p").as_deref(), Some("h.example"));
        assert_eq!(host_of_url("https://u:p@h.example:9000/p?x#y").as_deref(), Some("h.example"));
        assert_eq!(host_of_url("h.example:8080/p").as_deref(), Some("h.example"));
        assert_eq!(host_of_url("http://[::1]:8080/p").as_deref(), Some("::1"));
        assert_eq!(host_of_url("http:///nohost"), None);
    }

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

    // ── WS-C M8: startup auto-derivation ─────────────────────────────────────

    #[test]
    fn integer_sqrt_is_floor_sqrt() {
        for n in [0usize, 1, 2, 3, 4, 8, 15, 16, 17, 99, 100, 101, 1000, 1_000_000] {
            let r = super::integer_sqrt(n);
            assert!(r * r <= n && (r + 1) * (r + 1) > n, "isqrt({n}) = {r}");
        }
    }

    /// G-C1: derived values match the documented formula table **and** the resolved
    /// config is valid + satisfies the soft tuning invariants, for a sweep of N.
    #[test]
    fn derive_matches_table_and_invariants() {
        // (N, ttl, writer_depth, seen, ping, propagation) — computed from the §4.2 table.
        let cases = [
            (1usize,    5u8, 1024usize, 100_000usize,   1usize, 60u64),
            (3,         5,   1024,      100_000,         3,      60),
            (20,        5,   1024,      100_000,        20,      60),
            (50,        6,   1024,      100_000,        20,      60),
            (100,       7,   1024,      100_000,        20,      60),
            (1000,     10,   4000,    1_000_000,        31,      60),
        ];
        for (n, ttl, wd, seen, ping, prop) in cases {
            let mut c = GossipConfig::auto();
            // auto() leaves the five formula fields at the 0 sentinel.
            assert_eq!(
                (c.default_ttl, c.writer_channel_depth, c.max_seen_entries,
                 c.ping_peer_sample_size, c.propagation_window_secs),
                (0, 0, 0, 0, 0), "auto() must zero the formula fields");
            c.derive_unset(n);
            assert_eq!(c.default_ttl, ttl, "default_ttl @ N={n}");
            assert_eq!(c.writer_channel_depth, wd, "writer_channel_depth @ N={n}");
            assert_eq!(c.max_seen_entries, seen, "max_seen_entries @ N={n}");
            assert_eq!(c.ping_peer_sample_size, ping, "ping_peer_sample_size @ N={n}");
            assert_eq!(c.propagation_window_secs, prop, "propagation_window_secs @ N={n}");
            // The resolved config must pass validate() (no field left at the 0 sentinel).
            assert!(c.validate().is_ok(), "derived config must validate @ N={n}");
            // Soft invariants on the resolved config (defaults: health=10, evict=3, backoff=5).
            assert!(c.reconnect_backoff_secs + 2 < c.health_check_interval_secs, "invariant 1 @ N={n}");
            assert!(c.propagation_window_secs >= c.health_check_interval_secs * c.peer_eviction_intervals,
                "invariant 3 @ N={n}");
            // default_ttl must cover the gossip diameter ⌈log2(N+1)⌉ (invariant 4).
            let ceil_log2 = if n == 0 { 0 } else { (usize::BITS - n.leading_zeros()) as usize };
            assert!(c.default_ttl as usize >= ceil_log2.min(u8::MAX as usize) || c.default_ttl == 5,
                "invariant 4 @ N={n}");
        }
    }

    /// G-C1 edge: derivation is idempotent and `derive_unset` is a no-op once filled.
    #[test]
    fn derive_is_idempotent() {
        let mut c = GossipConfig::auto();
        c.derive_unset(100);
        let once = c.clone();
        c.derive_unset(100);
        assert_eq!(c, once, "second derive_unset must be a no-op");
        // And deriving on the static Default (no 0 sentinels) changes nothing.
        let mut d = GossipConfig::default();
        let before = d.clone();
        d.derive_unset(9999);
        assert_eq!(d, before, "derive must not touch explicitly-set (non-zero) fields");
    }

    /// G-C3: explicit operator values always win over derivation, regardless of N.
    #[test]
    fn explicit_values_override_derivation() {
        let mut c = GossipConfig {
            default_ttl:             9,
            writer_channel_depth:    7777,
            max_seen_entries:        222_222,
            ping_peer_sample_size:   33,
            propagation_window_secs: 4242,
            ..GossipConfig::default()
        };
        c.derive_unset(1_000_000); // large N would otherwise raise every field
        assert_eq!(c.default_ttl, 9);
        assert_eq!(c.writer_channel_depth, 7777);
        assert_eq!(c.max_seen_entries, 222_222);
        assert_eq!(c.ping_peer_sample_size, 33);
        assert_eq!(c.propagation_window_secs, 4242);
    }

    /// G-C3 (detection-not-prevention): an explicit config that violates a soft invariant
    /// is **honoured** (validate passes) — `audit_invariants` only warns, never rejects.
    #[test]
    fn invariant_violating_explicit_config_is_honoured() {
        let c = GossipConfig {
            // propagation (60) < eviction window (40 × 3 = 120) — trips invariant 3.
            health_check_interval_secs: 40,
            peer_eviction_intervals:    3,
            propagation_window_secs:    60,
            // backoff (39) ≥ health − 2 (38) — trips invariant 1.
            reconnect_backoff_secs:     39,
            ..GossipConfig::default()
        };
        c.audit_invariants(); // emits warns; must not panic
        assert!(c.validate().is_ok(), "soft-invariant violations are warned, not rejected");
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

    /// Serialises every test that mutates `GOSSIP_*` env vars: `apply_env_overrides`
    /// reads ALL of them, so two env tests racing in parallel threads observe each
    /// other's garbage. Recovers from poisoning (a failing env test must not cascade).
    fn env_test_lock() -> std::sync::MutexGuard<'static, ()> {
        static ENV_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
        ENV_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Restores (or removes) an env var on drop. Hold `env_test_lock()` for the
    /// guard's whole lifetime.
    struct EnvGuard(&'static str, Option<String>);
    #[allow(unsafe_code)]
    impl Drop for EnvGuard {
        fn drop(&mut self) {
            // SAFETY: mutations serialised by env_test_lock().
            unsafe {
                match &self.1 {
                    Some(v) => std::env::set_var(self.0, v),
                    None    => std::env::remove_var(self.0),
                }
            }
        }
    }

    #[test]
    #[allow(unsafe_code)]
    fn apply_env_overrides_sets_field() {
        let _lock = env_test_lock();
        let var = "GOSSIP_MAX_SEEN_ENTRIES";
        let _guard = EnvGuard(var, std::env::var(var).ok());
        // SAFETY: mutations serialised by env_test_lock().
        unsafe { std::env::set_var(var, "12345"); }
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

    #[test]
    fn validate_rejects_http_port_equal_to_bind_port() {
        // Falsification probe (analysis Run 23, dims 6/7): the documented
        // "http_port and bind_port must differ" invariant must surface a *typed*
        // FieldConflict — not silently pass validation, and not panic.
        let mut cfg = GossipConfig::default();
        cfg.http_port = Some(cfg.bind_port);
        match cfg.validate() {
            Err(GossipError::FieldConflict { field_a, field_b, .. }) => {
                assert_eq!(
                    (field_a, field_b),
                    ("http_port", "bind_port"),
                    "conflict must name the two offending fields"
                );
            }
            other => panic!("expected FieldConflict for http_port == bind_port, got {other:?}"),
        }
    }

    /// M2 Run-28 probe (dim 7 — configurability): a malformed or out-of-range
    /// `GOSSIP_*` env value must surface as a typed `GossipError::Parse`, never
    /// a panic, and must leave the config field untouched.
    /// PASSED at Run 28 — kept as a regression gate.
    #[test]
    #[allow(unsafe_code)]
    fn apply_env_overrides_rejects_malformed_value_with_typed_error() {
        let _lock = env_test_lock();
        let var = "GOSSIP_BIND_PORT";
        let _guard = EnvGuard(var, std::env::var(var).ok());

        for bad in ["not-a-port", "99999999", "-1", ""] {
            // SAFETY: mutations serialised by env_test_lock().
            unsafe { std::env::set_var(var, bad); }
            let mut cfg = GossipConfig::default();
            let before = cfg.bind_port;
            match cfg.apply_env_overrides() {
                Err(GossipError::Parse(_)) => {
                    assert_eq!(cfg.bind_port, before, "field must be untouched on parse failure ({bad:?})");
                }
                Ok(())   => panic!("malformed GOSSIP_BIND_PORT={bad:?} was silently accepted"),
                Err(e)   => panic!("expected GossipError::Parse for {bad:?}, got {e:?}"),
            }
        }
    }
}
