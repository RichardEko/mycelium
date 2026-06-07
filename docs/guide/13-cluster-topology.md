# Chapter 13 — Cluster Topology and Seed Configuration

How nodes find each other, how the gossip mesh forms, and how to size and
structure the seed set for different deployment shapes.

---

## How bootstrap works

`bootstrap_peers` is not a coordinator or a single point of truth. It is
a **contact list** — the addresses of a few well-known nodes a new node
dials immediately on startup to introduce itself. Once a handshake completes,
the new node learns the rest of the cluster through piggybacked peer lists
in Ping messages. From that point the seed nodes are peers like any other.

```
Node D starts
  │
  ├── dials seed-1:7947  →  receives Ping with known_peers: [seed-2, A, B, C]
  └── dials seed-2:7947  →  confirms topology
        │
        └── D dials A, B, C … topology converges over several ping rounds
```

**The bootstrap_peers list is read exactly once, at startup.** It has no
effect after `start()` returns. A seed node crashing after the cluster is
fully formed is invisible to existing members.

---

## Topology shapes

### 1 — Single seed (simplest, not HA)

```rust
config.bootstrap_peers = vec![NodeId::new("seed.internal", 7947)?];
```

Every worker dials the seed. Once the seed's peer list propagates, workers
discover each other and form a full mesh (or a partial mesh if
`max_active_connections` is set).

**Use when:** development, single-datacenter demos, clusters of ≤ 10 nodes.

**Risk:** if the seed is down when a new node starts, the new node cannot
join. Existing members are unaffected — they already know each other.

---

### 2 — Two seeds (recommended minimum for production)

```rust
config.bootstrap_peers = vec![
    NodeId::new("seed-1.internal", 7947)?,
    NodeId::new("seed-2.internal", 7947)?,
];
```

A joining node tries both in parallel; one reachable seed is sufficient.
The two seeds should be on separate hosts (or failure domains) so a single
failure does not block new joins.

**Use when:** production deployments of any size where new nodes may join
at any time.

---

### 3 — Three seeds (recommended for large or long-lived clusters)

Three seeds tolerate one seed being down for maintenance while still
providing redundancy. This matches typical Consul/etcd seed patterns and
is the right choice for clusters that need to survive rolling restarts of
the seed set itself.

```
GOSSIP_BOOTSTRAP_PEERS=seed-1:7947,seed-2:7947,seed-3:7947
```

**Use when:** clusters > 20 nodes, multi-AZ deployments, long-lived
production clusters.

---

### 4 — Full bootstrap mesh (all nodes listed as seeds)

```rust
// Every node lists every other node
config.bootstrap_peers = vec![
    NodeId::new("node-a", 7947)?,
    NodeId::new("node-b", 7947)?,
    NodeId::new("node-c", 7947)?,
];
```

Each node immediately connects to all others. There is no dependency on a
subset of long-lived seeds. This works well for **static, small clusters**
where the full address list is known at build time (e.g. a 3- or 5-node
embedded appliance, a fixed edge cluster).

**Implications:**
- Topology is fixed at the bootstrap list — no dynamic discovery needed.
- If any node is temporarily down, others do not notice during their own
  startup because they reach the remaining nodes.
- Set `max_active_connections = 0` (unlimited) and `max_peers = cluster_size`
  to keep the topology exactly as configured and prevent the health monitor
  from adding transient peers.

**Do not use for dynamic clusters** — adding a new node requires redeploying
config to every existing node, which is operationally expensive.

---

### 5 — Partial seeds with `max_active_connections` (large dynamic clusters)

For clusters of 20+ nodes, O(N²) connections become expensive. The gossip
still propagates cluster-wide with fewer connections because Ping messages
carry piggybacked peer lists — nodes learn the full topology even without
connecting to every peer.

```rust
config.bootstrap_peers = vec![
    NodeId::new("seed-1", 7947)?,
    NodeId::new("seed-2", 7947)?,
];
config.max_active_connections = 12;  // each node maintains ~12 outbound TCP conns
```

Bootstrap peers are **always included** in the active connection set.
The remaining slots (`max_active_connections - len(bootstrap_peers)`) are
filled from discovered peers, rotated on each topology change.

Gossip propagation with K active connections per node reaches the full
cluster in ≈ `log(N) / log(K)` hops. At K=12 this is ≤ 4 hops for N up to
20 000 nodes.

**Environment variable:** `GOSSIP_MAX_ACTIVE_CONNECTIONS=12`

---

## The role of seed nodes in practice

| Property | Detail |
|---|---|
| **Seeds are not coordinators** | They hold no special state. Any node can be a seed for any other node. |
| **Seeds are soft-coordinators for joins** | If all seeds are unreachable, new nodes cannot join. Existing members continue operating. |
| **Seeds do not need persistence** | A seed that restarts re-joins normally via its own bootstrap peers and receives full state via anti-entropy within one ping round. |
| **Seeds should be long-lived** | Use dedicated seed nodes (not workers) that start before workers and outlive them. They need minimal CPU/RAM — they carry the same KV state as any other node. |
| **Seeds should not be the sole persistence nodes** | Any node with `persistence` configured survives a clean restart. You do not need to route persistence through seeds. |

---

## Key configuration parameters

| Parameter | Default | When to change |
|---|---|---|
| `bootstrap_peers` | `[]` | Always set in production |
| `max_active_connections` | `0` (unlimited) | Set to 12–20 for clusters of 20+ nodes |
| `max_peers` | unlimited | Cap for fixed-topology clusters; leave unlimited for dynamic |
| `max_forwarding_peers` | unlimited | Set to `bootstrap_peers.len()` to pin topology to the seed mesh |
| `ping_peer_sample_size` | 8 | Raise to 16–32 on large clusters for faster topology convergence |
| `writer_channel_depth` | 64 | Raise to `N × fanout` (≥ 1024 at N=256) to avoid dropped frames |
| `reconnect_backoff_secs` | 5 | Raise to 30–60 on large clusters to avoid reconnect storms after partitions |
| `health_check_interval_secs` | 5 | Controls how quickly dead peers are detected and evicted |
| `peer_eviction_intervals` | 3 | Evict after this many missed pings (default: 15 s at 5 s interval) |

All parameters are settable via environment variables (prefix: `GOSSIP_`).
See `GossipConfig::apply_env_overrides` or run with `RUST_LOG=debug` to
log the resolved config at startup.

---

## Cluster startup ordering

**Recommended startup order:**

1. Start seed nodes first and wait for them to bind their TCP ports.
2. Start worker nodes. Each worker dials at least one seed and joins.

There is no strict requirement — nodes retry bootstrap dials with exponential
backoff until they succeed. However, if workers start before any seed is up,
they will retry for up to `reconnect_backoff_secs` seconds per attempt and
may log connection errors during this window.

**In Docker Compose** use `depends_on` with a health check on the seed's
`/ready` HTTP endpoint:

```yaml
worker:
  depends_on:
    seed:
      condition: service_healthy
seed:
  healthcheck:
    test: ["CMD", "curl", "-sf", "http://localhost:8081/ready"]
    interval: 2s
    retries: 10
```

---

## Partition and recovery behaviour

When a network partition heals:
- The TCP connection to the peer is re-established (with `reconnect_backoff_secs` jitter).
- Immediately on reconnect, `request_state()` fires an anti-entropy `StateRequest` to the peer.
- The peer responds with all KV keys that the rejoining node is missing.
- Convergence completes within one round-trip after the connection is restored.

**Partition-isolated seeds do not cause data loss.** Seeds hold a copy of the
KV store like every other node. During a partition, both sides continue operating
independently. On heal, LWW resolution converges each key to the highest-HLC
write. No coordinator is required; no recovery procedure is needed.

---

## Sizing worksheet

| Cluster size | Seeds | `max_active_connections` | `writer_channel_depth` | Notes |
|---|---|---|---|---|
| 1–5 nodes | N/A | 0 (unlimited) | 64 (default) | Full mesh by default |
| 6–20 nodes | 2 | 0 (unlimited) | 256 | Monitor `dropped_frames` |
| 21–100 nodes | 2–3 | 12 | 512–1024 | Partial mesh; raise `ping_peer_sample_size` to 16 |
| 101–500 nodes | 3 | 15–20 | 2048 | Raise `ping_peer_sample_size` to 32 |
| 500+ nodes | 3–5 | 20 | 4096 | Consider multi-datacenter seed placement |

---

## Avoiding common mistakes

**Listing workers as seeds** — Workers that restart or scale to zero will
cause join failures for other nodes. Use dedicated, long-lived seed processes.

**Single seed in production** — One seed going down for maintenance blocks
all new joins. Use at least two seeds on separate hosts.

**Unlimited connections on large clusters** — O(N²) TCP connections saturate
the Linux bridge iptables FORWARD chain at ~50 nodes in Docker. Set
`max_active_connections = 12` for any cluster that may exceed 20 nodes.

**`writer_channel_depth` too small** — The default of 64 is correct only for
clusters of ≤ 16 nodes. At N=256 with fan-out 4 the correct size is ≥ 1024.
Monitor `system_stats().dropped_frames`; a non-zero value means frames are
being silently discarded.

**No persistence on seeds** — Seeds that restart with no persistence re-join
and receive state via anti-entropy, but there is a brief window where
joining nodes learn from a seed that has not yet converged. Give seeds
persistence (`SyncMode::Flush`) to eliminate this window:

```rust
seed_config.persistence = Some(PersistenceConfig {
    path: "/data/mycelium.db".into(),
    sync_mode: SyncMode::Flush,
});
```
