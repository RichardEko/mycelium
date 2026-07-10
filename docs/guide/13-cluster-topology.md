# Chapter 13 — Cluster Topology and Seed Configuration

How nodes find each other, how the gossip mesh forms, and how to size and
structure the seed set for different deployment shapes.

---

## What actually makes a node part of a cluster

There is **no cluster object, no join API, and no registry** — membership is *emergent*. A node
is part of a cluster if two things hold:

1. **It can reach and gossip with the cluster's nodes.** You bootstrap by connecting to
   `bootstrap_peers`; gossip **peer-exchange** then spreads the full membership (nodes learn
   about peers they were never told about). You're in the cluster if the gossip reaches you.
2. **It passes admission** — with the `tls` feature, a peer is admitted only if it presents a
   certificate signed by the cluster's **CA** (`tls.ca_cert`). *That CA is the real definition
   of "this cluster"*: the set of mutually-reachable nodes holding a cert from the same CA.
   Without TLS there is no cryptographic gate — any node that can reach the port and speak wire
   v12 joins.

> **`cluster_name` does NOT define membership.** It is a **label only** — surfaced on `/stats`,
> as a `/metrics` `cluster` label, and in AgentFacts. It has *no effect on gossip or identity*.
> Two nodes with **different** `cluster_name`s on the same reachable network still form **one**
> cluster; the **same** `cluster_name` does **not** join two nodes that can't reach or
> authenticate each other. To *isolate* clusters, run a **separate mesh** — its own CA (or an
> unreachable network) — **not** a different name. (The cluster is also the data-isolation
> boundary: KV floods every node in it. See the guide on [security](09-security.md).)
>
> **Setting it** (optional; default = unlabelled): `GOSSIP_CLUSTER_NAME=prod-eu`, or
> `GossipConfig { cluster_name: Some("prod-eu".into()), .. }`. Read it back with
> `agent.cluster_name()`. Its whole job is telling *one* Prometheus/Grafana apart from several
> Mycelium environments (the `cluster="prod-eu"` metric label) — see
> [operations/observability](../operations/observability.md#naming-environments--monitoring-many-clusters).

## Scope vocabulary: `Cluster · Group · Individual`

The mesh has exactly three addressing scopes — a nesting, all → subset → one — used by both
signals (`SignalScope`) and consensus:

| Scope | Reaches | Signal | Consensus |
|---|---|---|---|
| **`Cluster`** | every node in the cluster | `SignalScope::Cluster` | `cluster_propose` |
| **`Group(name)`** | only nodes that joined the named group | `SignalScope::Group` | `group_propose` / `cross_group_propose` |
| **`Individual(node)`** | one specific node (RPC, votes) | `SignalScope::Individual` | — |

The relationship: `node ∈ group(s) ⊆ cluster`. **Cluster-scope = the whole cluster** — it is an
*addressing mode*, not a fourth topology level; there is nothing between cluster and group.

**Defining and monitoring a group** (who's in it, is it healthy): see the cookbook recipe
[*"How do I define a group, and monitor who's in it?"*](cookbook.md#how-do-i-define-a-group-and-monitor-whos-in-it)
— `join_group` / `define_capability_group` to define; `mesh().group_members(name)`,
`capabilities().resolve(filter)`, and `fleet_snapshot().governed_groups` to monitor.
**Monitoring the whole cluster** is `/stats` · `/metrics` · `/gateway/fleet` · `/gateway/diagnose`
→ [operations/observability](../operations/observability.md).

> **Note on "system".** Before 2026-07-10 cluster-scope was called `System` (`SignalScope::System`,
> `system_propose`, scope string `"system"`) — renamed to `Cluster` because it kept getting
> confused with the *cluster*. The old names still work: `system_propose` is a `#[deprecated]`
> alias, and the gateway/SDKs still accept `"system"`. The **one** surviving "system" is
> `system_stats()` — that is deliberate: it reports *this node's* runtime/protocol state, which is
> node-local, not a scope.

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
| `writer_channel_depth` | 1024 | Covers `N × fanout` up to N=256 at fan-out 4; raise beyond that or for bulk-write bursts |
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
| 1–5 nodes | N/A | 0 (unlimited) | 1024 (default) | Full mesh by default |
| 6–20 nodes | 2 | 0 (unlimited) | 1024 (default) | Monitor `dropped_frames` |
| 21–50 nodes | 2–3 | 12 | 1024 (default) | Partial mesh; raise `ping_peer_sample_size` to 16 |
| 51–100 nodes | 2–3 | 12 | 1024 (default) | Set `max_active_connections` **before** 50 — see cliff note below |
| 101–500 nodes | 3 | 15–20 | 2048 | Raise `ping_peer_sample_size` to 32 |
| 500+ nodes | 3–5 | 20 | 4096 | Consider multi-datacenter seed placement |

### The O(N²) TCP connection cliff

With `max_active_connections = 0` (unlimited), each node opens one outbound
TCP connection to every peer it knows about. For a cluster of N nodes this
produces N×(N−1)/2 total connections across the cluster:

| N | Total TCP connections (unlimited) | Docker bridge iptables FORWARD entries |
|---|---|---|
| 10 | 45 | ~45 |
| 20 | 190 | ~190 |
| 50 | 1 225 | ~1 225 — marginal on default bridge |
| 100 | 4 950 | **saturates** — new SYN packets dropped |
| 200 | 19 900 | well beyond Linux bridge default limits |

**The cliff is at approximately 50 nodes under Docker's default bridge driver.**
Above that, new TCP connections start timing out at the OS level (errno 110,
~2 min). This is not a Mycelium bug — it is a Linux bridge iptables FORWARD
chain limitation. The mitigation is `max_active_connections`:

```rust
// Each node maintains at most K outbound connections
config.max_active_connections = 12;
```

With K=12, gossip still propagates cluster-wide in ≈ `log(N) / log(K)` hops
(≤ 4 for N up to 20 000). Capability advertisements and anti-entropy are
equally unaffected because they use the existing connection pool.

**Set `max_active_connections` before your cluster reaches 50 nodes, not after.**
Once the iptables chain saturates, new connections start silently failing.

Alternative infrastructure fixes that remove the cliff entirely:
- Switch Docker network driver to `macvlan` (bypasses bridge iptables)
- Enable Linux `nftables` (hash-table replacement for the linear iptables chain)

A v2 structural fix (SWIM-style hybrid TCP/UDP transport) is on the roadmap —
see `ROADMAP.md` *v2.0 Milestones* item 5.

---

## Avoiding common mistakes

**Listing workers as seeds** — Workers that restart or scale to zero will
cause join failures for other nodes. Use dedicated, long-lived seed processes.

**Single seed in production** — One seed going down for maintenance blocks
all new joins. Use at least two seeds on separate hosts.

**Unlimited connections on large clusters** — O(N²) TCP connections saturate
the Linux bridge iptables FORWARD chain at ~50 nodes in Docker. Set
`max_active_connections = 12` for any cluster that may exceed 20 nodes.

**`writer_channel_depth` too small** — The default of 1024 covers `N × fanout`
up to N=256 at the default fan-out of 4. Size up beyond that, and for
bulk-write bursts (thousands of keys in one window) go to 4096+ — the
entry-volume scale test recorded drops at 4096 under a 5 000-key burst.
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

---

## Reference — the Docker integration-test cluster as a developer template

*Moved from the repo README (2026-07-10).*

The overlay scenarios in `tests/overlay/` are designed as copy-paste templates:

| Scenario | Pattern demonstrated |
|---|---|
| [`s11_task_auction.py`](../../tests/overlay/scenarios/s11_task_auction.py) | Exact-once task delivery — coordinator queues work, workers race via `subscribe_log_group` |
| [`s12_leader_election.py`](../../tests/overlay/scenarios/s12_leader_election.py) | Leader election + consensus-durable config — 3 concurrent `elect_leader` calls must converge, winner writes `consistent_set` |
| [`s13_shared_reasoning_log.py`](../../tests/overlay/scenarios/s13_shared_reasoning_log.py) | Multi-writer append — 3 nodes each write observations, all verify HLC ordering and gossip convergence |

```sh
make test-overlay   # 3-node Docker cluster, runs all three scenarios (~3 min on warm cache)
```

See [`tests/overlay/README.md`](../../tests/overlay/README.md) for the full developer guide.

---
