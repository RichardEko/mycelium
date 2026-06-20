# Deploying Mycelium

**Mycelium is a library, not a platform.** There is no daemon, control plane,
installer, or orchestrator to deploy. You embed the library in your process and
call `GossipAgent::start()`; that process *is* a full mesh node. "Deploying
Mycelium" means deploying *your* binary that embeds it.

> Audience: **DevOps**. The developer-side "how do I embed it" is in the
> [cookbook](../guide/cookbook.md).

## The minimum

```rust
let node = NodeId::new("0.0.0.0", 7946)?;
let mut cfg = GossipConfig::default();
cfg.bind_port = 7946;                                  // gossip (TCP)
cfg.bootstrap_peers = vec![NodeId::new("seed.internal", 7946)?];
let agent = Arc::new(GossipAgent::new(node, cfg));
agent.start().await?;
```

A node needs: a **gossip port** and at least one **bootstrap peer** to find the
mesh (a seed needs none). Everything else is optional.

## Ports

| Config | Purpose | Default |
|---|---|---|
| `bind_port` | gossip transport (TCP, and SWIM UDP if enabled) — node-to-node | required |
| `http_port` | the embedded gateway (diagnostics, AgentFacts, `/gateway/*`) | `None` (off) |
| `http_addr` | interface the gateway binds | `127.0.0.1` |

`http_port` must differ from `bind_port`. Leave `http_port = None` for a
headless node; set it (and `http_addr = "0.0.0.0"`) to expose the gateway. See
[observability.md](observability.md) for what the gateway serves.

## Seeds & bootstrapping

There is no special "master." A **seed** is just a node others list in their
`bootstrap_peers`; it has no extra role and can fail without electing a
replacement (peers re-bootstrap off any reachable member). Run 2–3 seeds for
redundancy and point everyone at all of them. Topology, sizing, and partition
recovery: [guide 13 · Cluster topology](../guide/13-cluster-topology.md).

## TLS / identity (recommended)

```rust
cfg.tls = Some(TlsConfig { auto_cert_dir: "/var/lib/mycelium/tls".into(), ..Default::default() });
```

With `tls` set, the node generates an Ed25519 identity + a CA-signed cert into
`auto_cert_dir` on first start, and all gossip is mTLS. **Every node in one
cluster must trust the same CA** — share the CA cert (`{auto_cert_dir}/ca-cert.pem`)
across nodes, or point them at a shared `auto_cert_dir`. The same key is the
node's identity for signed KV, consensus, audit, and AgentFacts. Rotation
without disruption: [cert-rotation.md](cert-rotation.md). Compliance features
(RBAC, audit, OIDC) build on this: [rbac.md](rbac.md), [audit.md](audit.md),
[sso.md](sso.md).

## Containers / Compose

No special base image — it's your Rust binary. Expose `bind_port` (and
`http_port` if used), give each node a stable address its peers can reach, and
mount a volume for `auto_cert_dir` (so identity survives restarts) and for the
WAL/persistence path if enabled. Reference multi-node setups:
[`examples/community`](../../examples/community/) (skillrunner cluster) and the
`tests/integration/docker-compose.*.yml` files.

## Sizing & tuning

Most defaults auto-derive from cluster size (WS-C). For large clusters or
constrained nodes, [tuning.md](tuning.md) covers `gossip_shards`,
`writer_channel_depth`, health/anti-entropy intervals, and
`GOSSIP_MAX_ACTIVE_CONNECTIONS` (the partial-mesh cap that avoids the O(N²)
connection ceiling). These can be set live — see [dynamic-scaling.md](dynamic-scaling.md).

## Restart behaviour

A restarted node re-bootstraps, re-learns the full KV state via anti-entropy,
and re-advertises its capabilities — there is no rejoin ceremony. With a
persisted `auto_cert_dir` it keeps its identity; with persistence enabled it
replays its WAL. Capability advertisements evaporate while a node is down and
reappear on restart (see [00 · Concepts](../guide/00-concepts.md) on evaporation).
