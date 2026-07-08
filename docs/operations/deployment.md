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

## Naming the cluster

Set `GOSSIP_CLUSTER_NAME` (or `GossipConfig::cluster_name`) to label the
environment — `prod-eu`, `staging`, … It is a pure operator label (no effect on
gossip, identity, or membership) that flows to `/stats`, the `/metrics` `cluster`
label, and AgentFacts, so one monitoring stack can tell environments apart. See
[observability.md](observability.md#naming-environments--monitoring-many-clusters).

## Containers / Compose

No special base image — it's your Rust binary. Expose `bind_port` (and
`http_port` if used), give each node a stable address its peers can reach, and
mount a volume for `auto_cert_dir` (so identity survives restarts) and for the
WAL/persistence path if enabled. Reference multi-node setups:
[`examples/community`](../../examples/community/) (skillrunner cluster) and the
`tests/integration/docker-compose.*.yml` files.

## Cloud / Kubernetes / bare metal

Mycelium ships **no Helm chart** and no opinionated, hardened product packaging — and that
is deliberate: a node is just a binary/container, and packaging would only track cloud churn
while every org's topology differs. It *does* ship **reference deployment scaffolding** to
copy and adapt: Kubernetes manifests —
[`deploy/kubernetes/`](../../deploy/kubernetes/) (`kubectl apply -k deploy/kubernetes`): a
seed StatefulSet + headless Service, a scalable worker StatefulSet, and a mgmt dashboard,
wired exactly as this section describes — and Terraform for the cluster itself —
[`deploy/terraform/`](../../deploy/terraform/) (EKS + ECR, or GKE + Artifact Registry), so
the whole path is `terraform apply` → push image → `kubectl apply -k`. Deploy it (or your
own) like any **stateful** service, minding two requirements that follow from the design:

1. **Stable network identity.** A node's `node_id` is its `host:port`; peers
   bootstrap to it by address. Each node needs an address that survives a restart
   (a static IP, a DNS name, or a Kubernetes *headless Service* + StatefulSet pod
   DNS like `node-0.gossip.svc`). Don't put gossip behind a round-robin load
   balancer — peers must reach *specific* nodes.
2. **Persistent identity + WAL.** Mount a durable volume for `auto_cert_dir` (so
   the Ed25519 identity survives restarts) and, if persistence is on, the WAL path
   (so state replays). On k8s that's a `volumeClaimTemplates` PVC per pod.

**Kubernetes:** a **StatefulSet** (stable pod identity + per-pod PVC) behind a
**headless Service** (stable per-pod DNS for bootstrap) is the natural fit; set
`GOSSIP_BOOTSTRAP_PEERS` (or the demo image's `MYCELIUM_PEERS`) to a seed pod's DNS
name, `readinessProbe` → `/ready`, `livenessProbe` → `/health`, and
`GOSSIP_CLUSTER_NAME` to the environment. Scrape `/metrics` with a `ServiceMonitor`.
The ready-to-apply manifests in [`deploy/kubernetes/`](../../deploy/kubernetes/) do
exactly this — start there rather than from scratch, and see
[`deploy/terraform/`](../../deploy/terraform/) to provision the EKS/GKE cluster they run on. **AWS/GCP/bare metal:** an instance/ECS-task
per node with a stable address (Elastic IP / internal DNS) + a durable disk (EBS /
PD) for `auto_cert_dir` + WAL; a sample systemd unit is just `ExecStart=mycelium`
with the `GOSSIP_*` env in the unit's `Environment=`. Elastic membership (add/remove
nodes) is then driven by [dynamic-scaling.md](dynamic-scaling.md) — the governors
self-heal the count; the substrate needs no orchestrator hook.

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

## Backup & restore

Persistence is a **WAL + periodic snapshot** in the node's data directory, and identity is
the Ed25519 key/cert under `auto_cert_dir`. Both are plain on-disk state, so backup and
restore are just directory operations:

- **Back up** the persistence data dir *and* the `auto_cert_dir` (the identity). Snapshot
  the volume, or copy the dirs while the node runs — the WAL makes a copy taken mid-write
  self-consistent on replay.
- **Restore** = put the dirs back where the node expects them and start it. On boot it
  replays the WAL up to the latest snapshot, then re-bootstraps and re-learns any newer KV
  from peers via anti-entropy (same path as [restart](#restart-behaviour)). Keeping
  `auto_cert_dir` means the node comes back with the *same* identity — no re-issue, no
  signature churn, and its audit/consensus history stays attributable to it.

Because state also lives redundantly across the mesh, a single node's data dir is not the
only copy of the cluster's KV — but it **is** the only copy of that node's identity and its
per-node audit chain ([audit.md](audit.md)), so the `auto_cert_dir` is the part you cannot
regenerate. Back it up. (There is no snapshot/restore *API* — Mycelium is a library; the
data dir is the interface.)
