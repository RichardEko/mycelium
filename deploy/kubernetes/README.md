# Mycelium on Kubernetes — reference cluster

A ready-to-apply cluster: **1 seed + N workers + a management dashboard**, wired the way
[`docs/operations/deployment.md`](../../docs/operations/deployment.md) describes (StatefulSet +
headless Service, seed-DNS bootstrap, `/ready`+`/health` probes, `/metrics` scrape annotations).
It is the same on any conformant Kubernetes — [kind](https://kind.sigs.k8s.io) locally, or EKS /
GKE / AKS for a real multi-host cluster — which is what makes it the answer to *"see Mycelium on
multiple machines"* and *"scale past the single-host ceiling"* in one artifact.

> **Mycelium is a library, not a platform.** There is no Mycelium daemon or control plane; a
> "node" here is the `three_node_demo` example binary (`docker/Dockerfile`) embedding
> `GossipAgent`, configured entirely by the `MYCELIUM_*` / `GOSSIP_*` env vars these manifests set.
> This is a *reference* deployment to copy and adapt, not a product to install.

## 1. Build & push the node image

The manifests reference the image name `mycelium-demo`. Build it from the repo root and push it
to a registry your cluster can pull from:

```sh
docker build -t ghcr.io/you/mycelium:v2.0.0 -f docker/Dockerfile .
docker push  ghcr.io/you/mycelium:v2.0.0
```

Point the manifests at your image by editing the `images:` block in
[`kustomization.yaml`](kustomization.yaml) (one place, whole cluster):

```yaml
images:
  - name: mycelium-demo
    newName: ghcr.io/you/mycelium
    newTag:  v2.0.0
```

For a **local kind cluster** you can skip the registry: `kind load docker-image mycelium-demo:latest`.

## 2. Deploy

```sh
kubectl apply -k deploy/kubernetes
kubectl -n mycelium rollout status statefulset/mycelium-seed
kubectl -n mycelium get pods -l app=mycelium -o wide     # -o wide shows which host each pod lands on
```

## 3. Watch it converge

```sh
kubectl -n mycelium port-forward svc/mycelium-mgmt 8090:8090
# → open http://localhost:8090 : the mesh dashboard. Watch workers appear, form the mesh,
#   and (kill a pod) evaporate + heal.
```

Or read a node's gateway directly:

```sh
kubectl -n mycelium port-forward statefulset/mycelium-worker 8300:8300
curl localhost:8300/stats      # membership, KV counts, dropped_frames
curl localhost:8300/metrics    # Prometheus exposition (the metrics in docs/operations/metrics.md)
```

## 4. Scale

```sh
kubectl -n mycelium scale statefulset mycelium-worker --replicas=50
```

**Why this scales past the Docker harness.** The in-repo scale harness
(`make test-scale`, [scale-tests.md](../../docs/wiki/dev/testing/scale-tests.md)) tops out near
**100 nodes on one host**: every container shares one Docker bridge, and the Linux bridge's
iptables FORWARD chain grows O(N²), so new connections start timing out. That ceiling is
*environmental — a single-host artifact, not a Mycelium limit*. On a multi-node Kubernetes cluster
the worker pods spread across hosts (`kubectl get pods -o wide` to confirm the spread), so each
host carries only its share of the connections and its own conntrack/iptables state. Add hosts →
add headroom. This is the "next level" past the single-VM harness.

To spread deliberately, add a `topologySpreadConstraint` or pod anti-affinity to
`worker.yaml` so the scheduler fans workers across nodes rather than packing them.

## What this reference does **not** include (adapt before production)

Deliberately minimal — [`docs/operations/production-readiness.md`](../../docs/operations/production-readiness.md)
is the gate. Notably:

- **No TLS.** Nodes gossip unauthenticated (as the scale harness does). Production needs the `tls`
  feature with a mounted `auto_cert_dir` — see [`cert-rotation.md`](../../docs/operations/cert-rotation.md).
  The node image would need a build with `--features tls` and a code/config path setting `cfg.tls`
  (there is no `GOSSIP_*` env var for the cert dir).
- **No persistence.** No `volumeClaimTemplates` — WAL and node identity are ephemeral, so a
  restarted pod rejoins as a fresh node. For durable identity/state, add a PVC per the deployment
  guide's "persistent identity + WAL volume" note.
- **No gateway auth / Ingress.** The gateway is cluster-internal only. Set `GOSSIP_GATEWAY_AUTH_TOKEN`
  and front it with an Ingress + TLS if you expose it.
- **Fixed resource requests.** The `requests`/`limits` are placeholders; size them from
  [`tuning.md`](../../docs/operations/tuning.md) for your workload.

## Validation status

These manifests are **rendered and structurally validated offline** (`kubectl kustomize` → 7
well-formed resources). They have **not** been applied to a live cluster in CI — apply them to
kind or a cloud cluster to exercise them end-to-end. If you hit an issue, the node config surface
is the `MYCELIUM_*`/`GOSSIP_*` table in [`deployment.md`](../../docs/operations/deployment.md).
