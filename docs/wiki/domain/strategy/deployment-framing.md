# Library, not platform — the binding deployment framing

↑ [strategy](strategy.md) · user-confirmed; binding on all docs and explanations

Mycelium is an embedded Rust library: **no daemon, no control plane, no broker**. A cluster
is *emergent from network reachability*. Deployment is whatever the devops team already
uses (K8s, cloud, Puppet…). Each cluster is standalone and isolated by construction; the
mgmt view sees only its own mesh. Fleet/datacenter observability belongs to the operator's
stack (Prometheus/Grafana, Datadog) aggregating each cluster's `/metrics` — never a
Mycelium cross-cluster feature (the `cluster_name` label exists precisely to support the
operator's aggregation). The correct analogy: Hazelcast-embedded / etcd-embedded. Never
write "platform" or "runtime cluster".
