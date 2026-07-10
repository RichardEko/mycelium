# 2026-07-10 — scope terminology: System → Cluster

Renamed the cluster-wide scope from `System` to `Cluster` (user-driven, conceptual-integrity):
the scope triple is now `Cluster · Group · Individual` (all/subset/one). `SignalScope::System`
→ `Cluster` (wire-compatible — same numeric tag `0`, no version bump); `system_propose` →
`cluster_propose` (`#[deprecated]` alias kept); gateway/SDK scope string `"cluster"` (default),
`"system"` still accepted. `system_stats()` untouched — it is node-local runtime state, the one
legitimate remaining "system". New guide section (13-cluster-topology) states what defines
cluster membership: reachability + CA, **not** `cluster_name` (a label; different names still
merge). runtime-invariants already carried the cluster-is-the-data-isolation-boundary invariant.
