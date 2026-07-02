## [2026-07-02] ingest | legible-emergence Phase 2 increment 2 — relational fields

Enriched the fleet snapshot (compute_fleet_snapshot) with the remaining relational fields:
throttle_graph (pure throttle_graph fn — M7 sys/rate/{observer}/{sender} edges + observed fps,
sorted; "who is throttling whom"), a convergence-health self-report (store_entries + store_hash
via store_hash_acc — two nodes at convergence share the hash, operator diffs across nodes), and
the cumulative commit_conflicts count. Deferred with rationale (need new gossiped state, taxonomy
§8): true cross-node store-divergence (a sys/health/ key) and per-slot commit-conflict "hot
slots." Core relational "localize" view now complete. +1 test (throttle_graph_reports_rate_edges
_sorted); 18 emergent tests. Gates: 312/0, feature clippy clean. Plan updated.
