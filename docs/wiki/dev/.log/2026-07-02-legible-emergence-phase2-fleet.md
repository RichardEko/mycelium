## [2026-07-02] ingest | legible-emergence Phase 2 increment 1 — /gateway/fleet snapshot

First Phase-2 code: the relational fleet snapshot (localize view). GET /gateway/fleet (scope
fleet:read, deny-by-default). compute_fleet_snapshot(ctx) in src/agent/emergent.rs assembles
from LOCAL KV: governed-group status (new pure governed_group_statuses — every fresh group's
intent vs observed, conflict flagged, sorted for cross-node determinism), capability-coverage
gaps, opacity (pct + pairs), and the flap/oscillation counters — each with the RT1/RT2
view_confidence header. Coordinator-free: any node answers, computed from converged KV.
Available whether or not the detector loop runs (it's a read view). Acceptance gate MET
(RT1-restated): test_fleet_snapshot_agrees_across_three_nodes_at_convergence — 3 started nodes,
seed a conflict + coverage gap on one, all three converge on the same DIAGNOSIS while
view_confidence stays each node's own. Gates: 311/0, feature clippy clean; +2 tests
(governed_group_statuses unit, 3-node agreement) + the fleet:read scope assertion. Ops +
diagnostics + plan updated. Remaining Phase 2: throttle graph, store-divergence, commit-conflict
hot slots.
