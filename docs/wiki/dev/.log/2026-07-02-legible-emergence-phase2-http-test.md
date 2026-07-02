## [2026-07-02] ingest | legible-emergence Phase 2 — /gateway/fleet live HTTP scope test

Closed the endpoint-wiring coverage gap: test_gateway_fleet_snapshot_endpoint_scope_gated
(compliance-gated, in http.rs tests) exercises the WIRED route + auth gate over real HTTP —
no token → 401, wrong-scope (kv:read) → 403 naming fleet:read, fleet:read token → 200 with the
relational snapshot shape (view_confidence.observer string, governed_groups/throttle_graph
arrays, store_hash number). Prior tests covered compute_fleet_snapshot + the required_scope
table function but not the live path. Gates: compliance suite 371/0, clippy (incl. compliance)
clean. Phase 2's fleet endpoint is now end-to-end verified (assembly + 3-node agreement + HTTP
+ scope gate).
