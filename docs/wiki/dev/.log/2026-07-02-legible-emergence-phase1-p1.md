## [2026-07-02] ingest | legible-emergence Phase 1 increment 1 — P1 detector

First diagnosability CODE. src/agent/emergent.rs: the emergent-detector infrastructure +
P1 (governed-group conflict, the #56 condition). Config-gated GOSSIP_EMERGENT_DETECTORS
(off by default, loop spawned only when enabled → zero overhead). Pure functions
detect_governed_group_conflicts (scan sys/govern/membership intents vs live grp/ member
count, RT3 evaporation-tolerant) + confirm_conflicts (hysteresis, the false-positive guard);
run_emergent_detectors loop sets the ctx.governed_group_conflicts gauge; ViewConfidence
(RT1/RT2 per-node-estimate header) computed on demand. Surfaced on /stats
(governed_group_conflicts always; view_confidence when enabled). 5 unit tests: #56 over-max,
under-min, healthy-in-bounds (false-positive gate), RT3 evaporated-intent, hysteresis. Gates:
297/0 default, feature-matrix clippy clean. Remaining Phase 1: P2/P3/P4/P6 detectors (same
shape), /metrics surface, live-cluster #56 test. Ops page + plan status updated.
