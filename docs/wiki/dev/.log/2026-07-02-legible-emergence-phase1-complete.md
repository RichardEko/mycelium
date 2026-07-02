## [2026-07-02] ingest | legible-emergence Phase 1 COMPLETE — live #56 test

Added the live end-to-end #56 reproduction integration test
(test_p1_governed_group_conflict_detector_fires_and_clears_end_to_end in lib_tests): a started
agent with emergent_detectors_enabled, the real publish_membership_intent path (governor caps
"workers" at [1,2]), 4 injected grp/ members (over cap = the #56 emergent-autojoin condition),
and poll the governed_group_conflicts gauge until it fires — then tombstone 3 members and poll
until it clears. Exercises the whole path the unit tests don't: config→start→spawned detector
loop→gauge. Passes (~10s, real 2s-tick timing). Gates: 309/0 default, feature clippy clean.

**Phase 1 COMPLETE:** all 5 KV-view detectors (P1/P2/P3/P4/P6) + /stats + /metrics + live #56
test. Plan status → Phases 0–1 done. Next: Phase 2 (/gateway/fleet relational snapshot,
computed locally, RT1 view-confidence-labelled, "three nodes agree at convergence" gate).
