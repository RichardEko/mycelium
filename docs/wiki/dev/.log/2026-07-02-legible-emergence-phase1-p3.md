## [2026-07-02] ingest | legible-emergence Phase 1 increment 5 — P3 oscillation (detectors complete)

Added P3 (opacity/pheromone oscillation) to src/agent/emergent.rs — the FIFTH and final Phase-1
detector. Opacity oscillation is the same presence-set-churn shape as P2 (membership flap), so it
REUSES the FlapTracker machinery: pure opacity_pairs (fresh-opaque (node,kind) from sys/load/) +
generic set_transitions (symmetric difference) feed a second FlapTracker; ≥4 toggles in 60s = a
(node,kind) hunting in/out of shed. Gauge opacity_oscillations on /stats + /metrics. Seeds
prev-opacity at spawn. 2 P3 tests (opacity_pairs selects fresh-opaque only; set_transitions =
symmetric diff). 16 emergent tests total; 308/0 default, feature clippy clean. **All 5 KV-view
detectors (P1/P2/P3/P4/P6) + the /stats+/metrics surface done.** Only remaining Phase-1 item: a
live-cluster #56 reproduction integration test (pure detectors already unit-tested). Phase 1 status
→ 🟢 DETECTORS DONE.
