## [2026-07-02] ingest | legible-emergence Phase 1 increment 4 — P2 failover flap

Added P2 (failover flap) to src/agent/emergent.rs — the detector for the plan's motivating
image ("node count flapping with no signal why", the #56 story). Pure membership_snapshot
(all grp/{group}/{node}, tombstones excluded) + flap_transitions (symmetric-difference of two
snapshots = joined/left pairs). Stateful sliding-window FlapTracker (per (group,node) VecDeque
of transition timestamps; ≥ FLAP_THRESHOLD=4 toggles within FLAP_WINDOW_MS=60s = a flap; a
settled failover ages out; a single join doesn't trip it). Loop seeds prev-membership at spawn
so the initial roster isn't counted as joins. Gauge membership_flaps on /stats + /metrics
(mycelium_emergent_membership_flaps). 3 P2 tests (transitions detect join+leave, sustained
toggling flags + ages out, single-failover not a flap). Gates: 306/0 default, feature clippy
clean; 14 emergent tests total. 4 of 5 detectors done. Remaining: P3 oscillation, live #56 test.
