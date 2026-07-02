## [2026-07-02] ingest | legible-emergence Phase 1 increment 3 — P6 coverage-gap

Added P6 (capability-coverage gap, RT3 flagship) to src/agent/emergent.rs:
detect_coverage_gaps(kv_state, now) — for each fresh req/ requirement, resolve its CapFilter
via resolve_filter_against_kv (which checks cap/ is_fresh); a requirement with zero fresh
providers is a gap. Deduped by capability id (ns/name). Loop-based with hysteresis (RT3: a
gap must be sustained past CONFIRM_TICKS to distinguish a retracted provider from a
merely-lapsed one), so it names "no provider visible from here", never "exists". Gauge
capability_coverage_gaps on /stats. Generalized the hysteresis into a shared confirm_by_key
(P1 + P6 both use it; confirm_conflicts is now a thin wrapper). 3 P6 tests (gap, covered =
no gap, generic hysteresis). Gates: 303/0 default, feature-matrix clippy clean; 11 emergent
tests total. Flagship pair (P4 RT2 + P6 RT3) now complete. Remaining Phase 1: P2 flap, P3
oscillation, /metrics, live #56 test.
