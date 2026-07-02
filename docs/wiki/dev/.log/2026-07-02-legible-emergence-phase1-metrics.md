## [2026-07-02] ingest | legible-emergence Phase 1 — /metrics surface

Completed the "surfaced on /stats AND /metrics" requirement. The detector loop now emits
(behind #[cfg(feature="metrics")], each tick) the Prometheus gauges: mycelium_emergent_
governed_group_conflicts (P1), _capability_coverage_gaps (P6), _opaque_node_pct (P4), and
the RT1/RT2 view-health gauges _peers_heard / _peers_known / _max_staleness_ms (so an
operator's alert can qualify a diagnostic by the observer's own view — peers_heard <<
peers_known ⇒ partial view). The loop is the periodic emitter (Prometheus scrapes a
registry, so gauges are set on-tick, not on-scrape), mirroring the tuning_governor's
emit_metrics pattern. Ops page /metrics row updated. 303/0, feature clippy (incl. metrics)
clean.
