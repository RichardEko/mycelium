## [2026-07-02] ingest | dev/diagnostics.md page + history staleness fix

Wiki catch-up after the Phase-1 diagnostics work. Two fixes: (1) history.md said Legible
Emergence was "the only open plan, proposed, not started" — stale; corrected to "Phases 0–1
shipped 2026-07-02, Phases 2–5 not started." (2) The emergent-detector layer had no reconciled
wiki page (only .log entries + endpoint-table cells) — added dev/diagnostics.md: the posture
(no-collector / per-node-estimate RT1-RT2 / detection-not-prevention / zero-overhead-off), the
five-detector table (P1/P4/P6/P2/P3 with /stats gauges + sources + shape), the three design
patterns (stateful-loop vs stateless-on-demand; generic confirm_by_key; P2/P3 share FlapTracker),
the RT3 evaporation discipline, and status/next. Linked from dev.md leaf list.
