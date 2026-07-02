## [2026-07-02] ingest | legible-emergence Phase 1 increment 2 — P4 opacity-storm

Added P4 (fleet-wide opacity storm, RT2 flagship) to src/agent/emergent.rs: pure
opaque_node_pct(kv_state, live_nodes, now, max_age) → distinct fresh-is_opaque nodes ÷ live
nodes, integer percent. A RAW GAUGE the operator thresholds (library-not-platform — no
in-code bound), read beside view_confidence (a storm degrades the gossip used to count, so
peers_heard<<peers_known signals undercount). Surfaced on /stats as opaque_node_pct when
enabled. Design point that emerged and is now recorded in the plan: STATEFUL detectors
(hysteresis, P1) live in the loop; STATELESS gauges (P4) compute on-demand in /stats — no
atomic, no loop change. 3 P4 tests (storm 75%, healthy 12%, stale-RT3 0%). Gates: 300/0
default, feature-matrix clippy clean (fixed a useless_conversion on encode_load_state which
already returns Bytes). 8 emergent tests total.
