## [2026-07-03] ingest | cap the cross-node explain fan-out (analysis Run 30 Scalability finding)

Run 30 (M2) scored **Scalability 7** (a floor dimension) on the observation that `assemble_explain`
(`GET /gateway/explain`) fanned a `sys.explain` RPC out to **every** known peer — O(peers), uncapped
— so an operator query on a 100+-node fleet could spray one RPC per node. Addressed.

`assemble_explain` now selects a **bounded subset** via the pure, unit-tested
`select_explain_targets(peers, cap)`: sorts peers by identity (a *deterministic, stable* subset
across repeated queries), takes the first `EXPLAIN_MAX_FANOUT = 32`, and returns the count skipped.
`ExplainResult` gains `not_queried: usize` — the RT3 honesty extended: a capped view *names* the
skipped peers' count rather than silently dropping them, exactly as `non_responders` names the
queried-but-silent peers. Non-zero `not_queried` ⇒ the operator knows to raise the cap or re-query
for a wider view. The concurrent-RPC count per query is now bounded at 32 regardless of fleet size;
the local ring + up to 32 peers already reconstruct essentially any incident.

Gate: `select_explain_targets_caps_the_fanout_and_names_the_remainder` (unit — cap holds, remainder
counted, subset deterministic); `test_explain_fanout_…` extended to assert `not_queried == 0` on a
small fleet. Pages: dev/diagnostics.md + dev/operations.md (explain now noted as capped). Not a
Finding fix (Run 30 recorded it as *shape*, not a defect) — a scale hardening of an operator-gated
surface.
