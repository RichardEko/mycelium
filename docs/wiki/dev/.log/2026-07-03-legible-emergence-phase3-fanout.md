## [2026-07-03] ingest | legible-emergence Phase 3 increment 2 — cross-node explain fan-out

The cross-node half of the "explain" phase. `assemble_explain(ctx, since)` (src/agent/emergent.rs)
starts from this node's `EventRing.since(cursor)` and fans a best-effort `sys.explain` RPC out to
every known peer, served by `run_explain_responder` (spawned alongside the detector loop under
`emergent_detectors_enabled`), then merges each node's single-author ring into one HLC-ordered
stream. Returns `ExplainResult{observer, events, responders, non_responders}`.

**The RT3 point (the reason it exists):** deliberately NOT `service().scatter_gather` — that
primitive aborts once `min_ok` replies land and discards *all* partial replies on
`InsufficientReplies`, i.e. the slow/partitioned nodes you most need during an incident are exactly
the ones dropped. Here each per-peer `rpc_call` has its own timeout, so a silent peer becomes a
*named* `non_responder` while every reply that does arrive still lands. "Render what you have + name
the gaps."

`Event` gained `Deserialize` (RPC round-trip via `serde_fixint`). Rings are single-author
(`record_event` stamps `node = self`), so the merged per-node streams are disjoint — no dedup needed.
`GET /gateway/explain` now returns the cross-node `ExplainResult` instead of the local-only ring.

Gate: `test_explain_fanout_assembles_cross_node_ring_and_names_non_responders` (lib_tests.rs) —
nodes A+B run detectors+responder and assemble each other's rings in HLC order; node C is a live
gossiping peer started *without* the responder, giving a deterministic named non-responder (a
stand-in for a slow/partitioned node with no eviction-timing race). Pages touched:
dev/diagnostics.md (Phase 3 status), plan legible-emergence.md (increment 2). Remaining Phase 3:
the #56-sequence reconstruction narrative (the collection primitive is now done; the rendering +
end-to-end #56 scenario test remain).
