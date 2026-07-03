## [2026-07-03] ingest | legible-emergence Phase 3 increment 3 — #56 reconstruction narrative (closes Phase 3)

The legibility layer that turns the assembled cross-node ring into an operator-readable story —
the Phase-3 acceptance bar ("reconstruct the #56 sequence with no code knowledge required").

`narrate(&[Event]) -> Vec<String>` (src/agent/emergent.rs): one line per event,
`[hlc N] <node> — <plain-English gloss> (<detail>)`. A gloss table translates each terse event
`kind` into what an on-call engineer needs (`governed_group_conflict` → "a group's live membership
left the governor's [min,max] band"; `membership_flap` → "a node is repeatedly joining and leaving
a group"; etc.). An **unknown kind falls back to its raw string** — a newly-added detector is
surfaced, never silently dropped. `ExplainResult` gains a `narrative` field, populated by
`assemble_explain` from the merged HLC-ordered events; `GET /gateway/explain` returns it alongside
the raw events.

Complementary change: the detector loop's `governed_group_conflict` event `detail` was enriched
from the terse "confirmed conflicts 0 → 1" to name the specific group + band from `GroupConflict`
("group(s) now outside the governor band: workers: 4 live vs band [1, 2]") — this is the "governor
capped at N, observed M" story beat, now legible in the narrative. Together conflict + flap events,
HLC-ordered across nodes, reconstruct the #56 governor-vs-autojoin sequence.

Gates: `narrate_renders_the_56_sequence_legibly` (synthetic full #56 sequence — ordered, glossed,
group/band survives, no raw kind leaks) + `narrate_surfaces_unknown_kinds_rather_than_dropping_them`
(unit); the cross-node e2e `test_explain_fanout_assembles_cross_node_ring_and_names_non_responders`
extended to assert the narrative names the `workers` conflict from *real* detector output. Pages
touched: dev/diagnostics.md (Phase 3 → complete), plan legible-emergence.md (status line + Phase 3
DONE + increment 3). **Phase 3 is complete.** Remaining: Phase 4 (fleet narrative — the "why is the
fleet in this state" synthesis), Phase 5 (operator surface).
