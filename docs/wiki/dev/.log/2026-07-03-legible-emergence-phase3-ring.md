## [2026-07-03] ingest | legible-emergence Phase 3 increment 1 — event ring + local explain

First Phase-3 code (the hard "explain" phase). EventRing (src/agent/emergent.rs): bounded
(EVENT_RING_CAP=1024, oldest-dropped), HLC-stamped, RT4 always-on-when-detectors-enabled — the
per-node source the cross-node fan-out will assemble. Event{hlc,node,kind,detail}; since(cursor)
returns HLC-ordered events >= cursor. Recorded from two clean sites: the detector loop (on
confirmed-count TRANSITIONS — onset/clear of conflict/gap/flap/oscillation, not a per-tick
firehose) and the consensus commit-conflict tripwire (gated on the flag). GET /gateway/explain
?since= (scope fleet:read) returns THIS node's ring HLC-ordered (increment 2 = cross-node
scatter-gather). New lock-table row 19 (EventRing::events, leaf Mutex<VecDeque>). Tests: ring
bounded + since-ordered unit; +fleet:read covers /explain. 20 emergent tests; 314/0; feature
clippy clean (removed dead len/is_empty). Remaining Phase 3: the scatter-gather fan-out (RT3 —
render what you have, name non-responders) + the #56-sequence reconstruction gate.
