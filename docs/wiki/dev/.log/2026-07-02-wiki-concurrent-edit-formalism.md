## [2026-07-02] ingest | mycelium-wiki Phase 0 formalism

Formalised the two load-bearing areas of the proposed mycelium-wiki primitive into a
pre-build design record `docs/design/wiki-concurrent-edit.md`: (1) section addressing —
a page is an ordered list of independently-LWW-keyed sections with stable, content-
independent section ids (not headings/ordinals), so different-section edits are a lock-free
CRDT; (2) the curator role state machine (Reader/Candidate/Curator/Stepping-Down,
lowest-id election reused from tuple/blackboard), the idempotent at-least-once reconcile
loop, and no-heartbeat failover (curator state is derivable from durable KV). Core
argument: single-writer (not merge determinism) buys convergence I1, which is why
free-for-all LLM merge is declined-with-evidence. Third instance of the exactly-once-effect
contract. Sketch's Phase 0 now marked done-ahead-of-build.
