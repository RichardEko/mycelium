## [2026-07-02] ingest | proposed mycelium-wiki primitive

Recorded the design sketch `docs/plans/mycelium-wiki.md` (proposed): a group-scoped
LLM-maintained wiki as the durable/curated fourth coordination primitive, sibling to the
tuple space (pull) and blackboard (claim). Added the primitive-taxonomy section to
`companions.md` and the plans-index row. Load-bearing decision captured: concurrent prose
edits don't LWW-merge, so v1 = section-granular keys + a recallable curator role
(LLM reconcile serialised at one writer-of-record); free-for-all LLM merge rejected
(non-deterministic → doesn't converge). Gated on demand, not feasibility.
