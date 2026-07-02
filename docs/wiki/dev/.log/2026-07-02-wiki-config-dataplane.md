## [2026-07-02] ingest | WikiConfig data-plane knobs drafted

Completed the WikiConfig sketch in wiki-concurrent-edit.md §6: added the data-plane knobs
grouped by concern — max_section_bytes (paragraph-scale merge unit, far below
MAX_KV_WRITE_BYTES; SectionTooLarge error added), proposal_ttl (evaporation bound on queue
growth, no coordinator), direct_new_sections (§2.1 direct-vs-queue policy), and
lint_probes_cited_facts (the only side-effecting lint check). Added compliance-gated
default_read_clearance (§4.3 per-page classification floor). Recorded the deliberate
NON-knobs as a divergence-from-siblings note: no persist/wal_path (content is durable KV,
proposals intentionally ephemeral), no assembly cache (read-time; deferred §7), no
reconcile-batch, no backpressure, no per-wiki LLM model (uses the agent's LlmBackend).
