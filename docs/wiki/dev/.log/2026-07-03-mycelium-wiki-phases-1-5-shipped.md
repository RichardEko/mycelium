# 2026-07-03 — mycelium-wiki build phases 1–5 shipped (companion complete end-to-end)

The `mycelium-wiki` companion went from *proposed* to *functionally complete*, all CI-green on the
**Wiki** job. Ingested to [companions](../companions/companions.md) + new page
[companions/wiki.md](../companions/wiki.md).

**Commits (this session):**
- Phase 1 · data plane — `WikiStore` trait + `FsStore` (manifest-last, torn-read-safe).
- Phase 2 · control plane — `65b31c2`: curator election + ring-failover + evaporating proposal queue +
  single-writer apply; cross-node `tests/failover.rs`.
- Phase 3 · reconcile — `c648255`: single-writer 3-way merge, drain **groups proposals by section**;
  `DirectReconciler` (lossless, idempotent append-merge) + `LlmReconciler` (feature `llm`).
- Phase 3 · lint loop — `224c45f`: curator-only periodic health check; `structural_lint` (dead
  cross-links, empty sections) + `SemanticLinter`/`LlmSemanticLinter` (cross-section self-consistency).
  Advisory only — detection, not prevention.
- Phase 4 · MCP tools — `5fbdb87`: `wiki.read`/`query`/`propose` over Mycelium's existing MCP invoke
  path (public API only, no core fork → why mesh-native MCP beats bespoke `/gateway/wiki/*`).
- Phase 5 · worked example — `5932ec1`: `examples/wiki_chat.rs` — one template, both use cases (import
  → grounded chat; writer=control plane, reader=data plane direct); `ci_smoke.sh` Docker-free.

**Durable facts worth not re-deriving:**
- The design pivot (KV-native section-CRDT → control-plane/data-plane) was driven by the **KV-floods-
  every-node** invariant: group scope is access/namespacing, never replication isolation. The corpus
  must live in a node-independent store or failover would have to transfer serving-node state.
- **Single-writer dividend:** grouping proposals by section before reconcile is what removes the CRDT —
  one writer holds the whole same-section batch. Idempotent append-merge (skip already-contained body)
  is what makes crash-mid-drain safe.
- **Prose vs structure split:** the LLM curates body prose; headings/attributes (join keys) stay
  deterministic and code-owned. Same split in the reconcile and the lint.
- **Reader node-independence** is the litmus that keeps the curator a *recallable role*, not a
  coordinator: `ask`/`chat` in the example use the data plane with no node at all.

**Open remainders (additive, non-blocking):** ~~Phase 4 bespoke REST routes + Python/TS `WikiClient`~~
**closed same-day** (commit `21917e0`: `/gateway/wiki/*` axum router + `mycelium.wiki.Wiki` /
`mycelium-ts` `Wiki`, `tests/gateway.rs`). Only the disconnected KV-native variant remains
(`docs/design/wiki-concurrent-edit.md`).

Related: [2026-07-03-wiki-approach-pivot-control-data-plane](2026-07-03-wiki-approach-pivot-control-data-plane.md).
