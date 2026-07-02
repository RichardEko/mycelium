## [2026-07-02] ingest | curator-role lib.rs API sketch

Added §6 to `docs/design/wiki-concurrent-edit.md`: a pre-build `lib.rs` API sketch of the
curator surface in the tuple/blackboard doc-comment idiom. Pins the load-bearing shape —
`WikiRole` (configured *intent*: Auto/Curator/Contributor/Reader) vs `CuratorState`
(observable *state*: Idle/Candidate/Curator/SteppingDown), `WikiConfig` curator knobs
(reconcile/lint/cap_refresh; failover ≈ 3× cap_refresh), `WikiError` (NoCurator/NoReconciler),
and the `Wiki` handle's read-only legibility accessors (`role`/`curator_state`/`is_curator`/
`current_curator`) with the private loop entry points annotated against invariants I1–I3.
The role/state split is public because "who is curating this domain right now?" is the
emergent-legibility question (adjacent to legible-emergence). Cross-linked from the sketch's
Phase 2. Signatures are pre-build; the shape is the commitment.
