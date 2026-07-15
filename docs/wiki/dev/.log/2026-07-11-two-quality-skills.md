# 2026-07-11 — two new quality skills (doc-coverage, publication-lint)

Promoted two emergent quality mechanisms from one-offs to reusable skills, extending the
mycelium-analysis / wiki-lint family.

- **`/doc-coverage`** (`.claude/commands/doc-coverage.md`) — the WHAT/WHY/HOW × Dev/Ops
  documentation-coverage audit (Diátaxis × audience). Adversarial (name-drop ≠ Clear), parallel
  auditor fan-out, per-cell Clear/Thin/Missing/N-A verdicts, tiered gap ranking. Maintains
  `docs/analysis/doc-coverage.md` (renamed from the dated seed → **living matrix**, diff each run)
  and carries a **calibration** section (a `Clear` cell later found thin → a ledger line — the
  discipline that makes it compound, borrowed from M2).
- **`/publication-lint`** (`.claude/commands/publication-lint.md`) — the honesty-lint for the
  **persuasion surface** (decks, papers, `philosophy.md`). Claims-vs-code, overclaim (CFT-not-BFT,
  unearned absolutes), roadmap-vs-shipped labelling, binding-framing compliance (library-not-platform,
  constructive examples), cross-artifact consistency. Weights *overclaim* (roadmap-sold-as-shipped) as
  Critical — the class of bug a human caught in the customer pitch, not a mechanism. Keeps an
  **overclaim ledger** in `docs/publications/README.md`.

Also: `run-tests.md` Level 1 now includes `cargo test -p mycelium-core` (was stale after the
2026-07-11 CI change that made core's suite run). Each artifact's docs/README row names its skill.

The pair rounds out the analysis family: mycelium-analysis (code) · wiki-lint (internal docs vs code)
· doc-coverage (doc completeness) · publication-lint (external claims vs reality).
