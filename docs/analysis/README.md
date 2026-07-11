# How Mycelium audits itself

*A note for technical reviewers.* This directory holds the project's self-audit outputs. This page
explains the system that produces them — and, more to the point, the property that makes it worth
your attention: **the audits are themselves audited.** Each mechanism keeps a dated ledger of its
own misses — verdicts it once declared clean that later proved wrong — and the mechanisms
cross-correct. What follows is candid rather than polished, deliberately: the credibility of a
self-audit comes from its willingness to record its own failures, and everything here is in-repo,
dated, and verifiable against commit history.

## Why it looks like this (the honest origin)

The code-quality audit (`mycelium-analysis`) began as a read-and-rate rubric: 25 dimensions, scored
1–10. By its thirteenth run it had **saturated** — scores clustered at 8–9 and measured the
*presence of artifacts* (a lock table exists, a policy is documented) rather than the *absence of
defects*. A real concurrency race shipped under fifteen consecutive 8–9 scores. That is the failure
this whole system is built around.

Methodology v2 (adopted 2026-06-10) rebuilt the loop into an audit:

- **Execution-evidence gate** — a dimension can score 9 only with *fresh execution evidence produced
  during that run* (a suite run, an endpoint probed, a benchmark). Reading code caps a score at 8. A
  10 additionally requires *external* validation, which correctly makes 10 unreachable from inside
  the loop.
- **Falsification quota** — every run must take its three highest scores and *try to break them* with
  an executable probe, not more reading.
- **Floor as the headline** — the report leads with the three lowest dimensions, never the mean.
- **Calibration ledger** — a running record of every score that was later proven wrong.

## The four mechanisms

Each audits a different surface against a different source of truth. All four are invoked as skills
(`.claude/commands/*.md`) and write their findings to durable, in-repo artifacts. *(Counts below are
a 2026-07-11 snapshot; the linked files are the live source — check them, don't trust this number.)*

| Mechanism | Audits | Source of truth | Output |
|---|---|---|---|
| `mycelium-analysis` | code & architecture — 25 dimensions | **execution evidence** (suites, probes, builds) | [`ratings.md`](ratings.md) — 43 runs, a 34-entry calibration ledger |
| `wiki-lint` | the internal knowledge base (`docs/wiki/`) | **the code it cites** (re-verified, not trusted) | dated `.log/` entries + the [miss-log](../wiki/dev/.log/lint-calibration.md) |
| `doc-coverage` | documentation *completeness* — WHAT/WHY/HOW × Dev/Ops | **adversarial read** (a name-drop is not coverage) | [`doc-coverage.md`](doc-coverage.md) — living matrix + calibration section |
| `publication-lint` | the *external* claim surface (decks, papers, philosophy) | **shipped reality** (code, milestones, CI) | fixes + an overclaim ledger in [`publications/README.md`](../publications/README.md) |

Two design choices are worth calling out. `wiki-lint`'s load-bearing check is **doc-vs-code
re-verification**: the wiki cites `src/` rather than paraphrasing it, and the lint re-confirms the
code still says what the page claims — so documentation drift is caught mechanically, not by memory.
`publication-lint` weights **overclaim** (selling roadmap as shipped, or a guarantee the substrate
doesn't make) as its highest severity, because that is the direction that misleads an outside reader
— it is the one lint whose failure mode is *external*.

## The property that makes it credible: calibration

Any audit can say "clean." The question a reviewer should ask is *how often "clean" is wrong* — and
whether the system measures that. Here it does. Each mechanism carries the same discipline the code
audit pioneered:

- `mycelium-analysis` → the **calibration ledger** in `ratings.md` (e.g. *"Concurrency scored 8–9 in
  Runs N–M while a real race existed; found by …"*).
- `wiki-lint` → the **miss-log**, `docs/wiki/dev/.log/lint-calibration.md` — six seeded entries, each
  a drift a prior lint declared clean (or a scope gap that let drift persist) *and the sharpening it
  produced*. A miss that doesn't sharpen a check is a lesson half-learned.
- `doc-coverage` → a **Calibration** section recording any cell rated `Clear` that later proved thin.
- `publication-lint` → an **overclaim ledger** in `publications/README.md`.

And the mechanisms **correct each other**. A concrete, in-repo example from 2026-07-11:

1. `doc-coverage` (run 2) found `docs/guide/09-security.md` citing a stale wire version (`v10`,
   current is `v12`) — in a section its own prior run had rated `Clear`. It fixed the prose and logged
   the miss.
2. That miss became an entry in `wiki-lint`'s new miss-log, which **sharpened** a `wiki-lint` check to
   grep *guide chapters* (not just front-door docs) for pinned version constants.
3. On its first exercise, that sharpened check caught a *residual* stale `v10` in the same file's
   diagram that `doc-coverage`'s prose fix had missed the day before.

The audits found what the audits missed. That cross-correction — not any single mechanism — is the
thing being offered for scrutiny.

## What you can verify without trusting us

- **The ledgers are real, dated, and in git.** `git log` any of the files above; the calibration
  entries carry the run/date at which a verdict was wrong and how it was found.
- **The code side is deterministic, not LLM-judged.** Correctness is enforced by CI gates, separate
  from the audits: the feature-matrix clippy set (`make check`), per-crate test jobs (including
  `mycelium-core`, whose whole suite runs since 2026-07-11), the no-retry Docker cluster suites, a
  nightly 100-node scale suite, `cargo audit`, and a wire back-compat gate that fails a release which
  would break a rolling upgrade. The audits reason about quality; CI *enforces* correctness.
- **The methodology is versioned and in-repo.** The skill definitions (`.claude/commands/`) are the
  exact instructions each run follows; the analysis methodology (`ratings.md` preamble) is dated and
  refined in place, never retro-edited.

## Honest scope

These are **internal, LLM-assisted audits plus deterministic CI gates.** They are not a substitute
for an independent security audit, a formal-methods proof, or third-party production validation — and
the framework says so itself: a perfect (10/10) score is defined as *unreachable from inside the
loop* precisely because external validation is what it lacks. What this system does provide is a
disciplined, self-correcting, and auditable account of the project's own understanding of its
quality — including, on the record, where that understanding has been wrong.
