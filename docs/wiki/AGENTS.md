# LLM Wiki — schema & workflows

This directory (`docs/wiki/`) is the project's single **LLM Wiki** (Andrej Karpathy's pattern,
<https://gist.github.com/karpathy/442a6bf555914893e9891c11519de94f>): a persistent, **compounding**,
interlinked knowledge base, compiled and maintained by the agents who work on this project. Unlike
RAG — which re-derives understanding from scratch every query — the wiki is compiled **once during
ingest** and kept current, so the next agent onboards from the wiki instead of re-reading every
plan, doc-comment, and analysis entry.

This `AGENTS.md` is the **schema**. Read it before ingesting or editing. The same pattern (and a
near-identical schema) runs in the Transparency Platform and Novus-i2 repos — keep the conventions
aligned unless Mycelium's library nature forces a divergence (the divergences are §"Code is canon"
and the doc-vs-code lint below).

## Code is canon — the library-repo rule

Mycelium is a Rust **library**. Its authoritative knowledge deliberately lives next to the
compiler, where rustdoc renders it and drift is closest to detection:

| Canon | Lives at |
|---|---|
| Public API + KV-namespace ownership table | `src/lib.rs` crate doc-comment |
| Wire format + version policy | `mycelium-core/src/framing.rs` (top) |
| HLC design + limits | `mycelium-core/src/hlc.rs` module doc |
| Capability/requirement model | `src/capability.rs` |
| Purpose / philosophy | `docs/philosophy.html` |
| Security posture | `docs/threat-model.md` |

**The wiki never duplicates canon — it cites it** (path, or `path § heading`), the way the other
projects cite their anchor by A-number. A wiki page that paraphrases code becomes a second source
of truth and *will* drift (analysis Run 28 caught exactly this: a "complete" lock table in
CLAUDE.md that three feature waves had silently outgrown). Pages carry what canon *cannot*:
synthesis across files, invariants and their rationale, operational lore, decision history,
domain theory. When a page must state a code-level fact, it cites the file and states the claim
in a form the lint pass can verify.

## The two top-level sections

Route a fact with: *"would this still be true if Mycelium were rewritten in another language?"*

| Section | Captures | Routing test |
|---|---|---|
| [`dev/`](dev/dev.md) | **How the substrate is built and verified** — architecture invariants, concurrency discipline, testing/scale lore, security workstreams, companion crates, ops surface, delivery history. | *No* — it's about our code, tests, and infra. |
| [`domain/`](domain/domain.md) | **The coordinator-free thesis and its world** — the theory (Holland/Hayek, the Coordinator Trap, HLKS), the publications corpus, governance patterns (management-as-intent), federation positioning (NANDA), commercial strategy. | *Yes* — true whatever the implementation. |

Where a concept meets its implementation, **cross-link** the sections rather than duplicating.
Top-level sections are **fixed** (`dev`, `domain`). Do not add another without explicit user
direction (no `visual/` — Mycelium has no UI design language; demo aesthetics live with the
examples). Within a section, create areas/sub-areas freely.

## The three layers

1. **Raw sources** (not stored here — **link, don't copy**): the code canon above; `README.md` /
   `ROADMAP.md`; `docs/plans/` (execution records, indexed at `docs/plans/README.md`);
   `docs/guide/` + `docs/operations/` + `docs/design/`; `docs/publications/` (indexed at
   `docs/publications/README.md`); `docs/analysis/ratings.md` (the M2 audit series + calibration
   ledger). The wiki is compiled *from* these.
2. **The wiki** (this directory): synthesised, interlinked Markdown in a **nested tree** —
   `<section>/<area>/<page>.md`. Three rules:
   - **Every folder has a folder-note named after the folder** (`<folder>/<folder>.md` — e.g.
     `dev/concurrency/concurrency.md`; the wiki root is `wiki.md`). A folder-note links **up** to
     its parent and **down** to its children. Read `wiki.md` first, then the section folder-note,
     then down. A page without siblings may sit directly in the section root as a leaf.
   - **Pages cross-link densely** — open with a breadcrumb up to the folder-note, link across to
     related pages, and **cite sources by path** rather than pasting. Files are kebab-case.
     Describe current-state *what it means / how it works* — invariants, the reconciled position,
     open questions.
   - **Each section keeps a `.log/`** at its root holding ingest history: **one dated file per
     ingest** (`YYYY-MM-DD-<slug>.md` — `## [YYYY-MM-DD] ingest | <topic>` + a few lines), never
     an append. `.log/log.md` carries the local how-to. Dot-prefixed so Obsidian ignores it.
3. **The schema** (this file).

## Workflows

**Query (start of any task).** Read `wiki.md` → the relevant section folder-note → down the links.
Don't re-derive from plans/docs what the wiki already states. The wiki is read-first, but canon
wins on conflict: if a page contradicts the code or its doc-comments, trust the code, then fix the
page (that's an ingest).

**Ingest (end of any completed piece of work) — non-negotiable.** When a change lands that makes
durable knowledge (a new invariant, a root-caused bug family, a shipped workstream, a revised
position): update the relevant page(s) to current state, refresh the folder-note(s) if the tree
changed, and add one dated file to the section's `.log/`. Ingest ≠ changelog — git and
`docs/plans/` hold the history; the wiki holds the *reconciled current state*.

**Lint (periodic — run via the `/wiki-lint` skill or by hand).** Four checks, most-valuable first:
1. **Doc-vs-code verification** — for every page claim that cites code, confirm the code still
   says it (grep the cited path; run the cited test; count the cited items). This is the check
   that would have caught the Run-28 lock-table drift years earlier. Fix or re-cite.
2. **Staleness** — pages contradicted by merged work that skipped ingest; ✅-shipped items still
   marked pending.
3. **Orphans & dead links** — pages no folder-note reaches; links to moved/deleted files.
4. **Coverage** — merged work with no wiki trace (compare `.log/` against `git log` since the
   last lint).
Record each lint pass as a `.log/` entry naming what was fixed.

## Memory vs wiki

An agent's private memory store (`~/.claude/.../memory/`) holds *user preferences and
session/working state*. The wiki is *shared, durable, versioned-with-the-repo* knowledge.
**Promote durable project knowledge to the wiki**; keep in memory only what is about the user
(preferences, how to engage) or about an in-flight session. A memory entry that merely mirrors a
wiki page should be a one-line pointer to it.

## What the wiki is not

Not a changelog (git + `.log/` one-liners), not the plans archive (`docs/plans/`), not the audit
series (`docs/analysis/ratings.md`), not rendered user documentation (`docs/guide/`,
`docs/operations/`), and never a copy of code doc-comments.
