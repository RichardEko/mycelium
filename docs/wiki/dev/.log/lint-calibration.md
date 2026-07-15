# wiki-lint calibration ledger (the miss-log)

The framework's own report card: every drift a **prior** lint pass declared clean — or a **scope
gap** that let drift persist unnoticed — recorded with what should have caught it and the sharpening
that resulted. This is to `wiki-lint` what the calibration ledger in `ratings.md` is to
`mycelium-analysis`: it measures whether "clean" verdicts predict reality, and it is what turns a
lint from a checklist into an audit.

**Review it before every lint** (a check with repeated misses in one area needs a *structural* fix,
not another point patch). **Append to it after** every pass — or every time drift surfaces
elsewhere (analysis, doc-coverage, a code review, a support question) — that a prior lint should
have caught.

Entry format:
`- {date}: {check} declared {area} clean [prior pass] but {drift} was live (found by {what}). Sharpening: {change}.`

## Misses

- 2026-07-02: **lock-order table** declared complete, but a `parking_lot::Mutex<VecDeque>` wrapped in
  a `type SenderLog` alias (`signal.rs:134`) was undeclared — even the Run-28-extended table missed it
  (found by the 2026-07-02 lint). Sharpening: added the lock-wrapping-type-alias grep
  (`^type|^pub type … | grep -i mutex|rwlock`) to §1.
- 2026-07-02: **lock-order table** — an undeclared lock site shipped while analysis scored Concurrency
  8–9 (found by analysis Run 28; `ratings.md` ledger 2026-07-02). Sharpening: the per-field-name diff
  + the table's explicit completeness claim ("add a row per new lock field").
- 2026-07-07: **KV-namespace table** — `src/lib.rs`'s table (and the front-door reserved list, which
  only ever diffed against *it*, not code) was missing **nine** live prefixes (`svc/ log/ clog/ lock/
  prompts/ skills/ installable/ comp/ wiki/`) (found by the 2026-07-07 lint). Sharpening: grep the
  workspace for prefix *writers* and diff against the table; widened the lock-grep to `mycelium-*/src/`.
- 2026-07-07: **examples.md demo count** — the wiki carried "eleven coop demos" though the smoke had
  run twelve since 2026-07-03 (found by wiki-lint 8, "the first lint that counted"; `ratings.md`
  ledger 2026-07-07). Sharpening: count from the live source; never pin a count the wiki says it won't.
- 2026-07-11: **scope gap — guide-chapter version constants.** `09-security.md` cited wire **v10
  "(current)"** and framed the rolling window as **v10 ↔ v9** through many passes; §1's cited-constant
  check covered only the front-door docs (`building-on-mycelium`, `faq`), not guide *chapters* (found
  by `doc-coverage` run 2). Sharpening: §1 now greps every `docs/guide/*.md` chapter that pins
  `WIRE_VERSION`/`PREV_WIRE_VERSION` and diffs against `framing.rs`. **On its first exercise the
  sharpened check earned its keep** — it caught a *residual* `wire v10` in the `09-security.md`
  mermaid diagram (line 33) that `doc-coverage` run 2's prose fix had missed; fixed to v12.
- 2026-07-11: **scope gap — testing.md gate-list vs actual CI.** `dev/testing/testing.md` listed the
  *clippy* of `mycelium-core` tests (implying coverage) while the crate's whole suite was never *run*
  in CI (no `-p mycelium-core` test job); §1 spot-checked `operations.md` endpoints but never diffed
  `testing.md`'s CI-gate block against `.github/workflows` (found by the mixed-version compat work).
  Sharpening: §1 now diffs the `testing.md` CI-gate list against the workflow `run:` steps.
- 2026-07-13: **scope gap — §4 treated "the knowledge has a home" as "covered."** The earlier
  2026-07-13 lint declared coverage complete for `coordination-approaches.md` on the grounds that the
  doc *exists* (user-facing) — but never checked that the **wiki cites it**. A cross-cutting decision
  the three companions all embody was reachable from guide/operations/design docs yet invisible from
  the wiki's own companions synthesis, contradicting the wiki's "code is canon, the wiki cites it"
  contract (found by the 2nd 2026-07-13 lint). Sharpening: §4 now asks not just "does durable knowledge
  have a home?" but "does the **wiki** cite that home?" — a new authoritative `docs/design/` decision
  the wiki's subject matter embodies must be linked from the relevant wiki page/folder-note, not merely
  exist elsewhere. Folded into the skill's §4.
- 2026-07-14: **scope gap — `examples.md` audited by count, not by category.** The examples-page checks
  had focused on the pinned *coop demo count* (the 2026-07-07 "eleven"→"twelve" miss) and never
  verified the page enumerates every example **category**. So the whole **visual-showcase** category
  (`conway`, `conway-gpu`) was absent from `dev/examples.md` for many passes, and this session's four
  new `*_viz` showcases had no home either (found by this lint after the showcase examples were added).
  Sharpening: §4/§1 examples-page check now verifies **category completeness** — starter · coop · AFN ·
  a2a · integration · **visual-showcases** — a whole category missing is a coverage finding, not just a
  wrong count. Fixed: added the *Visual showcases* bullet to `examples.md`.
- 2026-07-15: **the by-category check's own list was incomplete — a hardcoded category set is the
  same bug one level up.** The 2026-07-14 sharpening pinned the category list to `starter · coop · AFN
  · a2a · integration · visual-showcases`, so lints 3–4 applied it and declared the enumeration
  complete while **three whole categories were absent** from `dev/examples.md`: **Guardrails**
  (`mycelium-guardrails/examples/`), **Reasoning / LangGraph** (`mycelium-reason/examples/` + the Rust
  reason nodes the FAQ now cites), and the **Wiki companion** (`mycelium-wiki/examples/wiki_chat.rs`) —
  plus the **Skills / community** cluster as its own category. Found by a **user question** ("no
  examples for viewing audit or guardrails?") that triggered a workspace-wide example sweep. Root cause
  is identical to the count-vs-category miss, just one level up: a *fixed enumeration* (of categories,
  as before of counts) drifts the moment the tree grows past it — every companion crate's `examples/`
  was outside the scope the audit ever swept. **Sharpening (structural):** the category set must be
  **derived from the tree**, not hardcoded — enumerate `find . -path '*/examples/*.rs'` across **all**
  crates (not just `examples/` + coop) and diff the resulting categories against the page. And the
  durable fix for the recurrence: `dev/examples.md` should **cite `examples/README.md` § The suites**
  (the front-door index, now workspace-complete) as the canonical list, so the wiki synthesizes rather
  than maintains a parallel enumeration that silently falls behind. Folded into §4. Fixed: added the
  four missing category bullets to `examples.md`.
- 2026-07-15: **README section renames broke inbound `#anchor` links silently — twice in one restructure.**
  The capability-matrix restructure renamed `examples/README.md` sections (`Start here`/`The suites`/`Find
  one by layer` → `The capability matrix`/`The worlds`, plus `#ops-console`, `#research-artifacts`). Two
  cross-repo links that pointed *into* the old headings broke: `ui-example-contract.md` → `#find-one-by-layer`
  (fixed inline during the matrix commit) and `docs/guide/09-security.md` → `#the-suites` (shipped in the
  matrix commit; caught by *this* lint's §3). §3's dead-link check historically swept **outbound** links
  from the front-door docs (faq/building-on) but never **inbound** `README.md#anchor` links from the guide
  and wiki, so a heading rename in a heavily-linked front-door doc broke them with no check watching.
  **Sharpening (structural):** §3 now sweeps **inbound anchor links** — `grep -rnoE
  "examples/README\.md#[a-z-]+"` (and the same for any front-door doc whose headings changed) across
  `docs/` + crate READMEs, and confirms each `#anchor` still matches a live heading. Folded into §3. Fixed:
  repointed `09-security.md` → `#ops-console`.
- 2026-07-15: **UI-contract check verified a *known list*, never reconciled against the full tree-derived
  grep — so `ops_console` sat unclassified.** The §4 UI-example-contract check enumerates browser examples
  as `grep -rl 'include_str!.*\.html' examples` and had been verifying the 9 showcases + naming 2
  exceptions (`conway-gpu`, `three_node_demo`). But `examples/ops_console.rs` also `include_str!`s an
  `.html` — it matched the grep every pass — and was neither in the compliant-9 nor the exception list: it
  was silently skipped because it "obviously isn't a showcase." That is the count-vs-category bug again in
  a new place — the check trusted a curated list instead of reconciling every grep hit. Found when
  `ops_console`'s move to its own directory (2026-07-15) made me re-run the raw enumeration. `ops_console`
  is legitimately an **exception** — it is the *console itself*, the consumer/observer of `ui/viz`, not a
  showcase that advertises it, so rules 2 & 4 don't apply. **Sharpening (structural):** the check now
  requires **classifying every `include_str! html` hit** as *compliant* or *documented-exception* — an
  unclassified hit is itself a finding. Folded into §4 + the contract doc's Lint section. Fixed: added
  `ops_console` to the exceptions in `ui-example-contract.md`.
