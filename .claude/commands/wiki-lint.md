Run a lint pass over the LLM wiki (`docs/wiki/` — schema at `docs/wiki/AGENTS.md`) and fix
what it finds. Four checks, most-valuable first. Record the pass as a dated
`.log/YYYY-MM-DD-lint.md` entry in each section touched.

## 0. Review the calibration ledger (before the checks)

Read [`docs/wiki/dev/.log/lint-calibration.md`](../../docs/wiki/dev/.log/lint-calibration.md) — the
**miss-log**: every drift a *prior* lint declared clean, or a scope gap that let drift persist, with
the sharpening it produced. This is to `wiki-lint` what `ratings.md`'s calibration ledger is to
`mycelium-analysis` — it is what turns this from a checklist into an audit. Rules:

- A check with **repeated misses in one area** needs a *structural* fix, not another point patch —
  the ledger tells you which checks have been optimistic (the lock-order table and the reserved-prefix
  list each shipped drift twice before the check was widened).
- **Append to the ledger** whenever *this* pass — or anything since the last one (analysis,
  `doc-coverage`, a code review, a support question) — reveals drift a prior lint should have caught.
  A miss you fix silently is a miss the framework can't learn from. Every seeded sharpening below came
  from a real ledger entry; keep that loop closed.

## 1. Doc-vs-code verification (the load-bearing check)

For every wiki-page claim that cites code, confirm the code still says it. Minimum sweep:

- **Lock-order table** (`docs/wiki/dev/concurrency/lock-order.md`): grep the **whole
  workspace** — the table's scope is all crates since 2026-07-07 (rows 24–30 added the
  companions; the old `src/ mycelium-core/src/` sweep was the blind spot that hid them):
  `grep -rn "Mutex<\|RwLock<" src/ mycelium-core/src/ mycelium-*/src/ --include='*.rs'`
  outside test modules, **and** for lock-wrapping type aliases
  (`grep -rn "^type \|^pub type" … | grep -i "mutex\|rwlock"`) — an alias hides the lock
  from the field grep (the 2026-07-02 lint found `SenderLog` exactly this way). Diff both
  against the table's rows — grouped rows list every field name, so the diff is by name.
  Any undeclared site = a finding (this drift shipped once — analysis Run 28, calibration
  ledger 2026-07-02).
- **Named regression gates**: every test cited by name in a wiki page still exists
  (`grep -rn "<test_name>" src/ mycelium-core/src/ mycelium-*/src/ mycelium-*/tests/`).
  A renamed/deleted gate = a finding.
- **Cited constants/flags** (e.g. `MAX_KV_WRITE_BYTES`, `WIRE_VERSION`/`PREV_WIRE_VERSION`,
  `swim_failure_detector` default): read the cited file and confirm the stated value/default.
  **Scope includes guide *chapters*, not just the front-door docs** — grep every guide page that pins
  the wire version (`grep -rlnE "wire v[0-9]|WIRE_VERSION" docs/guide/`) and diff against
  `framing.rs`. `09-security.md` carried a stale `v10 "(current)"` / `v10↔v9` window through many
  passes because this check stopped at `building-on`/`faq` (ledger 2026-07-11).
- **KV-namespace table** (`src/lib.rs` §KV namespace ownership — code canon, but it drifts
  like a doc): grep the workspace for KV prefix writers
  (`kv_ns::` constants in `mycelium-core/src/signal.rs`, `format!("…/` keys in `kv.set`/
  `kv_set`/`publish_*` call sites, incl. companions) and confirm every live prefix has a
  row. The 2026-07-07 lint found NINE missing (`svc/ log/ clog/ lock/ prompts/ skills/
  installable/ comp/ wiki/`) — the front-door reserved list had inherited the same gap
  because it was only ever diffed against this table, not against code.
- **Endpoint/feature lists** (`docs/wiki/dev/operations.md`): spot-check against
  `src/agent/http.rs` routes and `Cargo.toml` features.
- **CI-gate list** (`docs/wiki/dev/testing/testing.md`): diff the documented gate block against the
  *actual* `run:` steps in `.github/workflows/*.yml`. A page that lists the *clippy* of a crate's
  tests can imply coverage CI doesn't provide — `mycelium-core`'s whole suite was clippy-compiled but
  never *run* (no `-p mycelium-core` test job), and `testing.md` read as if it were covered (ledger
  2026-07-11). Confirm every gate the page names has a live `run:` line.
- **External front-door docs that *restate* code facts** — `docs/guide/building-on-mycelium.md`
  (and lightly `docs/guide/faq.md`). These live outside `docs/wiki/` but duplicate code by
  design, so they drift like a wiki page and are higher-stakes (downstream integrators act on
  them). Verify: the reserved-KV-prefix list matches the `src/lib.rs` namespace-ownership
  table (top-level prefixes — grep `\| \`` rows, diff the sets); `WIRE_VERSION`; the eight
  sub-handle names; the `Cargo.toml` feature flags. A mismatch = a finding (fix the doc). The
  *linking* front-doors (the FAQ's routing tables) need only the dead-link check in §3.

Numbers the wiki deliberately does NOT pin (test counts, dep counts) are exempt — the
convention is "run the suite for the live total".

## 2. Staleness

Pages contradicted by work merged since the last lint: check each section's `.log/` dates
against `git log --oneline --since=<last lint>` — merged PRs with durable knowledge but no
ingest entry indicate a stale or missing page. ✅-shipped items still described as
pending/planned = a finding.

## 3. Orphans & dead links

Every page reachable from its folder-note chain up to `wiki.md`; every relative link
resolves; every folder has its `<folder>/<folder>.md` folder-note. Also resolve every
relative link in the external front-door docs (`docs/guide/faq.md`,
`docs/guide/building-on-mycelium.md`) — they route into the guide/examples and break silently
when a chapter or example is renamed/moved.

## 4. Coverage

Durable knowledge with no wiki home: scan recent merged work (plans marked complete,
analysis findings, new invariants in code comments) and file what's missing under the right
section (routing test in `AGENTS.md`). **Not just "does the knowledge exist somewhere" — does the
*wiki cite* it?** The wiki's contract is "code is canon, the wiki cites it", so a new authoritative
`docs/design/` decision (or ADR) whose subject the wiki already covers must be *linked* from the
relevant wiki page/folder-note, not merely exist in `docs/design/`. A merged design doc that the
companion/architecture pages embody but never reference = a coverage finding (add the pointer). This
is a scope gap a prior pass hit: it counted `coordination-approaches.md` as "covered" because it
existed user-facing, without checking the wiki linked it (ledger 2026-07-13).

## Output

Fix findings directly (they're doc edits), write the dated `.log/` lint entries naming what
was fixed, and report: findings by check, pages touched, anything needing a user decision
(e.g. a new top-level section — never add one unprompted). If a doc-vs-code finding reveals
the *code* is wrong rather than the page, stop and report it as a code bug instead of
"fixing" the wiki to match.

**Close the loop.** If this pass found drift a *prior* lint declared clean (or you learned of such
drift from analysis / `doc-coverage` / a review since the last pass), append a line to
[`.log/lint-calibration.md`](../../docs/wiki/dev/.log/lint-calibration.md): the check, the drift, how
it was found, and the **sharpening** — and, where the sharpening is a concrete check change, fold it
into the checks above (as the 2026-07-11 entries did). A miss recorded but not turned into a sharper
check is a lesson half-learned. A clean pass that surfaced no misses needs no ledger entry — don't
manufacture one.
