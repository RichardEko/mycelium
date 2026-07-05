Run a lint pass over the LLM wiki (`docs/wiki/` — schema at `docs/wiki/AGENTS.md`) and fix
what it finds. Four checks, most-valuable first. Record the pass as a dated
`.log/YYYY-MM-DD-lint.md` entry in each section touched.

## 1. Doc-vs-code verification (the load-bearing check)

For every wiki-page claim that cites code, confirm the code still says it. Minimum sweep:

- **Lock-order table** (`docs/wiki/dev/concurrency/lock-order.md`): grep the workspace for
  `Mutex<` / `RwLock<` field declarations outside test modules
  (`grep -rn "Mutex<\|RwLock<" src/ mycelium-core/src/ --include='*.rs'`) **and** for lock-
  wrapping type aliases (`grep -rn "^type \|^pub type" … | grep -i "mutex\|rwlock"`) — an
  alias hides the lock from the field grep (the 2026-07-02 lint found `SenderLog` exactly
  this way). Diff both against the table's rows. Any undeclared site = a finding (this
  drift shipped once — analysis Run 28, calibration ledger 2026-07-02).
- **Named regression gates**: every test cited by name in a wiki page still exists
  (`grep -rn "<test_name>" src/ mycelium-core/src/`). A renamed/deleted gate = a finding.
- **Cited constants/flags** (e.g. `MAX_KV_WRITE_BYTES`, `WIRE_VERSION`/`PREV_WIRE_VERSION`,
  `swim_failure_detector` default): read the cited file and confirm the stated value/default.
- **Endpoint/feature lists** (`docs/wiki/dev/operations.md`): spot-check against
  `src/agent/http.rs` routes and `Cargo.toml` features.
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
section (routing test in `AGENTS.md`).

## Output

Fix findings directly (they're doc edits), write the dated `.log/` lint entries naming what
was fixed, and report: findings by check, pages touched, anything needing a user decision
(e.g. a new top-level section — never add one unprompted). If a doc-vs-code finding reveals
the *code* is wrong rather than the page, stop and report it as a code bug instead of
"fixing" the wiki to match.
