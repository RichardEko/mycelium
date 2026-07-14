Audit whether every core Mycelium concept has a clear landing across a **WHAT · WHY · HOW ×
Dev · Ops** matrix, then refresh `docs/analysis/doc-coverage.md` (the living matrix + a dated
changelog) so drift is a diff, not a re-derivation. This is the documentation analogue of
`mycelium-analysis`: it measures the *absence of explanation gaps*, not the presence of pages.

**Why a matrix.** The framework is Diátaxis (Procida) crossed with audience: **WHAT** = Reference
(`00-concepts`, `src/lib.rs` canon), **WHY** = Explanation (`philosophy.html`, `design/` ADRs,
papers), **HOW** = Tutorial + How-to (`guide/` chapters, `cookbook`, `examples/`, `operations/`
runbooks). The audience axis (**Dev** = integrator building on the public API · **Ops** = operator
running a cluster) is the crossing Diátaxis lacks and where most real gaps hide (a concept with a
Dev how-to but no Ops runbook, or vice-versa).

**Adversarial rule (the whole point).** A name-drop is **not** "Clear." A cell is Clear only if a
reader *of that persona* lands a correct mental model **and** an actionable next step, in a doc
addressed to them or explicitly cross-linked from one — and you verified it by opening the doc, not
by trusting a title. This is the doc analogue of M2's execution-evidence gate: reading a table of
contents inflates coverage; opening the page is the evidence.

**Presence is not sufficiency — a HOW instruction must *work if followed literally*.** For any cell
whose doc gives a **setting / config / run instruction** (an env var, a config field, a command, a
version constant), a `Clear` verdict requires that the documented steps, followed literally, actually
*succeed* — or trace to code that makes them succeed. An instruction that is *present but silently
no-ops* is **Thin**, not Clear. Two calibration hits are of exactly this class: the stale wire-version
constant (2026-07-11) and `GOSSIP_CLUSTER_NAME` documented without the `apply_env_overrides()` it
requires (2026-07-14, a user hit it). So: spot-check the *value/behaviour* against code, not just its
presence in the prose.

## Step 0 — Baseline & diff gate

Read the current `docs/analysis/doc-coverage.md` first (the living matrix + its changelog). Then
diff the docs/code since the last run: `git log --oneline --since=<last-run date> -- docs/ src/
mycelium-*/src/`. **Re-audit a concept only when its docs or code plausibly changed**; otherwise
carry its prior cell verdicts with a `carried` note. A new concept (a new sub-handle, companion,
external standard, or layer) always gets a fresh row audited across all five cells. If nothing
material changed since the last run, append a one-line "no material diff" changelog entry and stop —
never re-run on an unchanged tree.

## Step 1 — Pin the concept inventory (fresh each run)

Derive the concept list from code, don't reuse a stale list:
- Sub-handles: `grep -nE "pub fn (kv|mesh|consensus|service|capabilities|mcp|llm|schema)\b" src/agent/mod.rs`.
- The three layers (gossip-KV · signal-mesh · consensus), scopes (`Cluster · Group · Individual`),
  membership + `cluster_name`, groups (three kinds + governed/ungoverned), locks, legible emergence,
  security (TLS/RBAC/SSO/audit), artifacts, federation/AgentFacts, reasoning/LLM/MCP/guardrails,
  companions (tuple-space · blackboard · wiki).
Cross-check against `src/lib.rs` (public re-exports) so nothing shipped is missing a row.

## Step 2 — The doc map (where each cell should live)

| Cell | Home |
|---|---|
| WHY (shared Dev+Ops) | `docs/philosophy.html` · `docs/design/*.md` ADRs · `docs/publications/` |
| WHAT · Dev | `docs/guide/00-concepts.md` · `src/lib.rs` crate doc · `docs/reference/` |
| HOW · Dev | `docs/guide/*.md` chapters · `docs/guide/cookbook.md` · `examples/` |
| WHAT/HOW · Ops | `docs/operations/*.md` runbooks (+ its `README.md` funnel) |
| Synthesis (agent-facing, not a persona) | `docs/wiki/` — cited, not a scored cell |

## Step 3 — Fan out auditors (parallel)

Partition the inventory into 3–5 clusters and dispatch one `general-purpose` agent per cluster (in
a single message so they run concurrently). Each agent opens the cited docs and returns, per concept,
a per-cell verdict with the **file:section or the named gap**:

- **Clear** — persona lands model + next step; cite `file:section`.
- **Thin** — present but partial: name-dropped, buried in a wrong-audience doc with no cross-link, or
  WHY/WHAT present but the actionable HOW missing.
- **Missing** — no landing for that persona.
- **N/A** — legitimately not that persona's concern (justify in one line; don't invent a gap).

Give every agent the rubric above verbatim and the doc map. Tell them the final message *is* the
report (consumed by the synthesizer, not shown to a human) and to end with their cluster's top-3 gaps
+ a concrete one-paragraph fix each.

## Step 4 — Synthesize the matrix + rank gaps

Merge into one `Concept × {WHY, WHAT·Dev, HOW·Dev, WHAT·Ops, HOW·Ops}` table (✓ / ~ / ✗ / —). Rank
the non-✓ cells into tiers: **Tier 1** = real holes (✗ / weak-pair cells that ship a silent gap);
**Tier 2** = HOW trapped in the wrong doc (content exists, mis-homed); **Tier 3** = WHY/discoverability
polish. The **floor (the ✗/✗/✗ cells) is the headline**, not a coverage percentage.

**Fix vs report.** Doc gaps are edits — fix them (verify against code first; a name-drop fix that
invents an API is worse than the gap). But if a "gap" turns out to be **the code missing the feature
the docs would describe**, stop and report it as a *code* gap, don't paper the docs over it. Verify
every anchor you add (the em-dash/`?`→`--` slug trap is real).

## Step 5 — Calibration (the discipline that makes it compound)

Keep a `## Calibration` section in `doc-coverage.md`: whenever a cell marked **Clear** in a prior run
is later found **Thin/Missing** (by this skill, by a user, or by a support question), append a line —
`{date}: {concept}·{cell} was Clear in run {d} but {what was actually missing} (found by {what})`.
This is the doc analogue of M2's ledger: it measures whether the audit's "Clear" verdicts predict
reality. A cell with repeated calibration hits deserves structural skepticism, not a re-asserted ✓.

## Step 6 — Persist & report

Refresh `docs/analysis/doc-coverage.md`: the current filled-in matrix, the ranked gaps + their fixes
(with commit refs once fixed), the calibration section, and a dated changelog entry at the top
naming what moved since the last run. Lead the user-facing report with the **floor + the Tier-1
count**, then the table. If a new top-level analysis artifact or wiki section seems warranted, propose
it — never add one unprompted.
