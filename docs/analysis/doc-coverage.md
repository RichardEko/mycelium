# Documentation coverage audit

> **Living matrix**, maintained by the `/doc-coverage` skill — refresh it as a *diff*, not a
> re-derivation. Seed run: **2026-07-10** (below). On each run, prepend a dated changelog entry, update
> the matrix cells that moved, and append to the calibration section any prior `Clear` cell later
> found thin.

A systematic audit of whether every core Mycelium concept has a clear landing across a
**WHAT · WHY · HOW × Dev · Ops** matrix — the doc analogue of `ratings.md`'s code audit. Run with
four parallel auditors (substrate · coordination primitives · fleet/groups/topology · security/
extensions/companions), each opening the actual docs and returning a per-cell verdict with the
file:section or the named gap. This file is the persisted result and the **re-run target**: a
future pass should diff against it, not start from scratch.

**Method.** Adversarial (a name-drop is not "Clear"). Cells: **Clear** (correct mental model +
actionable next step, in a doc addressed to that persona or cross-linked) · **Thin** (present but
partial / wrong-audience / HOW missing) · **Missing** · **N/A** (legitimately not that persona's
concern). WHY is usually shared Dev+Ops.

## Changelog

- **2026-07-15 (run 11)** — diff-gated, after the five-pass code audit (Runs 50–58, ~40 fixes) + the
  identity-auth work. **No cell verdict moves** (all stay ✓), but **two "must-be-accurate-if-followed-
  literally" staleness fixes** in the *scored* Ops runbooks, both created by this session's own code
  changes:
  - **Security · HOW·Ops** — `operations/cert-rotation.md` step 2 claimed `sys/identity` is "signed by
    the **old** key"; the code writes it **unsigned** (the identity-poisoning gap). Corrected to state
    unsigned + linked the new **`design/identity-authentication.md`** ADR — which also **enriches
    Security · WHY** (the identity trust-model + the phased fix). Calibration entry added (the **4th**
    claim-present-but-false hit — the **1st a *security guarantee***, not a setting/command).
  - **Operational readiness (HOW·Ops)** — `operations/observability.md`'s `/ready` row said "capabilities
    advertised + no dead shards"; this session changed `/ready` to reflect **startup completion** (a node
    advertising no soft state is now ready). Corrected. (Same drift also fixed in `wiki/dev/operations.md`
    during this session's wiki-lint.)
  The ~40 other code fixes are **bug-fixes that do not move concept landings** — a diff-gated carry for
  every other row. Zero new concepts (identity-auth is a *design* for the existing Security concept, not
  a new sub-handle/companion/standard).
- **2026-07-15 (run 10)** — diff-gated: **no cell moves; carried from run 9.** The delta is the two new
  **artifact-library browser showcases** (`provisioning_viz` :8097 — autonomic self-heal; `catalog_viz`
  :8098 — origin-death survival), both UI-contract compliant (verified live). Concept impact lands on the
  already-✓ **Artifacts / library · HOW·Dev**: a **visual landing** now exists alongside the CLI demos +
  the operations walkthrough — a Tier-3 discoverability add (same shape as runs 6–8), not a verdict
  change. Their run commands carry `--features wasm,metrics` (no repeat of the run-9 gap). Zero
  `src/`·`mycelium-*/src/` change.
- **2026-07-15 (run 9)** — diff-gated. Delta since run 8: the examples **capability-matrix**
  restructure (discoverability; rows already ✓), the **artifact deploy/install surfacing** (a guide
  ladder "Artifacts & deploy" row → `operations/artifacts.md § Solution/Dev`), the **`## Loads`
  banner** across the 5 runtime-loading demos, and the **philosophy `.html`→`.md` port** (WHY home
  renamed; content verbatim — WHY cells carry). Concept impact lands on **Artifacts / library ·
  HOW·Dev** (was `✓ ᵀ²`, mis-homed):
  - The surfacing + Loads banner make the Dev walkthrough **cross-linked from the Dev guide** and the
    5 demos self-declare **content · type · loaded-from** — the mis-homing is resolved.
  - **But the "must work if followed literally" check surfaced a real bug:** the `catalog` and
    `provisioning` run commands **omitted `--features wasm`** in **5 places** (the new guide-ladder row,
    `operations/artifacts.md`, `coop/README.md` ×2, and `presentation.html`), so following them
    literally fails with `error: target … requires the features: wasm`. **Fixed all 5** — the cell is
    now *genuinely* ✓ (cross-linked walkthrough + working commands + the Loads banner). Calibration
    entry below. *(`presentation.html` — a persuasion surface — was edited for the run-command fix only;
    flagged for the next publication-lint.)*
- **2026-07-15 (run 8)** — diff-gated: **no cell moves; carried from run 7.** The only concept-touching
  delta is the new `wiki_council_viz` browser showcase, which enriches the **Companions** row's
  wiki-companion HOW·Dev landing (a watchable specialist-fleet demo alongside the `wiki_chat` CLI). That
  row was already `✓✓✓✓✓`, so this is a Tier-3 discoverability add, not a verdict change — the same
  shape as runs 6–7 (a new/better landing for an already-Clear concept). Zero `src/`·`mycelium-*/src/`
  change; no calibration hit.
- **2026-07-15 (run 7)** — diff-gated: **no cell moves; carried from run 6.** The delta is the
  examples completeness sweep (guardrails · `mycelium-reason` · `wiki_chat` indexed; README restructured
  into one flow) + the Ops Console **Audit** tab. Concept impact lands on two already-`✓✓✓✓✓` rows:
  - **Reasoning / LLM / MCP / guardrails · HOW·Dev** — genuinely Clear before: `guide/16-guardrails.md`
    links the runnable `guardrail_fleet` / `guardrail_wedge`. The user's "no examples for guardrails"
    gap was examples-*index* discoverability (`examples/README.md` didn't list them), fixed under
    wiki-lint — **not** a concept-doc gap. No move.
  - **Security (TLS/RBAC/SSO/audit) · HOW·Dev** — Clear at the **API** level (`09-security.md`
    § "in practice" already has write/verify + a `GET /gateway/audit` curl), but it cross-linked **no
    runnable way to *see* the trail** — the "no examples for viewing audit" the user actually hit.
    **Fixed** (Tier-3 discoverability): a "See it live" pointer to the `community` mgmt UI + the new
    Ops Console Audit tab. Cell stays ✓. Calibration entry below.
  Zero `src/`·`mycelium-*/src/` change.
- **2026-07-14 (run 6)** — diff-gated: **no cell moves; carried from run 5.** Since run 5 the only
  concept-touching docs delta is on **Reasoning / LLM / LangGraph**: the FAQ now cites the langgraph
  `deploy/reheal` rung for its "survives the loss of the orchestrator node" claim (`eb89232`), and
  `15-reasoning-and-langgraph.md`'s flagship section links back to that FAQ positioning (`28ad9b1`) —
  so FAQ-claim ↔ ch15-how-to ↔ example are now three-way connected. That row was already `✓✓✓✓✓`, so
  this is a **Tier-3 discoverability improvement** (WHY↔HOW connectivity, shown-not-told), not a
  verdict change. Everything else in the delta is examples hygiene (orphan `mesh_demo` deleted,
  `diagnostics` registered, operator demos re-themed + ops-linked) and non-concept prose (the broker
  bullet de-jargon) — no concept's landing gained or lost. Zero `src/`·`mycelium-*/src/` change. No
  calibration hit (no prior-Clear cell found Thin).
- **2026-07-14 (run 5)** — diff-gated. Delta since run 4 is the `cluster_name` work (`f5c7f6c`,
  `15a33eb`): every example now sets a cluster name, and `guide/13-cluster-topology.md` gained the
  `apply_env_overrides()` caveat. **Calibration hit** (below): **Membership + cluster_name · HOW·Dev**
  was ✓ in run 1 (the seed called this corner *"the strongest, Clear on every cell"*), yet the
  documented `GOSSIP_CLUSTER_NAME=…` way to set it **silently no-ops** unless the app calls
  `apply_env_overrides()` — a real user hit exactly this. The instruction was *present* but did not
  *work when followed literally*. **Fixed** (`13-cluster-topology.md` ⚠️ caveat + build→apply→new
  sequence); the cell is now genuinely ✓. Separately, the **Ops Console** (`examples/ops_console.rs`)
  enriches **Observability · HOW·Ops** (a live dashboard over `/stats`·`/gateway/fleet`·
  `/gateway/diagnose`·`/metrics`) but that cell was already ✓ — no verdict move. No other concept
  touched; the rest carry from runs 3–4.
- **2026-07-14 (run 4)** — diff-gated: **no material diff to the matrix; carried unchanged from run 3.**
  Zero product-core (`src/` · `mycelium-*/src/`) change since run 3. The whole delta is
  examples/tooling/docs: four browser **visual showcases** (`microgrid_viz` · `stigmergy_viz` ·
  `redistribution_viz` · `llm_council_viz`, the `/state`+canvas pattern) plus their discoverability
  across all three surfaces (wiki `dev/examples.md`, `examples/README.md`, the presentation deck), the
  conway bind/URL fixes, and the local scale-nightly runner. This **enriches** the
  Companions (tuple-space/blackboard) and coordination **HOW·Dev** cells — a reader now has runnable
  visual demos — but those cells were already ✓, so **no verdict moves**. No new concept (a visual
  showcase is an example *category*, not a substrate concept warranting a row). Not re-audited; the
  next scored re-audit waits for a concept-cell-moving change (product code, a new sub-handle/companion,
  or a found gap).
- **2026-07-13 (run 3)** — diff-gated re-audit. The only material diff since run 2 is this session's
  two commits: `8456dc4` (wiki-store **section-granular CAS** — the dual-curator lost-update fix) and
  `d316cdf` (the **`coordination-approaches.md`** design note + cross-links). **No concept cell
  regressed.** The wiki-store CAS is an internal correctness fix, documented in `companions/wiki.md`
  and `wiki-concurrent-edit.md §3.5` (agent/WHY-facing — no new persona gap). The design note **closes
  a latent WHY gap** and produced a **calibration hit** (below): runs 1–2 scored **Distributed
  locks · WHY** and **Companions · WHY** as ✓, but the *cross-cutting* decision — *when to reach for
  the distributed lock vs the capability ring, and why all three companions reject it* — had no
  user-facing home. Each primitive's own rationale was covered; the **comparison spanning
  Locks+Companions+Consensus fell between the matrix rows** (a structural blind spot the row-by-row
  scoring cannot see). *Fixed:* `docs/design/coordination-approaches.md` (CP-vs-AP decision matrix +
  the rule + a fourth-companion checklist), cross-linked so **both** personas reach it — Dev via
  `04-consensus.md` / `faq.md`, Ops via `companions.md`, plus `exactly-once-effect.md`,
  `wiki-concurrent-edit.md`, and `docs/README.md`. All rows carry; WHY for Locks/Companions/Consensus
  is now genuinely — not nominally — Clear.
- **2026-07-11 (run 2)** — diff-gated re-audit. Nothing in the existing matrix's *concept* cells
  changed since the seed (the post-seed commits were the wire-compat gate, wiki ingests, the two new
  skills, and persuasion-surface fixes) — those rows **carry**. One **new concept row** was surfaced
  by the wire-compat gate: **Rolling upgrade**. WHY/WHAT/HOW·Dev were covered (`building-on-mycelium`,
  `faq`, `09-security`, `error-handling`); **HOW·Ops was Thin** — only a one-line "supported"
  assurance in `production-readiness`, no procedure. *Fixed:* added `operations/deployment.md §
  Rolling upgrades` (node-by-node procedure + the two-step-gap tripwire) with cross-links from
  `09-security` and `production-readiness`. Also fixed a **staleness the seed missed** —
  `09-security.md` cited wire **v10/v9** as current → **v12/v11** (logged under Calibration).
- **2026-07-10 (run 1, seed)** — the full four-auditor audit + Tier 1–3 remediation (below).

## Headline

The architecture holds up: `docs/README.md` assigns every area a document *type* and each doc a
declared *audience*, so the WHAT/WHY/HOW × Dev/Ops matrix is how the tree is actually cut, not a
retrofit. At audit time the large majority of cells were already Clear, **no cell was a black
hole**, and the recently-reworked cluster/group/`cluster_name` corner was the strongest (Clear on
every cell). The gaps clustered in one place: **operational failure-mode runbooks for the
consensus/lock family, and Dev guide-chapters for two shipped features.** All of them are now
closed (Tiers 1–3); the residue is genuinely nothing at ✗ or `~`.

## Final matrix (post-remediation)

Legend: ✓ Clear · — N/A. Every cell that was ✗/`~` at audit time is annotated with the pass that
closed it.

| Concept | WHY | WHAT·Dev | HOW·Dev | WHAT·Ops | HOW·Ops |
|---|:--:|:--:|:--:|:--:|:--:|
| Layer I — Gossip KV | ✓ | ✓ | ✓ ᵀ² | ✓ | ✓ |
| Layer II — Signal mesh | ✓ | ✓ | ✓ | ✓ | ✓ |
| Layer III — Consensus | ✓ | ✓ | ✓ ᵀ² | ✓ ᵀ¹ | ✓ ᵀ¹ |
| Capabilities / groups | ✓ | ✓ | ✓ | ✓ | ✓ |
| Distributed locks | ✓ | ✓ | ✓ | ✓ ᵀ¹ | ✓ ᵀ¹ |
| Services / RPC | — | ✓ | ✓ ᵀ² | ✓ | ✓ |
| Schema lifecycle | ✓ | ✓ | ✓ | ✓ | ✓ ᵀ¹ |
| Scopes (Cluster/Group/Individual) | ✓ ᵀ³ | ✓ | ✓ | ✓ ᵀ³ | — |
| Membership + cluster_name | ✓ | ✓ | ✓ | ✓ | ✓ |
| Groups (three kinds) | ✓ | ✓ ᵀ³ | ✓ | ✓ | ✓ |
| Legible Emergence | ✓ ᵀ³ | ✓ | ✓ ᵀ³ | ✓ | ✓ |
| Security (TLS/RBAC/SSO/audit) | ✓ | ✓ | ✓ | ✓ | ✓ |
| Artifacts / library | ✓ | ✓ | ✓ ᵀ² ᴿ⁹ | ✓ | ✓ |
| Federation / AgentFacts | ✓ | ✓ | ✓ ᵀ¹ | ✓ | ✓ |
| Reasoning / LLM / MCP / guardrails | ✓ | ✓ | ✓ ᵀ² | ✓ | ✓ |
| Companions | ✓ | ✓ | ✓ | ✓ | ✓ ᵀ² |
| Rolling upgrade (wire compat) | ✓ | ✓ | ✓ | ✓ | ✓ ᴿ² |

ᵀ¹ closed in Tier 1 · ᵀ² Tier 2 · ᵀ³ Tier 3 · ᴿ² closed in run 2 (2026-07-11) · ᴿ⁹ run-command fix + re-verified, run 9 (2026-07-15).

## What was found, and how it was closed

### Tier 1 — real holes (✗ / weak-pair cells)
- **Locks · HOW·Ops** was ✗ — a shipped feature with no operational recovery story. Fix: a
  `diagnostics.md` "Stuck / contended lock" runbook (`GET /consensus/lock/{name}` inspection,
  lease-expiry self-heal) + the new metric family.
- **Federation · HOW·Dev** was ✗ — the only external-interop standard with no guide chapter. Fix:
  new `guide/17-federation.md` (serve/verify the edge doc + the multi-author domain board).
- **Consensus · HOW·Ops** — diagnostics covered only the *conflict* case, not the common *no-quorum
  stall*, and consensus had no Prometheus surface. Fix: a "Consensus stalled — quorum unavailable"
  runbook + the `mycelium_consensus_*` metric family.
- **Schema · HOW·Ops** — `schema_mismatch` was a `/stats` scalar with no runbook. Fix: a "Schema
  mismatch" runbook + a mirror gauge.

New in Tier 1: the `mycelium_consensus_*` metric family — `mycelium_consensus_timeouts_total{reason}`
(event-emitted; `no_voters`/`quorum_short`/`all_opaque`/`empty_groups`) plus
`mycelium_consensus_commit_conflicts` / `mycelium_schema_mismatch` gauges mirroring the `/stats`
scalars. Deliberately **no per-lock gauge** (cardinality) — locks are consensus slots, inspected via
`GET /consensus/lock/{name}`.

### Tier 2 — Dev-guide HOW trapped elsewhere
- Consensus **leased commits** + the **converged-holder discipline** → added to `04-consensus.md`
  (were `src/lib.rs`/wiki only).
- **MCP external bridge** (`connect_mcp_server`) → new section in `06-tool-discovery.md` so the
  chapter matches its title; bridged tools land in the same `tools/` namespace.
- **Artifacts** Dev routing → `cookbook.md` recipe now points at the Solution/Dev + DevOps anchors.
- **Companions ops runbook** → new `operations/companions.md` (durability/WAL, capability-ring
  failover, the wiki's node-independent store, teardown; **none emit Prometheus metrics**).
- **RPC** discoverability → cookbook recipe links the service-layer reference.

### Tier 3 — WHY / discoverability polish
- **Legible Emergence** got a WHY landing in `philosophy.md` (under Emergent Levels / Anderson).
- **`explain` gateway-only** made intentional in `diagnostics.md` + `src/lib.rs` (it is a cross-node
  `sys.explain` RPC fan-out, not a local read — no in-process accessor by design).
- **System→Cluster** ops note in `observability.md` (the gateway still accepts `"system"`).
- Scope-unification WHY sentence (`13-cluster-topology.md`); three-kinds-vs-API cross-link
  (`00-concepts.md`).

## Bugs the audit surfaced (correctness, not coverage)

The digging turned up defects that reading-for-coverage exposed — the recurring lesson that
verifying-against-code finds real problems:

1. **Fencing-token doc drift** — `lock_service.rs:46` + `04-consensus.md:366` called `LockGuard::token`
   a "consensus ballot", contradicting the guide, the module's own test, and the #164 fix (the token
   is the commit HLC; the ballot regresses under gossip lag). Fixed.
2. **`GossipError` enum was fabricated** — `error-handling.md` documented `Network(String)` /
   `Config(String)` (which do not exist) and omitted five real variants incl. `FrameTooLarge`.
   Confirmed `mycelium::GossipError` is the re-exported mycelium-core enum; rewrote to the real 10.
3. **Broken/inaccurate anchors** — `#gossipError`→`#gossiperror`; the diagnostics verb table's
   "all also available programmatically" was false for `explain`.

## Artifacts created

- `docs/guide/17-federation.md` (new chapter)
- `docs/operations/companions.md` (new runbook)
- `mycelium_consensus_*` + `mycelium_schema_mismatch` metric family (`metrics.md` §Consensus/locks)
- Three new `diagnostics.md` pathologies + a `mycelium-consensus` Prometheus rule group
- **Run 2:** `operations/deployment.md § Rolling upgrades` (new operator procedure)
- **Run 3:** `docs/design/coordination-approaches.md` (new WHY decision guide — when to use the
  distributed lock vs the capability ring, and why the three companions reject it; cross-linked from
  `04-consensus.md`, `companions.md`, `exactly-once-effect.md`, `wiki-concurrent-edit.md`, the guide
  FAQ, and `docs/README.md`)

## Calibration

Prior `Clear` cells later found Thin/Missing — the ledger that scores this audit's own verdicts (the
doc analogue of `ratings.md`'s calibration ledger). A cell with repeated hits deserves structural
skepticism, not a re-asserted ✓.

- **2026-07-11 — Security · WHAT/HOW·Dev** was `Clear` in run 1 (seed) while `09-security.md` cited
  the wire version as **v10 "(current)"** and framed the rolling-upgrade window as **v10 ↔ v9** —
  both stale (current is v12/v11). Found by run 2's rolling-upgrade diff-audit. Root cause: the seed
  auditor confirmed the *concept* was explained but did not spot-check the *version constant* in the
  prose. Lesson for future runs: a `Clear` verdict on a doc that pins a constant/version must verify
  the value against code, not just its presence.
- **2026-07-13 — Distributed locks · WHY + Companions · WHY (cross-cutting)** were `Clear` in runs 1–2,
  but the decision *"which coordination primitive do I reach for, and why not the lock?"* had **no
  user-facing landing** — each primitive's own rationale existed (`04-consensus.md` for the lock,
  `exactly-once-effect.md` for the companions), yet the *comparison* lived nowhere: `companions.md`
  only **asserted** "no distributed lock" without the why. Found by a user question ("is the
  lock-vs-ring design decision documented anywhere?") + a doc audit that confirmed the absence. Root
  cause is **structural, not an oversight**: the matrix scores each concept's cells independently, so a
  cross-cutting decision guide that spans several concepts (here the CP-vs-AP coordination axis over
  Locks/Companions/Consensus) falls *between* rows and reads as covered when every individual cell is
  ✓. **Sharpening (a method change, not a point patch):** when two or more concepts share a decision
  axis, audit whether the *comparison itself* has a home — add a "cross-cutting decisions" pass that
  asks "if a reader must choose between these N concepts, where do they learn how?", distinct from
  scoring each concept's own WHY. Fixed: `coordination-approaches.md`.
- **2026-07-14 — Membership + cluster_name · HOW·Dev** was `Clear` in run 1 (the seed called this
  corner *"the strongest, Clear on every cell"*) while the documented way to set it via
  `GOSSIP_CLUSTER_NAME` **silently did nothing** — env vars only apply if the binary calls
  `cfg.apply_env_overrides()`, which `13-cluster-topology.md` never mentioned. Found by a user question
  ("cluster name is unset — how do I set it?"). Root cause: the auditor confirmed the instruction was
  *present*, not that it *works when followed literally*. **This is the 2nd hit of the same class**
  (Security wire-version, 2026-07-11 was the 1st): a cell marked Clear on the *presence* of an
  instruction/value whose content was actually stale or silently-failing. **Sharpening:** a `Clear`
  verdict on any doc that gives a **setting / config / run instruction** must verify the steps,
  followed literally, actually *succeed* (or trace to code that makes them succeed) — presence is not
  sufficiency; a silently-no-op instruction is **Thin**, not Clear. Folded into the skill's adversarial
  rule. Fixed: `guide/13-cluster-topology.md`.
- **2026-07-15 — Security (audit) · HOW·Dev** read `Clear` across runs 1–6: `09-security.md`
  § "The audit trail in practice" covers the trail's write / verify / query API thoroughly (incl. a
  `GET /gateway/audit` curl). But it cross-linked **no runnable demo or browser view** of the trail, so
  a dev's "how do I *watch* this fill?" had no landing. Found by a user question ("we have no examples
  for viewing audit or guardrails?"). **A lighter hit than a full Clear-found-Thin:** the API-level HOW
  *was* Clear (curl present), so this is a **runnable-landing** gap, not a mental-model gap. Root cause
  shares this session's theme — the example that would *be* that landing (`community`, the audit
  producer) wasn't cross-linked from the concept doc, and the companion examples (guardrails, reason)
  weren't indexed at all (the examples audit only swept `examples/` + coop). Fixed: a "See it live"
  pointer in `09-security.md` → the `community` mgmt UI + the new Ops Console **Audit** tab.
  **Sharpening:** a HOW·Dev `Clear` on a mechanism a reader would want to *watch* (audit, emergence,
  convergence) should confirm a **runnable / visual landing** is cross-linked, not just the API + a curl.
- **2026-07-15 — Artifacts / library · HOW·Dev** read `✓ (ᵀ²)` across runs 1–8 while the `catalog` and
  `provisioning` demo **run commands silently failed** — documented without the `--features wasm` those
  bins require (`coop/README.md` ×2, `operations/artifacts.md`, and — introduced in run 9's own diff —
  the new guide-ladder row + `presentation.html`), so a dev following any of them hit `error: target …
  requires the features: wasm`. **The 3rd hit of the "instruction present but no-ops/fails" class**
  (wire-version 2026-07-11; `GOSSIP_CLUSTER_NAME` 2026-07-14). Found by run 9's must-work-if-followed
  spot-check — and notably by *running the binary*, which is how the missing feature first surfaced.
  Root cause: prior `Clear` verdicts confirmed the *walkthrough prose + API* but never *ran the demo
  command*; the three bins that **did** carry `--features wasm`
  (`mcp_toolgrowth`/`model_deploy`/`reheal_deploy`) masked the two that didn't. **Reinforces the
  2026-07-14 sharpening** rather than adding a new one — the existing "verify the steps succeed" rule
  already covers this; the gap was applying it to the *run command*, not just env-vars/constants. Fixed
  all five.
- **2026-07-15 (run 11) — Security · HOW·Ops** read `✓` across runs 1–10 while `operations/cert-rotation.md`
  step 2 asserted `sys/identity/{self}` is **"signed by the old key"** — **false in code**: the entry is
  written UNSIGNED (`encode_identity_history` → raw `32×N` bytes; the publish is a bare `kv().set`, no
  Ed25519 signature). The signature was design intent that was never implemented, and the false claim
  masked the identity-poisoning gap (a compromised admitted node can LWW-inject a verifying key; code
  audit pass 3, 2026-07-15). Found by this session's code audit + this run's diff-check of the identity
  docs. **The 4th "claim present but false-in-code" hit — and the first where the false claim is a
  *security guarantee* ("signed"), not a setting/command** (wire-version 2026-07-11; `GOSSIP_CLUSTER_NAME`
  2026-07-14; artifact run-commands 2026-07-15). **Sharpening (extends the 2026-07-14 rule):** the
  value-vs-code check must also cover **asserted guarantees** — "signed" / "authenticated" / "verified" /
  "validated" — a `Clear` verdict on a doc claiming a crypto or safety property must confirm the code
  *performs* it, not merely that the property is described. Fixed: `cert-rotation.md` step 2 (states
  unsigned + links `design/identity-authentication.md`); also this session, the `rotate_identity` code
  comments + `wiki/dev/security.md`, and the stale `/ready` row in `observability.md`.

## Re-run guidance

The audit was a one-time systematic sweep; a re-run should be a **diff**. Re-audit a concept only
when its code/docs changed since the last run (run 8 baseline commit `6ff383f`: `git log 6ff383f..HEAD -- docs/ src/
mycelium-*/src/`). The matrix
above is the baseline: any cell dropping below ✓ is a regression. The method (four auditors, the
Clear/Thin/Missing rubric, the exact prompts) is reproducible from this session's transcript. New
concepts (a new sub-handle, a new companion, a new external standard) each need a fresh row audited
across all five cells.
