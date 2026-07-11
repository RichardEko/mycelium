Lint the **persuasion surface** — the decks, papers, and the philosophy — against shipped reality,
and fix (or flag) what has drifted. `wiki-lint` keeps the *internal* docs honest against code;
this keeps the *external, claim-making* docs honest against what actually ships. The stakes are
higher (a reviewer or buyer acts on these) and the dangerous direction is **overclaim** — selling
roadmap as shipped, or a guarantee the substrate doesn't make.

**Why this exists.** The failure mode is documented: the customer pitch once sold the *shipped*
Legible-Emergence work as "Next — on the roadmap," and a **human** caught it, not a mechanism. The
inverse (selling unbuilt work as done) is the reputational/contractual risk. Both are drift between
persuasion and reality, and neither `wiki-lint` nor `doc-coverage` looks at these files.

**Targets.** `docs/publications/presentation.html` (engineer-facing deck), `docs/publications/
customer-pitch.html` (buyer-facing deck), `docs/publications/paper1/`, `docs/publications/paper2a/`,
and `docs/philosophy.html`. Rendered PDFs are derived — lint the source.

## 1. Claim-vs-reality (the load-bearing check)

Every capability, status, and number claim must match what ships. Sweep each target:

- **Capability/feature claims** → the named feature/API exists in the code canon (grep `src/`,
  `mycelium-*/src/`) or a shipped milestone. A slide describing an API that isn't public = a finding.
- **Status claims** ("shipped" / "available" / "done" / "in production" / "roadmap" / "next") →
  reconcile against `CLAUDE.md`, `ROADMAP.md`, and `docs/plans/README.md` (all engineering plans
  shipped 2026-06-21; v2.0 complete, 16 milestones). **Both directions, and weight overclaim as the
  higher severity:** *roadmap-sold-as-shipped* is a Critical finding (a buyer acts on it);
  *shipped-sold-as-roadmap* is a Major undersell (leaves value on the table — the caught bug).
- **Performance / scale numbers** ("100 nodes", "N ms", "X ops/s") → must trace to a real
  benchmark or scale-test result (`benches/`, `make test-scale`), not be aspirational. An unsourced
  number is a finding; cite the source or cut it.
- **Version / architecture facts** → wire `v12`/PREV `11`; scopes `Cluster · Group · Individual`;
  three layers (gossip-KV · signal-mesh · consensus). A deck contradicting these = a finding.

If a claim reveals the **code is behind the pitch** (the feature genuinely isn't built), stop and
report it as a *product* gap — do not "fix" it by softening the deck to match a lie in the other
direction. And never fix an *undersell* by deleting a true capability; state it correctly.

## 2. Overclaim & the honesty of the ceiling

The substrate's own honest limits are in `philosophy.html` §"What This Architecture Is Not" — every
deck/paper must stay inside them. Flag:

- **CFT, not BFT.** Mycelium is coordinator-free / crash-fault-tolerant; **Byzantine fault
  tolerance is explicitly out of scope** (`framing.rs` signing doc). Any "Byzantine", "trustless",
  or "tamper-proof against malicious nodes" claim is a Critical overclaim.
- **Unearned absolutes** — "guarantees", "never fails", "zero-config", "infinitely scalable",
  "linearizable" (consensus reads are local/lease-aware, not linearizable). Each needs an evidenced
  qualifier or a cut.
- **Detection, not prevention** — the substrate *detects and names* violations (tripwires), it does
  not centrally prevent them. A deck claiming enforcement it doesn't have is a finding.

## 3. Roadmap-vs-shipped framing

Every slide/section that describes a capability must be unambiguously labelled **shipped**,
**roadmap**, or **research**. A capabilities slide that mixes the three without labels is the exact
shape of the caught bug — flag every unlabelled forward-looking claim.

## 4. Binding-framing compliance (durable user constraints)

- **Library, not platform.** No "platform", "daemon", "control plane", "cluster manager" language —
  a cluster is emergent from reachability + CA, there is no central runtime. Violations are findings.
- **Constructive example domains.** Examples must be eco/social-constructive (microgrids, food
  redistribution, co-ops); **never** war-room / crisis / military / surveillance framing. A pitch
  built on a crisis scenario is a finding regardless of how well it "sells."

## 5. Cross-artifact consistency

The two decks, the papers, and `philosophy.html` must agree with each other and with the wiki on the
core facts: the layer model, the scope vocabulary, the coordinator-free-CFT thesis, and the
"emergence of coordination is a prediction, not an embarrassment" argument. A number or claim that
differs across two artifacts is a finding (fix both to the code-canonical value).

## 6. Dead links & stale figures (lighter)

Resolve relative links; check that referenced figures/demos still exist and still show what the
caption claims.

## Output

Fix the clear factual findings directly (a wrong status, a dead link, a framing violation). For
**tone/severity judgment calls** (how strong a claim is defensible, whether a scenario reads as
crisis-framing) — flag with a recommendation rather than silently rewriting the author's voice; these
are the author's call. Report findings by severity (Critical = a buyer/reviewer would be misled;
Major = undersell or unsourced number; Minor = polish), name the file:location, and keep a running
`## Overclaim ledger` note in `docs/publications/README.md` — dated lines recording any overclaim that
reached an external artifact, the calibration analogue that tells you whether this lint is catching
them before the humans do. If a finding is a *product* gap (code behind the pitch), report it as that,
not as a doc edit.
