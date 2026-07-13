Evaluate the Mycelium project across 25 orthogonal dimensions, rate each 1–10,
then append a timestamped entry to `docs/analysis/ratings.md` so the series
can be tracked over time.

**Methodology v2 (M2), adopted 2026-06-10.** Runs 1–15 used v1 (read-and-rate).
M2 exists because v1 saturated: by Run 13 the scores measured the presence of
artifacts (lock tables, policies, guides) rather than the absence of defects,
and a real concurrency race shipped under fifteen consecutive 8–9 scores. M2
converts the skill from an assessment into an audit: scores require execution
evidence, every run must attempt to falsify its own best scores, and the series
keeps a calibration ledger that records when high scores were later proven
wrong. Entries are headed `Run {N} (M2)`; do not compare absolute values across
the v1/v2 boundary.

## Step 0 — Cadence gate and blind-scoring rule

**Cadence gate.** Before anything else, check the diff since the previous run
(commits + working tree). If there is no material change to code, tests, or
docs (e.g. only `ratings.md` itself), do NOT produce scores: append a one-line
note-only entry ("Run {N} (M2) — skipped, no material diff since Run {N−1}")
and stop. Never run more than once per day on the same diff.

**Blind scoring.** Do not read previous runs' scores or notes until your own
scores for this run are written down. Read `docs/analysis/ratings.md` only
afterwards — to determine the run number, update the calibration ledger, and
write the delta narrative. (Anchoring on the prior table is how a series
flatlines at 9.)

## Step 1 — Load context

Read the following files before rating anything. They are the canonical sources:

- `docs/philosophy.html` — the authoritative definition of purpose; anchor for
  dimensions 1 and 2. Read this first.
- `README.md`, `ROADMAP.md`, `CLAUDE.md` — architecture, layer model, active plans.
- `Cargo.toml` — features, dependencies, description.
- `src/lib.rs` — public API surface, KV-namespace ownership table, crate doc.
- `src/agent/mod.rs` — sub-handle accessors, lifecycle methods.

Then explore source files as needed to evaluate individual dimensions. Read deeply
where a rating is uncertain — shallow reads produce inflated scores.

---

## Step 2 — Rate all 25 dimensions

Rate each 1–10. Use the full range. Three M2 rules govern every score:

**Execution-evidence gate.** A dimension may score **9 only with fresh
execution evidence produced during this run** — a test suite executed, a build
performed, an endpoint probed, a benchmark run, a Docker scenario passed — and
the evidence must be named in the notes (suite + result). Reading code or
documentation alone, however careful, caps a dimension at **8**. A **10
additionally requires external validation** (a third-party production
deployment, an outside audit, an independent reproduction) — which correctly
makes 10 unreachable from inside this loop today. Never run a long suite *just*
to unlock a 9; if the evidence doesn't already exist from real verification
work, the honest score is 8.

**Rotating deep-dive.** Each run, five dimensions get the full adversarial
treatment (source-level reading, edge-case hunting, doc-vs-code
cross-checking); choose the five by rotation so all 25 are covered every five
runs, and record which five in the entry header. The remaining twenty are
re-scored only if the diff since the last run plausibly touches them;
otherwise carry the prior score with the note "carried (vN)" — an honest
"not re-examined" beats a re-asserted 9.

**Notes must cite evidence, not narrative.** Every score's note names a file,
test, run output, or finding. A score may only *increase* over the prior run
if the note cites a verifiable new artifact. Scores are never targets: if the
diff contains work whose stated purpose is moving a score (commit messages
referencing ratings), flag it in the entry rather than rewarding it.

**Unknown-unknowns reserve & carried-score decay (added 2026-07-06, Run 37).**
Known knowns / known unknowns / **unknown unknowns** — the ledger (26+ entries of
"scored 8–9 while a real bug existed") is empirical proof that this loop's scores
*systematically over-predict*: our "known-good" keeps harbouring defects we did not
know we did not know. So:
- **Every score carries an implicit unknown-unknown reserve.** An 8 means "best
  current estimate — no *known* defect, recently checked," **not** "solid, trust it."
  The gap between any score and 10 is not only room-to-improve; a real part of it is
  things we don't know we don't know. This is *why* 10 is unreachable from inside the
  loop and 9 needs fresh evidence — and it should temper the 8s too.
- **A "carried (vN)" score is a *decaying* claim, not a held one.** It asserts
  solidity on evidence nobody re-touched — the most over-confident kind of score.
  Mark every carried dimension **stale/unverified this run**; a confident 8 must be
  *re-earned* by fresh probing-that-found-nothing, not inherited. A dimension carried
  several runs without fresh evidence, **especially one with repeated ledger entries**,
  should be read as "possibly optimistic," and its note must say so.
- **The cure is more probing, not reflexive markdown.** Do not mark everything down to
  look humble — that is pessimism cosplaying as rigor. Score the honest best estimate,
  label its staleness, and let the falsification quota + rotating deep-dive go *earn*
  the confidence by looking where we haven't.
- **Never retroactively rewrite a past run's scores.** Each entry is a dated measurement
  under the rules in force *then*; a rule change (like the current-state principle above)
  applies **forward only**. A time-series is only meaningful if past values stand — and the
  calibration ledger already records when an old score was later shown miscalibrated, which
  is the correct, non-destructive way to carry a correction. (Learned 2026-07-06: retro-
  correcting one older run made the series *less* consistent than leaving all snapshots intact.)

### Identity

**1. Philosophy / Coherence with Goal**
Anchor: `docs/philosophy.html`. Does the implemented feature set serve the stated
purpose without drift? Is anything present that contradicts or dilutes what the
philosophy document says the system is? Check README and ROADMAP for any claims
that conflict with the philosophy.

**2. Conceptual Integrity**
Does the implementation feel like one mind designed it — consistent idiom, naming,
abstraction level, and decision-making throughout? Check naming conventions across
`src/`, the sub-handle API surface, example code, and guide chapters for divergence
from the patterns the philosophy establishes.

### Structure

**3. Architecture**
Are the three layers (KV / Signal / Consensus) correctly separated and respected?
Is the dependency graph acyclic? Read the namespace table in `src/lib.rs`, the
layer description in `CLAUDE.md`, and check whether higher-layer code writes
through the documented key prefixes rather than bypassing them.

**4. Modularity**
Can the eight sub-handles (`KvHandle`, `MeshHandle`, `CapabilitiesHandle`,
`ConsensusHandle`, `ServiceHandle`, `SchemaHandle`, `LlmHandle`, `McpHandle`)
be understood and reasoned about independently? Check `src/agent/` for hidden
coupling across handle boundaries, shared mutable state, and dependency direction.

### Interface

**5. API Design**
Read the public re-exports in `src/lib.rs` and each `*_handle.rs` file. Is the
surface minimal and hard to misuse? Are names consistent? Are there footguns,
over-exposed internals, or methods where the correct call sequence is non-obvious?

**6. Error Handling Model**
Check error types in `src/lib.rs`, propagation across all six handles, and the
example code error paths. Are errors typed consistently? Can callers distinguish
recoverable from unrecoverable? Is propagation via `?` coherent or mixed with
`unwrap()`?

**7. Configurability**
Check `src/config.rs`, feature flags in `Cargo.toml`, and environment variable
usage in `examples/` and `Makefile`. Is the configuration surface well-designed —
neither over-constrained nor arbitrarily large? Are operational knobs distinct from
code-change concerns?

### Implementation

**8. Language Best Practices**
Is Rust used idiomatically? Check for `unwrap()` / `expect()` outside test code,
`unsafe` blocks, unnecessary clones, lifetime anti-patterns, and missed
opportunities to use the type system for correctness. Check a cross-section of
`src/agent/` files.

**9. Concurrency Correctness**
Check `Arc`/`Mutex`/`RwLock` usage in `src/store.rs` and `src/connection.rs`,
`AtomicBool` patterns in `src/agent/capability_ops.rs` and `src/agent/demand.rs`,
channel usage, task spawning in `src/agent/tasks.rs`. Are shared-state boundaries
explicit? Are there potential deadlocks or race conditions?

**10. Resource Management**
Check handle drop semantics (capability advertisement TTL, lock guards in
`src/agent/consensus_handle.rs`), connection cleanup in `src/connection.rs`,
spawned task cancellation in `src/agent/tasks.rs`. Are lifecycles explicit and
correct?

### Correctness

**11. Semantic Correctness**
Does the implementation correctly solve the formal problems it claims?
- LWW convergence: `src/store.rs` merge logic
- HLC causality: `src/hlc.rs` tick/observe contract
- Consensus linearisability: `src/consensus.rs` quorum accounting
- Anti-entropy progress: reconnect logic in `src/connection.rs`
Look for off-by-one errors in quorum calculations, edge cases that break
convergence, and places where the formal guarantee and the code diverge.

### Resilience

**12. Robustness**
Check connection error paths, malformed wire frame handling in `src/framing.rs`,
TTL edge cases, and behaviour when a peer disappears mid-operation. Does the
system degrade gracefully or hard-fail on unexpected input?

**13. Security**
Check `src/tls.rs` (mTLS), Ed25519 signing in `src/consensus.rs`, input
validation in `src/framing.rs` and `src/agent/http.rs`. Assess authentication,
authorisation, secrets management, and whether the `tls` feature is usable
without expert knowledge.

**14. Failure Mode Legibility**
When things go wrong, does the system fail obviously and point to the cause?
Check error message quality throughout, log output in key failure paths, and
whether panics include actionable context. Compare against the opacity/load
mechanism — does it communicate *why* a node is unavailable?

### Performance

**15. Performance**
Check `src/store.rs` (LWW merge on hot path), `src/framing.rs` (serialisation),
`src/signal.rs` (fan-out loop), gossip broadcast. Are there unnecessary
allocations, copies, or blocking calls on async paths?

**16. Scalability**
How does behaviour change as node count or data volume grows? Check
`scan_prefix` complexity, capability resolution ranking algorithm,
anti-entropy round complexity, gossip fan-out strategy. Does the system have
a known cliff edge?

**Scale evidence (the standing gap this dimension has carried at 7 for many runs).** Code-reading
alone caps this at 8, and there is no scale execution in a normal run — which is *why* it stays a
carried 7. Two evidence sources close that, in preference order:

1. **The local nightly scale results** — if `~/mycelium-scale-results/results.csv` exists (the
   `scale-nightly-local.sh` launchd runner, macOS/Apple-Silicon — `scripts/launchd/`), **read it** and
   cite the latest rows. Format: `timestamp,suite,exit_code,result,note,log` for `scale` (100-node),
   `resilience` (~22-node crash/rejoin), `entries` (~30-node entry-volume). A recent green nightly is
   real 100-node-cluster evidence → an **evidenced 8** (cite the date + per-suite result, e.g. "2026-
   07-14 nightly: resilience PASS 11/11, entries integrity PASS"). This is *prior* evidence, not
   produced-this-run, so it supports 8, **not** 9. **Read the `scale` row honestly:** a `scale` FAIL
   is often the documented Docker-bridge *formation-variance ceiling*, not a regression
   (`docs/wiki/dev/testing/scale-tests.md`) — `resilience`/`entries` are the reliable pass/fail signal;
   weight those. **But classify every FAIL by opening its log — don't trust the result column alone:**
   a suite that *couldn't start* (`Cannot connect to the Docker daemon`, an image-build error, or
   VM/conntrack exhaustion after a preceding 100-node round — exit code 2 / `make: *** Error`) is an
   **environmental/runner** failure, **not** a substrate finding (the local runner restarts Colima
   between rounds to avoid exactly this). Only a suite that *ran and failed an assertion* (the runner
   reported a convergence/integrity/succession check failing) is a real finding → cap 6.
2. **Produced-this-run (for a 9-eligible number):** run the in-process SWIM oracle *this run* —
   `SWIM_ORACLE_N=100 cargo test --lib swim_scale_oracle -- --ignored --nocapture` — and cite it. It
   exercises the membership/failure-detection protocol at N=100 in-process (no Docker), so it is a
   *narrower* slice than the Docker nightly — a 9 on the oracle alone should note that caveat.

If neither is available (no nightly file, oracle not run), carry the prior score and mark it
**stale/unverified** — do not assert an evidenced score without one of the above.

### Verification

**17. Testability**
Is the design deterministic, injectable, and free of hidden global state?
Can individual components be exercised without starting a full cluster?
Check test utilities in `src/lib_tests.rs` and how unit tests construct
agents.

**18. Test Architecture**
Check `src/lib_tests.rs` (unit), `tests/` (integration scenarios), `fuzz/`
(fuzz targets). Is there an appropriate pyramid? Are property-based or fuzz
tests used where inputs are adversarial? Is the integration suite fast enough
for CI? Are the 12 scenarios covering the right invariants?

### Operations

**19. Observability**
Check `src/agent/http.rs` for the `/metrics` endpoint and management dashboard.
Are metrics, logs, and traces built into hot paths or absent? Can an operator
understand what the cluster is doing in production without modifying it?

**20. Debuggability**
Check the KV dump endpoint, mesh dashboard, `/ready` endpoint, and any internal
state inspection tools. Can a developer reproduce and understand a specific
failure using available tooling alone?

**21. Operational Readiness**
Check `is_ready()` / `/ready`, `shutdown_with_timeout()`, the `sys/load/`
back-pressure mechanism, Docker Compose setup in `examples/`, environment
variable configuration, and persistence/restart behaviour documented in
`README.md`.

### Sustainability

**22. Evolvability**
Check `CHANGELOG.md`, the wire version policy at the top of `src/framing.rs`
(`WIRE_VERSION` / `PREV_WIRE_VERSION`), the `[Unreleased]` section, and
`ROADMAP.md` v2 milestones. Is there a coherent backwards-compatibility policy?
Is technical debt being paid down or accruing?

**23. Documentation**
Read `docs/guide/` chapters 01–12, `docs/philosophy.html`, `README.md`. Are
the guide's code examples consistent with the current API (sub-handle syntax)?
Are they runnable? Is the philosophy document current? Is there a clear path
from zero to productive for a newcomer?

**24. Developer Experience**
Check `rust-toolchain.toml`, `Makefile`, build output quality, and the
`CLAUDE.md` on-ramp. How long does a clean `cargo build --lib` take? Are error
messages and diagnostics helpful? Is the contribution path clear?

**25. Dependency Hygiene**
Check `Cargo.toml` for dependency count, optional vs required classification,
and whether each dep is well-chosen and actively maintained. Verify
`--no-default-features` compiles (`gateway`-free build). Check `Cargo.lock`
is present. Assess supply chain risk from the dep graph.

---

## Step 2b — Falsification quota (mandatory)

After provisional scores are written, take the **three highest-scoring
dimensions** and attempt one falsification probe against each. A probe is an
*executable* attempt to break a documented invariant of that dimension — not
more reading. Examples:

- Write a test asserting an invariant the docs claim (convergence, ordering,
  idempotence, drop semantics) and run it. Construct the adversarial input
  yourself: equal timestamps, forged frames, raced startup, reversed apply
  order.
- Feed a malformed/hostile input to a live agent (garbage bytes on the gossip
  port, oversized frame, unknown wire version) and assert the process survives
  and stays serviceable.
- Drive a lifecycle edge (start → use → shutdown) and assert the claimed
  cleanup actually happened (`task_count`, fd counts, store state).

Rules:
- **Current score = current state (the governing principle, sharpened 2026-07-06, Run 37).**
  A dimension's score reflects its state *at the end of this run*. **Discovering a latent
  bug adds a calibration-ledger entry — that is where accountability for the *past*
  over-scoring lives — and does NOT, by itself, lower the current score.** Finding *and*
  fixing a defect makes the dimension's current state *better* (defect removed, and if
  gated, a new regression added), so its score does not drop; scoring the fix-run *below*
  the runs where the bug was hidden is incoherent, and it perversely punishes the digging
  M2 exists to reward. "We found a bug so there are probably more" is **not** a valid
  reason to lower a current score — it would justify downgrading every un-audited dimension,
  and the just-hardened one is the *best*-scrutinised, not the worst.
- A finding lowers the **current** score only when it reveals a **currently-unfixed
  weakness**. Concretely: a **live/unfixed finding caps the dimension at 6**; a finding
  "fixed" only by widening a timeout or a still-flaky gate also caps at 6 (not really
  fixed); a finding **fixed + left with a deterministic (non-flaky) regression gate this
  run** scores its fixed end-state (back to solid — an 8, or a 9 only with fresh
  whole-dimension execution evidence); a **structural weakness that persists** (e.g. an
  architecture that keeps permitting flaky CI-gating tests) legitimately scores sub-8 for as
  long as it is unaddressed — that is a current-state fact, not a discovery-penalty.
- Every finding is still written up in the entry's **Findings** section with severity
  (Critical / Major / Minor), reproduction, and affected dimension, and gets its ledger line.
- A probe that *passes* is kept as a permanent regression test where practical
  — the quota should grow the suite, not produce throwaway code.
- A probe that finds a real bug should leave behind a canary: either the fix +
  test, or a test documenting the current (wrong) behaviour with a comment, so
  the suite flips when it is fixed.
- Probes must vary across runs — do not re-run last run's probes and call it
  an attempt.

---

## Step 3 — Persist results

Append to `docs/analysis/ratings.md` (create the file and `docs/analysis/`
directory if they do not exist). Determine the run number by counting existing
`## ` headings in the file and adding 1.

**Calibration ledger.** The file carries a `## Calibration Ledger` section
immediately after the preamble. Whenever a bug is found (by a probe, by later
work, or in production) in a dimension that scored **≥ 8 at the time the bug
already existed**, append a ledger line:
`- {date}: {dimension} scored {N} in Runs {range} while {bug, one line} existed (found by {what}).`
The ledger is the framework's own report card — it measures whether scores
predict reality. Review it before scoring: a dimension with repeated ledger
entries deserves structural skepticism, not just a lower number.

Use this exact format (note the M2 header, the deep-dive list, the Findings
section, and that the **floor — the three lowest dimensions — is the headline
number**, with the mean kept only as a series-continuity footnote):

```
## {YYYY-MM-DD} — Run {N} (M2)

Deep-dive dimensions this run: {five, by rotation}. Execution evidence: {suites/builds/probes actually run}.

### Findings
{One per falsification finding: severity, dimension, description, repro/test name. "None" if all probes passed.}

| # | Dimension | Score | Notes |
|---|-----------|:-----:|-------|
| 1 | Philosophy / Coherence with Goal | ? | one-line justification |
| 2 | Conceptual Integrity | ? | |
| 3 | Architecture | ? | |
| 4 | Modularity | ? | |
| 5 | API Design | ? | |
| 6 | Error Handling Model | ? | |
| 7 | Configurability | ? | |
| 8 | Language Best Practices | ? | |
| 9 | Concurrency Correctness | ? | |
| 10 | Resource Management | ? | |
| 11 | Semantic Correctness | ? | |
| 12 | Robustness | ? | |
| 13 | Security | ? | |
| 14 | Failure Mode Legibility | ? | |
| 15 | Performance | ? | |
| 16 | Scalability | ? | |
| 17 | Testability | ? | |
| 18 | Test Architecture | ? | |
| 19 | Observability | ? | |
| 20 | Debuggability | ? | |
| 21 | Operational Readiness | ? | |
| 22 | Evolvability | ? | |
| 23 | Documentation | ? | |
| 24 | Developer Experience | ? | |
| 25 | Dependency Hygiene | ? | |
| — | **Floor (lowest 3)** | **?, ?, ?** | {the three dimension names} |
| — | Mean (continuity footnote) | ?.? | not a target; see M2 preamble |
```

Carried scores are noted as "carried (v{run})" in the notes column.

---

## Step 4 — Display to user

Lead with the **floor and the findings**, not the mean:

```
Mycelium Analysis — {YYYY-MM-DD} (Run {N}, M2)
Floor: {dim} {n}/10 · {dim} {n}/10 · {dim} {n}/10
Findings this run: {count} ({severities})  ·  Calibration ledger: {total} entries
```

Then print the full table with actual scores filled in:

```
Mycelium Analysis — {YYYY-MM-DD} (Run {N})
══════════════════════════════════════════════════════════════════════════
 #   Dimension                        Score   Notes
──────────────────────────────────────────────────────────────────────────
 1   Philosophy / Coherence            ?/10   ...
 2   Conceptual Integrity              ?/10   ...
 3   Architecture                      ?/10   ...
 4   Modularity                        ?/10   ...
 5   API Design                        ?/10   ...
 6   Error Handling Model              ?/10   ...
 7   Configurability                   ?/10   ...
 8   Language Best Practices           ?/10   ...
 9   Concurrency Correctness           ?/10   ...
10   Resource Management               ?/10   ...
11   Semantic Correctness              ?/10   ...
12   Robustness                        ?/10   ...
13   Security                          ?/10   ...
14   Failure Mode Legibility           ?/10   ...
15   Performance                       ?/10   ...
16   Scalability                       ?/10   ...
17   Testability                       ?/10   ...
18   Test Architecture                 ?/10   ...
19   Observability                     ?/10   ...
20   Debuggability                     ?/10   ...
21   Operational Readiness             ?/10   ...
22   Evolvability                      ?/10   ...
23   Documentation                     ?/10   ...
24   Developer Experience              ?/10   ...
25   Dependency Hygiene                ?/10   ...
──────────────────────────────────────────────────────────────────────────
     Floor (lowest 3)                  {dims + scores}
     Mean (continuity footnote)        ?.?/10
```

Then list the three lowest-scoring dimensions as the top improvement targets,
summarise the falsification probes (what was attempted, what was found), and
quote any new calibration-ledger entries.

---

## Scoring guidance

- **1–3**: Significant problems that would block a serious user or contributor.
- **4–6**: Functional but with notable gaps or rough edges. **6 is the cap for
  any dimension with a confirmed finding this run.**
- **7–8**: Solid; minor issues only. **8 is the cap without fresh execution
  evidence from this run.**
- **9**: Excellent, *and* backed by named execution evidence from this run.
- **10**: Externally validated (third-party deployment, outside audit,
  independent reproduction). Not achievable from inside this loop.

Be honest. Inflate nothing. The value of the score series is its accuracy
over time, not its flattery at any single point — and the calibration ledger
now keeps score on the scores. If you cannot evaluate a dimension from
available files, say so and score conservatively; "carried" is always more
honest than a re-asserted number.

$ARGUMENTS
