# Mycelium — Analysis Ratings

Project evaluation across 25 orthogonal dimensions, tracked over time.
Each run is appended by `/mycelium-analysis`. Higher is better; 10 = no
meaningful improvement possible at the current stage of the project.

**Methodology note:** Runs 1–15 used methodology v1 (read-and-rate). From
Run 16, methodology v2 (M2) applies: execution-evidence gate (9 requires
named execution evidence from the run; 10 requires external validation),
mandatory falsification probes against the top-3 dimensions, rotating
five-dimension deep-dives, blind scoring, a cadence gate, and this
calibration ledger. Do not compare absolute scores across the v1/v2 boundary.

**Current-state principle — bright line at Run 37 (adopted 2026-07-06).** From Run 37, a dimension's
score reflects its *current* state: a bug **found + fixed + deterministically gated in the same run**
scores its fixed end-state (not the old "cap a confirmed finding at 6" — that cap now applies only to a
defect *still live* at run's end), and discovering a latent bug adds a ledger entry (where accountability
for past over-scoring lives) rather than lowering the current score. Also from Run 37: every score
carries an *unknown-unknowns reserve*, and a `carried (vN)` score is a *decaying, unverified* claim.
**These apply forward only.** Runs ≤ 36 are **dated snapshots under the rules in force then** (incl. the
old cap-at-6 for a fixed-same-run finding — see Runs 20, 22, 32, 34) and are **not** retroactively
rewritten: a time-series is only meaningful if past measurements stand. The ledger already carries every
such finding.

## Calibration Ledger

Records bugs later found in dimensions that scored ≥ 8 while the bug already
existed. This is the framework's own report card.

- 2026-07-10 (Run 41): **API Design** scored 8 in Runs 39–40 while the put-vs-take discovery
  asymmetry existed (default-config `put` fails `NoProvider` instantly during capability
  discovery; #154 fixed only `take`/`complete`) — found by the Run-41 deep-dive after the
  succession test hit it in practice.
- 2026-07-10: **Semantic Correctness / Robustness** scored 8 through Run 40 while
  (a) the tuple-space promotion watch treated never-saw-a-primary as evaporated —
  permanent split-brain on slow hosts (found by the hosted cluster-suites gate, #158),
  and (b) a late-joining secondary never backfilled pre-join items, silently losing
  the backlog on the next failover (found by the succession-chain probe prompted by
  a direct user question — "but we don't test this?"; fixed with join-time backfill).
- 2026-06-10: **Concurrency Correctness** scored 8–9 in Runs 9–15 while the
  consensus listener registration race existed (handlers registered on the
  task's first poll, so proposals racing listener startup were silently
  dropped — node fails to vote). Found by an adversarial test during the
  lease/tripwire work; fixed same day (synchronous registration in
  `start_consensus_listener`).
- 2026-06-10: **Documentation** scored 8–9 in Runs 10–15 while CLAUDE.md
  conflated the wire hop-count TTL with key evaporation ("every key has a
  TTL"); evaporation is a read-side freshness convention
  (`CapEntry::is_fresh`), and the store never time-evicts live keys. Found by
  doc-vs-code cross-check during a philosophy audit; docs corrected same day.
- 2026-06-10: **Semantic Correctness** scored 8–9 in Runs 11–15 while LWW
  diverged permanently on equal-timestamp concurrent data writes (first
  arrival won; anti-entropy could not detect it because the digest hashes
  key+timestamp only). Found by M2 falsification probe in Run 16; fixed same
  day (`lww_wins` deterministic data-vs-data tiebreak).
- 2026-06-11: **Concurrency Correctness** scored 8–9 in Runs 9–16 while the
  prefix-index maintenance race existed (index updated *after* the store CAS
  in `apply_and_notify`, unserialised; a tombstone/insert race on one key
  leaves a live store entry permanently invisible to `scan_prefix` and the
  `cap_ns_index`; anti-entropy cannot repair it because re-applying the same
  (key, ts) loses LWW and never touches the index; introduced 2026-05-18,
  commit cd3368c). Found by M2 deep-dive probe in Run 18 — 86 of 100 000
  racing rounds lost the key on first execution.
- 2026-06-11: **Semantic Correctness** scored 8–9 in Runs 9–15 and 17–18
  while `Hlc::observe` accepted unbounded remote clock drift (one skewed
  peer drags the whole cluster's HLC forward irrecoverably; read-side
  evaporation — the substrate's failure detector, including tuple-space
  secondary promotion — is silently suspended for the full drift duration
  because `now.saturating_sub(written)` reads 0 until the wall clock catches
  up; the cited Kulkarni et al. 2014 algorithm mandates a drift bound).
  Found by M2 Run-19 deep-dive doc-vs-algorithm cross-check.
- 2026-06-11: **Dependency Hygiene** scored 9 in Runs 17–18 while two
  published RUSTSEC advisories applied to the lockfile (`bytes` 1.10.1,
  RUSTSEC-2026-0007 integer overflow in `BytesMut::reserve` — called by
  `read_frame` on the wire path; `tracing-subscriber` 0.3.19,
  RUSTSEC-2025-0055 log poisoning). Found by M2 Run-19 `cargo audit` probe;
  fixed same run via semver-compatible lock bumps.
- 2026-06-11: **Robustness** scored 8 in Runs 16–19 while `bincode_cfg()`
  set no decode byte-limit: a frame whose internal length prefix claims a
  huge element count makes bincode attempt an unbounded `Vec::with_capacity`
  and the process OOM-aborts (SIGABRT). One malformed frame from any peer —
  or a bit-flip on a non-TLS link — kills the node; the 10 MiB `read_frame`
  cap bounds the frame, not the element counts inside it. Found by the M2
  Run-20 decoder mini-fuzz; fixed same run with `.with_limit::<MAX_FRAME_BYTES>()`.
- 2026-07-13 (Run 44): **Concurrency Correctness / Semantic Correctness** scored 8 in Runs 40–43
  while the `mycelium-wiki` curator's whole-page `write_page` could **lose a concurrent edit** during a
  transient dual-curator window: the read-modify-write rewrote the *entire* page from the curator's
  in-memory snapshot, so two curators editing *different sections of the same page* clobbered each
  other, and the lost proposal was already tombstoned → unrecoverable. Per-object atomic writes stopped
  torn reads but not this lost update in the R-M-W. Found by a design-review question about whether the
  companions should adopt the new lock-manager (which surfaced the wiki's coordination as the one real
  exposure); fixed with **section-granular compare-and-swap** (immutable versioned objects published by
  atomic `hard_link`, `WikiError::Conflict` → re-reconcile) + a multithreaded stress gate + a
  concurrent-create gate. The store is now at-least-once/never-lose; exactly-once *effect* via the
  idempotent reconcile. (Companion-crate defect — these two dimensions are project-wide since the
  2026-07-07 lock-table scope widening.)
- 2026-06-11: **Test Architecture** scored 8–9 in Runs 1–19 while the two
  fuzz targets were counted in the pyramid but **never executed** in any run
  (built at most). The decoder DoS above is exactly what they exist to catch;
  it sat uncaught for the life of the series. Found by promoting an in-suite
  mini-fuzz in Run 20.
- 2026-07-09: **Concurrency Correctness** scored 8 across recent runs (≈32–39) while the
  gateway consumer-group endpoint's `subscribe_log_group` "distributed lock" was a bare LWW
  gossip-KV write with **no cross-node mutual exclusion** — every consumer "held" the claim and
  drained the whole stream (100% double-delivery). Found by the #147 CI-gating attempt → fixed #151.
- 2026-07-09: **Semantic Correctness** scored 8 across recent runs (≈32–39) while that same
  endpoint **violated its exact-once delivery contract** (double-delivery across nodes; overlay
  S11 "got 10" for 5 tasks). Found by gating the overlay suite (#149) → fixed #151.
- 2026-07-09: **Test Architecture** scored 7–8 in Runs 1–39 while the correctness **Docker suites
  ran manual-only** (overlay S11–S13, integration `make test`), never gating CI — which is *why*
  the exact-once bug above went undetected. Gating them (#147) surfaced both it and the S13 ~50%
  flake (#150, still unfixed on `main`).
- 2026-06-11: **Resource Management** scored 8 in Runs 16–20 while tombstone
  GC never fired — the GC predicate compared packed-HLC entry timestamps
  against a wall-clock-ms cutoff, unsatisfiable since the v9 HLC migration
  (3c4de6e, 2026-05-20). Found by the Run-21 falsification probe; fixed same
  run (`store::sweep_stale_tombstones` + regression test).
- 2026-06-11: **Semantic Correctness** scored 8 in Runs 16–18 and 20 while
  the same tombstone-GC defect made the documented GC semantics ("only
  tombstones are GC'd") false in effect. Found by the Run-21 probe.
- 2026-06-11: **Developer Experience** scored 8 in Runs 16–20 while the
  TypeScript SDK's `shardFor()` referenced `this._base` (runtime crash on
  every call) plus 7 further tsc errors, introduced 2026-05-25 (c6cc3ce);
  no tsc gate in CI. Found by a user-requested type check.
- 2026-06-11: **Documentation** scored 8 in Runs 16–20 while ROADMAP.md
  linked three example files deleted 2026-05-25 (95c92af). Found by the
  Run-21 link-integrity probe; fixed same run.
- 2026-06-12: **Architecture** scored 9 in Runs 19–22 and **Semantic
  Correctness** 8 in Runs 20/22 while Individual-scoped signals (RPC
  requests/responses, consensus votes) were silently dropped whenever the
  target was not in the sender's outbound peer list — partial meshes broke
  RPC and ballot voting with nothing logged, contradicting the documented
  unconditional-forwarding model. Found by the three-arm experiment
  bring-up (synchronized take-volley stall), not by a ratings probe; fixed
  same day (flood fallback + relay regression test).
- 2026-06-12: **Architecture** scored 9 and **Semantic Correctness** 8 (Run
  22) while fan-out activation was polled-only: inbound-only nodes (seeds,
  tuple primaries) were mute for live sends — including RPC responses and
  votes — for up to 2× health_check_interval after a peer connected. Found
  by the random-topology property test written as Run-22 follow-up #3; fixed
  same day (event-driven peer-list publication on insert). Second
  topology-dimension bug in two days: introspective tests never varied
  topology, and anti-entropy healing masked the symptom for KV.
- 2026-06-14: **Security** scored ≥ 8 across many prior runs while the `tls`
  transport **could not complete a single handshake** — no rustls
  `CryptoProvider` was ever installed (crate built `rustls` with
  `default-features = false, features = ["ring"]`, so the aws-lc-rs auto-install
  never fired), guaranteeing a panic on the first TLS accept/connect. The `tls`
  feature had therefore never actually run in-process; prior runs scored it on
  read + the Ed25519 sign/verify unit tests, which never exercised the rustls
  path. Found by the WS1 cross-node integration test (first code to stand up the
  live TLS transport); fixed same day (idempotent `ring` provider install in
  `build_rustls_configs`). Operational Readiness/Robustness share the miss.
- 2026-06-16: **Concurrency Correctness** scored 8 in Runs 23–25 while
  `helpers::merge_peer_keys` (WS5 retained verifying-key set) was a non-atomic
  get-clone-modify-insert, not a papaya `compute` — the recurring "lock-free op
  + unserialised derived effect" family. Two rotations for the same node merging
  concurrently each read the same base set and the later `insert` clobbered the
  earlier, **silently dropping a still-needed historical verifying key** (audit
  chains / committed values / role claims signed by the lost key become
  unverifiable). Flagged as a *watch-item* (not a capped finding) in Runs 23/24/25
  — i.e. the series saw it three times and scored 8 anyway. Confirmed a real
  Major defect this session by the `concurrent_merges_for_one_node_never_drop_a_key`
  probe (lost **894 of 1024** keys against the old impl); fixed same session
  (atomic `compute` closure) and the probe kept as a regression gate. Lesson: a
  "technically not retry-safe, but eventually-consistent" watch-item is still a
  data-loss bug until proven otherwise — execute the probe, don't carry the note.
- 2026-06-21: **Test Architecture** scored 8 in Runs 24–26 while
  `test_wsc_m8_auto_config_cluster_converges` (and the multi-node unit-test family
  generally) was non-deterministic under **parallel** execution — `start()`
  intermittently errors on transient bind/resource contention when many multi-node
  tests spin up agents at once (fails ~1 in 3 full-suite runs; passes **5/5 in
  isolation**). A product-correct feature (M8 auto-config) with a flaky harness:
  the default `cargo test --lib` is not deterministically green. Found by Run 27
  evidence-gathering; canary comment left on the test. The session's own additions
  (M7/M10 multi-node tests) plausibly raised the parallelism that surfaced it.
  Lesson: "all CI runs green" hid a 1-in-3 local flake — run the suite *twice* before
  trusting a clean result.
- 2026-07-02: **Concurrency Correctness** scored 9 in Run 27 (and 8–9 since Run 13)
  while `AgentStateMachine::transition` was check-then-act for its policy guards —
  budget checks read counters that are incremented only *after* commit, and the
  commit never re-reads `current` after the (up to 30 s) approval `await` — so two
  approval-gated `Invoking` transitions racing through the await both pass a
  `tool_budget = 1` check and both commit (probe admitted **2 of 1**), and a
  timeout-`Failed` state committed during the await is silently overwritten.
  `force_failed_transition` guards the same race in the other direction, so the
  family was known. Existed since `state_machine.rs` was introduced (2026-05-21,
  b0728e1). Found by the Run-28 deep-dive probe; fixed same day — `try_commit`
  validate-and-swap under the state lock with budget check + reserve as one atomic
  step, retried when the state moved during the approval await; the probe flipped to
  the regression gate `tool_budget_enforced_under_concurrent_approval_gated_transitions`.
- 2026-07-02: **Error Handling Model** and **Robustness** scored 8 through Run 27
  while `kv().set()` accepted values whose encoded frame exceeds `MAX_FRAME_BYTES`
  (10 MiB): the value applies locally and is WAL-appended, `set` returns `true`, and
  no error ever surfaces — but the per-peer writer's `write_frame` fails, which is
  treated as a *connection* failure (healthy TCP link torn down, queued frames
  dropped, backoff entered), and anti-entropy cannot repair the divergence because
  `StateResponse` is a single unchunked frame: an oversized response is skipped with
  a `warn!` and never retried ("StateRequest is only sent on first contact"). The
  same ceiling means a store whose divergent/full-dump set exceeds ~10 MiB can never
  bootstrap a late joiner (Scalability). Existed since the framing layer was written.
  Found by the Run-28 probe; fixed same day — (1) `MAX_KV_WRITE_BYTES` guard in
  `kv_set`/`kv_set_async` rejects an un-frameable write outright (`false`, nothing
  applied, `warn!`), (2) the per-peer writer drops a `FrameTooLarge` frame without
  tearing down the connection, (3) `StateResponse` is chunked (per-chunk byte budget;
  an individually un-frameable legacy entry is skipped by name and the rest of the
  sync proceeds). Probe flipped to
  `test_oversized_value_is_rejected_outright_and_cluster_stays_healthy`, plus the new
  gate `test_late_joiner_converges_past_frame_sized_store_via_chunked_anti_entropy`
  (12 MiB store + poison entry → late joiner converges).
- 2026-07-02: **Dependency Hygiene** scored 8 in Run 27 (no `cargo audit` that run)
  while `wasmtime-wasi` 45.0.2 carried the flaw published 3 days later as
  RUSTSEC-2026-0188 (WASI hard links/renames bypass `FilePerms`, CVSS 6.5 — the
  wasm-host sandbox surface). Found by the Run-28 `cargo audit` probe; fixed same day —
  `wasmtime-wasi` → 45.0.3 (and `anyhow` → 1.0.103 for the RUSTSEC-2026-0190 unsound
  warning); `cargo audit` re-run clean (0 vulnerabilities).
- 2026-07-02: **Test Architecture** scored 8 in Run 29 (evidence: "`cargo test --lib` 291/0
  run twice, deterministic") while `test_manage_opacity_gate_vetoes_then_library_overrides`
  flaked under the **full-feature** CI `Test` job (`tls,metrics,a2a,llm`) — a scheduler-
  starvation timing flake (the opacity governor's 100 ms ticker starves under ~314 parallel
  tests, so the guaranteed `fill==1.0` BOUNDARY_OPAQUE emission missed the test's 3 s poll).
  The Run-29 "deterministic" evidence was scoped to *default* features and never ran the
  matrix that flaked. Found by CI red on the docs-only Phase-0 commit (155f8b3); fixed by
  widening the structural poll 3 s → 10 s (the emission is guaranteed, only its latency under
  load wasn't). Lesson (again): "green twice" is only as broad as the feature set you ran —
  Run 29's determinism claim should have named its scope.
- 2026-07-03: **Test Architecture** — `test_individual_consumers_over_random_partial_meshes`
  (the random-topology individual-scope flood-fallback property test, added 7069d4c) flaked
  once on the CI `Test` job during Legible-Emergence Phase-3 increment 3 (commit 9ea4e35,
  which is orthogonal — it never enables the detectors). The topology is deterministic (fixed
  seeds 11/23/47) but multi-hop flood-fallback *delivery* is timing-bound, and the 8×1 s
  re-emit window occasionally starves under full CI load. Same family as the opacity-gate flake
  above: a guaranteed structural outcome whose latency-under-load exceeded a too-tight window.
  Fixed by widening the re-emit loop 8 → 20 attempts (still exits the instant delivery is
  observed — a structural condition, not a fixed sleep). Confirmed flaky, not a regression, by a
  clean job re-run with no code change.
- 2026-07-03: **Test Architecture** — `test_manage_opacity_gate_vetoes_then_library_overrides`
  flaked *again* on the CI `Test` job during Legible-Emergence Phase 5 (commit 4991d6f, orthogonal
  — Phase 5 only adds public wrapper methods + re-exports + a coop demo; suite was 345/0 locally).
  This is the **second** flake of this exact test: the guaranteed `fill==1.0` BOUNDARY_OPAQUE
  emission's schedule-latency exceeded the 3 s bound (2026-07-02, widened to 10 s), then the 10 s
  bound (2026-07-03, ~48 s saturated Test run). Widened 10 → 30 s. The recurrence is the lesson: a
  10 s ceiling was still inside the tail of CI scheduling latency under ~345 parallel full-feature
  tests; because the emission is *guaranteed* (only its schedule varies), the correct fix is a
  ceiling that comfortably clears the saturated-runner tail, not a structural rework.
- 2026-07-03: **Test Architecture — structural resolution** of the two flakes above (Run 30 scored
  this dimension **7**, marking the recurrence as a real weakness rather than hiding it under an 8).
  The opacity-gate flake is addressed at the root: the veto / library-override / hysteresis-clear
  *decision* is extracted from the async tick loop into pure functions (`opacity_state_for` /
  `opacity_transition`, `src/agent/opacity.rs`), and the invariant now lives on a **deterministic**
  gate (`opacity_gate_vetoes_below_full_then_the_library_overrides_at_full` +
  `opacity_clears_only_after_fill_falls_a_full_hysteresis_below_threshold` — no async, no ticker, no
  timeout). The integration test is now an explicit *wiring smoke* whose 30 s ceiling can no longer
  threaten the invariant. Lesson reversed: once a "widen the ceiling" flake **recurs**, the honest
  fix *is* the structural rework — separate the decision (pure, testable) from the scheduling
  (async, best-effort). Refactor is behavior-preserving (the integration tests still pass unchanged).
- 2026-07-05: **Concurrency Correctness** scored **8** in Runs 32–33 while the `mycelium-wiki`
  curator election could **split-brain**: the settle window in `run_election` is a fixed sleep
  (`(cap_refresh*2).max(2s)`), so a candidate ad lost to gossip latency lets two `Auto` nodes each
  see only themselves and both call `become_curator()` — which had **no step-down**, leaving two
  permanent writers of record against the shared store with no recovery (existed since the companion's
  election shipped, `65b31c2`, in the Run-32 window). **Both runs cited `failover.rs` ("election,
  ring-failover, single-writer apply — green") as the evidence the companion's concurrency was sound**
  — but that test's XOR "exactly one curator" gate (`poll_until`, 30 s) could pass by luck. Surfaced
  as an intermittent CI failure of `curator_elects_…` on PR #126; root-caused and fixed this session
  (`9e42453` — a curator **sentinel** applying lowest-id-wins continuously; higher-id curator resigns,
  stopping just its `curator_tasks` loops) with the deterministic canary
  `dual_curators_reconcile_to_a_single_writer` (verified: **fails at 30 s without the sentinel**,
  passes with). Lesson (third of its kind — cf. the WS5 `merge_peer_keys` watch-item and the Run-27
  "all CI green hid a flake"): **a green *non-deterministic* gate is not evidence** — the flaky XOR
  poll masked a Major defect for two runs while the dimension was even deep-dived (Run 32).
- 2026-07-05: **Test Architecture** scored **8** in Runs 32–33 while the `elastic_intent` coop demo
  (03) carried a latent CI-load flake: its Phase-1 readiness gate waited for TCP peers but **not** for
  TLS identity exchange (signed anti-entropy frames dropped — "identity not yet received" — leaking
  that latency into the convergence window), and the Phase-3 self-heal window (45 s) was marginal
  against the ~12 s governor cooldown. Green in Runs 32–33 (never manifested), then **all three
  `ci_smoke` retries failed** on the `9e42453` push (run `28739078960`). Scored 7 once known (Runs
  34–35); root-caused + fixed 2026-07-05 (PR #128 — structural bidirectional-propagation gate +
  widened heal window), verified **14/14 local + CI green** (incl. the Co-op job). Same family as the
  curator entry above — a timing-sensitive gate that was green until CI saturation exposed it.
- 2026-07-06: **Robustness / Failure-Mode Legibility** carried a real **control-signal-shed liveness
  bug** while `test_manage_opacity_gate_vetoes_then_library_overrides` was dismissed as a *timing* flake
  for **Runs 27–36** (widened 3 s → 10 s → 30 s; "structurally resolved" Run 30 by extracting the
  *decision* to pure gates — which never touched the actual cause). Root cause: the opacity governor
  emits `BOUNDARY_OPAQUE`/`TRANSPARENT` at `System` scope, and `ops::deliver_locally` probabilistically
  sheds non-`Individual` signals by `combined_fill = max(handler_fill, gossip_shard_fill)`; under CI
  gossip-drain starvation `gossip_shard_fill > 0`, so the governor's **single** boundary-transition
  emission could be shed from *local* delivery — the "I'm now shedding" signal dropped by the shedding
  mechanism, exactly under load. A permanent miss (emitted once), which is why no timeout ever helped,
  and why a *local* subscriber to a boundary transition could miss it in production. Found 2026-07-06 by
  a deliberate root-cause dig (not a probe); fixed by exempting boundary-transition kinds from the local
  shed (`ops.rs`, like `Individual`), pinned by the **deterministic** gate
  `ops::delivery_shed_tests::boundary_transition_signals_are_never_locally_shed` (fails w.p.≈1 at
  `combined_fill = 1.0` without the fix). Lesson: **a recurring "flaky test" is a defect until root-caused
  — three "resolutions" treated latency; the bug was a dropped signal a different layer down.**

**Dimensions:** Philosophy/Coherence · Conceptual Integrity · Architecture ·
Modularity · API Design · Error Handling · Configurability · Language Best
Practices · Concurrency Correctness · Resource Management · Semantic Correctness ·
Robustness · Security · Failure Mode Legibility · Performance · Scalability ·
Testability · Test Architecture · Observability · Debuggability · Operational
Readiness · Evolvability · Documentation · Developer Experience · Dependency Hygiene

---
- 2026-07-07: Test Architecture scored 8 in Runs 32–36 while the `mycelium-wiki` integration tests carried the `free_port()` bind-TOCTOU flake class (bare-unwrap re-bind, CI-gating) (found by a CI `AddrInUse` failure on an unrelated push; class retired with pair-granularity retries same day).
- 2026-07-07: Documentation scored 8 in Runs 33–37 while `docs/wiki/dev/examples.md` counted eleven coop demos though the smoke had run twelve since 2026-07-03 (found by wiki-lint 8 — the first lint that *counted*).

## 2026-06-04 — Run 1

| # | Dimension | Score | Notes |
|---|-----------|:-----:|-------|
| 1 | Philosophy / Coherence with Goal | 9 | Feature set maps tightly to Holland/Jini/OSGi/Paremus synthesis; library-not-platform honored; consensus overlay correctly positioned as higher-order |
| 2 | Conceptual Integrity | 7 | Core idioms consistent but README mixes old flat-API (23 calls) with new sub-handle API (38); `signal_wired_via().await` in README is a bug in a code example |
| 3 | Architecture | 8 | Three-layer model respected; namespace ownership table explicit; gateway feature gate clean; Layer I/II entanglement documented as known constraint |
| 4 | Modularity | 7 | Sub-handle facade is real API-level separation but all six handles share one `Arc<TaskCtx>` — isolation is external, not internal; 30+ impl files still tightly coupled |
| 5 | API Design | 8 | `#![forbid(unsafe_code)]`; sub-handle facade reduces surface; `CapabilityHandle` vs `CapabilitiesHandle` naming collision is a residual footgun |
| 6 | Error Handling Model | 6 | Eight distinct public error types with no documented relationship; 244 `.unwrap()` calls in production code (most are slice conversions, some are real); error propagation strategy undocumented |
| 7 | Configurability | 8 | 22+ documented `GossipConfig` fields; TOML + `GOSSIP_*` env vars; clean feature flag taxonomy; `writer_channel_depth` correctness threshold documented but not enforced at runtime |
| 8 | Language Best Practices | 8 | `#![forbid(unsafe_code)]`; idiomatic use of `thiserror`, `papaya`, `parking_lot`; one `std::sync::Mutex::lock().unwrap()` in capability_handle.rs is a mild inconsistency |
| 9 | Concurrency Correctness | 7 | Lock-free `papaya` hot paths; `AtomicU64/Bool` for counters; `grp_generation` cache invalidation is clean; two timing-sensitive tests flaky under load suggest real scheduling sensitivity |
| 10 | Resource Management | 7 | RAII for capabilities/locks/handles; GC task evicts orphaned watchers and quorum trackers (recent fix); TCP writer idle timeout; no documented bound on concurrently spawned tasks |
| 11 | Semantic Correctness | 7 | LWW merge correct; HLC Kulkarni 2014 with documented limits; `consistent_set` documented as "linearizable" but epidemic two-phase voting is closer to CASPaxos — formal gap between claim and protocol |
| 12 | Robustness | 7 | `MAX_FRAME_BYTES` bound; TTL decrement; reconnect backoff; `max_store_entries` OOM guard; fail-open signing-key verification creates a TOCTOU window during peer key exchange |
| 13 | Security | 6 | mTLS opt-in (off by default); HTTP gateway has no authentication; no gossip rate-limiting; fail-open on unverified signing keys; `signal_rx_from` sender auth is good but optional |
| 14 | Failure Mode Legibility | 7 | `dropped_frames` with actionable diagnostic guide; `gc_alive`/`health_monitor_alive` flags; `rpc_pending` mutex-poison recovery; consensus timeout returns vote counts; Nack reasons not surfaced |
| 15 | Performance | 8 | Benchmarks published; 16 ns get, 151 ns set; lock-free hot path; gossip sharding; zero-copy forward; `reqwest` non-optional adds binary weight for embedded targets |
| 16 | Scalability | 6 | System-scope gossip is O(n) fan-out; anti-entropy is O(n) rounds; `scan_prefix` O(|store|) fallback for unknown prefixes; no documented node-count ceiling tested beyond demo scale |
| 17 | Testability | 8 | `make_agent()` zero-port helper; `loopback_pair()` in-process TCP; `EchoBackend` for LLM; `alloc_port()` for live-node tests; `TaskCtx` wires through rather than injected |
| 18 | Test Architecture | 7 | 263 unit tests; 12 Docker integration scenarios; 2 fuzz targets; 3 overlay Python scenarios; no property-based tests for convergence invariants; test pyramid is reasonable |
| 19 | Observability | 7 | Prometheus endpoint (`--features metrics`, off by default); `system_stats()`; pre-built Grafana dashboard; no distributed tracing in core (OTEL only in skillrunner) |
| 20 | Debuggability | 7 | KV dump endpoint; `/stats`; `/ready`; management dashboard; `peer_drop_counts()`; consensus ballot state not directly inspectable via API |
| 21 | Operational Readiness | 8 | `is_ready()`/`/ready`; `shutdown_with_timeout()`; `sys/load/` back-pressure; Docker Compose; `GOSSIP_*` env vars; rolling upgrade window; no documented stop-the-world upgrade procedure |
| 22 | Evolvability | 8 | Wire version policy v2–v11 with one-version rolling window; CHANGELOG follows Keep a Changelog; forwarding stubs preserve backward compat; v2.0 milestones documented in ROADMAP |
| 23 | Documentation | 7 | Philosophy doc is excellent; guide chapters 01–12 use current API; README capability section uses old flat API; `signal_wired_via().await` bug in README code example; FIPA-ACL mapping is sophisticated |
| 24 | Developer Experience | 8 | `rust-toolchain.toml`; `CLAUDE.md` on-ramp; guide runnable-example column; diagnostic flow in README; no migration guide for flat-API → sub-handle transition |
| 25 | Dependency Hygiene | 7 | Well-chosen core deps; optional deps correctly feature-gated; `reqwest` required (not optional) pulls TLS into embedded builds; `tokio` `test-util` in `[dependencies]` not `[dev-dependencies]` |
| — | **Mean** | **7.3** | |

---

## 2026-06-04 — Run 2

| # | Dimension | Score | Notes |
|---|-----------|:-----:|-------|
| 1 | Philosophy / Coherence with Goal | 9 | Holland/Jini/OSGi/Paremus synthesis fully honored; "consistency as a service" framing intact; bearer-token auth is library-level, not platform-level — consistent with philosophy |
| 2 | Conceptual Integrity | 8 | `set_with_min_acks` rename removes naming–semantics gap; signal group vs capability group disambiguated in doc comments and guide; "no coordinator" claim now properly scoped in README and ROADMAP; sub-handle facade makes domain boundaries lexically explicit |
| 3 | Architecture | 8 | Layer I/II entanglement documented with v2 roadmap item; gateway feature gate clean; three-layer model and namespace ownership unchanged — no regressions |
| 4 | Modularity | 7 | Sub-handle facade provides API-level domain separation; `TaskCtx` is still a 30+ field God object shared by all six handles; true internal isolation requires v2 workspace split |
| 5 | API Design | 8 | `set_with_min_acks` improves semantics; bearer-token config as `Option<String>` is clean; `CapabilityHandle` (ad handle) vs `CapabilitiesHandle` (domain handle) naming ambiguity persists |
| 6 | Error Handling Model | 7 | `docs/guide/error-handling.md` documents all 8 public error types with recoverability classification and propagation strategy; `SchemaPublishResult::Conflict` advisory semantics documented; 181 production `.unwrap()` calls remain |
| 7 | Configurability | 8 | `gateway_auth_token: Option<String>` + `GOSSIP_GATEWAY_AUTH_TOKEN` env var added cleanly; scale test validated `GOSSIP_WRITER_CHANNEL_DEPTH` and `GOSSIP_PING_PEER_SAMPLE_SIZE` at 100 nodes |
| 8 | Language Best Practices | 8 | `rpc_pending` mutex poison recovery fix (no longer panics on poisoned lock); `#![forbid(unsafe_code)]` maintained; 181 production unwraps unchanged — most are slice/OnceLock conversions, a handful are real |
| 9 | Concurrency Correctness | 7 | Scenarios 04 and 07 flaky-test fixes (gossip timing robustness) are a positive signal; no formal deadlock proof; `AtomicBool` usage in opacity governor has no documented memory-ordering rationale |
| 10 | Resource Management | 7 | GC task evicts orphaned `quorum_trackers` and closed prefix watchers (prior fix persists); no new issues; spawned task bound still undocumented |
| 11 | Semantic Correctness | 7 | "No coordinator" overclaim now scoped correctly; `set_with_min_acks` name eliminates quorum–ACK ambiguity; `consistent_set` still described as "linearizable" while the epidemic two-phase protocol is closer to CASPaxos — formal gap persists |
| 12 | Robustness | 7 | Signal reorder buffer `warn!` on degraded flush (prior fix); no new robustness changes; fail-open on unverified Ed25519 keys during key exchange remains |
| 13 | Security | 7 | HTTP gateway bearer-token auth (`gateway_auth_token`) closes the main unauthenticated API surface; health/ready/stats/metrics intentionally public; mTLS still opt-in (trusted-domain default); no gossip rate-limiting |
| 14 | Failure Mode Legibility | 7 | No changes; `dropped_frames` diagnostic guide, `gc_alive`/`health_monitor_alive` flags, consensus vote counts on timeout remain; Nack reasons still not surfaced to callers |
| 15 | Performance | 8 | No changes; benchmarks published; lock-free hot path intact; `reqwest` still non-optional overhead for embedded targets |
| 16 | Scalability | 7 | 100-node Docker scale test passes reliably; practical ceiling (~200–400 nodes) documented; ROADMAP v2 milestone #4 specifies partial-mesh gossip fix with O(N·log N) target; O(N²) topology still present in current release |
| 17 | Testability | 8 | 265 tests (up from 263); `EchoBackend`, `loopback_pair`, `alloc_port` helpers unchanged; no structural changes |
| 18 | Test Architecture | 8 | 100-node Docker scale test (`make test-scale`) validates gossip convergence, KV propagation, and dropped-frame rate at production-adjacent scale; 265 unit + 12 integration + 2 fuzz + 3 overlay + 1 scale; still no property-based convergence tests |
| 19 | Observability | 7 | No changes; Prometheus endpoint (opt-in `metrics` feature); `system_stats()`; Grafana dashboard; OTEL only in skillrunner, not core |
| 20 | Debuggability | 7 | No changes; KV dump, `/stats`, `/ready`, management dashboard, `peer_drop_counts()` intact; consensus ballot state not inspectable via API |
| 21 | Operational Readiness | 8 | Gateway auth makes production HTTP deployment safe; `is_ready()`/`/ready`; `shutdown_with_timeout()`; Docker Compose; rolling upgrade window; no stop-the-world upgrade procedure documented |
| 22 | Evolvability | 8 | CHANGELOG updated with three new features; ROADMAP expanded with detailed partial-mesh gossip v2 milestone; wire version policy unchanged and correct |
| 23 | Documentation | 8 | `docs/guide/error-handling.md` closes the biggest documentation gap; "no coordinator" claim scoped correctly in two places; signal/capability group distinction documented; ROADMAP O(N²) engineering note is detailed and actionable; README capabilities section still uses old flat API (valid via forwarding stubs) |
| 24 | Developer Experience | 8 | No changes; `rust-toolchain.toml`, `CLAUDE.md`, scale test `make test-scale` target with `SCALE_WORKERS` override; CLAUDE.md scale test constraint documented for contributors |
| 25 | Dependency Hygiene | 7 | Gateway feature gate complete (Axum, tower-http, tokio-stream, futures-util all optional); `reqwest` still required (not optional) — adds TLS to all embedded builds; `tokio::test-util` still in `[dependencies]` not `[dev-dependencies]` |
| — | **Mean** | **7.6** | |

---

## 2026-06-04 — Run 3

| # | Dimension | Score | Notes |
|---|-----------|:-----:|-------|
| 1 | Philosophy / Coherence with Goal | 9 | Holland/Jini/OSGi/Paremus synthesis intact; library-not-platform honored; consensus overlay correctly positioned as higher-order concern; ROADMAP still uses "Linearizable" in the comparison table but API docs now correct |
| 2 | Conceptual Integrity | 8 | Sub-handle facade lexically enforces domain separation; `set_with_min_acks` rename correct; `CapabilityHandle` (ad handle) vs `CapabilitiesHandle` (domain handle) naming ambiguity persists; ROADMAP "linearizable" residue (4 occurrences) not yet fixed |
| 3 | Architecture | 8 | Three-layer model and namespace ownership unchanged; gateway feature gate clean; Layer I/II entanglement documented with v2 roadmap direction; no regressions |
| 4 | Modularity | 8 | `TaskCtx` now carries a comprehensive doc comment with field-group table, rationale (reference-cycle prevention), and v2 roadmap direction; CLAUDE.md "God Object" section added for contributors; API-level isolation via sub-handles intact; true runtime isolation still v2 |
| 5 | API Design | 8 | `CapabilityHandle` vs `CapabilitiesHandle` naming ambiguity persists; forwarding stubs listed in `GossipAgent` doc comment index create a dual-path surface; core ergonomics and surface discipline otherwise solid |
| 6 | Error Handling Model | 7 | `docs/guide/error-handling.md` from Run 2 covers all 8 public types with recoverability classification; ~181 production `.unwrap()` calls remain (most are slice/OnceLock conversions, some are real recovery gaps); no structured `Result` propagation across async boundaries documented |
| 7 | Configurability | 8 | 22+ documented `GossipConfig` fields; TOML + `GOSSIP_*` env vars; clean feature flag taxonomy; gateway auth token from Run 2 makes production HTTP deployment safe |
| 8 | Language Best Practices | 8 | `#![forbid(unsafe_code)]`; idiomatic `thiserror`, `papaya`, `parking_lot`; `grp_generation` ordering fix (Relaxed→Release/Acquire) removes a real correctness gap; memory ordering policy now codified for future contributors |
| 9 | Concurrency Correctness | 8 | `grp_generation` Release/Acquire pair correct and documented (comment in store.rs + tasks.rs); `AliveGuard`/`ListenerGuard` Relaxed usage explicitly justified; memory ordering policy documented in CLAUDE.md; `caps_advertised` Release/Acquire already correct; no formal deadlock proof |
| 10 | Resource Management | 7 | RAII for capabilities/locks/handles; GC task evicts orphaned watchers and quorum trackers; TCP writer idle timeout; spawned task bound still undocumented; `JoinSet` growth could be unbounded in long-running nodes |
| 11 | Semantic Correctness | 8 | `consistent_set` / `consistent_get` doc comments corrected throughout — API docs, forwarding stubs, HTTP endpoint descriptions, and README all updated from "linearizable" to accurate "ballot-serialized" description; ROADMAP still has 4 "linearizable" occurrences not yet fixed; LWW, HLC, anti-entropy remain correct |
| 12 | Robustness | 7 | `MAX_FRAME_BYTES` bound; TTL decrement; reconnect backoff; `max_store_entries` OOM guard; fail-open on unverified Ed25519 keys during peer key exchange remains |
| 13 | Security | 7 | Gateway bearer-token auth from Run 2 intact; mTLS still opt-in; no gossip rate-limiting; Ed25519 fail-open during key exchange; `signal_rx_from` sender auth is optional; no session-scoped capability views for LLM agents |
| 14 | Failure Mode Legibility | 7 | `dropped_frames` with actionable diagnostic guide; `gc_alive`/`health_monitor_alive` flags; consensus timeout returns vote counts; Nack reasons still not surfaced to callers; no structured panic context beyond `expect()` messages |
| 15 | Performance | 8 | Benchmarks published; 16 ns get, 151 ns set; lock-free hot path; gossip sharding; zero-copy forward; `reqwest` non-optional adds binary weight for embedded targets |
| 16 | Scalability | 7 | 100-node test passes; practical ceiling (~200–400 nodes) documented; O(N·log N) partial-mesh gossip is a v2 roadmap item; `scan_prefix` O(store) fallback for unknown prefixes; O(N²) topology in current release |
| 17 | Testability | 8 | `make_agent()`, `loopback_pair()`, `EchoBackend`, `alloc_port()` helpers intact; 263 unit tests confirmed; `TaskCtx` still wired-through rather than injected |
| 18 | Test Architecture | 8 | 263 unit + 12 integration + 2 fuzz + 3 overlay Python + 1 scale (100-node Docker); all 12 integration scenarios pass; no property-based convergence tests |
| 19 | Observability | 7 | Prometheus endpoint (opt-in `metrics` feature); `system_stats()`; Grafana dashboard; OTEL only in skillrunner, not in core gossip or signal hot paths; trace-level diagnostics for partition events absent |
| 20 | Debuggability | 7 | KV dump endpoint; `/stats`; `/ready`; management dashboard; `peer_drop_counts()`; consensus ballot state not directly inspectable via public API |
| 21 | Operational Readiness | 8 | `is_ready()`/`/ready`; `shutdown_with_timeout()`; `sys/load/` back-pressure; Docker Compose; `GOSSIP_*` env vars; rolling upgrade window (v10→v11 open); no stop-the-world upgrade procedure documented |
| 22 | Evolvability | 8 | Wire version policy v2–v11 with one-version rolling window; CHANGELOG updated; memory ordering policy documented prevents future ordering regressions; v2.0 workspace-split milestones documented in ROADMAP |
| 23 | Documentation | 8 | `TaskCtx` struct now carries a comprehensive contributor doc comment with field-group table, rationale, and v2 direction; memory ordering policy in CLAUDE.md is actionable; ROADMAP still uses "Linearizable" in 4 places; guide chapter 11 (AFN pipeline) absent from `docs/guide/` |
| 24 | Developer Experience | 8 | `rust-toolchain.toml`; `CLAUDE.md` on-ramp; memory ordering policy guides future atomic additions; TaskCtx section comments aid navigation for new contributors; `make test-scale` for 100-node validation |
| 25 | Dependency Hygiene | 7 | `gateway` feature gate complete (Axum, tower-http, tokio-stream, futures-util all optional); `reqwest` still required (not optional) — adds TLS transitive deps to all embedded builds; `tokio::test-util` still in `[dependencies]` not `[dev-dependencies]` |
| — | **Mean** | **7.7** | |

---

## 2026-06-06 — Run 4

| # | Dimension | Score | Notes |
|---|-----------|:-----:|-------|
| 1 | Philosophy / Coherence with Goal | 9 | Holland/Jini/OSGi/Paremus synthesis intact; library-not-platform honored; `max_active_connections` is an operational knob, not a philosophy violation; 50-node resilience test further validates the "survive churn" goal |
| 2 | Conceptual Integrity | 8 | ROADMAP "linearizable" residue fully cleared (0 occurrences); sub-handle facade enforces domain separation lexically; `CapabilityHandle` (ad handle) vs `CapabilitiesHandle` (domain handle) naming ambiguity persists |
| 3 | Architecture | 8 | Three-layer model and namespace ownership unchanged; gateway feature gate clean; iptables O(N²) constraint documented with v1 runtime mitigation (`max_active_connections`) and v2 structural fix (SWIM hybrid transport) in CLAUDE.md |
| 4 | Modularity | 8 | No structural change; sub-handle isolation at API level intact; `TaskCtx` God Object documented with v2 roadmap direction; true runtime isolation still v2 |
| 5 | API Design | 8 | `max_active_connections` field and `GOSSIP_MAX_ACTIVE_CONNECTIONS` env var added cleanly; `CapabilityHandle` vs `CapabilitiesHandle` naming ambiguity persists; no new footguns |
| 6 | Error Handling Model | 7 | No changes; `docs/guide/error-handling.md` covers all 8 public types; ~181 production `.unwrap()` calls remain; no structured Result propagation policy across async boundaries |
| 7 | Configurability | 8 | `max_active_connections` env var parsed via `map_err(GossipError::Parse)?` (not unwrap) — consistent with existing pattern; 23+ documented `GossipConfig` fields; Fisher-Yates capping documented in CLAUDE.md |
| 8 | Language Best Practices | 8 | Fisher-Yates partial shuffle implemented correctly using `fastrand`; loop bound `slots.min(non_bootstrap.len())` prevents OOB; no new unwraps; `#![forbid(unsafe_code)]` maintained |
| 9 | Concurrency Correctness | 8 | `max_active_connections` peer-selection runs inside existing `run_health_monitor` arc-and-loop without new shared state; no new lock ordering or atomics; memory ordering policy documented in CLAUDE.md remains accurate |
| 10 | Resource Management | 7 | No changes; RAII for capabilities/locks/handles intact; GC task evicts orphaned watchers; spawned task bound still undocumented; `JoinSet` growth could be unbounded in long-running nodes |
| 11 | Semantic Correctness | 8 | ROADMAP "linearizable" residue cleared (was 4 occurrences in Run 3); `consistent_set`/`consistent_get` API and HTTP docs accurate; LWW, HLC, anti-entropy correct; one correct negative use ("not a substitute for linearizable reads") in README |
| 12 | Robustness | 7 | 50-node resilience test validates crash recovery (5 workers), anti-entropy inbound/outbound (late joiner), and 3-cycle churn (10 workers); fail-open on Ed25519 key exchange remains; `MAX_FRAME_BYTES` and reconnect backoff unchanged |
| 13 | Security | 7 | No changes; bearer-token gateway auth from Run 2 intact; mTLS opt-in; no gossip rate-limiting; Ed25519 fail-open during key exchange; `signal_rx_from` sender auth optional |
| 14 | Failure Mode Legibility | 7 | No changes; `dropped_frames` diagnostic guide, `gc_alive`/`health_monitor_alive` flags, consensus vote counts on timeout remain; Nack reasons still not surfaced to callers |
| 15 | Performance | 8 | No changes; `max_active_connections` Fisher-Yates is O(K) not O(N) on every health-check tick — no regression; benchmarks published; lock-free hot path intact |
| 16 | Scalability | 7 | `max_active_connections` reduces O(N²)→O(N×K) per-node TCP connections at runtime — meaningful mitigation for 100–500 node deployments; default 0 (unlimited) preserves existing behaviour; O(N·log N) partial-mesh and SWIM UDP transport remain v2 roadmap items |
| 17 | Testability | 8 | No structural changes; `make_agent()`, `loopback_pair()`, `EchoBackend`, `alloc_port()` helpers intact; 263 unit tests confirmed; `TaskCtx` still wired-through |
| 18 | Test Architecture | 8 | 50-node resilience test (`make test-scale-resilience`) adds 4-phase crash/anti-entropy/late-joiner/churn coverage; `docker run` late-joiner validates anti-entropy inbound and gossip outbound independently; all 12 integration scenarios pass; no property-based convergence tests |
| 19 | Observability | 7 | No changes; Prometheus endpoint (opt-in `metrics` feature); `system_stats()`; Grafana dashboard; OTEL only in skillrunner, not in core gossip or signal hot paths |
| 20 | Debuggability | 7 | No changes; KV dump endpoint; `/stats`; `/ready`; management dashboard; `peer_drop_counts()`; consensus ballot state not directly inspectable via public API |
| 21 | Operational Readiness | 8 | `make test-scale-resilience` target adds operator-facing 50-node validation; `is_ready()`/`/ready`; `shutdown_with_timeout()`; `sys/load/` back-pressure; rolling upgrade window open (v10→v11) |
| 22 | Evolvability | 8 | v2.0 Milestone #5 (hybrid TCP/UDP transport, SWIM-style) documented in ROADMAP with trigger conditions and expected outcome; CLAUDE.md extended with v2 transport fix note; CHANGELOG updated; wire version policy unchanged |
| 23 | Documentation | 8 | ROADMAP "linearizable" residue cleared; SWIM-style v2 transport section detailed and accurate (correct SWIM protocol description — UDP pings, TCP state transfer); iptables constraint section in CLAUDE.md now cross-references v1 mitigation and v2 structural fix; guide chapter 11 (AFN pipeline) still absent from `docs/guide/` |
| 24 | Developer Experience | 8 | `make test-scale-resilience` and `make test-scale-resilience-clean` targets added; `RESILIENCE_WORKERS` override; `make test-scale-resilience RESILIENCE_WORKERS=10` for quick local validation; `CLAUDE.md` on-ramp unchanged |
| 25 | Dependency Hygiene | 7 | No changes; `gateway` feature gate complete; `reqwest` still required (not optional); `tokio::test-util` still in `[dependencies]` not `[dev-dependencies]` |
| — | **Mean** | **7.7** | |

---

## 2026-06-06 — Run 5

Changes since Run 4: anti-entropy on reconnect (d4520be); `is_ready()` / `/ready` endpoint (d4520be); durability contract documentation (d4520be); 6 integration scenarios fixed (4 outright failing + 2 flaky → all 12 passing — 2ef4b4a, ad4122f, eb74cf0); `ConsensusPair` helper + consensus test bug (tests passing by ballot-retry luck, not correct setup — 5bf958b); CLAUDE.md testing conventions (3261b11).

| # | Dimension | Score | Notes |
|---|-----------|:-----:|-------|
| 1 | Philosophy / Coherence with Goal | 9 | Holland/Jini/OSGi/Paremus synthesis intact; library-not-platform honored; anti-entropy-on-reconnect aligns directly with "always partition-tolerant substrate" goal |
| 2 | Conceptual Integrity | 8 | `CapabilityHandle` (ad handle) vs `CapabilitiesHandle` (domain handle) naming ambiguity persists; otherwise consistent throughout |
| 3 | Architecture | 8 | Three-layer model and namespace ownership unchanged; anti-entropy-on-reconnect is correctly placed in `run_health_monitor` (tasks.rs) — no layer violations |
| 4 | Modularity | 8 | Sub-handle facade intact; `TaskCtx` God Object documented with v2 roadmap direction |
| 5 | API Design | 8 | `is_ready()` public method added cleanly; `CapabilityHandle` vs `CapabilitiesHandle` naming residue persists |
| 6 | Error Handling Model | 7 | No changes; `docs/guide/error-handling.md` covers all 8 types; ~181 production `.unwrap()` calls remain |
| 7 | Configurability | 8 | No new config fields; `health_check_max_jitter_ms` now exercised correctly in test helpers |
| 8 | Language Best Practices | 8 | `#![forbid(unsafe_code)]` maintained; anti-entropy fix is a clean loop addition; `caps_advertised` Release/Acquire ordering consistent with documented policy |
| 9 | Concurrency Correctness | 8 | Anti-entropy on reconnect adds `request_state` inside an existing lock-free peer-set diff — no new shared state; memory ordering policy intact |
| 10 | Resource Management | 7 | No changes; spawned task bound still undocumented |
| 11 | Semantic Correctness | 8 | Anti-entropy-on-reconnect closes a correctness gap: soft-state keys (caps, locality) now propagate within one gossip round-trip of reconnection rather than waiting up to 30 s; LWW and HLC correctness unchanged |
| 12 | Robustness | 8 | ↑ Anti-entropy on reconnect is a real production fix — previously a restarted node's capabilities/locality wouldn't propagate to the cluster until the next advertisement tick (5–30 s); now immediate; 6 integration scenarios stabilised (4 outright failing, 2 flaky — all 12 now pass); fail-open on Ed25519 key exchange remains |
| 13 | Security | 7 | No changes; bearer-token gateway auth intact; mTLS opt-in; no gossip rate-limiting |
| 14 | Failure Mode Legibility | 7 | No changes; Nack reasons still not surfaced to callers |
| 15 | Performance | 8 | `request_state` on reconnect is one extra StateRequest per new peer-set member — negligible; lock-free hot path intact |
| 16 | Scalability | 7 | No changes; O(N²) topology in current release; O(N·log N) partial-mesh is v2 roadmap |
| 17 | Testability | 8 | `ConsensusPair` encapsulates correct 4-step multi-node setup; `TaskCtx` still wired-through rather than injected — incremental improvement |
| 18 | Test Architecture | 9 | ↑ Two independent quality improvements: (a) 6 integration scenarios fixed (4 failing + 2 flaky → all 12 passing reliably); (b) consensus unit tests refactored from "passing by ballot-retry luck" to "passing by correct structural polling + proper listener setup" — tests now assert the intended invariant, not an accidental side-effect of the retry window |
| 19 | Observability | 7 | No changes; Prometheus endpoint (opt-in `metrics` feature); Grafana dashboard; OTEL only in skillrunner |
| 20 | Debuggability | 7 | No changes; consensus ballot state still not directly inspectable |
| 21 | Operational Readiness | 9 | ↑ `is_ready()` + `/ready` (503 while starting, 200 when soft state hydrated) implements the standard Kubernetes two-probe liveness/readiness distinction; previously only `/health` (liveness) existed — no way to know if capabilities had been advertised post-restart |
| 22 | Evolvability | 8 | No changes; wire version policy intact |
| 23 | Documentation | 8 | Durability contract section added to `src/lib.rs` crate doc (what needs at least one persistent node, what regenerates on reconnect); CLAUDE.md testing conventions section documents `start_consensus_listener` requirement and structural polling principle |
| 24 | Developer Experience | 9 | ↑ `ConsensusPair` helper + CLAUDE.md testing conventions document a non-obvious pitfall (ballot retry window masks peer connectivity race); anti-entropy-on-reconnect + `/ready` make restart behaviour predictable; `make test-scale-resilience RESILIENCE_WORKERS=10` for quick local validation |
| 25 | Dependency Hygiene | 7 | No changes; `reqwest` still required; `tokio::test-util` still in `[dependencies]` not `[dev-dependencies]` |
| — | **Mean** | **7.8** | |

---

## 2026-06-07 — Run 6

Changes since Run 5: Ed25519 fail-open → fail-closed (`SignedData` from unknown signers now dropped with `warn!` — fail-closed is safe because anti-entropy-on-reconnect delivers the signer's `sys/identity/` key within one gossip round-trip); complete mutex poison recovery (10 remaining `.lock().unwrap()` in `helpers.rs`, `capability_ops.rs`, `capability_handle.rs`, `http.rs` → `.unwrap_or_else(|e| e.into_inner())`); 4 clippy errors fixed (`a2a.rs` dead_code, `lifecycle.rs` Arc::clone ×2, `prompt.rs` while-let) — clippy now passes at 0 warnings with full `--features tls,metrics,a2a,llm -D warnings`; `docs/guide/13-cluster-topology.md` — comprehensive cluster operations chapter (seed shapes, sizing worksheet, partition recovery, Docker Compose health-check pattern); all test levels pass: 277 unit, 12/12 integration, 5/5 scale (100 nodes), 10/10 resilience (21 nodes).

| # | Dimension | Score | Notes |
|---|-----------|:-----:|-------|
| 1 | Philosophy / Coherence with Goal | 9 | Holland/Jini/OSGi/Paremus synthesis intact; fail-closed Ed25519 tightens substrate invariants without architectural drift |
| 2 | Conceptual Integrity | 8 | `CapabilityHandle` (advertisement handle) vs `CapabilitiesHandle` (domain handle) naming residue persists; topology guide uses consistent glossary |
| 3 | Architecture | 8 | Three-layer model and namespace table unchanged; `cap_ns_index` secondary index provides O(1) cap lookups; `reqwest` bleeds into non-gateway builds via `capability_config.rs` and `bulk.rs` |
| 4 | Modularity | 8 | Sub-handle facade intact; `TaskCtx` God Object documented; v2 crate-split roadmap item unchanged |
| 5 | API Design | 8 | No changes; `is_ready()` + sub-handle surface clean; `CapabilityHandle` naming residue noted |
| 6 | Error Handling Model | 7 | Complete mutex poison recovery (all `.lock().unwrap()` now `.unwrap_or_else`); ~170+ production unwraps remain (most infallible slice conversions); error taxonomy and guide unchanged |
| 7 | Configurability | 8 | No config API changes; topology guide documents which knobs to tune and when, making the 30-field `GossipConfig` surface approachable for operators |
| 8 | Language Best Practices | 8 | 0 clippy warnings at full feature matrix (`--features tls,metrics,a2a,llm -D warnings`); mutex poison recovery complete; `#![deny(unsafe_code)]` enforced; only `unsafe` in codebase is `std::env::set_var` inside a test |
| 9 | Concurrency Correctness | 8 | No new concurrency code; papaya pin() guard invariant documented and respected; atomic ordering policy unchanged |
| 10 | Resource Management | 7 | No changes; spawned task count still unbounded — no documented ceiling on concurrent capability advertisement tasks |
| 11 | Semantic Correctness | 8 | LWW merge, HLC causality, quorum accounting unchanged; anti-entropy-on-reconnect correctness confirmed by 21-node resilience test (all 3 Phase 3 late-joiner probes pass) |
| 12 | Robustness | 9 | ↑ Ed25519 fail-closed plugs the explicitly called-out gap from Run 5; `SignedData` from unknown signers dropped rather than accepted; recovery within one round-trip via anti-entropy confirmed; complete mutex poison recovery eliminates the cascade-panic risk from Mutex poisoning |
| 13 | Security | 8 | ↑ Ed25519 now fail-closed (was fail-open); bearer-token gateway auth; mTLS opt-in; gossip rate-limiting still absent |
| 14 | Failure Mode Legibility | 7 | `warn!` for Ed25519 drops is specific and actionable; Nack reasons still not surfaced to callers; no changes to ballot state visibility |
| 15 | Performance | 8 | No changes; `cap_ns_index` O(1) cap lookups; lock-free papaya hot paths intact |
| 16 | Scalability | 7 | Topology guide documents sizing worksheet and `max_active_connections` partial-mesh mitigation; O(N²) cliff documented but not architecturally addressed until v2 |
| 17 | Testability | 8 | No changes; 277 tests; `ConsensusPair` helper; `TaskCtx` still wired-through rather than injected |
| 18 | Test Architecture | 9 | 277/277 unit, 12/12 integration, 5/5 scale, 10/10 resilience all pass; full feature matrix (`--features tls,metrics,a2a,llm`) now verified at 0 warnings |
| 19 | Observability | 7 | No changes; `/metrics` Prometheus endpoint; `dropped_frames` tracked; no OTEL span propagation in gossip hot path |
| 20 | Debuggability | 7 | No changes; topology guide documents `/ready` + `/health` usage; consensus ballot state still not directly inspectable |
| 21 | Operational Readiness | 9 | Topology guide (chapter 13) covers seed configuration, Docker Compose health-check pattern, partition recovery, sizing worksheet — closes the primary operational documentation gap |
| 22 | Evolvability | 8 | No changes; wire v11 / v10 rolling window intact; CHANGELOG maintained |
| 23 | Documentation | 8 | Chapter 13 topology guide added; all core chapters (01–04) verified to use current sub-handle API syntax; chapter 11 (`11-semantic-coordination.md`) still referenced in README but file does not exist |
| 24 | Developer Experience | 9 | 0 clippy warnings at full feature matrix now enforced; topology guide reduces operational guesswork; test suite covers all five levels |
| 25 | Dependency Hygiene | 7 | `reqwest` still in `[dependencies]` (used by `capability_config.rs` + `bulk.rs` — bleeds Hyper into bare-metal builds); `tokio-test-util` still in `[dependencies]` not `[dev-dependencies]` |
| — | **Mean** | **7.9** | |

---

## 2026-06-07 — Run 7

| # | Dimension | Score | Notes |
|---|-----------|:-----:|-------|
| 1 | Philosophy / Coherence with Goal | 9 | Holland/Jini/OSGi/Paremus synthesis fully honored; `reqwest` now optional restores the "library, not platform" constraint for bare-metal targets; no feature drift |
| 2 | Conceptual Integrity | 8 | `CapabilityHandle` (ad handle) vs `CapabilitiesHandle` (domain handle) naming ambiguity persists as the one remaining idiom inconsistency; all other naming consistent |
| 3 | Architecture | 8 | Three-layer model and namespace ownership unchanged; `reqwest` properly gated — `--no-default-features` build passes cleanly; no regressions |
| 4 | Modularity | 8 | Sub-handle facade intact; `TaskCtx` God Object documented with v2 roadmap direction; no structural change |
| 5 | API Design | 8 | `#[tracing::instrument]` on all 11 critical public methods (KvHandle×4, MeshHandle×3, ConsensusHandle×4) adds observability without API surface change; `CapabilityHandle` naming residue persists |
| 6 | Error Handling Model | 7 | Infallible `.unwrap()` converted to `.expect("message")` in connection.rs, signal.rs, store.rs, bulk.rs — intent now documented; ~200 production unwraps remain, majority in test helper functions and framing tests; no structural error-type changes |
| 7 | Configurability | 8 | No config API changes; tracing instrument on hot-path methods provides dynamic observability gain at no config overhead |
| 8 | Language Best Practices | 9 | ↑ Infallible unwrap → `.expect()` conversion documents intent at call sites; `cargo build --lib --no-default-features` passes (reqwest now optional); 277/277 tests at 0 clippy warnings; `#![forbid(unsafe_code)]` maintained; `field_reassign_with_default` allow is correctly scoped to test code |
| 9 | Concurrency Correctness | 8 | No new concurrency code; atomic ordering policy unchanged and correct; memory ordering policy documentation intact |
| 10 | Resource Management | 7 | No changes; spawned task count still undocumented; RAII handle semantics intact |
| 11 | Semantic Correctness | 8 | LWW, HLC causality, consensus quorum accounting all correct; no semantic changes in this run |
| 12 | Robustness | 9 | No regression; Ed25519 fail-closed from Run 6 persists; mutex poison recovery complete; anti-entropy-on-reconnect persists |
| 13 | Security | 8 | No changes; Ed25519 fail-closed; bearer-token gateway auth; mTLS opt-in; gossip rate-limiting still absent |
| 14 | Failure Mode Legibility | 7 | `.expect("message")` on formerly silent unwraps makes panics actionable; tracing spans on critical paths improve distributed debugging context; Nack reasons still not surfaced to callers |
| 15 | Performance | 8 | `#[tracing::instrument]` at trace/debug level is zero-cost when no subscriber is installed (tracing is no-op by default); no hot-path regression; benchmarks unchanged |
| 16 | Scalability | 7 | No changes; O(N²) topology cliff documented but architecturally deferred to v2 |
| 17 | Testability | 8 | 277 unit tests pass; no structural testability change; `TaskCtx` still wired-through rather than injected |
| 18 | Test Architecture | 9 | 277/277 unit, 12/12 integration, 100-node scale, 21-node resilience all pass; full feature matrix (`--features tls,metrics,a2a,llm`) verified at 0 warnings; no property-based convergence tests |
| 19 | Observability | 8 | ↑ `#[tracing::instrument]` on `KvHandle::{set,get,set_async,scan_prefix}`, `MeshHandle::{emit,emit_ordered,emit_async}`, `ConsensusHandle::{group_propose,system_propose,consistent_set,distributed_lock}` — 11 spans on the operations that matter most; combined with existing `/metrics` Prometheus endpoint and Grafana dashboard, operators can now correlate latency to specific operations; OTEL still only in skillrunner |
| 20 | Debuggability | 7 | Tracing spans help; consensus ballot state still not directly inspectable via public API |
| 21 | Operational Readiness | 9 | No changes; `is_ready()`/`/ready`; `shutdown_with_timeout()`; chapter 13 topology guide; rolling upgrade window |
| 22 | Evolvability | 8 | No changes; wire v11 / v10 rolling window intact; CHANGELOG maintained |
| 23 | Documentation | 8 | No new chapters; chapter 11 (`11-semantic-coordination.md`) still absent but referenced in docs/guide/README.md; existing chapters 01–13 use current sub-handle API |
| 24 | Developer Experience | 9 | `cargo build --lib --no-default-features` now confirmed clean — contributors can develop against the bare substrate without pulling Hyper; tracing spans on critical paths aid local debugging; all existing DX improvements persist |
| 25 | Dependency Hygiene | 8 | ↑ `reqwest` is now optional (`gateway` feature only) — `cargo build --lib --no-default-features` passes and does not pull Hyper; `tokio-test-util` still in `[dependencies]` not `[dev-dependencies]` (minor residue); supply chain risk otherwise low |
| — | **Mean** | **8.0** | |

## 2026-06-07 — Run 8

| # | Dimension | Score | Notes |
|---|-----------|:-----:|-------|
| 1 | Philosophy / Coherence with Goal | 9 | Exceptional Holland/Jini/Paremus alignment; "no coordinator" claim now properly qualified with seed-as-soft-coordinator and consensus proposer caveats |
| 2 | Conceptual Integrity | 8 | Consistent idioms throughout; LLM/MCP methods remain on `GossipAgent` rather than a typed handle — minor inconsistency with the sub-handle pattern |
| 3 | Architecture | 8 | Clean layer separation enforced by namespace table; Layer I/II entanglement documented as known v2 item; `TaskCtx` God Object section added to CLAUDE.md |
| 4 | Modularity | 8 | Six handles independently understandable and storeable; `TaskCtx` shared state is the only remaining coupling, deferred to workspace split in v2 |
| 5 | API Design | 8 | Minimal public surface; sub-handle pattern is clean and hard to misuse; `set_with_min_acks` naming now correct; no footguns in core paths |
| 6 | Error Handling Model | 7 | All production `unwrap()` converted to `expect("infallible: reason")`; `GossipError::State(String)` still opaque; no structured error code taxonomy |
| 7 | Configurability | 9 | Comprehensive `GossipConfig` with env overrides (`GOSSIP_*`), well-organized feature gates, TOML load, CLI flags — covers all operational concerns |
| 8 | Language Best Practices | 8 | `#![deny(unsafe_code)]`, idiomatic async Rust; dual concurrent map (`dashmap` + `papaya`) is minor redundancy; no `unwrap()` in production paths |
| 9 | Concurrency Correctness | 8 | Memory ordering policy documented for every atomic; lock-free hot paths; `JoinSet` reaping prevents task accumulation; no identified races |
| 10 | Resource Management | 8 | `task_count` in `SystemStats` now surfaced; per-peer drop counts via `peer_drop_counts()`; explicit drop semantics on capability handles |
| 11 | Semantic Correctness | 8 | LWW merge correct (tombstone wins on tie); HLC tick/observe contract correct; quorum arithmetic correct; epidemic Paxos re-proposal sound |
| 12 | Robustness | 8 | Graceful degradation on shard death; listener auto-restart; anti-entropy on reconnect; missing frame-size cap is known DoS surface |
| 13 | Security | 7 | mTLS opt-in with Ed25519 identity; signed consensus payloads; frame-size DoS still open; no RBAC; gateway auth is `compliance` feature (unimplemented) |
| 14 | Failure Mode Legibility | 8 | `ConsensusResult` variants carry detail; `task_count` exposes leaks; `/consensus/{slot}` HTTP inspector; `peer_drop_counts` identifies slow peers |
| 15 | Performance | 8 | 151 ns `set`, 16 ns `get` benchmarked; tracing at `trace!` level; gossip fan-out is O(K) per node not O(N); no hot-path allocations identified |
| 16 | Scalability | 8 | O(N²) TCP cliff now documented with explicit connection-count table and iptables saturation thresholds; `max_active_connections` mitigation documented |
| 17 | Testability | 8 | Deterministic single-process unit tests; injectable config; no global state; fuzz coverage on framing and HLC |
| 18 | Test Architecture | 8 | 277 unit, 12 integration, 3 overlay, 2 fuzz, 2 scale tests; good pyramid shape; property-based / proptest coverage absent |
| 19 | Observability | 8 | Prometheus + Grafana, structured tracing, OTEL traces, `task_count` in stats, `/consensus/{slot}` added this run |
| 20 | Debuggability | 8 | `/consensus/{slot}` endpoint for live inspection; `task_count` catches leaks; `peer_drop_counts()` identifies slow peers; KV dump via scan |
| 21 | Operational Readiness | 8 | `/ready` probe, `shutdown_with_timeout`, persistence (`SyncMode::Flush`), Docker Compose health check wiring documented |
| 22 | Evolvability | 8 | Wire version policy (`WIRE_VERSION` + `PREV_WIRE_VERSION`), CHANGELOG under `[Unreleased]`, ROADMAP v2 milestones; debt documented not hidden |
| 23 | Documentation | 8 | 13 guide chapters (ch.01–13) + philosophy.html; ch.11 (consensus guide) missing; no CONTRIBUTING.md; API examples use current sub-handle syntax |
| 24 | Developer Experience | 8 | CLAUDE.md onramp with operational diagnostics reference; `rust-toolchain.toml`; Makefile; no visible CI config in repo |
| 25 | Dependency Hygiene | 8 | Optional deps properly feature-gated; `dashmap`/`papaya` redundancy; one transitive deprecation warning (`block v0.1.6`); `Cargo.lock` present |
| — | **Mean** | **8.0** | |

## 2026-06-07 — Run 9

| # | Dimension | Score | Notes |
|---|-----------|:-----:|-------|
| 1 | Philosophy / Coherence with Goal | 9 | Holland/Jini/Paremus synthesis fully realised; "no coordinator" properly scoped; every feature traces back to the stated substrate purpose |
| 2 | Conceptual Integrity | 8 | Sub-handle pattern consistent across six domains; `register_prompt_skill` / `register_mcp_tool` still live on `GossipAgent` directly rather than typed handles — deliberate but still reads as inconsistency |
| 3 | Architecture | 8 | Layer separation enforced by namespace table; papaya pin-guard `await` invariant now documented in `KvState`; Layer I/II entanglement acknowledged as v2 roadmap item |
| 4 | Modularity | 8 | Six handles independently usable; single concurrent map (`papaya`) throughout after dashmap removal; TaskCtx shared state remains the only cross-handle coupling |
| 5 | API Design | 8 | Minimal, hard-to-misuse surface; `AlreadyRunning`/`Shutdown` typed errors; `signal_rx_from` trust filter elegant; no new footguns |
| 6 | Error Handling Model | 8 | `GossipError::AlreadyRunning` and `Shutdown` are matchable structural variants; all production paths use `expect("infallible: reason")`; `Network(String)` / `Config(String)` are still catch-alls for their domains |
| 7 | Configurability | 9 | Comprehensive `GossipConfig` with env overrides, feature gates, TOML, CLI — all operational knobs exposed without requiring code changes |
| 8 | Language Best Practices | 9 | `#![deny(unsafe_code)]`, no production `unwrap()`, single concurrent map (papaya) throughout after dashmap removal, `is_some_and` used idiomatically; `async-trait` is the only lingering rough edge |
| 9 | Concurrency Correctness | 8 | Memory ordering policy documented for every atomic; `last_state_sent` is task-local (no sharing); no identified races |
| 10 | Resource Management | 8 | `task_count` in SystemStats; anti-entropy cooldown adds per-connection `Instant` (zero heap); explicit handle drop semantics throughout |
| 11 | Semantic Correctness | 8 | LWW convergence and HLC monotonicity now property-tested with proptest; formal guarantees match code; chunked anti-entropy gap documented |
| 12 | Robustness | 9 | StateRequest cooldown prevents scan-flood DoS; frame rejection tests verify zero-length and oversized paths work; existing infrastructure (frame cap, connection limit, TTL clamp, malformed-frame skip) comprehensive |
| 13 | Security | 7 | mTLS + Ed25519 + gateway bearer token; StateRequest cooldown adds per-connection DoS protection; no global rate limit, no RBAC, `compliance` feature unimplemented |
| 14 | Failure Mode Legibility | 8 | `AlreadyRunning`/`Shutdown` give clear callsite diagnostics; `expect("infallible: ...")` messages explain invariants; cooldown logs at `debug!`; no regression |
| 15 | Performance | 8 | 151 ns set, 16 ns get; O(K) gossip fan-out; anti-entropy cooldown adds only an `Instant::elapsed()` check on the hot path |
| 16 | Scalability | 8 | O(N²) cliff documented with explicit table; `max_active_connections` mitigation documented; v2 SWIM transport on roadmap |
| 17 | Testability | 8 | Deterministic, injectable, no hidden global state; proptest now part of the test tool-chain |
| 18 | Test Architecture | 9 | 287 unit + 12 integration + 2 fuzz + 10 proptest (LWW convergence, HLC monotonicity, framing round-trip) — four-tier pyramid; property coverage of core formal invariants |
| 19 | Observability | 8 | Prometheus + OTEL + `task_count` + `/consensus/{slot}`; no regression |
| 20 | Debuggability | 8 | `/consensus/{slot}`, `task_count`, `peer_drop_counts()`, KV dump; no new tools this run |
| 21 | Operational Readiness | 8 | `/ready`, `shutdown_with_timeout`, persistence, Docker health checks; no regression |
| 22 | Evolvability | 8 | CHANGELOG consistently updated; dashmap removal is clean dep-tree hygiene; wire-version policy unchanged |
| 23 | Documentation | 9 | All 13 guide chapters now exist (ch.11 written this run); CONTRIBUTING.md comprehensive with build matrix, layer rules, wire-version policy; no guide chapter gaps remaining |
| 24 | Developer Experience | 8 | CONTRIBUTING.md now covers full contribution path; `CLAUDE.md` onramp detailed; no CI config in repo remains the main gap |
| 25 | Dependency Hygiene | 9 | dashmap removed — single concurrent map (`papaya`) throughout; all optional deps properly feature-gated; `Cargo.lock` present; `--no-default-features` compiles cleanly |
| — | **Mean** | **8.2** | |

---

## 2026-06-07 — Run 10

Changes since Run 9: `LlmHandle` and `McpHandle` sub-handles added (`17aef72`), completing the 8-handle typed facade; all six LLM prompt-skill methods (`register_prompt_skill`, `call_prompt_skill`, `update_prompt`, `get_prompt`, `list_prompts`, `delete_prompt`) moved off `GossipAgent` onto `LlmHandle`; MCP bridge methods (`register_mcp_tool`, `connect_mcp_server`) moved to `McpHandle`; `advertise_capability` return type renamed from `CapabilityHandle` to `CapabilityReg` — resolves the long-standing handle-naming ambiguity; `tokio test-util` confirmed in `[dev-dependencies]` (not `[dependencies]`); all test levels pass; 0 clippy warnings.

| # | Dimension | Score | Notes |
|---|-----------|:-----:|-------|
| 1 | Philosophy / Coherence with Goal | 9 | Holland/Jini/OSGi/Paremus synthesis fully honored; handle completion solidifies the "library, not platform" contract; no feature drift |
| 2 | Conceptual Integrity | 9 | `CapabilityReg` (advertisement lifetime) vs `CapabilitiesHandle` (domain handle) naming ambiguity resolved — the last named idiom inconsistency is gone; all eight domains now follow the same pattern; `LlmHandle`/`McpHandle` are idiomatic with the rest |
| 3 | Architecture | 8 | Three-layer model and namespace table unchanged; gateway feature gate clean; Layer I/II entanglement documented with v2 roadmap direction; `reqwest` now optional — no regressions |
| 4 | Modularity | 9 | Eight independently understandable, storable, moveable handles: `KvHandle`, `MeshHandle`, `CapabilitiesHandle`, `ConsensusHandle`, `ServiceHandle`, `SchemaHandle`, `LlmHandle`, `McpHandle`; `TaskCtx` shared state remains the only coupling, correctly deferred to v2 workspace split; LLM skills registry moved into `TaskCtx` so `LlmHandle` needs no borrow of the agent |
| 5 | API Design | 9 | `CapabilityReg` return type makes drop semantics explicit at the type level; `#[must_use]` on `advertise_capability`; eight-handle facade is minimal, orthogonal, hard to misuse; no remaining public footguns in core paths; `signal_rx_from` trust filter is elegant |
| 6 | Error Handling Model | 8 | `GossipError::AlreadyRunning`/`Shutdown` matchable; all production `.unwrap()` are in test helpers or genuinely infallible; `Network(String)` / `Config(String)` remain catch-alls for their domains — no structured error code taxonomy; error-handling guide covers all 8 public types |
| 7 | Configurability | 9 | Comprehensive `GossipConfig` with env overrides (`GOSSIP_*`), TOML load, CLI flags, feature gates — all operational knobs exposed without requiring code changes; cluster topology guide makes the 30-field surface approachable |
| 8 | Language Best Practices | 9 | `#![deny(unsafe_code)]`, 0 clippy warnings at full feature matrix, `tokio test-util` in `[dev-dependencies]`, `CapabilityReg` `#[must_use]`, proptest on core invariants; `async-trait` is the only lingering rough edge |
| 9 | Concurrency Correctness | 8 | Memory ordering policy documented for every atomic; `LlmHandle` accesses `llm_skills` via `TaskCtx` field — same concurrency model as other handles; no new lock ordering issues; no formal deadlock proof |
| 10 | Resource Management | 8 | `CapabilityReg` makes advertisement lifetime explicit and drop-based; `task_count` in `SystemStats`; RAII throughout; spawned task ceiling still not documented |
| 11 | Semantic Correctness | 8 | LWW convergence and HLC monotonicity property-tested; quorum arithmetic correct; `consistent_set` correctly described as "ballot-serialized" throughout; no regressions |
| 12 | Robustness | 9 | Ed25519 fail-closed; anti-entropy-on-reconnect; mutex poison recovery complete; `MAX_FRAME_BYTES` bound; listener auto-restart; 21-node resilience test validates late-joiner and churn recovery |
| 13 | Security | 8 | mTLS + Ed25519 + gateway bearer token; signed consensus payloads; no gossip rate-limiting; no RBAC; `compliance` feature unimplemented; `signal_rx_from` sender auth covers semantic injection |
| 14 | Failure Mode Legibility | 8 | `ConsensusResult` variants carry detail; `expect("infallible: …")` messages document invariants; `task_count` exposes leaks; `peer_drop_counts` identifies slow peers; Nack reasons still not surfaced to callers |
| 15 | Performance | 8 | 151 ns set, 16 ns get benchmarked; O(K) gossip fan-out; lock-free hot paths; `LlmHandle` adds no overhead — same `Arc<TaskCtx>` clone pattern; no hot-path regressions |
| 16 | Scalability | 8 | O(N²) TCP cliff documented with explicit table; `max_active_connections` mitigation operational; v2 SWIM hybrid transport on roadmap; `scan_prefix` O(store) fallback for unknown prefixes |
| 17 | Testability | 8 | Deterministic, injectable, no hidden global state; proptest on LWW/HLC/framing; `ConsensusPair` helper; `EchoBackend` for LLM; `TaskCtx` still wired-through rather than injected |
| 18 | Test Architecture | 9 | 287 unit + 12 integration + 2 fuzz + 3 overlay + 2 scale (100-node + 21-node resilience) + proptest — five-tier pyramid covering formal invariants; all pass; full feature matrix (`--features tls,metrics,a2a,llm`) at 0 warnings |
| 19 | Observability | 8 | Prometheus + Grafana, `#[tracing::instrument]` on 11 critical methods, `task_count`, `/consensus/{slot}`; OTEL still only in skillrunner |
| 20 | Debuggability | 8 | `/consensus/{slot}`, `task_count`, `peer_drop_counts()`, KV dump; handle typesystem makes ownership traces easier to follow; consensus ballot state not directly inspectable via API |
| 21 | Operational Readiness | 9 | `/ready`, `shutdown_with_timeout`, persistence (`SyncMode::Flush`), Docker Compose health-check wiring, chapter 13 topology guide, `is_ready()` Kubernetes readiness probe |
| 22 | Evolvability | 8 | Wire version policy (`WIRE_VERSION` + `PREV_WIRE_VERSION`); CHANGELOG under `[Unreleased]`; ROADMAP v2 milestones; 8-handle facade documented as the stable public surface going forward |
| 23 | Documentation | 9 | All 13 guide chapters present; CONTRIBUTING.md with CLA, build matrix, layer rules, wire-version policy; philosophy.html; API examples use `agent.llm()` / `agent.mcp()` syntax throughout; no chapter gaps |
| 24 | Developer Experience | 9 | `cargo build --lib --no-default-features` clean; CONTRIBUTING.md; CLAUDE.md onramp; Makefile; tracing spans on critical paths; handle pattern is learnable from one example; no CI config in repo remains the main gap |
| 25 | Dependency Hygiene | 9 | `tokio test-util` in `[dev-dependencies]`; `reqwest` optional (`gateway` feature only); `papaya` single concurrent map throughout; all optional deps correctly feature-gated; `Cargo.lock` present; `--no-default-features` compiles cleanly |
| — | **Mean** | **8.6** | |

---

## 2026-06-07 — Run 11

Changes since Run 10: `KvStore`/`KvState` architectural split (`src/store.rs`) with `Deref<Target=KvStore>` keeping all call sites unchanged; Layer I/II Bridge Invariant table added to `CLAUDE.md` naming `apply_and_notify` and `subscribe/subscribe_prefix` as the sole crossing points; Lock-Order Table added to `CLAUDE.md` documenting 6 lock sites with the "no simultaneous acquisition" invariant; `test_lww_convergence_two_concurrent_writers` and `test_cross_group_propose_requires_all_group_quorums` unit tests added (290 tests total with full feature matrix); `kv_payload_size` and `capability_resolve` Criterion benchmarks added to `benches/throughput.rs`; `#[non_exhaustive]` added to all 9 public error/result enums (`GossipError`, `ConsistencyError`, `RpcError`, `QuorumError`, `ScatterError`, `ShardError`, `BulkError`, `SchemaError`, `McpError`); `read_frame_accepts_prev_wire_version` wire rolling-upgrade test added; `consistent_get` doc updated with staleness bound (`anti_entropy_interval_secs`, default 30 s).

| # | Dimension | Score | Notes |
|---|-----------|:-----:|-------|
| 1 | Philosophy / Coherence with Goal | 9 | KvStore/KvState split is a Layer I/II structural improvement — entirely consistent with the substrate-not-platform philosophy; no feature drift |
| 2 | Conceptual Integrity | 9 | Naming and idiom consistent throughout; `KvStore` vs `KvState` distinction is clear and documented; no new inconsistencies introduced |
| 3 | Architecture | 9 | `KvStore` (Layer I) / `KvState` (Layer II bridge) split materialises the documented conceptual separation in code; CLAUDE.md bridge invariant names the two crossing points; `Deref` ensures zero call-site churn; `TaskCtx` God Object deferred to v2 with explicit roadmap |
| 4 | Modularity | 9 | Unchanged from Run 10 — eight independently storable handles; KvStore/KvState split adds internal clarity without changing external handle boundaries |
| 5 | API Design | 9 | Unchanged from Run 10; no new surface changes |
| 6 | Error Handling Model | 8 | `#[non_exhaustive]` now on all 9 public error enums — semver-safe variant additions guaranteed; `Network(String)` / `Config(String)` catch-all variants remain, no structured error code taxonomy |
| 7 | Configurability | 9 | Unchanged from Run 10 |
| 8 | Language Best Practices | 9 | Unchanged; `#[non_exhaustive]` adds idiomatic forward-compatibility signal |
| 9 | Concurrency Correctness | 9 | Lock-Order Table documents 6 lock sites with the "no simultaneous acquisition" invariant; `!Send` Mutex guard compiler enforcement noted; papaya pin() guard invariant documented; no formal deadlock proof but the table precludes the classic acquire-in-different-order pattern |
| 10 | Resource Management | 8 | Unchanged from Run 10; spawned task ceiling still undocumented per operation type |
| 11 | Semantic Correctness | 9 | `test_lww_convergence_two_concurrent_writers` verifies HLC-ordered convergence (sequenced writes prevent concurrent-equal-timestamp ambiguity); `test_cross_group_propose_requires_all_group_quorums` proves split-brain property for multi-group proposals; `consistent_get` staleness bound (30 s) documented; epidemic Paxos vs true linearizability gap acknowledged but not formally analysed |
| 12 | Robustness | 9 | Unchanged from Run 10 |
| 13 | Security | 8 | Unchanged from Run 10; no gossip rate-limiting; `compliance` feature unimplemented |
| 14 | Failure Mode Legibility | 8 | Unchanged from Run 10 |
| 15 | Performance | 9 | `kv_payload_size` benchmarks (64/1 024/65 536 bytes) show framing cost scaling; `capability_resolve` benchmarks (1/10/50/100 providers) characterise O(providers) scan; bench file fully updated to sub-handle API; no hot-path regressions; consensus round-trip not yet benchmarked |
| 16 | Scalability | 8 | Unchanged from Run 10 |
| 17 | Testability | 8 | Unchanged from Run 10; `KvStore` slightly more isolatable but `TaskCtx` wiring unchanged |
| 18 | Test Architecture | 9 | 290 unit (full feature matrix) + 12 integration + 2 fuzz + 3 overlay + 2 scale + proptest; two new targeted correctness tests (LWW convergence, cross-group split-brain); wire rolling-upgrade test in framing.rs; five-tier pyramid intact |
| 19 | Observability | 8 | Unchanged from Run 10 |
| 20 | Debuggability | 8 | Unchanged from Run 10 |
| 21 | Operational Readiness | 9 | Unchanged from Run 10 |
| 22 | Evolvability | 9 | `#[non_exhaustive]` on all 9 public error enums — wire-safe downstream match arms; `read_frame_accepts_prev_wire_version` test verifies rolling-upgrade window; CHANGELOG [Unreleased] comprehensive; ROADMAP v2 milestones current; wire version policy documented and tested end-to-end |
| 23 | Documentation | 9 | Unchanged from Run 10 |
| 24 | Developer Experience | 9 | Lock-Order Table and Layer I/II Bridge Invariant in CLAUDE.md improve contributor onramp for concurrency-sensitive work |
| 25 | Dependency Hygiene | 9 | Unchanged from Run 10 |
| — | **Mean** | **8.7** | |

---

## 2026-06-07 — Run 12

Changes since Run 11: Anti-entropy timing resonance bug fixed — cooldown reduced from `interval_secs` to `interval_secs - 1` in `src/connection.rs`, eliminating the race where the health monitor's retry could arrive at the exact cooldown boundary; bootstrap peer re-trigger loop added to health monitor in `src/agent/tasks.rs` so nodes in `cached_ping_targets` (bootstrap peers) correctly re-trigger anti-entropy on reconnect after cluster restart; scenario 04 (full-cluster restart with WAL recovery) now passes consistently; `docs/operations/tuning.md` added (318 lines) with quick-reference table of 16 parameters, 5 hard invariants with mathematical bounds, scaling guidelines for 4 cluster size ranges, reconnect storm mitigations, tombstone safety window formula, and monitoring checklist; structured error variants (`InvalidField`, `FieldConflict`, `NodeIdMismatch`, `FrameTooLarge`, `UnsupportedWireVersion`) replace stringly-typed `Config(String)` / `Network(String)` catch-alls; `max_inbound_frames_per_sec` and `max_concurrent_bulk_handlers` added to `GossipConfig` with env vars; 100-node scale test and 21-node resilience test (3 churn cycles) confirm no regressions.

| # | Dimension | Score | Notes |
|---|-----------|:-----:|-------|
| 1 | Philosophy / Coherence with Goal | 9 | Holland/Jini/OSGi/Paremus synthesis fully honored; library-not-platform positioning intact; KvStore/KvState split maintains substrate purity; no feature drift |
| 2 | Conceptual Integrity | 9 | Eight-handle facade consistent across all domains; naming unambiguous; `docs/operations/tuning.md` invariants match code behaviour; no new idiom inconsistencies |
| 3 | Architecture | 9 | Three-layer model enforced by namespace table; KvStore/KvState split materialises Layer I/II boundary; gateway feature gate clean; entanglement documented with v2 roadmap |
| 4 | Modularity | 9 | Eight independently understandable, moveable, storable handles; TaskCtx shared state correctly deferred to v2 workspace split; internal coupling explicit and documented |
| 5 | API Design | 9 | `CapabilityReg` return type makes drop semantics explicit; `#[must_use]` on `advertise_capability`; eight-handle pattern minimal and orthogonal; no footguns in core paths |
| 6 | Error Handling Model | 8 | `InvalidField`/`FieldConflict`/`NodeIdMismatch` structured variants replace stringly-typed catch-alls; `#[non_exhaustive]` on all 9 public error enums; `Network(String)` still present for unclassified I/O errors; no structured error code taxonomy |
| 7 | Configurability | 9 | `max_inbound_frames_per_sec` and `max_concurrent_bulk_handlers` added; all 16 tuning parameters now documented with hard invariants in `docs/operations/tuning.md`; operational knobs exposed without code changes |
| 8 | Language Best Practices | 9 | 0 clippy warnings at full feature matrix; `#![deny(unsafe_code)]`; no production `unwrap()`; `#[non_exhaustive]` on all public error enums; proptest on core invariants |
| 9 | Concurrency Correctness | 9 | Lock-Order Table documents 6 lock sites with no-simultaneous-acquisition invariant; Release/Acquire ordering documented per atomic; `!Send` Mutex enforced by compiler; no new lock sites introduced |
| 10 | Resource Management | 8 | `CapabilityReg` RAII for advertisement lifetime; `task_count` in SystemStats; RAII throughout; per-operation spawned task ceiling (`max_concurrent_bulk_handlers`) now documented and enforced |
| 11 | Semantic Correctness | 9 | Anti-entropy timing resonance eliminated: `interval - 1` cooldown gives deterministic 1 s margin; bootstrap re-trigger loop handles `cached_ping_targets` gap; scenario 04 confirms end-to-end; LWW convergence and cross-group split-brain property-tested |
| 12 | Robustness | 9 | All 12 integration scenarios pass; 100-node scale (0 dropped frames); 21-node resilience (3 churn cycles); Ed25519 fail-closed; `MAX_FRAME_BYTES` bound; `max_inbound_frames_per_sec` guards against misbehaving peers |
| 13 | Security | 8 | mTLS + Ed25519 + gateway bearer token + signed consensus payloads + `signal_rx_from` sender auth; `max_inbound_frames_per_sec` adds basic gossip rate-limiting; no RBAC; `compliance` feature unimplemented |
| 14 | Failure Mode Legibility | 8 | Structured error variants carry field context; `ConsensusResult` variants carry detail; `task_count` exposes leaks; `peer_drop_counts` identifies slow peers; Nack reasons still not surfaced to callers |
| 15 | Performance | 9 | 151 ns set, 16 ns get benchmarked; O(K) gossip fan-out; lock-free hot paths; `kv_payload_size` and `capability_resolve` benchmarks; no hot-path regressions from new config fields |
| 16 | Scalability | 8 | O(N²) TCP cliff documented with cluster-size table in `tuning.md`; `max_active_connections` mitigation operational; `scan_prefix` O(store) fallback documented; v2 SWIM hybrid transport on roadmap |
| 17 | Testability | 8 | Deterministic, injectable, no hidden global state; proptest on LWW/HLC/framing; `ConsensusPair` helper; `EchoBackend` for LLM; `TaskCtx` still wired-through rather than injected |
| 18 | Test Architecture | 9 | 290 unit + 12 integration + 2 fuzz + 3 overlay + 2 scale (100-node + 21-node resilience) + proptest; all pass including previously flaky scenario 04; five-tier pyramid |
| 19 | Observability | 8 | Prometheus + Grafana; `#[tracing::instrument]` on critical methods; `task_count`; `/consensus/{slot}`; monitoring checklist in `tuning.md`; OTEL still only in skillrunner |
| 20 | Debuggability | 8 | `/consensus/{slot}`, `task_count`, `peer_drop_counts()`, KV dump; monitoring checklist in `tuning.md`; consensus ballot state not directly inspectable via API |
| 21 | Operational Readiness | 9 | `/ready`, `shutdown_with_timeout`, persistence (`SyncMode::Flush`), Docker Compose health-check wiring, `is_ready()` Kubernetes readiness semantics; `docs/operations/tuning.md` fills the operational runbook gap |
| 22 | Evolvability | 9 | `#[non_exhaustive]` on all 9 public error enums; `WIRE_VERSION`/`PREV_WIRE_VERSION` tested end-to-end; CHANGELOG [Unreleased] comprehensive; ROADMAP v2 milestones current |
| 23 | Documentation | 9 | All 13 guide chapters; CONTRIBUTING.md; philosophy.html; API examples use sub-handle syntax; `docs/operations/tuning.md` adds operational runbook with hard invariants; 5 invariants have mathematical proofs |
| 24 | Developer Experience | 9 | `cargo build --lib --no-default-features` clean; CONTRIBUTING.md + CLAUDE.md onramp; tuning invariants help operators configure correctly without reading source; handle pattern learnable from one example |
| 25 | Dependency Hygiene | 9 | `tokio test-util` in `[dev-dependencies]`; `reqwest` optional; `papaya` single concurrent map; all optional deps feature-gated; `Cargo.lock` present; `--no-default-features` compiles cleanly |
| — | **Mean** | **8.7** | |

---

## 2026-06-07 — Run 13

Changes since Run 12: `GossipError::Io` variant doc clarified — explicitly states it fires only during startup-time I/O (TCP listener bind, WAL read, TLS setup); runtime peer errors absorbed internally and surfaced via `dropped_frames`/`peer_drop_counts()`, callers directed to `err.kind()` for sub-classification. `BulkTransport::active_handlers: Arc<AtomicU64>` added — incremented before `tokio::spawn`, decremented via `ActiveHandlerGuard` RAII on task exit (including panic/cancel); surfaced as `SystemStats::active_bulk_handlers`; CLAUDE.md and `docs/operations/tuning.md` monitoring checklist updated. ROADMAP.md v2.0 Milestones extended with items 6 (RBAC gossip-level authorization, `compliance` feature, trigger = regulated-industry deployment) and 7 (cluster-wide distributed rate-limiting, trigger = confirmed intra-cluster abuse). All 276 unit tests, 12 integration scenarios, 100-node scale, and 21-node resilience pass.

| # | Dimension | Score | Notes |
|---|-----------|:-----:|-------|
| 1 | Philosophy / Coherence with Goal | 9 | Holland/Jini/Paremus synthesis fully honored; library-not-platform intact; no feature drift |
| 2 | Conceptual Integrity | 9 | Eight-handle facade consistent; `CapabilityReg` naming resolved last ambiguity; all idioms aligned |
| 3 | Architecture | 9 | KvStore/KvState split materialises Layer I/II boundary; namespace table enforced; gateway feature gate clean; entanglement documented with v2 roadmap |
| 4 | Modularity | 9 | Eight independently understandable, moveable, storable handles; TaskCtx coupling explicit; v2 workspace split roadmapped |
| 5 | API Design | 9 | `CapabilityReg` drop semantics explicit at type level; `#[must_use]` on advertisement; eight-handle facade minimal and orthogonal; no footguns |
| 6 | Error Handling Model | 8 | `Io` doc now distinguishes startup-vs-runtime boundary and directs callers to `err.kind()`; `#[non_exhaustive]` on all 9 public enums; numerous infallible `.expect()` are correct but no structured taxonomy between them |
| 7 | Configurability | 9 | 16+ tunable parameters with hard invariants and scaling guidelines; tuning.md makes the surface approachable; all knobs exposed without code changes |
| 8 | Language Best Practices | 9 | 0 clippy warnings at full feature matrix; `#![deny(unsafe_code)]`; `ActiveHandlerGuard` RAII idiomatic; infallible `.expect()` documented with invariant strings |
| 9 | Concurrency Correctness | 9 | Lock-Order Table documents 6 sites; `active_handlers` uses Relaxed (correct for a diagnostic counter); `ActiveHandlerGuard` covers drop/panic; no new contention |
| 10 | Resource Management | 9 | `active_bulk_handlers` via `ActiveHandlerGuard` closes the last untracked resource gap; all lifecycle patterns RAII-governed and observable |
| 11 | Semantic Correctness | 9 | Anti-entropy timing resonance eliminated; bootstrap re-trigger loop correct; LWW convergence and split-brain property-tested; scenario 04 reliable |
| 12 | Robustness | 9 | All 12 integration + 100-node scale + 21-node resilience (3 churn cycles) pass; Ed25519 fail-closed; `MAX_FRAME_BYTES`; per-peer rate-limiting |
| 13 | Security | 8 | mTLS + Ed25519 + bearer token + signed consensus + `signal_rx_from` + per-peer rate-limiting; RBAC and cluster-wide rate-limiting now formally documented v2 milestones with design sketches |
| 14 | Failure Mode Legibility | 8 | Structured error variants carry field context; `active_bulk_handlers` surfaces handler saturation; `Io` doc points to `err.kind()`; Nack reasons still not surfaced to callers |
| 15 | Performance | 9 | 151 ns set, 16 ns get; O(K) fan-out; lock-free hot paths; `active_handlers` counter is Relaxed atomic — zero hot-path cost |
| 16 | Scalability | 8 | O(N²) TCP cliff documented; `max_active_connections` + `max_inbound_frames_per_sec` mitigations operational; v2 SWIM transport and distributed rate-limiting roadmapped |
| 17 | Testability | 8 | Deterministic, injectable, no hidden global state; proptest on LWW/HLC/framing; `ConsensusPair` helper; `TaskCtx` still wired-through not injected |
| 18 | Test Architecture | 9 | 276 unit + 12 integration + 2 fuzz + scale + resilience + proptest; five-tier pyramid; all pass; full feature matrix at 0 warnings |
| 19 | Observability | 8 | Prometheus + Grafana + tracing spans + `task_count` + `active_bulk_handlers` + `/consensus/{slot}`; tuning.md checklist updated; OTEL still only in skillrunner |
| 20 | Debuggability | 8 | `/consensus/{slot}` + `task_count` + `active_bulk_handlers` + `peer_drop_counts()` + KV dump + tuning.md checklist; consensus ballot state not directly inspectable |
| 21 | Operational Readiness | 9 | `/ready` + `shutdown_with_timeout` + `SyncMode::Flush` + Docker health-checks + tuning.md with 5 hard invariants + monitoring checklist includes `active_bulk_handlers` |
| 22 | Evolvability | 9 | `#[non_exhaustive]` on all 9 enums; wire version policy tested; ROADMAP v2 milestones now 7 items (RBAC + distributed rate-limiting explicitly designed); CHANGELOG comprehensive |
| 23 | Documentation | 9 | All 13 guide chapters; CONTRIBUTING.md; `error.rs` doc explains startup-vs-runtime I/O boundary; tuning.md monitoring checklist complete; sub-handle API examples consistent |
| 24 | Developer Experience | 9 | No-default-features clean; CLAUDE.md + CONTRIBUTING.md onramp; tuning invariants let operators configure without reading source; `active_bulk_handlers` observable without code changes |
| 25 | Dependency Hygiene | 9 | `tokio test-util` in `[dev-dependencies]`; `reqwest` optional; all optional deps feature-gated; `Cargo.lock` present; `--no-default-features` compiles cleanly |
| — | **Mean** | **8.8** | |

---

## 2026-06-08 — Run 14

Changes since Run 13: no code changes (`Cargo.lock`, `docs/analysis/ratings.md`, `docs/philosophy.html` are the only working-tree modifications). Today's substantive work is a documentation/theoretical expansion: `docs/philosophy.html` now carries §9 (Promise Theory Convergence — Holland scope distinction vs Burgess), §10 (Subsidiarity Principle — Ostrom polycentric governance + Olson capture dynamics + Bookchin/Öcalan mandate TTL + Dewey epistemic symmetry); seven properties (1–4 epistemic correctness, 5 Capture Resistance, 6 Mandate TTL, 7 Epistemic Symmetry) and four failure modes (I acute collapse, II chronic erosion, III internal capture, IV class entrenchment) are now articulated. `docs/internal/paper2_substrate_convergence.html` working draft exists for Paper 2a (HLKS convergence, SFI target) and 2b (capture dynamics). `docs/internal/compliance_audit_mechanics.html` added for SOC 2 / HIPAA operational mechanics. Paper 1 ("The Coordinator Trap", Nicholson 2026) is now confirmed on SSRN; source at `docs/arxiv/main.tex`. 290 unit tests pass at full feature matrix `--features tls,metrics,a2a,llm`.

| # | Dimension | Score | Notes |
|---|-----------|:-----:|-------|
| 1 | Philosophy / Coherence with Goal | 9 | Philosophy document now articulates four-tradition convergence (Holland/Hayek/Burgess/Ostrom) plus mandate TTL (Bookchin/Öcalan) and epistemic symmetry (Dewey); seven substrate properties and four failure modes formalised; implementation continues to satisfy Properties 1–4 by design and Property 6 structurally (TTL'd KV entries dissolving Layer III roles); no feature drift; theoretical grounding now deeper than implementation needs strictly require |
| 2 | Conceptual Integrity | 9 | Eight-handle facade consistent; `CapabilityReg` naming resolved; all idioms aligned; philosophy framework consistent across philosophy.html, paper2 draft, and CLAUDE.md |
| 3 | Architecture | 9 | KvStore/KvState split materialises Layer I/II boundary; namespace table enforced; gateway feature gate clean; entanglement documented with v2 roadmap; no regressions |
| 4 | Modularity | 9 | Eight independently understandable, moveable, storable handles; TaskCtx coupling explicit; v2 workspace split roadmapped |
| 5 | API Design | 9 | `CapabilityReg` drop semantics explicit at type level; `#[must_use]` on advertisement; eight-handle facade minimal and orthogonal; no footguns |
| 6 | Error Handling Model | 8 | `Io` doc distinguishes startup-vs-runtime boundary; `#[non_exhaustive]` on all 9 public enums; structured `InvalidField`/`FieldConflict`/`NodeIdMismatch` variants; `Network(String)` catch-all remains for unclassified I/O; no structured error code taxonomy |
| 7 | Configurability | 9 | 16+ tunable parameters with hard invariants and scaling guidelines in `docs/operations/tuning.md`; all knobs exposed without code changes |
| 8 | Language Best Practices | 9 | 0 clippy warnings at full feature matrix; `#![deny(unsafe_code)]`; `ActiveHandlerGuard` RAII idiomatic; infallible `.expect()` documented with invariant strings |
| 9 | Concurrency Correctness | 9 | Lock-Order Table documents 6 sites; `active_handlers` Relaxed correct for diagnostic counter; `ActiveHandlerGuard` covers drop/panic; memory ordering policy codified in CLAUDE.md |
| 10 | Resource Management | 9 | `active_bulk_handlers` via `ActiveHandlerGuard` closes the last untracked resource gap; all lifecycle patterns RAII-governed and observable |
| 11 | Semantic Correctness | 9 | Anti-entropy timing resonance eliminated (`interval-1` cooldown); bootstrap re-trigger loop correct; LWW convergence and split-brain property-tested; epidemic Paxos vs true linearizability gap acknowledged in docs |
| 12 | Robustness | 9 | All 12 integration + 100-node scale + 21-node resilience pass; 290 unit tests at full feature matrix; Ed25519 fail-closed; `MAX_FRAME_BYTES`; per-peer rate-limiting |
| 13 | Security | 8 | mTLS + Ed25519 + bearer token + signed consensus + `signal_rx_from` + per-peer rate-limiting; RBAC and cluster-wide rate-limiting are documented v2 milestones with design sketches; `compliance` feature unimplemented |
| 14 | Failure Mode Legibility | 8 | Structured error variants carry field context; `active_bulk_handlers` surfaces handler saturation; `Io` doc points to `err.kind()`; Nack reasons still not surfaced to callers |
| 15 | Performance | 9 | 151 ns set, 16 ns get; `kv_payload_size` and `capability_resolve` benchmarks; O(K) fan-out; lock-free hot paths; `active_handlers` counter is Relaxed atomic — zero hot-path cost |
| 16 | Scalability | 8 | O(N²) TCP cliff documented with cluster-size table; `max_active_connections` + `max_inbound_frames_per_sec` mitigations operational; v2 SWIM hybrid transport and distributed rate-limiting on roadmap |
| 17 | Testability | 8 | Deterministic, injectable, no hidden global state; proptest on LWW/HLC/framing; `ConsensusPair` helper; `EchoBackend` for LLM; `TaskCtx` still wired-through rather than injected |
| 18 | Test Architecture | 9 | 290 unit (full feature matrix `tls,metrics,a2a,llm`) + 12 integration + 2 fuzz + 3 overlay + 2 scale (100-node + 21-node resilience) + proptest; five-tier pyramid; all pass |
| 19 | Observability | 8 | Prometheus + Grafana + tracing spans + `task_count` + `active_bulk_handlers` + `/consensus/{slot}`; tuning.md checklist; OTEL still only in skillrunner, not core |
| 20 | Debuggability | 8 | `/consensus/{slot}` + `task_count` + `active_bulk_handlers` + `peer_drop_counts()` + KV dump + tuning.md checklist; consensus ballot state not directly inspectable from API |
| 21 | Operational Readiness | 9 | `/ready` + `shutdown_with_timeout` + `SyncMode::Flush` + Docker health-checks + `docs/operations/tuning.md` with 5 hard invariants + monitoring checklist |
| 22 | Evolvability | 9 | `#[non_exhaustive]` on all 9 public enums; wire version policy tested; ROADMAP v2 milestones now include RBAC, distributed rate-limiting, self-tuning metabolism (8–10); CHANGELOG comprehensive |
| 23 | Documentation | 9 | All 13 guide chapters; CONTRIBUTING.md; `docs/philosophy.html` now articulates four-tradition cross-domain framework with seven properties and four failure modes; Paper 1 on SSRN; Paper 2a/2b draft at `docs/internal/paper2_substrate_convergence.html`; `docs/internal/compliance_audit_mechanics.html` added; README capabilities section migration guide for flat-API → sub-handle still absent |
| 24 | Developer Experience | 9 | `cargo build --lib --no-default-features` clean; CLAUDE.md + CONTRIBUTING.md onramp; tuning invariants let operators configure without reading source; eight-handle pattern learnable from one example |
| 25 | Dependency Hygiene | 9 | `tokio test-util` in `[dev-dependencies]`; `reqwest` optional; all optional deps feature-gated; `Cargo.lock` present; `--no-default-features` compiles cleanly |
| — | **Mean** | **8.8** | |

---

## 2026-06-10 — Run 15

Changes since Run 14: v1.1.0 released (commercial-license note in `Cargo.toml`, Tathata Systems); ROADMAP v1.x gaps #7 (durable audit trail), #8 (RBAC subset), #9 (SSO/Entra) committed, plus an uncommitted 87-line **production-hardening gate** section naming four sub-gates (AuthN/Z+RBAC, tamper-evident audit, crown-jewel posture, support/SLA) as the regulated-buyer evaluation lens; **entry-volume scale test** added (`make test-scale-entries` — 30 nodes, 5 000×512 B entries, 7 phases measuring live-gossip fraction, anti-entropy sweep tail, stability, random-sample integrity, backpressure; burst and paced write modes; documented in CLAUDE.md with rationale for staying below the iptables ceiling); **`examples/coordinator_comparison.rs`** (+ plot/runner scripts) — empirical companion to Paper 2a measuring misroute rate, staleness, and decision latency for broker-mediated vs locally-resolved routing on the identical substrate; compiles clean. Verified this run: 290/290 unit tests at full feature matrix (`tls,metrics,a2a,llm`), clippy 0 warnings.

| # | Dimension | Score | Notes |
|---|-----------|:-----:|-------|
| 1 | Philosophy / Coherence with Goal | 9 | Holland/Hayek/Burgess/Ostrom synthesis intact; `coordinator_comparison.rs` turns the core thesis into a measurable experiment on the same substrate — the strongest philosophy-to-code alignment artifact yet; no feature drift |
| 2 | Conceptual Integrity | 9 | Eight-handle facade consistent; new example uses current sub-handle API (`kv()`, `service()`) idiomatically; entry-volume test follows established runner/helper conventions |
| 3 | Architecture | 9 | Three-layer model and namespace table unchanged; KvStore/KvState split intact; new work is test/example/roadmap only — no structural change, no regressions |
| 4 | Modularity | 9 | Eight independently storable handles; `TaskCtx` (22 fields) remains the documented God Object with v2 workspace-split direction; no new coupling |
| 5 | API Design | 9 | No surface changes; `CapabilityReg` drop semantics, `#[must_use]`, eight-handle orthogonality all persist; example code demonstrates the API reads naturally at ~350 lines |
| 6 | Error Handling Model | 8 | No changes; structured variants + `#[non_exhaustive]` on all 9 public enums; `Network(String)` catch-all remains; no structured error code taxonomy |
| 7 | Configurability | 9 | Entry-volume test exercises `GOSSIP_WRITER_CHANNEL_DEPTH=4096` and `GOSSIP_MAX_STORE_ENTRIES=200000` overrides with documented rationale — config surface proven tunable for volume workloads without code changes |
| 8 | Language Best Practices | 9 | 0 clippy warnings at full feature matrix verified this run; `#![deny(unsafe_code)]`; example code clean (env-var `unwrap()`s acceptable in example context) |
| 9 | Concurrency Correctness | 9 | Lock-Order Table (6 sites, no nested acquisition) unchanged; memory ordering policy intact; no new concurrency code in library |
| 10 | Resource Management | 9 | No changes; `ActiveHandlerGuard` RAII, `task_count`/`active_bulk_handlers` observability persist |
| 11 | Semantic Correctness | 9 | 290/290 tests pass; LWW convergence and cross-group split-brain property-tested; anti-entropy timing fixes persist; entry-volume test adds end-to-end convergence-completeness verification (5 000 entries, byte-count integrity sample) |
| 12 | Robustness | 9 | No changes; Ed25519 fail-closed, mutex poison recovery, frame caps, per-peer rate-limiting all persist; entry-volume test confirms no eviction/flapping at 5 000-entry volume |
| 13 | Security | 8 | Production-hardening gate section converts vague "harden it" into four concrete sub-gates with existing/in-flight/to-design inventory per gate — excellent planning legibility, but gaps #7–#9 remain unimplemented; crown-jewel posture (egress policy, at-rest encryption, threat model) identified as new work |
| 14 | Failure Mode Legibility | 8 | No changes; Nack reasons still not surfaced to callers; entry-volume test's backpressure phase gives operators an actionable `dropped_frames` → channel-depth diagnostic |
| 15 | Performance | 9 | No hot-path changes; benchmarks persist; entry-volume test adds write-throughput and convergence-latency measurement at the system level |
| 16 | Scalability | 8 | Entry-volume axis now empirically characterised (live-gossip fraction vs anti-entropy sweep tail at 5 000 entries × 30 nodes) — the second scale axis is no longer untested; O(N²) TCP topology cliff still present in v1.x with SWIM transport deferred to v2 |
| 17 | Testability | 8 | No structural changes; `TaskCtx` still wired-through rather than injected; deterministic helpers persist |
| 18 | Test Architecture | 9 | Suite now spans six tiers: 290 unit + proptest + 2 fuzz + 12 integration + 3 overlay + 3 scale variants (100-node node-count, 21-node resilience, 30-node entry-volume) — the new test covers the previously unvalidated volume axis with explicit phase structure; consensus-protocol property tests still absent |
| 19 | Observability | 8 | No changes; Prometheus + Grafana + tracing spans + `task_count` + `active_bulk_handlers` + `/consensus/{slot}`; OTEL still only in skillrunner |
| 20 | Debuggability | 8 | No changes; consensus ballot state still not directly inspectable beyond `/consensus/{slot}` |
| 21 | Operational Readiness | 9 | Entry-volume test doubles as a capacity-planning tool (paced mode simulates sustained-rate load; overrides documented in Makefile help); `/ready`, `shutdown_with_timeout`, tuning.md invariants persist |
| 22 | Evolvability | 9 | Production-hardening gate maps every security gap to a procurement-facing sub-gate with explicit existing/in-flight/to-design status — debt is not just documented but sequenced; v1.x gaps #7–#9 and v2 milestones 8–10 committed; wire policy unchanged |
| 23 | Documentation | 9 | CLAUDE.md entry-volume section explains both the what and the why-30-nodes; coordinator_comparison doc-comment ties code to Paper 2a §9; philosophy.html §9–§10 (Promise Theory, Subsidiarity) carried from Run 14; all 13 guide chapters persist |
| 24 | Developer Experience | 9 | `make test-scale-entries` with `ENTRY_COUNT`/`ENTRY_BYTES`/`WRITE_DELAY_MS`/`SCALE_ENTRIES_WORKERS` overrides and worked examples in Makefile comments; no CI config in repo remains the main gap |
| 25 | Dependency Hygiene | 9 | No dependency changes; v1.1.0 published with all optional deps feature-gated; transitive `block v0.1.6` future-incompat warning (dev-deps only) persists |
| — | **Mean** | **8.8** | |

---

## 2026-06-10 — Run 16 (M2)

**Methodology v2 rebaseline** — first run under the execution-evidence gate,
falsification quota, and calibration ledger. Scores are not comparable to the
v1 series; the step change from 8.8 to 8.1 is methodological, not regression.
Blind-scoring exemption: prior runs were unavoidably in context this session;
the blind rule applies from Run 17.

Deep-dive dimensions this run: 9 (Concurrency), 10 (Resource Mgmt),
11 (Semantic Correctness), 12 (Robustness), 18 (Test Architecture).
Next run by rotation: 1–5.

Execution evidence this run: 302/302 unit tests at full feature matrix
(`tls,metrics,a2a,llm`); clippy 0 warnings (`-D warnings`, full matrix);
`--no-default-features` build; all examples build; 12/12 integration
scenarios; 100-node scale 5/5 with 0 dropped frames (fresh Docker VM);
21-node resilience 10/10; 30-node entry-volume 6/6 (100% live-gossip
fraction); 3 falsification probes executed.

Changes since Run 15: epoch-leased commitments (`committed_lease_secs` +
`consensus/lease/{slot}` + lease-aware readers); commit-conflict tripwire
(`SystemStats::commit_conflicts`, `/stats`); proposer-side clobber guard;
consensus listener registration race fixed (synchronous handler
registration); LWW equal-timestamp tiebreak (`lww_wins`); philosophy §5a
(Anderson / symmetry breaking / corrected litmus); CLAUDE.md hop-TTL vs
evaporation correction + Layer III invariant-posture section; namespace
table promise-strength note.

### Findings

- **Major (fixed in-run)** — *Semantic Correctness*: LWW diverged permanently
  on equal-timestamp concurrent data writes; first arrival won on each node,
  and the value-blind anti-entropy digest (key ⊕ timestamp) made the
  divergence undetectable. Probe: `lww_equal_timestamp_concurrent_data_converges`
  (initially failed: node1=`from-a`, node2=`from-b`). Fixed with deterministic
  data-vs-data tiebreak in `lww_wins`; probe kept as regression test;
  calibration ledger entry recorded. Dimension capped at 6 this run per M2.
- **Probe passed** — *Robustness*: live agent survived garbage bytes, a 4 GiB
  length-prefix announcement, and zero-length frames on the gossip port; no
  dead shards, fully serviceable after (`probe_garbage_on_gossip_port_survives`,
  kept).
- **Probe passed** — *Resource Management*: `shutdown_with_timeout` drains all
  tracked tasks to 0 and releases the gossip port for rebinding
  (`probe_shutdown_drains_tasks_and_releases_port`, kept).
- **Test-infra (not a dimension cap)** — repeated same-day 100-node rounds
  degrade Docker VM formation (PASS → 80/100 → 97/100); fresh engine restart
  → 5/5 with 0 drops. Documented in CLAUDE.md.

| # | Dimension | Score | Notes |
|---|-----------|:-----:|-------|
| 1 | Philosophy / Coherence with Goal | 8 | Reading-capped (M2). §5a closes the Layer III derivation gap (Anderson level-laws, corrected litmus #3, mandate-TTL-for-decisions); lease feature is the philosophy applied to code; `coordinator_comparison` exists but was not executed this run |
| 2 | Conceptual Integrity | 8 | Reading-capped. Lease deliberately reuses the read-side evaporation convention (`CapEntry::is_fresh` pattern) — idiom-consistent; tripwire follows the diagnostics-counter idiom |
| 3 | Architecture | 8 | Reading-capped. Tripwire/lease kept wholly in Layer III (no substrate write-guard — dependency direction preserved and now documented in CLAUDE.md §Layer III invariant posture); `--no-default-features` build executed |
| 4 | Modularity | 8 | Reading-capped. Eight handles unchanged; `commit_conflicts` added to TaskCtx (the God Object grows by one diagnostic field; v2 split still roadmapped) |
| 5 | API Design | 8 | Reading-capped. `committed_lease_secs: Option<u64>` opt-in with `None` default preserves behaviour; `consensus_rx` raw-view vs `consensus_get` lease-aware split documented |
| 6 | Error Handling Model | 7 | `Network(String)` catch-all and no structured error-code taxonomy persist (v1 finding, unchanged) |
| 7 | Configurability | 8 | Suites exercised env-override surface (`GOSSIP_WRITER_CHANNEL_DEPTH` etc.) but the config surface was not deep-dived this run |
| 8 | Language Best Practices | 9 | **Evidence:** clippy 0 warnings at `-D warnings`, full feature matrix, executed this run; `#![deny(unsafe_code)]`; `lww_wins` extracted as shared comparator rather than duplicated |
| 9 | Concurrency Correctness | 8 | Deep-dive. Listener registration race found and fixed this session (calibration ledger); fix verified by 302 unit + 12/12 integration re-runs. Capped at 8 as the ledger now shows this dimension's artifacts (lock tables, ordering policy) did not predict the race |
| 10 | Resource Management | 9 | Deep-dive + probe. **Evidence:** `probe_shutdown_drains_tasks_and_releases_port` passed (task_count → 0, port rebindable); `ActiveHandlerGuard`/RAII patterns re-verified |
| 11 | Semantic Correctness | 6 | Deep-dive + probe. **Capped: Major finding this run** (LWW equal-timestamp divergence, see Findings). Fixed in-run with deterministic tiebreak + regression test; 302 unit incl. convergence tests pass — expect recovery next run with sustained evidence |
| 12 | Robustness | 9 | Deep-dive + probe. **Evidence:** `probe_garbage_on_gossip_port_survives` passed; 21-node resilience 10/10 re-executed post-LWW-change; Ed25519 fail-closed and frame caps re-verified by suite |
| 13 | Security | 8 | tls-feature unit tests executed (signing/verification paths); tripwire adds violation legibility; gaps #7–#9 (audit, RBAC, SSO) remain unimplemented v1.x line items |
| 14 | Failure Mode Legibility | 8 | Tripwire `warn!` + `commit_conflicts` counter + `lease_expired` on `/consensus/{slot}` all tested this run; consensus Nack reasons still not surfaced to callers |
| 15 | Performance | 8 | Capped: no benchmarks executed this run. `lww_wins` adds a byte-compare only on the equal-timestamp path (nonce-deduped duplicates never reach it) — reading-verified only |
| 16 | Scalability | 8 | **Evidence:** 100-node 5/5 with 0 dropped frames (fresh VM) + 30-node entry-volume 6/6 executed this run; held at 8 because the O(N²) TCP ceiling remains the structural v1.x constraint (SWIM transport is v2) |
| 17 | Testability | 8 | Probes were straightforward to write against public API + test helpers (good sign); `TaskCtx` still wired-through rather than injected |
| 18 | Test Architecture | 9 | Deep-dive. **Evidence:** all tiers executed this run — 302 unit (3 new probes now permanent) + proptest + 2 fuzz targets (built) + 12/12 integration + 3 scale suites; falsification quota now institutionalised in the methodology |
| 19 | Observability | 8 | `commit_conflicts` on `/stats`; suites exercised health/ready/stats endpoints; OTEL still skillrunner-only |
| 20 | Debuggability | 8 | `/consensus/{slot}` now distinguishes never-committed from lease-expired; ballot internals still not inspectable |
| 21 | Operational Readiness | 9 | **Evidence:** all four Docker suites executed this run end-to-end against `/health`, `/ready`, `/stats`; VM-fatigue diagnostic procedure added to CLAUDE.md |
| 22 | Evolvability | 8 | Reading-capped. CHANGELOG discipline maintained (3 Added + 2 Fixed this run, incl. rolling-upgrade note for the LWW tiebreak); wire version unchanged |
| 23 | Documentation | 8 | Reading-capped. Hop-TTL vs evaporation conflation fixed (calibration ledger); §5a + namespace lease row + Layer III posture section added; guide chapters not re-verified this run |
| 24 | Developer Experience | 8 | Reading-capped. Builds + suites green from clean state; no CI config in repo remains the standing gap |
| 25 | Dependency Hygiene | 8 | `--no-default-features` build executed; no dep changes; transitive `block v0.1.6` future-incompat (dev-deps only) persists |
| — | **Floor (lowest 3)** | **6, 7, 8** | Semantic Correctness (6, capped by finding), Error Handling (7), ten dimensions tied at 8 — Security is the most actionable of them |
| — | Mean (continuity footnote) | 8.1 | not a target; M2 step change — see methodology note |

## 2026-06-11 — Run 17 (M2)

Deep-dive dimensions this run (by rotation from Run 16): 1 (Philosophy), 2
(Conceptual Integrity), 3 (Architecture), 4 (Modularity), 5 (API Design).
Next run by rotation: 6–10.

**Process disclosure.** The scoring agent had read Runs 15–16 earlier in the
same working session (for the coordinator_comparison analysis), so the blind
rule is compromised this run; scores were written from this run's own
evidence before re-opening this file, but prior exposure existed.

Diff since Run 16: `mycelium-tuple-space` workspace companion crate (Linda-
style pull buffer: single-lock store, 4-record WAL with atomic
CompleteRecord + epoch'd compaction, primary/secondary replication +
emergent promotion, Auto election with lowest-candidate tie-break,
sys/tuple metrics + pressure pheromone, HTTP gateway, py/ts SDKs);
integration scenario 13; `coordinator_comparison` demoted (doc-comment
scope warning); Paper 1 §3.3/§8/§9.5 and Paper 2a §Two Grades/§Promise
Reading/§Homogenisation Corollary/§Empirical rewrite; two pre-existing
no-default-features warnings fixed in `bulk.rs`. Working tree only — not
yet committed.

Execution evidence this run: companion suite 27 tests across 6 binaries
incl. 3 fresh falsification probes (re-run post-fix); core 304/304 at full
feature matrix (`tls,metrics,a2a,llm`); clippy `-D warnings` clean on both
crates, all targets, both feature configs; `--no-default-features` build 0
warnings; `make test` 13/13 Docker scenarios incl. new scenario 13 (ran
earlier this run, pre-fix; the fix is companion-internal and the companion
suite was re-run post-fix); release perf smoke `wal_throughput_smoke`:
3.79M transient put/take pairs/s, 215k WAL-backed put/take/ack cycles/s.

### Findings

- **Minor — Observability (#19, capped at 6).** `StageState::waiters_count`
  underflowed to ~4.29e9 (u32 wrap) whenever a parked take timed out and a
  later put skipped its dead sender: the timeout path and the dispatch pop
  path both decremented. The garbage value fed `sys/tuple/...waiters`, the
  depth RPC, and `/api/tuple`. Found by new probe
  `metrics_accounting_identity`; fixed in-run (counter is now a strict
  mirror of deque membership — decrement only at pop, under the stage
  lock; underflow structurally impossible) and the probe stays as a
  regression test. No calibration-ledger entry: the bug was introduced and
  caught within the same run's diff — it never existed under a prior ≥ 8
  score. First run in which the falsification quota caught a defect before
  it shipped.

Falsification probes (3, against the three highest provisional scores):
`wal_torn_tail_recovery` (#11 — WAL truncated at EVERY byte offset inside
the final record; recovery exact at all offsets; log re-appendable — PASS);
`metrics_accounting_identity` (#19 — chaotic lifecycle then exact-zero
quiescence accounting — FAIL → finding above → fixed → PASS);
`shutdown_flushes_wal_and_restart_recovers` (#21 — full lifecycle drill:
traffic in four item states, shutdown, restart over same WAL; exactly the
unacked set recovers, acked items do not resurrect — PASS). All three are
permanent regression tests (`store.rs` tests, `tests/probes.rs`).

| # | Dimension | Score | Notes |
|---|-----------|:-----:|-------|
| 1 | Philosophy / Coherence with Goal | 9 | Deep-dive. The run's central act was adversarial: `coordinator_comparison` (Run 15's "strongest alignment artifact") was invalidated as a tautology measuring its own staleness arithmetic — logged here as the falsification the quota intends. Its replacement is execution-backed: the pull thesis is now a running artifact (scenario 13 in 13/13 Docker suite; failover drill; both papers reframed around two-grades/promise/homogenisation). Evidence: scenario 13 PASS, `failover.rs` suite, `probes.rs` drill |
| 2 | Conceptual Integrity | 8 | Deep-dive. Companion reuses the read-side evaporation idiom (pressure pheromone, capability election), namespace-table convention extended (`tuple/inflight`, `sys/tuple`), error/`#[non_exhaustive]`/handle idioms match core; one deliberate divergence documented in-code (flat capability names — parser rejects `/` in segments). Reading-capped |
| 3 | Architecture | 9 | Deep-dive. Companion-crate boundary is compiler-enforced: public API only, zero core changes required (`with_http_routes` was already public); two architecture conflicts found and resolved by design, not patching (sys/load opacity would false-trigger promotion → own-prefix pheromone; cap-key `/` limit → flat names). Evidence: full-matrix builds + `--no-default-features` 0 warnings executed |
| 4 | Modularity | 8 | Deep-dive. TaskCtx God Object untouched by the entire companion (the strongest modularity evidence yet is what did NOT need changing); mirror state confined to `TupleSpace`; no new cross-handle coupling. Reading-capped |
| 5 | API Design | 8 | Deep-dive. Four-role `TupleRole`, `BackpressureMode`, `#[non_exhaustive]` `TupleError`, Arc-handle pattern consistent with core; `Client` role added when Auto became electing (plan gap); `local_depth` vs `depth` split documented. Reading-capped |
| 6 | Error Handling Model | 7 | `Network(String)` catch-all persists in core; `TupleError::Rpc(String)` reproduces the same untyped-catch-all gap in new code |
| 7 | Configurability | 8 | `TupleConfig` 13 knobs incl. `cap_refresh` as single-knob test cadence (proven across 4 test files); env overrides for scenario 13 (`MYCELIUM_TUPLE_ROLE/NS`); reading-capped |
| 8 | Language Best Practices | 9 | **Evidence:** clippy `-D warnings` clean this run — both crates, all targets, both feature configs; `#![deny(unsafe_code)]` in companion; the two remaining `unwrap()`s are length-guarded slice converts |
| 9 | Concurrency Correctness | 7 | Two consecutive runs with concurrency-accounting defects (Run 16 listener race; Run 17 waiters double-decrement — found by probe, fixed). Lock-order documented in `store.rs` header (WAL→stage→inflight) and `concurrent_100`/storm paths pass, but the ledger pattern warrants structural skepticism, not an 8 |
| 10 | Resource Management | 8 | Companion tasks are abort-on-shutdown and not in agent `task_handles` (documented); shutdown drill verifies WAL flush + clean agent stop but does not assert task/fd counts for companion tasks — that residual gap keeps this at 8 |
| 11 | Semantic Correctness | 9 | **Evidence:** `wal_torn_tail_recovery` passed at every truncation offset; WAL replay/inflight/compaction/epoch-chunk suite; atomic CompleteRecord crash-window test pair; `concurrent_100` exactly-once; core 304/304 incl. Run-16 LWW regression. Recovered from Run 16 cap with sustained + new evidence |
| 12 | Robustness | 8 | Torn-tail recovery proven; record decoder fully bounds-checked (`.get()` throughout) but UNFUZZED while parsing peer-supplied bytes (replicate handler) — gap named, see #18 |
| 13 | Security | 7 | New intra-cluster mutation surface: any peer can `ack`/`replicate` against a tuple namespace (consistent with the documented operator-owned-cluster trust model, but unauthenticated and unreviewed); core tls/signing untouched |
| 14 | Failure Mode Legibility | 8 | 9 tracing sites in companion (promotion warn names ns; requeue warns per id; replication-unconfirmed warns per node); `TupleError` Display strings actionable; pressure value carries depth+timestamp. No log-assertion tests → 8 |
| 15 | Performance | 9 | **Evidence:** release smoke this run — 3.79M transient pairs/s, 215k WAL cycles/s vs plan target 50k (4.3×, with WAL). Hot path measured, not asserted: WAL page-cache append under mutex costs ~4.6 µs/cycle amortised |
| 16 | Scalability | 7 | Single-primary ceiling (documented, sharding designed not built); per-claim inflight KV key gossips cluster-wide (metadata-only but O(cluster) chatter per item); per-put secondary resolve+spawn; no tuple-space entry-volume test yet |
| 17 | Testability | 9 | **Evidence:** 19 store tests run in 0.05 s with zero cluster (transient mode + temp WAL); all three probes were writable in minutes against public/`pub(crate)` seams — the quota itself is the testability measurement |
| 18 | Test Architecture | 8 | Pyramid: 19 unit + 8 e2e (5 files) + scenario 13 + 3 probes-as-regressions + ignored perf smoke. Gaps: no fuzz target for the WAL/replicate record decoder (core has fuzz for its own decoders; the new adversarial surface is uncovered), no property tests on replay |
| 19 | Observability | 6 | **Capped: finding this run** (waiters_count underflow shipped garbage to sys/tuple metrics, depth RPC, /api/tuple). Fixed in-run + regression kept; surface itself is rich (role/depth/inflight/totals/p99/pressure + /api/tuple aggregation verified in scenario 13 phase IV) — expect recovery next run with sustained evidence |
| 20 | Debuggability | 8 | Inflight keys + pressure pheromone are operator-readable JSON in plain KV; `/api/tuple` one-call cluster view; WAL has test-only reader but no operator dump tool |
| 21 | Operational Readiness | 9 | **Evidence:** lifecycle drill probe passed (shutdown fsync → restart → exact unacked-set recovery, four item states); failover promotion drill; 13/13 scenarios; promotion latency documented (≈3× cap_refresh) |
| 22 | Evolvability | 7 | Companion WAL has no format-version/magic header: an unknown record kind reads as a torn tail → silent truncation of old logs on any future format change (upgrade data-loss hazard, named improvement target); core wire policy (v11/v10 window) unchanged |
| 23 | Documentation | 8 | CLAUDE.md §TupleSpace (key facts for future sessions), 952-line plan doc now implemented, both papers updated, crate-level rustdoc with passing doctest; gaps: no guide chapter; `paper2a/main.html` stale vs `main.tex` |
| 24 | Developer Experience | 8 | `cargo -p mycelium-tuple-space` workflows fast (unit suite 0.05 s); single-knob test cadence; still no CI config in repo |
| 25 | Dependency Hygiene | 9 | **Evidence:** `--no-default-features` 0-warning build executed this run (was 3 warnings — fixed in `bulk.rs`); companion adds zero new transitive deps (papaya/parking_lot/bytes/base64 all already in core's tree; gateway deps optional); `block v0.1.6` (dev-deps) persists |
| — | **Floor (lowest 3)** | **6, 7, 7** | Observability (6, capped by finding); five-way tie at 7 — Error Handling, Concurrency Correctness, Security, Scalability, Evolvability; Concurrency and Evolvability are the most actionable |
| — | Mean (continuity footnote) | 8.0 | not a target; see M2 preamble |

## 2026-06-11 — Run 18 (M2)

Deep-dive dimensions this run (by rotation from Run 17): 6 (Error Handling),
7 (Configurability), 8 (Language Best Practices), 9 (Concurrency Correctness),
10 (Resource Management). Next run by rotation: 11–15.

Second run today; cadence gate passed because the diff since Run 17 is
material and distinct (f9bd7e0: tuple-space WAL format header + counter-
invariant property test; d804d2a: Cargo.lock). Blind rule: only Run 16/17
deep-dive rotation header lines were read before scoring; full tables read
after provisional scores were written.

**Score-targeting disclosure (per M2):** f9bd7e0's commit message explicitly
references "Run-17 improvement targets (2) and (3)". The work is substantive —
a named upgrade data-loss hazard closed with refusal semantics and tests, and
a model-based property test that re-finds the Run-17 underflow when its fix is
reverted — and scores moved only where this run's own probes independently
verified the artifacts (#22 torn-header probe, #19 property test executed).
Flagged here rather than taken on faith.

Execution evidence this run: core 305/305 unit tests at full feature matrix
(`tls,metrics,a2a,llm`, includes 2 new probe regressions); tuple-space 24/24
(`--features gateway`, includes new torn-header probe +
`counters_match_reference_model` 64-case property run); clippy `-D warnings`
clean at full matrix (lib+tests) AND `--no-default-features`; 4 falsification
probes executed (1 deep-dive + 3 quota), one of which FAILED and produced the
Major finding below.

### Findings

- **Major — Concurrency Correctness (#9, capped ≤ 6, scored 5).**
  `apply_and_notify` maintains the prefix index (and `cap_ns_index`) *after*
  the store CAS, outside any serialisation with it. When a tombstone (lower
  ts) and a live insert (higher ts) race on the same key — reachable whenever
  a delete races a rewrite arriving on different shards/tasks — both CAS in
  ts order, but if the tombstone thread's `prefix_index_remove` lands after
  the insert thread's `prefix_index_insert`, the store holds a live key the
  index has lost. `scan_prefix` (and every capability-resolution path through
  `cap_ns_index`) then silently misses the live key. The divergence is
  permanent until the key is rewritten: anti-entropy re-applies the same
  (key, ts), loses LWW, `changed` stays false, and the index is never
  repaired. Evaporating `cap/` keys self-heal on the next heartbeat; `grp/`,
  `consensus/`, and user keys do not. The `PrefixIndex` doc comment claims
  the index is "updated atomically in apply_and_notify" — it is not.
  Reproduction: probe `prefix_index_consistent_under_tombstone_insert_race`
  (src/store.rs) lost 86 of 100 000 keys on first execution (Apple Silicon).
  NOT fixed in-run — the fix requires serialising index maintenance with the
  CAS (e.g. per-bucket reconcile-under-lock that re-reads the store entry), a
  hot-path design decision worth deliberate review; the probe is kept as an
  `#[ignore]`d canary documenting the bug, to be un-ignored as the regression
  gate when fixed. Calibration ledger entry recorded (bug existed since
  2026-05-18 under 8–9 scores in Runs 9–16). Third consecutive M2 run in
  which a probe found a real concurrency defect.
  *Addendum (same day, post-run, at user request):* fixed by replacing
  update-derived index maintenance with a stripe-locked reconcile
  (`KvStore::index_stripes`, 64 stripes; re-read the stored entry, set
  membership in `prefix_index`/`cap_ns_index`/`peer_localities` to match it).
  Canary un-ignored — passed 6/6 consecutive executions post-fix (failed at
  86/100 000 pre-fix); new 8-thread mixed-churn test
  `secondary_structures_consistent_under_concurrent_churn` covers all three
  secondary structures in both directions (improvement target #2); CHANGELOG
  [Unreleased] gained the tuple-space ship + WAL header + this fix
  (improvement target #3); lock-order table row 7 added to CLAUDE.md.
  Core suite 307/307 and clippy `-D warnings` clean in both feature configs
  after the change; companion suite green. Scores above are the run's
  snapshot and stand unmodified; Run 19 verifies recovery with sustained
  evidence.
  *Addendum 2 (same day, post-run, at user request):* systematic sweep of
  the "lock-free op + unserialised derived effect" family across all 34
  papaya mutation sites in `src/`. Four further defects found and fixed,
  each with a regression test: (1) **signal handler registration panicked
  under contention** — single-use `slot.take().expect()` inside a papaya
  `compute` closure, which papaya re-invokes on CAS retry; reproduced
  instantly by `concurrent_same_kind_signal_registration_does_not_panic`
  (6 threads panicked pre-fix), fixed by cloning per invocation. (2)
  **Concurrent same-key `set_with_min_acks` callers starved each other** —
  single-slot tracker overwrite + unconditional cleanup deleting the other
  caller's tracker; now a copy-on-write tracker list with identity-checked
  removal (`kv_quorum::{install_tracker, remove_tracker}`), Rust API and
  HTTP gateway. (3) **LLM prompt-skill races** — double dispatch-loop spawn
  via `is_empty()` check-then-act (now atomic swap) and stale-handle drop
  deleting a newer re-registration (now `Arc::ptr_eq`-conditional). (4)
  **A2A cleanup sweep evicted live tasks** — collect-then-unconditional-
  remove (now conditional `compute`). Sites verified CORRECT by the same
  sweep: `get_or_spawn_writer`, `ShardedSeen::evict_below`, peer eviction,
  subscriptions/prefix-watcher sweeps, roster-cache generation ordering.
  CLAUDE.md gains a "Lock-free mutation rules" section codifying the two
  idioms. **Process finding:** the dim-24 note "still no CI config" carried
  through Runs 16–18 was stale — `.github/workflows/ci.yml` has existed
  since 2026-06-03; its actual gaps (clippy not at full feature matrix, no
  tuple-space job, no `--no-default-features` job) are now closed, all
  gates verified locally. Second stale-carried-note instance this run
  (after dim-6 `Network(String)`): Run 19 should re-verify carried notes,
  not just carried scores. Post-sweep gates: core 311/311 (default 295/295),
  companion 24/24 + clippy `--all-targets`, clippy `-D warnings` at full
  matrix and `--no-default-features`.
- **Probe passed — Error Handling / Resource Management (#6/#10):**
  `test_lifecycle_error_contract_and_task_drain` (new, kept) — second
  `start()` returns `AlreadyRunning`; `start()` after shutdown returns
  `Shutdown`; `shutdown_with_timeout` drains `task_count` to exactly 0. The
  documented lifecycle contract had no prior test.
- **Probe passed — Evolvability (#22):** `wal_torn_header_refused_untouched`
  (new, kept) — a 7-byte header torn between magic and version is refused
  with the file byte-identical, closing the boundary case the shipped header
  tests did not cover.
- **Probe passed — Language Best Practices (#8):** clippy `-D warnings` clean
  at `--no-default-features` (a config the standard gate does not run);
  production `unwrap()/expect()` audit across 8 core files found 6 sites, all
  invariant-guarded with messages; `unsafe` appears only in one SAFETY-
  commented test-only env-var guard under `#[allow(unsafe_code)]`.
- **Process note:** Runs 16–17 carried the dim-6 note "`Network(String)`
  catch-all persists" although it was eliminated in v1.1.0 (2026-06-07,
  replaced by `FrameTooLarge`/`UnsupportedWireVersion`). Carried notes can
  rot; this deep-dive corrects it. The honest residual gap is the companion's
  `TupleError::Rpc(String)`.

| # | Dimension | Score | Notes |
|---|-----------|:-----:|-------|
| 1 | Philosophy / Coherence with Goal | 9 | carried (v17) |
| 2 | Conceptual Integrity | 8 | carried (v17) |
| 3 | Architecture | 9 | carried (v17) |
| 4 | Modularity | 8 | carried (v17) |
| 5 | API Design | 8 | carried (v17) |
| 6 | Error Handling Model | 8 | Deep-dive. `error.rs` is exemplary: per-variant recoverability docs, `#[non_exhaustive]`, explicit runtime-IO-absorption policy; 10 typed sub-enums across subsystems; prior runs' `Network(String)` note was stale (fixed in 1.1.0). **Evidence:** lifecycle error-contract probe passed. Held at 8 by `TupleError::Rpc(String)` |
| 7 | Configurability | 8 | Deep-dive. 51 fields, all `validate()`d (range + cross-field conflicts), 24+ `GOSSIP_*` env overrides, TOML round-trip + env-override + validation tests in the executed suite; 8 well-documented feature flags. Wart: `GOSSIP_GOSSIP_CHANNEL_CAPACITY` double prefix |
| 8 | Language Best Practices | 9 | Deep-dive. **Evidence:** clippy `-D warnings` clean this run at full matrix (lib+tests) and `--no-default-features`; `#![deny(unsafe_code)]`; 6 production expect sites all length/filter-guarded |
| 9 | Concurrency Correctness | 5 | Deep-dive. **Capped: Major finding this run** (prefix-index/store divergence under tombstone-insert race — see Findings). Third consecutive run with a confirmed concurrency defect; ledger now has 2 entries for this dimension — the lock-order table and ordering policy keep not predicting the races that exist outside the locks |
| 10 | Resource Management | 8 | Deep-dive. 9 RAII `Drop` guards (AliveGuard, ListenerGuard, OpacityDropGuard, StagedGuard, ActiveHandlerGuard, LockGuard…); JoinSet swap-out drain with abort fallback. **Evidence:** lifecycle probe re-verified task_count → 0. Run-17 companion-task accounting gap stands |
| 11 | Semantic Correctness | 8 | Down from 9: `kv_scan_prefix` completeness is compromised by the #9 race (cross-ref; capped only on #9). Tuple-space side strengthened: `counters_match_reference_model` (64 random op sequences vs reference model, every-step checks) executed this run |
| 12 | Robustness | 8 | carried (v17) |
| 13 | Security | 7 | carried (v17) |
| 14 | Failure Mode Legibility | 8 | carried (v17); WAL refusal errors name both versions and the path (verified by probe) |
| 15 | Performance | 9 | carried (v17, release perf smoke) |
| 16 | Scalability | 7 | carried (v17) |
| 17 | Testability | 9 | carried (v17); corroborated — all 4 of this run's probes were writable in minutes against public/`pub(crate)` seams |
| 18 | Test Architecture | 7 | Down from 8: the #9 probe exposed that core had ZERO concurrent-stress coverage of `apply_and_notify`/index consistency — the canary is the first — and that hole hosted a Major bug for 3+ weeks. Credit: counter-model property test closes Run-17's named property-test gap (executed this run) |
| 19 | Observability | 8 | Recovered from Run-17 cap (waiters underflow) with the promised sustained evidence: **counters_match_reference_model** checks every monitoring counter against a reference model after every step, executed this run; underflow canaries structural. Core SystemStats counters still have no equivalent model test |
| 20 | Debuggability | 8 | carried (v17) |
| 21 | Operational Readiness | 9 | carried (v17, lifecycle drill + 13/13 scenarios) |
| 22 | Evolvability | 8 | Up from 7: Run-17's named hazard closed — MTSWAL magic + u16 version, future-version/foreign files refused byte-untouched, header survives compaction, replay cursor clamps past header (f9bd7e0). **Evidence:** 4 shipped header tests + this run's torn-header probe all executed. Held from 9: tuple-space ship and WAL header absent from CHANGELOG [Unreleased] |
| 23 | Documentation | 8 | carried (v17) |
| 24 | Developer Experience | 8 | carried (v17); still no CI config |
| 25 | Dependency Hygiene | 9 | **Evidence:** clippy `--no-default-features` executed this run; diff adds only `proptest` as companion dev-dep (already in core's dev tree — zero new transitive deps); `block v0.1.6` future-incompat (dev-deps) persists |
| — | **Floor (lowest 3)** | **5, 7, 7** | Concurrency Correctness (5, capped by finding); Security, Scalability, Test Architecture tied at 7 — Concurrency is the actionable one: fix the index race |
| — | Mean (continuity footnote) | 8.0 | not a target; see M2 preamble |

## 2026-06-11 — Run 19 (M2)

Deep-dive dimensions this run (by rotation from Run 18): 11 (Semantic
Correctness), 12 (Robustness), 13 (Security), 14 (Failure Mode Legibility),
15 (Performance). Next run by rotation: 16–20.

**Process disclosure.** The blind rule is fully compromised this run: the
scoring agent authored the entire diff under evaluation (the Run-18
race-family sweep) in the same session, with Run 18's table in context.
Both fix commits reference M2 run findings by name (flagged per M2's
score-targeting rule). Mitigation: scores moved only where this run's own
probes or suites produced independent evidence, and the run's two new
findings are both AGAINST dimensions that benefited from no fix work.
Third run today; cadence gate passed on a material, distinct diff
(787 insertions: six concurrency fixes + tests + CI expansion).

Execution evidence this run: core 312/312 at full feature matrix on the
security-bumped lockfile (bytes 1.11.1 / tracing-subscriber 0.3.20 /
tokio 1.46.1); tuple-space 24/24; clippy `-D warnings` clean at full
matrix and `--no-default-features`; remote CI 3/3 jobs green (run
27335333481 — first run of the expanded workflow); `cargo audit`
(2 vulnerabilities found → fixed → clean); `cargo tree` dependency-
direction probe; release perf smoke `apply_and_notify_throughput_smoke`:
635k writes/s single-thread, 2.28M/s 8-thread 64-hot-key contention.

### Findings

- **Major — Semantic Correctness (#11, capped ≤ 6, scored 6).**
  `Hlc::observe` absorbs remote physical time with an unbounded `max`,
  deviating from the drift-bound requirement of the cited Kulkarni et al.
  2014 algorithm. One peer with a skewed clock (NTP failure; or hostile in
  a non-TLS cluster) drags every node's HLC forward — `max` never decays,
  so there is no recovery until the wall clock catches up. Downstream
  impact: read-side evaporation computes `now.saturating_sub(written)`,
  which reads 0 for future stamps, so capability evaporation — the
  substrate's failure detector, on which tuple-space secondary promotion
  also depends — is silently suspended for the full drift duration (a
  7-day-skewed peer ⇒ up to 7 days without failure detection,
  cluster-wide, no log line). NOT fixed in-run: a drift bound changes
  accept/reject behaviour on the gossip path and deserves a deliberate
  design pass (clamp vs reject, configurable bound, warn-on-large-drift).
  Canary: `hlc::tests::observe_bounds_remote_clock_drift` (`#[ignore]`d,
  flips when the bound lands); impact documented by
  `future_stamped_entry_outlives_its_evaporation_window_by_the_drift`
  (capability.rs — asserts current wrong behaviour, inverted on fix).
  Calibration ledger entry 5. Top improvement target.
  *Addendum (same day, post-run, at user request):* all three improvement
  targets addressed. (1) Drift bound implemented — `Hlc::observe` clamps
  remote physical time to `wall_now + GossipConfig::max_clock_drift_ms`
  (default 5 min, `0` disables, `GOSSIP_MAX_CLOCK_DRIFT_MS` env), with a
  rate-limited `warn!` naming the drift (the #14 legibility fix); freshness
  is now a SYMMETRIC window (`CapEntry`/`ReqEntry::is_fresh` quarantine
  stamps further than 3× in the future), so failure detection no longer
  depends on sender clock sanity even for stamps inside the drift bound.
  Canary `observe_bounds_remote_clock_drift` un-ignored and passing; impact
  test inverted to `future_stamped_entry_is_quarantined_not_fresh`; module
  doc records the documented trade-off (out-of-bound stamps waive
  local-write dominance; store-level rejection deferred to the next
  wire-policy pass). (2) CI gains a `cargo audit` job (taiki-e prebuilt
  install) plus a weekly Monday cron — advisories land without code pushes,
  which is exactly how the Run-19 finding escaped push-triggered gates.
  (3) Bincode succession decided and recorded as ROADMAP v2 milestone 11:
  stay-and-monitor short-term (lockfile-pinned, audit-job-tracked);
  hand-rolled fixed-layout codec at the next WIRE_VERSION bump (v12) —
  `WireMessage` is a small closed enum whose layout is already hand-managed,
  so owning the codec removes the unmaintained dependency without a
  dedicated wire break. Post-fix gates: core 315/315 full matrix,
  tuple-space 24/24, clippy `-D warnings` clean both configs, ci.yml
  YAML-validated. Scores stand as the run's snapshot; Run 20 verifies #11
  recovery with sustained evidence.
- **Major — Dependency Hygiene (#25, capped ≤ 6, scored 6, fixed in-run).**
  `cargo audit` probe found 2 vulnerabilities: bytes 1.10.1
  (RUSTSEC-2026-0007, integer overflow in `BytesMut::reserve` — called by
  `read_frame` on the wire path; exploitability mitigated by the 10 MiB
  frame cap upstream) and tracing-subscriber 0.3.19 (RUSTSEC-2025-0055,
  ANSI-escape log poisoning). Fixed in-run via semver lock bumps (+ tokio
  1.46.1 for the RUSTSEC-2025-0023 broadcast unsoundness warning); full
  gate re-run green on the new lockfile; audit now reports 0
  vulnerabilities. Residual: 5 unmaintained-crate warnings, most notably
  **bincode — the wire-format codec — is unmaintained** (RUSTSEC-2025-0141);
  flagged as a roadmap-level concern, not a quick fix. Calibration ledger
  entry 6. CI gains no audit job yet — adding one is the obvious follow-up.
- **Probe passed — Performance (#15):** `apply_and_notify_throughput_smoke`
  (release): 635k writes/s single-thread distinct keys; 2.28M/s under
  8-thread 64-hot-key contention — the Run-18 stripe-lock reconcile scales
  with threads and its hot-path cost is immaterial at gossip rates. Kept as
  an `#[ignore]`d perf smoke.
- **Probe passed — Architecture (#3):** `cargo tree -e normal` confirms the
  companion-crate boundary (0 tuple-space references in core's normal
  deps; exactly 1 core dep in the companion — the documented dev-dep cycle
  is dev-only). Bonus live demonstration: the layer-boundary enforcement
  test `layer1_modules_do_not_reference_higher_layers` caught this run's
  own canary when it was first placed in hlc.rs referencing a capability
  type — the boundary is mechanically enforced, including over comments.

| # | Dimension | Score | Notes |
|---|-----------|:-----:|-------|
| 1 | Philosophy / Coherence with Goal | 9 | carried (v17) |
| 2 | Conceptual Integrity | 8 | carried (v17) |
| 3 | Architecture | 9 | **Evidence:** cargo-tree dependency-direction probe passed; layer-enforcement test demonstrated live (caught the canary's misplacement this run) |
| 4 | Modularity | 8 | carried (v17) |
| 5 | API Design | 8 | carried (v17) |
| 6 | Error Handling Model | 8 | carried (v18) |
| 7 | Configurability | 8 | carried (v18) |
| 8 | Language Best Practices | 9 | **Evidence:** clippy `-D warnings` clean this run, full matrix + `--no-default-features`, on the bumped lockfile |
| 9 | Concurrency Correctness | 7 | Up from 5 citing verifiable artifacts: six family fixes + six regression tests in-suite (312/312), remote CI green, sweep verified 7 sites already correct. Two ledger entries keep this below 8 until a clean deep-dive run |
| 10 | Resource Management | 8 | carried (v18) |
| 11 | Semantic Correctness | 6 | Deep-dive. **Capped: Major finding** — unbounded HLC drift acceptance suspends evaporation/failure-detection for the drift duration (see Findings). LWW/HLC property tests pass (312/312) but the algorithm deviates from its cited source |
| 12 | Robustness | 8 | Deep-dive. `read_frame` validation re-verified (length/version gates, budget-limited reads, EOF); Run-16 garbage probe still green in-suite; no fresh fuzz execution this run |
| 13 | Security | 7 | Deep-dive. `verify_strict` + CA-based mTLS + PKCS8 loading read clean; carried gaps stand (unauthenticated tuple ack/replicate, no RBAC/audit); drift acceptance is also an unauthenticated-input-shapes-state surface in non-TLS clusters |
| 14 | Failure Mode Legibility | 7 | Deep-dive. Error strings consistently actionable (WAL refusal names versions+path; UnsupportedWireVersion carries hint) — but the run's central finding is a SILENT cluster-wide distortion: observe() absorbing a week of drift logs nothing. Warn-on-large-drift is the cheap legibility fix |
| 15 | Performance | 9 | Deep-dive. **Evidence:** release perf smoke this run — 635k/s single-thread, 2.28M/s contended; stripe-lock reconcile cost immaterial; scales with threads |
| 16 | Scalability | 7 | carried (v17) |
| 17 | Testability | 9 | **Evidence:** this run's canary, impact test, and perf smoke were each written+run in minutes; the arch-enforcement test gave immediate structural feedback on test placement |
| 18 | Test Architecture | 8 | Up from 7 citing artifacts: +6 race regressions, churn stress, perf smoke, drift canary pair; arch-enforcement test proven live. Remaining gap: no fuzz target for tuple-space WAL/replicate decoder |
| 19 | Observability | 8 | carried (v18) |
| 20 | Debuggability | 8 | carried (v17) |
| 21 | Operational Readiness | 9 | carried (v17, lifecycle drill + 13/13 scenarios) |
| 22 | Evolvability | 8 | CHANGELOG current (Security section added for the advisory bumps); wire policy unchanged; bincode-unmaintained is the open strategic question for the wire format |
| 23 | Documentation | 8 | carried (v17) |
| 24 | Developer Experience | 8 | Remote CI 3/3 green this run (first run of expanded workflow); held at 8: actions/checkout@v4 Node-20 deprecation lands 2026-06-16 (5 days) — bump to v5 pending |
| 25 | Dependency Hygiene | 6 | **Capped: Major finding this run** (2 RUSTSEC vulnerabilities found by audit probe; fixed in-run, audit now clean — expect recovery next run with sustained evidence). bincode unmaintained remains; no CI audit job yet |
| — | **Floor (lowest 3)** | **6, 6, 7** | Semantic Correctness (6, HLC drift), Dependency Hygiene (6, fixed in-run), four-way tie at 7 (Concurrency, Security, Scalability, Failure Mode Legibility) — the HLC drift bound is the actionable one and lifts both #11 and #14 |
| — | Mean (continuity footnote) | 7.9 | not a target; see M2 preamble |

## 2026-06-11 — Run 20 (M2)

Deep-dive dimensions this run (by rotation from Run 19): 16 (Scalability),
17 (Testability), 18 (Test Architecture), 19 (Observability),
20 (Debuggability). Next run by rotation: 21–25.

**Process disclosure.** Fourth run today; the scoring agent authored the
entire diff under evaluation (the demo-smoke / schema-re-assertion work and
all four prior sessions) with Run 19's table in context, so the blind rule is
compromised as in Runs 17–19. Mitigation unchanged: scores moved only on this
run's own probe/suite evidence, and the run's one finding is against
Robustness, a dimension this session's work did not touch. Cadence gate
passed on a material diff (8 commits since Run 19; 904 insertions).

Execution evidence this run: core 319/319 at full feature matrix
(`tls,metrics,a2a,llm`) + 320/320 with `fuzz-internals` (mini-fuzz);
tuple-space 24/24; clippy `-D warnings` clean at full matrix and
`--no-default-features`; demo-smoke (community cluster + mock LLM) 8/8
consecutive local + green on remote CI (run 27352303760); 3 new falsification
probes executed.

### Findings

- **Major — Robustness (#12, capped ≤ 6, scored 6, fixed in-run).**
  `bincode_cfg()` carried no decode byte-limit, so a frame whose internal
  length prefix claims a huge element count drove an unbounded
  `Vec::with_capacity` → process OOM-abort (SIGABRT, observed: "memory
  allocation of 2346361151233958999 bytes failed"). `read_frame` caps the
  *frame* at MAX_FRAME_BYTES but not the element counts decoded from inside
  it, so one malformed frame from any connected peer — or a bit-flip on a
  non-TLS link — aborts the node. Reachable on the live gossip path; all
  decoders (wire, capability, signal, locality, WAL sync) share the config,
  so the whole wire surface was exposed. Found by the new decoder mini-fuzz
  (`mini_fuzz_decoders_survive_adversarial_bytes`, 20 504 inputs: noise +
  truncation + single-bit-flip mutation of a valid frame; reproduced on a
  bit-flip case). Fixed in-run with
  `.with_limit::<MAX_FRAME_BYTES>()`; mini-fuzz now passes and stays as an
  in-suite regression gate (`fuzz-internals` feature). Calibration ledger
  entry 5. Top improvement target until the dedicated fuzz job catches up.
- **Probe passed — Semantic Correctness / Scalability (#11/#16):**
  `anti_entropy_delivers_pre_connection_writes` (kept). Reconstructed the
  community-demo cold-start flake: a spoke writes a key while its seed is not
  yet listening, the seed starts later, and anti-entropy must close the gap
  that live gossip missed by construction. Converged in ~252 ms (2-node).
  So the substrate's anti-entropy closure is sound; the demo flake was
  specifically the *schema* keys racing connection setup, correctly fixed by
  periodic re-assertion rather than a substrate bug. Resolves the open
  question carried from Run 19.
- **Probe passed — Operational Readiness (#21):**
  `ready_gate_flips_on_first_capability_advertisement` (kept) — `/ready`
  returns non-200 before any capability is advertised and flips to 200 after
  the first advertisement tick. The documented gate had no prior test.

| # | Dimension | Score | Notes |
|---|-----------|:-----:|-------|
| 1 | Philosophy / Coherence with Goal | 9 | carried (v19) |
| 2 | Conceptual Integrity | 8 | carried (v19) |
| 3 | Architecture | 9 | carried (v19) |
| 4 | Modularity | 8 | carried (v19) |
| 5 | API Design | 8 | carried (v19) |
| 6 | Error Handling Model | 8 | carried (v18) |
| 7 | Configurability | 8 | carried (v18) |
| 8 | Language Best Practices | 9 | **Evidence:** clippy `-D warnings` clean this run, full matrix + `--no-default-features`; `#![deny(unsafe_code)]` |
| 9 | Concurrency Correctness | 7 | carried (v19); no new concurrency finding this run |
| 10 | Resource Management | 8 | carried (v18) |
| 11 | Semantic Correctness | 8 | Recovered from Run-19 cap: HLC drift bound shipped + **anti_entropy_delivers_pre_connection_writes** probe passed this run (convergence closure verified, 2-node). Held at 8: single-probe evidence, not a broad convergence run |
| 12 | Robustness | 6 | **Capped: Major finding this run** — unbounded decode allocation / OOM-abort from one malformed frame (see Findings). Fixed in-run (`.with_limit`); mini-fuzz kept as regression gate |
| 13 | Security | 7 | carried (v19); the decode-limit fix also closes a remote-DoS vector, but the unauthenticated tuple-mutation / no-RBAC gaps still set the ceiling |
| 14 | Failure Mode Legibility | 7 | carried (v19); the OOM-abort was the opposite of legible (bare allocation message, SIGABRT) — now prevented, but a reminder the score isn't a 9 |
| 15 | Performance | 9 | carried (v19, release perf smoke); no fresh perf run this run |
| 16 | Scalability | 7 | Deep-dive. Anti-entropy delta carries the full `(key,timestamp)` index per round (`key_timestamps`) — O(n) digest cost that grows with store size; single-primary tuple ceiling and O(N²) TCP unchanged. No fresh scale execution beyond the 2-node anti-entropy probe |
| 17 | Testability | 9 | Deep-dive. **Evidence:** 3 probes written+run in minutes this run; the mini-fuzz reused `fuzz_internals` (internals reachable for tests behind a feature, not public — the right seam); ready-gate drove the live gateway |
| 18 | Test Architecture | 7 | Deep-dive. **Down from 8** + ledger entry: the two fuzz targets were counted in the pyramid for 19 runs but never executed, and a Major decoder DoS sat uncaught the whole time. The in-suite mini-fuzz (this run) and the demo-smoke job (this session) close two long-standing gaps; the dedicated `fuzz/` targets still have no CI job |
| 19 | Observability | 8 | Deep-dive. `/stats` (`dropped_frames`, `commit_conflicts`, `task_count`), `/metrics`, schema re-assertion observable under `skills/`; rich surface, but no fresh metrics probe this run and OTEL remains skillrunner-only |
| 20 | Debuggability | 8 | Deep-dive. `/mgmt` dashboard + `/gateway/kv/*` scan + agent card + `/consensus/{slot}` lease view; `/mgmt` reachability is now asserted by the demo-smoke job. Ballot internals still not inspectable |
| 21 | Operational Readiness | 9 | **Evidence:** `ready_gate_flips_on_first_capability_advertisement` probe passed this run; demo-smoke exercises start → A2A traffic → SIGTERM-clean shutdown 8/8 |
| 22 | Evolvability | 8 | carried (v19); CHANGELOG current (smoke + schema re-assertion entries); bincode-succession is ROADMAP v2 milestone 11 |
| 23 | Documentation | 8 | carried (v19, full guide alignment); 105 rustdoc intra-doc link warnings (`GossipAgent::*` links that moved to handles) noted as a cleanup target, not re-scored down without a deep-dive |
| 24 | Developer Experience | 8 | carried (v19); CI now builds examples + runs the demo smoke, tightening the contributor feedback loop; `checkout@v4` Node-20 deprecation (2026-06-16) still pending |
| 25 | Dependency Hygiene | 8 | Recovered from Run-19 cap (advisories fixed; CI audit job added); held at 8: no fresh `cargo audit` run this run, bincode-unmaintained persists |
| — | **Floor (lowest 3)** | **6, 7, 7** | Robustness (6, capped by finding); five-way tie at 7 — Concurrency, Security, Failure Mode Legibility, Scalability, Test Architecture. Robustness is the actionable one (fix shipped; needs the fuzz CI job to stay caught) |
| — | Mean (continuity footnote) | 8.0 | not a target; see M2 preamble |

## 2026-06-11 — Run 21 (M2)

Deep-dive dimensions this run (by rotation from Run 20): 21 (Operational
Readiness), 22 (Evolvability), 23 (Documentation), 24 (Developer Experience),
25 (Dependency Hygiene). Next run by rotation: 1–5.

**Process disclosure.** Second run today (Run 20 at 15:28; cadence gate passed
on a material diff: docs reorganization commits 13b3d47 + 89c9b82, plus
working-tree TypeScript SDK fixes and the in-run fixes below). The blind rule
held genuinely this run: all scores were fixed before ratings.md was opened.
The scoring session also ran the full test pyramid earlier today on user
request (not to unlock scores) — that evidence is cited where used.

Execution evidence this run: core lib 321/321 at full feature matrix
(`tls,metrics,a2a,llm`; 319 + 2 probe tests kept); tuple-space 33/33; clippy
`-D warnings` clean (full matrix); `tsc --noEmit` clean (mycelium-ts);
integration 13/13 (first run 12/13 — scenario-12 flake, see Findings);
`make test-scale` 100-node 5/5; `make test-scale-resilience` 21-node 10/10
across 4 phases; `make test-scale-entries` 30-node × 5000 keys 6/6 (100%
live-gossip fraction, sweep tail 0); 3 feature-combo builds
(`--no-default-features` alone, `+tls`, `+metrics`); link-integrity probe over
all md/html docs.

### Findings

- **Major — Resource Management / Semantic Correctness (#10/#11, both capped
  ≤ 6, fixed in-run).** Tombstone GC has never fired since the v9 HLC
  migration (3c4de6e, 2026-05-20): the GC predicate compared the store
  entry's **packed HLC** timestamp (`(physical_ms << 16) | logical`) against
  a **wall-clock-millisecond** cutoff (`tasks.rs` run_gc_task). A packed
  stamp is ~65 536× any ms cutoff, so `v.timestamp < tombstone_cutoff` was
  unsatisfiable — every tombstone accumulated forever (unbounded store growth
  on delete-heavy workloads; "only tombstones are GC'd" was in effect
  "nothing is ever GC'd"). Every other timestamp consumer
  (`CapEntry::is_fresh`, seen-set eviction) correctly unpacks via
  `hlc::physical_ms`; the GC was the one that didn't. Found by this run's
  falsification probe against Semantic Correctness (the planned probe — the
  equal-timestamp LWW tiebreak — turned out to be already covered by unit +
  proptest, so the probe moved to the next classic LWW edge: tombstone
  lifecycle). Fixed in-run: sweep extracted to
  `store::sweep_stale_tombstones` (unpacks via `physical_ms`, preserves the
  Run-18 conditional-remove discipline) + regression test
  `tombstone_gc_sweep_unpacks_hlc_timestamps` (kept). Calibration ledger
  entries 9–10.
- **Minor — Test Architecture / Failure Mode Legibility (#18/#14, both
  capped ≤ 6, fixed in-run).** Integration scenario 12 (prompt skills) flaked
  on its first execution today: `/gateway/llm/call returned no output —
  response: {}`; identical rerun passed 13/13. Root cause: the scenario's
  `curl -sf --max-time 10` raced the gateway's internal 30 s RPC timeout, and
  because the gateway returns **HTTP 200 + `{"error":...}` JSON** for all
  runtime failures, `-f` can only fail on the curl timeout — yielding the
  maximally illegible empty-object diagnostic. Fixed in-run: scenario now
  passes `timeout_ms:10000 < --max-time 15` so the gateway's legible error
  JSON always wins the race. The 200-on-error envelope itself is noted as an
  API-surface wart under #14. Supporting #18 gap: the TypeScript SDK is not
  type-checked in CI (see next finding).
- **Minor — Developer Experience (#24, capped ≤ 6, fixed in working tree).**
  The shipped TypeScript SDK had 8 `tsc` errors, including a runtime-breaking
  one: `shardFor()` referenced `this._base` (property is `base`) and omitted
  the path's leading slash — every call would have thrown since the method
  shipped (c6cc3ce, 2026-05-25). Found by a user-requested type check this
  morning; no CI gate runs `tsc --noEmit` on mycelium-ts. Calibration ledger
  entry 11.
- **Minor — Documentation (#23, capped ≤ 6, fixed in-run).** Link-integrity
  probe over all markdown/HTML docs found ROADMAP.md still linking three
  example files (`capability_market.rs`, `emergent_pool.rs`,
  `locality_wiring.rs`) deleted on 2026-05-25 (archived in dd03725, archive
  deleted in 95c92af). Fixed in-run (links converted to plain references with
  a retirement note); probe re-run reports 0 broken links. Calibration ledger
  entry 12.
- **Probe passed — Operational Readiness (#21):**
  `test_shutdown_lifecycle_edges_never_started_and_double` (kept) — shutdown
  on a never-started agent returns promptly (no hang on never-spawned tasks),
  and a second shutdown after a completed one is an idempotent no-op.
- **Probe passed — Dependency Hygiene / feature matrix (#25):** three
  feature-combo builds compile clean, including the CI-unexercised
  `--no-default-features --features tls` and `--no-default-features
  --features metrics` corners.

| # | Dimension | Score | Notes |
|---|-----------|:-----:|-------|
| 1 | Philosophy / Coherence with Goal | 8 | Re-examined (philosophy.html pull-aligned in 89c9b82): current, Holland mapping intact, TupleSpace pull-pattern refinement integrated; 8 is the no-execution-evidence cap for a doc-anchored dimension |
| 2 | Conceptual Integrity | 8 | carried (v19) |
| 3 | Architecture | 9 | carried (v19); the in-run GC fix respected the layer split (sweep lives in store.rs, task only schedules it) |
| 4 | Modularity | 8 | carried (v19) |
| 5 | API Design | 8 | carried (v19) |
| 6 | Error Handling Model | 8 | carried (v18); the gateway 200-on-error envelope is recorded under #14 this run — candidates for one shared finding if it recurs |
| 7 | Configurability | 8 | carried (v18) |
| 8 | Language Best Practices | 9 | **Evidence:** clippy `-D warnings` clean at full matrix this run; `#![deny(unsafe_code)]` |
| 9 | Concurrency Correctness | 7 | carried (v19); the extracted sweep preserves the conditional-remove rule from the Run-16–18 race family — no new finding |
| 10 | Resource Management | 6 | **Capped: Major finding** — tombstone GC never fired (unbounded tombstone accumulation); fixed in-run + regression test. Ledger entry 9 |
| 11 | Semantic Correctness | 6 | **Capped: same finding** — documented GC semantics ("only tombstones are GC'd") diverged from code since 2026-05-20. The originally-planned probe (equal-ts LWW tiebreak) was already covered by `lww_equal_timestamp_concurrent_data_converges` + proptest. Ledger entry 10 |
| 12 | Robustness | 8 | Recovered from Run-20 cap. **Evidence:** resilience suite 10/10 this run (crash/rejoin, late-joiner anti-entropy, 3× churn); decode mini-fuzz ran green inside the 321. Held at 8: one run since a Major decode-DoS; dedicated `fuzz/` CI job still pending |
| 13 | Security | 7 | carried (v19) |
| 14 | Failure Mode Legibility | 6 | **Capped: Minor finding** — gateway 200-on-error + curl `-f` produced the empty `{}` diagnostic that made the scenario-12 flake opaque; scenario fixed, envelope wart remains |
| 15 | Performance | 8 | **Evidence (re-scored 9→8 on fresh data):** entries test 100% live-gossip at write end, sweep tail 0 — but `dropped_frames` 56 (100-node, depth 2048) and 92 (entries, depth 4096) under burst show default backpressure headroom is thin |
| 16 | Scalability | 8 | Up from 7 on fresh execution: 100-node 5/5 and 30-node × 5000-entry 6/6 this run (the entry-volume axis Run 20 lacked). Structural ceilings unchanged (O(n) `key_timestamps` digest, O(N²) TCP managed by connection cap) — not a 9 |
| 17 | Testability | 9 | **Evidence:** 321 lib tests in 5.3 s; both probes this run were writable in minutes against public + pub(crate) seams |
| 18 | Test Architecture | 6 | **Capped: Minor finding** — scenario-12 flake (timing-budget race, fixed); TS SDK has no tsc gate in CI; `fuzz/` targets still lack a dedicated CI job |
| 19 | Observability | 8 | carried (v20); `dropped_frames` on `/stats` did its job in both scale tests this run |
| 20 | Debuggability | 8 | carried (v20) |
| 21 | Operational Readiness | 9 | Deep-dive. **Evidence:** integration 13/13 incl. KV-persistence + full-cluster restart; resilience 10/10; lifecycle-edge probe passed (never-started + double shutdown, kept) |
| 22 | Evolvability | 8 | Deep-dive. Wire policy v2–v11 with documented downgrade shims; `read_frame_accepts_prev_wire_version` ran green in this run's suite; CHANGELOG `[Unreleased]` current. Held at 8: wire codec sits on unmaintained bincode (succession = ROADMAP v2 milestone 11) |
| 23 | Documentation | 6 | **Capped: Minor finding** — 3 dangling ROADMAP example links post-reorg (fixed; link probe now 0 broken). Guide chapters verified on current sub-handle API (61 call sites). Ledger entry 12 |
| 24 | Developer Experience | 6 | **Capped: Minor finding** — TS SDK shipped 8 type errors incl. runtime-breaking `shardFor()`; no tsc CI gate. Rust-side DX strong (Makefile, CLAUDE.md on-ramp, 5 s test cycle). Ledger entry 11 |
| 25 | Dependency Hygiene | 8 | Deep-dive. **Evidence:** 3 feature-combo builds clean this run; 251 deps full-matrix vs 61 minimal (annotation-disciplined optionals); `block` future-incompat is dev-only (wgpu→metal). Held at 8: bincode-unmaintained persists; no fresh `cargo audit` this run |
| — | **Floor (lowest 3)** | **6, 6, 6** | Six-way tie at 6, all capped by findings: Resource Management, Semantic Correctness, Failure Mode Legibility, Test Architecture, Documentation, Developer Experience. All four findings fixed in-run/in-tree; the caps record that they shipped |
| — | Mean (continuity footnote) | 7.6 | not a target; see M2 preamble |

## 2026-06-12 — Run 22 (M2)

Deep-dive dimensions this run (by rotation from Run 21): 1 (Philosophy),
2 (Conceptual Integrity), 3 (Architecture), 4 (Modularity), 5 (API Design).
Next run by rotation: 6–10.

**Process disclosure.** This session authored the entire diff under evaluation
(12 commits since Run 21: gateway status codes, depth-default alignment, three
new CI jobs, the AFN pull migration + smoke harness, Linda-lanes callouts,
paper/philosophy boundary sections, blackboard deferral) and wrote Run 21, so
the blind rule is compromised as in Runs 17–21. Mitigation unchanged: scores
moved only on named execution evidence from this window. Cadence gate passed
(new day; material diff).

Execution evidence this run: lib 323/323 full matrix (322 + this run's probe
test), and again at `--test-threads=32` (probe C); clippy `-D warnings` clean;
`tsc --noEmit` clean; integration 13/13 (post-gateway-change run, scenario 12
exercising the new status codes); CI run 27398387370 fully green — all 8 jobs
including the first executions of `afn-smoke` (both pipeline modes, 24/24
each), `sdk-ts` (tsc gate), and the dedicated `fuzz` job (2 × 120 s libFuzzer,
wire + capability decoders); local afn ci_smoke pull 24/24 in 1.9 s + push
24/24; cargo-tree dependency-direction probe.

### Findings

- **Minor — Test Architecture (#18, capped ≤ 6, fixed in-run).**
  `test_commit_conflict_tripwire` flaked on a loaded 4-vCPU CI runner: its 4 s
  structural-poll budget expired (panic "tripwire did not fire on conflicting
  COMMIT") while 0/40 local repetitions fail — CPU starvation under ~16
  parallel test threads, not a logic regression. Fixed by widening the budget
  to 15 s (the poll is structural, so a genuinely broken tripwire still fails
  deterministically). Found by CI in this run's window, not by a probe. No
  ledger entry: #18 scored 6–7 while the test existed.
- **Probe passed — Architecture (#3):** dependency-direction acyclicity —
  `cargo tree --edges normal` shows zero `mycelium → mycelium-tuple-space`
  normal edges (dev-only cycle as documented) and exactly one
  `mycelium-tuple-space → mycelium` edge. The composability claim's structural
  precondition holds.
- **Probe passed — Operational Readiness (#21):**
  `test_gateway_port_closes_on_shutdown` (kept) — /health serves 200 while
  live, and the gateway port refuses connections within the shutdown grace
  window. The LB-drain invariant had no prior test; a zombie listener
  answering health checks from a dead agent would now be caught.
- **Probe passed — Testability (#17):** full suite at `--test-threads=32`
  (4× default parallelism): 323/323 in 5.3 s — the shared intern pool,
  fixed-seed hash state, and atomic port allocator show no cross-test
  interference under stress.

| # | Dimension | Score | Notes |
|---|-----------|:-----:|-------|
| 1 | Philosophy / Coherence with Goal | 8 | Deep-dive. The one known drift instance — the flagship AFN demo running the coordinator anti-pattern — was eliminated this window and is execution-gated (afn-smoke green both modes). Residual keeps it at 8: guide 07's body still teaches coordinator-push as *the* concept; only the appended callout corrects it |
| 2 | Conceptual Integrity | 8 | Deep-dive. Gateway error convention unified (the 200-on-error outlier removed; verified by `test_llm_call_no_provider_returns_404` + 13/13). Residual: `timeout_ms` (4 uses) vs `timeout_secs` (13 uses) split across gateway endpoints |
| 3 | Architecture | 9 | Deep-dive. **Evidence:** dependency-direction probe passed (acyclic, dev-only cycle as documented); integration 13/13 + scenario 13 exercise both documented layer crossings; Run-21 GC fix kept sweep logic in store.rs with tasks.rs only scheduling |
| 4 | Modularity | 8 | Deep-dive. Companion crate consumes only the public API (scenario 13 + afn-smoke as living proof); ceiling unchanged — `TaskCtx` 22-field bundle couples every handle through one Arc (documented v2 split) |
| 5 | API Design | 8 | Deep-dive. llm/call outlier fixed and tested; SDKs type-gated in CI now. Residuals: the timeout_ms/timeout_secs naming split; py `TupleSpace` constructs a fresh httpx client per call |
| 6 | Error Handling Model | 8 | carried (v18); gateway envelope unification is an improvement within the same model |
| 7 | Configurability | 8 | carried (v18); `writer_channel_depth` default now matches every doc that mentions it (four conflicting values reduced to one) |
| 8 | Language Best Practices | 9 | **Evidence:** clippy `-D warnings` clean at full matrix this run; `#![deny(unsafe_code)]` |
| 9 | Concurrency Correctness | 7 | carried (v19) |
| 10 | Resource Management | 8 | Recovered from Run-21 cap: tombstone-GC fix + `tombstone_gc_sweep_unpacks_hlc_timestamps` green in this run's 323 (×2 incl. 32-thread) |
| 11 | Semantic Correctness | 8 | Recovered from Run-21 cap: 323/323 incl. LWW tiebreak proptest, sweep regression, consensus suite; integration 13/13 |
| 12 | Robustness | 8 | The gap Run 20–21 named is closed: dedicated fuzz job executed in CI (2 × 120 s libFuzzer green). Held at 8 — time-box is shallow; no new adversarial-input probe this run |
| 13 | Security | 7 | carried (v19) |
| 14 | Failure Mode Legibility | 8 | Recovered from Run-21 cap: failures on /gateway/llm/call now surface as 404/502/504 with the JSON body kept (`test_llm_call_no_provider_returns_404`; scenario 12 in 13/13); SSE in-stream error asymmetry documented at the handler |
| 15 | Performance | 8 | carried (v21, entries-test data); no fresh perf run |
| 16 | Scalability | 8 | carried (v21, 100-node + entry-volume runs); none this window |
| 17 | Testability | 9 | **Evidence:** probe C — 323/323 at `--test-threads=32` in 5.3 s, no hidden-global-state interference; this run's two probe tests written and green in minutes |
| 18 | Test Architecture | 6 | **Capped: Minor finding** — tripwire-test flake under CI load (fixed in-run, 4 s → 15 s structural budget). Counterweight noted, not scored: 8 CI jobs green incl. first sdk-ts, fuzz, afn-smoke executions; both demo modes regression-gated |
| 19 | Observability | 8 | carried (v20) |
| 20 | Debuggability | 8 | carried (v20) |
| 21 | Operational Readiness | 9 | **Evidence:** `test_gateway_port_closes_on_shutdown` probe passed + kept; afn-smoke drives full start → health → work → teardown cycles for 2 × 3-node clusters in CI |
| 22 | Evolvability | 8 | carried (v21); CHANGELOG current through this window's changes |
| 23 | Documentation | 8 | Recovered from Run-21 cap: 0 broken links; depth default aligned repo-wide; lanes-vs-Linda called out in 10 places incl. crate doc + guide. Residual shared with #1: guide 07 body pre-dates the pull migration |
| 24 | Developer Experience | 8 | Recovered from Run-21 cap: `tsc --noEmit` green and now CI-gated (sdk-ts job); contributor loop unchanged otherwise |
| 25 | Dependency Hygiene | 8 | carried (v21 feature-combo builds); no new audit run |
| — | **Floor (lowest 3)** | **6, 7, 7** | Test Architecture (6, capped by the flake finding); Concurrency Correctness and Security tie at 7 (both carried — neither re-examined since v19; due in the 6–10 rotation next run) |
| — | Mean (continuity footnote) | 8.0 | not a target; see M2 preamble |

## 2026-06-13 — Run 23 (M2)

Deep-dive dimensions this run (by rotation from Run 22): 6 (Error Handling),
7 (Configurability), 8 (Language Best Practices), 9 (Concurrency Correctness),
10 (Resource Management). Next run by rotation: 11–15.

**Process disclosure.** This session authored the entire diff under evaluation
(3 commits since Run 22: v2 milestone 14 substrate-native supervision, milestone
15 OBR resolve-from-installable-catalog, and the Core Principles compliance
gate) and is producing this run, so the blind rule is compromised as in Runs
17–22. Mitigation: the diff is **docs-only** (ROADMAP.md) — no code, tests, or
config changed — so every code dimension is unaffected by the authorship; scores
moved only on named execution evidence. Cadence gate passed (new day; material
doc diff).

Execution evidence this run: full-matrix lib suite `tls,metrics,a2a,llm`
**325 passed / 0 failed / 1 ignored** (16.9 s); gateway-free
`cargo build --lib --no-default-features` **exit 0** (6m01s clean); clippy
`--lib --tests --features tls,metrics,a2a,llm -D warnings` **clean** (0 lints);
new probe test `validate_rejects_http_port_equal_to_bind_port` **green**; tree
verification that all five calibration-ledger bug-fix canaries are present
(`lww_wins`, `index_stripes` stripe locks, `max_clock_drift_ms`/`with_max_drift`,
`with_limit::<MAX_FRAME_BYTES>`, `sweep_stale_tombstones`); and — run
post-hoc at the user's request — afn-smoke `examples/fluid_pipeline/ci_smoke.sh
both` (Docker-free) **green both modes**: canonical coordinator-free pull
**24/24** + push baseline **24/24**.

Falsification quota — 3 probes against high-scoring dimensions, all **passed**:
- **Architecture (#3, 9):** gateway-free `--no-default-features` build compiled
  clean — the documented "gateway compiled away; gossip core/KV/signal/consensus
  remain" layer-separability claim holds.
- **Language Best Practices (#8, 9):** clippy `-D warnings` clean at full feature
  matrix incl. `--tests`; only `unsafe` outside tests is edition-2024
  `std::env::set_var` in `#[cfg(test)]` config tests.
- **Error Handling / Configurability (#6/#7, 8):** constructed an adversarial
  config (`http_port == bind_port`) and asserted `validate()` returns the *typed*
  `GossipError::FieldConflict { field_a, field_b }` with correct field names —
  not a silent pass, not a panic. No prior test existed; the regression test was
  kept (suite-growing).
- Probes deliberately varied from Run 22 (which probed dependency-direction
  acyclicity, shutdown-port closure, 32-thread parallelism). Testability (#17)
  and Operational Readiness (#21) keep their Run-22 probe-backed 9s as carried,
  corroborated this run only by the green standard-parallelism suite — disclosed,
  not independently re-probed.

### Findings

None. All three falsification probes passed; no dimension capped this run. No new
calibration-ledger entry (no bug surfaced in a dimension scoring ≥ 8).

| # | Dimension | Score | Notes |
|---|-----------|:-----:|-------|
| 1 | Philosophy / Coherence with Goal | 9 | **Evidence:** afn-smoke (`ci_smoke.sh both`, Docker-free) green this run — canonical coordinator-free **pull 24/24** + push baseline **24/24**; the flagship AFN demo *embodies* the no-coordinator philosophy end-to-end, not merely asserts it. Reinforced by the Core Principles compliance gate added this window (philosophy now normative; milestones 14/15 gated on it). The Run-22 guide-07 residual was re-checked and does **not** hold (07-pipelines.md is pull-canonical) — no residual remains. (10 unreachable: needs external/third-party validation.) |
| 2 | Conceptual Integrity | 8 | carried (v22); `timeout_ms`/`timeout_secs` gateway naming split residual unchanged |
| 3 | Architecture | 9 | **Evidence:** gateway-free `--no-default-features` build passed this run (layer/feature separability); compliance gate now codifies layer discipline + the "don't teach Layer I a Layer III law" invariant normatively. TaskCtx coupling lives in #4, not here |
| 4 | Modularity | 8 | carried (v22); `TaskCtx` 22-field Arc bundle still couples every handle (documented v2 split, milestone 1) |
| 5 | API Design | 8 | carried (v22) |
| 6 | Error Handling Model | 8 | Deep-dive. `GossipError` is one coherent `thiserror` enum (82 lines) with actionable messages (`UnsupportedWireVersion` carries a hint; `FieldConflict` names both fields); probe confirmed the typed `FieldConflict` variant is actually produced, not stringly-typed. Propagation via `?`. No standout to justify 9 |
| 7 | Configurability | 8 | Deep-dive. `validate()` covers single-field bounds *and* cross-field conflicts (http_port≠bind_port, http_addr IP parse); env overrides parse to typed `GossipError::Parse`; TOML round-trip tested; 1044-line surface justified by documented tunables. FieldConflict regression test added + green |
| 8 | Language Best Practices | 9 | Deep-dive. **Evidence:** clippy `-D warnings` clean at full matrix + `--tests` this run; ~zero production `unsafe` (only edition-2024 `set_var` in test cfg); edition 2024, rust 1.88, type-driven correctness (NodeId, sealed handles) |
| 9 | Concurrency Correctness | 7 | Deep-dive. All five ledger-bug canaries verified present in tree; lock-order table flat (no nested acquisitions); 325 green incl. the new random-topology property test (7069d4c). **Held at 7, not raised:** two ledger entries (listener race, prefix-index race) mandate structural skepticism, and no *fresh* adversarial concurrency probe was run this window (probes targeted #3/#8/#6-7) |
| 10 | Resource Management | 8 | Deep-dive. `sweep_stale_tombstones` (store.rs:658) + its regression test green in this run's 325; RAII drop on `CapabilityHandle`/`LockGuard`/`MailboxHandle`/`BulkServeHandle` confirmed. Unprobed and flagged for next run: the CLAUDE-noted task-leak path (per-peer writer exit on disconnect) and task_count-to-baseline on shutdown |
| 11 | Semantic Correctness | 8 | carried (v22); canaries `lww_wins` (data-vs-data tiebreak) and `max_clock_drift_ms`/`Hlc::with_max_drift` confirmed present this run, with the Run-19 drift regression test + proptests green in the 325-run |
| 12 | Robustness | 8 | carried (v22); `with_limit::<MAX_FRAME_BYTES>` decode cap confirmed present (framing.rs:231); no fresh adversarial-input probe (fuzz toolchain — nightly/cargo-fuzz — not installed locally) |
| 13 | Security | 7 | carried (v19/v22); not re-examined since v19 — due in the 11–15 rotation next run |
| 14 | Failure Mode Legibility | 8 | carried (v22) |
| 15 | Performance | 8 | carried (v21); no fresh perf run |
| 16 | Scalability | 8 | carried (v21); no fresh scale run |
| 17 | Testability | 9 | carried (v22, 32-thread probe); corroborated this run only by green standard-parallelism 325-run — 32-thread stress not re-executed |
| 18 | Test Architecture | 7 | Recovered from Run-22 flake cap (6): the 15 s structural-poll budget fix is in-tree and the tripwire test was green in this run's 325 with no flake. **Held at 7, not 8:** the heavy ledger entry (fuzz targets inert for 19 runs) is only mitigated by CI execution — I could not run the fuzz targets locally this run (no nightly/cargo-fuzz) |
| 19 | Observability | 8 | carried (v20) |
| 20 | Debuggability | 8 | carried (v20) |
| 21 | Operational Readiness | 9 | carried (v22, shutdown-port probe); not independently re-probed this run |
| 22 | Evolvability | 8 | Re-eval (docs diff): v2 milestones 14/15 added and now governed by the compliance gate; wire policy unchanged (v11 / PREV 10, rolling window open). No CHANGELOG-relevant code change this window |
| 23 | Documentation | 8 | Re-eval (docs diff): ROADMAP gained a normative compliance gate + two cross-referenced milestones, internally consistent, no broken intra-doc refs introduced. (The Run-22 "guide 07 pre-dates the pull migration" residual was re-checked this run and dropped — see #1: 07-pipelines.md is pull-canonical.) |
| 24 | Developer Experience | 8 | carried (v22) |
| 25 | Dependency Hygiene | 8 | carried (v21); gateway-free build passed (the `--no-default-features` dep-hygiene requirement holds), Cargo.lock present. **Caveat:** no `cargo audit` this run — and the ledger records advisories previously lurking under a high score here, so this 8 is unverified against the advisory DB this window. `block v0.1.6` future-incompat is a transitive *dev*-dependency (wgpu→objc), not in the production graph |
| — | **Floor (lowest 3)** | **7, 7, 7** | Concurrency Correctness (#9), Security (#13), Test Architecture (#18) — Test Architecture recovered 6→7 (flake cap resolved); the other two are carried 7s awaiting the 11–15 / next deep-dive rotation. Floor unchanged by the dim-1 bump (1 was not in the lowest 3) |
| — | Mean (continuity footnote) | 8.1 | not a target; see M2 preamble (8.04→8.08 on the dim-1 8→9 bump) |

## 2026-06-14 — Run 24 (M2)

Deep-dive dimensions this run (by rotation from Run 23): 11 (Semantic
Correctness), 12 (Robustness), 13 (Security), 14 (Failure Mode Legibility),
15 (Performance). Also re-examined 6/9/10 — the heavy diff (the entire v1.x
compliance layer) touched error handling, concurrency, and resource lifecycles.
Next run by rotation: 16–20.

**Process disclosure.** This session authored the entire diff under evaluation —
the full v1.x Production Readiness Gap closure across five merged PRs (WS1 RBAC,
WS2 tamper-evident audit, WS3 crown-jewel data-at-rest + egress, WS4 OIDC SSO,
WS5 hot cert rotation) plus the two follow-ups (multi-key archival, full egress
coverage) and the doc-alignment sweep — and is producing this run, so the blind
rule is compromised as in Runs 17–23. Unlike Run 23 this is **major code**, not
docs-only. Mitigation: scores moved only on named execution evidence + the new
falsification probe, and **ledger-aware skepticism was applied** — Security and
Concurrency were held at 8 *despite* fresh green evidence (see Findings/notes),
and a new ledger entry was filed for a security gap this very session uncovered
(the `tls` transport had never run — missing rustls CryptoProvider).

Execution evidence this run: full-matrix lib suite `tls,metrics,a2a,llm`
**340 passed / 0 failed / 1 ignored** (16.5 s); `compliance` lib suite
**366 passed / 0 failed / 1 ignored** (16.9 s, includes the new probe); default
**318** and `tls` **323** (earlier same session); clippy
`--lib --tests --features compliance -D warnings` **clean**; gateway-free
`cargo build --lib --no-default-features` **exit 0**; `Cargo.lock` present
(**427** transitive deps with the full optional-feature set); new probe test
`probe_concurrent_audit_chain_is_contiguous_and_verifies` **green**.

### Findings
None. The falsification probe passed (below).

Falsification quota — one new executable probe against the highest-risk *new*
code (the WS5 audit chain lock), plus two corroborating probe-suites that ran
green this run:
- **Concurrency Correctness / Semantic Correctness / Security (the audit chain):**
  fired **64 concurrent `audit()` calls** on one node and asserted the per-node
  hash chain is contiguous `0..64`, every content hash distinct, verifies from
  genesis, and a post-hoc edit still fails verification. **Passed** — the chain
  lock (#8) serialises seq/prev_hash assignment correctly and the
  sign-outside-the-lock optimisation does not corrupt linkage under contention.
  Kept as a regression test (`probe_concurrent_audit_chain_is_contiguous_and_verifies`).
- **Semantic Correctness (rotation):** the existing chain-spanning-a-rotation +
  cross-node rotation-verify tests (`chain_spanning_a_key_rotation_verifies_against_the_key_set`,
  `test_ws5_rotate_identity_verifies_across_rotation_on_peer`) ran green —
  retained-key verification holds across a key rotation.
- **Security (OIDC):** the alg-confusion probe (`hs256_alg_confusion_is_rejected`)
  + iss/aud/exp/unknown-kid rejections ran green — the asymmetric-only allowlist
  holds. (Carried from this session's authorship, re-executed this run.)

| # | Dimension | Score | Notes |
|---|-----------|:-----:|-------|
| 1 | Philosophy / Coherence with Goal | 8 | v1.x security added with explicit fidelity to the layering litmus: `sys/` tripwire is detection-not-prevention (NOT an `apply_and_notify` guard), RBAC/egress gate *action/admission* never forwarding, roles/audit are signed KV entries that *use* the substrate. Read-only (caps at 8). |
| 2 | Conceptual Integrity | 8 | New modules (rbac/audit/oidc) mirror existing idioms — `SignedAuditRecord` ↔ `SignedRoleClaim`, `bincode_cfg` encode/decode, cfg-gating like `tls`. Read-only. |
| 3 | Architecture | 8 | Three-layer separation intact; security features layer on documented prefixes (`sys/role`, `sys/audit`); gateway-free `--no-default-features` build clean (execution evidence of layer separability) but no fresh dep-graph audit → 8. |
| 4 | Modularity | 8 | Sub-handles independent; compliance code well-isolated. `helpers.rs` now hosts shared key helpers used by 4 verify paths (acceptable util coupling). Read-only. |
| 5 | API Design | 8 | New surface (advertise_roles/audit/rotate_identity/egress_policy/with_data_at_rest_cipher + OidcConfig/GatewayToken/EgressPolicy) consistent + opt-in + hard-to-misuse (empty egress = allow-all). Read-only. |
| 6 | Error Handling Model | 8 | Typed: `InvalidField` for tls-required, precise `AuditVerifyError{seq}`, coarse `OidcError` (deliberate — no leak to unauthenticated callers). Error paths exercised green. Minor: best-effort `let _ = kv().set()` swallows (documented, anti-entropy-recoverable) keep it off 9. |
| 7 | Configurability | 8 | New serializable config (scoped tokens, egress, oidc) cleanly separated from the runtime `DataAtRestCipher` (builder, not config). Read-only. |
| 8 | Language Best Practices | 9 | clippy `--lib --tests -D warnings` **clean** across default/tls/llm/compliance/CI this run (named execution evidence); no `unwrap()` in new non-test code; ArcSwap/let-chains/`is_multiple_of` idiomatic. |
| 9 | Concurrency Correctness | 8 | Concurrent-audit probe passed (lock #8 serialises correctly; sign moved outside the lock). Held at 8 **not 9** (ledger-aware): `merge_peer_keys` is a get-modify-insert on papaya **not** using `compute()` — the recurring "lock-free op + unserialised derived effect" family; eventually-consistent via the single-writer watcher re-scan, but technically not retry-safe. Watch-item. |
| 10 | Resource Management | 8 | New resources bounded: OidcVerifier (one reqwest client + 1-entry TTL JWKS cache, dropped with the server), `audit_chain` Mutex, ArcSwap, cipher OnceLock. No new spawned tasks; probe drives start→use→shutdown. `task_count==0`-after-shutdown not re-probed for new fields → 8. |
| 11 | Semantic Correctness | 9 | **Deep-dive.** Hash-chain integrity (contiguity + prev_hash + sig), retained-key verify-any across rotation, multi-key archival full-history — all exercised by the 366-test compliance suite + the new concurrent-chain probe, green this run (named execution evidence). LWW/HLC core unchanged. |
| 12 | Robustness | 8 | **Deep-dive.** Malformed handling: `parse_identity_keys` rejects non-32-multiples (no panic), OIDC handles garbage/unknown-kid/JWKS-fetch-failure (keeps last good key set + warn), egress `permits_url` fail-closed on unparseable host, audit decode is `.ok()`. SignedData fail-open preserved. No fresh malformed-wire-frame probe this run → 8. |
| 13 | Security | 8 | **Deep-dive.** Signed RBAC (forged role → None), OAuth2 ACLs (deny-by-default, public edge open), OIDC asymmetric-only alg allowlist (alg-confusion rejected, tested), tamper-evident audit (tested incl. concurrent), data-at-rest hook, egress allowlist (now full coverage), retained-key rotation + `sys/` tripwire. Fresh green probes — but held at 8: the **tls-never-ran** gap this session uncovered (now a ledger entry), the documented compromise-rotation caveat (retired keys stay trusted for verification), and the `merge_peer_keys` concern. Honest 8 over a fresh-evidence 9. |
| 14 | Failure Mode Legibility | 8 | **Deep-dive.** New `sys_namespace_violations` stat + `warn!` (mirrors commit_conflicts), `AuditVerifyError` names the offending seq, egress denials name the policy, rotation warns. Read-only assessment of message quality → 8. |
| 15 | Performance | 7 | **Deep-dive.** Design is perf-conscious — sign-outside-lock (µs critical section), lock-free ArcSwap on the hot signing/handshake path, verify-any over a tiny key set, cached JWKS — but **no benchmark run this run**; conservative per the evidence gate. |
| 16 | Scalability | 7 | New documented-unbounded growth surfaces: `sys/audit` (per-node, retention = operator's), `sys/identity` (+32 B/rotation), `peer_keys` retained set. Core scale (100-node) not re-run this run. Conservative. |
| 17 | Testability | 8 | Security-critical logic isolated as pure functions (verified_roles, validate_token, parse/encode_identity_keys, scope_admits, required_scope) unit-tested without a cluster; in-process mock-IdP/mock-MCP; tls single-node agents. Read-only assessment of the property. |
| 18 | Test Architecture | 8 | Strong pyramid (pure unit + in-process integration + 13 Docker scenarios + fuzz); probes kept as regression tests. Held at 8 (not 9): an intermittent multi-node timing test flaked twice this session (non-deterministic test = real test-arch wart). |
| 19 | Observability | 8 | `/gateway/audit` (verify + per-record content_hash), `sys_namespace_violations` on `/stats`, the audit trail itself. Read-only (no live `/metrics` probe this run). |
| 20 | Debuggability | 8 | Signed who-did-what audit trail + two promise-strength tripwires (`commit_conflicts`, `sys_namespace_violations`) + seq-naming verify errors materially aid debugging. Read-only. |
| 21 | Operational Readiness | 8 | Five new ops runbooks (rbac/audit/crown-jewel/sso/cert-rotation); hot rotation = zero-downtime ops; opt-in compliance with documented config; CLA/CI workflow fixed. Gateway-free build green. No Docker scenario run this run → 8. |
| 22 | Evolvability | 8 | Wire stays v11 (new features are KV-namespace/HTTP/config, not wire); `sys/identity` 32→32×N format is back-compatible (32 B still parses); compliance cleanly feature-gated; documented follow-ups now closed. Read-only. |
| 23 | Documentation | 8 | Heavily updated + audited this run: CLAUDE.md per-WS sections, README §Security Model, guide ch.09 + index, threat model, 5 ops runbooks, presentation deck, ROADMAP — three stale spots (guide index, skillrunner ref, deck counts) found and fixed. Docs not executable → caps at 8. |
| 24 | Developer Experience | 8 | CLAUDE.md on-ramp updated; clippy clean; one-flag `compliance`; test fixtures provided. Build time not measured this run → 8. |
| 25 | Dependency Hygiene | 8 | New deps all optional + feature-gated + well-chosen: `sha2` (already transitive via ed25519-dalek), `jsonwebtoken` (uses `ring`, present), `arc-swap` (tiny, ubiquitous). `--no-default-features` builds clean (execution evidence) — default/embedded supply chain unaffected. 427 transitive deps with the full optional set. |
| — | **Floor (lowest 3)** | **7, 7, 8** | Performance (15), Scalability (16), Concurrency Correctness (9 — the `merge_peer_keys` non-`compute()` watch-item) |
| — | Mean (continuity footnote) | 8.0 | not a target; see M2 preamble |

## 2026-06-16 — Run 25 (M2)

Deep-dive dimensions this run (by rotation from Run 24): 16 (Scalability),
17 (Testability), 18 (Test Architecture), 19 (Observability), 20 (Debuggability).
Also re-examined 3 (Architecture) + 4 (Modularity) — the diff is the entire v2
M1/M2/M3 program (the `mycelium-core` workspace split, `consensus` feature gate,
SchemaHandle/KvHandle/MeshHandle pushdown), which lands squarely on the layer
boundary. Next run by rotation: 21–25.

Diff since Run 24: 28 commits — M1 (`mycelium-core` crate carved at the II↔III
seam: leaf modules + transport + `CoreCtx`/`TaskCtx` God-Object split), M2
(`consensus` feature gate — Layer III opt-out), M3 (handle pushdown +
`KvQuorumExt` overlay for `set_with_min_acks`). Pure structural refactor: no wire
change (stays v11), test totals preserved (318 default), Deref coercion keeps the
~380 `ctx.<field>` sites and 8 sub-handle accessors unchanged.

Execution evidence: `cargo test --lib -p mycelium-core` → **82 passed**;
`cargo test --lib -p mycelium` → **236 passed** (318 default total, matches
CLAUDE.md); `cargo test --lib --no-default-features --features gateway`
(consensus opt-out) → **193 passed**; `cargo build -p mycelium-core` (standalone
substrate) clean (20.7s); `cargo build --lib --no-default-features --features
gateway` clean (47s); `cargo tree` dep counts: **52 core / 139 full** (confirms
the M1 "≈48 vs ≈140" dep-tree-win claim by execution).

### Findings
None confirmed. All falsification probes passed or strengthened the score:

- **Probe 1 (Architecture, #1 dim, score 9) — inverted-dependency / acyclic
  invariant.** `grep` for any `mycelium::` / `use mycelium` / `extern crate
  mycelium` reference inside `mycelium-core/src/` → **zero hits**; core's
  `Cargo.toml` has no dependency on the parent. The "core cannot reference the
  full crate (would be a Cargo cycle)" claim is a *true compile-time guarantee*,
  not a convention. Reinforced by `cargo build -p mycelium-core` succeeding in
  isolation. PASS.
- **Probe 2 (Architecture/Philosophy) — Layer-III drift into the substrate.**
  `grep` for quorum/ballot *accounting* in core flagged 3 files; inspection
  shows they are the Layer-II distinct-sender evidence query (`SignalHandlers::
  quorum`, counting senders of a signal kind in a window) and doc-comments — NOT
  ballot/COMMIT logic, which remains in `src/consensus.rs` (upper crate).
  `set_with_min_acks` is explicitly documented in `kv_handle.rs` as living in the
  upper crate (`kv_quorum_ext.rs`). The substrate is genuinely unaware of
  agreement. PASS (strengthens #1/#3).
- **Probe 3 (Test Architecture, score 8) — is the M2 "consensus-disabled node
  still forwards PROPOSE/VOTE/COMMIT" invariant tested, or asserted-only?** No
  dedicated cross-feature regression test exists. *However* it is structurally
  guaranteed: forwarding lives in `mycelium-core`, which has no `consensus`
  feature at all, so the forward path cannot gate on it. Verified `grep` finds no
  consensus-feature gate on core's forward path. Behaviour is correct by
  construction → a minor coverage observation, **not a confirmed bug**. Noted as
  a test-arch wart (keeps #18 at 8, not 9).

### Calibration ledger
No new entry — no bug found this run.

| # | Dimension | Score | Notes |
|---|-----------|:-----:|-------|
| 1 | Philosophy / Coherence with Goal | 8 | The M1 crate cut *operationalises* the layering litmus test: substrate (I+II) is now a separate crate that "cannot reference the layers above" — the philosophy doc's central rule is a compile error to violate. Consensus stays an emergent concern atop the signal mesh (verified core has no ballot logic). Read philosophy.html in full; no execution to falsify a philosophy → caps at 8. |
| 2 | Conceptual Integrity | 8 | One mind: the split preserved all idiom — Deref coercion (`TaskCtx`→`CoreCtx`, sub-handles unchanged), `KvQuorumExt` overlay mirrors the existing extension-trait pattern, doc-comments explain *why* `set_with_min_acks` lives upstream. No naming/abstraction divergence introduced. Read-only → 8. |
| 3 | Architecture | 9 | **Deep-dive.** The inverted-dependency invariant is now a *compile-time guarantee*, falsification-probed this run: core never references parent (grep → 0), builds standalone, consensus opt-out builds + 193 tests green. New verifiable artifact (the crate boundary did not exist before M1) — the one increase this run, execution-backed. |
| 4 | Modularity | 8 | **Deep-dive.** Substrate now physically separable: `mycelium-core` (52 deps) compiles + tests (82) without the parent. The three core↔upper couplings (ReplyInterceptor / QuorumObserver / SnapshotDeferHook) are `None`-safe hooks, not hard deps. Held at 8 (not 9): handles still share `Arc<CoreCtx>` mutable state — sanctioned cohesion, not full independence. |
| 5 | API Design | 8 | carried (v24). `KvQuorumExt` overlay is the only surface change — additive extension trait, `kv()` handle unchanged. Read-only. |
| 6 | Error Handling Model | 8 | carried (v24). Refactor moved error types into `mycelium-core/src/error.rs`; no propagation pattern changed. |
| 7 | Configurability | 8 | carried (v24). `consensus` feature is a new opt-out knob (default-on); config surface otherwise unchanged. |
| 8 | Language Best Practices | 8 | carried **down** from v24's 9: that 9 was earned with a fresh `clippy -D warnings` run; I did **not** re-run clippy this run (per M2: don't run a suite just to unlock a 9). Refactor itself is idiomatic — Deref, extension traits, ArcSwap retained. Honest 8 absent fresh lint evidence. |
| 9 | Concurrency Correctness | 8 | carried (v24). Refactor moved concurrency-bearing code (store `index_stripes`, `signal.rs` fan-out) across the crate boundary; the 82 core tests — incl. the prefix-index race regression — pass, evidence the moved code still serialises. Lock-order table unchanged. **Post-scoring (same session):** the standing `merge_peer_keys` non-`compute()` watch-item was probed at user request and **confirmed a real Major defect** — lost 894/1024 keys under concurrent rotation; fixed (atomic `compute`) + regression gate added; ledger entry recorded. The blind score (8) stands as the state at scoring time; this watch-item-confirmed-as-bug is exactly the calibration the ledger exists to capture. |
| 10 | Resource Management | 8 | carried (v24). `spawn_task` + `task_handles` JoinSet moved into `CoreCtx` intact; no lifecycle change. Not re-probed this run. |
| 11 | Semantic Correctness | 8 | carried down from v24's 9 (that 9 was compliance-hash-chain-specific). Core LWW/HLC semantics freshly green this run (`lww_newer_wins`, `lww_equal_timestamp_concurrent_data_converges`, `tombstone_gc_sweep_unpacks_hlc_timestamps` in the 82-test core run) — but consensus linearisability/anti-entropy not freshly probed, and the ledger history (equal-ts, HLC drift) warrants not re-asserting 9 lightly. |
| 12 | Robustness | 8 | carried (v24). Wire/decoder path (`framing.rs`) moved to core unchanged — `MAX_FRAME_BYTES` limit + decode byte-limit intact; framing proptest in the green core run. No fresh malformed-frame probe this run. |
| 13 | Security | 8 | carried (v24). `tls.rs`/`stream.rs` moved into core under the forwarded `tls` feature; no auth/crypto logic changed. The Run-24 tls-handshake ledger fix stands. Read-only this run. |
| 14 | Failure Mode Legibility | 8 | carried (v24). Tripwires (`commit_conflicts`, `sys_namespace_violations`) + typed verify errors unchanged. Read-only. |
| 15 | Performance | 7 | carried (v24). Hot paths (LWW merge, framing, fan-out) moved verbatim into core; no perf regression expected but **no benchmark run this run** → conservative per evidence gate. |
| 16 | Scalability | 7 | **Deep-dive.** `scan_prefix` confirmed bucketed by first path segment — O(\|bucket\|) not O(\|store\|) (`PrefixIndex`, `store.rs:180`). Documented cliff edge (Docker bridge iptables O(N²) at 100 nodes) persists with v1 mitigation (`GOSSIP_MAX_ACTIVE_CONNECTIONS`) + roadmapped v2 SWIM/UDP structural fix. No scale test (100-node / entry-volume) run this run → known cliff keeps it at 7. |
| 17 | Testability | 8 | **Deep-dive.** Design demonstrably injectable: substrate testable in isolation (82 core tests, no full cluster), feature-sliced runs (default / consensus-off) both green this run, components constructible per-crate. Held at 8 (not 9): the full tls/metrics/a2a/llm/compliance matrix and the 13 Docker scenarios were not exercised this run. |
| 18 | Test Architecture | 8 | **Deep-dive.** Healthy pyramid: 390 `#[test]`/`#[tokio::test]` fns across both crates, 13 integration scenarios, 2 fuzz targets, 3 proptest blocks (hlc/framing/store). M3 pushdown relocated handle tests into core (`schema/kv_handle_tests`) without loss. Held at 8: the M2 "consensus-off still forwards" invariant has no dedicated regression test (structurally guaranteed but untested — Probe 3); Docker scenarios not CI-run this session. |
| 19 | Observability | 8 | **Deep-dive.** Metrics on hot paths verified in core: `gossip_store_entries` (gauge), `gossip_anti_entropy_rounds_total`, `gossip_messages_received_total`, `gossip_signals_delivered_total`/`_rejected_total`, `gossip_rpc_latency_ms`. `/metrics` (Prometheus), `/stats`, `/ready`, `/health` endpoints present; tracing throughout. Read-only — `/metrics` not live-probed this run → 8. |
| 20 | Debuggability | 8 | **Deep-dive.** 44 HTTP routes incl. KV inspection (`/kv` GET/POST/DELETE, `/kv/keys`, `/kv/quorum`), per-slot consensus inspection (`/consensus/{slot}`, lease-aware), signal SSE (`/signals/{kind}`), `/stats` with the two tripwire counters. Strong introspection surface; not driven live this run → 8. |
| 21 | Operational Readiness | 8 | carried (v24). `consensus`-off minimal embed is a new deployment shape (193 tests green); gateway-free + standalone-core builds clean. No Docker scenario run this run. |
| 22 | Evolvability | 8 | carried (v24), reinforced: the v2 roadmap is *executing* — M1/M2/M3 landed as a clean structural refactor with zero wire change (v11 held) and zero test loss. The TaskCtx God-Object split + Layer I/II crate cut are debt being paid down, not accrued. Read-only assessment → 8. |
| 23 | Documentation | 8 | carried (v24). CLAUDE.md + ROADMAP updated for M1/M2/M3 (crate split, feature gate, handle table); execution records in `docs/plans/v2-m1`/`v2-m3`. Doc-vs-code spot-checks (dep counts, test totals, scan_prefix complexity) all matched reality this run. Docs not executable → 8. |
| 24 | Developer Experience | 8 | carried (v24). Build times observed this run: standalone core 20.7s, gateway-free full crate 47s, core test cycle 2.2s. `consensus`/`gateway` one-flag opt-outs; `-p` scoping works. Workspace ergonomics clean. |
| 25 | Dependency Hygiene | 8 | carried (v24). The M1 dep-tree win is now execution-verified: 52 core vs 139 full unique normal deps (`cargo tree`); `--no-default-features` + standalone-core both build. `Cargo.lock` present. No fresh `cargo audit` this run → held at 8. |
| — | **Floor (lowest 3)** | **7, 7, 8** | Performance (15), Scalability (16), Concurrency Correctness (9 — the standing `merge_peer_keys` non-`compute()` watch-item) |
| — | Mean (continuity footnote) | 8.0 | not a target; see M2 preamble |

## 2026-06-19 — Run 26 (M2)

Deep-dive dimensions this run (by rotation, Run 26 ≡ Runs 1/6/11/16/21 cycle):
1 (Philosophy), 2 (Conceptual Integrity), 3 (Architecture), 4 (Modularity),
5 (API Design). Next run by rotation: 6–10.

Diff since Run 25: PRs #44–#50 — WS-F federation kickoff + the new
`mycelium-agentfacts` companion crate (M16-A self-certified AgentFacts edge
endpoint; M16-B per-field-signed CRDT update layer; CRDT-assembled domain-facts
endpoint; OR-Map design note), a new public node-identity signing API
(`GossipAgent::sign_with_identity` / `identity_public_key`, tls), the shared
`crate::test_util::alloc_port` flake fix (PR #50, retiring the AddrInUse family),
and the conway_gpu dep-bloat removal (PR #40, wgpu out of dev-deps). (The larger
WS-C metabolism / elastic-governance / WS-E autonomic-provisioning program that
also merged in this window is carried at prior scores except where a fresh probe
or suite this run touched it — see notes.)

Execution evidence: `cargo test --lib --features tls,metrics,a2a,llm` →
**286 passed, 0 failed, 1 ignored** (64.6s); `cargo test -p mycelium-core --lib`
→ **122 passed, 0 failed, 1 ignored** (2.2s); `cargo test -p mycelium-agentfacts`
→ green (unit + 3 CRDT integration + doctests); `cargo clippy -p
mycelium-agentfacts --all-targets -- -D warnings` → clean (57.8s); `cargo build
--lib` → exit 0. Falsification: 3 probes, all PASS (acyclicity grep, namespace
ownership grep, new hostile-input regression test run green).

### Findings
None confirmed. All three falsification probes (run against the top dimensions)
passed:

- **Probe 1 (Architecture, #3, score 9) — acyclic inverted-dependency invariant.**
  `grep -rn "use mycelium::" mycelium-core/src/` → **0 hits**; `mycelium-core`'s
  Cargo.toml has no `mycelium` dependency. The "core cannot reference the layers
  above (would be a Cargo cycle)" claim remains a true compile-time guarantee, not
  a convention. Re-confirmed by 122 core tests green standalone. PASS.
- **Probe 2 (Architecture / Layer ownership, #3) — does the new governance code
  (`membership_governor.rs`, `tuning_governor.rs`) bypass documented prefixes?**
  Grep of every string literal it writes → only `sys/govern/`, `sys/govern/fleet`,
  `sys/govern/membership/`. Governance rides the owned `sys/govern/` prefix via the
  `FleetIntent` transport (`src/agent/intent.rs`: publish + gossip + evaporate +
  node-target + reconcile loop), agency-above / mechanism-in-core. No write to
  consensus/, no substrate modification. PASS (reinforces #1/#3).
- **Probe 3 (Philosophy/API/Robustness — WS-F self-cert surface) — can `verify()`
  be made to panic or accept a substitution on hostile input?** New regression
  test `mycelium-agentfacts/tests/falsification_run26.rs`: short/empty/oversized/
  zero signatures all return `false` (no panic); `read_verified_fields` for an
  unknown node yields an empty map (never a forged-through field). Both green. The
  surface is total. PASS. Combined with the in-crate tamper test
  (`per_field_merge_lww_and_forgery_rejection`: a forged `facts/{node}/…` write is
  LWW-accepted by the substrate but dropped at read), the detection-not-prevention
  posture holds end to end.

Minor observation (not a finding — does not break a documented invariant):
`crdt.rs::peer_identity_key` verifies fields against only the **current** identity
key (`bytes[..32]`), unlike the WS5 retained-key-set verify paths
(connection/consensus/rbac/audit) which try the full key history. After an identity
rotation, a still-fresh field signed by the retired key would fail verification
until republished. Safe in practice (AgentFacts fields are short-TTL and re-signed
on every change; current key is published first), but a divergence from the
retained-key-set pattern worth tracking if facts TTLs are ever lengthened.

### Calibration ledger
No new entry — no bug found this run. The standing `merge_peer_keys` watch-item
named in the Run 25 floor was fixed in that session (atomic `compute`, regression
gate `concurrent_merges_for_one_node_never_drop_a_key`); no replacement watch-item
this run.

| # | Dimension | Score | Notes |
|---|-----------|:-----:|-------|
| 1 | Philosophy / Coherence with Goal | 8 | **Deep-dive.** WS-F extends the companion-crate constructive-proof discipline to federation: `mycelium-agentfacts` builds entirely on the public API (run-dark by default, self-certified — "trust is the fetcher's decision", Core Principle 1), and the CRDT update layer is *exactly* the "missing protocol" NANDA's v0.3 names but doesn't deliver — LWW+HLC+anti-entropy + per-entry signatures = field-level merge. Governance ("management = intent + local reconcile", `intent.rs`) is the philosophy's elastic-management stance in code: evaporating soft-state, node-sovereign reconcile, never a command. Read philosophy.html §1–6 in full; no drift found. Caps at 8 (philosophy can't be execution-falsified; 9 needs external validation). |
| 2 | Conceptual Integrity | 8 | **Deep-dive.** One mind sustained across a 4th companion crate: agentfacts mirrors tuple-space/wasm-host idiom (public-API-only, `alloc_port` test helper, shared-cert-dir mTLS test pattern). `intent.rs` deliberately uses free functions over a generic `IntentGovernor<T>` and cites the Rule of Three for deferring abstraction — disciplined, not ad-hoc. The "stable substrate-shaped struct + single NANDA-mapping point" (`to_nanda_jsonld`) is the same decouple-from-churn instinct as the wire-version freeze. Read-only → 8. |
| 3 | Architecture | 9 | **Deep-dive.** Inverted-dependency invariant re-probed and green (Probe 1: core⊥mycelium, 0 grep hits + 122 core tests standalone). New federation + governance land *above* the substrate without touching it: WS-F is a separate crate; governance writes only `sys/govern/` (Probe 2) and is built on the `FleetIntent` KV-transport, agency-above/mechanism-in-core. Two new edge endpoints mount via `with_http_routes` (the sanctioned extension seam). Execution-backed this run (fresh acyclicity + namespace probes both PASS) → holds at 9. |
| 4 | Modularity | 8 | **Deep-dive.** Federation is now a *physically separable* crate depending only on `mycelium` (default-features = false, tls+gateway) — composability proof #4. Within it, `crdt.rs` (M16-B push) and `lib.rs`/`http.rs` (M16-A pull) are cleanly separated; `SignedField`/`AgentFacts` are independent units. Held at 8 (not 9): the in-crate sub-handles still share `Arc<CoreCtx>` mutable state (sanctioned cohesion, unchanged since Run 25); the new agent-layer governors (`membership_governor`, `tuning_governor`) share `TaskCtx`. |
| 5 | API Design | 8 | **Deep-dive.** New public surface is minimal and well-shaped: `sign_with_identity(&[u8]) -> Option<[u8;64]>` + `identity_public_key() -> Option<[u8;32]>` (tls-gated, `None` when no identity — total, hard to misuse). `FleetIntent` trait exposes exactly 3 facets (written_at_ms/stamp/target); everything policy stays in the governor. AgentFacts `from_agent`/`signed_agent_facts` return `Option` (no identity ⇒ `None`, not a panic). Read-only, but the surface is clean → 8. |
| 6 | Error Handling Model | 8 | carried (v25). No error-type changes this run; WS-F surface uses `Option` returns + `bool` queued-writes consistently (no new `Result` taxonomy). |
| 7 | Configurability | 8 | carried (v25). WS-C auto-derivation + hot-reload and elastic governor config are new knobs (default-on / opt-in), structurally consistent with the existing feature/config split; not deep-dived this run. |
| 8 | Language Best Practices | 8 | Fresh-but-scoped evidence: `clippy -p mycelium-agentfacts --all-targets -D warnings` clean this run; the new code (`intent.rs`, `crdt.rs`, `lib.rs`) is idiomatic (let-else, `is_none_or`, `Option` combinators, no `unwrap` outside tests). Full-crate clippy not re-run → honest 8, not 9. |
| 9 | Concurrency Correctness | 8 | carried (v25). `merge_peer_keys` race fixed last session; no new concurrency-bearing path probed this run beyond the intent reconcile loop (select! over watch + tick + shutdown — standard, no shared lock). Lock-order table unchanged. |
| 10 | Resource Management | 8 | carried (v25). `spawn_intent_reconciler` exits on shutdown (`shutdown.wait_for`); governors are `spawn_task`-tracked. No lifecycle regression; not re-probed. |
| 11 | Semantic Correctness | 8 | carried (v25), reinforced: the WS-F CRDT merge (LWW+HLC per field, distinct-key concurrency, forgery-drop-at-read) is freshly green (`per_field_merge_lww_and_forgery_rejection`, `intra_domain_field_gossips_and_verifies_cross_node`, `domain_facts_assembles_verified_per_node_board`). Consensus linearisability/anti-entropy not re-probed → not raised to 9. |
| 12 | Robustness | 8 | carried (v25), reinforced by Probe 3: the WS-F verify surface is total against malformed/hostile signatures + unknown nodes (new regression test green). Broader malformed-frame/decoder paths not re-probed this run → 8. |
| 13 | Security | 8 | carried (v25). New self-certified signing (Ed25519 over canonical JSON), per-field signatures, forgery rejection at read, hostile-input-safe `verify()` — all freshly green. Detection-not-prevention posture intact (forged KV write LWW-accepted but never verifies). No external audit + broader mTLS/RBAC/audit not re-probed → 8. |
| 14 | Failure Mode Legibility | 8 | carried (v25). Tripwires + typed verify errors unchanged; WS-F drops (forged/stale/unknown-key) are silent-at-read by design (the served board simply omits them) — legible via absence, consistent with evaporation. |
| 15 | Performance | 7 | carried (v25). No benchmark run this run; WS-F adds JSON canonicalisation + Ed25519 verify on the *edge read* path (not the gossip hot path), bounded by `scan_prefix(facts/)`. Conservative per evidence gate. |
| 16 | Scalability | 7 | carried (v25). `domain_facts` is O(facts entries) per edge pull; no scale test (100-node / entry-volume) run this run; the documented Docker-bridge iptables cliff persists (WS-B SWIM + WS-C auto-derivation mitigate, don't eliminate). |
| 17 | Testability | 8 | carried (v25), reinforced: WS-F crate is testable in isolation (per-crate tls agents over shared cert_dir, no full external cluster); added a probe regression test this run with no harness friction. Full feature/Docker matrix not exercised → 8. |
| 18 | Test Architecture | 8 | carried (v25). Suite grew (new `falsification_run26.rs` + 3 CRDT integration tests + per-field/forgery/cross-node coverage); pyramid stays healthy. The "consensus-off still forwards" coverage wart (Run 25 Probe 3) is unchanged → 8. |
| 19 | Observability | 8 | carried (v25). Elastic Track 3 added `/gateway/govern` + governance Prometheus metrics + audit; `/metrics`/`/stats`/`/ready`/`/health` unchanged. `/metrics` not live-probed this run → 8. |
| 20 | Debuggability | 8 | carried (v25). New edge endpoints (`/.well-known/agent-facts`, domain-facts board) add introspection surface; KV/consensus/SSE inspection routes unchanged. Not driven live this run → 8. |
| 21 | Operational Readiness | 8 | carried (v25). WS-E autonomic provisioning + elastic governance + WS-F run-dark federation are new operational shapes; agentfacts is opt-in/operator-served. No Docker scenario run this run. |
| 22 | Evolvability | 8 | carried (v25), reinforced: the v2 roadmap is executing at pace — WS-B/C/E/F all shipped in-window with the wire version held at v12 and no test loss. AgentFacts' single-mapping-point decoupling from NANDA's churning field names is forward-compat by design. Read-only → 8. |
| 23 | Documentation | 8 | carried (v25). New crate docs (agentfacts lib/crdt module docs are unusually clear on the NANDA-decoupling rationale), WS-C/elastic plans, OR-Map design note, cert-rotation/sso/crown-jewel ops docs. Doc-vs-code spot-checks (companion-crate contract, namespace ownership) matched reality. Not executable → 8. |
| 24 | Developer Experience | 8 | carried (v25). conway_gpu/wgpu removed from dev-deps (lighter clean build); shared `alloc_port` test util ends the AddrInUse flake family. Build/test cycles observed green this run (core 2.2s, agentfacts clippy 57.8s). → 8. |
| 25 | Dependency Hygiene | 8 | carried (v25). agentfacts adds ed25519-dalek/serde_json/base64/axum (all already in the parent tree) + reqwest (dev-only); wgpu dev-dep removed (PR #40). `Cargo.lock` present. `--no-default-features` not re-run this run + no fresh `cargo audit` → 8. |
| — | **Floor (lowest 3)** | **7, 7, 8** | Performance (15), Scalability (16), and the lowest 8-tier — Robustness (12)/Security (13): broad paths verified only on the new WS-F surface this run, not end to end |
| — | Mean (continuity footnote) | 8.0 | not a target; see M2 preamble (sum 199/25 = 7.96) |

## 2026-06-21 — Run 27 (M2)

Deep-dive dimensions this run (by rotation): Concurrency Correctness (9), Semantic Correctness (11), Security (13), Scalability (16), Evolvability (22). Execution evidence: `cargo test --lib` default = **286 passed / 1 failed (flake) / 1 ignored** (re-run 286/0; flake characterised below); companion libs green standalone — blackboard **15**, tuple-space **31**, core **125** (+3 kept probes → 128); `cargo clippy --lib` **0 warnings**; `cargo build --lib --no-default-features` clean; three falsification probes (Architecture/Concurrency/Semantic) all PASS and kept as regression tests. Diff since Run 26: ~30 PRs — the 11-demo coop suite + two-audience docs, **WS-D** (M6 + CT revocation log), **WS-F** (schema migrations), **WS-G** (M13 keyed-take + the `mycelium-blackboard` companion crate), **M7** distributed rate-limiting, **M10** fence-free live timing reconfiguration. v2.0 acceptance gate now MET — all 16 milestones (M1–M16) delivered.

### Findings
- **Minor — Test Architecture (18):** `test_wsc_m8_auto_config_cluster_converges` flakes under *parallel* execution — `start()` errors on transient bind/resource contention (~1 in 3 full-suite runs; **5/5 in isolation**). Product feature (M8 auto-config) is correct; the harness is non-deterministic under load. Canary comment left on the test; calibration-ledger entry added. Caps Test Architecture at 6.

| # | Dimension | Score | Notes |
|---|-----------|:-----:|-------|
| 1 | Philosophy / Coherence with Goal | 8 | M10's **consensus-fence decline** (`timing_governor` docs: a fence would import a coordinator to prevent transient variation the self-healing substrate tolerates — CP1) and the blackboard `rd`/`in` split are textbook philosophy-coherent. No drift across the huge diff. Qualitative → caps at 8. |
| 2 | Conceptual Integrity | 8 | One mind sustained: `mycelium-blackboard` faithfully mirrors the tuple-space idiom; the tripwire family extended consistently (`cap_authz_violations`/`schema_mismatch`/`rate_limited_senders` all follow `commit_conflicts`); `TimingIntent` reuses the `FleetIntent` transport. Declined-with-evidence decisions (G2 overlay, M10 fence) show consistent judgment. Read-only → 8. |
| 3 | Architecture | 9 | **Deep-dive-adjacent. Probe 1 PASS** (executable): `cargo tree -p mycelium-core -i mycelium` → no edge (inverted-dependency = compile-time guarantee); `sys/rate/` written only by `rate.rs` (M7 namespace discipline). M7 is detection-only (KV evidence; the limiter is a read-side decision); M10 rides `HotConfig` + intent — both land without a layer violation. Execution-backed → 9. |
| 4 | Modularity | 8 | Companion crates build + test **standalone** (blackboard 15, tuple-space 31) — composability proof holds for a 5th crate. Handle boundaries intact. Held at 8: agent-layer governors still share `TaskCtx` (sanctioned cohesion); no new modularity-specific probe. |
| 5 | API Design | 8 | New surface is minimal + total: `claim(&Predicate) -> Option<Fact>` (non-blocking, loser gets `None`), `govern_timing(h, r, target)`, `migrate_payload` returns `NoMigrationPath` not a guess, `revoke_identity_key`. Consistent with the handle idiom; no footgun found. Read-only → 8. |
| 6 | Error Handling Model | 8 | `BlackboardError`/`MigrationError` follow the typed-`#[non_exhaustive]` idiom; `Io` variant drops `PartialEq` exactly as `TupleError` does (tests use `matches!`). No product-path `unwrap`. carried/re-checked → 8. |
| 7 | Configurability | 8 | New knobs off-by-default and env-wired (`GOSSIP_RATE_OBSERVATION`, hot timing params); `--no-default-features` builds (fresh). Structurally consistent. 8. |
| 8 | Language Best Practices | 8 | `cargo clippy --lib` 0 warnings (fresh); idiomatic papaya `compute`/`pin`, deref-coercion at call sites. Not deep-dived; clippy-clean is necessary-not-sufficient for 9 → 8. |
| 9 | Concurrency Correctness | 9 | **Deep-dive. Probe 2 PASS** (executable): the M7 rate decider `reconcile_throttle` is idempotent (re-run = no drift) and threshold is strict `>` (exactly-at-threshold not throttled — no off-by-one). M7's connection-hot-path change reads the throttle once/window (off the frame path) + is zero-overhead when disabled; `rate_throttle` is papaya (lock-free). Blackboard claim single-owner (16-thread test green). Ledger-aware (2 prior entries) — probed the invariant families directly. Execution-backed → 9. |
| 10 | Resource Management | 8 | `Blackboard::shutdown` aborts tasks + retracts ads (failover test exercises it); capability TTL/evaporation drives promotion. No new leak found. carried → 8. |
| 11 | Semantic Correctness | 9 | **Deep-dive. Probe 3 PASS** (executable): equal-timestamp LWW **converges regardless of apply order** (deterministic byte tiebreak) and data does **not** resurrect over an equal-ts tombstone — directly re-probes the Run-16 ledger bug, now a kept regression gate. Schema migration: no path → `NoMigrationPath`, never a partial apply. Blackboard claim exactly-once. Execution-backed → 9. |
| 12 | Robustness | 8 | Corrupt-tail WAL truncation (blackboard + tuple), v1-replay-accept, malformed-frame survival; M7 off-by-default = no robustness regression on the hot path. Not deep-dived this run → 8. |
| 13 | Security | 8 | **Deep-dive (read-level).** WS-D shipped end-to-end: key revocation (excluded from every retained-key verify path), RFC-6962 Merkle inclusion proofs (`/gateway/transparency`), resolve-time capability ACL verifying *signed* roles against the retained-non-revoked key set, consensus-distributed policy. Detection-not-prevention throughout. Capped at **8**: the `compliance` suite was **not** re-run *this* analysis run (CI-green during the session, but M2 requires fresh-this-run execution for 9). |
| 14 | Failure Mode Legibility | 8 | New legible signals: `rate_limited_senders`/`cap_authz_violations`/`schema_mismatch` on `/stats`; `NoMigrationPath{from,to}`; `AuditVerifyError` names the offending seq. carried/extended → 8. |
| 15 | Performance | 8 | M7 adds one branch on the inbound frame path, taken only when a limit/observation is active; the KV evidence write is once-per-peer-per-second on rollover (off the frame path). Blackboard claim non-blocking. No fresh benchmark this run → 8. |
| 16 | Scalability | 7 | **Deep-dive.** Two honest considerations surfaced: (a) M7's `sys/rate/` evidence is `O(observers × senders)` entries — bounded by `max_active_connections` (O(N×K)) but a real volume cost at large N, and the decider re-scans it every 2 s; (b) the blackboard/tuple **claim path is single-primary** (horizontal claim throughput does not scale with N — by design, with failover not sharding). No cliff, but not unbounded. The substrate itself scales (WS-B SWIM/partial-mesh/Merkle). No fresh scale run → 7. |
| 17 | Testability | 8 | The three probes this run were each writable as pure in-isolation tests (`reconcile_throttle`, `lww_wins`, `BoardStore::transient`) — strong evidence the design is injectable/deterministic at the unit level. The *parallel-suite* non-determinism (finding) is a harness, not a design, issue. 8. |
| 18 | Test Architecture | 6 | **FINDING (Minor):** the default unit suite is non-deterministic under parallel load (`test_wsc_m8_auto_config_cluster_converges` flakes ~1/3; 5/5 in isolation). Large, layered suite (286 default + 15 blackboard + 31 tuple + 125 core + 13 Docker scenarios + fuzz) and the 3 new probes grew it — but a 1-in-3 local flake caps the dimension at 6. |
| 19 | Observability | 8 | `/stats` now carries five tripwire counters + `task_count`; `/gateway/transparency`, `/gateway/govern/timing`, the blackboard/tuple gateways. `/metrics` (Prometheus) intact. carried/extended → 8. |
| 20 | Debuggability | 8 | Tripwires + typed errors name the offending entity (sender / seq / from→to / claim id). carried → 8. |
| 21 | Operational Readiness | 8 | `/ready`, `shutdown_with_timeout`, the governance surfaces (`/gateway/govern/timing` + audit), M7/M10 operator knobs, blackboard/tuple gateways + py/ts SDKs. carried/extended → 8. |
| 22 | Evolvability | 8 | **Deep-dive.** Wire stays **v12** — verified M7 (KV-only) and M10 (HotConfig + intent KV) introduce **no `WIRE_VERSION` bump**; `PREV = 11` keeps the rolling-upgrade window. The WS-F schema-migration engine *is* the evolvability tooling. Debt is reasoned (G2/M10 declined-with-evidence) and paid (this run's flake found+documented, scorecard refreshed). v2.0 all-16 complete. No single execution probe lifts to 9 → 8. |
| 23 | Documentation | 8 | Dev docs integrated this session (blackboard into 00-concepts/cookbook/14-patterns; **stale M13 ref fixed**; exactly-once contract doc; M7/M10 + WS-G plans; scorecard refreshed to reality). Qualitative → 8. |
| 24 | Developer Experience | 8 | CLAUDE.md gained a blackboard section + the on-ramp is current; clean clippy. The parallel flake mildly dents DX (an occasional local red). 8. |
| 25 | Dependency Hygiene | 8 | `--no-default-features` builds clean (fresh); blackboard adds only standard deps (tokio/bytes/parking_lot); companion crates are public-API-only. `Cargo.lock` present; bincode retired (WS-B). No fresh `cargo audit` this run → 8. |
| — | **Floor (lowest 3)** | **6, 7, 8** | Test Architecture (18) · Scalability (16) · Performance (15) [representative 8-tier] |
| — | Mean (continuity footnote) | 8.0 | not a target; see M2 preamble (sum 200/25 = 8.0) |

## 2026-07-02 — Run 28 (M2)

Deep-dive dimensions this run (by rotation, resuming the cycle Run 27 deviated from): 6 (Error Handling), 7 (Configurability), 8 (Language Best Practices), 9 (Concurrency Correctness), 10 (Resource Management). Also re-examined 12/16/25/23/18/14 — the run's findings and the diff (cluster_name ops feature, alloc_port flake fix, quinn-proto RUSTSEC bump, ~15 docs PRs) touch them. Execution evidence: `cargo test --lib` default = **287 passed / 0 failed / 1 ignored** (pre-probe baseline; the Run-27 flake did NOT recur — PR #110's alloc_port fix held under full parallel load); `mycelium-core` **127/0**; `cargo clippy --lib --tests` clean (exit 0); `cargo audit` executed (1 vulnerability found — Finding 3); four falsification probes written and executed (two PASS kept as regression gates, two CONFIRMED findings left as canaries). Diff since Run 27: PRs #110–#124 — small code surface (cluster_name → /stats + /metrics label + AgentFacts; test-port ephemeral-floor fix; quinn-proto 0.11.15) + large docs volume (publications corpus ×4 DOIs, customer deck, plans index, Legible Emergence plan).

### Findings

- **Major — Error Handling (6), Robustness (12), Scalability (16):** a `kv().set()` value whose encoded frame exceeds `MAX_FRAME_BYTES` (10 MiB) is accepted (`true`, applied locally, WAL-appended) but **can never leave the node, and no error surfaces anywhere on the caller's side**. The per-peer writer treats the resulting `FrameTooLarge` as a connection failure: it tears down the healthy TCP link, drops all queued frames, and enters backoff (`writer.rs:144`). Anti-entropy cannot repair the divergence: `StateResponse` is a **single unchunked frame** — an oversized response is skipped with a responder-side `warn!` and never retried (`connection.rs:361`, "StateRequest is only sent on first contact"). Consequences: (a) silent permanent divergence for any >10 MiB value; (b) one bad write disrupts unrelated traffic; (c) a store whose full-dump/divergent set exceeds ~10 MiB can never bootstrap a late joiner — an undocumented hard scalability ceiling (the entry-volume scale test's 5 000×1 KiB sits comfortably below it; nothing gates a user's payload). Repro: `test_oversized_value_is_accepted_locally_but_silently_never_propagates` (lib_tests, **canary — documents the wrong behaviour, flip when fixed**; confirmed live: small canary key propagated, 10 MiB+64 KiB key visible on A, absent on B). Suggested fix: size-check in `kv_set`/`make_gossip_update` returning a typed error, and chunk `StateResponse`.
- **Major — Concurrency Correctness (9):** `AgentStateMachine::transition` is check-then-act. Policy guards read budget counters that are incremented only *after* commit, and the commit (`*self.current.lock() = to`) never re-reads `current` after the up-to-30 s approval `await` — so (a) two approval-gated `Invoking` transitions racing through the await **both pass `tool_budget = 1`** (probe committed 2 of 1), and (b) a state committed during the await (e.g. the timeout executor's `Failed`) is silently overwritten, resurrecting a failed agent. `force_failed_transition` explicitly guards the same race in the opposite direction, so the family was known. Present since 2026-05-21 (b0728e1) — spans Runs 13–27 incl. Run 27's execution-backed 9 (ledger entry added). Repro: `state_machine.rs::tool_budget_bypassed_by_concurrent_approval_gated_transitions` (**canary**). Suggested fix: hold the commit lock across guard re-check, and reserve budget at check time (or CAS on `from`).
- **Major — Dependency Hygiene (25):** `cargo audit` (fresh this run): **RUSTSEC-2026-0188** — `wasmtime-wasi` 45.0.2, CVSS 6.5, WASI hard links/renames bypass `FilePerms` for the destination; fix ≥ 45.0.3 available. This is the WS-E wasm-host **sandbox** surface (provisioned-artifact isolation). Published 2026-06-24 — three days after Run 27 scored 25 an 8 without running audit (ledger entry added); the quinn-proto bump (PR #124) caught the *other* June advisory but not this one. CI's `audit` job gates `cargo audit`, so the next push goes red regardless. Plus 5 allowed warnings (unmaintained: `backoff`, `instant`, `rustls-pemfile`, dev-only `bincode`; unsound: `anyhow` 1.0.102 downcast_mut, RUSTSEC-2026-0190 — worth a lock bump sweep).
- **Minor — Documentation (23):** CLAUDE.md's Lock-Order Table claims to list "**all** `Mutex` and `RwLock` sites in the codebase" (8 entries) and that "`std::sync::Mutex` … is used throughout" — both false: ≥ 7 undocumented lock fields exist (`state_machine.rs` `policy`/`current`/`task_id`/`timeout_handle` — **parking_lot**, not std; `swim.rs` `pending`/`membership`; `capability_ops.rs` `FilterOpacityRegistry::entries`). Spot-checks found no nesting violations (the flat-acquisition invariant appears to hold; parking_lot guards are equally `!Send` across `await`), but the load-bearing concurrency doc has drifted — precisely the doc a future session trusts when adding lock #9.
- **Minor — Failure Mode Legibility (14):** `shutdown_with_timeout`'s drain-timeout `warn!` (`lifecycle.rs:598–602`) reports a wrong count: the drained `JoinSet` was swapped into the `drain` future, so when the timeout cancels it the stuck tasks are dropped (JoinSet drop aborts them — behaviour is fine) and the re-locked `task_handles` it counts is the *empty* replacement — the warning will typically say 0 tasks failed to exit. Read-level finding (not probed); noted, not capped.

Probe disposition: `apply_env_overrides_rejects_malformed_value_with_typed_error` (dim 7 — PASS: malformed/overflow/empty `GOSSIP_BIND_PORT` → typed `GossipError::Parse`, field untouched, no panic) and `test_capability_reg_drop_tombstones_advertisement` (dim 10 — PASS: drop retracts the `cap/` key) kept as permanent regression gates. The two failing probes left as documented canaries that flip when fixed.

**Same-day remediation (post-scoring — scores above reflect the state at audit time):** all three Major findings and both Minor findings fixed the same day. Finding 1: `MAX_KV_WRITE_BYTES` reject guard in `kv_set`/`kv_set_async`, writer keeps the connection on `FrameTooLarge` (drops only the offending frame, counted in `dropped_frames`), `StateResponse` chunked with per-entry skip for un-frameable legacy entries; both canaries flipped to regression gates and a new 12 MiB late-joiner + poison-entry gate added. Finding 2: `try_commit` validate-and-swap (budget check + reserve atomic under the state lock; commit retried if the state moved during the approval await); `force_failed_transition` counter resets moved under the same lock. Finding 3: `wasmtime-wasi` → 45.0.3, `anyhow` → 1.0.103, `cargo audit` clean. Finding 4: lock-order table extended to rows 9–14 (incl. the parking_lot flavour) + a keep-it-honest rule. Finding 5: the drained `JoinSet` now outlives the timeout, so the abandon-warn counts the actual stuck tasks. Also fixed: the rustc-1.96 `int_plus_one` lint (`config.rs:1521`) so `clippy -D warnings` is green on the floating stable toolchain. Ledger entries updated in place. Score effect lands in Run 29 per the increase-needs-artifact rule.

| # | Dimension | Score | Notes |
|---|-----------|:-----:|-------|
| 1 | Philosophy / Coherence with Goal | 8 | carried (v27). Diff is docs-heavy (publications corpus, deck, plans) and consistent with the philosophy; cluster_name is deliberately a label with "no effect on gossip, identity, or membership" — coherent. |
| 2 | Conceptual Integrity | 8 | carried (v27). cluster_name plumbed uniformly (/stats + metrics global label + AgentFacts `cluster`, absent-omitted); one nit: `state_machine.rs` silently uses parking_lot while the rest of the tree is std::sync (see Finding 4). |
| 3 | Architecture | 9 | Inverted-dependency guarantee re-verified by execution this run: `mycelium-core` builds + tests **standalone** (127/0) — the compile-time non-reference to `mycelium` is exercised by every `-p mycelium-core` run. Namespace discipline intact in the diff. |
| 4 | Modularity | 8 | Core standalone 127/0 (fresh); handle boundaries untouched by diff. carried (v27) otherwise. |
| 5 | API Design | 8 | carried (v27). `cluster_name()` accessor is minimal/consistent. Note: Finding 1 exposes a surface wart — `set() -> bool` means "queued", so unsendable values are indistinguishable from success (scored under 6). |
| 6 | Error Handling Model | **6** | **Deep-dive; FINDING (Major, capped).** The typed-error *design* is genuinely strong (`GossipError` non_exhaustive + per-domain enums, all `std::error::Error`, recoverable-vs-startup documented) — but the oversized-value path shows a documented error variant (`FrameTooLarge`, "Reduce the value size…") that **no caller can ever receive**: `kv.set` returns `true` and the failure is absorbed in the writer. Canary kept. |
| 7 | Configurability | 8 | **Deep-dive; probe PASS.** 45 `GOSSIP_*` env overrides, `validate()` with typed field-named errors (~30 checks), per-field doc-comments, TOML round-trip test; new `GOSSIP_CLUSTER_NAME` empty→None handled. Probe (malformed env → typed error, field untouched) kept as a gate. Minor wart: cluster_name stored untrimmed. 96-pub-field surface is large but disciplined. |
| 8 | Language Best Practices | 8 | **Deep-dive.** `#![deny(unsafe_code)]` (lib + tuple crate), zero product-path unwraps without documented infallibility, clippy clean this run (fresh, exit 0). Held at 8: `metrics` recorder `expect` (`http.rs:113`) can panic a host app that installed its own global recorder — a library-embed footgun; and the std/parking_lot split is un-flagged idiom divergence. |
| 9 | Concurrency Correctness | **6** | **Deep-dive; FINDING (Major, capped).** `transition()` check-then-act confirmed by probe (2 of budget-1 admitted). Third ledger entry for this dimension — the recurring shape is again "check, await/defer, act on stale read," the exact family CLAUDE.md's papaya rules warn about, this time on a parking_lot path outside the documented lock table. Everything else spot-checked held (flat lock acquisitions, guards released pre-await, swim/capability leaf locks clean). |
| 10 | Resource Management | 8 | **Deep-dive; probe PASS** (capability drop → tombstone, kept as gate). Shutdown path re-read: tombstone sweep, CAS to Stopped, per-peer writer signalling, JoinSet drain-with-timeout all sound; the drain-timeout *log* miscounts (Finding 5, legibility not leak). RAII guards throughout (`OpacityDropGuard`, `ActiveHandlerGuard`, `ListenerGuard`, tuple `BulkServeHandle`). |
| 11 | Semantic Correctness | 8 | Kept Run-16/27 LWW + equal-ts regression gates executed inside the fresh 287/0 suite. Held at 8 (was 9): Finding 1 breaks the *anti-entropy progress* guarantee at >frame-size divergence — merge logic is right, but the documented "anti-entropy repairs divergence" claim now has a known envelope. |
| 12 | Robustness | **6** | **FINDING (Major, capped, shared with 6).** A single locally-originated oversized write tears down healthy connections, drops unrelated queued frames, and enters backoff — degradation is neither graceful nor contained. Inbound-path hardening (decode limits, fuzz gates) remains good; the egress path lacks the same discipline. |
| 13 | Security | 8 | carried (v27). WS-D surface unchanged this diff. RUSTSEC-2026-0188 scored under 25 (it is the wasm-host companion's sandbox dep, not shipped `mycelium` core); noted here because sandbox escape-adjacent. `compliance` suite not re-run this run. |
| 14 | Failure Mode Legibility | 7 | Finding 1 is substantially a legibility failure (the *only* trace of a permanently-diverged write is a responder-side `warn!`; nothing on the writer, `/stats`, or the caller) + Finding 5 (drain-timeout warn miscounts). Tripwire/typed-error work remains strong; evidence-cited step down from 8. |
| 15 | Performance | 8 | carried (v27). Diff adds one Option read on /stats and a metrics global label — no hot-path change. |
| 16 | Scalability | **6** | **FINDING (Major, capped, shared with 6/12).** The unchunked `StateResponse` is a hard ceiling orthogonal to node-count: total store (full-dump path to a v11/fresh peer) or per-bucket divergence > ~10 MiB ⇒ late joiners never converge, with only a warn on the responder. Previously scored 7 on honest volume-cost grounds; the ceiling is sharper than that — it's a cliff, and it's undocumented outside the code. |
| 17 | Testability | 8 | All four probes this run were writable as isolated tests (pure config fn; paused-time canary; two-node live pair on `alloc_port`) — injectability holds. PR #110's ephemeral-floor fix removed the Run-27 flake at the source (verified: full parallel suite green). |
| 18 | Test Architecture | 8 | Recovered from Run-27's 6: the parallel flake was fixed at source (PR #110, shared bind-verified `alloc_port`) and the full default suite ran green under parallel load this run (287/0, fresh). Suite grew by 4 (2 gates + 2 canaries). Fuzz targets still CI-only. |
| 19 | Observability | 8 | cluster_name lands as `/stats` field + Prometheus **global label** + AgentFacts `cluster` — the right mechanism (label once, every series tagged). No fresh endpoint probe this run → 8. |
| 20 | Debuggability | 8 | carried (v27). |
| 21 | Operational Readiness | 8 | carried (v27); cluster_name closes a real multi-environment ops gap (one Grafana, N clusters). |
| 22 | Evolvability | 8 | carried (v27). Wire stays v12/PREV=11; diff introduces no format change (AgentFacts field is additive-omitted). |
| 23 | Documentation | 7 | **FINDING (Minor).** The docs *volume* this diff is impressive (publications corpus with DOIs, plans index, docs/README map, two-audience ops docs) — but the Lock-Order Table's completeness claim is false (≥7 missing sites, wrong Mutex flavour) and it is exactly the doc future concurrency work leans on. Evidence-cited step down from 8; cheap to fix. |
| 24 | Developer Experience | 8 | carried (v27); the flake fix improves day-one DX (deterministic green suite, verified this run). Heads-up found while gating this run's probe code: `cargo clippy -p mycelium-core --lib --tests -- -D warnings` **fails at HEAD** on pre-existing test code (`config.rs:1521`, `clippy::int_plus_one`, new in rustc 1.96 — `rust-toolchain.toml` floats `stable`, so CI goes red when runners pick up 1.96). One-line fix (`n + 1 <= 1` → `n < 1`) or pin the toolchain. |
| 25 | Dependency Hygiene | **6** | **FINDING (Major, capped).** Fresh `cargo audit`: RUSTSEC-2026-0188 open in `Cargo.lock` with a fix available; CI audit job will fail the next push. Pattern note: this is the second audit-window miss (Run 19's `bytes`/`tracing-subscriber` was the first) — advisories published between runs go unseen because audit only runs inside analysis runs; a scheduled audit job (cron, not just push-gated) would close the window. |
| — | **Floor (lowest 3)** | **6, 6, 6** | Error Handling (6) · Concurrency Correctness (9) · Robustness (12) — with Scalability (16) and Dependency Hygiene (25) also at 6 |
| — | Mean (continuity footnote) | 7.6 | not a target; see M2 preamble (sum 189/25) |

## 2026-07-02 — Run 29 (M2)

Deep-dive dimensions this run (by rotation): 11 (Semantic Correctness), 12 (Robustness), 13 (Security), 14 (Failure Mode Legibility), 15 (Performance). Also re-scored the five dimensions Run 28 capped at 6 (Error Handling, Concurrency, Robustness, Scalability, Dependency Hygiene) and the two docs dimensions (Documentation, Developer Experience) — the diff is the same-day remediation of all five Run-28 findings **plus** the LLM-wiki adoption. Execution evidence: `cargo test --lib` = **291 passed / 0 failed / 1 ignored**, run **twice** (75s + 84s — deterministic, the Run-27 flake stays retired); `mycelium-core --lib` **128/0**; `cargo audit` **0 vulnerabilities** (4 unmaintained-only allowed warnings); three falsification probes (Semantic/Architecture/Concurrency) all PASS, the new one kept as a regression gate; two `/wiki-lint` passes this session (7 doc-vs-code findings fixed → the lock-order table is now executably complete).

This is a **recovery run**: no code changed since Run 28 (the diff is docs — the wiki adoption + CLAUDE.md thinning + lint fixes), but every Run-28 finding shipped a verified fix *within* Run 28's own commits, so the five dimensions that were finding-capped at 6 now re-score against green gates. Scores rise only where a gate ran green **this** run (per the increase-needs-artifact rule); dimensions with no fresh evidence stay at 8.

### Findings
None — all three falsification probes passed. (1) Semantic: a new 24-permutation equal-timestamp convergence probe across **three** data writers **plus** a tombstone tie — every apply order converges (tombstone wins the tie; else lexicographically-greatest value; data never resurrects over an equal-ts tombstone), extending Run-27's two-writer probe; kept as `test_equal_timestamp_lww_converges_across_three_writers_all_orders`. (2) Architecture: executable inversion check — `grep` finds **zero** `mycelium::`/`use mycelium` references in `mycelium-core/src/` and `mycelium` is absent from core's `Cargo.toml` (a different angle than Run-27's `cargo tree`). (3) Concurrency: executable lock-table completeness — field-grep **plus** alias-grep (`SenderLog`, `GossipRxs`) reconcile 1:1 against the table's 18 rows, no unlisted lock site; confirms this session's `/wiki-lint` fix closed the Run-28 drift.

| # | Dimension | Score | Notes |
|---|-----------|:-----:|-------|
| 1 | Philosophy / Coherence with Goal | 8 | carried (v28). `philosophy.html` unchanged; docs-only diff stays coherent (the wiki is explicitly "code is canon, cite don't fork"). |
| 2 | Conceptual Integrity | 8 | carried (v28). The Run-28 nit (parking_lot vs std split "silent") is now *documented* in the lock-order table (rows 9–11 flagged as parking_lot) — no longer silent, but a read-only improvement → held 8. |
| 3 | Architecture | 9 | **Probe PASS (executable).** Zero upward references from `mycelium-core` (grep) + absent from core Cargo.toml + `mycelium-core --lib` 128/0 standalone this run. Inversion is a compile-time guarantee, re-verified a fresh way. |
| 4 | Modularity | 8 | Core standalone 128/0 (fresh); handle boundaries untouched. carried (v28). |
| 5 | API Design | 8 | carried (v28). The Run-28 `set() -> bool` wart is now *partly* mitigated (oversized → `false` + `warn!` + documented in the KvHandle doc-comment) but bool still conflates "channel full" with "rejected" → held 8. |
| 6 | Error Handling Model | 8 | **Recovered from 6.** The Run-28 Major (unreachable `FrameTooLarge`) is fixed: `kv_set`/`kv_set_async` reject oversized writes with `false` before applying; gate `test_oversized_value_is_rejected_outright_and_cluster_stays_healthy` green in the 291/0 run. Back to the design-strong 8 (no fresh whole-model stress → not 9). |
| 7 | Configurability | 8 | carried (v28). Env-robustness gate + the new `env_test_lock()` serialisation both green in 291/0. |
| 8 | Language Best Practices | 8 | carried (v28). `int_plus_one` (rustc 1.96) fixed → `clippy -p mycelium-core -- -D warnings` clean; the `metrics` recorder `expect` footgun still stands → held 8. |
| 9 | Concurrency Correctness | 8 | **Recovered from 6. Probe PASS (executable).** The `transition()` race is fixed (`try_commit` validate-and-swap + budget reserve under the state lock); gate `tool_budget_enforced_under_concurrent_approval_gated_transitions` green in 291/0, and the lock-table-completeness probe passed. Not 9: whole-dimension re-sweep not attempted; recovery is fix-verified, not exhaustive. |
| 10 | Resource Management | 8 | carried (v28). Drain-timeout warn miscount (Run-28 Finding 5) fixed — the drained `JoinSet` outlives the timeout. |
| 11 | Semantic Correctness | 9 | **Deep-dive. Probe PASS (executable).** New 24-perm 3-writer + tombstone-tie convergence gate green (`apply_and_notify` path). Run-28's anti-entropy-envelope concern is *fixed* (chunked `StateResponse`); gate `test_late_joiner_converges_past_frame_sized_store_via_chunked_anti_entropy` green in 291/0. LWW+HLC (drift clamp, symmetric freshness) re-read at source, sound. |
| 12 | Robustness | 8 | **Deep-dive. Recovered from 6.** The Run-28 Major (oversized write tears down healthy connections) is fixed: the writer drops a `FrameTooLarge` frame and *keeps* the connection (counted in `dropped_frames`); the garbage-port survival test + oversized gate both green in 291/0. Inbound decode limits/fuzz intact. |
| 13 | Security | 8 | **Deep-dive (read-level).** WS1–WS5 surface unchanged since Run 28; `cargo audit` **0 vulns** this run (RUSTSEC-2026-0188 closed — wasmtime-wasi 45.0.3). Capped at 8: `compliance` suite not re-run *this* run (M2 requires fresh execution for 9). |
| 14 | Failure Mode Legibility | 8 | **Deep-dive. Recovered from 7.** Run-28's "only trace is a responder-side warn" is fixed on both ends: `kv_set` warns + returns `false` to the caller, the writer counts the dropped frame, and the drain-warn miscount is corrected. Tripwire counters on `/stats` intact. |
| 15 | Performance | 8 | **Deep-dive.** Hot paths re-read: `lww_wins` is `#[inline]` and branch-only; `apply_to_store` clones `Bytes` once outside the retry closure (O(1) refcount); chunking adds a bounded pass only on the rare join-time `StateResponse`, off the live path. No fresh benchmark → 8. |
| 16 | Scalability | 8 | **Recovered from 6.** The unchunked-`StateResponse` cliff is fixed (per-chunk byte budget); gate `test_late_joiner_converges_past_frame_sized_store_via_chunked_anti_entropy` (12 MiB store + poison entry) green in 291/0. Merkle O(divergence) anti-entropy intact. No fresh scale run → 8. |
| 17 | Testability | 8 | carried (v28). This run's probes were again writable as pure/in-isolation tests (`KvState::new(0)` + `apply_and_notify`; executable greps). Determinism re-verified (291/0 twice). |
| 18 | Test Architecture | 8 | carried (v28). Suite grew by 1 (the 3-writer gate); 291/0 twice confirms parallel determinism holds. Fuzz still CI-only. |
| 19 | Observability | 8 | carried (v28). |
| 20 | Debuggability | 8 | carried (v28), nudged by the wiki: the lock-order table + runtime-invariants pages now give a diagnosable single source for the recurring race family (`docs/wiki/dev/concurrency/`). No fresh tooling probe → 8. |
| 21 | Operational Readiness | 8 | carried (v28). |
| 22 | Evolvability | 8 | carried (v28). Wire stays v12/PREV=11; docs-only diff. The `/wiki-lint` skill + `.log/` ingest discipline is new debt-control machinery, but its value is unproven over time → held 8. |
| 23 | Documentation | 8 | **Recovered from 7.** The Run-28 Minor (lock-order table falsely claiming completeness) is *fixed and now executably enforced*: the table went 8→18 rows across two `/wiki-lint` passes, and doc-vs-code verification is a repeatable skill. Plus the whole LLM-wiki (`docs/wiki/`, 34 files) + a thinned 838→~105-line CLAUDE.md. Not 9: newcomer-path/guide-runnableness not exercised this run; the wiki is new and unvalidated by an outside reader. |
| 24 | Developer Experience | 8 | carried (v28), improved: CLAUDE.md is now a thin query-first on-ramp pointing at the wiki; the rustc-1.96 clippy break is fixed (green on floating stable). Held 8 — no fresh clean-build-time measurement. |
| 25 | Dependency Hygiene | 8 | **Recovered from 6.** `cargo audit` run fresh this run: **0 vulnerabilities** (RUSTSEC-2026-0188 closed via wasmtime-wasi 45.0.3, RUSTSEC-2026-0190 via anyhow 1.0.103). 4 unmaintained-only warnings remain (backoff/instant/rustls-pemfile + dev-only bincode) — allowed, not vulns. `--no-default-features` build intact. |
| — | **Floor (lowest 3)** | **8, 8, 8** | all non-9 dimensions tie at 8 — the Run-28 finding-driven 6/6/6 trough closed once the same-day fixes' gates ran green here (exactly what the ledger predicts a fixed finding should do) |
| — | Mean (continuity footnote) | 8.1 | not a target; see M2 preamble (sum 203/25 = 8.12) |

## 2026-07-03 — Run 30 (M2)

Deep-dive dimensions this run (by rotation, resuming the 11–15 cycle Run 29 ran): 16 (Scalability), 17 (Testability), 18 (Test Architecture), 19 (Observability), 20 (Debuggability) — a clean fit: the run's whole diff is the **Legible Emergence** diagnosability layer, which lands squarely on 17/18/19/20 and adds a new scale-sensitive surface (16). Also re-scored 1 (Philosophy), 3 (Architecture), 14 (Failure Mode Legibility) — the diff is a textbook expression of the philosophy — and 24 (Developer Experience) — the feature-gating friction this session. Execution evidence: `cargo test --lib --features tls,metrics,a2a,llm` = **345 passed / 0 failed / 1 ignored** (this session, feature matrix); default-feature `Test` CI job **323/0**; `cargo clippy` clean on **three** feature sets (`--no-default-features`, feature-matrix, `-p mycelium-wasm-host`); CI green across all 13 jobs incl. the 12-demo coop suite (with the new induce-and-diagnose demo) and time-boxed fuzz; the coop `diagnostics` demo runs and diagnoses a real conflict from a non-seeding node; **three falsification probes (Philosophy/coordinator-free/legibility) all PASS and are kept as regression gates**; one `/wiki-lint` pass this session (6 doc-vs-code staleness findings fixed → `dev/` reconciled to Phases 2–5 shipped, lock-order table re-verified executably complete: all 21 lock sites + 2 aliases map to 19 rows). Diff since Run 29: 38 commits / +4511 lines — **Legible Emergence Phases 1–5 complete** (the five detectors + `/metrics`, the `/gateway/fleet` snapshot, the cross-node `/gateway/explain` event ring + #56 narrative, the `/gateway/diagnose` rule engine, the operator surface: public `fleet_snapshot()`/`fleet_diagnosis()` API + runbook + alert recipes + the coop demo) + two recurring-flake hardenings + the wiki lint.

### Findings
None. Three probes attempted against the three highest-scoring dimensions; all passed and were kept:
- **Philosophy (detection, not prevention)** — `probe_diagnosis_observes_but_never_corrects_a_conflict`: hammering `fleet_diagnosis()`/`fleet_snapshot()` over a seeded governed-group conflict leaves the `grp/` membership unchanged (the diagnosis *names*, never *corrects*). PASS.
- **Architecture (coordinator-free / no collector)** — `probe_lone_node_diagnoses_without_a_collector`: a lone node with zero peers computes a well-formed nominal diagnosis from its own KV, no quorum/aggregator/hang. PASS.
- **Failure-Mode Legibility** — `probe_every_diagnosis_finding_is_operator_legible`: with two distinct real pathologies seeded, every finding's `cause` is an actionable sentence (`Action:`) and never leaks its raw snake_case pathology id. PASS.

| # | Dimension | Score | Notes |
|---|-----------|:-----:|-------|
| 1 | Philosophy / Coherence with Goal | 9 | **↑ from 8. Probe PASS (executable).** Legible Emergence is a textbook expression of the philosophy: coordinator-free diagnosis is the direct *anti*-thesis of the "coordinator trap" (`philosophy.html` — Paremus' central reconciliation engine); detectors are detection-not-prevention (the probe proves the diagnosis never corrects); diagnostics-as-data is library-not-platform. No drift added by 4.5k lines. |
| 2 | Conceptual Integrity | 8 | The three-verb spine (localize·explain·diagnose) + the RT1–RT4 red-team discipline are threaded consistently through `emergent.rs`; naming (`compute_fleet_*`, `diagnose_fleet`, `narrate`) is uniform. Reading judgment → cap 8 (no whole-idiom probe). |
| 3 | Architecture | 9 | **Probe PASS (executable).** The diagnostics layer is node-local KV *reads* (tier-(b) taxonomy); it never teaches Layer I a higher law (no prefix write-guards in `apply_and_notify`), writes only its own `sys/health` pheromone, and the commit-conflict tripwire lives in the consensus layer. Lone-node probe confirms the coordinator-free property is structural. Held 9. |
| 4 | Modularity | 8 | `emergent.rs` is a self-contained module; the public surface is two read-only methods + a handful of re-exported plain structs. The `GroupStatus` name-collision with the mesh type was resolved by *not* re-exporting it (reached via `FleetSnapshot.governed_groups`). carried (v29). |
| 5 | API Design | 8 | New `fleet_snapshot()`/`fleet_diagnosis()` are minimal, read-only, argless — hard to misuse; diagnosis-as-data mirrors the HTTP surface exactly. The Run-28 `set() -> bool` conflation still stands → held 8. |
| 6 | Error Handling Model | 8 | The diagnostics are infallible-by-design (a read-only snapshot/diagnosis struct, no `Result`) — correct for a best-effort local estimate; `assemble_explain` degrades gracefully (a failed peer RPC becomes a *named* `non_responder`, never an error). carried (v29). |
| 7 | Configurability | 8 | `emergent_detectors_enabled` (env `GOSSIP_EMERGENT_DETECTORS`), off by default, zero-overhead-off; the snapshot/diagnosis work with or without the loop. carried (v29). |
| 8 | Language Best Practices | 8 | `clippy -D warnings` clean across three feature sets this session (caught + fixed inline: a `collapsible_if` let-chain, `unnecessary_sort_by`→`sort_by_key(Reverse)`, a dead `Severity::Info` variant, feature-gated dead code). Idiomatic let-chains (26 uses). The `metrics` recorder `expect` footgun still stands → held 8. |
| 9 | Concurrency Correctness | 8 | New `EventRing` leaf `Mutex<VecDeque>` (never across `await`) + the `assemble_explain` `JoinSet` fan-out + `commit_conflict_slots` retry-safe papaya `compute`. Lock-order table re-verified *executably complete* this session (all 21 `Mutex`/`RwLock` sites + 2 aliases → 19 rows). 345/0. No fresh concurrency probe → held 8. |
| 10 | Resource Management | 8 | `EventRing` is bounded (1024, oldest-dropped); the fan-out uses per-peer timeouts (no leak); the detector loop + explain responder are spawned only when enabled and shut down via `watch`. `probe_shutdown_drains_tasks_and_releases_port` PASS this run. carried (v29). |
| 11 | Semantic Correctness | 9 | Held 9. The new diagnosis rules are semantically sound (conflict = observed outside `[min,max]`; storm = `opaque_pct ≥ 34`; findings HLC-ordered) — verified by 5 unit rules + the real-KV grounding gate + the legibility probe. LWW/HLC convergence unchanged (Run-29 3-writer gate still green in 345/0). |
| 12 | Robustness | 8 | `assemble_explain` is the robustness story this run: partial fan-out failure yields a *named* gap, not a dropped node (RT3) — deliberately not `scatter_gather`, which discards partials. The demo's own poll caught a transient under-min conflict correctly (the system flagged reality). carried (v29). |
| 13 | Security | 8 | The three new diagnostics endpoints are scope-gated (`fleet:read`, deny-by-default) — `test_gateway_fleet_snapshot_endpoint_scope_gated` (401/403/200) + `required_scope` assertions green. `cargo audit` NOT re-run this run → cap 8. |
| 14 | Failure Mode Legibility | 9 | **↑ from 8. Probe PASS (executable). The headline.** This is the dimension the entire diff exists to move: `diagnose_fleet` names each pathology in code-free, actionable terms; the throttle graph supplies the *because* for opacity; the RT1/RT2 `caveat` stops a blind node reading as healthy. The probe proves every finding is operator-legible; the coop demo shows a non-designer diagnosis end-to-end. |
| 15 | Performance | 8 | No hot-path change: detectors are node-local prefix scans, zero-cost when the loop is off; `/metrics` gauges set on the existing tick. carried (v29). |
| 16 | Scalability | 7 | **↓ from 8. Deep-dive.** The new `/gateway/explain` fan-out is **O(peers), uncapped** — one `sys.explain` RPC to *every* known peer per operator query (best-effort, but no top-N / sampling / cap). Fine as an on-demand operator action, a real edge at 100+ nodes if scripted. Snapshot/diagnosis stay O(entries) local. No fresh scale run (`make test-scale`) this run → the fan-out characteristic + no fresh evidence caps it at 7. Not a Finding (operator-gated, not a broken invariant) — a noted scale edge. |
| 17 | Testability | 8 | **Deep-dive.** Exemplary: `diagnose_fleet`/`narrate` are pure functions unit-tested with synthetic snapshots; the cross-node behaviour grounds against a *real* KV snapshot; this run's three probes were writable as clean in-isolation tests over the public API. Design is injectable, deterministic. Design-strong → held 8 (probes evidence testability but don't *stress* it). |
| 18 | Test Architecture | 7 | **↓ from 8. Deep-dive.** Honest markdown: **two** timing tests flaked on CI this session — `test_manage_opacity_gate_…` for the **second** time (the Run-29-era 3s→10s widening was insufficient; still starved under ~345 parallel full-feature tests → widened to 30s) and `test_individual_consumers_over_random_partial_meshes` (8→20 attempts). Both are latency-tail flakes of *guaranteed* outcomes, both now ledgered. The recurrence is the signal: the suite carries timing-fragile tests whose ceilings sit inside the CI-saturation tail. Pyramid + fuzz + the new coop induce-and-diagnose gate are otherwise strong. |
| 19 | Observability | 8 | **Deep-dive.** Materially improved this run — the `/gateway/diagnose` narrative + 8 `mycelium_emergent_*` gauges + the Prometheus alert recipes (incl. the `peers_heard < peers_known` partial-view alert) + `/stats` scalars. An operator can now read *why*, not just *what*. Capped at 8: no fresh execution *probe* of the metrics/endpoint surface this run (reading + feature-presence caps at 8). |
| 20 | Debuggability | 8 | **Deep-dive.** The cross-node causal `explain` ring + the #56 narrative reconstruction is precisely a debugging tool — assemble what happened across nodes in HLC order, name the non-responders. Demonstrated by the demo + `test_explain_fanout_…`. Capped at 8: no fresh *debugging-session* probe (the ring's value is shown, not stress-tested, this run). |
| 21 | Operational Readiness | 8 | The `operations/diagnostics.md` runbook (one entry per pathology) + alert recipes + the Docker-free induce-and-diagnose coop demo raise ops readiness; `is_ready`/`shutdown` unchanged. No fresh ops probe → held 8. |
| 22 | Evolvability | 8 | The detector/rule layer is built to extend: adding a detector is one function + one `narrate` gloss entry, and an unknown event kind falls back to its raw string (`probe`/`narrate_surfaces_unknown_kinds` proves it's surfaced, never dropped). Wire unchanged (v12/PREV=11). carried (v29). |
| 23 | Documentation | 8 | Two-audience diagnostics docs shipped (operator runbook + guide/14 pattern 11 + wiki `dev/diagnostics.md`), reconciled by a `/wiki-lint` pass this session. Minor currency gap: `ROADMAP.md` ("last updated 2026-06-14") predates and does not headline Legible Emergence — tracked in `docs/plans/` instead. Held 8. |
| 24 | Developer Experience | 7 | **↓ from 8.** Real friction this session: the **feature-gated dead-code trap** turned three commits CI-red before diagnosis (an item used only under `gateway`/`metrics` is dead under `--no-default-features` / wasm-host); the mitigation (two added clippy gates + a testing-conventions note) is good but the trap is a sharp edge, and the coop crate pulls wasmtime/cranelift into *every* binary (cold `--bin diagnostics` build is very heavy). Honest 7. |
| 25 | Dependency Hygiene | 8 | No new dependencies added by 4.5k lines (diagnostics reuse `papaya`/`bytes`/`serde_fixint`); `--no-default-features` build verified this session; `Cargo.lock` present. `cargo audit` NOT re-run this run → held 8 (Run-29's fresh 0-vuln result stands but is not re-verified here). |
| — | **Floor (lowest 3)** | **7, 7, 7** | Scalability · Test Architecture · Developer Experience — the honest trough this run: an uncapped operator fan-out, two recurring timing flakes, and feature-gating friction. All three are *shape*, not defects (no Finding). |
| — | Mean (continuity footnote) | 8.0 | not a target; see M2 preamble (sum 201/25 = 8.04). Four 9s (Philosophy, Architecture, Semantic Correctness, Failure Mode Legibility), three probe-backed this run. |

## 2026-07-03 — Run 31 (M2)

Deep-dive dimensions this run (by rotation, resuming 16–20 with 21–25): 21 (Operational Readiness), 22 (Evolvability), 23 (Documentation), 24 (Developer Experience), 25 (Dependency Hygiene). Also re-scored 16 (Scalability) + 18 (Test Architecture) — the diff **is** the remediation of Run 30's 7/7/7 floor. Execution evidence: the three floor-fix commits each **CI-green** this session (16c3039 explain-cap, 3b7294e opacity-decision-extraction, 93c29d6 DX) — coop-smoke 12/12, opacity tests 15/0, the cap unit + e2e green, `cargo tree` + build A (no wasmtime) + build B (wasm ok) verifying the coop split; **three fresh falsification probes (Philosophy/Architecture/Semantic) all PASS and are kept**; two `/wiki-lint` passes today (one with 6 fixes, one clean). Diff since Run 30: 4 commits / +324−54 over 16 files — nothing new, purely the floor remediation + a lint.

### Findings
None. Three probes attempted against three of the four 9-scored dimensions (varied from Run 30's set), all passed and kept:
- **Philosophy (diagnostics as a pure read)** — `probe_r31_diagnosis_is_idempotent_no_accumulating_state`: 50 diagnoses against unchanged KV return the same load-bearing findings every time — a pure function of the store, no accumulating state. PASS.
- **Architecture / Scalability (the capped fan-out)** — `probe_r31_explain_fanout_is_bounded_and_deterministic_for_any_fleet`: for N ∈ {0…4000}, targets ≤ cap, `targets + skipped == N` (nothing dropped uncounted), and the subset is deterministic. PASS.
- **Semantic Correctness (the override boundary)** — `probe_r31_opacity_override_boundary_is_exactly_at_full`: the decision/scheduling split did not shift the library override — it fires at `fill >= 1.0` and not a hair below (0.999 with a vetoing gate ⇒ Hold). PASS.

| # | Dimension | Score | Notes |
|---|-----------|:-----:|-------|
| 1 | Philosophy / Coherence with Goal | 9 | Held. Probe PASS (new angle). The floor fixes are all in-philosophy: the cap keeps RT3 honesty (`not_queried` names the skipped, never silently drops); the opacity split preserves detection-not-prevention; `make check` is tooling, not substrate. |
| 2 | Conceptual Integrity | 8 | carried (v30). The `not_queried` field extends the existing `non_responders` honesty idiom; the opacity `OpacityTransition` enum matches the codebase's decision-enum style. |
| 3 | Architecture | 9 | Held. Probe PASS (executable). The opacity **decision/scheduling split** is a clean architectural improvement (pure decision, async scheduling); the capped fan-out stays node-local + observer-independent (no coordination introduced). |
| 4 | Modularity | 8 | carried (v30). `select_explain_targets` + `opacity_transition` are small pure units extracted without widening any interface. |
| 5 | API Design | 8 | carried (v30). `ExplainResult.not_queried` is additive; `make check`/`check-full` are the discoverable one-command gate. |
| 6 | Error Handling Model | 8 | carried (v30). |
| 7 | Configurability | 8 | carried (v30). New `EXPLAIN_MAX_FANOUT` cap constant + the coop `wasm` opt-in feature are cleanly scoped knobs. |
| 8 | Language Best Practices | 8 | carried (v30). clippy `-D warnings` clean across feature sets each fix (caught + fixed a `sort_by_key` + a `collapsible_if` inline). |
| 9 | Concurrency Correctness | 8 | The opacity refactor is **behavior-preserving** (verified: opacity tests 15/0 incl. the live integration wiring smoke); the cap **bounds** the concurrent `sys.explain` RPCs at 32. carried (v30) with a fresh artifact. |
| 10 | Resource Management | 8 | Improved-and-held: the explain fan-out is now bounded (≤ cap concurrent RPCs regardless of fleet size) — a resource ceiling where there was none. `probe_shutdown_drains_tasks` still green. |
| 11 | Semantic Correctness | 9 | Held. Probe PASS (executable). The extracted `opacity_transition` is faithful to the original inline semantics (override exactly at 1.0; hysteresis clear); the diagnosis rules unchanged. |
| 12 | Robustness | 8 | carried (v30). The cap's `not_queried` extends graceful partial-view handling (a capped query is *named* capped, not silently partial). |
| 13 | Security | 8 | carried (v30). No auth/scope surface change; `/gateway/explain` stays `fleet:read`. `cargo audit` not re-run → cap 8. |
| 14 | Failure Mode Legibility | 9 | Held. `not_queried` keeps a capped explain legible (the operator sees the fan-out was bounded, not a silent partial). Diagnosis narrative unchanged. |
| 15 | Performance | 8 | carried (v30). The cap reduces worst-case fan-out cost at scale; no hot-path change. |
| 16 | Scalability | 8 | **↑ from 7 (Run-30 finding fixed).** The uncapped O(peers) `/gateway/explain` fan-out is now capped at `EXPLAIN_MAX_FANOUT = 32` with the remainder named (`not_queried`); unit + property probe confirm bounded-for-any-N. Not 9: no fresh 100-node scale run this run. |
| 17 | Testability | 8 | carried (v30), reinforced: the opacity **decision is now unit-testable purely** (the whole point of the extraction) and the cap is a pure function — three of this run's probes were writable as clean in-isolation tests. |
| 18 | Test Architecture | 8 | **↑ from 7 (Run-30 finding fixed).** The recurring opacity-gate flake (flaked twice) is **structurally eliminated** — the invariant moved from the flaky async path to a deterministic pure gate; the integration test is now an explicit wiring smoke. Not 9: `test_individual_consumers_over_random_partial_meshes` remains a widened (8→20) integration property test, not restructured. |
| 19 | Observability | 8 | carried (v30). |
| 20 | Debuggability | 8 | carried (v30). |
| 21 | Operational Readiness | 8 | **Deep-dive.** The `operations/diagnostics.md` runbook + alert recipes + the induce-and-diagnose demo (Phase 5) stand; `make check` shortens the dev/ops feedback loop; `is_ready`/`shutdown` unchanged and `probe_shutdown_drains_tasks` green. No fresh ops-lifecycle probe *this* run → held 8. |
| 22 | Evolvability | 8 | **Deep-dive.** Two clean extension examples landed: the coop `wasm` **optional-dependency** feature (add capability without bloating the default graph) and the opacity decision/scheduling split (change the schedule without touching the decision). Wire stays v12/PREV=11. |
| 23 | Documentation | 8 | **Deep-dive.** Strong discipline this session: two `/wiki-lint` passes (dev/ reconciled to Legible-Emergence complete; the cap + `make check` documented in CLAUDE.md, testing.md, diagnostics.md/operations.md). Persistent gap: `ROADMAP.md` ("last updated 2026-06-14") still does not headline Legible Emergence (tracked in `docs/plans/` instead) → held 8. |
| 24 | Developer Experience | 8 | **↑ from 7 (Run-30 finding fixed). Deep-dive.** Both Run-30 pains addressed + verified: `make check` is the one-command pre-push gate whose `--no-default-features` clippy is the fast catcher for the feature-gated dead-code trap; `mycelium-wasm-host` is now optional behind `wasm`, so `cargo run --bin diagnostics` (+ 9 demos) build with **no wasmtime** (`cargo tree` + build A confirm; CI coop-smoke green). Not 9: the two wasm demos are still a heavy build (inherent to wasmtime), and the trap is now *catchable* rather than *impossible*. |
| 25 | Dependency Hygiene | 8 | **Deep-dive.** Improved: `mycelium-wasm-host` demoted to an **optional** coop dep (wasmtime/cranelift out of the default coop graph — `cargo tree` verified); no new dependencies added by 4 commits; `--no-default-features` intact; `Cargo.lock` present. `cargo audit` not re-run this run → held 8 (Run-29's fresh 0-vuln stands, unverified here). |
| — | **Floor (lowest 3)** | **8, 8, 8** | All non-9 dimensions tie at 8 — Run 30's 7/7/7 trough (Scalability · Test Architecture · Developer Experience) closed once each fix's gate ran green, exactly what the ledger predicts a remediated finding should do. |
| — | Mean (continuity footnote) | 8.2 | not a target; see M2 preamble (sum 204/25 = 8.16). Four 9s (Philosophy, Architecture, Semantic Correctness, Failure Mode Legibility), three probe-backed this run. |

## 2026-07-03 — Run 32 (M2)

Deep-dive dimensions this run (rotation 1–5): Philosophy, Conceptual Integrity, Architecture, Modularity, API Design — plus **10 Resource Management** (probe-driven, where the finding landed). The material diff since Run 31 is the **entire `mycelium-wiki` companion** (build phases 1–5 + the Phase-4 gateway/SDK remainder — 12 commits, `940a336`…`7b2f1e3`): a group-scoped, LLM-curated wiki (control-plane/data-plane), the durable third coordination primitive. Execution evidence this run: `cargo test -p mycelium-wiki --features control-plane|llm|gateway` (16/18 lib + cross-node `failover.rs` + `gateway.rs` lifecycle, all green), `./mycelium-wiki/ci_smoke.sh` (import→grounded-chat over both UC corpora), clippy `-D warnings` clean across all three features + examples, `tsc --noEmit` clean, and **CI green** on every wiki commit incl. the remainder (`28678377221`: wiki + sdk-ts success). Core mycelium (KV/Signal/Consensus) is **untouched** by this diff → its dimensions carry Run 31.

### Findings
- **Major · Resource Management** — the `Wiki` curator's background loops (`drain` / `lint` / `run_election` / `watch_and_promote`) are each spawned with a captured `Arc<Self>` and loop **unconditionally**, and the `Wiki` had **no `shutdown` and no `Drop`** — a strong-reference cycle: a `Wiki` can never be reclaimed and its tasks run until the tokio runtime ends. Masked in tests (agent shutdown ends the runtime), but a leak for any process that creates/discards wikis; also a divergence from the sibling `Blackboard::shutdown` idiom (and from core's `spawn_task`, which the agent *does* drain — the wiki used raw `tokio::spawn`, bypassing that tracking). **Found by the deep-dive probe on dim 10; confirmed by reading (`h.abort()` present in `mycelium-blackboard/src/lib.rs:623`, absent in the wiki). Fixed same run** (`Wiki::shutdown` aborts the tasks and awaits cancellation so the `Arc<Self>` releases; retracts the cap ads) with a canary — `agent::tests::shutdown_breaks_the_task_cycle_and_frees_the_wiki` (a `Weak<Wiki>` no longer upgrades after `shutdown` + drop), green. **No ledger line:** Run 31 (the prior audit) *predates* the wiki code, so no run scored Resource Management ≥8 while this bug existed — the framework caught it in the first audit after it landed.

| # | Dimension | Score | Notes |
|---|-----------|:-----:|-------|
| 1 | Philosophy / Coherence with Goal | 9 | **Deep-dive.** carried (v31), reinforced: the wiki is a faithful application of the philosophy — coordinator-free (a *recallable* curator elected on the ring, ring-failover, "if the curator vanishes the wiki stays readable"), library-not-platform (a companion over the public API), detection-not-prevention applied to meaning (advisory lint, never auto-rewrite). The external store is *composed* app-infra (like Postgres/RAG), not a substrate broker → no dilution of "embedded, broker-less". |
| 2 | Conceptual Integrity | 7 | **↓ from 8. Deep-dive.** The companion mostly mirrors the blackboard idiom (roles, ring election, `tests/failover.rs`, gateway + py/ts SDKs) — but shipped **without the `shutdown` every sibling has**, using raw `tokio::spawn` instead of the agent's tracked `spawn_task`. A real one-mind-consistency miss (the finding); fixed this run, but it *shipped*, so the score reflects it. |
| 3 | Architecture | 9 | carried (v31). The companion respects the layer model: **public API only** (no core fork), the proposal queue rides KV (correct layer), election rides capabilities; the control-plane/data-plane split is clean (readers hit the store directly, node-independent). Fresh evidence it holds: `failover.rs` + `gateway.rs` green. |
| 4 | Modularity | 8 | **Deep-dive.** Clean module split (`model`/`store`/`fs`/`agent`/`reconcile`/`lint`/`mcp`/`http`), each feature-gated; decision logic injected via `CuratorBrain` (reconciler + optional semantic linter) so the curator's policy is swappable without touching the election/drain machinery. Core sub-handles untouched. |
| 5 | API Design | 8 | **Deep-dive.** Surface is small and role-clear (`propose`/`read`/`query`; `register_mcp_tools`; `http_router`); reader vs writer path is explicit. Wrinkles: three constructors (`new`/`with_reconciler`/`with_brain`), and the API shipped with a **completeness hole** — no `shutdown` (added this run). Not higher because the hole was real. |
| 6 | Error Handling Model | 8 | carried (v31). `WikiError` (Io/Serde/BadPath) propagates via `?`; the gateway maps it to status codes (BadPath→400, else 502); LLM/undecodable-proposal errors degrade (fallback / drop) rather than wedge. |
| 7 | Configurability | 8 | `WikiConfig` (group/role/cap_refresh/drain_interval/lint_interval) + the `control-plane`/`llm`/`gateway` feature split are cleanly scoped knobs; default features `[]` (pure data plane, no mycelium dep). |
| 8 | Language Best Practices | 8 | carried (v31), reinforced: clippy `-D warnings` clean across control-plane/llm/gateway **+ examples** this run (caught + fixed a `sort_by_key`/`Reverse` + a `type_complexity` inline). |
| 9 | Concurrency Correctness | 8 | carried (v31). Core unchanged. The companion's concurrency is exercised by the cross-node `failover.rs` (raced startup, election, ring-failover, single-writer apply) — green; the lifecycle defect was a *resource* cycle, not a data race. |
| 10 | Resource Management | 6 | **Deep-dive · FINDING (capped).** The task/`Arc<Self>` cycle with no teardown (above). Fixed + canary this run, but M2 caps a confirmed finding at 6 for the run it was live. Recovers next run if the fix holds. |
| 11 | Semantic Correctness | 9 | carried (v31). Core LWW/HLC/consensus untouched. Companion-level: the reconcile's idempotent append-merge (skip already-contained body) and stable opaque `mint_section_id` are unit-tested (`direct_is_idempotent_on_replay`, `section_ids_are_stable…`). |
| 12 | Robustness | 8 | carried (v31). Companion degrades on bad input: undecodable proposals are dropped (queue never wedges), LLM outage falls back to append-merge, gateway rejects wrong-group with 400, path traversal guarded (`BadPath`). |
| 13 | Security | 8 | carried (v31). No core auth change. The wiki gateway is unauthenticated like the blackboard's (behind the node); the store path is traversal-guarded. `cargo audit` not re-run → cap 8. |
| 14 | Failure Mode Legibility | 9 | carried (v31). Companion adds legible signals: `tracing::warn` on lint findings + LLM-reconcile failure + curator-evaporation; `last_lint()` surfaces the group-function output. |
| 15 | Performance | 8 | carried (v31). No hot-path change to core. |
| 16 | Scalability | 7 | **↓ from 8.** Companion-level cliff: the curator's **lint reads the entire corpus every `lint_interval`** (O(corpus), no incremental path) and `drain_once` scans the whole `wiki/{group}/proposal/` prefix each tick. Curator-only + infrequent (30 s default) bounds the blast radius, but it is an uncapped O(corpus) periodic cost — unlike the explain fan-out that got *capped* in Run 30/31. A known characteristic, flagged not fixed. |
| 17 | Testability | 8 | **Deep-dive.** Exemplary at the companion level: pure functions (`structural_lint`, `DirectReconciler::merge`, `mint_section_id`), injectable `Reconciler`/`SemanticLinter`/`LlmBackend` (tested with `EchoBackend`), `FsStore` on a tempdir, `free_port`; MCP/gateway handlers extracted as `pub(crate)` fns testable without the invoke machinery. Fresh evidence: the suites ran green. The lifecycle gap *was* a testability hole (couldn't assert teardown) — the canary closes it. |
| 18 | Test Architecture | 8 | **Deep-dive.** Right pyramid for the companion: pure-fn units + cross-node integration (`failover.rs`, `gateway.rs`) + a Docker-free worked-example smoke (`ci_smoke.sh`, both UC corpora + a negative check), all CI-gated. No fuzz/property in the companion (no adversarial wire parsing to warrant it). Watch item: `failover.rs`/`gateway.rs` ran ~60 s under local compile-contention (generous structural polls absorbed it; CI isolated = fast). |
| 19 | Observability | 8 | carried (v31). |
| 20 | Debuggability | 8 | carried (v31). |
| 21 | Operational Readiness | 8 | carried (v31). Core `is_ready`/`shutdown_with_timeout`/load unchanged. The companion shipped without teardown (the finding) — now `Wiki::shutdown` exists — but project-level readiness holds on the core. |
| 22 | Evolvability | 8 | carried (v31). Wire untouched (v12/PREV=11); the wiki is purely additive; the design record retains the disconnected KV-native variant as a labelled alternative. |
| 23 | Documentation | 8 | Companion is well-documented (module rustdoc, `docs/plans/mycelium-wiki.md`, the ingested wiki page `docs/wiki/dev/companions/wiki.md`, a runnable worked example). No `docs/guide/` chapter for the wiki yet; the Run-31 `ROADMAP.md` staleness persists → held 8. |
| 24 | Developer Experience | 8 | carried (v31). CI extended per-feature (`control-plane`/`llm`/`gateway` + the smoke); clippy-clean gates; `make check` unchanged. |
| 25 | Dependency Hygiene | 8 | The companion's default features are `[]` (no mycelium dep — pure data plane); `axum`/`tokio`/`async-trait`/`parking_lot`/`bytes` are all **optional** behind features; SDKs add nothing new (`httpx` already a py dep). `Cargo.lock` updated for the gateway edges. `cargo audit` not re-run → cap 8. |
| — | **Floor (lowest 3)** | **6, 7, 7** | Resource Management (finding) · Conceptual Integrity · Scalability — all three are the **new companion's** growing pains, not core regressions. The floor dropped from Run 31's 8/8/8 because this run *found a real Major defect* in freshly-landed code — the audit working as designed (cf. the M2 rationale). |
| — | Mean (continuity footnote) | 8.0 | not a target; see M2 preamble (sum 200/25 = 8.0). Four carried 9s (Philosophy, Architecture, Semantic Correctness, Failure Mode Legibility — core untouched by this diff). The drop from 8.2 is the single Major finding (RM 8→6) + two honest companion dings (CI, Scalability). |

## 2026-07-04 — Run 33 (M2)

Deep-dive dimensions this run (rotation 6–10): Error Handling, Configurability, Language Best Practices, Concurrency Correctness, Resource Management — plus re-scored 2 (Conceptual Integrity), 16 (Scalability), 25 (Dependency Hygiene) where the diff did real work. The material diff since Run 32 is **the Run-32 remediation + two new workstreams**: `23325a9` `Wiki::shutdown` (fixes the RM finding), `7dc8b04` change-driven lint (fixes the Scalability ding), `4408331` the membership-gated **access broker**, and `49ccbf4` **fully retiring `bincode`** (golden-vector tests replace the dev-only oracle → RUSTSEC-2025-0141 cleared), plus a large docs sweep (ROADMAP/CHANGELOG/guide/presentation refresh, the go-live + customer-pilot ops checklists, 4 wiki-lint passes). Execution evidence this run: mycelium-core lib **129/0**; mycelium-wiki `--features control-plane` **20/0** (incl. the `shutdown_breaks_the_task_cycle_and_frees_the_wiki` + `curator_lints_only_after_a_change` canaries), `tests/access.rs` **2/0**, `failover`/`gateway` green, `--features llm` 18/0; the codec **`adversarial_bytes_never_panic`** probe (extended this run to fuzz the rewritten `decode_wire_v11` + assert an oversized length prefix errors) **passed**; the store **LWW** suite 8/0; clippy `-D warnings` clean across mycelium-core + the main feature-matrix + all wiki features; **`cargo audit` no longer reports `bincode`** (CI audit job success on `49ccbf4`).

### Findings
None. All three falsification probes passed (below). The Run-32 Major finding (the `Wiki` task/`Arc<Self>` cycle) is **fixed and confirmed** by its canary this run.

| # | Dimension | Score | Notes |
|---|-----------|:-----:|-------|
| 1 | Philosophy / Coherence with Goal | 9 | carried (v32). The access broker is textbook philosophy: a *recallable*, membership-gated grant (management-as-intent), read-path stays node-independent (broker off the read path). The customer-pilot doc frames a pilot as the *external validation the audit loop can't self-supply* — no drift, honest about the ceiling. |
| 2 | Conceptual Integrity | 8 | **↑ from 7 (Run-32 ding fixed).** `Wiki::shutdown` now matches the `Blackboard::shutdown` idiom; the access broker's RPC handler mirrors the blackboard's `rpc_rx` loop. The one-mind consistency miss (missing teardown) is closed. |
| 3 | Architecture | 9 | carried (v32). The broker uses **point-to-point RPC, not KV**, for grants — the correct layer choice (a credential must not flood the cluster as gossiped KV would); public-API-only, no core fork. `bincode` removal is internal. |
| 4 | Modularity | 8 | carried (v32). New `broker.rs` is a clean module; `CuratorBrain` bundles reconciler + semantic-lint + membership as one injected policy object. |
| 5 | API Design | 8 | carried (v32), and the Run-32 completeness hole is filled: `Wiki::shutdown` + `request_store_access`/`Membership`/`StoreGrant`/`AccessError` are a small, role-clear surface. |
| 6 | Error Handling Model | 8 | **Deep-dive.** `AccessError` (NoCurator/Denied/Rpc/Decode) is a clean typed enum with `Display`+`Error`; `WikiError` propagates via `?`; the gateway maps to status codes; LLM/undecodable-proposal paths degrade rather than wedge. Core model unchanged. |
| 7 | Configurability | 8 | **Deep-dive.** Membership moved to `CuratorBrain` (curator policy) so it needs no `WikiConfig`-literal churn; feature flags stay orthogonal; `bincode` removal trims the dep surface. Core `config.rs` unchanged. |
| 8 | Language Best Practices | 8 | **Deep-dive.** clippy `-D warnings` clean across mycelium-core + the full main feature-matrix + all wiki features **this run** (fresh); the golden-vector + broker code is idiomatic (typed errors, no runtime `unwrap`/`unsafe` added). |
| 9 | Concurrency Correctness | 8 | **Deep-dive.** carried (v32) reinforced: the broker's RPC handler is a **tracked** curator task (aborted by `shutdown`); cross-node `failover`/`access`/`gateway` tests exercise raced startup + election + point-to-point RPC, green. The Run-32 lifecycle cycle (a *resource* defect, not a data race) is closed. |
| 10 | Resource Management | 8 | **Deep-dive. ↑ from 6 (the Run-32 finding, fixed).** `Wiki::shutdown` aborts the drain/lint/election/watch/**broker** tasks and awaits cancellation so the `Arc<Self>` releases (cycle broken); canary `shutdown_breaks_the_task_cycle_and_frees_the_wiki` (a `Weak` no longer upgrades) green this run. Not 9: I did not re-audit the other companions for an analogous cycle. |
| 11 | Semantic Correctness | 9 | carried (v32). Core LWW/HLC/consensus untouched; the **LWW suite (8/0)** re-run this run covers the equal-timestamp determinism the ledger once caught. Reconcile idempotence + stable `mint_section_id` tested. |
| 12 | Robustness | 8 | carried (v32). The codec I modified retains graceful-drop: `adversarial_bytes_never_panic` (extended to the rewritten v11 path + an oversized-length assertion) passed — a malformed/hostile frame errors, never panics/OOMs. |
| 13 | Security | 8 | carried (v32). No core auth change. Supply-chain: the unmaintained `bincode` is out of the entire tree (RUSTSEC-2025-0141 cleared) — but the wire codec was already hand-rolled since M11, so this is hygiene, not new hardening. `cargo audit` still warns on `instant` (transitive) → not higher. |
| 14 | Failure Mode Legibility | 9 | carried (v32). The broker logs each grant/deny with the requester id; `AccessError` names the reason (no-curator vs denied vs transport); fleet diagnosis unchanged. |
| 15 | Performance | 8 | carried (v32). No hot-path change; the change-driven lint removes idle CPU (a scalability, not throughput, win). |
| 16 | Scalability | 8 | **↑ from 7 (Run-32 ding fixed).** The curator lint is now **change-driven** (`lint_dirty` flag): an idle wiki does zero whole-corpus lint work; canary `curator_lints_only_after_a_change` (counter flat while idle, advances on a write) green this run. `drain`'s `scan_prefix` confirmed prefix-index-backed (O(pending), not O(all KV)). |
| 17 | Testability | 8 | carried (v32), reinforced: three new canaries (shutdown, change-driven-lint, access grant/deny) + golden-vector unit tests, all pure/injectable and fast. |
| 18 | Test Architecture | 8 | carried (v32). +`tests/access.rs` cross-node; the `bincode` oracle → **golden byte-vectors** is a test-architecture improvement (pins the frozen wire format with zero dependency). Right pyramid; no throwaway probes (the v11 fuzz was kept). |
| 19 | Observability | 8 | carried (v32). +`lint_pass_count()` exposes curator lint activity. |
| 20 | Debuggability | 8 | carried (v32). |
| 21 | Operational Readiness | 8 | carried (v32). Materially better docs (the `production-readiness.md` go-live pre-flight + `customer-pilot.md`) and `Wiki::shutdown` gives the companion a real teardown — but docs+API alone, no fresh ops-lifecycle probe → held 8. |
| 22 | Evolvability | 8 | carried (v32). Wire policy intact (v12/PREV 11); `bincode` retirement + golden vectors leave a cleaner, dependency-free format-pinning story; the access broker is purely additive. |
| 23 | Documentation | 8 | carried (v32). Large, verified sweep: guide taxonomy now includes the wiki + a cookbook recipe, the pitch deck gained the wiki primitive + fleet-diagnostics + per-primitive example links, and **the Run-30/31/32 ROADMAP staleness is fixed** (v2.0-complete status, sizing pointer). Four `/wiki-lint` passes clean. Held 8 (docs cap without an execution axis; the newcomer path is strong but unmeasured). |
| 24 | Developer Experience | 8 | carried (v32). `make check` + per-feature CI gates unchanged; clippy-clean throughout; the golden-vector change removed a dev-dependency. |
| 25 | Dependency Hygiene | 8 | **Deep-dive.** `bincode 2.0.1` **fully retired** — gone from `Cargo.lock` and every `Cargo.toml` (net −1 crate); the CI `cargo audit` job passed on `49ccbf4` with the RUSTSEC-2025-0141 advisory gone. `--no-default-features` intact; `Cargo.lock` present. Not 9: `instant 0.1.13` (transitive) still carries an unmaintained warning, so the tree is not advisory-clean. |
| — | **Floor (lowest 3)** | **8, 8, 8** | The Run-32 trough (Resource Management 6 · Conceptual Integrity 7 · Scalability 7) is **fully remediated** — each fix shipped with a canary that ran green this run. The floor returns to Run-31's 8/8/8: find → fix → confirm, exactly what the ledger predicts a remediated finding should do. |
| — | Mean (continuity footnote) | 8.16 | not a target; see M2 preamble (sum 204/25 = 8.16). Back to the Run-31 baseline after Run-32's dip. Four 9s (Philosophy, Architecture, Semantic Correctness, Failure Mode Legibility). No new ledger line: this run's probes found nothing, and the Run-32 defect was scored 6 (not ≥8) while it existed, so it never earned one. |

## 2026-07-05 — Run 34 (M2)

Deep-dive dimensions this run: **9 Concurrency Correctness, 10 Resource Management, 12 Robustness,
14 Failure Mode Legibility, 18 Test Architecture** — finding-driven (the curator step-down landed
squarely on all five), so the rotation deviates from the strict 11–15 that would follow Run 33's 6–10;
rotation resumes 11–15 next run. The material diff since Run 33 is **the wiki curator step-down fix**
(`9e42453`, PR #127) plus the external-facing docs sweep (FAQ + Building-on-Mycelium integrator
on-ramp, README front-doors + four-paper corpus DOIs, the wiki-lint scope extension, a `schema()`→
`schemas()` doc fix) and one `/wiki-lint` ingest. Execution evidence this run: `cargo test -p
mycelium-wiki --features llm` = **26/0** (lib 22/0 + access 2/0 + failover 2/0), on a **cold** rebuild
(post `cargo clean`); the deterministic canary `dual_curators_reconcile_to_a_single_writer`
**verified to fail at 30 s without the sentinel and pass (~3 s) with it** (the run's falsification
probe, executed); `cargo clippy -p mycelium-wiki --all-targets --all-features -D warnings` clean; **PR
#127 CI green across all 14 jobs** (core lib `Test`, `Clippy`, gateway-free build, all companions,
AFN/coop/demo smokes, RUSTSEC `cargo audit`). Core `mycelium` (KV/Signal/Consensus) is untouched by
this diff → its dimensions carry Run 33.

### Findings

**Major — Concurrency Correctness (capped 6) — `mycelium-wiki` curator split-brain.** `run_election`
settles on a fixed window, so a lost gossip race lets two `Auto` nodes both `become_curator()`; there
was **no step-down**, so both stayed writers of record against the shared store permanently. Confirmed
by the `dual_curators_reconcile_to_a_single_writer` probe (two forced curators never reconcile without
the fix — XOR poll times out at 30 s). Fixed this session (`9e42453` — curator sentinel, lowest-id-wins
applied continuously, higher-id resigns) with the probe kept as the regression gate. New calibration
ledger line added (Concurrency scored 8 in Runs 32–33 while it existed, both citing the flaky
`failover.rs` XOR gate as evidence). Recovery to 8 expected Run 35, per the find→fix→confirm pattern.

| # | Dimension | Score | Notes |
|---|-----------|:-----:|-------|
| 1 | Philosophy / Coherence with Goal | 8 | carried (v33). The step-down is philosophically on-message — a *recallable role* that self-heals to a single writer is the coordinator-free thesis, not a coordinator. Read-only this run. |
| 2 | Conceptual Integrity | 8 | carried (v33). The sentinel/resign mirror the existing `watch_and_promote` idiom and the blackboard election; `curator_tasks` split from `tasks` is a clean extension. The `schema()`→`schemas()` doc typo (caught + fixed, `b872a5e`) is a minor blemish, not drift. |
| 3 | Architecture | 8 | carried (v33). Layers untouched; the fix is companion-local and builds on the public API only. Namespace table verified against `building-on-mycelium.md` (lint) — exact match. |
| 4 | Modularity | 8 | carried (v33). |
| 5 | API Design | 8 | carried (v33). Sub-handle surface unchanged; `resign`/sentinel are private. |
| 6 | Error Handling Model | 8 | carried (v33). |
| 7 | Configurability | 8 | carried (v33). |
| 8 | Language Best Practices | 8 | carried (v33) + fresh artifact: `clippy --all-targets --all-features -D warnings` clean on the companion incl. the new code; the `match … if guard` avoids `single_match`. |
| 9 | Concurrency Correctness | **6** | **Deep-dive; FINDING (Major, capped).** The wiki curator split-brain (no step-down → two permanent writers) confirmed by the `dual_curators_reconcile_to_a_single_writer` probe. **Fourth** ledger entry for this dimension, and the second of the "a green non-deterministic gate was mistaken for evidence" shape. Fixed + canary this session; recovery expected Run 35. |
| 10 | Resource Management | 8 | **Deep-dive.** The `resign()` teardown is correct — takes `curator_tasks` under lock, drops the lock, then aborts+awaits (releasing each loop's `Arc<Self>`), and `shutdown` now drains `curator_tasks` too; the sentinel that triggers resign lives in `tasks` and ends by returning, so it never aborts itself. Verified green under the full wiki suite + canary. Not capped — this is the fix side, not the defect. Not 9: no fresh RM-specific probe (fd/task-count assertion) beyond the suite. |
| 11 | Semantic Correctness | 8 | carried (v33). Core LWW/HLC/consensus untouched; the CI `Test` job (core lib) green on #127, but not deep-dived this run. |
| 12 | Robustness | 7 | **Deep-dive.** Dinged (not capped): the split-brain's defining trait was **no recovery** from an off-nominal election race — a graceful-degradation failure, now self-healing via the sentinel. Distinct facet from the Concurrency cap. Malformed-frame / peer-loss paths unchanged from Run 33. |
| 13 | Security | 8 | carried (v33). No change; gateway-no-auth-by-default still documented (now also in `building-on-mycelium.md` §4). |
| 14 | Failure Mode Legibility | 8 | **Deep-dive.** The step-down is *legible*: `resign` emits `tracing::warn!("wiki: stepped down — a lower-id curator exists")`, where the old dual-curator state was silent. A net legibility gain; held at 8 (the improvement is one log line, not a subsystem). |
| 15 | Performance | 8 | carried (v33). The sentinel adds one `resolve_role("curator")` per `cap_refresh` — negligible. |
| 16 | Scalability | 8 | carried (v33). |
| 17 | Testability | 8 | carried (v33). The `spawn_role` helper + forced-role construction made the split-brain deterministically reproducible — evidence the design is injectable. |
| 18 | Test Architecture | 7 | **Deep-dive.** Dinged: the `failover.rs` XOR poll was a **non-deterministic gate that hid a Major bug for two runs** (the recurring Test-Architecture weakness — this is its ~7th ledger appearance). Remediated at the root here with a *deterministic* canary that forces the split-brain, and I verified it bites. Held at 7 (not 8) to mark the recurrence honestly, mirroring Run 30. |
| 19 | Observability | 8 | carried (v33). |
| 20 | Debuggability | 8 | carried (v33). |
| 21 | Operational Readiness | 8 | carried (v33). |
| 22 | Evolvability | 8 | carried (v33). Wire policy intact (v12/PREV 11); wiki-lint extended to guard the external front-door docs against doc-vs-code drift (`0c54910`) — a debt-reducing move. |
| 23 | Documentation | 8 | carried (v33) + real additions: the FAQ + Building-on-Mycelium integrator on-ramp (two-audience front doors), README corpus DOIs. The `schema()` typo shipped in `building-on-mycelium.md` and was caught by the very lint check added to guard it — honest wash, held 8. |
| 24 | Developer Experience | 8 | carried (v33). The on-ramp + copyable `CLAUDE.md` snippet materially improve the downstream-integrator path. |
| 25 | Dependency Hygiene | 8 | carried (v33). No new deps (the fix uses existing `tokio`); RUSTSEC `cargo audit` job green on #127. |
| — | **Floor (lowest 3)** | **6, 7, 7** | Concurrency Correctness (6, capped finding) · Robustness (7) · Test Architecture (7) — all three facets of the one curator-split-brain defect (the bug, its non-recovery, and the flaky gate that hid it), each now remediated with the deterministic canary. |
| — | Mean (continuity footnote) | 7.84 | not a target; see M2 preamble (sum 196/25 = 7.84). Down from Run 33's 8.16 — the honest cost of a Major finding surfacing in the shipped companion. No 9s this run: the headline is a real defect in shipped code, which is not the posture for handing out top marks. Expect recovery toward the Run-33 baseline next run as the fix confirms. **[2026-07-06 note — snapshot preserved: a retro-correction to 8/8/7 under the later *current-state principle* (adopted Run 37, see the preamble bright-line) was applied and then reverted. Time-series scores are dated measurements under the rule in force *then*; the principle applies forward, and the ledger already carries this finding. Under the current rule this run would read Concurrency 8 · Robustness 8 · Test Architecture 7.]** |

## 2026-07-05 — Run 35 (M2) — recovery confirmation

**Cadence note:** no material code/test/docs diff since Run 34 (its commit was `ratings.md` only), so
this is **not** a full 25-dimension re-audit — it is a *targeted recovery confirmation* of the three
dimensions Run 34 capped/dinged for the curator split-brain, triggered by **new post-merge execution
evidence** rather than new code (mirrors the same-day Run 28→29 recovery). The other 22 dimensions
**carry Run 34** unexamined.

Confirmation evidence: the fix (`9e42453`) is now green on the **mainline** across repeated CI, and the
contrast is the point — the `Wiki (data plane)` job was **red on two pre-fix main pushes** (`54efcdc`
09:07, `0c54910` 10:30 — the split-brain flaking in the wild) and **green on three consecutive
post-fix pushes** (`b872a5e`, `5ea48f2`, and the Run-34 commit `81a914a`, run `28739434717`: `Test`,
`Wiki (data plane)`, and the 12-demo Co-op suite all success). The deterministic canary
`dual_curators_reconcile_to_a_single_writer` rides in that green job. Red-before / green-after ×3 is the
find→fix→confirm signature the cap predicted.

### Findings
None. This run confirms the Run-34 finding is remediated; no new probe was run (no new diff to probe).

| # | Dimension | Score | Notes |
|---|-----------|:-----:|-------|
| 9 | Concurrency Correctness | **8** | **Recovered from 6.** The split-brain fix is confirmed on the mainline — `Wiki (data plane)` red ×2 pre-fix → green ×3 post-fix, the deterministic canary green in each. Not 9: recovery is fix-verified (repeated CI), not a whole-dimension re-sweep — the Run-29 recovery-language precedent. |
| 12 | Robustness | **8** | **Recovered from 7.** The "no recovery from a lost election race" facet is closed — the election is now self-healing (higher-id curator resigns), confirmed by the green-after streak. |
| 18 | Test Architecture | 7 | **Held at 7 (not recovered).** The *specific* failover gate is now deterministic, but the dimension's recurring-flake weakness stayed live *this same day*: the 12-demo Co-op suite flaked on the fix-merge push (`9e42453`, run `28739078960`) and passed on the next push — a different timing-sensitive gate, the same pattern. Honest hold, not a re-assertion. |
| — | **Floor (lowest 3)** | **7, 8, 8** | Test Architecture (7) · Concurrency Correctness (8, recovered) · Robustness (8, recovered). The curator-split-brain trough is remediated and confirmed; Test Architecture remains the standing weakness (its ~7th ledger appearance + a fresh same-day Co-op flake). |
| — | Mean (continuity footnote) | 7.96 | not a target (sum 199/25 = 7.96, carrying 22 dims from Run 34). Recovering toward the Run-33 8.16 baseline; the residual gap is Test Architecture (7), which the series has now flagged repeatedly — the honest next target, not the curator code. |

## 2026-07-05 — Run 36 (M2)

Deep-dive dimensions this run: **18 Test Architecture, 17 Testability** — diff-driven; the entire
material diff since Run 35 is **one example file** (`26375e5`, PR #128 — the `elastic_intent` coop-demo
hardening, +22/−5), which is squarely a Test-Architecture concern and was the Run-35 floor. The other
23 dimensions **carry Run 33/34** (the `mycelium`/`mycelium-core` core is untouched since Run 33 — the
whole Run 34/35/36 arc is companion + example + docs). Execution evidence this run: the `elastic_intent`
demo **14/14 clean sequential local runs** (was ~1-in-3 pre-fix — reproduced, fixed, re-verified);
**PR #128 CI green across all 14 jobs incl. the Food-Rescue Co-op suite** (the previously-flaking job);
`cargo clippy -p mycelium-coop-examples --all-targets -- -D warnings` clean. This is a **small-diff
recovery run**: it completes the Run-34 trough's remediation by lifting the last floor item
(Test Architecture), which Run 35 deliberately held at 7 while the coop flake was still live.

### Findings
None. The coop flake was found (CI red on `9e42453`), root-caused, and fixed in this session's prior
work (PR #128); this run confirms the remediation. A new calibration-ledger line is added for it (it was
latent-while-scored-8 in Runs 32–33). No fresh probe on the untouched core — it was heavily probed in
Runs 28–33 and no diff touches it.

| # | Dimension | Score | Notes |
|---|-----------|:-----:|-------|
| 1 | Philosophy / Coherence with Goal | 8 | carried (v34). Core untouched; no execution axis this run. |
| 2 | Conceptual Integrity | 8 | carried (v34). |
| 3 | Architecture | 8 | carried (v34). |
| 4 | Modularity | 8 | carried (v34). |
| 5 | API Design | 8 | carried (v34). |
| 6 | Error Handling Model | 8 | carried (v34). |
| 7 | Configurability | 8 | carried (v34). The demo tunes `health_secs`/cooldown via existing config knobs — no surface change. |
| 8 | Language Best Practices | 8 | carried (v34) + fresh: `clippy --all-targets -D warnings` clean on the changed example. |
| 9 | Concurrency Correctness | 8 | carried (v35 recovery). Core untouched; the wiki curator fix stays confirmed on the mainline. |
| 10 | Resource Management | 8 | carried (v34). |
| 11 | Semantic Correctness | 8 | carried (v34). Core LWW/HLC/consensus untouched; not probed this run. |
| 12 | Robustness | 8 | carried (v35 recovery). |
| 13 | Security | 8 | carried (v34). The `elastic_intent` demo exercises the mTLS identity path (its flake *was* the identity-exchange window) — no security defect, a test-timing one. |
| 14 | Failure Mode Legibility | 8 | carried (v34) + a small gain: the demo's new readiness gate asserts a legible "cluster did not become signed-ready within 60s" instead of a confusing downstream failure. |
| 15 | Performance | 8 | carried (v34). |
| 16 | Scalability | 8 | carried (v34). |
| 17 | Testability | 8 | **Deep-dive.** The fix *demonstrates* testability: the flake was locally reproducible (~1-in-3) and the demo became a **structural** gate (prove bidirectional signed propagation before the timed phase) rather than a race — the lore-correct "structural poll, not a fixed sleep." Held 8 (fresh execution evidence, but this is one example, not a harness-wide change). |
| 18 | Test Architecture | 8 | **Deep-dive; RECOVERED from 7.** The Run-35 floor item — the `elastic_intent` coop flake — is fixed and verified (14/14 local + PR #128 CI green incl. the Co-op job). Not 9: the dimension's chronic recurring-flake history (its ~7th ledger appearance, now +1 for this demo) warrants continued watch, not a top mark — fixing flakes one-by-one is not the same as eliminating the pattern. |
| 19 | Observability | 8 | carried (v34). |
| 20 | Debuggability | 8 | carried (v34). |
| 21 | Operational Readiness | 8 | carried (v34). |
| 22 | Evolvability | 8 | carried (v34). |
| 23 | Documentation | 8 | carried (v34). |
| 24 | Developer Experience | 8 | carried (v34). |
| 25 | Dependency Hygiene | 8 | carried (v34). No dep change (the demo uses existing APIs). |
| — | **Floor (lowest 3)** | **8, 8, 8** | All dimensions at 8 — the Run-34 trough (Concurrency 6 · Robustness/Test-Architecture 7) is now **fully remediated and confirmed**: the curator split-brain (PR #127) and the coop flake (PR #128) both fixed with verified canaries/green CI. Floor returns to the Run-31/33 8/8/8. |
| — | Mean (continuity footnote) | 8.00 | not a target (sum 200/25 = 8.00). A flat 8 is the honest read of a small-diff recovery run: everything solid, the two Run-34/35 defects remediated, and **no dimension carries fresh execution evidence this run that uniquely earns a 9** (the session's headline was finding+fixing defects, which argues against top marks, not for them). Below Run-33's 8.16 precisely because Run 33's four 9s were execution-backed *that* run and have not been re-earned since. |

## 2026-07-06 — Run 37 (M2)

Deep-dive dimensions this run: **9 Concurrency Correctness, 12 Robustness, 14 Failure-Mode Legibility,
17 Testability, 18 Test Architecture** — finding-driven (the opacity control-signal-shed lands on all
five); rotation deviated to where the finding is, as prior finding-runs have. The material code diff
since Run 36 is a **single fix** (`d1db7bd` / PR #129 — never load-shed boundary-transition signals
locally) plus a large **proposed/not-started v3.0 strategy** docs sweep (pattern-coverage rescope to
*coordination*; the two v3.0 primaries `mycelium-reason` + `mycelium-guardrails`; the LLM-DX build/adopt/
interop strategy; the LangGraph-checkpointer-vs-A2A integration map). Core `mycelium` KV/HLC/consensus
is untouched. Execution evidence this run: `mycelium-core` lib **131/0** (incl. the new
`ops::delivery_shed_tests::boundary_transition_signals_are_never_locally_shed`); main lib
`--features tls,metrics,a2a,llm` **354/0**; `cargo clippy` clean on core + the main feature matrix; the
regression **verified to FAIL with the fix neutralized** (deterministic at `combined_fill = 1.0`); PR
#129 **CI green 14/14** incl. the `Test` job that carried the flake.

**Carried-score caveat — the stale 8s (unknown-unknowns reserve).** Only **6** dimensions have *fresh
evidence this run*: 8 (clippy), 9, 12, 14, 17, 18 (deep-dived). The other **19 8s are carried on Run-36
(or older) evidence and are UNVERIFIED this run** — 1 Philosophy · 2 Conceptual Integrity · 3
Architecture · 4 Modularity · 5 API Design · 6 Error Handling · 7 Configurability · 10 Resource
Management · 11 Semantic Correctness · 13 Security · 15 Performance · 16 Scalability · 19 Observability ·
20 Debuggability · 21 Operational Readiness · 22 Evolvability · 23 Documentation · 24 Developer
Experience · 25 Dependency Hygiene. Per the ledger's demonstrated miss rate, read each as "no *known*
defect, not re-probed" — a *decaying* claim, possibly optimistic — **not** "solid." The two bugs this
session were both unknown-unknowns in dimensions scored 8; several of these carried 8s (esp. those with
ledger histories — Concurrency-adjacent paths, Semantic Correctness, Robustness surfaces) are the likely
homes of the next one. The honest way to convert a stale 8 into a confident one is a probe that finds
nothing — not inheritance.

### Findings

**Major — Robustness (scored 8 — current-state; *not* capped, see the Mean note's correction chain) — opacity control-signal-shed liveness bug.** The opacity governor emits
`BOUNDARY_OPAQUE`/`TRANSPARENT` at `System` scope, and `ops::deliver_locally` probabilistically sheds
non-`Individual` signals by `combined_fill = max(handler_fill, gossip_shard_fill)`. Under CI gossip-drain
starvation `gossip_shard_fill > 0`, so the governor's **single** boundary-transition emission could be
shed from *local* delivery — a permanent miss (the "I'm now shedding" signal dropped by the shedding
mechanism, precisely under load); a local subscriber could miss the transition in production. **Found by
a deliberate root-cause dig** into the 10-run "flaky" `test_manage_opacity_gate_vetoes_then_library_overrides`
(three prior "resolutions" — 3 s→10 s→30 s widening, then a Run-30 pure-function extraction — all treated
it as *latency*; the bug was a dropped signal a layer down). Fixed same session (`d1db7bd`) by exempting
boundary-transition kinds from the local shed (like `Individual` scope) + the deterministic regression
above. **Ledger line already recorded** (2026-07-06, in the fix commit) — that is where the past
over-scoring is accounted for. The *current* Robustness state (defect removed + permanent gate) is
therefore scored **8**, not degraded: finding-and-fixing a bug does not lower a current-state score.

| # | Dimension | Score | Notes |
|---|-----------|:-----:|-------|
| 1 | Philosophy / Coherence with Goal | 8 | carried (v36). The v3.0 primaries (coordinator-free guardrails, substrate-native DX) are on-thesis; read-only this run. |
| 2 | Conceptual Integrity | 8 | carried (v36). |
| 3 | Architecture | 8 | carried (v36). The fix is layer-local (`mycelium-core/ops.rs`); no namespace/layer change. |
| 4 | Modularity | 8 | carried (v36). |
| 5 | API Design | 8 | carried (v36). |
| 6 | Error Handling Model | 8 | carried (v36). |
| 7 | Configurability | 8 | carried (v36). |
| 8 | Language Best Practices | 8 | carried (v36) + fresh: `clippy -D warnings` clean on core + main matrix incl. the changed `deliver_locally`. |
| 9 | Concurrency Correctness | 8 | **Deep-dive.** The fix is in the signal-delivery path but is a *policy* correction (exempt control kinds from the probabilistic shed), not a data-race fix — no new lock/`await` interleaving; the shed is a pure per-signal decision. Held 8: the defect was Robustness (a dropped control signal under load), not a concurrency race. |
| 10 | Resource Management | 8 | carried (v36). |
| 11 | Semantic Correctness | 8 | carried (v36). Core LWW/HLC/consensus untouched; not re-probed. |
| 12 | Robustness | **8** | **Deep-dive; FINDING (Major) — corrected from 6→7→8, see note.** Control-plane `BOUNDARY_OPAQUE`/`TRANSPARENT` were subject to the data-plane probabilistic local shed → dropped under load. **Found + fixed + deterministic regression this run, so the *current* state is a latent defect removed + a permanent gate added — strictly better than Run 36, hence 8, not degraded.** The past over-scoring (Runs 27–36) is recorded in the ledger; that is where the accountability lives, not in the current score. Not 9: only this one slice of Robustness got fresh execution evidence. |
| 13 | Security | 8 | carried (v36). |
| 14 | Failure Mode Legibility | **8** | **Deep-dive.** The boundary-transition *signal* is the local legibility mechanism for "why is this node shedding," and it was the thing dropped under load — now fixed. The durable KV load-state always propagated (bounding the blast radius to local *signal* subscribers), and the fix restores the local path; held 8 (finding was primarily a Robustness/delivery issue, not a legibility-subsystem weakness). |
| 15 | Performance | 8 | carried (v36). |
| 16 | Scalability | 8 | carried (v36). |
| 17 | Testability | 8 | **Deep-dive.** The fix demonstrates good testability — `deliver_locally` is a pure, unit-testable function; the regression pins the invariant *deterministically* (`combined_fill = 1.0` forces the shed) with no async/ticker, exactly the "extract the decision, test it deterministically" pattern. Held 8 (fresh evidence, but one function). |
| 18 | Test Architecture | **7** | **Deep-dive.** The sole sub-8, and *not* a discovery-penalty: it's a **genuine current, structural, un-remediated weakness** — the suite permits timing-sensitive integration tests that gate CI and can *mask* real defects (just demonstrated: a Major liveness bug hid for 10 runs), with **no structural prevention** (each flake is fixed reactively; 8th ledger entry). This run improved it (deterministic regression + the methodology now forbids counting a timeout-widen as a "fix"), but the class-level gap is real and present. 8 is earnable once flaky wall-clock gates are structurally excluded from CI-gating tests. |
| 19 | Observability | 8 | carried (v36). |
| 20 | Debuggability | 8 | carried (v36). |
| 21 | Operational Readiness | 8 | carried (v36). |
| 22 | Evolvability | 8 | carried (v36). Wire policy intact; the v3.0 roadmap is now explicit (two primaries + candidates) — additive, not debt. |
| 23 | Documentation | 8 | carried (v36). Large v3.0 *positioning* sweep added, but it is proposed/speculative (self-flagged as doc-debt risk in this session's critique) — net-neutral to the user-facing docs bar; held 8. |
| 24 | Developer Experience | 8 | carried (v36). |
| 25 | Dependency Hygiene | 8 | carried (v36). No dep change. |
| — | **Floor (lowest 3)** | **7, 8, 8** | Test Architecture (7 — the sole sub-8: a *genuine current* structural weakness, the recurring flaky-gate pattern with no structural prevention, 8th ledger entry) · two 8s. Robustness and Failure-Mode Legibility are back at 8 — their defect is *fixed and gated*, so their current state is not degraded. |
| — | Mean (continuity footnote) | 7.96 | not a target (sum 199/25 = 7.96). Essentially flat vs Run 36's 8.00; the only sub-8 is Test Architecture, a real *current* weakness — not a penalty for fixing a bug. **Correction chain (this run was re-scored twice under challenge): first cut capped Robustness at 6 (mechanical finding-cap); second held it at 7 ("lasting skepticism"); both were the same error — a *fixed-and-gated* defect makes the dimension's current state *better*, not worse, and the accountability for the *past* over-scoring lives in the calibration ledger, not the current number. Final: current score = current state; a discovered-and-fixed bug adds a ledger entry and does not lower the current score. The methodology is amended to this principle (not just the narrow cap exception).** |

## 2026-07-07 — Run 38 (M2)

Deep-dive dimensions this run (rotation — the least-recently-covered band): **11 Semantic
Correctness, 13 Security, 15 Performance, 19 Observability, 20 Debuggability**. The material
diff since Run 37 is the largest single-day workstream of the series: the **artifact library**
(design record `docs/design/artifact-library.md`, ✅ adopted & implemented same day) — 25
commits: durable `FsLibrarySource` + signed manifest, librarian role + capability-ring holder
discovery, `ArtifactKind` + `ArtifactRuntime`/`Installed` (WasmHost becomes one runtime;
`BlobRuntime` streams/places models), whole-entry provenance, resource-aware eligibility
(§4.4), probe-gated health, the HTTP object-store source, honest `catalog`/`mcp_toolgrowth`
demos + the **`model_deploy`** manual demo (a real 19 MB GGUF **and its deployment profile as
two signed artifacts**, activated into Ollama, generating real tokens), three wiki-lints, the
KV-namespace-table canon repair (nine missing prefixes), the lock-order table extended to the
full workspace (rows 20–30), and the wiki test port-race class retired. Execution evidence this
run: `mycelium-wasm-host` **55/0** lib + **4/0** e2e (incl. the three kept falsification probes
below and the four lifecycle/concurrency tests); `mycelium-wiki` control-plane **24/0** +
gateway; `make check` green; coop `ci_smoke` **12/12**; **`model_deploy` run live twice on real
hardware** (real streamed percent, real `ollama create`, real tokens; `ollama show` asserting
the governed SYSTEM prompt); **8 CI runs green today** (one red caught + fixed: the RUSTSEC
audit + the wiki flake — both same-day); `cargo audit` clean post-`crossbeam-epoch` bump.

**Falsification probes (three highest provisional: 10, 18→re-judged, 1) — all kept as
permanent tests:**
- **Probe A (Resource Management)** `agent_shutdown_mid_install_is_harmless` — agent shut down
  under an in-flight gated install; the detached task resolves, nothing panics, rounds on the
  dead agent are safe no-ops. **PASS.**
- **Probe B (Test Architecture / codec)** `decode_and_manifest_parse_are_total_over_adversarial_bytes`
  — every truncation + per-byte corruption of a signed entry: no panic, and nothing that still
  decodes passes provenance **except mutations inside the deliberately-unsigned cost-hint bytes**
  (the property encodes the design's exact signing boundary). Manifest lines: malformed/unknown-
  version lines are errors, never skips. **PASS.**
- **Probe C (Philosophy / no-coordinator)** `self_election_writes_no_scheduler_state_to_the_fleet`
  — after resource-checked election + install + extra rounds, the only new fleet KV is the
  `cap/` advertisement family: no assignment keys, no resource gossip, no queue. **PASS** (first
  attempt failed on the probe's *own* timing bug — the ad's KV write lands async; fixed with a
  structural poll. Probe defect, not system defect.)

### Findings
- **Minor — Evolvability (22): CHANGELOG `[Unreleased]` said "Nothing yet"** after the entire
  workstream shipped. **Fixed this run** (full Added/Fixed entry written); scored at fixed
  state.
- **Minor (gap, not an invariant break) — Observability (19): the new artifact tripwire
  counters (`ineligible_skips`, resource-skip reasons) are programmatic-only** — not exported
  via `/stats` or `/metrics` (no doc promises it, hence a gap not a broken claim; the honest
  fix is `metrics`-facade instrumentation in the companion). **Live** → 19 scores 7 and heads
  the improvement targets.
- **Standing (Run 37's structural weakness, RECONFIRMED live): Test Architecture (18)** — the
  class "timing-sensitive integration tests gate CI with no structural prevention" produced a
  red main *today* (wiki `AddrInUse`). The port-race *family* is now structurally retired
  (pair-retry idiom + testing.md lore) and today's new tests are structural-poll clean, but the
  class-level gap Run 37 named persists → 18 stays 7. Ledger line added for Runs 32–36.
- Ledger line also added for Documentation (23), Runs 33–37 (the examples.md eleven-vs-twelve
  miscount, found by lint 8).

| # | Dimension | Score | Notes |
|---|-----------|:-----:|-------|
| 1 | Philosophy / Coherence with Goal | 9 | Probe C (kept test): self-election leaves zero fleet scheduler state; `ci_smoke` 12/12 + `model_deploy` live — the coordinator-free provisioning thesis *executed* end-to-end (librarian = role; origin death → installs continue). Design §4.4 explicitly rejects resource gossip/best-fit ranking. |
| 2 | Conceptual Integrity | 8 | Diff-reviewed: the new subsystem carries the house invariants (detection-not-prevention → tripwires; restart≡provisioning → ordering, health, retry ALL reduce to it; content-address everywhere). No fresh whole-dimension evidence. |
| 3 | Architecture | 8 | L5 held: core `src/`+`mycelium-core/` untouched all day (verified by diff); companion built on public API only; namespace table repaired (nine missing prefixes — lint 7) now matches writers. `make check` (incl. no-default-features) green. |
| 4 | Modularity | 8 | Kind-dispatch registry keeps runtimes independent; the demand/presence/shed loops were untouched by the generalization — evidence the seams were right. |
| 5 | API Design | 8 | New surface is builder+RAII consistent; one API risk noted: `Installed::probe` cheap-under-lock is a doc contract only; `ActivateFn` runs sync in async context (documented). |
| 6 | Error Handling Model | **7** | **Down from 8 (current-state):** the new app layer is stringly (`InstallError(String)`, `ActivateFn → Result<(), String>`) against the typed core — a real consistency regression introduced today. Functional (retry semantics work, tested) but callers can't match on cause. |
| 7 | Configurability | 8 | Resource policy (probe + headroom, default-on 0.8), install budget, chunk size, egress — knobs are operational and orthogonal. `reqwest` ungated in wasm-host noted under 25. |
| 8 | Language Best Practices | 8 | clippy `-D warnings` clean across the full matrix + wasm-host all-targets (several lints caught + fixed during the day); no `unsafe`; but see 6 — the stringly app-layer errors keep this at 8. |
| 9 | Concurrency Correctness | 8 | Heavy new concurrent machinery (async reservations, token-checked completion, rows 20–23) with dedicated race tests green (`withdraw_during_install…`, `failed_install…`, joint-reservation accounting). Ledger-heavy dimension; tests are same-day self-authored — 8, not 9. |
| 10 | Resource Management | **9** | Probe A (kept) + the lifecycle suite: shed deletes placed bytes, stale installs explicitly uninstalled, librarian handle aborts on drop, `Wiki`-style task hygiene respected. Named fresh evidence across the dimension's new surface. |
| 11 | Semantic Correctness | 8 | **Deep-dive (read):** `lww_wins` convergence re-derived by hand (tombstone-beats-data at equal ts is order-independent; byte tiebreak commutes — both application orders checked); HLC tick CAS + saturating logical verified; quorum = floor(N/2)+1 with documented no-BFT scope. Core suites not re-run this run → 8, honestly read-capped. |
| 12 | Robustness | 8 | Fresh adversarial evidence on the new surface (Probe B; lying-source in both pull flavours; complete-or-absent under ENOSPC/crash; unknown-version rejection) — but the dimension is broader than today's slice and Run 37's finding is one day old. |
| 13 | Security | 8 | **Deep-dive:** whole-entry provenance closed a real re-label hole *before* first deployment (tamper matrix run today: capability re-label, kind flip, requirement tamper all fail; hints-only mutations pass — by design, probe B pins the exact boundary). Egress gate denies before dispatch (zero-connection test). Gap found by the dive: **publisher-key rotation/revocation posture is undocumented** (trusted-list-restart is the implicit story) — keeps this at 8. |
| 14 | Failure Mode Legibility | 8 | Distinct tripwire reasons, install errors carry retry semantics in the message, probe-withdraw warns with artifact ids, loading tiers make progress visible. |
| 15 | Performance | 8 | **Deep-dive (read):** streaming pull never materializes a model in memory and runs on the blocking pool; probe pass is O(hosted) under one lock; noted: `provision_round` clones the catalog Vec per round and `eligible()` re-locks per entry — fine at current scale, flagged for a future catalog-size cliff. No benchmark run → capped 8. |
| 16 | Scalability | 8 | Per-hash librarian ads rejected with explicit namespace-flood analysis (§6); direct store pulls keep models off the 10 MiB mesh frames; loading-tier re-adverts bounded to 10 % steps. |
| 17 | Testability | **9** | The whole day is the evidence: every new mechanism was testable via injected fakes (`GatedRuntime`, `TrackingRuntime`, `FixedProbe`, lying sources, gated semaphores) without a cluster; 55-test suite exercises them; probes A–C were writable in minutes *because* the seams exist. |
| 18 | Test Architecture | **7** | Held at Run 37's 7 — the structural weakness it named **reconfirmed live today** (a wiki port-race flake turned main red). The *family* is retired structurally (pair-retry + lore) and the new suite is exemplary (lifecycle, property probe B kept, structural polls), but timing-sensitive CI-gating tests still have no class-level prevention. 9th ledger entry. |
| 19 | Observability | **7** | **Deep-dive; Minor gap (live):** node surface strong (`/stats` tripwires, `/metrics`, explain/diagnose — read, not probed live this run), but the new artifact tripwire counters are **programmatic-only**; an operator can't see resource-skip storms without embedder wiring. Fix path named (metrics facade in the companion). |
| 20 | Debuggability | 8 | **Deep-dive (read):** failure paths log with artifact ids + reasons; the librarian logs reconcile deltas; `model_deploy` preflight prints exact remediation; kv-dump + explain/diagnose unchanged. Not probed live → 8. |
| 21 | Operational Readiness | 8 | The artifacts runbook is now a real operator path (publish flow with footprint guidance, librarian how-to, remote-store note); `model_deploy` doubles as an operator rehearsal and was run twice for real. |
| 22 | Evolvability | 8 | Finding (Minor) fixed this run: CHANGELOG Unreleased written. Versioned entry encoding with explicit rejection semantics; clean-slate decision + declined-with-evidence step 7 both recorded ADR-style; only the crate-naming question open. |
| 23 | Documentation | **9** | Three wiki-lint passes executed today (findings fixed, links verified programmatically each pass); the design note reads end-to-end as the day's decision record; runbook, README, demo docs all reconciled same-day (ship-time ingest held twice). Ledger line added for the pre-existing examples.md miscount — past accountability, current state earned. |
| 24 | Developer Experience | 8 | `make check` contract defended under pressure (the llm_agent de-simulation was *declined* specifically to keep wasmtime out of it — recorded); one-call publish step; demo preflights. |
| 25 | Dependency Hygiene | 8 | Adds: `sysinfo` (system+disk features only), `reqwest` (rustls, no default features — but ungated in wasm-host: flagged), `async-trait`. `cargo audit` clean in CI after same-day RUSTSEC-2026-0204 bump (bench-only path). |
| — | **Floor (lowest 3)** | **7, 7, 7** | Error Handling (stringly new app layer) · Test Architecture (standing structural weakness, reconfirmed live) · Observability (companion tripwires not operator-visible). |
| — | Mean (continuity footnote) | 8.04 | not a target (sum 201/25). Up from 7.96 on named fresh evidence (four 9s, each probe- or execution-backed); the floor widened to three 7s — all three are *current, actionable* weaknesses, which is the audit working. |

## 2026-07-08 — Run 39 (M2)

Deep-dive dimensions this run: **3 Architecture, 5 API Design, 13 Security, 23 Documentation, 25 Dependency Hygiene** — the band the v3.0 companions (`mycelium-reason`, `mycelium-guardrails`) most touched, and the least-recently deep-dived. Execution evidence (all this run): `make check` green; `cargo clippy --lib --no-default-features -D warnings` clean (feature-gated dead-code trap); `mycelium-reason` suite 9+2+6; `mycelium-guardrails --features compliance` suite 8+2+1 (incl. the Tier-C denial+seal+chain-verify test); public-API-only grep on both new companions (empty). Context: 41 commits / 10 PRs (#130–#139) since Run 38 — both v3.0 primaries shipped (the LangGraph DX ladder + structural guardrails).

### Findings
None — all three falsification probes passed. **Probe A (Architecture/Modularity, kept as the standing composability check):** grep both new companions for `pub(crate)`/`mycelium_core::`/internal reach-ins → empty; they compose on the public `mycelium` API only (zero core changes for either primary — verified by diff). **Probe B (Dependency Hygiene):** `--no-default-features` clippy clean — the two new crates don't break the minimal build. **Probe C (Security/hard-prevention):** the guardrails compliance suite's `tier_c_denies_unauthorized_caller_and_seals_the_denial` — an unauthorized caller is rejected at the provider, the `Invoke`/`Denied` record is sealed, and `audit_verify` passes — ran green (falsifies "the Tier-C gate can be bypassed / the denial can be forged"). *Note (not ledger material):* v3.0 development found+fixed+gated three real issues in **new** code — a same-millisecond HLC trace-key collision (per-writer substreams), the dead-node routing latency (liveness filter + `failover_timeout`, canary `liveness_filter_drops_a_non_peer_cap`), and the empty-blob mesh-fetch edge — none existed during a prior scored run, so no calibration-ledger entry (they are healthy digging, not miscalibration).

| # | Dimension | Score | Notes |
|---|-----------|:-----:|-------|
| 1 | Philosophy / Coherence with Goal | 8 | Both v3.0 primaries deepen the thesis: `mycelium-guardrails` **actively refused** a central policy authority (self-imposed stance — a policy server is the chokepoint non-goal), `mycelium-reason` added zero core changes. Thesis freshly re-evidenced via the public-API-only probe (composability) + the `guardrail_fleet` no-chokepoint demo, but not a full thesis-execution run this cycle → decayed from Run 38's model_deploy-earned 9. |
| 2 | Conceptual Integrity | 8 | The companions carry the house idiom (RAII handles, public-API-only, honest-framing) and add a new one consistently — *tier/strength-labelled honesty* runs through both tracks (`expressible≠validated`, the three guardrail tiers, PROVES/DOES-NOT-PROVE). No fresh whole-dimension evidence → 8. |
| 3 | Architecture | 8 | **Deep-dive:** L5 boundary held — both primaries are companions on the public API (Probe A empty), zero `src/`/`mycelium-core/` changes (diff-verified); new KV prefixes documented (lint 2). `make check` + `--no-default-features` green this run. The `InferenceRouter` is a companion-layer *routing policy* over load-blind `resolve`, not a core change — correct placement. |
| 4 | Modularity | 8 | The two new companions don't cross-couple (Probe A); each handle/crate reasoned independently. Carried 8 + fresh composability evidence. |
| 5 | API Design | 8 | **Deep-dive (source read):** the new surfaces are builder+RAII consistent and hard-to-misuse — `Policy`→`apply`→`AppliedPolicy`, `InferenceRouter`/`serve_model`/`ReasonClient`, `prove_denials`. Highlight: `Policy::strength_report()` makes guarantee-strength *legible* rather than hiding it (the tier labels), and `narrate_proof` states its own limits in output. No footgun found; 9 would need whole-dimension execution beyond reading → 8. |
| 6 | Error Handling Model | **7** | Carried from Run 38 (current-state): the new *companion* layers are cleanly typed (`RouteError`, `RouteExhaustedError`, `PolicyViolation`, `ResumeError`), but the Run-38 finding — the stringly `InstallError(String)`/`ActivateFn→Result<(),String>` app layer in `mycelium-wasm-host` — **persists untouched**, so the dimension's current state still carries the regression. The v3.0 work is a positive counter-example, not a fix. |
| 7 | Configurability | 8 | `RouterConfig` (now with `failover_timeout`), the `Policy` builder, and the crate feature layering (`llm`/`gateway`/`compliance`) are orthogonal and operational. |
| 8 | Language Best Practices | 8 | clippy `-D warnings` clean across the feature matrix incl. both new crates (fresh this run); companions idiomatic (RAII, struct-update to dodge `field_reassign_with_default`, no `unwrap` off the test path, no `unsafe`). |
| 9 | Concurrency Correctness | 8 | Carried 8 + a net-positive this cycle: the dead-node routing weakness (found in new code) was fixed with the live-SWIM-membership filter + `failover_timeout` and a deterministic canary; **zero new locks** in either companion (lock-order table stays 30 rows). Core store/connection/consensus **not re-probed this run** — ledger-heavy dimension, so carried-stale, not lifted. |
| 10 | Resource Management | 8 | Companion RAII is clean (`ModelReg`/`GuardHandle`/`BlobServerHandle`/`ModelDependency` drop-retract; `AppliedPolicy` holds the state machine). Decayed from Run 38's lifecycle-suite-earned 9 — that suite was not re-run this cycle. |
| 11 | Semantic Correctness | 8 | The trace HLC-collision catch (per-writer substreams) and the checkpointer's content-addressing (dedup) are correct-by-construction; the blob tier verifies SHA-256 on read; core LWW/HLC/quorum unchanged and not re-derived this run → carried 8. |
| 12 | Robustness | 8 | New adversarial surface handled: dead-node routing (liveness + fast failover), unauthorized invokes (Tier-C reject+seal), verify-on-read blobs, the checkpointer's empty-blob edge. Broader dimension not fully re-probed → 8. |
| 13 | Security | 8 | **Deep-dive:** the guardrails track is a genuine security *addition*, freshly evidenced (Probe C green): Tier-C `authorized_callers` = hard prevention (unauthorized invoke rejected at the provider), denials **sealed** into the tamper-evident Ed25519 chain, `prove_denials` reconstructs+verifies, principals mTLS-bound. The **honest three-tier framing** (hard vs self-imposed vs transition) is security maturity, not marketing. 9 would need the whole security surface (mTLS/framing/http-auth/consensus-signing) freshly exercised; Run-38's publisher-key-rotation-posture gap also still open → 8. |
| 14 | Failure Mode Legibility | 8 | Improved this cycle: `prove_denials`/`narrate_proof` (why an agent was stopped, cryptographically), `strength_report` (which guarantee is which), reason traces, and the router's dead-node failure now legible. Carried 8, nudged by real new tooling. |
| 15 | Performance | 8 | Carried from Run 38's read-8, **stale**: no benchmark run this cycle. The router adds a cheap `peers()` set-membership filter; `failover_timeout` improves dead-node tail latency. Run 38's flagged `provision_round` catalog-clone cliff is unaddressed but unchanged. |
| 16 | Scalability | 8 | Carried, **stale**: no fresh scale-suite run. New companion limit, documented and honest: the reason blob tier is single-frame v1 (≤ 8 MiB; chunked transfer is the named follow-up); the checkpointer floods only *metadata* in KV (payloads in the blob tier) — scale-conscious by design. |
| 17 | Testability | 8 | The companions are highly testable (real-agent `start_pair`/`start_mesh` idioms, injectable backends `EchoBackend`, deterministic demos); their suites run in ~1–2 s. Decayed from Run 38's injected-fakes 9 (that evidence was the artifact-library day, not re-run at that scope here). |
| 18 | Test Architecture | **7** | **Structural weakness persists (current-state, per Run 38).** The v3.0 work added many *deterministic* tests (the guardrail canaries, the liveness canary, the fleet/wedge demos, the flake tier from Run 38) — genuinely healthy — but the standing class-level issue (timing-sensitive socket-binding integration tests have no *structural* prevention, only the retry-tier mitigation) is unaddressed, and the Ollama flagship variant is shipped **compile-verified-but-unrun** (a coverage gap). Sub-8 until the structure changes, not the count. |
| 19 | Observability | **7** | Carried from Run 38 (structural gap persists): the new operator-visible surfaces are real (`/gateway/reason/trace`, `prove_denials`, `strength_report`), but Run-38's specific finding — the artifact/companion tripwire counters are programmatic-only, invisible to an operator without embedder wiring — is **unaddressed and not live-probed this run**. The new tooling is a noted plus; the flagged gap keeps it at 7. |
| 20 | Debuggability | 8 | Carried: `prove_denials`/`narrate`/trace replay + the strength report make the new subsystems inspectable; kv-dump + explain/diagnose unchanged. Not live-probed this run → carried 8. |
| 21 | Operational Readiness | 8 | Carried: `is_ready`/`shutdown`/back-pressure unchanged; the companions shut down cleanly (RAII); guide chapters 15/16 add operator paths. The Ollama variant is manual/unrun (readiness caveat, counted under 18). |
| 22 | Evolvability | 8 | The companion-crate pattern is the evolvability *proof*: two major v3.0 primaries added with **zero core changes** and WIRE_VERSION unchanged (12); CHANGELOG maintained across all PRs; ADR-style plans + reassessments recorded. |
| 23 | Documentation | 8 | **Deep-dive:** guide **chapters 15 + 16** added (comprehensive, tier-honest); a fresh **wiki-lint 2** executed this run (links verified, ledger/positioning/roadmap staleness fixed, both companions homed in `companions.md`). Decayed from Run 38's three-lints-that-day 9 — this run verified links + staleness but did not re-verify every guide example runnable end-to-end. |
| 24 | Developer Experience | 8 | `make check` + the per-crate `ci_smoke.sh` (both guardrail demos, the reason example) + the CLAUDE.md on-ramp; the public-API companion pattern is a clean contribution path. Clean build this run. |
| 25 | Dependency Hygiene | 8 | **Deep-dive:** the new crates add minimal, well-chosen deps (`mycelium-reason`: tokio/bytes/serde/sha2/tracing/+axum; `mycelium-guardrails`: tokio/tracing only). `--no-default-features` compiles (Probe B); `Cargo.lock` present; `cargo audit` green in CI. No supply-chain expansion of note. |
| — | **Floor (lowest 3)** | **7, 7, 7** | Error Handling (wasm-host stringly app-layer regression, persists from Run 38) · Test Architecture (structural no-class-level-prevention weakness + Ollama variant unrun) · Observability (companion tripwires programmatic-only, persists). All three are *current, actionable, and orthogonal to the v3.0 work* — the audit correctly not crediting the primaries for fixing them. |
| — | Mean (continuity footnote) | 7.9 | not a target (sum 197/25 = 7.88). Down from 8.04 **not because the project regressed** — the floor is identical (same three structural 7s) and two v3.0 primaries shipped clean with fresh passing evidence — but because Run 38's four fresh-evidence 9s (Philosophy/Resource-Mgmt/Testability/Documentation, all artifact-library-day-specific) decayed to 8 under the execution-evidence gate: this cycle's fresh evidence is companion-specific and honestly supports solid 8s, not re-earned 9s. The mean is a footnote; the floor is the headline. |

## 2026-07-09 — Run 40 (M2)

Deep-dive dimensions this run: **9 Concurrency Correctness, 11 Semantic Correctness, 12 Robustness, 18 Test Architecture, 21 Operational Readiness** — the band the #151 exact-once fix + the deploy scaffolding most touched. Execution evidence (all this run): `make test-overlay` **6/6** (S11 exact-once got-5 + S12/S13 green each run); lib suite **354 passed / 0 failed**; `make check` green (feature-matrix clippy incl. `--no-default-features`); falsification probes — LWW (3) + HLC (1) + framing (1) + oversized (1) all pass; and earlier this session: langgraph rungs 0–6 **7/7** end-to-end, coop `ci_smoke` **12/12**, `hello_mesh`/`hello_capability`/`conway` run. Context: #140–#151 since Run 39 (test-util floor fix, examples standard, k8s+terraform deploy scaffolding, and the **#151 exact-once correctness fix**).

### Findings
- **Major (fixed this run) — Concurrency/Semantic Correctness.** `subscribe_log_group`'s gateway consumer-group endpoint had a bare-LWW "distributed lock" with **no cross-node mutual exclusion** → 100% double-delivery (overlay S11 "got 10" for 5 tasks). Found by gating the manual-only overlay suite (#147). **Fixed (#151):** single-active consumer via a leased consensus claim with **converged-holder confirmation** (the propose return is optimistic; read the LWW-by-HLC converged committed holder). Verified `make test-overlay` **6/6**; contract clarified + pinned in `runtime-invariants`. Regression gate: overlay S11 (deterministic, not yet CI — overlay job is on the held #147 branch). Ledger: 2 entries.
- **Minor (unfixed on `main`) — Test Architecture.** S13 (integration tuple-space) is ~50% flaky; `main`'s `run.sh` is one-shot (retry-hardening stuck on held #147). Tracked #150. Caps the dimension.
- Falsification probes (3, all passed, kept as gates): LWW/HLC convergence, framing malformed/oversized frame survival, overlay exact-once under concurrent cross-node consumers.

| # | Dimension | Score | Notes |
|---|-----------|:-----:|-------|
| 1 | Philosophy / Coherence with Goal | 8 | The #151 contract clarity (single-active log consumer vs tuple-space work queue) *reinforces* coordinator-free/library-not-platform; deploy scaffolding is explicitly "reference, not platform". No drift. Read-based → 8. |
| 2 | Conceptual Integrity | 8 | Consistent idiom; `SubscribeHandle` removed cleanly. One honest ding surfaced+fixed: "consumer group" naming implied load-balancing while the contract is single-active — now disambiguated in docs. |
| 3 | Architecture | 8 | Layers respected; the exact-once fix uses Layer-III consensus correctly at the overlay endpoint; companions still public-API-only. Carried (v39) + fix examined. |
| 4 | Modularity | 8 | `SubscribeHandle` module deleted with no coupling fallout (lib 354/0). Carried (v39). |
| 5 | API Design | 8 | `subscribe_log_group` contract now legible; surface stable. Carried (v39), stale on the un-touched handles. |
| 6 | Error Handling Model | 7 | **Carried (v39), stale.** The Run-38/39 finding — stringly `InstallError(String)` in `mycelium-wasm-host` app layer — **persists untouched** this cycle. Current-state weakness. |
| 7 | Configurability | 8 | Carried (v39), stale — not re-examined; no config surface change of note. |
| 8 | Language Best Practices | 8 | Carried (v39), stale. `#151` code is idiomatic (matches!/`let-else`, no new unsafe); the 433 `unwrap/expect` in core+agent are dominated by poison-recovering lock idioms + serde-on-known-good, not re-audited line-by-line this run. |
| 9 | Concurrency Correctness | 8 | **Deep-dive.** A real cross-node race (exact-once double-delivery) found **and fixed + verified 6/6** this run — current-state improves, not drops (fix + deterministic gate). Held at 8 not 9: ledger-heaviest dimension (now +2 entries), and the fix just shipped — "solid + verified", not "excellent". |
| 10 | Resource Management | 8 | Carried (v39), stale. The leased claim adds a lease-renew loop that exits on lost-claim (no leak path found on read); not lifecycle-probed beyond the endpoint. |
| 11 | Semantic Correctness | 8 | **Deep-dive.** Exact-once (a semantic contract) was violated, now fixed + gated; the consensus optimistic-commit semantics are correctly handled (converged-holder read). LWW/HLC probes pass (3+1). Held 8 — just-shipped fix + ledger history temper from 9. |
| 12 | Robustness | 8 | **Deep-dive + probe.** Malformed/oversized framing tests pass this run; the consensus optimistic-commit edge (two proposers both commit) is now *handled by design* rather than a latent hazard. 9 would need the whole hostile-input surface freshly swept → 8. |
| 13 | Security | 8 | Carried (v39), stale. Guardrails Tier-C + tamper-evident chain unchanged; Run-38 publisher-key-rotation-posture gap still open. Not re-probed this run. |
| 14 | Failure Mode Legibility | 8 | Carried (v39), stale. `#151`'s honest doc + the runtime-invariants "do not fix these" note improve legibility of *this* subsystem; broader surface not re-swept. |
| 15 | Performance | 8 | Carried (v39), stale — no fresh perf run. Leased-claim adds a periodic consensus renew (bounded, ~2/s standby retries), not on the data hot path. |
| 16 | Scalability | 7 | **Decayed (was v39 8), stale.** No fresh scale-suite run in two cycles; the single-host Docker-bridge ~100-node cliff persists (documented). Multi-host k8s is a real *mitigation path* but **validated-offline, not applied** — an unverified escape, so the carried 8 decays to an honest 7. |
| 17 | Testability | 8 | Carried (v39). `test-util`/`alloc_port` feature (merged #141) genuinely improved cross-crate injectability; not re-deep-dived this run. |
| 18 | Test Architecture | 6 | **Deep-dive — floor + headline.** The Run-39 structural weakness (no class-level prevention of timing-flaky tests) **manifested concretely**: S13 is ~50% flaky and **unfixed on `main`** (one-shot `run.sh`; retry stuck on held #147, #150). And the exact-once bug hid precisely *because* the correctness Docker suites (overlay) run **manual-only, never gating CI** — a structural gap this session exposed. Real strengths too (test-util, coop 12/12, overlay now a real gate) — but a live/unfixed flaky scenario caps at 6. |
| 19 | Observability | 8 | **Lifted from v39 7.** The Run-38/39 finding (companion/artifact tripwire counters programmatic-only) is addressed: #140/#144 emit route + guardrail counters through the `metrics` facade (`/metrics`) and ship `docs/operations/metrics.md` as the single reference. Merged artifacts; the facade is no-op without a recorder (not live-scraped this run) → 8 not 9. |
| 20 | Debuggability | 8 | Carried (v39), stale. `/gateway/explain`+`/diagnose`, mesh dashboard, `/ready` unchanged. |
| 21 | Operational Readiness | 8 | **Deep-dive.** Real addition: the reference **k8s manifests** (rendered, 7 resources) + **Terraform** (EKS/GKE) making `deployment.md`'s prose real — but **validated-offline / not-applied** (honest labels). Core `is_ready`/`shutdown`/`sys/load` solid; coop+overlay+langgraph op-adjacent suites ran green. The unverified-on-a-real-cluster caveat holds it at 8. |
| 22 | Evolvability | 8 | Carried (v39). Wire v12/PREV 11 policy intact (framing untouched by #151); CHANGELOG `[Unreleased]` actively maintained; #151 removed a module cleanly. |
| 23 | Documentation | 8 | Examples-doc **standard + index** (#146, 5 READMEs normalized), the #151 contract clarity across library+gateway docs + the S11 docstring, three wiki-lints this session (incl. today's post-#151 doc-vs-code fixes). Read+lint-verified → 8. |
| 24 | Developer Experience | 8 | `hello_mesh`/`hello_capability` starters + the funnel (#142/#143) and the examples standard genuinely smooth onboarding; `make check` is the one-command gate. Carried+examined → 8. |
| 25 | Dependency Hygiene | 8 | Carried (v39). `--no-default-features` compiles (`make check` this run); `Cargo.lock` present; no new deps from #151 (fix is pure std/existing). `cargo audit` green in CI. |
| — | **Floor (lowest 3)** | **6, 7, 7** | **Test Architecture** (S13 ~50% flake unfixed on `main` + correctness suites manual-only — the gap that hid #151) · **Error Handling** (wasm-host stringly app-layer, persists from Run 38) · **Scalability** (single-host cliff; multi-host escape validated-offline-not-applied). |
| — | Mean (continuity footnote) | 7.8 | not a target (sum 196/25 = 7.84). Essentially flat vs Run 39's 7.88, but the *shape* moved: Observability lifted 7→8 (#144 addressed its finding), Test Architecture dropped 7→6 (its structural weakness manifested concretely as an unfixed flaky scenario), Scalability decayed 8→7 (stale + unverified escape). The floor **worsened** (6 vs 7) — correctly: the headline is that gating the manual-only suites found a real exact-once bug (now fixed, ledger-recorded) and a still-unfixed flake. The mean is a footnote; the floor is the story. |

## 2026-07-10 — Run 41 (M2)

Deep-dive dimensions this run: **1 Philosophy, 2 Conceptual Integrity, 3 Architecture, 4 Modularity, 5 API Design** (new rotation cycle). Execution evidence (all this run): `make check` green (feature-matrix clippy incl. `--no-default-features`); local `make test` **13/13**; hosted cluster-suites **green ×4** (runs 29071006043 + 2 stability reruns + combined-build 29074990753 — Integration 13/13 + Overlay 3/3 each); tuple-space failover suite **6/6** (incl. the 3 new promotion/succession gates, one canary-verified failing on pre-fix code); probes `state_chunk_paginates_without_loss_or_duplication` **1/1** and `connect_peer_racing_shutdown_is_safe` **1/1** (both kept as permanent tests); wiki lint 10 doc-vs-code sweep clean; RUSTSEC audit green on every PR. Context: #152–#160 since Run 40 — connect_peer (+active warm), spurious-promotion fix, join-time backfill, HTTP reuseaddr, the Docker cluster-suites CI gate (no retries), and the self-hosted scale nightly.

### Findings
- **Major, live — 18 Test Architecture:** integration scenario 11 (AFN) **flaked once on the hosted no-retries gate** (main-push run `ead3f6d`: died ~5 s in, empty stderr, 12/13; adjacent runs green) and the **red main run went unnoticed** at the time — found by this run's P-18 probe checking the path filter. Cause unknown; filed **#161**. Caps 18 at 6.
- **Minor, live — 5 API Design:** `put` under the default `BackpressureMode::Raise` fails `NoProvider` *instantly* during capability discovery while `take`/`complete` wait (`resolve_primary_blocking` — #154 fixed only the read side). Hit in practice writing the succession test. Caps 5 at 6.
- **Minor, fixed this run — harness:** the scenario ERR trap did not fire inside shell *functions* (`set -o errtrace` missing) — the direct reason the AFN failure is undiagnosed. Fixed in `helpers.sh` + verified with a function-internal failure naming its line; the next AFN occurrence self-diagnoses.
- Probes P-11 (state_chunk pagination: no loss/duplication, acks drop, cursor terminates) and P-9 (`connect_peer` hammered across shutdown: no panic, clean teardown, post-shutdown inert) both **passed**.

| # | Dimension | Score | Notes |
|---|-----------|:-----:|-------|
| 1 | Philosophy / Coherence with Goal | 8 | **Deep-dive.** `connect_peer` litmus-checked against the Holland model: forwarding stays unconditional (pin ≠ withholding; flood fallback intact); detection-not-prevention upheld through a heavy week (tripwires + warns, no Layer-I laws). Philosophy re-read this run. |
| 2 | Conceptual Integrity | 7 | **Deep-dive.** New API naming consistent (`connect_peer`/`disconnect_peer`, `state_chunk` beside `wal_read_chunk`). Warts: `connect_peer` housed in `kv.rs` though it is routing, not KV; `run_gossip_shard` at 18 hand-threaded params and growing. |
| 3 | Architecture | 8 | **Deep-dive.** The pin lives at the right layer (routing latency, not admission/semantics); backfill is companion-internal over the public RPC; lint 10 verified namespace + lock tables against code. |
| 4 | Modularity | 7 | **Deep-dive.** Sub-handles clean; internal plumbing accretes — `pinned_peers` hand-threaded through mod/lifecycle/tasks is the third such shared-set (peers, writers, pins). |
| 5 | API Design | 6 | **Deep-dive — finding (live, Minor).** put-vs-take discovery asymmetry under default config (above). Otherwise the week's additions are good citizens (idempotent, paired, documented call-ahead contract). |
| 6 | Error Handling Model | 7 | Typed errors held under this week's failures (lib surfaced `AddrInUse` as typed Io; the panic was the example's `expect`). Un-swept handles stale. |
| 7 | Configurability | 7 | Orphan grace deliberately hardcoded (10 ticks, documented); `cap_refresh` doc updated. Carried elsewhere (v40), stale. |
| 8 | Language Best Practices | 8 | `make check` clean across the matrix this run; clippy caught + fixed type-complexity in new code. |
| 9 | Concurrency Correctness | 8 | P-9 shutdown-race probe passed + kept as gate; lock-order table reconciled name-by-name (lint 10); `pinned_peers` correctly lock-free. Ledger-heaviest dimension — 8, not 9. |
| 10 | Resource Management | 8 | P-9 doubles as lifecycle evidence: new warm-keeper/backfill/promotion tasks tear down cleanly under a shutdown race; post-shutdown calls inert. |
| 11 | Semantic Correctness | 8 | P-11 passed; failover 6/6 (canary-verified gates); hosted suites green ×4. Held from 9: the undiagnosed AFN flake (#161) could yet land here. |
| 12 | Robustness | 8 | Restart robustness materially improved (reuseaddr #160) and verified on hosted restart scenarios ×4; hostile-input surface not re-swept this run. |
| 13 | Security | 7 | Carried (v38) — stalest carried dimension; unverified this run, read as possibly optimistic. |
| 14 | Failure Mode Legibility | 8 | The week's diagnostics *worked in anger*: node-log dump named the split-brain + AddrInUse directly; errtrace fix verified live. |
| 15 | Performance | 7 | Carried (v39), stale. |
| 16 | Scalability | 7 | Carried (v39), stale; 100-node nightly queued on runner registration (#157). |
| 17 | Testability | 8 | 3-agent succession chains + both probes constructed rapidly against public APIs — fresh usage evidence of the harness. |
| 18 | Test Architecture | 6 | **Finding (live, Major) — floor + headline.** The pyramid is the strongest it has ever been (unit → deterministic gates → Docker PR gate, no retries → scale nightly), and it is *catching everything* — but a once-flaked, undiagnosed scenario (#161) sits live on the no-retries gate, and its red main run went unnoticed. Cap lifts when AFN is diagnosed (errtrace now makes that self-serve). |
| 19 | Observability | 7 | Carried (v40), stale. |
| 20 | Debuggability | 8 | Trap + node-log dump + take instrumentation diagnosed three real failures this week from CI logs alone (named runs). |
| 21 | Operational Readiness | 8 | Hosted restart scenarios green ×4; succession failover (operator story: replace a dead node, no restarts) now executed + gated. |
| 22 | Evolvability | 8 | CHANGELOG discipline held across 6 merges; wire v12 stable; dead-end branches cleaned. |
| 23 | Documentation | 8 | Lint 10 same-day clean (3 findings fixed); cluster-suites page files the week's knowledge; front-door prefix list corrected. |
| 24 | Developer Experience | 8 | `make check` (~3 min) used throughout; red gates now name their dying line + dump node logs. |
| 25 | Dependency Hygiene | 8 | RUSTSEC audit green on every PR this week; `--no-default-features` clippy clean this run. No new deps for any of the six merges. |
| — | **Floor (lowest 3)** | **6, 6, 7** | API Design · Test Architecture · (Security, stalest of the 7s) |
| — | Mean (continuity footnote) | 7.52 | not a target; see M2 preamble |

**Delta vs Run 40:** floor unchanged in shape (6/6/7 vs 6/7/7) but the 6s moved — Run 40's S13 flake is *fixed at the substrate* (two root causes, both gated), and 18's cap is now a single undiagnosed flake (#161) rather than a structural gap; 5 drops 8→6 on a live finding the succession work exposed. The week validated M2's core bet: every hosted red decomposed into a real defect (five fixed since Run 40), and both of this run's passing probes are now permanent gates.

## 2026-07-10 — Run 42 (M2)

Deep-dive dimensions this run: **6 Error Handling, 7 Configurability, 8 Language Best Practices, 9 Concurrency Correctness, 10 Resource Management** (rotation). Execution evidence (all this run): probes `keyed_and_unkeyed_takes_never_interfere`, `probe_framed_but_corrupt_message_survives`, `shutdown_with_parked_take_waiter_is_prompt` — **3/3 pass**, all kept as permanent tests; clippy clean (main matrix + tuple-space `--all-targets`); #162's full CI green earlier today (incl. both Docker suites on the PR *and* the main-push combined run; lib 357/0; failover 7/7). Context: second run today — justified by #162's material diff (self-flood routing fix + discovery symmetry, both canary-verified). **Transparency flag:** dims 5/18 concern fixes authored this same session in direct response to Run 41 — the loop working as designed (findings → fixes → gates), but same-day re-scoring is noted, not hidden. Carried scores are hours old, not decayed.

### Findings
None new — all three probes passed. Standing finding from Run 41 remains live: **#161** (AFN flaked once on the hosted gate, cause unknown; a real contributor — the self-targeted flood — was removed in #162, but that is not claimed as the fix). It keeps dim 18 capped.

| # | Dimension | Score | Notes |
|---|-----------|:-----:|-------|
| 1 | Philosophy / Coherence with Goal | 8 | carried (v41, hours old) |
| 2 | Conceptual Integrity | 7 | carried (v41) — kv.rs housing + 18-param spawn warts stand |
| 3 | Architecture | 8 | carried (v41); the #162 carve-out documented as routing-not-admission with not-precedent framing (lint 11) |
| 4 | Modularity | 7 | carried (v41) |
| 5 | API Design | 8 | Run 41's live finding **fixed + canary-verified gate** (#162: all five racing ops now wait; `depth` deliberately fail-fast, documented); full CI green. Fixed end-state per current-state rule; same-session flag above |
| 6 | Error Handling Model | 8 | **Deep-dive.** Typed enums both layers; non-test unwrap/expect across hot files ≈4, each justified (guarded parse ×2, fail-fast recorder init, infallible local_addr); this week's real failures surfaced as typed errors (AddrInUse → typed Io; NoProvider contract now symmetric) |
| 7 | Configurability | 7 | **Deep-dive.** 47 validation/InvalidConfig sites incl. cross-knob tuning invariants; orphan grace deliberately hardcoded (documented). Env-var story spread across examples keeps it at 7 |
| 8 | Language Best Practices | 8 | **Deep-dive.** `unsafe` only in env-test helpers (`config.rs` tests); clippy `-D warnings` clean incl. `--all-targets` this run |
| 9 | Concurrency Correctness | 8 | **Deep-dive.** New tasks re-read (warm-keeper/backfill guards correct: `is_primary` exit, bounded per-tick work); P-C exercises waiter×shutdown interplay. Ledger-heaviest — stays 8 |
| 10 | Resource Management | 8 | **Deep-dive + probe.** P-C passed: shutdown prompt with a parked take waiter (no wedge), waiter resolves by its own timeout contract. Pins bounded by cluster size (deliberate no-disconnect, documented) |
| 11 | Semantic Correctness | 8 | P-A passed: keyed/unkeyed lane isolation holds (documented invariant, now gated) |
| 12 | Robustness | 8 | P-B passed: framed-but-corrupt codec input rejected cleanly (0 dead shards, serviceable) — one layer deeper than Run 40's framing probes |
| 13 | Security | 7 | carried (v38) — **stalest carry; deep-dive next run by rotation (dims 11–15)** |
| 14 | Failure Mode Legibility | 8 | carried (v41) |
| 15 | Performance | 7 | carried (v39), stale |
| 16 | Scalability | 7 | carried (v39), stale; scale nightly still queued on runner registration |
| 17 | Testability | 8 | carried (v41); three probes again constructed rapidly on public APIs |
| 18 | Test Architecture | 6 | **Standing Run-41 finding (#161) still live** — one undiagnosed flake on the no-retries gate. Contributor removed (#162), errtrace armed; cap lifts on diagnosis or a clean nightly stretch (nightly hasn't run yet) |
| 19 | Observability | 7 | carried (v40), stale |
| 20 | Debuggability | 8 | carried (v41) |
| 21 | Operational Readiness | 8 | carried (v41) |
| 22 | Evolvability | 8 | carried (v41); CHANGELOG discipline held through #162 |
| 23 | Documentation | 8 | lint 11 same-day (3 staleness fixes: CLAUDE.md carve-out clause, errtrace lesson, discovery-symmetry contract) |
| 24 | Developer Experience | 8 | carried (v41) |
| 25 | Dependency Hygiene | 8 | carried (v41); RUSTSEC green on #162 |
| — | **Floor (lowest 3)** | **6, 7, 7** | Test Architecture · Security (stalest) · Conceptual Integrity |
| — | Mean (continuity footnote) | 7.64 | not a target; see M2 preamble |

**Delta vs Run 41 (same day):** API Design 6→8 (finding fixed + gated, #162 green); everything else carried or confirmed by deep-dive/probe. 18 stays 6 honestly — #161 is not resolved by having removed one contributor. The three probes grew the suite by three permanent gates (lane isolation, codec-layer hostility, waiter×shutdown lifecycle).

## 2026-07-10 — Run 43 (M2)

Deep-dive dimensions this run: **11 Semantic Correctness, 12 Robustness, 13 Security, 14 Failure Mode Legibility, 15 Performance** (rotation — this is Security's first deep-dive since v38, the stalest carry). Execution evidence (all this run): probes `probe_gateway_auth_gates_the_wire`, `probe_config_rejection_names_the_field`, `probe_tombstone_survives_replay_of_older_write` — **3/3 pass**, kept as permanent tests; clippy `-D warnings` clean (lib+tests); the anchor-fragment doc checker (lint 13) run repo-wide. Context: three docs-focused workstreams since Run 42 — the #163 spawn-context/kv.rs refactor (merged green, both Docker suites), the README 1,604→192 restructure + examples audit, and the operations-docs pass — plus 3 wiki lints. **Transparency:** third run today, and the user directed it at the docs work; docs dimensions (23/24) are scored on that just-authored work — flagged, not hidden. This run's *audit weight* is the Security/correctness deep-dive, not the docs self-marking.

### Findings
None. All three probes passed. The gateway-auth probe initially "failed" on my own wrong test routes (`/gateway/kv/{key}` path vs the real `?key=` query form) — corrected; the wire auth itself gates correctly (401 bare / 401 wrong-token / 200 authorized, monitoring plane open, hostile bodies clean). Standing finding **#161** (AFN one-off) remains live and keeps dim 18 at 6 until the nightly gate produces a clean stretch or a named recurrence.

| # | Dimension | Score | Notes |
|---|-----------|:-----:|-------|
| 1 | Philosophy / Coherence with Goal | 8 | carried (v41) |
| 2 | Conceptual Integrity | 8 | **Re-checked (Run-42 warts fixed in #163):** the ListenerContext idiom now applies to all 4 spawn tasks; kv.rs fossil split into topology.rs/introspect.rs. The two named warts are gone — verified in code, not asserted. Up from 7 |
| 3 | Architecture | 8 | carried (v41); #163 moved no layer boundaries (pure impl-block relocation) |
| 4 | Modularity | 8 | **Re-checked:** #163 removed the 3rd hand-threaded shared-set anti-pattern (context structs) + the mis-housed method. Up from 7 |
| 5 | API Design | 8 | carried (v42, hours old); discovery-symmetry fix holding |
| 6 | Error Handling Model | 8 | carried (v42) |
| 7 | Configurability | 7 | carried (v42), stale |
| 8 | Language Best Practices | 8 | clippy clean this run incl. the new probes + #163 |
| 9 | Concurrency Correctness | 8 | carried (v42); #163's context structs are move-semantics only, no new shared state |
| 10 | Resource Management | 8 | carried (v42) |
| 11 | Semantic Correctness | 8 | **Deep-dive + probe.** P-tombstone passed: a delete survives replay of the original older-HLC write (LWW anti-resurrection holds against the adversarial reorder). LWW/HLC merge re-read; no divergence found |
| 12 | Robustness | 8 | **Deep-dive + probe (via auth probe's hostile-body arm).** 1 MiB garbage + malformed JSON to a gated JSON endpoint → clean client errors, gateway stays serviceable. Framing + codec layers already gated (Runs 40/42) |
| 13 | Security | 8 | **Deep-dive — first since v38 (stalest carry, now re-earned).** `probe_gateway_auth_gates_the_wire` is the missing end-to-end test: configured token → 401 bare / 401 wrong / 200 authorized on the *wire*, monitoring plane deliberately open. Auth middleware (`gateway_auth`, one `route_layer` over all `/gateway`), OIDC validation, mTLS admission all present + coherent. 8 not 9: no fresh adversarial pen-test of the OIDC/JWKS path or the scoped-token matrix this run |
| 14 | Failure Mode Legibility | 8 | **Deep-dive + probe.** `probe_config_rejection_names_the_field`: a rejected config names the offending knob (operator sees `reconnect_backoff_secs`, not a generic error). Consistent with the week's node-log-dump / errtrace / typed-error wins |
| 15 | Performance | 7 | **Deep-dive (read-only — no fresh bench this run, so capped at 8, held 7).** LWW merge + framing hot paths re-read; no new allocation/copy regressions from #163 (moves, not clones). Perf baselines now live in operations/tuning.md but weren't re-run → honest 7 |
| 16 | Scalability | 7 | carried (v39), stale; scale nightly still queued on runner registration |
| 17 | Testability | 8 | carried (v42); 3 more probes built rapidly on public APIs + one on the HTTP wire |
| 18 | Test Architecture | 6 | **#161 still live** — cap holds. The suite grew (probes → permanent gates; the anchor-checker is new doc-CI-worthy tooling) but the one undiagnosed flake on the no-retries gate stands until the nightly speaks |
| 19 | Observability | 8 | **Re-checked (ops pass):** the operator surface for the week's new signal is now covered — `individual_flood_fallbacks` + liveness fields documented on `/stats`, the topology-pressure warn has a runbook entry with the remedy. Up from 7 |
| 20 | Debuggability | 8 | carried (v41) |
| 21 | Operational Readiness | 8 | carried (v41) |
| 22 | Evolvability | 8 | carried (v41); CHANGELOG held through #163 + both docs passes |
| 23 | Documentation | 8 | **Re-checked (the docs work this run scored):** README 1,604→192 (one-home-per-fact, reference merged into owning pages, no loss); examples audit (orphans indexed, conway-gpu README, coop template conformance, honest counts); ops pass (config-table dedup, new-surface coverage); 3 lints incl. a new anchor-fragment checker that found a pre-existing broken anchor. Strong — but 8 not 9: the restructure is same-day and unverified by an outside reader; a "9" wants external confirmation the new front page actually onboards faster |
| 24 | Developer Experience | 8 | **Re-checked:** the front page is now a real handshake (192 lines, layers-at-a-glance → depth links); every example has a run command + what-it-demonstrates; the doc template is now actually followed by its own reference example (coop). Solid at 8 |
| 25 | Dependency Hygiene | 8 | carried (v42); no new deps across #163 or the docs passes; RUSTSEC green |
| — | **Floor (lowest 3)** | **6, 7, 7** | Test Architecture · Configurability (stalest carry now) · Performance/Scalability |
| — | Mean (continuity footnote) | 7.76 | not a target; see M2 preamble |

**Delta vs Run 42:** five dimensions re-earned an 8 on *verified* change rather than carry — Conceptual Integrity (7→8) and Modularity (7→8) because #163's fixes were checked in code; Observability (7→8) because the ops pass closed the new-surface gap; Documentation and DX confirmed at 8 on the restructure. Security (13) got its overdue deep-dive and a real new end-to-end gate — the stalest carry is now the *best-scrutinised* dimension, exactly M2's intent. Floor unchanged at 6 (Test Architecture / #161) — honestly held; the docs work doesn't touch it. Mean drift up (7.64→7.76) reflects the re-checks, not target-chasing; three new permanent probe-gates + the anchor-checker grew the verification surface.

## 2026-07-13 — Run 44 (M2)

Deep-dive dimensions this run: 5 API Design · 9 Concurrency Correctness · 11 Semantic Correctness · 15 Performance · 18 Test Architecture. Execution evidence: `cargo test -p mycelium-core --lib` **131 pass** (the suite the CI-gap fix now *runs*, not just clippy-compiles); `distributed_lock` (3) + `overlay_consistent` (6) regression pass; `cargo test -p mycelium-wiki --features control-plane` **28 pass** incl. the new `concurrent_creates_of_the_same_new_section_elect_exactly_one` probe run **20× under load, 0 fail**; the earlier-this-session `concurrent_idempotent_appends…` stress gate (25× 0-fail) + `a_stale_write_into_a_gc_gap…` head-check gate; `cargo bench --bench gateway_overhead` (loopback ~40 µs); clippy clean across the feature matrix.

### Findings
None — all Run-44 falsification probes passed (the concurrent-create race, the lock mutual-exclusion regression, the mycelium-core suite). **Note:** the `mycelium-wiki` dual-curator lost-update defect was found + fixed + gated *earlier this session* (section-granular CAS); it existed while Concurrency/Semantic scored 8 in Runs 40–43, so it is recorded in the **Calibration Ledger**, not as a live Run-44 finding — the current state is *fixed with a deterministic stress gate*, which is the fixed-end-state M2 scores.

| # | Dimension | Score | Notes |
|---|-----------|:-----:|-------|
| 1 | Philosophy / Coherence with Goal | 8 | carried (v41); the new `coordination-approaches.md` + CAP deck slide reinforce the coordinator-free/CFT thesis (AP default, opt-in CP) — nothing contradicts philosophy. Stale otherwise |
| 2 | Conceptual Integrity | 8 | System→Cluster scope rename (#167, wire-compatible) unified the scope vocabulary across `SignalScope`/consensus/gateway; companions all share the ring-election idiom. Verified in commits |
| 3 | Architecture | 8 | carried (v43), stale; the lock service is Layer III, the wiki CAS is companion-level — no layer boundary crossed, namespace ownership intact |
| 4 | Modularity | 8 | carried (v43); re-confirmed the three companions depend on `mycelium` **public API only** (`default-features=false`, no consensus) while adding the CAS surface |
| 5 | API Design | 8 | **Deep-dive.** New `WikiStore` CAS surface (`read_versioned`/`write_section`/`update_manifest`, typed `WikiError::Conflict`) + `LockService` (`try_lock`/`lock`/`with_lock` scoped section, fencing token public). Hard to misuse; the "Conflict → re-read" contract is explicit. 8, no fresh whole-surface misuse-fuzz |
| 6 | Error Handling Model | 8 | carried (v42); `WikiError::Conflict` is a typed, actionable "retry" signal, not a stringly error |
| 7 | Configurability | 7 | carried (v42), **stale — not re-examined**; floor |
| 8 | Language Best Practices | 8 | clippy clean across the feature matrix incl. the new bench + probes; the CAS is idiomatic (`hard_link`, no `unsafe`, no lib `unwrap`) |
| 9 | Concurrency Correctness | 8 | **Deep-dive.** Found+fixed+gated the wiki dual-curator lost-update (ledger); section-CAS stress 25× + concurrent-create 20× (0 fail), lock #164 mutual-exclusion/release regression passes, core suite 131 pass. Best-scrutinised dimension this run — but **held at 8, not 9**: the ledger has 3 prior concurrency entries *and this run found a fresh one*, i.e. the surface keeps harbouring defects; and connection/tasks/capability_ops were not freshly probed |
| 10 | Resource Management | 8 | carried (v42); `hard_link` temp is unlinked after publish (no leak), lock guard is RAII (`Drop` releases), curator task-cycle `shutdown` gate holds |
| 11 | Semantic Correctness | 8 | **Deep-dive.** CAS **never-lose** proven (30-run min = target, never under); at-least-once + idempotent reconcile = exactly-once *effect* (matches `exactly-once-effect.md`); lock mutual-exclusion holds. **8 not 9**: ledger has 4 entries (LWW/HLC/promotion/backfill); LWW/HLC/anti-entropy not freshly re-probed this run |
| 12 | Robustness | 7 | carried (v43) but **not re-probed this run**; new attack surface (the gateway bench's live server, the CAS store) went un-fuzzed, and the ledger has 2 entries → treated as a decaying claim, marked down to 7. Floor |
| 13 | Security | 8 | carried (v43, deep-dived last run — mTLS/OIDC/`gateway_auth`), **stale**; not re-probed this run |
| 14 | Failure Mode Legibility | 8 | carried (v43); the new `mycelium_consensus_*` metric family + consensus/lock runbooks *add* operator legibility but were **not probed to fire** — held at 8 on last run's earned probe, not raised on unproven artifacts |
| 15 | Performance | 8 | **Deep-dive + fresh bench.** `benches/gateway_overhead.rs` measures the HTTP-gateway overhead at **~40 µs loopback** (was an unsourced "~1 ms" claim → 25× pessimistic; now a reproducible gate). Up from 7. **8 not 9**: the KV/framing/fanout hot-path throughput benches were not re-run this session |
| 16 | Scalability | 7 | carried (v39), **stalest carry — 5 runs, no scale test run this session**; floor |
| 17 | Testability | 8 | the new probes build on the public `WikiStore` API + tempdirs (no cluster); the gateway bench boots a single node — injectable, deterministic where it counts |
| 18 | Test Architecture | 7 | **Deep-dive.** Up from 6: the cap reason (**#161 undiagnosed flake**) is resolved — root-caused + fixed via #162, documented in `cluster-suites.md` — and the **mycelium-core CI coverage gap is closed** (its 131-test suite was clippy-compiled but never *run*; now `ci-retest.sh -p mycelium-core`, verified green). New deterministic gates added (wiki stress/create, mixed-version wire-compat). **Not 8**: the socket-binding *retry-tier* (`ci-retest.sh`) and the still-**un-built live two-binary mixed-version test** are structural residuals |
| 19 | Observability | 8 | carried (v43); the `mycelium_consensus_timeouts_total{reason}` counter + mirror gauges are a real new operator surface (cardinality-safe, no per-lock gauge) |
| 20 | Debuggability | 8 | carried (v41), **stale — 3 runs unverified**; possibly optimistic per the decay rule |
| 21 | Operational Readiness | 8 | carried (v41); new consensus/lock diagnostics runbooks + the rolling-upgrade procedure touch it, but the core `/ready`/shutdown/back-pressure mechanisms were not re-probed |
| 22 | Evolvability | 8 | the **mixed-version wire-compat gate** (v11↔v12 `decode_wire_v11` + a CI job asserting cross-version decode) is a concrete backwards-compat win; `WIRE_VERSION=12`/`PREV=11` policy honoured; CHANGELOG maintained |
| 23 | Documentation | 8 | heavy verified churn this session (coordination-approaches decision note, CAP slide, doc-coverage run 3, 2 wiki-lint + 2 publication-lint passes, the bench-sourced perf number) — all lint-clean. 8 not 9: same-day, no external-reader validation |
| 24 | Developer Experience | 8 | carried; `make check`/`CLAUDE.md` on-ramp intact; clippy clean; the new bench is one `cargo bench` command |
| 25 | Dependency Hygiene | 8 | carried (v42); the gateway bench reuses existing `reqwest`/`criterion` (async_tokio) — **no new deps**; `--no-default-features` compiles per CI |
| — | **Floor (lowest 3)** | **7, 7, 7** | Configurability · Scalability · Robustness (Test Architecture also 7, lifted from 6) |
| — | Mean (continuity footnote) | 7.84 | not a target; see M2 preamble |

**Delta vs Run 43:** the floor lifts 6→7 (**Test Architecture 6→7**) because both things capping it are resolved — #161 root-caused+fixed (#162) and the mycelium-core suite now *runs* in CI (a real M2-Run-20-class coverage gap closed), verified green this run — held at 7 not 8 by the socket-binding retry-tier + the un-built live mixed-version test. **Performance 7→8** on the first fresh gateway bench (the "~1 ms" claim was 25× pessimistic; now a sourced gate). **Concurrency (9) and Semantic (11)** held at 8 on deep-dive + probes despite a *found+fixed+gated* defect — the fix makes the current state better (per the current-state principle), and the ledger entry, not a score drop, is where the past over-scoring is accounted; kept off 9 by their combined 7-entry ledger history. **Robustness 8→7** honestly decayed (new surface un-fuzzed this run). Mean 7.76→7.84 reflects the two earned rises, not target-chasing; two new permanent probe-gates (concurrent-create, and the earlier stress/head-check gates) + the sourced perf bench grew the verification surface. No live findings this run.

## 2026-07-14 — Run 45 (M2) — skipped, no material product diff since Run 44

Cadence gate: the diff on `src/` · `mycelium-*/src/` · `tests/` · `benches/` · `Cargo.toml` since Run 44 (`28a5f18`) is **empty**. The only commits are ops tooling (`ed1a138` the local nightly scale runner — `scripts/scale-nightly-local.sh` + launchd plist) and edits to the `mycelium-analysis` skill itself (`fb6715c` — dim-16 now reads the nightly results + classifies infra-vs-assertion FAILs). Neither is product code/tests/product-docs, so a full re-score would carry ~24/25 from the day-old Run 44 — exactly the near-duplicate the gate exists to prevent. **Not scored.** The one dimension with new input, **Scalability (16)**, deliberately stays a carried 7 rather than being lifted here: the first nightly produced `scale` PASS but `resilience`/`entries` were **infra-FAILs** (exit 2 — Colima's daemon went unreachable after the 100-node round, VM fatigue; *not* a substrate failure), so the evidence is partial. The runner was fixed (restart Colima between suites, `b2d8177`); the lift to an evidenced 8 waits for a **clean** nightly (all three green) — recorded here so the next run knows the evidence pathway is live but not yet clean.
