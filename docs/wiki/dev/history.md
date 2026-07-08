# dev/history — the delivery ledger

↑ [dev/](dev.md) · full execution records: `docs/plans/README.md` (the canonical index)

Reconciled current state of *what shipped when* — so no session re-derives it from git.
As of 2026-06-21 all v1.x/v2.0 engineering plans were shipped. Since then, **Legible Emergence
(diagnosability) is COMPLETE — all phases 0–5 shipped** (2026-07-02/03; see
[diagnostics.md](diagnostics.md) and `docs/plans/legible-emergence.md`):

- **Phase 0** — the pathology taxonomy design record (RT1–RT4 red-team baked in).
- **Phase 1** — the five coordinator-free emergent detectors + `/stats`/`/metrics`.
- **Phase 2** — `GET /gateway/fleet`, the relational fleet snapshot (throttle graph, cross-node
  store-convergence, commit-conflict hot slots).
- **Phase 3** — the HLC-stamped `EventRing` + `GET /gateway/explain`, cross-node causal
  reconstruction (best-effort fan-out naming non-responders; the #56 narrative).
- **Phase 4** — `GET /gateway/diagnose`, the `diagnose_fleet` rule engine (the "why is the fleet
  in this state" narrative, one rule per pathology).
- **Phase 5** — the operator surface: public `fleet_snapshot()`/`fleet_diagnosis()` API,
  `docs/operations/diagnostics.md` runbook + Prometheus alert recipes, guide pattern 11, and the
  coop `diagnostics` demo (induce-and-diagnose, Docker-free in CI).

The three-verb operator spine — **localize** (`/fleet`) · **explain** (`/explain`) · **diagnose**
(`/diagnose`) — is shipped, tested, and documented for both audiences.

## Post-v2.0: downstream on-ramp + hardening (2026-07-04/06)

- **`mycelium-wiki` curator step-down** (#127): the companion (group-scoped LLM-curated wiki,
  control-plane/data-plane — shipped 2026-07-03) gained a split-brain guard. The election settles on a
  fixed window, so a lost gossip race could leave two nodes self-elected — both writing the shared store
  with no recovery. A curator **sentinel** now applies lowest-id-wins *continuously* (a higher-id curator
  resigns → returns to the reader failover-watch), with the deterministic canary
  `dual_curators_reconcile_to_a_single_writer`. Root-caused as a single-writer defect (analysis Run 34,
  Major); red-before/green-after on the CI `Wiki (data plane)` job.
- **Downstream-integrator on-ramp** (#125, #126 + direct docs): a two-audience front door —
  `docs/guide/faq.md` (human orientation: is-this-for-me / which-primitive / why-not-X) and
  `docs/guide/building-on-mycelium.md` (the integrator contract: public-API-only rule, reserved KV
  prefixes, the invariants, a copyable `CLAUDE.md` snippet) — linked from the README (two-audience split)
  and the crate-root doc (surfaces on docs.rs). Plus the tuple-space **`redistribution`** worked example
  (equal footing with blackboard `microgrid` / wiki `wiki_chat`), the README four-paper corpus DOIs, and
  `/wiki-lint` **extended** to guard the front-door docs that *restate* code facts against doc-vs-code
  drift (caught a `schema()`→`schemas()` slip on its first pass).
- **Coop suite hardening** (#128): the `elastic_intent` demo's CI-load flake fixed structurally — a
  bidirectional-signed-propagation readiness gate (keeps the TLS identity-exchange window out of the
  convergence poll) + a self-heal window sized past the ~12 s governor cooldown. Verified 14/14 local +
  CI green (the previously-flaking `Food-Rescue Co-op suite` job).
- **Opacity control-signal-shed fix** (#129, 2026-07-06): a *real liveness bug* hiding behind a 10-run
  "flaky" test. The opacity governor emits `BOUNDARY_OPAQUE`/`TRANSPARENT` at `System` scope, and
  `ops::deliver_locally` probabilistically sheds non-`Individual` signals by `combined_fill`; under CI
  gossip-drain starvation the governor's single boundary-transition emission could be shed from *local*
  delivery — the "I'm now shedding" signal dropped by the shedding mechanism, precisely under load.
  Fixed by exempting boundary-transition kinds from the local shed (like `Individual`); deterministic
  regression `ops::delivery_shed_tests::boundary_transition_signals_are_never_locally_shed` (verified to
  fail without the fix). Root-caused by a deliberate dig (analysis Run 37, Major) after three prior
  "resolutions" mis-treated it as scheduling latency.
- **v3.0 positioning** (2026-07-05/06): a pattern-landscape scan established the
  substrate covers the *coordination* pattern space **natively or by composition of native primitives**
  (only ANP wire-protocol conformance needs new code; orchestrator is a non-goal). Recorded **two
  primary v3.0 deliverables** — `mycelium-reason` (LLM-authoring DX) and `mycelium-guardrails`
  (structural, coordinator-free guardrails) — plus packaging candidates. RAG / HITL / *content*
  guardrails are framed as **use-case functions** (external services accessed *through* the mesh — the
  wiki precedent), not substrate work. Homes: `ROADMAP.md` → v3.0 · `docs/wiki/domain/pattern-coverage.md`
  · `docs/plans/mycelium-{reason,guardrails}.md`. **Both primaries shipped 2026-07-08 (#130–#139) — see
  the two entries below;** this bullet records the positioning that preceded them (was "PROPOSED, not
  started" when written).

- **Artifact library — steps 1–5 shipped** (2026-07-07, commits `910c1ff`…`22ac02b`; design record
  `docs/design/artifact-library.md`): the durable origin tier + install generalization for
  `mycelium-wasm-host`. **Data:** `FsLibrarySource` (content-addressed blob dir, complete-or-absent
  writes) + the signed **manifest** (the library's own catalogue; publisher keys stay in CI) + a
  clean-slate versioned entry encoding with an explicit `ArtifactKind`, provenance now binding the
  *whole entry* (version‖kind‖artifact‖capability — closes a re-labeling hole). **Roles:** the
  **librarian** (`spawn_librarian` — serve + one `artifact/librarian` cap + stateless manifest→KV
  reconcile, signature-scoped) and `MeshArtifactSource::resolving` (holders discovered via the
  capability ring — no hardcoded provider ids). **Install:** `ArtifactRuntime`/`Installed` traits —
  `WasmHost` is now the engine inside *one* runtime; `BlobRuntime` places models/data
  (ranged/streamed pull via `RangedArtifactSource`, temp+rename, activation hook, pluggable probe);
  the `Provisioner` gained a kind registry, eligibility (kind + size budget + **resource
  headroom** — signed per-entry `requires`, `ResourceProbe`, in-flight reservations counted;
  §4.4, step 4b) with a tripwire counter, async `Installing→Live` reservations (token-checked),
  and **real** `{ns}/loading` pct tiers driven by actual bytes. **Honest demos:** `catalog` (runtime-read library → librarian →
  discovered pull → origin killed + library deleted → late joiner installs from a peer cache) and
  `mcp_toolgrowth` (the converter's arithmetic **arrives** as a new committed WASM fixture,
  bridged over MCP; activation-vs-installation taught explicitly); `llm_agent`'s percent loops
  stay simulated by decision (wasmtime must not enter `make check` via root dev-deps) and say so.
  Lock-order rows 20–22. **Complete** — step 6 shipped (`BlobFetcher`/`PrefetchingSource`/`HttpLibrarySource`: any HTTP(S) blob store, egress-gated, vendor SDKs via the trait); step 7 declined-with-evidence (three async faces already serve every consumer — note §10). **Session tail (same day):** the coverage review found `Installed::probe` was exposed but consumed by nothing — a **probe health pass** now opens every `provision_round` (fail → withdraw → the normal machinery reinstalls once the retracted ad clears the local view; probes are cheap-under-lock by contract); four lifecycle/concurrency tests landed (full per-kind lifecycles incl. blob probe-self-heal + shed-deletes-the-file; failed-install reservation-drop-retry; withdraw-during-install stale teardown), and the **`model_deploy` manual demo** proves the Blob path with a real 19 MB GGUF — **weights + deployment profile as two signed artifacts** (profile → weights by content address; failed-activation-retry is the ordering — note §4.3.1), streamed with honest percent, resolved + activated via `ollama create` (with `ollama show` asserting the arrived SYSTEM prompt is the one running), probe-gated, then generating real tokens (`ArtifactKind` note: a closed crate-owned enum — custom *runtimes* are the open axis, not custom kinds). Open: the crate-naming question only. **Run-38 floor fixed same day** (typed `InstallError` by stage; `mycelium_artifact_*` metrics-facade tripwires + recorder-backed test; the CI **flake tier** — `scripts/ci-retest.sh`, failed-tests-only retry with mandatory flake annotations, the class-level prevention Run 37 asked for).

- **`mycelium-reason` — v3.0 primary #1, LLM-authoring DX, COMPLETE**
  (2026-07-08, PRs #130–#136; plan `docs/plans/mycelium-reason.md` + `…-examples.md`, positioning
  `docs/wiki/domain/pattern-coverage.md` → the LLM-DX axis, guide **chapter 15**). The first *built* v3.0
  deliverable. Preceded by a **code-verified pre-implementation reassessment**
  (five bindings; corrected the 2026-07-07 addenda's overstatement that an attributed
  `cap/{node}/llm/inference` convention existed — it did not; and that resolution consults opacity — it
  does not). **PR #130 — the `mycelium-reason` crate** (public-API-only companion, no `mycelium-wasm-host`
  dep): ① **capability-routed inference** (`serve_model` = model-is-a-prompt-skill `llm/{model}` + a
  parallel attributed `llm-meta/{model}` ad; `InferenceRouter` = resolve → drop opaque nodes → rank by
  pheromone `peer_load` fill → failover — the routing layer the load-blind `resolve` deliberately
  omits), ② **fleet-reasoning traces** (`TraceRecorder`/`replay`/`narrate` on the log overlay, optional
  WS2 audit-chain anchoring under `compliance`), ③ **artifact-aware resume** (demand half:
  `require_model` + structural `await_ready` + `llm/loading` progress), plus the **content-addressed
  blob tier** (`FsBlobStore`/`MeshBlobStore`/`spawn_blob_server` — SHA-256 ids, verify-on-read, verified
  peer fetch, ≤ 8 MiB single-frame v1) and `/gateway/reason/{blob,trace}` routes. Implementation caught a
  real plan error — a single shared trace stream collides same-millisecond HLC keys across writers (the
  HLC's per-node logical counter) and LWW-drops records — fixed with **per-writer substreams**
  `reason/{run_id}/{node}`, merged on HLC at replay. Zero new locks. **PR #131 — the Python tier**
  (Tiers 1+2): **`langgraph-checkpoint-mycelium`** (a `BaseCheckpointSaver` — index rows in gossiped KV
  `ckpt/`/`ckptw/` with metadata inline for payload-free `list`, payloads in the blob tier with one blob
  per channel value so unchanged values dedup across super-steps; sync + async; **cross-node `StateGraph`
  resume proven in CI** — node B continues what node A checkpointed) and **`mycelium.call_typed`** (a
  through-the-mesh prompt-skill call with a balanced-brace JSON scanner + pydantic validation-feedback
  retry; pydantic via the `typed` extra). Landed the repo's **first Python CI job** (`python-sdk`: builds
  the `reason_node` example, boots a two-node mesh, runs both pytest suites — 14 tests). A checkpointer
  edge exposed and fixed the crate's empty-blob path (a typed `None` serializes to zero bytes = `SHA-256("")`;
  an empty fetch reply means *miss*, so `MeshBlobStore::get` answers it from the address alone). Reserved
  prefixes claimed: KV `ckpt/`·`ckptw/`·`log/reason/`, capability `reason/blob-cache`, RPC
  `reason.blob.fetch`. **PRs #132–#136 completed the LangGraph example ladder** (`docs/plans/mycelium-reason-examples.md`,
  built flagship-first): **#132** the routing gateway surface (`POST /gateway/reason/route` + Python
  `ReasonClient`) — needed because `/gateway/llm/call` is single-shot; **#133** the echo-CI **deploy/reheal
  flagship** (a graph's model dependency follows it across node death: checkpoint on A → gossip to B →
  kill A → B reheals the model via the mesh blob fetch + `serve_model` bridge → resume routes to B);
  **#134** a real router-robustness fix the flagship's de-risking surfaced — a killed node poisoned
  routing for ~90 s (capability-freshness window; mesh RPC has no fast-fail), fixed with a **live-SWIM-membership
  filter** (`InferenceRouter` routes only to `peers()`+self) + a **`RouterConfig::failover_timeout`** (8 s;
  non-final attempts fail over fast, the last gets the full budget); canary `liveness_filter_drops_a_non_peer_cap`;
  **#135** rungs 0/1/2/3/5 (`examples/langgraph/`) + the ladder README + a small trace-recording surface
  (`run_id` on the route endpoint); **#136** guide chapter 15 + the **Ollama-manual** real-model variant
  (`examples/coop/src/bin/reheal_deploy.rs` — real GGUF via `model_deploy`'s `BlobRuntime`, `supervise(min=1)`-driven
  reheal, node-unique Ollama names; manual/not-CI, compile-verified only). All CI-green. Open: the
  `mycelium-reason` crate-naming question (shared with the artifact library); the Ollama variant is
  compile-verified but unrun (needs a live Ollama + GGUF).

- **`mycelium-guardrails` — v3.0 primary #2, structural coordinator-free guardrails, COMPLETE**
  (2026-07-08, PRs #137–#139; plan `docs/plans/mycelium-guardrails.md`, positioning
  `docs/wiki/domain/pattern-coverage.md` → Structural guardrails, guide **chapter 16**). *What an agent
  may do* — packaged on the public API only. Preceded by a **code-verified reassessment** (six bindings)
  whose headline reshaped the plan: the mechanisms are real but deliver **three distinct strength tiers**,
  so an honest policy must say which clause compiles to which. **PR #137 — the crate**: a tier-labelled
  `Policy` → `apply()` compiling one declaration to **Tier A** boundary (`join_group` — drop-before-handler,
  self-imposed prevention), **Tier B** `AgentPolicy` (tool allow/deny + budgets, self-imposed at state
  transitions), **Tier C** `authorized_callers` (**hard prevention** — an unauthorized invoke is rejected
  at the provider, the one gate that's real prevention not promise-strength); `Policy::strength_report()`
  is the legibility (it discloses each clause's tier); the **self-imposed stance** is a decision (no remote
  policy authority — a central policy server is the chokepoint non-goal). It ships the reusable Tier-C gate
  + **denial sealing** (`check_caller`/`guarded_rpc_serve` seal `Invoke`/`Denied` into the tamper-evident
  chain) that previously only SkillRunner had. **PR #138 — the policy-audit verification tool**
  (`prove_denials`/`narrate_proof`): reconstruct a provider's chain, re-verify it, and prove the guardrail
  fired — with **honest framing** encoded in the output (it PROVES *this provider tamper-evidently sealed
  stopping X*; it DOES NOT prove *X could not have done Y anywhere* — per-node chains, only guarded caps
  seal) + the watchable `guardrail_wedge` example. **PR #139 — chapter 16 + `guardrail_fleet`** (all three
  tiers *actually firing* in a constructive co-op fleet; the Tier-A boundary *drop* — a non-event — proven
  by a positive/bounded-negative/bracket sequence). Revocation is **self-sovereign** (`revoke_identity_key`
  — a node revokes only its own keys; the levers over a misbehaving peer are narrowing its allowlist or
  dropping its role, never pushing policy in). All CI-green; a `Guardrails (v3.0)` CI job. Zero new locks.
  Open: broader packaging refinements + the crate-naming question.

## v2.0 (2026-06-21) — all 16 milestones M1–M16, acceptance gate met, no deferrals

| Workstream | Delivered | PRs |
|---|---|---|
| WS-A crate/API | M1 `mycelium-core` split · M2 `consensus` gate · M3 handle pushdown | #8 |
| WS-B scale/transport | M4 partial mesh · M5 SWIM (default **on**) · M11 codec (bincode retired, RUSTSEC-2025-0141) + Merkle anti-entropy, wire **v12**/PREV 11 | #19, #21, #22 |
| WS-C metabolism | M8 auto-derivation · M9 hot-reload/ClusterTuner + governor · elastic MembershipGovernor · M7 distributed rate-limit · M10 fence-free live timing | #26–#27, #105–#107 |
| WS-D security | M6 capability authz + CT revocation log | #77–#82 |
| WS-E code mobility | M12/M15/M14 — `mycelium-wasm-host` autonomic provisioning | #32–#42 |
| WS-F federation | M16 AgentFacts + schema migrations — `mycelium-agentfacts` | #44–#49, #83–#88 |
| WS-G coordination | M13 keyed take · `mycelium-blackboard` | #89–#100 |

Declined-with-evidence (kept as decisions, not debt): WS-G exactly-once overlay
(`docs/design/exactly-once-effect.md`), M10 consensus fence, WS-E epoch limits +
strict-consensus singleton, OR-Map for gcap (`docs/design/or-map-gcap-evaluation.md`).

## v1.x production readiness (complete)

WS1 RBAC/identity · WS2 tamper-evident audit · WS3 crown-jewel (feature-free) · WS4 OIDC
SSO · WS5 hot cert rotation — see [security](security.md); plan
`docs/plans/v1x-completion.md`. Support/SLA is commercial-track
([strategy](../domain/strategy/strategy.md)).

## Earlier landmarks

Sub-handle facade + gateway feature gate (pre-release remediation) · fuzz harness ·
locality/topology Phases 0–7 · cross-group consensus (Phase 8) · watcher C2 · signal
reorder buffer (wire v11 `hlc_seq`) · semantic coordination + schema registry · TupleSpace
companion (2026-06-11) · CI/test hygiene 2026-06-19 (shared `alloc_port`, PR #50; wgpu
dev-dep removed, PR #40; ephemeral-floor fix, PR #110).

## The self-audit series

`docs/analysis/ratings.md` — 37 runs; methodology M2 since Run 16 (execution-evidence gate,
falsification probes, calibration ledger). Run 28 (2026-07-02): 5 findings (3 Major), all
fixed same day — the oversized-write family, the state-machine commit race, RUSTSEC-2026-0188.
Run 34 (2026-07-05): the `mycelium-wiki` curator split-brain (Major, single-writer, #127). Run 37
(2026-07-06): the opacity control-signal-shed (Major, #129). 27 calibration-ledger entries.
**Methodology upgraded 2026-07-06 (bright line at Run 37):** *current score = current state* — a bug
found + fixed + deterministically gated in the same run scores its fixed end-state (not the old cap-at-6),
and finding-and-fixing a bug never lowers a score (accountability for past over-scoring lives in the
ledger); an *unknown-unknowns reserve* + *carried-score decay* temper confident 8s; and **past run scores
are never retroactively rewritten** (a time-series is only meaningful if its measurements stand). Pre-37
runs are dated snapshots under the prior rule.
