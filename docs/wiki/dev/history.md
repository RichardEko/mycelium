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

## Post-v2.0: downstream on-ramp + hardening (2026-07-04/05)

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

`docs/analysis/ratings.md` — 36 runs; methodology M2 since Run 16 (execution-evidence gate,
falsification probes, calibration ledger). Run 28 (2026-07-02): 5 findings (3 Major), all
fixed same day — the oversized-write family, the state-machine commit race, RUSTSEC-2026-0188.
Run 34 (2026-07-05): the `mycelium-wiki` curator split-brain (Major, single-writer) — found via
a CI flake, fixed same session (#127); floor dipped to 6/7/7 then recovered to **8/8/8 by Run 36**
as the fix (#127) and the coop-flake fix (#128) confirmed green. 25 calibration-ledger entries.
