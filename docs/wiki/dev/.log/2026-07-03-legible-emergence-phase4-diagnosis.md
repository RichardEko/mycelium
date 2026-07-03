## [2026-07-03] ingest | legible-emergence Phase 4 — fleet diagnosis (the "why" narrative)

The differentiator payoff: `diagnose_fleet(&FleetSnapshot) -> FleetDiagnosis` (src/agent/emergent.rs)
— a **pure, templated rule engine** that turns the Phase-2 relational snapshot into a code-free,
actionable *diagnosis*. The three-verb spine is now complete: **localize** (`/gateway/fleet`) ·
**explain** (`/gateway/explain`) · **diagnose** (`/gateway/diagnose`, scope `fleet:read`).

One rule per Phase-0 pathology, each firing only on its condition:
- `governed_group_thrash` (Critical) / `governed_group_conflict` (Warning) — a conflicting group,
  escalated to the #56 governor-vs-auto-join thrash when membership is also flapping; names group,
  band, observed count, and the fix.
- `opacity_storm` (≥34% opaque, Critical) / `opacity_present` (Warning) — the **throttle graph
  supplies the *because*** ("rate-limited edges n3→n7 @ 5 fps — the likely reason"). This is the
  plan's canonical example ("work pools on node-7 because nodes 3,4,5 are opaque, reason rate-limit").
- `capability_coverage_gap`, `opacity_oscillation`, `commit_conflict` — each actionable; the
  coverage gap keeps the RT3 "not visible from here" honesty.

Findings sort most-severe-first; healthy fleet → no findings + "nominal" summary. **RT1/RT2:** a
`caveat` is attached when the observer's own view is partial (`peers_heard < peers_known`) or
self-degraded — a clean diagnosis from a blind node must not read as "the fleet is healthy". New
types: `Severity` (Info<Warning<Critical, Ord), `Finding{pathology,severity,cause}`,
`FleetDiagnosis{observer,summary,findings,caveat}`. `compute_fleet_diagnosis(ctx)` =
diagnose_fleet(snapshot); the HTTP handler `gw_diagnose` returns it.

Gates: five unit rules (each pathology → actionable cause; ordering; healthy=nominal;
partial-view caveat) + `test_fleet_diagnosis_names_a_real_governed_group_conflict` (grounds the
engine against a *real* KV-derived snapshot, not a synthetic struct — the snapshot's `conflict` flag
is a pure KV scan, so no detector loop/hysteresis needed to surface it) + the `/gateway/diagnose`
scope assertion (`fleet:read`, deny-by-default). Pages: dev/diagnostics.md (Phase 4 section + title),
plan legible-emergence.md (status → Phases 0–4 done, Phase 4 → DONE). **Phases 0–4 complete.**
Remaining: Phase 5 (operator surface — runbook, alerts, docs).
