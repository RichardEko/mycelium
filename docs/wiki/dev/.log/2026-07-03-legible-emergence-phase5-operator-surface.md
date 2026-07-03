## [2026-07-03] ingest | legible-emergence Phase 5 — operator surface (closes Legible Emergence)

The packaging phase: surface the diagnostics as **data** (the library-not-platform line) + the
two-audience docs + the induce-and-diagnose CI demo.

**Diagnostics as data (public API).** New `GossipAgent::fleet_snapshot()` and `fleet_diagnosis()`
(src/agent/kv.rs) — the same content as `GET /gateway/fleet` / `/gateway/diagnose`, callable
programmatically with no HTTP/auth. Re-exported the types from lib.rs: `FleetSnapshot`,
`FleetDiagnosis`, `Finding`, `Severity`, `StoreConvergence`, `ThrottleEdge`, `ViewConfidence`. NB:
emergent's `GroupStatus` is NOT re-exported at the top level — the bare name is already the
mesh-dashboard type; reach it via `FleetSnapshot.governed_groups`. (Bonus: the public methods make
`compute_fleet_snapshot`/`_diagnosis` live in `--no-default-features` builds, no longer dead there.)

**The CI gate — induce-and-diagnose demo.** `examples/coop/src/bin/diagnostics.rs` (Food-Rescue
step 12): a two-depot mesh; depot-a caps `rush-pool` at [1,2] but 4 depots register in it (a benign
intent-vs-reality mismatch, constructive framing — not a crisis); **depot-b** diagnoses it from its
own gossiped KV (proving coordinator-free — the node that names the conflict never saw it seeded).
Added to `examples/coop/ci_smoke.sh` as demo 12 (markers "All assertions passed" + "diagnosed the
governed-group conflict"), so the coop suite in CI now gates it Docker-free with the 3× retry the
other multi-node demos use.

**Two-audience docs.** Operator: `docs/operations/diagnostics.md` — the three verbs
(localize/explain/diagnose + endpoints + API), "read the caveat first" (RT1/RT2), one runbook entry
per pathology (means / read via gauge+snapshot+explain / do), and **Prometheus alert recipes** on the
`mycelium_emergent_*` gauges (incl. the `peers_heard < peers_known*0.5` partial-view alert). Added to
docs/operations/README.md. Developer: guide/14-patterns-and-pitfalls.md pattern 11 ("diagnose from
any node — diagnostics as data"; anti-pattern = a central collector).

**Legible Emergence is COMPLETE (Phases 0–5).** The three-verb operator spine — localize
(`/fleet`) · explain (`/explain`) · diagnose (`/diagnose`) — is shipped, tested, documented for both
audiences, and demonstrated end-to-end in CI. Remaining ideas (Phase-4 store-divergence rule, a
`/mgmt` dashboard diagnostics view) are optional polish, not gaps.
