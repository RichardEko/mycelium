# 2026-07-10 — front-page restructure + examples audit (third-party DX pass)

User-initiated audit: "is the doc set fit for third-party purpose; the front page is
incredibly long." Findings + actions:

- **README 1,604 → 192 lines.** The old page was a deliberate "single-file reference"
  doubling the guide. Now a true front page; the ~1,100 reference lines moved to owning pages
  as "Reference —" sections: guide 00 (skills-vs-MCP), 01 (Layer I API/observability),
  02 (capability subsystem), 03 (signal mesh in depth incl. opacity-vs-inhibition), 04
  (Layer III + consistency overlay), 05 (SkillRunner + prompt skills), 13 (Docker cluster
  template), cookbook (service layer), operations/tuning (perf baselines + GossipConfig
  reference). All intra-block links re-relativized; full-repo link sweep clean.
- **Examples audit:** five orphans indexed; `conway-gpu/` README created (was none);
  `coop/` brought onto the template (Objective/How-to-run) + the undocumented
  `reheal_deploy` (M+) got its block; counts corrected (14 = 12 CI + 2 manual — README said
  "eleven", index/CI said "12"); guide's duplicate portfolio table now defers to
  `examples/README.md` as the single index.
- **Pre-existing dead link found:** guide/12 → `README.md#durability-contract` (section
  didn't exist anywhere); repointed to ch. 01's reference + `persistence.rs` docs.
- Convention note for future lints: **"Reference —" sections at page bottoms are moved
  README content** (provenance lines mark them); the README is deliberately lean — resist
  re-growing it, add to the owning page instead.

Pages touched: README.md, guide 00/01/02/03/04/05/12/13 + cookbook + guide/README,
operations/tuning.md, docs/README.md, examples/{README, coop, conway-gpu, langgraph},
wiki dev/examples.md.

**Part 2 — operations docs pass (same lens, same day):** the ops set was already strong (a
real persona-routed funnel, 84–250-line runbooks, consistent numbered checklists where it
matters). Three findings, fixed: (1) the README restructure's own config-table append
duplicated tuning.md's quick-reference — merged (10 unique rows + the precedence note folded
into the canonical table, duplicate section removed); (2) **the week's new operator surface
had zero runbook coverage** — the topology-pressure warn, `connect_peer`, and
`individual_flood_fallbacks` now have a tuning.md runbook entry + observability.md counter
docs with a remedy link (`dead_shards`/liveness fields added to the /stats row too); (3) the
funnel's tuning row understated the file's grown scope. Verified current: deployment.md knows
deploy/ (k8s+terraform), production-readiness documents the iptables scale ceiling, link
sweep clean.
