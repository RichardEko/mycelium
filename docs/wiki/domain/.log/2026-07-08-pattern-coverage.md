# 2026-07-08 — ingest: v3.0 primaries shipped (pattern-coverage staleness fix)

Part of wiki-lint 2 (see `dev/.log/2026-07-08-lint-2.md`). Both v3.0 primaries completed since the page
was last touched, so `domain/pattern-coverage.md` had two stale positioning claims:

- **Structural guardrails section** read as *proposed* ("Now a primary v3.0 deliverable — mycelium-guardrails").
  Rewrote to **✅ SHIPPED (`mycelium-guardrails`, #137–#139)** and folded in the design's defining
  finding from the code-verified reassessment: **three strength tiers** surfaced by
  `Policy::strength_report()` — **Tier C** `authorized_callers` = *hard prevention* (unauthorized invoke
  rejected at the provider + denial sealed into the tamper-evident chain); **Tier A** boundary =
  self-imposed prevention (drop-before-handler, promise-strength vs a malicious node); **Tier B**
  `AgentPolicy` = self-imposed at state transitions. Named the `prove_denials` verification tool with its
  honest framing (*provable-stopping*, not global negative proof) and the self-imposed stance (no remote
  policy authority — the chokepoint non-goal).
- **LLM-DX axis section** marked only the first tranche (#130/#131); added the full LangGraph example
  ladder (#132–#136): routing surface, the deploy/reheal flagship, the router-robustness fix it surfaced
  (live-SWIM filter + fast failover), rungs 0–5, chapter 15, the Ollama-manual variant.

No coverage claim changed — both were already framed correctly (structural guardrails = native strength;
LLM-DX = a distinct axis). This was purely marking *proposed → shipped* with the code-verified detail.
Code-anchored: the tier framing matches `mycelium-guardrails/src/policy.rs::Strength`; the router fix
matches `mycelium-reason/src/route.rs` (liveness filter + `failover_timeout`).
