# Changelog — mycelium-guardrails

All notable changes to this crate. It versions **independently** of the Mycelium substrate (at
2.x) — it is built on the public `mycelium` 2.x API only. Versioning follows
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [1.0.0] — 2026-07-26

**API-stability commitment.** The guardrails surface is now stable — no breaking change without a
2.0 major bump: the tier-labelled `Policy` (Tier C `authorized_callers` hard-prevention, Tier A
`Boundary`, Tier B `AgentPolicy`), `Policy::strength_report()`, and the `prove_denials`
verification tool.

Freezing the API is honest because the crate's **scope is feature-complete**: its remaining limits
— promise-strength for the self-imposed tiers (A/B), eventually-consistent policy propagation, and
coarse revocation — are **by design**, inherent to a coordinator-free model with no central policy
authority, not missing features. `strength_report()` and `prove_denials` surface those limits
rather than hiding them.

Shipped in the initial tranche (PRs #137–#139, 2026-07-08); this release commits to that surface.

### Note

Promoted from `0.1.0` with no code change — a versioning correction. `0.1.0` under-signalled a
crate that is CI-gated (test + `compliance` matrices + clippy), documented (guide chapter 16), and
inside the self-audit scope.
