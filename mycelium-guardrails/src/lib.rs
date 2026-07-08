//! # mycelium-guardrails — structural, coordinator-free guardrails on Mycelium's public API
//!
//! *What an agent may do* — which groups it acts within, which tools/budgets gate its state
//! transitions, which callers may invoke its guarded capabilities — declared **once** as a
//! [`Policy`] and compiled onto a node by [`apply`]. This is the **structural / capability**
//! guardrail (constraining agent *action*), not content moderation (an external service reached
//! *through* the mesh). Strategy and code-verified bindings: `docs/plans/mycelium-guardrails.md`.
//!
//! ## No central chokepoint
//!
//! A coordinator-based system enforces guardrails at the coordinator — a single point of bypass,
//! compromise, and scaling. Coordinator-free, enforcement is at **every receiver's boundary** and
//! **every provider's own gate**: no central policy engine to bypass, compromising one node cannot
//! lift the fleet's policy, and audit is per-node and tamper-evident.
//!
//! ## The three strength tiers (the honesty of the design)
//!
//! A guardrail must say which clause compiles to which guarantee. [`Policy::strength_report`]
//! discloses exactly that, per active clause:
//!
//! - **[`Strength::HardPrevention`] (Tier C)** — hard prevention: an unauthorized action is
//!   *rejected at the provider*, not just detected. Applies to invocations of the node's own served
//!   capabilities (`authorized_callers`), and the denial is **sealed** into the tamper-evident
//!   audit chain (`Invoke`/`Denied`, verified principal) — the "prove X was stopped" foundation.
//! - **[`Strength::SelfImposedPrevention`] (Tier A)** — structural prevention for an honest node
//!   (drop-before-handler at the boundary), but coarse (by group/scope) and self-imposed: a
//!   malicious node could ignore its own boundary.
//! - **[`Strength::SelfImposedTransition`] (Tier B)** — self-imposed, enforced at agent state
//!   transitions (`AgentPolicy`); a side effect not preceded by a policed transition is not caught.
//!   Legible, not hard.
//!
//! ## Self-imposed stance
//!
//! [`apply`] configures **this** node. There is no remote authority: no path sets another node's
//! policy (the coordinator-free thesis — a central policy server is the chokepoint non-goal). A
//! supervisor may *observe* the resulting `agent/{node}/policy` KV entry; it can never impose one.
//!
//! ## Namespaces touched
//!
//! - `grp/{group}/{node}` — Tier A boundary membership (via `mesh().join_group`).
//! - `agent/{node}/policy` · `agent/{node}/state` — Tier B AgentPolicy + live state.
//! - `cap/{node}/…` — Tier C `authorized_callers` stamped onto advertised capabilities.
//! - `sys/audit/{node}/…` — Tier C sealed `Denied` records (feature `compliance`).

mod apply;
mod policy;
#[cfg(feature = "compliance")]
mod guard;
#[cfg(feature = "compliance")]
mod verify;

pub use apply::{apply, AppliedPolicy};
pub use policy::{Clause, Policy, Strength};

/// The Tier-C hard-prevention surface (feature `compliance`): the `authorized_callers` invoke-time
/// gate and its denial sealing. [`check_caller`] composes into any serve loop; [`guarded_rpc_serve`]
/// spawns the loop for you. [`AppliedPolicy::guard`] stamps the allowlist onto a capability.
#[cfg(feature = "compliance")]
pub use guard::{check_caller, guarded_rpc_serve, CallerVerdict, GuardHandle};

/// The policy-audit **verification tool** (feature `compliance`): reconstruct a provider's
/// tamper-evident chain and prove which unauthorized invocations it sealed as denied.
/// [`prove_denials`] returns a [`DenialProof`] of [`SealedDenial`]s; [`narrate_proof`] renders it
/// with the honest framing (provable-*stopping* of these denials — **not** a global "X could not
/// have done Y" claim; the chain is per-node and only gated capabilities seal denials).
#[cfg(feature = "compliance")]
pub use verify::{narrate_proof, prove_denials, DenialProof, SealedDenial};
