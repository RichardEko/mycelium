# 16 · Guardrails

Chapter 15 was about *authoring* the reasoning a fleet does. This one is about *bounding what it
may do* — structural, coordinator-free guardrails. It is grounded in the
[`mycelium-guardrails`](../../mycelium-guardrails/) companion and its runnable wedge; the design and
its code-verified bindings are in [`docs/plans/mycelium-guardrails.md`](../plans/mycelium-guardrails.md).

Two questions this chapter answers:

- **"How do you keep fleet agents safe without a central policy chokepoint?"** A coordinator-based
  system enforces guardrails at the coordinator — a single point of bypass, compromise, latency, and
  scaling, and the mainstream "guardrail proxy in front of the model" *is* that coordinator.
  Coordinator-free, enforcement is at **every receiver's boundary** and **every provider's own gate**:
  no central engine to bypass, compromising one node cannot lift the fleet's policy, and audit is
  per-node and tamper-evident.
- **"What can I actually constrain?"** *What an agent may do* — which groups it participates in, which
  tools/budgets gate its reasoning, which callers may invoke its capabilities. This is the **structural
  / capability** guardrail. It is **not** content moderation (toxicity / PII / jailbreak): that is a
  *use-case function*, an external service (Llama Guard, NeMo) reached *through* the mesh, not part of
  this companion (see [pattern-coverage](../../docs/wiki/domain/pattern-coverage.md) → use-case
  functions). Constraining *action* is the harder, architectural problem, and where a coordinator-free
  substrate is uniquely strong.

---

## The honest core: three strength tiers

The one thing a guardrail must never do is over-claim. Mycelium's mechanisms are real and individually
strong, but they deliver **three different strengths of guarantee**, and a policy has to say which
clause compiles to which. This is the design's integrity — `Policy::strength_report()` discloses it
per active clause:

| Tier | Strength | Mechanism | What it means |
|---|---|---|---|
| **C** | **Hard prevention** | `authorized_callers` on a served capability | An unauthorized invocation is **rejected at the provider** and the denial is **sealed** into the tamper-evident audit chain. Real prevention — not promise-strength — *for actions that route through a gating provider*. |
| **A** | Self-imposed prevention | receiver `Boundary` (groups/scope) | A node **structurally cannot act** on a signal outside its boundary (drop-before-handler). But coarse (by group/scope, not tool/argument) and self-imposed: a *malicious* node could ignore its own boundary. Real prevention for an honest node. |
| **B** | Self-imposed, transition-level | Layer-V `AgentPolicy` | Tool allow/deny, call/turn budgets, required capabilities, approval — enforced at the agent's state transitions. Self-imposed and *transition-level*: a side effect not preceded by a policed transition is not caught. Legible, not hard. |

Why three and not one honest "prevention": the substrate's admission model *prevents* an honest node
from acting outside its boundary (Tier A), and a capability's `authorized_callers` *hard-prevents* an
unauthorized caller at the provider (Tier C) — but the agent-level tool policy (Tier B) governs the
agent's own state machine, which it applies to itself. Collapsing these would claim central,
malicious-proof enforcement the coordinator-free model deliberately doesn't provide. Naming the tiers
is what makes the safety story trustworthy.

---

## The policy API — one declaration, self-imposed

```rust
use mycelium_guardrails::{Policy, apply};

let policy = Policy::new()
    .act_within_groups(["region-north"])        // Tier A — boundary
    .deny_tools(["shell", "wire_transfer"])     // Tier B — AgentPolicy
    .tool_budget(20)                            // Tier B
    .authorized_callers([coordinator_id]);      // Tier C — provider gate

for clause in policy.strength_report() {        // the legibility: which clause is which guarantee
    println!("{} [{}] — {}", clause.name, clause.tier.label(), clause.detail);
}

let applied = apply(policy, &agent).await;      // compiles onto THIS node
```

`apply` compiles the one declaration down to the three mechanisms: it joins the boundary groups
(Tier A), installs the `AgentStateMachine` policy (Tier B), and records the caller allowlist the
provider gate consults (Tier C).

**Self-imposed, by design.** `apply` configures **this** node. There is no remote authority — nothing
sets another node's policy. This is the coordinator-free thesis: a central policy server is exactly the
chokepoint the design exists to remove. A supervisor may *observe* the resulting `agent/{node}/policy`
KV entry; it can never *impose* one. The levers a fleet has over a *misbehaving* peer are the ones the
substrate already gives — narrow its `authorized_callers` allowlist, drop its role, or (self-sovereign)
key revocation — not a push of policy into its process.

---

## Proving a guardrail fired

Prevention you can't prove is a promise. The Tier-C gate **seals** every unauthorized invocation as an
`Invoke`/`Denied` record — verified principal, Ed25519-signed, hash-chained — into the provider's audit
stream. The verification tool reconstructs and re-verifies that chain:

```rust
use mycelium_guardrails::{prove_denials, narrate_proof};

let proof = prove_denials(&any_node, provider.node_id(), Some(caller_id));
for line in narrate_proof(&proof) { println!("{line}"); }
```

Any node can run it — the chain gossips fleet-wide, so a neutral third party proves the denial exactly
as the provider would. **Read the framing honestly** (the tool states it in its own output):

- It **PROVES**: the provider *tamper-evidently sealed stopping* these callers — the records cannot be
  forged, reordered, or removed without the chain failing to verify.
- It **DOES NOT PROVE**: that a caller "could not have done Y *anywhere*." The chain is **per-node**, and
  only *guarded* capabilities that reach the gate seal denials — absence here is not proof of absence
  elsewhere. If the chain doesn't verify, the proof is *voided*, not asserted.

That precise claim — *provable-stopping*, not global negative proof — is a category of one: no central
chokepoint, and the proof survives compromising any single node.

---

## The wedge, runnable

```sh
cargo run -p mycelium-guardrails --features compliance --example guardrail_wedge
```

A provider prints its tier-labelled strength report, an unauthorized agent is **structurally stopped**
at the gate, an authorized one is **admitted**, and a **neutral observer** node reconstructs the
cryptographic proof — with a negative control (the authorized caller has zero sealed denials). It is
the whole story in one deterministic run.

The broader [`guardrail_fleet`](../../mycelium-guardrails/examples/guardrail_fleet.rs) example composes
all three tiers in one constructive-domain fleet and shows each one *actually firing*: a region-scoped
agent that never acts on another region's signal (Tier A), an agent blocked from a denied tool at its
transition (Tier B), and an unauthorized caller rejected and sealed at a provider (Tier C).

---

## Composition — revocation and the levers over a peer

The strong mechanisms below the guardrail compose without new code:

- **Self-sovereign key revocation** (`GossipAgent::revoke_identity_key`, `compliance`). A node revokes
  one of *its own* Ed25519 keys; once the revocation gossips, that key fails verification fleet-wide
  (`known_verifying_keys` minus the revoked set), and this flows transitively — a role claim or audit
  record signed by the revoked key reads back as void. This is the key-hygiene / compromise-recovery
  path. It is **self-sovereign** on purpose: no node can revoke another's key (that would be the
  central authority the thesis rejects). Merkle transparency + `GET /gateway/transparency` give
  client-verifiable inclusion proofs of a revocation.
- **The levers over a *misbehaving* peer** are therefore: narrow the guarded capability's
  `authorized_callers` (Tier C — future invokes denied and sealed), drop the peer's role (its
  role-gated capabilities evaporate at resolve), or let the peer self-revoke a compromised key. All are
  eventually-consistent (gossip-speed), and all are *legible* (the change and its effects are
  observable), which is the coordinator-free stance: detection + legibility over central mandate.

---

## Honest limits (state them, don't gloss them)

- **Promise-strength, not central mandate** (Tiers A/B). Each node enforces its own boundary and its own
  agent policy; the substrate makes a violation *legible* (tamper-evident audit) rather than centrally
  *preventing* it. A node that ignores its own boundary is *detected*, not stopped. For hard prevention
  of a specific action, that action must route through a capability whose `authorized_callers` *is* the
  gate (Tier C) — a design choice per guardrail.
- **Policy is eventually-consistent.** An allowlist change, a role drop, or a revocation propagates at
  gossip speed — not instantly global. The anti-entropy tradeoff, applied to policy.
- **Content safety is out of scope.** Toxicity / PII / jailbreak moderation is a use-case function — an
  external Llama-Guard-class service reached through the mesh, not this companion.

---

**Next:** back to the [guide index](README.md), or [09 · Security](09-security.md) for the mTLS /
Ed25519 / RBAC / audit primitives this composes. The full strategy and the code-verified strength-tier
bindings are in [`docs/plans/mycelium-guardrails.md`](../plans/mycelium-guardrails.md).
