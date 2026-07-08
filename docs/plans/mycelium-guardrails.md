# mycelium-guardrails — structural, coordinator-free guardrails (design sketch)

**Status:** 🟡 **IN PROGRESS — v3.0 *primary* deliverable (alongside `mycelium-reason` DX).**
PR 1 shipped (the policy API): the `mycelium-guardrails` crate with the self-imposed, tier-labelled
`Policy` → `apply()` (boundary + `AgentPolicy` + `authorized_callers`), `Policy::strength_report()`,
and the Tier-C `check_caller`/`guarded_rpc_serve` gate that seals `Invoke`/`Denied` into the audit
chain (feature `compliance`). ✅ **PR 2 shipped: the worked wedge demo + the policy-audit
verification tool** — `prove_denials`/`narrate_proof` (`src/verify.rs`) reconstruct a provider's
tamper-evident chain and prove the guardrail fired (honest framing — *provable-stopping* of the
sealed denials, **not** a global "X could not have done Y"; the chain is per-node and only gated
capabilities seal denials); the self-contained `examples/guardrail_wedge.rs` (unauthorized agent
structurally stopped at the provider gate, proof reconstructed by a neutral observer node) +
`ci_smoke.sh` earn it at the smoke bar. Still forthcoming: the broader worked examples + a guide
chapter.
Mostly packaging + worked examples over existing substrate mechanisms; a small ergonomic policy layer
is the only new code. Positioning: [`../wiki/domain/pattern-coverage.md`](../wiki/domain/pattern-coverage.md)
→ Structural guardrails. **Amended 2026-07-08** (pre-implementation reassessment, code-verified — see
the dated addendum §below: the mechanisms are real, but their guarantees fall in **three distinct
strength tiers**, and the honest lead wedge is the *hard-prevention* one, which is already ~80% wired).

## Addendum (2026-07-08) — pre-implementation reassessment (code-verified)

A code-level pass over all six composition mechanisms. Headline: the plan's central claim is **true** —
the receiver `Boundary` *structurally prevents* out-of-boundary action — but "mostly packaging + a small
ergonomic layer" undersells the design, because the mechanisms deliver **three different strengths of
guarantee**, and an honest policy must say which clause compiles to which. Six bindings:

1. **Boundary = real structural prevention, but coarse + self-imposed.** `Boundary::admits(scope)`
   (`mycelium-core/src/signal.rs`) is a **drop-before-handler** gate at `ops.rs:29` /
   `connection.rs:457` — an out-of-boundary signal never reaches any handler (the delivered `scope`
   field is "informational only"; admission is enforced structurally). *But:* it admits by **scope**
   (`System` / `Group` / `Groups` / `Individual` — group membership or node id), so it is **coarse** —
   it gates *which signals a node acts on*, not *which tool/data/argument*. And it is the **acting node
   enforcing its own boundary** (forwarding stays unconditional — a node relays signals it won't act
   on). ⇒ **Tier A: prevention for an honest node, promise-strength against a malicious one** (the plan
   already states this; keep it prominent).

2. **`authorized_callers` is the one HARD-prevention path — but the provider must opt in.** Two
   enforcement points, very different strength: resolve-time `can_see` (`capability_handle.rs:139`) is
   a *weak visibility filter* (hides the cap; a caller who knows `(ns,name)` can still RPC it);
   invoke-time `caller_authorized` (`mod.rs:1151` → `rbac.rs:148`, admits by node-id **or role**) is
   the *authoritative* gate that rejects an unauthorized RPC. **The core mesh RPC does NOT auto-enforce
   it** — the provider must call `caller_authorized` in its serve loop, and the **only** production call
   site today is SkillRunner (`src/bin/skillrunner/runner.rs:78`). ⇒ **Tier C: true hard prevention of
   a specific action — but only if that action routes through a provider that gates.** This is the
   load-bearing correction: "structurally stopped" at the capability layer is real, not promise-strength
   — *for authz-gated actions*.

3. **The audit "prove X could not have done Y" story is real in shape, thin in coverage — and the
   cheapest high-value new code.** The chain (`src/agent/audit.rs`, `compliance`) is per-node,
   Ed25519-signed, hash-chained, and verifiable (integrity + contiguity + signature via
   `verify_chain`). Crucially it **records denials** (`AuditOutcome::Denied` with a signature-verified
   `principal`) — but the *only* production site sealing a denial is again SkillRunner's authz reject
   (`runner.rs:81`); capability-authz and gateway-scope denials are **not** sealed despite the doc. And
   `principal` is a free-form `String`, trustworthy only where the caller sets it from a verified
   `req.sender()`. ⇒ **New code, small + high-value: seal `audit(Denied)` at the capauthz /
   `authorized_callers` reject paths**, so "prove the fleet stopped X" is broad, not SkillRunner-only.

4. **`AgentPolicy` already exists — but at transition-strength and self-imposed only.** Layer-V
   `AgentPolicy` (`state_machine.rs`, **always compiled**) already carries `allowed_tools` /
   `denied_tools` / `tool_budget` / `max_turns` / `required_capabilities` / `require_approval_for` /
   `state_timeouts`, atomically enforced. **Two caveats a guardrail must own:** (a) it guards the state
   **transition** (`→ Invoking{tool}`), *not the effect* — a side-effect not preceded by a policed
   transition bypasses it (convention-strength); (b) it is **self-imposed** — `set_policy` is local,
   `agent/{node}/policy` KV is **publish-only** (a supervisor can *observe* or *veto a pre-declared
   tool*, never *impose* a policy). ⇒ **Tier B: self-imposed, transition-level.** So the "ergonomic
   policy API" is partly *repackaging AgentPolicy*, but effect-level enforcement, remote/supervisory
   policy, and argument/data/spend scoping are **genuinely new** — more than "a small ergonomic layer."

5. **capauthz (WS-D M6) gates the *advertiser*, not the caller; CT revocation revokes a *key*.**
   `resolve_for_caller` routes around an advertiser lacking a required role (`capauthz.rs`, resolve-time,
   `compliance`) — a different axis from `authorized_callers` (*may this node provide* vs *may this
   caller invoke*). CT revocation (`revocation.rs`) revokes an **Ed25519 key**: a revoked key fails
   verification fleet-wide (via `known_verifying_keys` minus the revoked set) once it gossips, and this
   flows transitively — a role claim signed by a revoked key reads as no-roles. Merkle transparency +
   `/gateway/transparency` give client-verifiable inclusion proofs. Real and strong; the guardrail
   *composes* it (revoke a compromised agent's key → its role-gated caps evaporate fleet-wide) rather
   than adding to it.

6. **The wiki access broker is a grant-time membership gate only.** `permits(node_id)` at grant time
   (a one-shot RPC handshake), then direct reads forever — no per-read gating, and for `FsStore` no
   real revocation (coarse: change the allowlist → *future* grants denied; current holders keep the
   location). Fine as a data-access-bootstrap precedent; do not over-claim it as live enforcement.

**Reframed lead wedge (the honest strongest first demo).** Not the boundary demo (Tier A, self-imposed
— an honest node *choosing* not to act reads weakly). Lead with the **Tier-C hard-prevention** story,
which is ~80% already wired: **an agent invokes a tool it is not authorized for → the provider's
`caller_authorized` rejects the RPC → the denial is sealed into the tamper-evident chain (`Invoke/
Denied`, verified principal) → tooling reconstructs the chain and *proves* the agent was stopped.**
SkillRunner already does the reject-and-seal (`runner.rs:78-91`); the companion adds the *policy
declaration* that produces the `authorized_callers`, the boundary complement, and the **verification
tool** ("here is the cryptographic proof X was denied Y"). That is a category-of-one claim — no
central chokepoint, and the proof survives compromising any single node.

**Net effect on scope.** Genuinely new code, in priority order: (i) seal denials at the two unsealed
reject paths (small); (ii) the ergonomic **policy API** that compiles one declaration to boundary +
`authorized_callers` + `AgentPolicy` clauses, *labelling each clause's strength tier*; (iii) the
**policy-audit verification** tool; (iv) the worked wedge + examples. The self-imposed-vs-remote-policy
question (binding #4) is a **decision the design must make** — see the reassessment note below the
sequencing section; the thesis-aligned answer is *self-imposed + legible*, not a remote policy server
(which is the chokepoint non-goal).

## What it is

The **structural / capability guardrails** layer — *what an agent is allowed to do* (which tools, data,
spend, groups) — packaged as a first-class safety story. It is **not** content guardrails
(toxicity / PII / jailbreak): that is a use-case function, an external service (Llama Guard, NeMo)
accessed *through* the mesh. This is the harder, architectural problem — constraining agent *action* —
and where a coordinator-free substrate is uniquely strong.

## The compelling frame — no central chokepoint

A coordinator-based system enforces guardrails at the coordinator: a **single point of bypass,
compromise, latency, and scaling** — and the mainstream "guardrail proxy in front of the model" *is*
that coordinator. Coordinator-free, enforcement is at **every receiver's `Boundary`**: no central
policy engine to bypass; compromising one node cannot lift the fleet's policy; and audit is **per-node
and tamper-evident**. This is the Layer-II boundary/opacity mechanism (the paper's admission model)
doing exactly what it was built for — reframed as governance/safety.

## The composition base (mostly packaging — the mechanisms already ship)

- **Receiver-side signal `Boundary`** (Layer II) — a node *structurally cannot act* on a signal outside
  its boundary. Admission control at the point of action.
- **Capability authorization + CT revocation** (WS-D M6) — who may advertise/invoke what, revocably.
- **`tool_budget` / `max_turns`** (Layer-V `AgentPolicy`) — spend/turn caps, enforced under the state lock.
- **Membership-gated access broker** (wiki) — who is granted a resource.
- **mTLS identity + tamper-evident hash-chained audit** (WS1/WS2) — non-repudiable "who did what."
- **Opacity** — load-based admission back-pressure.

## What's genuinely new (small, scoped)

- **An ergonomic policy API** — declare an agent/group's allowed tools/data/spend/groups in *one* place,
  compiled down to boundaries + capability authz + `tool_budget`. Today these are separate mechanisms;
  the companion unifies them into a single, legible "policy."
- **Policy-audit verification** — "prove agent X *could not have* done Y" from the tamper-evident audit
  chain; a revocation-took-effect view.
- The **worked examples** that earn the claim (the real deliverable).

## Sequencing — lead with the differentiating, mostly-composed wedge

**Wedge first (a validated example, not the full policy API):** an agent **structurally stopped at a
boundary** from an out-of-boundary action, **with the audit proving it** — built from existing
`Boundary` + audit, minimal new code. That single demo makes the "no central chokepoint, tamper-evident"
story concrete. Then the ergonomic policy API + the revocation/budget examples.

Build/adopt/interop symmetry with DX: **BUILD** structural guardrails (native, un-adoptable — no one
else has receiver-side boundaries); **ADOPT/INTEROP** content guardrails (external Llama-Guard-class
service via the mesh — a use-case function, not this companion).

## Non-goals

- **Content guardrails** (toxicity / PII / jailbreak / moderation) — use-case function, external service
  via the mesh (`pattern-coverage.md` → Use-case functions).
- A central policy server — reintroduces the chokepoint this exists to remove.

## Expressible ≠ validated (and the honest limits)

- **Promise-strength, not central mandate.** Each node enforces its own boundary; the substrate makes a
  violation *legible* (tamper-evident audit) rather than centrally *preventing* it. A node that ignores
  its own boundary is *detected*, not stopped — on thesis ("detection + legibility"), but state it, don't
  gloss it. (For hard prevention of a specific action, that action must route through a capability whose
  authz *is* the gate — a design choice per guardrail.)
- **Policy is eventually-consistent.** A revocation/policy change propagates at gossip speed (the CT
  revocation log bounds the window) — not instantly global. The anti-entropy tradeoff, applied to policy.
- The whole framing is a **reframe of existing mechanisms** — a hypothesis until the wedge example ships,
  earned at the `ci_smoke` bar like every other v3.0 row.

## Trigger

A customer whose agents touch real tools / data / spend and who needs *provable*, bypass-resistant
action constraints — or a positioning need to answer "how do you keep fleet agents safe without a
central policy chokepoint?"
