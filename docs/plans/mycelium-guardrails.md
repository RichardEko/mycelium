# mycelium-guardrails — structural, coordinator-free guardrails (design sketch)

**Status:** 🔵 **PROPOSED — v3.0 *primary* deliverable (alongside `mycelium-reason` DX), not started.**
Mostly packaging + worked examples over existing substrate mechanisms; a small ergonomic policy layer
is the only new code. Positioning: [`../wiki/domain/pattern-coverage.md`](../wiki/domain/pattern-coverage.md)
→ Structural guardrails.

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
