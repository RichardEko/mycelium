# Coordinator-freeness composes — the scale-invariant boundary

↑ [theory](theory.md) · synthesis of 2026-06-13 + the 2026-06-15 NANDA deep dive

**Headline thesis:** Holland's signal/boundary primitive is **scale-invariant**, which is
*why* the no-coordinator property carries upward without re-engineering per layer. The
recurring primitive is **self-election of participation scope**: node (`join_group` +
`Boundary::admits`) → group (emergent capability groups) → domain (peer admission + the
opt-in A2A/gateway edge — a domain can run fully dark) → federation (the domain elects
whether to publish AgentFacts into the quilt). Two consequences:

1. **Anti-capture is intrinsic** — federation is a boundary *choice with exit*; no domain
   can be captured into a federation it doesn't elect. This is the monetary-ecology L0
   "exit" invariant at domain scale.
2. **The recursion is adaptive, not identical** — the boundary re-specialises its threat by
   scale: overload (opacity/load-shed) at the node; *capture* ("control whether I'm seen")
   at the domain↔federation edge. Same primitive, opposite-facing threats.

MCB (moral-circle breadth) is recovered at the *local* boundary scale; the global backbone
stays MCB-neutral by necessity — layering lets neutrality and breadth coexist.

## Instance 1 — Bitcoin: emergent (not designed) ecological good

The protocol is ecologically **agnostic by design** (prices energy, not its source); the
cheapest joules are increasingly stranded/waste (curtailed renewables, flared/landfill
methane), so price-seeking miners migrate toward climate-positive consumption.
Neutrality-is-the-price-of-credible-neutrality; on the ecological axis the market may
deliver MCB emergently — stronger than legislating it. Numbers (Batten/Woo/DARI, Jan-2026):
~52.6–56.7% sustainable, up from ~34% (2021); ~29 carbon-negative methane operations offset
~7% of emissions. Caveats to keep: "sustainable %" is definition-laden; the honest baseline
is fiat's full systemic cost; the defensible claim is *agnostic-by-design*, not *negative*.

## Instance 2 — NANDA: Mycelium is a sovereign quilt-patch (verified vs arXiv 2507.14263 + 2508.03101)

- NANDA = **the Index** (global handle → signed AgentFacts; nothing Mycelium-shaped — the
  layer above, zero overlap) + **the Registry Quilt** (federated registries via SWIM +
  delta gossip + Merkle anti-entropy + OR-Map CRDTs + Ed25519 cross-signing + CT log —
  the same mechanism family at inter-org/Byzantine scope).
- **NANDA's scope statement draws our boundary:** it explicitly defers "agent-to-agent
  messaging, runtime coordination, state replication" — that deferred list *is* Mycelium's
  Layers I/II/III. A Mycelium cluster = one sovereign patch; **AgentFacts is the single
  outward contract** (shipped: `mycelium-agentfacts`, WS-F/M16).
- **The one friction:** transport adjacency. Never let Mycelium's intra-cluster gossip *be*
  the inter-registry Quilt transport — different trust scope (intra-domain sub-second vs
  cross-org Byzantine 60s SLO). Different scopes keep them complementary.
- **External corroboration, citable:** an MIT-backed project independently landing on
  coordinator-free gossip convergence (and independently choosing SWIM) is convergent-design
  evidence for the Coordinator-Trap thesis and for Mycelium's own M5 choice.
- **Techniques lifted** (all since shipped): Merkle anti-entropy (wire v12), CT-style
  revocation/transparency log (WS-D), AgentFacts emission (M16). OR-Map was evaluated and
  declined (`docs/design/or-map-gcap-evaluation.md`).
- **Capture risk relocates:** from "single backbone" to which quilt patches / AgentFacts
  certifiers become de-facto dominant — design-mitigated, not outcome-guaranteed. Inside-
  the-patch is Mycelium's lane.

Corrected 2×2 (money ↔ agents): L0 federation/trust = BTC-era L0 ↔ NANDA; local sovereign
economy = Fedimint ↔ Mycelium; global settlement = BTC L1 ↔ *(absent — the agent settlement
rail is still empty)*. Lietaer (2001) is the pre-BTC ancestor of the layered thesis.
