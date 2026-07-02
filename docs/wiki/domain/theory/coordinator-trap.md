# The Coordinator Trap — epistemic necessity, not performance

↑ [theory](theory.md)

**The core claim:** any coordinator must model the resource state of every agent it
coordinates and keep that model current. In a heterogeneous fleet whose capabilities, load,
and state change continuously, no coordinator can have sufficient local knowledge — it is
not just slower; it is *structurally incapable*. This is Holland's emergent-coordination
argument and Hayek's dispersed-knowledge argument arriving at the same place.

**Lineage (get the order right):** Mycelium started from John Holland's *Signals and
Boundaries* (2012) — the signal mesh, receptor routing, emergent groups, and the
capability/wiring subsystem are direct implementations of his CAS building blocks. Hayek
(*The Use of Knowledge in Society*, 1945) was discovered later as a complementary epistemic
framing; two independent traditions converging strengthens the claim. Paper 1 documents the
three structural failure modes of coordinator frameworks (unbounded audit obligation,
context loss on restart, output-format mismatch) as irreducible-by-improvement.

**Where the claim is strongest:** the gossip substrate + receptor routing (tight
derivation; alternatives demonstrably wrong for the use case) and the capability/wiring/
demand subsystem (most novel; no standard prior art). **Where qualified:** the consensus
layer — Raft is also *correct*, just not optimal under the embedded/decentralised
constraint; a design choice, not a unique derivation. Post-review moderation discipline
(see [publications](../publications.md)): the cross-domain convergence is *homologous*
evidence, not proof.

**As a moat:** the published corpus is prior art — any competitor either cites it or
consciously diverges; either way Mycelium is the reference point. Competitor paths all
favour the incumbent: copying the model concedes the design; using a broker hits the
epistemic ceiling empirically; claiming narrower scope concedes the market. Compounds with
the SOC 2 process-time moat ([strategy](../strategy/licensing-and-compliance.md)). When
positioning: lead with the epistemic argument — performance is measurable and can be
competed away; epistemic impossibility cannot.
