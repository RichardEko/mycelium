# Mycelium — Architectural Philosophy

This document records *why* Mycelium is shaped the way it is. It is the reference
point for evaluating future changes — not a manifesto, but a precise statement of
the conceptual foundations and the decisions that follow from them. Any proposed
addition that cannot be reconciled with this document deserves careful scrutiny
before it is accepted.

---

## Foundational Model — Holland's Signals & Boundaries

The architecture is grounded in John Holland's framework for Complex Adaptive Systems
as formalised in *Signals and Boundaries: Building Blocks for Complex Adaptive Systems*
(MIT Press, 2012).

Holland's core thesis: the behaviour of complex adaptive systems emerges from two and
only two primitives.

**Signals** propagate through a medium unconditionally. No signal is withheld from
propagation on the basis of who might act on it. The medium floods.

**Boundaries** are receptor sets. Each agent holds a boundary — a set of conditions
under which it *acts* on a signal. The boundary controls acting, not receiving.
Forwarding is always unconditional.

This inversion is the key insight. Conventional distributed systems thinking routes
messages to known recipients. Holland's model (and Mycelium's) changes the medium
and lets any agent whose boundary matches respond. The emitter does not need to know
who is listening. Topology does not need to be managed explicitly. The system tolerates
churn without stalling because there is no routing table to maintain.

### Direct mapping to Mycelium

| Holland concept | Mycelium implementation |
|---|---|
| Signal propagation — unconditional flooding | All nodes forward all `Signal` frames regardless of scope |
| Boundary — receptor set controlling action | `Boundary::admits()` — controls delivery, never forwarding |
| Stigmergy — state left in the medium | `sys/load/{node}/...` pheromone keys — opacity written to KV, read by others |
| Tag matching — signals find matching receptors | `SignalScope::Group(name)` — receptor identity via group membership |
| Building blocks — small recombinable units | `CapabilityGroupDef` composing filter + provides + requires |
| Meta-dynamics — agents forming coalitions | Emergent groups: nodes self-join by evaluating their own capabilities |
| Constraint satisfaction across agent coalitions | `cross_group_propose` — all voting blocs must independently reach quorum |

---

## Layering Principle

Complexity must emerge from composition at higher layers. It must not be baked into
the substrate.

The substrate (Layers I and II — KV store and signal mesh) has no concept of agreement,
coordination, or workflow. It propagates bytes and admits signals. Everything else —
consensus, capability wiring, sharding, RPC — is layered on top and uses the substrate
rather than bypassing it.

**Consensus is an emergent higher-order concern, not a substrate primitive.**

`ConsensusEngine` is implemented entirely through the signal mesh. `PROPOSE`, `VOTE`,
and `COMMIT` are signal payloads riding ordinary `Signal` frames. The commitment
semantics — ballot numbering, quorum checking, KV write on commit — are logic that the
proposer applies to the signal stream. Layer II has no concept of "agreement." The
substrate is unaware that consensus is happening.

This is the correct separation. If consensus were a substrate primitive, you would have
over-specified the foundation for the 90% of interactions that do not require commitment
semantics, and you would have constrained what can be built on top.

The same principle applies to every higher-layer concern. Ask of any proposed addition:
*does this belong in the substrate, or is it a higher-order concern that should use the
substrate?* If it is higher-order, it must not require changes to the signal propagation
or KV replication machinery.

---

## Cooptation of Prior Art

Two prior frameworks contributed specific primitives that were retained, stripped of
their implementation ceremony, and re-expressed as substrate properties.

### OSGi Requirements & Capabilities

OSGi (Open Service Gateway initiative) formalised a dependency model where modules
declare capabilities they provide and requirements they need; a resolver matches them.
The primitive is correct: declarative matching between providers and consumers, with
the resolver handling wiring.

What OSGi got wrong: resolution is *static* — performed once at bundle-install time.
This makes it unsuitable for dynamic systems where participants come and go.

Mycelium keeps the primitive (capabilities, requirements, filter-based matching) and
makes evaluation *continuous*. Capabilities appear and disappear at runtime. Requirements
are re-evaluated against the current mesh state. Emergent groups form and dissolve
dynamically. The resolver runs on every relevant KV change, not once at startup.

### Jini Leases

Jini (Sun Microsystems, 1998) introduced the insight that distributed resource
registrations should *decay* rather than persist indefinitely. A service holds a lease
on its registration; if it does not renew, the registration expires. This provides
implicit failure detection without requiring an explicit deregistration protocol.

The insight is correct. The implementation was protocol-heavy: explicit `Lease` objects,
`renew()` RPCs, a lease manager, explicit cancellation.

Mycelium keeps the insight and discards the protocol. Capability KV entries carry a TTL.
They evaporate unless the agent keeps re-advertising. There is no lease object, no
renewal RPC, no lease manager. The act of remaining alive and active *is* the renewal —
the agent walks the same path and the pheromone trail stays fresh. A dead node's
capabilities simply fade.

This is the Holland-approved version: failure detection is emergent (entries evaporate),
not protocol-explicit (lease expiry notifications). The evaporation rate is a substrate
property, not a protocol concern.

---

## The "Strip the Ceremony" Heuristic

Both OSGi and Jini solved real problems correctly at the conceptual level but carried
too much implementation ceremony — explicit managers, lifecycle protocols, object
handles. The pattern that produces better architecture is:

1. Identify the correct *concept* in the prior work.
2. Ask: what substrate *property* would produce the same behaviour without an explicit
   protocol?
3. Implement the property; let the behaviour emerge.

Jini leases → TTL as natural evaporation.
OSGi static resolution → continuous evaluation against live KV state.
Explicit routing tables → unconditional propagation + boundary admission.
Explicit failure detection → pheromone trail evaporation.

When a proposed feature requires a manager, a coordinator, an explicit lifecycle
protocol, or a renewal RPC, apply this heuristic before accepting it. There is almost
always a substrate-property equivalent that is simpler, more robust, and more consistent
with the rest of the architecture.

---

## Litmus Tests for Proposed Changes

Before accepting any significant addition, apply these three questions.

**1. Substrate or higher-order concern?**

Does this belong in Layer I (KV replication) or Layer II (signal mesh), or is it a
higher-order pattern that should be built *using* those layers? If it is higher-order,
it must use the substrate rather than modify it. A new consensus variant is not a new
Layer II primitive — it is new logic in Layer III that emits signals through the existing
mesh.

**2. New primitive or composition?**

Is this genuinely a new primitive that cannot be expressed as a composition of existing
ones? New primitives raise the conceptual surface area for every future user and
contributor. They need exceptional justification. If the proposed behaviour can be
achieved by composing `emit`, `advertise`, `resolve`, `propose`, or `set`, it should be.

**3. Protocol or substrate property?**

Does this require explicit protocol machinery — a manager, a lifecycle, a renewal RPC,
an explicit deregistration — that could instead be expressed as a substrate property?
If yes, find the evaporation equivalent. The Jini anti-pattern (correct concept, wrong
implementation) is the most common failure mode for otherwise sound proposals.

---

## What This Architecture Is Not

Understanding the boundaries matters as much as understanding the model.

**Not a message broker.** There is no central broker, no topic registry, no persistent
queue. Signals are ephemeral. If a node misses a signal, it misses it — this is
intentional. Durable delivery is a higher-order concern built on the KV layer or
consensus, not a substrate property.

**Not a service mesh.** There is no sidecar, no control plane, no certificate authority
external to the cluster. mTLS is peer-to-peer; the Ed25519 keypair is the node's
identity. The mesh is the library.

**Not an actor framework.** Actors have explicit addresses and explicit lifecycle
management. Mycelium nodes have capabilities and boundaries. The difference is not
cosmetic — it determines whether topology must be managed explicitly (actors) or
emerges from capability matching (Mycelium).

**Not eventually consistent in the eventual-consistency-as-compromise sense.** The KV
layer is last-write-wins with HLC causal ordering. This is a deliberate choice, not a
concession. For coordination that requires strong agreement, the consensus layer exists.
The two tiers are complementary, not redundant.

---

## The Novel Synthesis

No single element of Mycelium is without precedent. The novelty is in the synthesis:

- Holland's signal/boundary substrate
- OSGi's capability/requirement matching, made continuous
- Jini's lease insight, expressed as TTL evaporation
- Epidemic consensus as an emergent higher-order concern, not a substrate primitive
- All of it implemented as a single embeddable Rust library — no broker, no
  coordinator, no external dependencies for the core substrate

The sum is more than the parts because the parts were each solving the same underlying
problem from different directions. Holland described the model theoretically. OSGi and
Jini approached it pragmatically but with too much ceremony. Mycelium is what you get
when you take the concepts, strip the ceremony, and let the behaviour emerge from
substrate properties — as Holland would have wanted.

---

## Further Reading

- John Holland — *Signals and Boundaries: Building Blocks for Complex Adaptive Systems* (MIT Press, 2012)
- John Holland — *Complexity: A Very Short Introduction* (Oxford, 2014)
- Marco Dorigo & Thomas Stützle — *Ant Colony Optimization* (MIT Press, 2004) — stigmergy in detail
- OSGi Alliance — *OSGi Core Release 8 Specification*, Chapter 27: Capabilities and Requirements
- W. Keith Edwards — *Core Jini* (2nd ed., Prentice Hall, 2000) — lease model, Chapter 4
