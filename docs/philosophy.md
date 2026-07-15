# Mycelium — Architectural Philosophy

The conceptual foundations behind Mycelium's design — and the litmus tests for evaluating changes against them. This is the reference document for maintaining coherence over time.

## Foundational Model

*Holland's Signals & Boundaries*

The architecture is grounded in John Holland's framework for Complex Adaptive Systems as formalised in *Signals and Boundaries: Building Blocks for Complex Adaptive Systems* (MIT Press, 2012). Holland's core thesis: the behaviour of complex adaptive systems emerges from two and only two primitives.

**Primitive one — Signals — unconditional propagation.** Signals propagate through the medium unconditionally. No signal is withheld from propagation on the basis of who might act on it. The medium floods.

**Primitive two — Boundaries — receptor sets controlling action.** Each agent holds a boundary — a set of conditions under which it *acts* on a signal. The boundary controls acting, not receiving. Forwarding is always unconditional.

> **The key inversion.** Conventional distributed systems route messages to known recipients — A wants to affect B, so A sends to B. Holland's model changes the medium and lets any agent whose boundary matches respond. The emitter does not need to know who is listening. Topology does not need to be managed explicitly. The system tolerates churn without stalling because there is no routing table to maintain.

### Direct mapping to Mycelium

| Holland concept | Mycelium implementation |
|---|---|
| Signal propagation — unconditional flooding | All nodes forward all `Signal` frames regardless of scope |
| Boundary — receptor set controlling action | `Boundary::admits()` — controls delivery, never forwarding |
| Stigmergy — state left in the medium | `sys/load/{node}/…` pheromone keys — opacity written to KV, read by others |
| Tag matching — signals find matching receptors | `SignalScope::Group(name)` — receptor identity via group membership |
| Building blocks — small recombinable units | `CapabilityGroupDef` composing filter + provides + requires |
| Meta-dynamics — agents forming coalitions | Emergent groups: nodes self-join by evaluating their own capabilities |
| Constraint satisfaction across agent coalitions | `cross_group_propose` — all voting blocs must independently reach quorum |

## Layering Principle

*Substrate vs Emergent Concern*

Complexity must emerge from composition at higher layers. It must not be baked into the substrate.

The substrate — Layers I and II (KV store and signal mesh) — has no concept of agreement, coordination, or workflow. It propagates bytes and admits signals. Everything else — consensus, capability wiring, sharding, RPC — is layered on top and *uses* the substrate rather than bypassing it.

> **Consensus is an emergent higher-order concern, not a substrate primitive.** `ConsensusEngine` is implemented entirely through the signal mesh. `PROPOSE`, `VOTE`, and `COMMIT` are signal payloads riding ordinary `Signal` frames. The commitment semantics — ballot numbering, quorum checking, KV write on commit — are logic the proposer applies to the signal stream. Layer II has no concept of "agreement." The substrate is unaware that consensus is happening.

This is the correct separation. If consensus were a substrate primitive, you would have over-specified the foundation for the 90% of interactions that do not require commitment semantics, and you would have constrained what can be built on top.

> **The test for any proposed addition:** does this belong in the substrate, or is it a higher-order concern that should use the substrate? If it is higher-order, it must not require changes to the signal propagation or KV replication machinery.

## Cooptation of Prior Art

*OSGi · Paremus · Jini*

Two prior frameworks contributed specific primitives that were retained, stripped of their implementation ceremony, and re-expressed as substrate properties.

- **John Holland — Signals & Boundaries** — *Conceptual framework · 2012.* Formalises CAS as signal propagation + boundary admission. Provides the theoretical model that explains why unconditional propagation with selective acting is correct for complex adaptive systems.
- **OSGi Requirements & Capabilities** — *Primitive: R&C matching · 2003 – present.* Declarative matching between capability providers and requirement consumers. Correct primitive; made static (deploy-time only) in mainstream adoption.
- **Paremus Service Fabric** — *Proved at runtime · 2010 – 2015.* Demonstrated OSGi R&C applied as a *continuous runtime resolver* — re-resolving as services appeared, disappeared, and changed. Proved the concept. Crucially: the R&C graph was a *declared target state*. The runtime was continuously monitored against it, and deltas were driven back into convergence. Drift from declared intent was structurally prevented — not merely discouraged. What remained was a central reconciliation engine holding that target state: if it went down, reconciliation stopped. The coordinator trap in a more sophisticated form. Positioned as infrastructure competing against VMware, Docker, Mesosphere with a small self-funded UK team. The market reached for containers instead.
- **Jini — Lease model** — *Primitive: lease decay · 1998.* Distributed resource registrations should decay rather than persist indefinitely. Correct insight; implemented as explicit `Lease` objects with `renew()` RPCs, a lease manager, and explicit cancellation.

### OSGi R&C and Paremus — what was kept and what was discarded

Mycelium keeps the R&C primitive and makes evaluation **continuous**. Capabilities appear and disappear at runtime. Requirements are re-evaluated against the current mesh state. Emergent groups form and dissolve dynamically. The resolver runs on every relevant KV change, not once at startup.

From Paremus specifically, Mycelium inherits the insight that the R&C graph should represent a *target state* that the system continuously converges toward — not a static snapshot consulted at deploy time. What Mycelium discards is the central reconciliation engine that computed those deltas. In Mycelium there is no target state graph held anywhere and no engine driving convergence. Each node independently evaluates its own membership against live KV state. Every TTL refresh is a reconciliation tick. Every capability advertisement is a node asserting its own current state. The reconciliation loop *is* the gossip mesh — distributed, coordinator-free, and degrading gracefully rather than failing catastrophically when any single component stops.

> **The key inversion.** In Paremus, the target state was held *above* the runtime in a central graph and pushed down into components. In Mycelium, the target state is *compiled into the runtime components themselves* — each node carries its own fragment as capability and requirement declarations. There is nothing external to converge toward. The mesh assembles the whole picture bottom-up from those fragments, and convergence emerges upward. The application's intended topology is not a document held somewhere; it is the aggregate of what every node declares itself to be.

### Jini leases — what was kept and what was discarded

Mycelium keeps the decay insight and discards the protocol. Capability KV entries carry a TTL. They evaporate unless the agent keeps re-advertising. There is no lease object, no renewal RPC, no lease manager. The act of remaining alive and active *is* the renewal — the agent walks the same path and the pheromone trail stays fresh. A dead node's capabilities simply fade.

### The proxy inversion — what was deliberately not kept

Jini's other defining idea was the **dynamic proxy**: a client looking up a service received not an address but a serialized object, supplied by the service itself, implementing the service interface. Inside that object, the service chose the transport, the serialization, the caching strategy — the client's lower layers were not infrastructure but *payload, shipped from above*. Note the direction: this is the exact inverse of emergence. Where emergence grows macro order out of micro interactions nobody specified, the proxy starts from the macro specification (the interface) and manufactures the micro realization to fit — top-down realization, in Ellis's sense of macro-level selection among micro-level possibilities. In Hayek's vocabulary: *taxis* injected into the transport layer, per service, per interaction.

Two structural consequences follow, and they explain more about Jini's fate than the usual "ahead of its time" account:

| Consequence | Why it follows |
|---|---|
| **Top-down plumbing requires a universal bottom** | To let the top configure the bottom, every participant must execute arbitrary supplied bottoms — for Jini, a JVM everywhere. Substrate independence at the interface was bought with total platform capture one level down. Bottom-up emergence has the opposite tolerance: a dumb, fixed, *shared* medium tolerates heterogeneity in everything above it. |
| **Fragmenting the medium forfeits fleet-level emergence** | Holland's model needs a shared medium — flooding, evaporation, anti-entropy, freshness are *properties of the medium*, and they are what higher layers compose into consensus, capability ecologies, and pressure-driven work distribution. N services shipping N private transports leave nothing common for anything to emerge *in*. The pairwise interaction is optimized; the ecology is starved. |

Mycelium therefore draws the line precisely: the wire format is frozen (`WIRE_VERSION` policy) and identical for every node; no service can reach down and re-plumb another participant's stack. The only downward path is the stigmergic one — pheromone fields constraining where locally-deciding agents mass — which is influence *through* the shared medium, never configuration *of* it. The v2 WASM-component milestone is, knowingly, Jini's proxy idea returning with a better universal bottom (sandboxed, capability-scoped, no platform capture): it ships *capability implementations into hosts*, and still refuses to ship *client-side plumbing into callers*. Jini's mistake was never mobile code — it was letting mobile code constitute the medium.

> **The Paremus lesson — library, not platform.** Positioning Service Fabric as runtime infrastructure placed it in direct competition with VMware, Docker, Mesosphere, and later Kubernetes. The commercial outcome had multiple causes — resource asymmetry, existing enterprise relationships, the weight of familiar mental models. Those are market realities, not architectural ones. The architectural lesson, however, stands independently: a substrate that requires a deployment slot competes for infrastructure ownership and inherits all the operational friction that comes with it. Mycelium draws the opposite conclusion from first principles — the substrate must be a library embedded in the caller's process. No daemon, no control plane, no installer, no orchestrator. The operator's existing infrastructure is irrelevant to Mycelium because Mycelium does not touch it. The library *is* the infrastructure, running inside the process that needs it.

## The "Strip the Ceremony" Heuristic

*Design Pattern*

Both OSGi and Jini solved real problems correctly at the conceptual level but carried too much implementation ceremony — explicit managers, lifecycle protocols, object handles. The pattern that produces better architecture:

| Prior art (ceremony) | Mycelium (substrate property) |
|---|---|
| Jini: `Lease.renew()` RPC + lease manager | TTL as natural evaporation — re-advertisement is renewal |
| OSGi: static bundle-install resolver | Continuous evaluation against live KV state |
| Explicit routing tables | Unconditional propagation + boundary admission |
| Explicit failure detection + deregistration | Pheromone trail evaporation on TTL expiry |
| Jini: lease manager + explicit cancellation | Tombstone write + TTL — fading is failure |
| Paremus: central reconciliation engine + target state graph | Gossip mesh — every node self-evaluates; reconciliation is emergent |

> **The three steps:** (1) identify the correct *concept* in the prior work; (2) ask what substrate *property* produces the same behaviour without an explicit protocol; (3) implement the property and let the behaviour emerge. When a proposed feature requires a manager, a coordinator, an explicit lifecycle protocol, or a renewal RPC, apply this heuristic before accepting it.

## The Pull Pattern: Linda Without the Coordinator

*Linda · pull vs push*

The sharpest application of the strip-the-ceremony heuristic is to the oldest coordination medium of all — Gelernter's tuple space (Linda, 1985). The classical critique is that the tuple space is itself a coordinator: a shared medium that must stay accessible, consistent, and available to every participant, so fault-tolerant variants (FT-Linda, JavaSpaces) had to bolt on replication and consistency protocols. That critique is half right, and separating the two halves is what recovers what Linda got right.

Two roles are entangled in the tuple space, and only one of them is the coordinator:

| Role of the tuple space | Is it the coordinator trap? |
|---|---|
| **Decision-maker** — allocating work, predicting who should do what | It plays this role in *no sense at all*. Workers `take` when ready; claiming work is the one coordination act that needs no knowledge of any other participant's state. There is nothing to predict, so nothing to be stale. |
| **Rendezvous point** — a meeting place on the data path | A capacity-and-availability concern, like any shared medium. Real, but ordinary — and *not* what the coordinator trap is about. FT-Linda and JavaSpaces struggled because they answered this second concern with designed-in consistency machinery: the ceremony. |

This is the push/pull distinction stated precisely. A *push* distributor must hold a model of every worker's state to route work — the model goes stale, and staleness is what drives misrouting and the audit burden. A *pull* rendezvous holds no such model: readiness is self-announced by the claim itself, so the variable that staleness would corrupt does not exist. The coordinator trap lives entirely in the first role. Strip it away and a rendezvous point remains — and a rendezvous point's availability is the kind of problem a coordinator-free substrate already solves.

One further strip-the-ceremony move deserves explicit notice, because the "Linda-style" label invites a wrong assumption: Mycelium's space keeps Linda's *generative decoupling* and *blocking pull*, but discards associative template matching. Classic Linda retrieves tuples by pattern (`in(("stage-b", ?id, ?data))`) over one flat bag — the matching engine is more ceremony attached to the rendezvous point. Here the space is **lane-addressed**: named per-stage FIFO lanes, opaque payloads, and an item's pipeline position is the lane it sits in. `complete()` is an atomic lane-to-lane move; a worker's only "filter" is choosing which lane to `take()` from. The per-lane depth counters this yields are themselves the pressure pheromone that fluid workers route on — the retrieval model and the stigmergy collapse into one mechanism.

### Where associative matching earns its keep

Stripping the matching engine was not a deletion — it was a relocation, and saying so precisely matters. The architecture keeps associative matching in two places, both on the *control plane*: the capability resolver's attribute filters and the signal boundary's receptor predicates are template matchers, re-evaluated continuously. What it refuses is matching on the *durable claim path*, where a matching engine un-buys every lane property at once: O(1) claims become scans, "depth" stops being well-defined so backpressure and the pressure pheromone vanish, and the one-record stage transition stops making sense. The working decomposition: **match to select a lane; pull from the lane.** Four workloads resist that decomposition, and they mark the lane model's boundary of validity:

| Residual workload | Why lanes break | Layering-consistent answer |
|---|---|---|
| **Fan-in joins** — claim an invoice *and* its matching PO | One lane per correlation key — degenerates to one item per lane | Keyed-exact-match `take`: still O(1), still lane-accounted; a small extension, not a different architecture |
| **Contract-net claiming** — multi-dimensional predicates living on the item | Lane-name encoding explodes combinatorially as predicate cardinality approaches item count | Per-item matching is irreducible here — companion-crate territory |
| **Blackboard working memory** — opportunistic reasoning over shared typed facts (LLM-agent scratchpads are this, reborn) | No stages exist: the flow topology is emergent per item | Flooding + boundaries already give propagation and triggering; the one missing primitive is competitive destructive *claim-by-predicate* with tuple-space exactly-once discipline |
| **Semantic claiming** — claim the task nearest my skill embedding | Similarity is not expressible as a lane name at all | A ranking concern for the selection edge — never inside the WAL'd claim path |

Each row passes the litmus tests the same way the tuple space did: the substrate is not changed; a higher-order concern composes on top of it. If a blackboard companion is ever built, it should be built exactly as the tuple space was — entirely on the public API, with the new claim primitive carrying the same WAL and in-flight discipline — so the constructive proof extends to associative workloads rather than being amended by them.

> **Constructive proof, not argument.** The `mycelium-tuple-space` companion crate rebuilds the most coordinator-shaped workload there is — work distribution — using *only* the substrate's public API, with no component that predicts any agent's state. The serving node is discovered by capability advertisement; its failure is detected by the same TTL evaporation that fades any pheromone; and a mirror promotes itself through the same emergent election as every other role in the system. Three results carry the weight: (i) 1,000 items across ten producers and ten workers delivered exactly once, no participant ever holding a view of another's load; (ii) kill the serving node with unacknowledged work live and a standby promotes itself and serves the survivors under their original identifiers — acknowledged items do not resurrect; (iii) the full put/take/complete/ack lifecycle drives across nodes through the HTTP gateway. The historical objection to tuple spaces is answered by construction: the rendezvous point's availability comes from substrate evaporation and emergent promotion, and its decision authority is zero because it makes no decisions. The pull pattern survives; only the coordinator-shaped infrastructure around it was ever wrong.

## Litmus Tests for Proposed Changes

*Evaluation Criteria*

Any significant proposed addition must be able to answer these three questions cleanly. If the answers require contortion, the proposal is probably wrong at the architectural level regardless of how useful it sounds.

1. **Substrate or higher-order concern?** Does this belong in Layer I (KV replication) or Layer II (signal mesh), or is it a higher-order pattern that should be built *using* those layers? If it is higher-order, it must use the substrate rather than modify it.
   *Fail: modifies signal propagation or KV replication*
2. **New primitive or composition?** Is this genuinely a new primitive that cannot be expressed as a composition of existing ones? New primitives raise the conceptual surface area for every user and contributor. If the behaviour can be achieved by composing `emit`, `advertise`, `resolve`, `propose`, or `set`, it should be.
   *Fail: adds a primitive achievable by composition*
3. **Protocol or substrate property?** Does this require explicit protocol machinery — a manager, a lifecycle, a renewal RPC, an explicit deregistration — that could instead be expressed as a substrate property? If yes, find the evaporation equivalent.
   *Fail: requires a manager, coordinator, or lifecycle RPC*

> **The informal check:** "Would Holland approve?" It compresses the whole framework into one question: does this emerge from simple rules interacting, or does it require a manager, a coordinator, or a lifecycle protocol? The latter almost always signals a layer violation or a misidentified primitive.

## Emergent Levels and Symmetry Breaking

*Anderson — More Is Different*

The litmus tests above are written as universal tests, but they are not — and saying so precisely matters, because the consensus layer would otherwise appear to fail test 03. The resolution comes from P. W. Anderson's *"More Is Different"* (Science, 1972): each level of complexity exhibits genuinely new laws that cannot be derived by averaging the level below. Emergent behaviours are not aggregates — they are *different*. Anderson's central mechanism is **symmetry breaking**: the laws of the lower level are symmetric, but particular configurations of the system break that symmetry and acquire properties the laws alone do not show.

Applied here: the substrate's laws are symmetric — every node identical, every rule local. As the system scales, particular *configurations* break that symmetry: a proposer, a quorum, a group with a roster. These are broken-symmetry *states*, not new laws. They are contingent and reversible — the proposer role dissolves when the ballot completes, the quorum's membership is whoever responded, the mandate evaporates on TTL. A *designed-in* coordinator is symmetry breaking baked into the laws themselves: permanent, privileged, load-bearing. Holland predicts the first kind will emerge at scale; the coordinator trap is the second kind. The litmus tests alone cannot distinguish them; level theory can.

The same pattern is the oldest result in the economics this document already leans on: Coase's firm (1937). The market — a pure signal/boundary substrate — spontaneously generates firms, literal coordinators ("islands of conscious power"), wherever transaction costs make coordination pay. The substrate disciplines them: when internal coordination costs exceed market costs, the firm shrinks or dies. Mycelium's consensus coalition is exactly this — an emergent coordinator that exists only while it pays and dissolves when the decision completes. **Complex societies do need coordinators; they emerge — they are not the starting point.** The emergence of coordination is a prediction of the coordinator-free substrate, not an embarrassment to it.

### The corrected litmus

Anderson cuts both ways, and the second edge is what keeps this framework falsifiable rather than a license for special pleading. Chemistry transcends physics conceptually, but no molecule violates conservation of energy: **emergent levels add laws; they never get to violate the laws below.** Without that constraint, "it's a new emergent level" would exempt any feature from any test.

| An emergent layer (e.g. Layer III) may | It may never |
|---|---|
| Add laws of its own level: ballots, quorums, roles, listeners, an explicit lifecycle — protocol machinery is what protocols are made of | Require the substrate to withhold or condition propagation |
| Hold transient broken-symmetry states: a proposer, a leader, a coalition | Demand a permanently privileged node, or roles that escape evaporation |
| Express its state as ordinary keys and signals, riding the substrate | Make the substrate aware of, or dependent on, the layer above it |

Litmus test 03 is therefore a *substrate-level law*: binding absolutely on Layers I and II, binding on emergent layers only in the "may never" form. Layer III passes the corrected test verifiably: consensus frames are ordinary signals forwarded unconditionally, the namespace is just keys, nothing in Layers I–II blocks on a ballot, and every role dissolves.

> **The inverted dependency — and why it is not a defect.** The constraint runs one way only. The substrate retains the right to violate Layer III's laws: namespace ownership is convention, and a rogue write to `consensus/committed/` would be accepted by LWW like any other. That asymmetry is the *definition* of which level is foundational. The correct response is detection above, ignorance below: the consensus listener's commit-conflict tripwire (`SystemStats::commit_conflicts`) refuses to endorse a conflicting commit and makes the violation legible — but no write guard is added to the substrate, because teaching Layer I a Layer III law would invert the dependency that makes Layer I the foundation. Commitments in Mycelium are promise-strength, not mechanism-strength — which is the only honest strength a coordinator-free system can offer, and Promise Theory's bound exactly.

> **Mandate TTL applies to decisions too.** If Layer III's roles must evaporate, so should its outputs: a permanent commitment is an eternal mandate, which §10 identifies as a failure mode. Epoch-leased commitments (`ConsensusConfig::committed_lease_secs`) apply the read-side evaporation convention to committed values themselves — an expired lease reads as not-committed and the slot reopens; renewal is a fresh quorum round, not a renewal RPC. Permanence remains available for ledger-shaped uses; decay is the default posture the philosophy recommends for leadership and configuration decisions.

### Legible emergence — reading the emergent level

If coordinators are emergent broken-symmetry states rather than designed-in machinery, a practical worry follows: an emergent level with no coordinator has no console — nowhere to ask "is the fleet healthy?" Anderson's own point answers it. The emergent level has its own laws, so it has its own *observables*: opacity storms, a group whose roster has left its governed band, a capability with no live provider, back-pressure oscillating. These are properties of the higher level, invisible in any single node's local rules — yet every node holds the same gossiped substrate the level emerged from, so **every node can compute the diagnosis itself**. There is no collector to deploy and no privileged observer to make load-bearing; the diagnosis is as coordinator-free as the thing it diagnoses. This is *legible emergence*: the fleet is diagnosable without a fleet manager.

The honesty is in the qualifier. A node diagnoses from *its own* view, which may be partial — so each diagnosis carries a view-confidence caveat (how many peers it is hearing, how stale its inputs are), and at convergence independent nodes agree while a *disagreement* between them is itself the signal that one is partitioned. A per-node estimate labelled as such, not a false single source of truth — the same discipline this document applies to leases and mandates, turned on observability. The taxonomy and detection tiers are in [the legible-emergence design record](design/legible-emergence-taxonomy.md); the operator runbook is [diagnostics.md](operations/diagnostics.md).

## What This Architecture Is Not

*Boundary Conditions*

Understanding the boundaries matters as much as understanding the model.

**Not a message broker.** No central broker, no topic registry, no persistent queue. Signals are ephemeral. A node that misses a signal misses it — intentionally. Durable delivery is higher-order, built on KV or consensus.

**Not a service mesh.** No sidecar, no control plane, no external certificate authority. mTLS is peer-to-peer; the Ed25519 keypair is the node's identity. The mesh is the library.

**Not an actor framework.** Actors have explicit addresses and explicit lifecycle management. Mycelium nodes have capabilities and boundaries. Topology emerges from capability matching rather than being managed explicitly.

**Not a platform.** No daemon, no orchestrator, no installer, no control plane. A Rust crate embedded in the process that needs it. The operator's existing infrastructure is irrelevant — Mycelium does not touch it.

> **On consistency:** the KV layer is last-write-wins with HLC causal ordering. This is a deliberate choice, not a concession. For coordination requiring strong agreement, the consensus layer exists. The two tiers are complementary, not redundant — and the choice between them is always the caller's.

## The Novel Synthesis

*Sum > Parts*

No single element of Mycelium is without precedent. The novelty is in the synthesis — and in recognising that ideas from different decades and different domains were all pointing at the same underlying model.

**From prior art to synthesis**

- **Holland (2012)** → Signal/boundary substrate; stigmergy; emergent coalition formation
- **OSGi R&C (2003+)** → Capability/requirement matching primitive — made continuous, not static
- **Paremus (2010–15)** → Proof that runtime continuous resolution works — and the positioning lesson
- **Jini (1998)** → Lease insight re-expressed as TTL evaporation — no protocol, no manager
- **Burgess (1998+)** → Promise Theory: agents promise only their own behaviour; coordination emerges from voluntary promise observation — a third independent derivation of the same substrate model
- **Synthesis** → Single embeddable Rust library — no broker, no coordinator, no external dependencies for the core substrate

The sum is more than the parts because the parts were each solving the same underlying problem from different directions. Holland described the model theoretically. OSGi and Jini approached it pragmatically but with too much ceremony. Paremus proved runtime resolution worked but fought the wrong competitive battle. Burgess formalised why the coordinator-free approach is the only approach that can make honest promises. Mycelium is what you get when you take the concepts, strip the ceremony, position it as a library rather than a platform, and let the behaviour emerge from substrate properties — as Holland, Hayek, and Burgess would all independently have wanted.

## The Hayek Parallel

*Political Economy*

The coordinator trap is not a new discovery. Friedrich Hayek described it in 1945 — for economies. In *"The Use of Knowledge in Society"* (American Economic Review, 1945), Hayek argued that the central planning problem is not computational — it is epistemic. No central planner can possess the distributed, local, tacit knowledge held by individual market participants. Prices are not just numbers; they are signals that aggregate and propagate dispersed knowledge without anyone needing to understand the whole. Attempts to replace this with a central planning apparatus fail not because planners are incompetent but because the knowledge required for correct decisions is **structurally inaccessible** from any central point.

| Planned economy | Mediated hierarchy |
|---|---|
| Central planner holds target state | Coordinator holds coordination state |
| Agents report up; planner decides; commands issue down | Agents submit output; mediator synthesises; directives broadcast |
| Planner cannot possess all local knowledge | Coordinator cannot possess domain expertise for every agent |
| Knowledge aggregation fails as economy grows | Audit burden grows linearly with agent population |
| Plan drifts from reality | Context lost on coordinator restart |

| Market economy | Mycelium |
|---|---|
| Prices propagate local knowledge unconditionally | Signals propagate unconditionally through the mesh |
| Firms act on prices that match their position | Agents act on signals that match their boundary |
| No central knowledge required | No coordinator required |
| Failure is local — firms exit, market continues | Node failure is local — TTL evaporates, mesh continues |
| Emergent order from local interactions | Emergent coordination from boundary admission |

Hayek's market is a signal/boundary system. He just did not have Holland's vocabulary.

The intuition that a sufficiently intelligent coordinator — with enough information and computing power — could outperform the distributed system is seductive precisely because it *feels* like it should work. The appearance of control is reassuring even when it is structurally impossible. Mediated agent hierarchies appeal for exactly the same reason: the CognitiveEngine looks like it is in control. The appearance of coordination is reassuring even as the audit burden accumulates, context is lost on every restart, and agents are reduced to workers awaiting instructions from above.

> The insight is the same in all three cases — Hayek's economics (1945), Holland's complex systems (2012), Mycelium's distributed agents: **distributed local knowledge, expressed through signals and boundaries, produces emergent order that no coordinator can match.** Mark Burgess's Promise Theory, derived independently from distributed systems engineering, arrives at the same conclusion from a fourth direction.

## The Promise Theory Convergence

*Burgess · A Fourth Derivation*

Mark Burgess developed Promise Theory beginning in the late 1990s, initially to formalise the semantics of configuration management systems — specifically CFEngine, which he built. The central claim is precise: autonomous agents can only make promises about their *own* behaviour, never about the behaviour of others. A promise is unilateral. Coordination is not designed in; it emerges when agents voluntarily observe each other's promises and adjust their own behaviour in response.

This is a third independent derivation of the same substrate model that Holland derived from complex adaptive systems biology and Hayek derived from economic epistemics. Burgess arrived there from distributed systems engineering, with no reference to either. The convergence across three unconnected intellectual traditions is not coincidence — it reflects a structural property of the problem.

### How Promise Theory maps to Mycelium's layers

| Layer | Promise Theory reading | Match quality |
|---|---|---|
| **Layer I — KV store** | `advertise_capability()` is a body promise: "I promise I can do X." The TTL is the promise's validity period — if not renewed, the promise lapses. Gossip is the propagation mechanism by which promises become observable to others. LWW resolution is the deterministic tie-breaking rule for conflicting promises about the same key — no coordinator, just a consistent rule every agent applies locally. | Exact |
| **Layer II — Signal mesh** | Receptor-based routing is agents making *use promises*: "I promise to act on signals of type X within scope Y." Emergent group membership is agents making *obligation promises*: "I promise to behave as a member of group G if I hold capabilities C." No group coordinator assigns membership — agents self-select by evaluating their own promises against the group definition. | Exact |
| **Layer III — Consensus** | Does not map cleanly. Consensus requires agents to make binding commitments about what a quorum will accept — which is a promise about other agents' behaviour. PT says this is categorically impossible to guarantee. A distributed lock by definition creates a global mutual exclusion constraint that relies on all participants keeping the same obligation promise; if any agent defects, the invariant breaks. | Partial — see below |

### How Promise Theory handles coordination problems

Burgess does not abandon coordination — he reframes it. For each class of coordination problem that consensus typically addresses, PT offers a weaker but more honest substitute:

- **Distributed locks → convergent obligation promises.** Rather than "atomically prevent others from accessing X," agents publish an obligation promise ("I will not modify X while this key exists") and observe whether others have done the same. Exclusivity is emergent from voluntary compliance — it is only as strong as the promise-keeping of participants, which PT accepts as the honest bound.
- **Point-in-time invariants → eventual semantic invariants.** "All agents agree on value X at time T" is physically unachievable (information has propagation latency). The honest form is "all agents will eventually converge to value X." LWW gossip with HLCs is the canonical implementation — exactly Mycelium's Layer I.
- **Leader election → voluntary singleton.** Rather than a quorum vote, an agent that wants to be the unique handler of a resource publishes a body promise ("I am handling resource R") with a TTL. Other agents observe and defer. If two agents simultaneously publish competing promises, a pre-specified deterministic rule (e.g., lexicographic node ID ordering) resolves locally on observation — no ballot, no proposer. This is precisely what `suggest_leader` does.
- **Synchronized time → spacetime semantics.** In Burgess's spacetime model of PT, promises propagate at finite speed. There is no "global now." An agent acts when information arrives within its causal neighbourhood — which is exactly what HLCs (Hybrid Logical Clocks) track. Causal ordering, not wall-clock synchrony, is the right primitive.

### Where the map tears — and why

Promise Theory's constraint on Layer III is real, but the reason matters: it is not that Burgess examined emergent consensus and rejected it. It is that PT's formal calculus was designed for a specific domain — configuration management of autonomous agents at a scale where agents remain atomic. The formalism has no machinery for what happens when CAS scale in size and complexity: the emergence of higher-order coordination structures from the local rules themselves.

PT treats agents as permanent primitives. It has no model for metaagents, for coalitions that form and dissolve, for higher-order agents whose behaviour emerges from the interactions of constituent agents below. This is not a deliberate philosophical rejection — it is a scope limitation. The mathematics PT uses simply does not reach to that level of the problem.

### Where Holland goes further

Holland's CAS theory explicitly predicts that complex systems, as they scale, will develop emergent coordination structures. This is not a failure of the local-rule principle — it is a *consequence* of it. When local rules are applied by enough agents across enough interactions, higher-order agents emerge: coalitions with collective behaviour, metaagents that act as units, emergent hierarchies that were not designed but were selected for by the system's adaptive dynamics. Holland calls these **building blocks composing into higher-level building blocks** — recursively, without limit.

From this perspective, Mycelium's Layer III consensus does not contradict Holland's framework — it instantiates it. The quorum that forms around a proposal is an emergent coalition: a set of nodes that, by applying their local rules (respond to a ballot, emit an acceptance), temporarily act as a metaagent with collective behaviour. The proposer is not a fixed coordinator appointed from outside; it is whoever initiates a proposal — a transient role that dissolves when the ballot completes. The quorum's membership is dynamic and determined by who responds, not pre-assigned.

> **The distinction that matters:** a *designed-in* coordinator violates Holland's principles because it assumes the coordinator possesses knowledge the local rules do not. An *emergent* consensus structure — one that forms from local rules, has no fixed membership, and dissolves after each decision — is exactly what CAS theory predicts will appear at scale. Mycelium's epidemic consensus is the second kind. Promise Theory's formalism cannot distinguish between them; Holland's can.

This is why Holland is the deeper and more comprehensive foundation. His framework encompasses all three layers: the gossip substrate and signal mesh at Layers I and II directly, and the emergent consensus coalitions of Layer III as a predicted higher-order property of the same CAS dynamics. Burgess provides rigorous validation of Layers I and II, and the four PT coordination substitutes (convergent obligations, eventual invariants, voluntary singleton, spacetime semantics) are valuable design heuristics — but PT does not have the theoretical reach to account for what the full system produces when it scales.

### The convergence — and its scope

Each tradition arrives independently at the coordinator-free substrate. They differ in how far they can follow the system as it scales.

| Tradition | Derivation | Layers I + II | Layer III |
|---|---|---|---|
| **Holland (2012)** — *Complex Adaptive Systems* | Biological — signals, boundaries, building blocks composing into metaagents | Full | Full — emergent consensus coalitions are a predicted CAS outcome at scale |
| **Hayek (1945)** — *Political Economy* | Economic — dispersed local knowledge, price signals as coordination mechanism | Full | Silent — Hayek's model operates at the level of markets, not transaction semantics |
| **Burgess (1998+)** — *Distributed Systems* | Engineering formalism — unilateral promises, convergent obligation semantics | Full | Partial — PT rejects consensus as designed-in coordination; its formalism has no model for emergent consensus as a CAS outcome |
| **SWIM / gossip literature** — *Systems Research* | Empirical — epidemic dissemination, failure detection without masters | Full | Out of scope — gossip literature does not address serialisable operations |

> The convergence is real and the architectural correctness claim stands — but Holland is the load-bearing foundation, not one of four equals. Hayek, Burgess, and the gossip literature each independently validate the coordinator-free substrate and strengthen the claim. Only Holland's framework is general enough to follow the system all the way to the emergent coordination structures that complexity produces at scale. That is why Mycelium started from *Signals and Boundaries*, not from the others.

## The Subsidiarity Principle

*Political Philosophy · Societal Systems*

The coordinator-free principle is not unique to distributed computing. Political philosophy and sociology have been working through the same question — at what scale should decisions be made locally, and what emergent coordination structures does scale necessarily produce? — for considerably longer. The conclusion they reach is the same.

### Subsidiarity

The formal political principle is **subsidiarity**: decisions should be made at the lowest level competent to make them, and authority should scale upward only when the local level genuinely cannot resolve the problem. The principle appears in Catholic social teaching (Quadragesimo Anno, 1931), in federalist political theory, and in the Treaty of Maastricht (1992) as a constitutional constraint on EU governance — the Union acts "only if and insofar as the objectives of the proposed action cannot be sufficiently achieved by the Member States."

The critical word is *emergent*. The higher coordination layer is not designed in from the start — it is invoked when and because the lower level proves insufficient. The federal parliament does not replace the village council; it handles the class of problems the village council cannot: common defence, cross-boundary commerce, monetary stability. Outside that scope, the village council remains sovereign.

| Societal level | Coordination mechanism | Mycelium layer |
|---|---|---|
| Village / small community | Informal gossip, reputation, voluntary reciprocity. No formal institutions needed at Dunbar-scale (~150). Local knowledge is sufficient for all decisions. | Layers I + II — gossip KV, signal mesh, capability resolution. Sufficient for small-to-medium clusters. No consensus invocation required. |
| City / region | Emergent specialisation: guilds, councils, courts. Roles differentiate as the scale of interaction exceeds any individual's knowledge. Coordination is still local by domain — the trade council handles trade, the water board handles water. | Emergent capability groups, wiring, demand pressure. Specialised nodes form functional coalitions. Capability discovery handles routing without a central registry. |
| Nation / federation | Federal institutions handling cross-boundary coordination problems that local governance cannot resolve alone. Invoked for the specific class of problems requiring collective binding commitment. Local governance remains primary. | Layer III — consensus, distributed locks, leader election. Invoked for operations requiring linearisable cross-node agreement. Opt-in. Does not replace Layers I and II; handles only what they cannot. |

### Ostrom and polycentric governance

Elinor Ostrom (Nobel Prize in Economics, 2009) worked out the most rigorous account of how communities govern shared resources without external coordinators. Her work on governing the commons overturned the orthodox assumption that commons are inevitably over-exploited without either private ownership or state control. The actual finding: communities develop their own governance institutions — boundary rules, conflict resolution mechanisms, collective-choice arrangements, monitoring — that sustain the commons indefinitely. The key condition: the institutions emerge from below, adapted to local conditions, rather than being imposed from outside.

Ostrom called the resulting structure **polycentric governance**: multiple overlapping governance centres at different scales, each locally sovereign, able to coordinate upward when necessary. No single centre is in control. Each level handles the class of problem appropriate to its scale. The boundaries between levels are permeable and negotiated, not fixed by statute.

Her eight design principles for commons governance that sustain themselves map almost directly to Mycelium's structural properties:

| Ostrom design principle | Mycelium property |
|---|---|
| Clearly defined boundaries — who belongs, what is governed | TTLs evaporate membership; capability filters define group admission; scope boundaries constrain signal propagation |
| Rules adapted to local conditions — not universally imposed | Each node's capability set determines its group membership and routing; no global rule-setter; behaviour emerges from local configuration |
| Collective-choice arrangements — those affected can modify the rules | Consensus layer (`group_propose`, `cluster_propose`) provides the mechanism for collective binding decisions when needed |
| Monitoring — behaviour is observable to participants | `/stats`, `/ready`, `/metrics`, audit trail, Prometheus scrape endpoint; cluster state is visible to any participant |
| Graduated sanctions — violations meet proportionate response | Opacity/load mechanism: nodes that exceed load or fail liveness checks are marked opaque and excluded from routing — without being removed from the cluster |
| Conflict-resolution mechanisms — low-cost local dispute resolution | LWW resolution for concurrent writes (cheap, local); consensus for operations requiring explicit agreement (opt-in, higher cost) |
| Recognition of the right to organise — no external authority overrides local governance | No external coordinator required to form, modify, or dissolve a group; emergent groups self-assemble from capability declarations |
| Nested enterprises — governance structures nest within larger structures | The three-layer architecture: local KV, group signal mesh, cross-group consensus. Each layer handles its own class of problem; higher layers are invoked, not imposed. |

### The scale-transition and where it goes wrong

In human societies as in distributed systems, informal coordination works until scale exceeds the capacity of local knowledge. Institutions emerge not because someone designed them in but because the needs of scale produce them. The institution — the council, the court, the federal agency — is the emergent higher-order structure that CAS theory predicts will appear. It is selected for because without it the system cannot resolve the coordination problem.

Political philosophy also identifies the failure mode. When emergent coordination layers expand beyond their necessary scope — when bureaucracies grow to serve their own perpetuation rather than the coordination problem that created them — the system acquires all the pathologies of the designed-in coordinator. Local knowledge is overridden by central policy. Diversity is suppressed in favour of uniformity. The coordination layer that emerged as a servant becomes a principal.

Mancur Olson worked out the mechanism for why this expansion happens in *The Logic of Collective Action* (1965) and *The Rise and Decline of Nations* (1982). The argument is precise: a small group that gains enormously from controlling a coordination mechanism will always outspend and out-organise the large number of agents who each lose a small amount from that control. Diffuse losers cannot coordinate to resist concentrated winners — the coordination required to resist capture is itself a collective action problem that the diffuse majority cannot solve. The result: emergent coordination layers tend to be captured by the interests with the most to gain from controlling them, and once captured they write the rules that prevent their own dissolution. Ostrom shows what correct decentralised governance looks like; Olson explains why it tends to be displaced even when it works.

Together, Ostrom and Olson define the complete design problem. Building a coordinator-free substrate with correct emergent governance (Ostrom's prescription) is necessary but not sufficient. The capture-resistance of the Layer III coordination mechanisms — whether their control can be acquired cheaply by concentrated interests — determines whether the design survives contact with Olson dynamics at scale.

> The subsidiarity principle is the ongoing corrective: **continuously ask whether the higher layer is invoked because it is genuinely necessary, or because it has become self-perpetuating.** In Mycelium this is structural: Layer III is opt-in and stateless between operations. It cannot expand to claim the substrate because the substrate does not depend on it. The architecture enforces subsidiarity by construction.

### The mandate TTL principle — Rojava and Democratic Confederalism

Ostrom and Olson together define the design problem at the level of the substrate: build coordinator-free governance that resists external capture. But there is a further failure mode that neither directly addresses: *internal capture*, where the agents who operate Layer III coordination mechanisms accumulate incumbency and progressively reconfigure the mechanism to perpetuate their own roles — without any external actor involved. Properties 1–5 cannot prevent this. The mechanism requires a sixth structural property: the mandate TTL.

Murray Bookchin developed the intellectual foundation in *The Ecology of Freedom* (1982) and *Urbanization Without Cities* (1992, reissued as *From Urbanization to Cities*). His framework of **libertarian municipalism** places the commune — a Dunbar-scale community of roughly 30 to 400 households — as the irreducible base unit of social organisation. Decisions at commune scale; delegates federate upward with *specific mandates* for defined problem classes, not general authority. Coordination roles carry explicit terms; when the term expires, the role is torn down and reconstituted from scratch, not merely re-elected. Cooling-off periods prevent the same person from immediately re-occupying the same role.

Abdullah Öcalan applied Bookchin's framework to create **Democratic Confederalism**, the governance model of the Rojava cantons in northern Syria. Implemented in practice since 2012 under active adversarial pressure — surrounded by hostile state actors, an active civil war, and an international blockade — the cantonal structure is one of the few real-world stress tests of coordinator-free polycentric governance at municipal scale. It has operated continuously for over a decade. The key structural properties in practice: co-presidency roles (ensuring no single individual can hold a role), fixed terms with mandatory rotation, recall mechanisms allowing communes to revoke a delegate's mandate at any time, and deliberate dissolution of coordination structures at term expiry rather than continuation by default.

The insight this adds to the Ostrom/Olson picture: Olson dynamics operate on coordination mechanisms from the *outside* (concentrated interests capturing the mechanism). The incumbency accumulation problem operates from the *inside* (role occupants capturing the mechanism through tenure). The mandate TTL is the structural response to the inside problem. It bounds the informational, relational, and normative advantages that incumbency accumulates, resetting each of them at every dissolution.

In Mycelium, this principle is implemented structurally rather than by social convention. A `leader_election` result is a gossip KV entry with a TTL. When the TTL expires, the role dissolves and the substrate returns to the same state as before the election. No persistent coordination authority exists between operations. The next election starts fresh. No agent can accumulate incumbency in a role whose existence is bounded by a KV TTL that no single agent controls. Rojava provides the governance proof-of-concept; Mycelium's stateless Layer III encodes it as a technical invariant.

### Epistemic symmetry and the coordination class

Property 6 dissolves the formal role. It does not dissolve two knowledge asymmetries that survive mandate rotation and reproduce the same structural effect as incumbency even when the formal constraint is scrupulously observed.

The first is **data locality**. The gossip substrate replicates values; it does not replicate the causal reasoning that produced them. A prior role occupant has internalized the history of how the current state arrived at its present form — the sequence of decisions, the rejected alternatives, the context behind each entry. A fresh occupant inherits the outcome without the reasoning, and will tend to defer to or reconstruct the prior framing simply because it is the only interpretation already embedded in the state. The data asymmetry makes the prior occupant's model the default even after their formal authority has ended.

The second is the **coordination class**. Meta-knowledge of the coordination game — procedural shortcuts, strategic framing opportunities, coalition dynamics under ballot conditions — is tacit: it cannot be transferred by documentation and is acquired only through direct participation. The mandate TTL moves the specific individual out of the role; the meta-knowledge advantage persists through informal networks and reputation. Critically, the electorate's deficit compounds the effect: agents who do not understand the coordination game select based on the *appearance* of competence, which systematically favours prior coordination participants regardless of the TTL's formal constraints. The same class cycles through roles even when no individual holds the same role twice.

This points directly at a structural implication for human societies: the more transparent and universal the education system is — the more it distributes civic meta-knowledge broadly rather than concentrating it in a schooled class — the more resistant the political substrate becomes to coordination-class entrenchment. An open education system is not merely a social good; it is a substrate property. Bookchin's argument is precisely this: the commune is both governance structure and educational institution, because without the second, the first is eventually captured from within by the subset of the population that understands how it works. Dewey arrived at the same conclusion independently in *Democracy and Education* (1916): democracy requires educated citizens not as a nice-to-have but as a structural necessity for the mechanism to function as designed.

In Mycelium terms, this is **Property 7 — Epistemic Symmetry**: the meta-knowledge required to participate in Layer III must be minimised (protocol simplicity), universally accessible (causal state history replicated through the WAL, not just current values), and developed through ordinary participation (Layers I and II experience builds the substrate familiarity that Layer III requires). A substrate satisfying Properties 1–6 but not Property 7 produces a structurally entrenched coordination class: formally rotating, substantively permanent.

> **The complete failure-mode picture.** Failure Mode I: coordinator-based design from the start — epistemic collapse (Coordinator Trap). Failure Mode II: correct substrate, Property 5 absent — external capture (Olson dynamics). Failure Mode III: correct substrate, Property 6 absent — internal capture (incumbency accumulation). Failure Mode IV: correct substrate, Property 7 absent — structural class entrenchment (data locality and meta-game advantage survive rotation). Each requires a distinct structural remedy; none of the four can substitute for another.

The political analogy is not decorative. Subsidiarity, polycentric governance, the Ostrom design principles, and the mandate TTL are the result of decades of empirical study of what makes distributed coordination sustainable at scale. They converge with Holland, Hayek, and Burgess because they are studying the same underlying problem in a different domain. The correct answer is the same: local self-determination wherever possible; emergent consensus structures where scale requires them; the higher layer as servant, not master; and the servant's mandate explicitly limited in time.

## Further Reading

- **John Holland — Signals and Boundaries: Building Blocks for Complex Adaptive Systems.** MIT Press, 2012 — the primary theoretical reference; foundational for the signal/boundary substrate model
- **John Holland — Complexity: A Very Short Introduction.** Oxford University Press, 2014 — tighter distillation of the same ideas; the best entry point
- **Marco Dorigo & Thomas Stützle — Ant Colony Optimization.** MIT Press, 2004 — stigmergy and pheromone trail mechanics in depth; the biological basis for opacity composition
- **OSGi Alliance — OSGi Core Release 8 Specification, Chapter 27: Capabilities and Requirements.** The formal specification of the R&C model Mycelium coopts for continuous runtime resolution
- **Jim Waldo et al. — Jini Architectural Overview.** Sun Microsystems Technical Report, January 1999 — founding white paper; the lease model origin
- **Ken Arnold, Bryan O'Sullivan, Robert Scheifler, Jim Waldo, Ann Wollrath — The Jini Specification.** Addison-Wesley, 1999 — full treatment of the lease model; Chapter 4 for the core protocol
- **Paremus Service Fabric.** Runtime OSGi Requirements & Capabilities resolution (2010–2015) — the direct conceptual predecessor to Mycelium's continuous capability resolver; proved the concept a decade before the agent infrastructure market was ready for it
- **F. A. Hayek — "The Use of Knowledge in Society".** American Economic Review, 35(4), September 1945 — the epistemic argument against central coordination; Hayek's price mechanism is a signal/boundary system; the coordinator trap described for economies 67 years before Holland formalised it for complex systems
- **Elinor Ostrom — "Governing the Commons: The Evolution of Institutions for Collective Action".** Cambridge University Press, 1990 — the empirical foundation for polycentric governance; Chapter 3 for the design principles; the central finding is that communities sustain common-pool resources through bottom-up institutional emergence, not external coordination; directly parallels the CAS and subsidiarity arguments
- **Mancur Olson — "The Logic of Collective Action".** Harvard University Press, 1965 — the formal mechanism for why decentralised systems get captured: concentrated interests always outspend diffuse ones; read alongside Ostrom as the complementary pair — Ostrom shows what works, Olson shows why it tends to be displaced even when it works
- **Mancur Olson — "The Rise and Decline of Nations".** Yale University Press, 1982 — the long-run application: distributional coalitions accumulate in stable societies and progressively capture coordination mechanisms; the empirical case for why epistemically correct substrate designs require capture-resistance properties, not just epistemic correctness
- **Murray Bookchin — "The Ecology of Freedom: The Emergence and Dissolution of Hierarchy".** Cheshire Books, 1982; revised edition AK Press, 2005 — the foundational text for libertarian municipalism; the commune as irreducible base unit; coordinator-free polycentric governance traced through ecology, social history, and political theory; read as the political complement to Holland's CAS theory — both arrive independently at local self-organisation as the structurally correct answer
- **Abdullah Öcalan — "Democratic Confederalism".** International Initiative, 2011 — the direct source of the Rojava governance model; argues for commune-level self-determination federated through mandate-constrained delegation; specific structural account of how mandate TTL — fixed terms with active dissolution and cooling-off — prevents Layer III capture by its own operators; the Rojava cantons have operated this model under adversarial conditions since 2012
- **John Dewey — "Democracy and Education".** Macmillan, 1916 — the structural argument that democracy requires educated citizens not as an ancillary good but as a design requirement: without broadly distributed civic competence, the coordination mechanisms of self-governance are captured by those who understand them; independently derives Property 7 (epistemic symmetry) from educational philosophy decades before the governance and computing literatures articulate it
- **Mark Burgess — "Promise Theory: Principles and Applications".** XAB Press, 2nd ed. 2018 — the full formal treatment; start with Chapter 2 (autonomous agents) and Chapter 5 (convergence semantics); the coordination-without-coordinators argument is in Chapter 7
- **Mark Burgess — "In Search of Certainty: The Science of Our Information Infrastructure".** O'Reilly, 2013 — the accessible entry point; Chapter 6 ("Promises") is the clearest single-chapter account of why unilateral promises produce more robust distributed systems than coordination-based designs; explicitly discusses the failure modes of consensus under partition

---

*mycelium · Architectural Philosophy · 2026-06-07 · Holland · OSGi · Paremus · Jini · Hayek · Burgess · Ostrom · Olson · Bookchin · Öcalan — stripped of ceremony · expressed as substrate properties*
