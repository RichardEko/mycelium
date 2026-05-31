# The Coordinator Trap: Why Mediated Multi-Agent Architectures Cannot Scale and a Substrate-Based Alternative

**Authors:** Dr. Richard Nicholson  
**Target venue:** AAMAS 2027 — International Conference on Autonomous Agents and Multi-Agent Systems  
**Status:** First draft — 2026-05-28

---

## Abstract

Multi-agent coordination systems are converging on two dominant architectural patterns: mediated hierarchies, in which a central engine aggregates agent outputs, decides, and issues commands; and registry-based discovery, in which a global index brokers introductions between agents. Both patterns introduce a coordinator — by different names — that becomes a bottleneck, a single point of failure, and an unbounded audit obligation as agent populations grow. We demonstrate by construction that these failure modes are not implementation deficiencies but structural consequences of the coordinator assumption itself. Drawing on Holland's framework for Complex Adaptive Systems, we propose an alternative in which coordination emerges from substrate properties rather than explicit protocols. We present Mycelium, a working implementation of this model as an embeddable Rust library, and show how its three-layer architecture — gossip KV store, signal mesh with boundary admission, and epidemic consensus — structurally addresses each failure mode rather than ameliorating it.
---

## 1. Introduction

The deployment of autonomous AI agent fleets is accelerating faster than the coordination infrastructure beneath them can reliably support. The academic literature is beginning to document the consequences. Silo-Bench [CITE-SILOBENCH], a 2025 empirical study of distributed multi-agent LLM systems, finds that "coordination overhead compounds with scale, eventually eliminating parallelization gains entirely" — adding more agents actively reduces system efficiency beyond a threshold population. A 2025 survey of multi-agent coordination architectures [CITE-MAS-SURVEY] concludes that the dominant paradigm — centralised orchestrators with predefined agent topologies — "introduces single points of failure and scalability bottlenecks" that are structural rather than incidental. The RAPS framework paper [CITE-RAPS] identifies predefined communication topologies as a fundamental architectural constraint, arguing that the community's reliance on manual orchestration "struggles to generalise across shifting task distributions" and represents an unresolved scalability dilemma. Carvalho [CITE-CARVALHO] observes that modern AI orchestration frameworks are independently rediscovering coordination solutions from the 1980s and 1990s without engaging with that literature — repeating the same architectural mistakes that were documented and, in some cases, partially solved forty years ago.

These academic findings are confirmed in practitioner experience. Valenti [CITE-SIGNAL-NOISE] documents an unbounded audit obligation arising from AI-mediated agent coordination at a Cisco-internal deployment (introduced in §2.1): all artifacts, regardless of the effort or reasoning behind them, arrive with identical surface polish, making quality assessment unsustainable at scale. She writes: *"The three-hour issue and the five-second issue have the same voice, the same structure, the same length."* In a companion account [CITE-CONTEXT-PROBLEM], the same author documents agents losing all state between sessions, restarting from zero, and re-litigating decisions already settled — and an attempted fix that introduces a shared knowledge graph that drifts — exactly the coordinator-state failure mode that Section 4 will dissect.

We argue that these failure modes — documented independently by academic measurement and practitioner experience — are not bugs in specific implementations. They are structural consequences of a single shared assumption: that a designated component must hold authoritative state about the agent population and through which coordination passes. We call this the *coordinator assumption*, and we show that any architecture built on it must exhibit these failure modes regardless of how carefully it is engineered.

This paper makes four contributions:

1. **A historical account** of how the coordinator assumption has persisted across fifty years of distributed computing research — from actor models to blackboard systems to modern LLM orchestration frameworks — and why no prior system eliminated it.
2. **A precise causal account** of how the coordinator assumption produces each documented failure mode, through an agent-theoretic lens.
3. **A theoretical foundation** for coordinator-free multi-agent coordination grounded in Holland's signal/boundary model for Complex Adaptive Systems [CITE-HOLLAND].
4. **Mycelium** — a working implementation of that model as an embeddable Rust library — demonstrating that each failure mode is structurally prevented or structurally highly improbable in the coordinator-free substrate.

The remainder of this paper is organised as follows. Section 2 describes the two dominant contemporary expressions. Section 3 traces the history of the coordinator assumption across five decades. Section 4 traces the causal chain from mediated hierarchy to each observed failure mode. Section 5 presents Holland's theoretical framework. Section 6 situates Mycelium within the relevant prior art. Section 7 presents the Mycelium architecture layer by layer. Section 8 presents evaluation. Section 9 discusses implications and future work.

---

## 2. Background: The Two Dominant Patterns

### 2.1 Mediated Hierarchy

The most widely deployed pattern for multi-agent coordination is the mediated hierarchy. A designated coordinator — variously called an orchestrator, a cognitive engine, or a manager agent — receives outputs from all agents, synthesises them, and issues directives back to the agent population.

Cisco's internal Mycelium project [CITE-CISCO-MYCELIUM] — which shares its name with the system presented in this paper, though the two are otherwise unrelated — is a well-documented contemporary instance of this pattern. A CognitiveEngine mediates all agent interaction. Agents never communicate directly. When coordination is required, the engine decomposes the agents' natural-language positions into discrete issues, runs a NegMAS Stacked Alternating Offers (SAO) negotiation protocol over multiple rounds [CITE-NEGMAS], synthesises a result, compiles it into a plan, and broadcasts it to all participants. The demonstration case shows two agents reaching agreement on a transaction over fourteen negotiation rounds. The system includes a 300-second watchdog timeout to abort sessions where agents become unresponsive, and explicitly notes that coordination state is held in memory and lost on server restart.

Kubernetes [CITE-K8S] represents a more mature instance of the same architectural pattern. A control plane holds the declared target state; reconciliation loops continuously compare actual cluster state to that target and issue corrective actions. The coordinator (the control plane) is more sophisticated than a negotiation engine, but the structural relationship is identical: state is held above the runtime, and correctness is enforced top-down.

### 2.2 Registry-Based Discovery

The second dominant pattern is registry-based discovery. Rather than a runtime coordinator, a persistent global registry stores agent capability declarations. Agents query the registry to locate peers with matching capabilities; the registry brokers the introduction.

MIT's NANDA project [CITE-NANDA] is the leading contemporary instance at internet scale. NANDA implements a decentralised registry — described by its authors as "DNS for agents" — in which agents register structured AgentFacts including capability declarations, credentials, and behavioural history, anchored to W3C Decentralised Identifiers (DIDs). Other agents query the registry; NANDA's adapter layer handles protocol bridging across MCP, A2A, and NLWeb. The project envisions a "Quilt" federation of enterprise, government, Web3, and civil-society registries composing into an internet-scale agent discovery infrastructure. Whatever its scale and sophistication, the registry is the coordinator by another name: a designated component that must hold authoritative state about the agent population and through which discovery passes.

### 2.3 The Shared Assumption

Superficially, these two patterns are different. A mediated hierarchy coordinates behaviour at runtime; a registry brokers introductions at discovery time. But they share a single structural assumption: **there exists a designated component that holds authoritative state about the agent population and through which coordination passes.**

In the mediated hierarchy, this component is the coordinator. In the registry model, it is the registry. Both are coordinators by different names. Actor-based systems distribute the coordinator but leave explicit topology management as an obligation on the emitter — a softer form of the same assumption, examined in §3.2. All three inherit a common set of failure modes with a history that predates AI agents by decades. Section 3 traces that history; Section 4 dissects the failure modes themselves.

### 2.4 The Separation Tax

The coordinator assumption has a concrete operational cost that the preceding architectural analysis may understate. A representative conventional ML inference pipeline must address at minimum four distinct infrastructure concerns: task dispatch and queuing (Redis/Celery or Kafka depending on scale), worker registration and discovery (Consul, etcd, or a sidecar), failure recovery and dead-letter handling, and cross-component observability. Each concern requires a separately deployed, operated, and monitored service. The pipeline is not the model — it is the scaffolding around the model.

Each concern is a separate operational surface: a separate failure mode, a separate scaling policy, a separate upgrade cycle, and a conceptual translation layer. The coordination work — route an item to a capable worker, throttle when workers are saturated, clean up abandoned items, make topology visible — is the same abstract work at every level. The *separation tax* is the cost of performing that work redundantly at each concern boundary rather than once in a unified substrate.

The two dominant patterns described in §§2.1–2.2 do not reduce this tax. They add another coordination system on top of the existing stack. This paper's claim is that the separation tax is not a necessary cost of multi-agent coordination. It is the cost of applying the coordinator assumption independently at each layer of the stack.

---

## 3. A History of the Coordinator Assumption

The coordinator assumption — that a designated component must hold authoritative state and through which coordination passes — is not new. It has been the default in distributed computing for over fifty years. Each generation has recognised the problems it causes and attempted to mitigate them without eliminating the assumption itself.

### 3.1 Blackboard Systems (1970s–80s)

The blackboard model, most prominently realised in the HEARSAY-II speech understanding system [CITE-HEARSAY], organises coordination around a central shared data structure — the blackboard — and a set of specialised knowledge sources that read from and write to it. A control component monitors the blackboard and decides which knowledge source to activate next. The pattern is explicit: central state, central control. Knowledge sources do not communicate directly; they communicate via the coordinator. HEARSAY demonstrated that the model could solve hard problems. It also demonstrated the failure modes: the control component became a bottleneck and a single point of failure, and the blackboard accumulated stale entries as the system evolved.

### 3.2 The Actor Model (Hewitt, 1973)

Hewitt, Bishop and Steiger's Actor model [CITE-HEWITT] provided the first formal model of concurrent computation as communicating entities. Actors maintain private state and communicate exclusively via asynchronous message passing. The model eliminates shared mutable state — a genuine advance — but retains explicit addressing: to send a message, an actor must know the recipient's address. There is no single coordinator, but this is a softer form of the coordinator assumption: topology must be managed explicitly by the sender. An actor that changes its role or capabilities cannot automatically re-wire the communication patterns of its senders — the emitter bears the topology management burden. The Actor model correctly identified agents as the unit of composition; it did not identify boundaries as the mechanism for self-organising communication, and it left explicit topology management as an unresolved obligation on the system designer.

Mature actor runtimes — most notably Erlang/OTP — address fault tolerance and process supervision without a central coordinator, and represent a genuine step beyond the original formulation. The addressing problem persists, however: Erlang's process registry is a coordinator with a narrower mandate, and OTP supervision trees impose a coordinator hierarchy for failure propagation.

### 3.3 Linda and the Tuple Space (Gelernter, 1985)

Gelernter's Linda [CITE-LINDA] introduced generative communication via a shared tuple space. It is best understood as an iteration on the blackboard model: the explicit control component is removed, but the shared medium remains — processes write typed tuples to it and other processes match and retrieve them by pattern. The medium still holds state; agents still interact with the medium rather than with each other directly. Carriero and Gelernter's 1992 CACM paper [CITE-LINDA-COORD] established that coordination is a distinct language design concern, separable from computation — a correct and underappreciated insight.

What Linda retained, however, is a shared coordinator: the tuple space itself must be accessible, consistent, and available to all participants. Fault-tolerant variants (FT-Linda, JavaSpaces) required replication schemes and consistency protocols for the shared space. The medium is the coordinator. The coordinator trap persists in a subtler form.

The distinction that matters — and that Section 7 will realise — is precise: in Linda, the admission decision is a retrieval pattern issued against the shared medium, which must remain consistently accessible to all participants. An alternative is for each node to hold its own boundary as a local declaration evaluated against live distributed state: the medium is never queried, the node self-filters, and there is no shared state to keep consistent.

### 3.4 BDI Agents and FIPA Standards (1990s)

The Belief-Desire-Intention (BDI) agent architecture [CITE-BDI] and the Foundation for Intelligent Physical Agents (FIPA) agent communication standards [CITE-FIPA] brought deliberative reasoning to multi-agent systems. FIPA defined a Directory Facilitator (DF) — a registry where agents register capabilities and query for peers — and an Agent Management System (AMS) as lifecycle controller. The DF is precisely the registry-based pattern that MIT NANDA echoes today, four decades later: a designated component holds authoritative state about the agent population. The JADE framework [CITE-JADE], the dominant FIPA implementation, made the DF a hard architectural requirement.

### 3.5 The Pattern That Persists

Across five decades, each model correctly identified coordination as a concern distinct from computation. None eliminated the coordinator assumption:

| Model | Coordinator retained |
|---|---|
| Blackboard (HEARSAY, 1980) | Central blackboard + control component |
| Actor model (Hewitt, 1973) | Explicit addressing — sender must know recipient |
| Linda (Gelernter, 1985) | Shared tuple space as coordinator |
| BDI / FIPA / JADE (1990s) | Directory Facilitator as registry |
| Modern LLM orchestration (2020s) | Central orchestrator / CognitiveEngine |

Carvalho [CITE-CARVALHO] observes that modern AI agent frameworks are independently rediscovering Linda's tuple space pattern — implementing ad-hoc shared stores with task claiming, polling, and matching — without awareness of forty years of research on its failure modes and partial solutions. The community is not learning from history. It is repeating it.

Each generation recognised the coordinator's failure modes and attempted to mitigate them. None asked whether the coordinator could be eliminated entirely.

---

## 4. The Coordinator Trap: From Architecture to Symptom

We use the term *coordinator trap* to describe the set of failure modes that any architecture inheriting the coordinator assumption must exhibit. These are not bugs; they are structural properties. A better coordinator does not escape them. A smarter mediator does not escape them. The trap is the coordinator's existence, not its quality.

We trace each failure mode through the agent lens — asking not merely what the system does wrong but what it implies about the agents' relationship to information, state, and decision.

### 4.1 The Mediated Hierarchy Described Precisely

In a mediated hierarchy, agents are workers in a fanout RPC system. They receive structured payloads from the coordinator, produce responses, and return them. The coordinator aggregates responses, applies its decision logic, and broadcasts results. The agent has no autonomy over what signals it receives — it processes whatever the coordinator sends. Its "boundary" — the set of signals it acts upon — is managed externally by the coordinator's routing decisions, not declared internally by the agent itself.

This is a critical point. When we call these components "agents" we are importing a concept from the Complex Adaptive Systems literature. Holland [CITE-HOLLAND] defines an agent as an entity that holds a *boundary* — a receptor set specifying the conditions under which it acts on incoming signals. The boundary is intrinsic to the agent, not delegated from outside. However, in a mediated hierarchy, agents satisfy none of this definition: they receive what the coordinator routes, not what they declare themselves competent to receive. They are passive processors of coordinator-routed payloads. We return to this category error in Section 4.5.

### 4.2 Audit Burden: The Consequence of Post-Admission Broadcasting

The unbounded audit obligation follows directly from the coordinator's broadcast model. It is documented in practitioner experience at Cisco's Mycelium deployment by Valenti [CITE-SIGNAL-NOISE], whose account provides a precise field diagnosis of the structural consequences analysed here.

The coordinator receives all agent outputs and produces a synthesised result. That result is broadcast to all downstream consumers: other agents, human reviewers, monitoring systems. Every output of every agent passes through the coordinator and produces a coordinator-level artifact with identical structure and apparent authority, regardless of the quality or validity of the underlying agent reasoning.

This is post-admission filtering: artifacts are produced first; quality is assessed after. The coordinator is the filter, but it operates after production, not before. A coordinator that synthesised more carefully would reduce noise, but it could never eliminate the audit obligation entirely because quality assessment requires domain expertise that the coordinator cannot possess for every domain it mediates. As agent populations grow, the coordinator's cognitive load grows linearly with the number of domains it must evaluate. The audit burden scales with the coordinator.

The three-hour issue and the five-second issue have the same voice because the coordinator processes them identically [CITE-SIGNAL-NOISE]. There is no mechanism in the architecture to distinguish them before they produce output. The distinction, if it is made at all, must be made by a human reviewer after the fact — which is precisely the unbounded obligation Valenti describes.

### 4.3 Context Loss: The Consequence of Coordinator-Held State

The second failure mode — agents restarting from zero, re-litigating settled decisions — follows from the coordinator's role as the system's memory.

In a mediated hierarchy, shared state lives in or near the coordinator. Agents are stateless workers; they hold no durable view of prior decisions. When the coordinator restarts (or in the case of Cisco's Mycelium v1, simply when the server process is restarted), the state is gone. When a new agent joins, it has no independent access to prior decisions — it must query the coordinator, which synthesises a catchup briefing from whatever logs survive.

This is the classic distributed systems mistake: centralising state simplifies the coordinator's internal logic but makes every participant dependent on the coordinator's availability and memory continuity. The agents are not resilient in isolation because they were never designed to hold state independently. The coordinator *is* the memory; the agents are its terminals.

The attempted fix documented in [CITE-CONTEXT-PROBLEM] — a filesystem-based shared memory — correctly identifies that state should be held outside the coordinator. But it introduces a shared mutable store with no eviction policy: entries persist indefinitely, accumulate, and drift from current system state. This is not a solution to the coordinator trap; it is a redistribution of the same problem.

### 4.4 Output Format Mismatch: The Consequence of Absent Boundary Model

The third failure mode — agent logs structured for machines, routed into human channels and failing to communicate — follows from the coordinator's undifferentiated broadcast.

When the coordinator synthesises a result and broadcasts it, it has no model of its recipients' boundaries — no representation of which consumers can act on which signals, in which form, at which level of detail. All downstream consumers receive the same output because, from the coordinator's perspective, they are all equivalent downstream consumers of its decisions. The coordinator routes to roles or channels, not to declared receptor sets.

A human reviewer is not equivalent to an agent as a consumer of a coordination decision. A human can skim for significance, ask different questions, and have a different attention budget and a different tolerance for structured verbosity. But the coordinator cannot know this without a model of recipient boundaries — and building and maintaining such a model is precisely the kind of overhead that grows without bound as the agent population grows.

### 4.5 The Category Error: Workers Dressed as Agents

The three failure modes share a common root: in a mediated hierarchy, components called "agents" are not agents in any theoretically meaningful sense.

As established in §4.1, Holland defines an agent as an entity whose boundary — a receptor set — is intrinsic to the agent, not delegated from outside. A component whose domain is determined by the coordinator's routing decisions — which receives what the coordinator sends, not what it declares itself competent to receive — is not an agent. It is a worker node in a fanout RPC system.

This is not a semantic quibble. The distinction determines whether filtering happens before or after production (pre-admission vs post-production), whether state is held distributed or centralised (inside each agent vs inside the coordinator), and whether the system tolerates coordinator failure gracefully (coordinator-free: agents hold their own state and boundaries) or catastrophically (mediated hierarchy: agents are coordinator-dependent).

Calling these components "agents" while designing them as workers inherits none of the architectural properties that make genuine agents scalable. It merely imports the vocabulary without the substance.

The exit from the coordinator trap requires not a better coordinator but the elimination of the coordinator from the substrate. A substrate in which each agent holds its own receptor set — in which coordination emerges from the match between signals and declared boundaries, without any routing table, registry, or mediating component — would make the failure modes structurally prevented or structurally highly improbable. Not ameliorated; structurally prevented or highly improbable at the substrate level. Such a substrate was described theoretically by John Holland.

---

## 5. Theoretical Foundation: Holland's Signal/Boundary Model

John Holland's framework for Complex Adaptive Systems [CITE-HOLLAND, CITE-HOLLAND-INTRO], formalised in *Signals and Boundaries: Building Blocks for Complex Adaptive Systems* (MIT Press, 2012), provides the theoretical basis for a coordinator-free architecture.

Holland's thesis: the behaviour of complex adaptive systems emerges from two and only two primitives.

**Signals** propagate through a medium unconditionally. No signal is withheld from propagation on the basis of who might act on it. The medium floods.

**Boundaries** are receptor sets. Each agent holds a boundary — a set of conditions under which it *acts* on a signal. The boundary controls acting, not receiving. Forwarding is always unconditional.

This inversion is the key insight. Conventional distributed systems thinking routes messages to known recipients. The emitter must know who is listening; topology must be managed explicitly. Holland's model changes the medium: any agent whose boundary matches a signal responds. The emitter does not need to know who is listening. Topology does not need to be managed explicitly. The system tolerates churn without stalling because there is no routing table to maintain, no coordinator to query, no registry to consult.

**The coordinator trap dissolved.** In Holland's model there is no coordinator because none is needed. Filtering is pre-admission, not post-production: an agent whose boundary does not match a signal produces no response to it — no artifact, no log entry, nothing to audit. State is held distributed, inside each agent as its capability and requirement declarations: there is no coordinator memory to lose. Recipients are self-differentiating: each agent's boundary determines what it receives, so the output format mismatch between humans and agents is a boundary design problem, not a routing problem.

**Stigmergy and TTL evaporation.** Holland identifies stigmergy — state left in the medium by agents, readable by other agents — as the mechanism through which agents coordinate without direct communication. In biological systems this takes the form of pheromone trails: a path walked frequently stays fresh; an unused path fades [CITE-DORIGO]. Applied to distributed state management, this suggests that shared state should carry a time-to-live and evaporate unless actively refreshed. An agent that is alive and relevant keeps its state fresh; a dead or departed agent's state simply fades. There is no explicit deregistration, no failure detection protocol, no tombstone management required. Failure is emergent: the absence of refreshment *is* the failure signal.

Applied to the failure modes diagnosed in Section 4: pre-admission filtering structurally prevents the audit burden — an agent whose boundary does not match a signal produces no artifact to audit. Distributed TTL state makes coordinator-type context loss structurally highly improbable — there is no coordinator memory to lose; state evaporation under simultaneous node failure remains possible but requires active failure of TTL renewal across all replicas simultaneously. Self-differentiating boundaries make output format mismatch a design problem rather than a routing obligation — human-facing and agent-facing consumers declare different boundaries and self-select the signals relevant to them. Each failure mode is not ameliorated; it is either structurally prevented or made structurally highly improbable at the substrate level.

---

## 6. Prior Art: Correct Concepts, Wrong Implementation

Three prior systems identified the correct underlying concepts but implemented them with too much protocol ceremony, or in the wrong deployment model, to achieve their full potential.

### 6.1 Jini and the Lease Insight (Sun Microsystems, 1998)

Jini [CITE-JINI-ARCH, CITE-JINI-SPEC] was designed to address runtime dynamism: in a live network, services appear, depart, and change in ways no deployment manifest can anticipate, and a crashed service cannot explicitly deregister. The lease was Jini's answer — decay is the right primitive for an unpredictably dynamic world. A service holds a lease on its registration; if it does not renew, the registration expires, providing implicit failure detection without requiring an explicit deregistration protocol.

The insight is correct. The implementation was protocol-heavy: explicit `Lease` objects, `renew()` RPCs, a lease manager, explicit cancellation. The ceremony obscures the substrate property — that registrations should evaporate unless actively maintained — behind an explicit lifecycle protocol.

### 6.2 OSGi Requirements and Capabilities

OSGi's insight was that software agility is a function of modular, dynamically assembleable components. The OSGi Alliance [CITE-OSGI] formalised this as a dependency model in which software modules declare capabilities they provide and requirements they need; a resolver matches providers to consumers. The primitive is correct: declarative matching between providers and consumers, with the resolver handling wiring.

What mainstream OSGi adoption got wrong was treating resolution as static — performed once at bundle-install time. A module that disappeared at runtime left a gap with no in-process mechanism for repair; closing it required a human to rebuild and redeploy. Dynamic assembly was the goal; deploy-time resolution was where that ambition stopped. Paremus Service Fabric addressed this directly.

### 6.3 Paremus Service Fabric and the Reconciliation Engine

Paremus Service Fabric (circa 2010–2015) [CITE-PAREMUS] began as a Jini runtime with OSGi components, using Jini's service discovery as the underlying discovery mechanism. Over time, Jini service discovery was replaced with the OSGi Remote Services specification backed by an in-house gossip-based implementation — decentralised discovery, TTL-managed registrations, no central lookup service. What emerged from this evolution was a deliberate fusion of Jini's runtime dynamism with OSGi's modular assembly model: the R&C graph as a *declared target topology*, with resolution made continuous against a live, gossip-discovered environment rather than a static deployment manifest. The runtime was continuously monitored against that declared target state and deltas driven back into convergence — closer to the Kubernetes control loop than to a shared knowledge graph, and requiring no human redeploy.

What did not change was the hermetic relationship between the gossip layer and the reconciliation engine: 'as above, so below' — the declared target state lived in the central engine above; the gossip layer revealed the landscape below; the engine held the authority to reconcile the two. The coordinator trap persisted not in the discovery mechanism but in the architectural layer that held the target.

What Paremus still required was a *central reconciliation engine* holding that target state. The engine computed deltas; the engine issued corrections; the engine was the coordinator. If it went down, reconciliation stopped. The coordinator trap in a more sophisticated form.

### 6.4 Flow-Based Programming (Morrison, c. 1971)

Flow-Based Programming [CITE-FBP], developed by J. Paul Morrison at IBM in the early 1970s, conceived programs as networks of independent black-box processes communicating through bounded connections. The model separates coordination — the network topology and its capacity constraints — from computation — the component logic — and it is correct in this separation. Three things FBP got right:

**Reusable processes.** A component knows nothing about its neighbours — only its own input and output ports. This makes components genuinely reusable across different topologies without modification.

**Automatic backpressure.** Bounded connections block writers when full. A fast producer cannot overwhelm a slow consumer; the network self-regulates without any explicit flow-control logic in the components.

**Declarative topology.** The wiring diagram — which component connects to which — is expressed as a data structure separate from the component code, amenable to visual inspection and offline analysis.

What FBP retained was an external medium to which all these properties belong. The bounded connections, the port bindings, the network definition — all live in an FBP runtime outside the component processes. Components are defined relative to their runtime; topology is managed by the runtime, not by the components themselves. The FBP runtime *is* the coordinator: it allocates connections, manages backpressure, and resolves the wiring. Moving from an FBP runtime to a Kafka cluster is an infrastructure substitution, not an architectural shift. The coordinator trap remains.

### 6.5 The Strip-the-Ceremony Pattern

A pattern emerges across all four cases:

| Prior art | Correct concept | Implementation ceremony | Substrate property equivalent |
|---|---|---|---|
| Jini | Registrations should decay | `Lease.renew()` RPC + lease manager | TTL as natural evaporation |
| OSGi | Declarative capability matching | Static bundle-install resolver | Continuous evaluation against live state |
| Paremus | Continuous reconciliation toward target state | Central reconciliation engine + target state graph | Gossip mesh — every TTL refresh is a reconciliation tick |
| FBP | Backpressure, declarative topology, reusable processes | External FBP runtime managing connections | Opacity KV flag — agent self-declares saturation; resolvers skip it |

The pattern that produces better architecture: identify the correct concept, find the substrate *property* that produces the same behaviour without an explicit protocol, implement the property and let the behaviour emerge. When a proposed feature requires a manager, a coordinator, an explicit lifecycle protocol, or a renewal RPC, apply this heuristic before accepting it.

---

## 7. Mycelium: A Coordinator-Free Substrate

Mycelium is an embeddable Rust library implementing Holland's signal/boundary model. It has no daemon, no control plane, no installer, no orchestrator. It is a crate embedded in the caller's process. The operator's existing infrastructure — Kubernetes, bare metal, cloud VMs, edge devices — is irrelevant because Mycelium does not touch it.

The architecture is three layers; each directly addresses one of the three structural failure modes identified in Section 4: audit burden (§4.2), context loss (§4.3), and output format mismatch (§4.4).

### 7.1 Layer I — KV Store: Eliminating Context Loss

**The layer.** Layer I is a gossip-replicated key-value store. Every entry carries a Hybrid Logical Clock timestamp for causal last-write-wins ordering and a time-to-live. Entries are replicated across the cluster via epidemic gossip; anti-entropy reconciliation runs on peer reconnection. There is no primary, no leader, no coordinator. Every node holds a full replica of the key namespace relevant to it.

**The failure mode eliminated.** Context loss in the mediated hierarchy arises because shared state is held in or near the coordinator. When the coordinator restarts, state is lost; when a new agent joins, it must request a catchup briefing from the coordinator.

In Mycelium's Layer I, there is no coordinator memory to lose. State is distributed across the mesh. A node that restarts reconnects to its peers, runs anti-entropy, and recovers full mesh state within one gossip cycle. A new node joining the cluster receives capability and decision state directly from its peers; no coordinator query is required. There is no entity whose restart causes context loss.

**TTL evaporation as stigmergy.** Every capability advertisement, every group membership record, every pheromone-style opacity flag is a KV entry with a TTL. A live node continuously re-advertises; the trail stays fresh. A dead node stops re-advertising; its entries evaporate. There is no explicit failure detection protocol because absence of refreshment *is* the failure signal. The mesh always reflects the current live population.

**The key inversion from Paremus.** In Paremus, the target state was held *above* the runtime in a central graph and pushed down into components. In Mycelium, the target state is *compiled into the runtime components themselves* — each node carries its own fragment as capability and requirement declarations. There is nothing external to converge toward. The mesh assembles the whole picture bottom-up from those fragments, and convergence emerges upward. The application's intended topology is not a document held somewhere; it is the aggregate of what every node declares itself to be.

### 7.2 Layer II — Signal Mesh: Eliminating Audit Burden and Output Mismatch

**The layer.** Layer II is an ephemeral signal mesh implementing Holland's signal/boundary model directly. Signals are emitted with a scope — `Individual`, `Group`, `System`, or `Groups` (multi-group union) — and propagated unconditionally to all reachable nodes. At each receiving node, `Boundary::admits()` evaluates the signal against the node's declared receptor set. Only nodes whose boundary matches the signal act on it. Forwarding is always unconditional; acting is always conditional on boundary admission.

**The failure mode eliminated: audit burden.** The audit burden in the mediated hierarchy arises because the coordinator broadcasts synthesised results to all consumers regardless of their domain relevance. Filtering is post-production.

In Layer II, filtering is pre-admission and intrinsic to each agent. A signal scoped to the `gpu-compute` group only reaches nodes whose boundary includes `gpu-compute` membership. A signal about financial compliance only reaches nodes in the `compliance` group. Agents in unrelated groups receive and forward the signal but do not act on it — there is no artifact, no log entry, no audit obligation produced. The three-hour problem and the five-second problem never land on a reviewer who cannot evaluate them because the boundary prevents admission in the first place.

**The failure mode eliminated: output format mismatch.** The coordinator's output format mismatch arises because it has no model of recipient boundaries — it broadcasts uniformly because all consumers are equivalent from its perspective.

In Layer II, recipient heterogeneity is the design. A signal emitted to a `human-review` group reaches only nodes whose boundary includes human-review membership. A signal emitted to `automated-pipeline` reaches a different set of nodes. The emitter composes a signal for its intended audience; boundary admission ensures delivery to the matching receptor set. Human-facing and agent-facing consumers self-differentiate via their boundary declarations without any routing logic in the emitter.

### 7.3 Capability and Requirement Declarations: Eliminating the Category Error

**The mechanism.** Each Mycelium node holds a `CapabilityGroupDef` — a declarative specification of capabilities it provides and requirements it needs. The node independently evaluates whether it should join each capability group by testing its own capabilities against the group's filter, right now, against live KV state. Membership is self-assigned, not coordinator-assigned. It is re-evaluated on every relevant KV change.

**The failure mode eliminated: the category error.** In the mediated hierarchy, an agent's "boundary" — the signals it acts on — is determined by the coordinator's routing. The agent is a worker. In Mycelium, each node's boundary is determined by its own capability declarations, compiled in at build time as the `CapabilityGroupDef`. The boundary is intrinsic, not delegated. The node *is* its boundary.

This is a genuine agent in Holland's sense: a component that holds its own receptor set, evaluates signals against it, and acts selectively. The boundary is not managed externally; it is declared as part of the component's definition. The application's coordination topology — who talks to whom, who acts on what — emerges from the composition of these declarations across the live node population, not from a coordinator's routing table.

### 7.4 Layer III — Consensus: Eliminating the Coordinator Bottleneck

**The layer.** Layer III provides epidemic consensus — `group_propose`, `system_propose`, and `cross_group_propose` — implemented entirely through the signal mesh. `PROPOSE`, `VOTE`, and `COMMIT` are signal payloads riding ordinary Layer II `Signal` frames. The commitment semantics — ballot numbering, quorum checking, KV write on commit — are logic that the proposer applies to the signal stream. Layer II has no concept of "agreement." The substrate is unaware that consensus is happening.

**Cross-group consensus.** `cross_group_propose` supports ballots where multiple named capability groups act as independent voting blocs. A proposal commits only when every group independently reaches its required quorum fraction. This supports multi-AZ durability requirements (quorum from `az-east` AND `az-west`), compliance ratification (a `compliance` group with veto rights), and hierarchical AI pipelines (coordinators and workers must both agree). All without a coordinator — each group's quorum is computed locally from KV-advertised membership.

**The failure mode eliminated.** The mediated hierarchy's coordinator is a bottleneck: coordination throughput is bounded by the coordinator's capacity, and the coordinator's failure stops coordination entirely. In Layer III, there is no coordinator. Consensus emerges from the signal exchange among participants. No single node's failure prevents other ballots from proceeding. The system degrades gracefully — a reduced participant population means reduced quorum, not coordination failure.

### 7.5 Agentic Flow Networks

The three-layer architecture described in §§7.1–7.4 is sufficient to implement a distinct multi-agent computational pattern that we term an *Agentic Flow Network* (AFN). An AFN is a topology of capability-bounded agent groups in which work flows through the mesh as a sequence of KV state transitions, each stage consuming from one key prefix and producing to the next.

An AFN has five structural properties:

1. **Substrate Unity.** All coordination — capability advertisement, group membership, backpressure, work items, completion signals — is carried by the same gossip KV substrate. There is no separate message queue, registry, or coordination bus. A single replicated data structure is the coordination medium.

2. **Topology Emergence.** Which nodes participate in which pipeline stages is not configured by an operator. Each node evaluates its own capabilities against the group definitions and self-joins the matching stages. A new node with a matching capability automatically joins the appropriate stage. A failed node's advertisements evaporate; the topology heals without intervention.

3. **Fluid Allocation.** There is no static assignment of workers to stages. Any node capable of serving a stage joins its group; the group's size fluctuates with available capacity. A stage that is undersupplied can be horizontally scaled by launching nodes with the matching capability; no reconfiguration of existing nodes is required.

4. **Opacity Backpressure.** A saturated node writes an opacity flag to `sys/load/{self}/capacity` (TTL ~5 s), making itself invisible to resolvers at the next stage. The topology self-throttles without any flow-control protocol: saturation is a KV entry that evaporates when load clears.

5. **TTL-Native Cleanup.** Work items written to the KV substrate carry TTLs. Abandoned items — from failed workers or cancelled requests — evaporate automatically. There is no dead-letter queue, no explicit cancellation protocol, no garbage collection task. Absence of refreshment is the failure signal.

These five properties are validated empirically by `examples/fluid_pipeline/` — a 10-worker, 4-stage news article pipeline (fetch → parse → score → publish) running over a shared KV ring as distributed buffer, exercised end-to-end by integration scenario 11. The topology assembles itself, the opacity backpressure mechanism is confirmed by direct observation, and all work items are cleaned up on worker exit without explicit deregistration.

An AFN is not a new programming model layered on top of Mycelium. It is the name for the pattern that naturally arises when a distributed application is built correctly on a substrate that eliminates the coordinator.

### 7.6 The Economic Case: Quadratic Cost Decomposition

The architectural argument for coordinator-free design — that mediated hierarchies produce structural failure modes — is independent of cost. There is also a direct economic argument.

LLM agent execution cost scales superlinearly with iteration count. Because each API call re-processes the entire accumulated context, the token count at iteration k is ΔT + τ·k (where ΔT is the initial prompt and τ is context growth per iteration). Summing over k iterations yields a total proportional to k²: cost is quadratic in iteration count, not linear. This follows mathematically from the autoregressive transformer architecture shared by all major LLMs.

The decomposition leverage is direct. Splitting a k-iteration task into M independent subtasks of k/M iterations each reduces total cost by factor M: M × (k/M)² = k²/M, versus k² for the monolithic run. A 4-way decomposition of a 100-iteration task costs 4 × 25² = 2,500 versus 100² = 10,000 — a 4× saving from a 4× decomposition. This follows directly from the cost structure, not from a heuristic. The analysis assumes approximately linear context growth per iteration — the expected case when each step adds tool results, reasoning traces, or intermediate outputs; actual cost growth may be lower where outputs are consistently short, but the quadratic scaling relationship holds whenever context accumulates.

A mediated hierarchy cannot realise this saving without explicit orchestration overhead. The coordinator must decompose the task, route subtasks, collect results, and synthesise — and the synthesis step, where the coordinator aggregates M subtask results into a combined output, recreates the full accumulated context and pays the quadratic penalty once more, partially or entirely undoing the decomposition saving. However, Mycelium's capability-bounded groups implement decomposition automatically: each group's TTL constrains how long a work item remains live in that stage, which under typical workloads approximates a bound on effective k for that group — a time bound rather than a strict iteration count bound, but one that produces the same practical effect when per-iteration latency is roughly constant. The quadratic cost accumulation resets at each group boundary, and no coordinator synthesis is required. The coordinator-free design is therefore not merely structurally correct but structurally cheaper.

The practical significance of this saving is reinforced by empirical energy measurements. White [CITE-WHITE2025] demonstrates that inference energy per query varies by two orders of magnitude across model families and sizes, and that energy cost is highly sensitive to input characteristics. Where the quadratic accumulation compounds an already expensive per-call baseline, the saving from decomposition is not a theoretical nicety — it is an operational necessity.

---

## 8. Evaluation

Mycelium's correctness across its three-layer architecture is validated by 243 unit tests and 12 integration scenarios run against a live 5-node Docker cluster. Scenarios cover KV replication under partition and reconnection, signal delivery and boundary admission, capability group formation and dissolution, consensus quorum under node failure, cross-group voting, the full Agentic Flow Networks pipeline, and Prompt Skills cross-node KV propagation with LLM invocation. All 12 scenarios pass at HEAD.

The implementation is publicly available at https://github.com/RichardEko/mycelium under the AGPL-3.0 licence. The AGPL-3.0 licence is intentional: applications that deploy Mycelium as part of a networked service must publish their source under the same terms, closing the SaaS loophole that permissive licences leave open. Organisations requiring proprietary embedding may obtain a commercial licence from Tathata Systems Ltd. The integration test harness is included in the repository and reproducible with a single `make test` invocation against a Docker-composed 5-node cluster.

---

## 9. Discussion

### 9.1 The Hayek Parallel

The coordinator trap is not a new discovery. Friedrich Hayek described it in 1945 — for economies.

In *"The Use of Knowledge in Society"* [CITE-HAYEK], Hayek argued that the central planning problem is not computational — it is epistemic. No central planner can possess the distributed, local, tacit knowledge held by individual market participants. Prices are not just numbers; they are signals that aggregate and propagate dispersed knowledge through the economy without anyone needing to understand the whole. Attempts to replace this with a central planning apparatus fail not because planners are incompetent but because the knowledge required for correct decisions is structurally inaccessible from any central point.

The parallel to the coordinator trap is exact. A planned economy and a mediated hierarchy share the same failure mode: a central node that must aggregate knowledge it cannot fully possess, synthesise decisions on behalf of participants who understand their local context better than any coordinator can, and issue commands downward into a system whose ground truth is always more current at the edges than at the centre. A market economy and Mycelium share the same solution: signals propagate local knowledge unconditionally; participants act on signals that match their position; emergent order arises from those local interactions without any node needing a view of the whole.

Hayek's market is a signal/boundary system. He just did not have Holland's vocabulary.

The intuition that a sufficiently intelligent coordinator — with enough information and computing power — could outperform the distributed system is seductive precisely because it *feels* like it should work. The appearance of control is reassuring even when it is structurally impossible. This is why mediated hierarchies keep being built despite their failure modes: the coordinator looks like it is in control, even as the audit burden accumulates and context is lost on every restart.

The insight is the same in each case: **distributed local knowledge, expressed through signals and boundaries, produces emergent order that no coordinator can match.**

The Hayek parallel is not decorative. It establishes that the coordinator trap is a structural consequence of centralising information aggregation in any complex adaptive system — not a software engineering mistake that could be engineered away with a better implementation.

### 9.2 The Beinhocker Parallel: Organisations as Complex Adaptive Systems

Beinhocker's *The Origin of Wealth* [CITE-BEINHOCKER] reaches the same conclusion from a third direction: organisations. Drawing on extensive empirical analysis of corporate performance and adaptability, Beinhocker finds that hierarchical organisations centralising strategic information and decision-making systematically underperform distributed ones — not because their leaders are less capable, but because the local knowledge required for adaptive decisions cannot travel up the chain fast enough to remain actionable by the time it arrives. The CognitiveEngine coordinating AI agents and the central command chain coordinating human organisations are structurally identical and fail for the same reason. Hayek, Holland, and Beinhocker are describing the same property of complex adaptive systems at three different scales — economies, biological systems, and organisations. The coordinator trap is not a software engineering peculiarity. It is a structural consequence of centralising coordination in any sufficiently complex adaptive system.

### 9.3 Limitations

Mycelium assumes a cluster the operator owns. Mycelium's scope is intra-cluster; cross-organisational discovery requires external registry infrastructure. Mycelium's conforming A2A endpoint makes it reachable from any A2A-speaking registry without modification to either side. Ephemeral signals are intentionally not durable — a node that misses a signal misses it, and the TTL on capability entries enforces a hard iteration ceiling on any agent group. Both are deliberate: in systems where cost distributions have heavy tails under variable context growth, hard ceilings are the only reliable bound — no fixed contingency percentage contains the risk when the tail is severe. Durable delivery is a higher-order concern built on the KV layer or consensus, not a substrate property. The gossip substrate assumes eventual connectivity; a fully partitioned cluster cannot converge.

Boundary admission requires agents to declare their boundaries correctly. A misconfigured boundary — too broad or too narrow — produces incorrect routing without any coordinator to catch the error. This places a correctness obligation on the capability declarations that the mediated hierarchy places on the coordinator instead. Neither is strictly easier; the burden is different in character.

The quadratic cost decomposition in §7.6 assumes that the k-iteration task can be partitioned into M independent subtasks. Many real agentic workloads are inherently sequential — multi-step reasoning chains, document drafting, iterative refinement — where each step depends causally on the prior and decomposition is not valid. The saving applies where subtask independence holds; it does not apply universally.

**When not to use Agentic Flow Networks.** Five categories of requirement are structurally mismatched with the AFN pattern:

*Strict ordering required.* The KV substrate uses last-write-wins under Hybrid Logical Clock ordering. Items arriving out of causal order are resolved by timestamp, but processing order within a stage is not preserved. Applications that require strict FIFO or total-order delivery through a pipeline stage need a durable ordered append-only log (Apache Kafka, NATS JetStream) at that stage boundary.

*Exactly-once delivery.* Gossip replication is at-least-once within a TTL window. A worker that crashes mid-processing may leave a partially-processed item visible to another worker. Applications requiring transactional exactly-once semantics — idempotency keys, two-phase commit, offset management — should use a system designed for that guarantee (Kafka transactional APIs, message brokers with acknowledgement semantics) for the affected stage transitions.

*Complex DAG with cross-stage dependencies.* AFN stages are loosely coupled by KV prefix convention. A pipeline where stage C requires outputs from both stage A and stage B, with conditional branching and retry loops, maps poorly to the flat KV topology. Orchestrated DAG execution (Apache Airflow, Dagster, Prefect) provides dependency tracking, conditional execution, and human-in-the-loop approval gates that the AFN pattern does not.

*Long-term log retention.* TTL evaporation is a correctness property of the substrate, not a configurable option. Work items that complete disappear. If the application requires an audit log, a long-term event stream, or replay capability, those concerns must be handled by a separate append-only store; the AFN substrate is not the appropriate vehicle.

*Cross-cluster or cross-datacenter fan-out at scale.* The gossip mesh operates within a single cluster where every node can reach every other node within a bounded number of hops. A deployment spanning multiple independent clusters — multi-cloud, multi-datacenter active-active, or federated enterprise environments — requires a federation layer between clusters. Mycelium's A2A adapter provides the protocol surface for this; the operational deployment of that federation layer is outside Mycelium's scope.

### 9.4 Future Work

- **Empirical comparison** against a deployed mediated hierarchy at equivalent agent counts. Key measurements: (i) coordination convergence time — single-ballot `group_propose` vs NegMAS SAO N-round negotiation; (ii) failure tolerance — coordinator failure in a mediated hierarchy vs random node failure in Mycelium; (iii) state freshness under churn — TTL evaporation latency vs knowledge-graph drift rate; (iv) audit obligation under load — artifact count growth as agent population scales.
- **Formal verification** of the signal/boundary substrate properties using TLA+ or similar.
- **Cross-cluster federation** — practical experience deploying multiple independent Mycelium clusters registered with an internet-scale A2A gateway, measuring discovery latency and trust propagation across organisational boundaries.
- **Signal reorder buffer** — receiver-side per-(sender, kind) HLC-keyed causal delivery for applications requiring strict signal ordering.

---

## 10. Conclusion

The coordinator is not a solution to the multi-agent coordination problem. It is a restatement of the problem at a different scale. Mediated hierarchies make the coordinator responsible for filtering, memory, routing differentiation, and fault tolerance — properties that in a correctly designed substrate belong to the agents themselves. Registry-based systems distribute discovery without eliminating the coordinator; the registry is a coordinator with a narrower mandate.

Unbounded audit obligation, context loss across restarts, and output format mismatch between heterogeneous consumers are not bugs in specific implementations. They are structural consequences of the coordinator assumption, predictable from first principles, and irreducible by improving the coordinator.

Holland's signal/boundary model provides the theoretical basis for a different architecture: one in which coordination emerges from substrate properties rather than explicit protocols. Mycelium implements this model as an embeddable library. Each of its three layers — gossip KV store, signal mesh with boundary admission, epidemic consensus — addresses a mirrored failure mode, not by handling each failure gracefully but by making it structurally prevented or, where substrate constraints apply, structurally highly improbable.

The target state for a Mycelium application is not a document held in a registry or a graph maintained by a reconciliation engine. It is the aggregate of what every node declares itself to be, compiled into the runtime components at build time, assembled bottom-up by the mesh at runtime, and always current because anything not actively maintained evaporates. Coordination emerges. No coordinator required.

The coordinator trap is not a new discovery. Hayek described it for economies in 1945. Holland formalised it for complex systems in 2012. Mycelium implements the solution for AI agent fleets. The insight is the same in all three cases: distributed local knowledge, expressed through signals and boundaries, produces emergent order that no coordinator can match.

---

## References

[CITE-SILOBENCH] Silo-Bench: A Scalable Environment for Evaluating Distributed Coordination in Multi-Agent LLM Systems, arXiv:2603.01045, 2025.

[CITE-MAS-SURVEY] Multi-Agent Coordination across Diverse Applications: A Survey, arXiv:2502.14743, 2025.

[CITE-RAPS] Towards Adaptive, Scalable, and Robust Coordination of LLM Agents: A Dynamic Ad-Hoc Networking Perspective, arXiv:2602.08009, 2025.

[CITE-CARVALHO] O. Carvalho, "Our AI Orchestration Frameworks Are Reinventing Linda (1985)," otavio.cat, 2025.

[CITE-HEARSAY] L. D. Erman, F. Hayes-Roth, V. R. Lesser, and D. R. Raj Reddy, "The HEARSAY-II Speech Understanding System," *ACM Computing Surveys*, 12(2):213–253, 1980.

[CITE-HEWITT] C. Hewitt, P. Bishop, and R. Steiger, "A Universal Modular ACTOR Formalism for Artificial Intelligence," *IJCAI*, pp. 235–245, 1973.

[CITE-LINDA] D. Gelernter, "Generative Communication in Linda," *ACM Transactions on Programming Languages and Systems*, 7(1):80–112, 1985.

[CITE-LINDA-COORD] N. Carriero and D. Gelernter, "Coordination Languages and Their Significance," *Communications of the ACM*, 35(2):97–107, 1992.

[CITE-BDI] A. S. Rao and M. P. Georgeff, "BDI Agents: From Theory to Practice," *ICMAS*, pp. 312–319, 1995.

[CITE-FIPA] FIPA (Foundation for Intelligent Physical Agents), *FIPA Agent Management Specification*, 2004.

[CITE-JADE] F. Bellifemine, G. Caire, and D. Greenwood, *Developing Multi-Agent Systems with JADE*, Wiley, 2007.

[CITE-BEINHOCKER] E. D. Beinhocker, *The Origin of Wealth: Evolution, Complexity and the Radical Remaking of Economics*, Harvard Business School Press, 2006. Chapter 16: "Organization: A Society of Minds," pp. 349–380.

[CITE-SIGNAL-NOISE] J. Valenti, "Signal to Noise," juliavalenti.com, April 2026.

[CITE-CONTEXT-PROBLEM] J. Valenti, "The Context Problem Nobody Talks About With Multi-Agent Teams," juliavalenti.com, 2026.

[CITE-CISCO-MYCELIUM] mycelium-io/mycelium, GitHub, 2026. https://github.com/mycelium-io/mycelium

[CITE-HOLLAND] J. H. Holland, *Signals and Boundaries: Building Blocks for Complex Adaptive Systems*, MIT Press, 2012.

[CITE-HOLLAND-INTRO] J. H. Holland, *Complexity: A Very Short Introduction*, Oxford University Press, 2014.

[CITE-NANDA] R. Raskar et al., "Unlocking the Internet of AI Agents via the NANDA Index," arXiv:2507.14263, 2025.

[CITE-NEGMAS] Y. Mohammad, D. Viqueira, A. Ayerbe, and A. Kissos, "NegMAS: A Platform for Automated Negotiations," 2020.

[CITE-K8S] B. Burns, B. Grant, D. Oppenheimer, E. Brewer, and J. Wilkes, "Borg, Omega, and Kubernetes," *ACM Queue*, 14(1), 2016.

[CITE-OSGI] OSGi Alliance, *OSGi Core Release 8 Specification*, Chapter 27: Capabilities and Requirements, 2020.

[CITE-PAREMUS] Paremus Ltd., *Paremus Service Fabric*, 2010–2015. Runtime OSGi Requirements & Capabilities resolution; direct conceptual predecessor to Mycelium's continuous capability resolver.

[CITE-JINI-ARCH] J. Waldo, *Jini Architectural Overview*, Sun Microsystems Technical Report, January 1999.

[CITE-JINI-SPEC] K. Arnold, B. O'Sullivan, R. Scheifler, J. Waldo, and A. Wollrath, *The Jini Specification*, Addison-Wesley, 1999.

[CITE-DORIGO] M. Dorigo and T. Stützle, *Ant Colony Optimization*, MIT Press, 2004.

[CITE-HAYEK] F. A. Hayek, "The Use of Knowledge in Society," *American Economic Review*, 35(4):519–530, September 1945.

[CITE-FBP] J. P. Morrison, *Flow-Based Programming: A New Approach to Application Development*, 2nd ed., CreateSpace, 2010. First developed as an internal IBM paper, c. 1971.

[CITE-OSGI-MOD] R. Nicholson, "Modularity," OSGi Alliance / Eclipse Foundation, osgi.org/resources/modularity/.

[CITE-OSGI-CMB] R. Nicholson, "Complexity, Modularity and Business," OSGi Alliance / Eclipse Foundation, osgi.org/resources/complexity-modularity-and-business/.

[CITE-BRAIN-IOT-1] R. Nicholson et al., "BRAIN-IoT: Model-Based Framework for Dependable Sensing and Actuation in Intelligent Decentralized IoT Systems," *IEEE*, October 2019.

[CITE-BRAIN-IOT-2] R. Nicholson et al., "Dynamic Fog Computing Platform for Event-Driven Deployment and Orchestration of Distributed Internet of Things Applications," *IEEE*, July 2019.

[CITE-WHITE2025] M. J. White, "Inference Scaling Laws for Mathematical Code Generation: Family-Specific Behavior and Energy Analysis," November 2025. https://quantumzzxxyy.github.io/quantumzzxxyy/inference_scaling_final.pdf

---

## About the Author

**Dr. Richard Nicholson** is CEO and founder of Tathata Systems Ltd and Chief AI Transformation Officer at Novus-i2, a strategic transformation company based in the United Kingdom. He was the founder and CEO of Paremus Ltd., the company behind Paremus Service Fabric — the continuous OSGi Requirements and Capabilities runtime resolver discussed in Section 6.3 as direct prior art to Mycelium's capability model. First-hand experience designing and deploying Service Fabric over a decade of production use informed both the architectural critique presented in this paper and the design decisions made to move beyond it.

He served as President, Board Director, and Treasurer of the OSGi Alliance for approximately six years, the standards body responsible for the Requirements and Capabilities specification that provides the formal vocabulary for declarative capability matching at the core of Mycelium's design. During that tenure he authored two Alliance position papers — *Modularity* [CITE-OSGI-MOD] and *Complexity, Modularity and Business* [CITE-OSGI-CMB] — which develop the theoretical grounding in complexity and modularity that informs this work. He was awarded the OSGi Laureate (2019) for outstanding contributions to the Alliance and the adoption of OSGi technology, and the OSGi Leadership Award (2019) for his service as a board officer and advocate for OSGi modularity strategy. He was co-architectural lead for the EU Horizon 2020 Brain-IoT project, a federated platform for emergent coordination across heterogeneous IoT and edge devices, with two IEEE publications arising from that work [CITE-BRAIN-IOT-1, CITE-BRAIN-IOT-2].

He holds a D.Phil in Astrophysics from the University of Sussex, based primarily at the Royal Greenwich Observatory, where his research combined large-scale spectroscopic observations of elliptical galaxies using the Anglo-Australian, William Herschel, and Isaac Newton telescopes with kinematic and dynamic modelling from custom Fortran code. He holds a first-class Bachelor's degree in Physics from the University of Manchester.
