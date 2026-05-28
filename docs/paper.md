# The Coordinator Trap: Why Mediated Multi-Agent Architectures Cannot Scale and a Substrate-Based Alternative

**Authors:** TBD  
**Target venue:** AAMAS 2027 — International Conference on Autonomous Agents and Multi-Agent Systems  
**Status:** First draft — 2026-05-27

---

## Abstract

Multi-agent coordination systems are converging on two dominant architectural patterns: mediated hierarchies, in which a central engine aggregates agent outputs, decides, and issues commands; and registry-based discovery, in which a global index brokers introductions between agents. Both patterns introduce a coordinator — by different names — that becomes a bottleneck, a single point of failure, and an unbounded audit obligation as agent populations grow. We demonstrate that these failure modes are not implementation deficiencies but structural consequences of the coordinator assumption itself. Drawing on Holland's framework for Complex Adaptive Systems, we propose an alternative in which coordination emerges from substrate properties rather than explicit protocols. We present Mycelium, a working implementation of this model as an embeddable Rust library, and show how its three-layer architecture — gossip KV store, signal mesh with boundary admission, and epidemic consensus — structurally eliminates each failure mode rather than ameliorating it. We further show that Mycelium and MIT's NANDA registry are complementary layers at different scales: NANDA handles internet-scale cross-organisational discovery; Mycelium handles everything inside the cluster. The two layers compose without modification to either.

---

## 1. Introduction

The deployment of autonomous AI agent fleets is accelerating faster than the coordination infrastructure beneath them can reliably support. The academic literature is beginning to document the consequences. Silo-Bench [CITE-SILOBENCH], a 2025 empirical study of distributed multi-agent LLM systems, finds that "coordination overhead compounds with scale, eventually eliminating parallelization gains entirely" — adding more agents actively reduces system efficiency beyond a threshold population. A 2025 survey of multi-agent coordination architectures [CITE-MAS-SURVEY] concludes that the dominant paradigm — centralised orchestrators with predefined agent topologies — "introduces single points of failure and scalability bottlenecks" that are structural rather than incidental. The RAPS framework paper [CITE-RAPS] identifies predefined communication topologies as a fundamental architectural constraint, arguing that the community's reliance on manual orchestration "struggles to generalise across shifting task distributions" and represents an unresolved scalability dilemma. Carvalho [CITE-CARVALHO] observes that modern AI orchestration frameworks are independently rediscovering coordination solutions from the 1980s and 1990s without engaging with that literature — repeating the same architectural mistakes that were documented and, in some cases, partially solved forty years ago.

These academic findings are confirmed in practitioner experience. Valenti [CITE-SIGNAL-NOISE] documents an unbounded audit obligation arising from AI-mediated agent coordination deployed at Cisco: all artifacts, regardless of the effort or reasoning behind them, arrive with identical surface polish, making quality assessment unsustainable at scale. She writes: *"The three-hour issue and the five-second issue have the same voice, the same structure, the same length."* In a companion account [CITE-CONTEXT-PROBLEM], the same author documents agents losing all state between sessions, restarting from zero, and re-litigating decisions already settled — and an attempted fix that introduces a shared knowledge graph that drifts, exactly the failure mode already diagnosed elsewhere.

We argue that these failure modes — documented independently by academic measurement and practitioner experience — are not bugs in specific implementations. They are structural consequences of a single shared assumption: that a designated component must hold authoritative state about the agent population and through which coordination passes. We call this the *coordinator assumption*, and we show that any architecture built on it must exhibit these failure modes regardless of how carefully it is engineered.

This paper makes four contributions:

1. **A historical account** of how the coordinator assumption has persisted across fifty years of distributed computing research — from actor models to blackboard systems to modern LLM orchestration frameworks — and why no prior system eliminated it.
2. **A precise causal account** of how the coordinator assumption produces each documented failure mode, through an agent-theoretic lens.
3. **A theoretical foundation** for coordinator-free multi-agent coordination grounded in Holland's signal/boundary model for Complex Adaptive Systems [CITE-HOLLAND].
4. **Mycelium** — a working implementation of that model as an embeddable Rust library — demonstrating that each failure mode is structurally impossible in the coordinator-free substrate, and that it composes naturally with MIT NANDA [CITE-NANDA] for internet-scale federation.

The remainder of this paper is organised as follows. Section 2 traces the history of the coordinator assumption across five decades. Section 3 describes its two dominant contemporary expressions. Section 4 traces the causal chain from mediated hierarchy to each observed failure mode. Section 5 presents Holland's theoretical framework. Section 6 situates Mycelium within the relevant prior art. Section 7 presents the Mycelium architecture layer by layer. Section 8 addresses NANDA composability. Section 9 presents evaluation. Section 10 discusses implications and future work.

---

## 2. A History of the Coordinator Assumption

The coordinator assumption — that a designated component must hold authoritative state and through which coordination passes — is not new. It has been the default in distributed computing for over fifty years. Each generation has recognised the problems it causes and attempted to mitigate them without eliminating the assumption itself.

### 2.1 Blackboard Systems (1970s–80s)

The blackboard model, most prominently realised in the HEARSAY-II speech understanding system [CITE-HEARSAY], organises coordination around a central shared data structure — the blackboard — and a set of specialised knowledge sources that read from and write to it. A control component monitors the blackboard and decides which knowledge source to activate next. The pattern is explicit: central state, central control. Knowledge sources do not communicate directly; they communicate via the coordinator. HEARSAY demonstrated that the model could solve hard problems. It also demonstrated the failure modes: the control component became a bottleneck and a single point of failure, and the blackboard accumulated stale entries as the system evolved.

### 2.2 The Actor Model (Hewitt, 1973)

Hewitt, Bishop and Steiger's Actor model [CITE-HEWITT] provided the first formal model of concurrent computation as communicating entities. Actors maintain private state and communicate exclusively via asynchronous message passing. The model eliminates shared mutable state — a genuine advance — but retains explicit addressing: to send a message, an actor must know the recipient's address. The emitter-must-know-who-is-listening assumption is structural. Topology must be managed explicitly. An actor that changes its role or capabilities cannot automatically re-wire the communication patterns of its senders. The Actor model correctly identified agents as the unit of composition; it did not identify boundaries as the mechanism for self-organising communication.

### 2.3 Linda and the Tuple Space (Gelernter, 1985)

Gelernter's Linda [CITE-LINDA] introduced generative communication via a shared tuple space. Processes write typed tuples to a shared medium; other processes match and retrieve them by pattern. This is the closest prior art to Holland's signal propagation model: the medium holds state; agents interact with the medium rather than with each other directly. Carriero and Gelernter's 1992 CACM paper [CITE-LINDA-COORD] established that coordination is a distinct language design concern, separable from computation — a correct and underappreciated insight.

What Linda retained, however, is a shared coordinator: the tuple space itself must be accessible, consistent, and available to all participants. Fault-tolerant variants (FT-Linda, JavaSpaces) required replication schemes and consistency protocols for the shared space. The medium is the coordinator. The coordinator trap persists in a subtler form.

### 2.4 BDI Agents and FIPA Standards (1990s)

The Belief-Desire-Intention (BDI) agent architecture [CITE-BDI] and the FIPA agent communication standards [CITE-FIPA] brought deliberative reasoning to multi-agent systems. FIPA defined a Directory Facilitator (DF) — a registry where agents register capabilities and query for peers — and an Agent Management System (AMS) as lifecycle controller. The DF is precisely the registry-based pattern that MIT NANDA echoes today, four decades later: a designated component holds authoritative state about the agent population. The JADE framework [CITE-JADE], the dominant FIPA implementation, made the DF a hard architectural requirement.

### 2.5 The Pattern That Persists

Across five decades, each model correctly identified coordination as a concern distinct from computation. None eliminated the coordinator assumption:

| Model | Coordinator retained |
|---|---|
| Blackboard (HEARSAY, 1980) | Central blackboard + control component |
| Actor model (Hewitt, 1973) | Explicit addressing — sender must know recipient |
| Linda (Gelernter, 1985) | Shared tuple space as coordinator |
| BDI / FIPA / JADE (1990s) | Directory Facilitator as registry |
| Modern LLM orchestration (2020s) | Central orchestrator / CognitiveEngine |

Carvalho [CITE-CARVALHO] observes that modern AI agent frameworks are independently rediscovering Linda's tuple space pattern — implementing ad-hoc shared stores with task claiming, polling, and matching — without awareness of forty years of research on its failure modes and partial solutions. The community is not learning from history. It is repeating it.

The missing step in all prior work is the same: the elimination of the coordinator from the substrate entirely, replaced by a substrate property — Holland's signal/boundary model — that makes coordinator-free coordination not merely possible but structurally necessary.

---

## 3. Background: The Two Dominant Patterns

### 3.1 Mediated Hierarchy

The most widely deployed pattern for multi-agent coordination is the mediated hierarchy. A designated coordinator — variously called an orchestrator, a cognitive engine, or a manager agent — receives outputs from all agents, synthesises them, and issues directives back to the agent population.

Cisco's internal Mycelium project [CITE-CISCO-MYCELIUM] is a well-documented contemporary instance of this pattern. A CognitiveEngine mediates all agent interaction. Agents never communicate directly. When coordination is required, the engine decomposes the agents' natural-language positions into discrete issues, runs a NegMAS Stacked Alternating Offers (SAO) negotiation protocol over multiple rounds [CITE-NEGMAS], synthesises a result, compiles it into a plan, and broadcasts it to all participants. The demonstration case shows two agents reaching agreement on a transaction over fourteen negotiation rounds. The system includes a 300-second watchdog timeout to abort sessions where agents become unresponsive, and explicitly notes that coordination state is held in memory and lost on server restart.

Kubernetes [CITE-K8S] represents a more mature instance of the same architectural pattern. A control plane holds the declared target state; reconciliation loops continuously compare actual cluster state to that target and issue corrective actions. The coordinator (the control plane) is more sophisticated than a negotiation engine, but the structural relationship is identical: state is held above the runtime, and correctness is enforced top-down.

### 3.2 Registry-Based Discovery

The second dominant pattern is registry-based discovery. Rather than a runtime coordinator, a persistent global registry stores agent capability declarations. Agents query the registry to locate peers with matching capabilities; the registry brokers the introduction.

MIT's NANDA project [CITE-NANDA] is the leading contemporary instance at internet scale. NANDA implements a decentralised registry — described by its authors as "DNS for agents" — in which agents register structured AgentFacts including capability declarations, credentials, and behavioural history, anchored to W3C Decentralised Identifiers (DIDs). Other agents query the registry; NANDA's adapter layer handles protocol bridging across MCP, A2A, and NLWeb. The project envisions a "Quilt" federation of enterprise, government, Web3, and civil-society registries composing into an internet-scale agent discovery infrastructure.

### 3.3 The Shared Assumption

Superficially, these two patterns are different. A mediated hierarchy coordinates behaviour at runtime; a registry brokers introductions at discovery time. But they share a single structural assumption: **there exists a designated component that holds authoritative state about the agent population and through which coordination passes.**

In the mediated hierarchy, this component is the coordinator. In the registry model, it is the registry. Both are coordinators by different names. Both inherit a common set of failure modes that we examine in the next section.

---

## 4. The Coordinator Trap: From Architecture to Symptom

We use the term *coordinator trap* to describe the set of failure modes that any architecture inheriting the coordinator assumption must exhibit. These are not bugs; they are structural properties. A better coordinator does not escape them. A smarter mediator does not escape them. The trap is the coordinator's existence, not its quality.

We trace each failure mode through the agent lens — asking not merely what the system does wrong but what it implies about the agents' relationship to information, state, and decision.

### 4.1 The Mediated Hierarchy Described Precisely

In a mediated hierarchy, agents are workers in a fanout RPC system. They receive structured payloads from the coordinator, produce responses, and return them. The coordinator aggregates responses, applies its decision logic, and broadcasts results. The agent has no autonomy over what signals it receives — it processes whatever the coordinator sends. Its "boundary" — the set of signals it acts upon — is managed externally by the coordinator's routing decisions, not declared internally by the agent itself.

This is a critical point. When we call these components "agents" we are importing a concept from the Complex Adaptive Systems literature that implies agents hold their own receptor sets, evaluate incoming signals against those sets, and act selectively. In a mediated hierarchy, agents do none of this. They are passive processors of coordinator-routed payloads. We return to this category error in Section 3.5.

### 4.2 Audit Burden: The Consequence of Post-Admission Broadcasting

Valenti's first problem — the unbounded audit obligation — follows directly from the coordinator's broadcast model.

The coordinator receives all agent outputs and produces a synthesised result. That result is broadcast to all downstream consumers: other agents, human reviewers, monitoring systems. Every output of every agent passes through the coordinator and produces a coordinator-level artifact with identical structure and apparent authority, regardless of the quality or validity of the underlying agent reasoning.

This is post-admission filtering: artifacts are produced first; quality is assessed after. The coordinator is the filter, but it operates after production, not before. A coordinator that synthesised more carefully would reduce noise, but it could never eliminate the audit obligation entirely because quality assessment requires domain expertise that the coordinator cannot possess for every domain it mediates. As agent populations grow, the coordinator's cognitive load grows linearly with the number of domains it must evaluate. The audit burden scales with the coordinator.

The three-hour issue and the five-second issue have the same voice because the coordinator processes them identically. There is no mechanism in the architecture to distinguish them before they produce output. The distinction, if it is made at all, must be made by a human reviewer after the fact — which is precisely the unbounded obligation Valenti describes.

### 4.3 Context Loss: The Consequence of Coordinator-Held State

The second failure mode — agents restarting from zero, re-litigating settled decisions — follows from the coordinator's role as the system's memory.

In a mediated hierarchy, shared state lives in or near the coordinator. Agents are stateless workers; they hold no durable view of prior decisions. When the coordinator restarts (or in the case of Cisco's Mycelium v1, simply when the server process is restarted), the state is gone. When a new agent joins, it has no independent access to prior decisions — it must query the coordinator, which synthesises a catchup briefing from whatever logs survive.

This is the classic distributed systems mistake: centralising state simplifies the coordinator's internal logic but makes every participant dependent on the coordinator's availability and memory continuity. The agents are not resilient in isolation because they were never designed to hold state independently. The coordinator *is* the memory; the agents are its terminals.

Valenti's attempted fix — a filesystem-based shared memory — correctly identifies that state should be held outside the coordinator. But it introduces a shared mutable store with no eviction policy: entries persist indefinitely, accumulate, and drift from current system state. This is not a solution to the coordinator trap; it is a redistribution of the same problem.

### 4.4 Output Format Mismatch: The Consequence of Absent Boundary Model

The third failure mode — agent logs structured for machines, routed into human channels and failing to communicate — follows from the coordinator's undifferentiated broadcast.

When the coordinator synthesises a result and broadcasts it, it has no model of its recipients' boundaries — no representation of which consumers can act on which signals, in which form, at which level of detail. All downstream consumers receive the same output because, from the coordinator's perspective, they are all equivalent downstream consumers of its decisions. The coordinator routes to roles or channels, not to declared receptor sets.

A human reviewer is not equivalent to an agent as a consumer of a coordination decision. A human can skim for significance, asks different questions, has a different attention budget and a different tolerance for structured verbosity. But the coordinator cannot know this without a model of recipient boundaries — and building and maintaining such a model is precisely the kind of overhead that grows without bound as the agent population grows.

### 4.5 The Category Error: Workers Dressed as Agents

The three failure modes share a common root: in a mediated hierarchy, components called "agents" are not agents in any theoretically meaningful sense.

Holland [CITE-HOLLAND] defines an agent — in the CAS sense established in Section 5 — as an entity that holds a *boundary* — a receptor set specifying the conditions under which it acts on incoming signals. The boundary is the agent's own declaration of its domain. It is intrinsic, not delegated. A component whose domain is determined by the coordinator's routing decisions — which receives what the coordinator sends, not what it declares itself competent to receive — is not an agent. It is a worker node in a fanout RPC system.

This is not a semantic quibble. The distinction determines whether filtering happens before or after production (pre-admission vs post-production), whether state is held distributed or centralised (inside each agent vs inside the coordinator), and whether the system tolerates coordinator failure gracefully (yes, because agents hold their own state and boundaries) or catastrophically (yes, because agents are coordinator-dependent).

Calling these components "agents" while designing them as workers inherits none of the architectural properties that make genuine agents scalable. It merely imports the vocabulary without the substance.

---

## 5. Theoretical Foundation: Holland's Signal/Boundary Model

John Holland's framework for Complex Adaptive Systems [CITE-HOLLAND], formalised in *Signals and Boundaries: Building Blocks for Complex Adaptive Systems* (MIT Press, 2012), provides the theoretical basis for a coordinator-free architecture.

Holland's thesis: the behaviour of complex adaptive systems emerges from two and only two primitives.

**Signals** propagate through a medium unconditionally. No signal is withheld from propagation on the basis of who might act on it. The medium floods.

**Boundaries** are receptor sets. Each agent holds a boundary — a set of conditions under which it *acts* on a signal. The boundary controls acting, not receiving. Forwarding is always unconditional.

This inversion is the key insight. Conventional distributed systems thinking routes messages to known recipients. The emitter must know who is listening; topology must be managed explicitly. Holland's model changes the medium: any agent whose boundary matches a signal responds. The emitter does not need to know who is listening. Topology does not need to be managed explicitly. The system tolerates churn without stalling because there is no routing table to maintain, no coordinator to query, no registry to consult.

**The coordinator trap dissolved.** In Holland's model there is no coordinator because none is needed. Filtering is pre-admission, not post-production: an agent whose boundary does not match a signal produces no response to it — no artifact, no log entry, nothing to audit. State is held distributed, inside each agent as its capability and requirement declarations: there is no coordinator memory to lose. Recipients are self-differentiating: each agent's boundary determines what it receives, so the output format mismatch between humans and agents is a boundary design problem, not a routing problem.

**Stigmergy and TTL evaporation.** Holland identifies stigmergy — state left in the medium by agents, readable by other agents — as the mechanism through which agents coordinate without direct communication. In biological systems this takes the form of pheromone trails: a path walked frequently stays fresh; an unused path fades. Applied to distributed state management, this suggests that shared state should carry a time-to-live and evaporate unless actively refreshed. An agent that is alive and relevant keeps its state fresh; a dead or departed agent's state simply fades. There is no explicit deregistration, no failure detection protocol, no tombstone management required. Failure is emergent: the absence of refreshment *is* the failure signal.

---

## 6. Prior Art: Correct Concepts, Wrong Implementation

Three prior systems identified the correct underlying concepts but implemented them with too much protocol ceremony, or in the wrong deployment model, to achieve their full potential.

### 6.1 OSGi Requirements and Capabilities

The OSGi Alliance [CITE-OSGI] formalised a dependency model in which software modules declare capabilities they provide and requirements they need; a resolver matches providers to consumers. The primitive is correct: declarative matching between providers and consumers, with the resolver handling wiring.

What mainstream OSGi adoption got wrong was treating resolution as static — performed once at bundle-install time. This made it unsuitable for dynamic systems where participants come and go. The resolver ran at deploy time; a module that disappeared at runtime left a gap with no mechanism for repair.

### 6.2 Paremus Service Fabric and the Reconciliation Engine

Paremus Service Fabric (circa 2010–2015) [CITE-PAREMUS] demonstrated that the OSGi Requirements and Capabilities model could be applied as a *continuous runtime* resolver — re-resolving dependencies as services appeared, disappeared, and changed, adapting the running system accordingly rather than requiring a redeploy.

To be precise about what Paremus achieved: the R&C graph was a *declared target state*. The runtime was continuously monitored against that target, and deltas were driven back into convergence whenever they appeared. This is closer to the Kubernetes control loop than to a shared knowledge graph — drift from declared intent was structurally prevented, not merely discouraged.

What Paremus still required was a *central reconciliation engine* holding that target state. The engine computed deltas; the engine issued corrections; the engine was the coordinator. If it went down, reconciliation stopped. The coordinator trap in a more sophisticated form.

A second lesson from Paremus is positional rather than architectural: Service Fabric was deployed as *runtime infrastructure* — a platform that sat beneath the application and managed it. This placed it in direct competition with VMware, Docker, Mesosphere, and later Kubernetes — organisations with enormous resources, existing enterprise relationships, and vast integration surface. The architecture was superior; the market chose familiar and good-enough. The lesson: substrate must be a library embedded in the caller's process, not a platform competing for deployment slots.

### 6.3 Jini and the Lease Insight

Jini [CITE-JINI-ARCH, CITE-JINI-SPEC] introduced the insight that distributed resource registrations should *decay* rather than persist indefinitely. A service holds a lease on its registration; if it does not renew, the registration expires. This provides implicit failure detection without requiring an explicit deregistration protocol.

The insight is correct. The implementation was protocol-heavy: explicit `Lease` objects, `renew()` RPCs, a lease manager, explicit cancellation. The ceremony obscures the substrate property — that registrations should evaporate unless actively maintained — behind an explicit lifecycle protocol.

### 6.4 The Strip-the-Ceremony Pattern

A pattern emerges across all three cases:

| Prior art | Correct concept | Implementation ceremony | Substrate property equivalent |
|---|---|---|---|
| Jini | Registrations should decay | `Lease.renew()` RPC + lease manager | TTL as natural evaporation |
| OSGi | Declarative capability matching | Static bundle-install resolver | Continuous evaluation against live state |
| Paremus | Continuous reconciliation toward target state | Central reconciliation engine + target state graph | Gossip mesh — every TTL refresh is a reconciliation tick |

The pattern that produces better architecture: identify the correct concept, find the substrate *property* that produces the same behaviour without an explicit protocol, implement the property and let the behaviour emerge. When a proposed feature requires a manager, a coordinator, an explicit lifecycle protocol, or a renewal RPC, apply this heuristic before accepting it.

---

## 7. Mycelium: A Coordinator-Free Substrate

Mycelium is an embeddable Rust library implementing Holland's signal/boundary model. It has no daemon, no control plane, no installer, no orchestrator. It is a crate embedded in the caller's process. The operator's existing infrastructure — Kubernetes, bare metal, cloud VMs, edge devices — is irrelevant because Mycelium does not touch it.

The architecture is three layers. We present each layer alongside the failure mode it structurally eliminates.

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

---

## 8. The Two-Layer Stack: Mycelium and NANDA

NANDA [CITE-NANDA] and Mycelium are not competing architectures. They operate at different scales and compose without modification.

NANDA addresses the internet-scale cross-organisational discovery problem: how does an agent in one organisation find and verify an agent in a completely different organisation, across the open internet, with cryptographic identity assurance? This requires a registry of some kind — some external anchoring infrastructure to cross organisational boundaries. NANDA's decentralised registry, DID-anchored AgentFacts, and protocol bridging (MCP, A2A, NLWeb) are the right architecture for that scope.

Mycelium addresses the intra-cluster coordination problem: how do agents within a cluster coordinate without a broker? This does not require a registry because capability state is continuously gossiped through the mesh — every node's view of the cluster is already local, sub-millisecond, and always fresh.

The composability result is straightforward. Mycelium exposes a `/.well-known/agent.json` endpoint as a conforming A2A server. NANDA's paper [CITE-NANDA] identifies the A2A path as the standard entry point for internet-visible agents. A single NANDA `AgentAddr` record pointing at a Mycelium cluster's A2A endpoint registers the entire cluster on the agent internet. No Mycelium code changes are required. NANDA covers cross-org discovery and VC-signed attestation; Mycelium covers everything inside the cluster.

NANDA's own analysis [CITE-NANDA] identifies a "trusted intra-cluster path" requiring sub-millisecond local writes, no external DNS, and Ed25519-signed state. Mycelium's Layer I (gossip KV, wire v10 with Ed25519-signed updates) is precisely that path. The two systems were designed independently against the same requirements at their respective scales; they compose naturally.

---

## 9. Evaluation

### 9.1 Coordination Convergence Time

[BENCHMARK: single-ballot `group_propose` vs NegMAS SAO N-round negotiation for equivalent 3-agent coordination decision. Expected: single round vs up to 300s. Measure: wall clock from proposal emit to commit write in KV.]

[BENCHMARK: `cross_group_propose` with 2 groups × 3 nodes each vs mediated hierarchy equivalent. Measure: same.]

### 9.2 Failure Tolerance

[BENCHMARK: coordinator failure in mediated hierarchy vs random node failure in Mycelium cluster. Measure: coordination availability before and after failure, recovery time. Expected: mediated hierarchy coordination halts on coordinator failure; Mycelium degrades gracefully to reduced quorum.]

### 9.3 State Freshness Under Churn

[BENCHMARK: TTL evaporation vs knowledge graph drift rate. Introduce node departures at varying rates. Measure: time from departure to state evaporation in Mycelium (expected: TTL period, ~5s default) vs time to stale entry detection in filesystem-based memory (expected: unbounded without explicit eviction).]

### 9.4 Audit Obligation Under Load

[BENCHMARK: artifacts produced per coordination decision as agent population grows from 3 to 30. Mediated hierarchy: O(N) — each agent's output passes through coordinator and generates artifact. Mycelium: O(matching) — only agents whose boundary matches the signal produce responses.]

### 9.5 Existing Integration Evidence

Mycelium's correctness across its three-layer architecture is validated by 239 unit tests and 11 integration scenarios run against a live 5-node Docker cluster. Scenarios cover KV replication under partition and reconnection, signal delivery and boundary admission, capability group formation and dissolution, consensus quorum under node failure, cross-group voting, and the full Agentic Flow Networks pipeline. All 11 scenarios pass at HEAD.

---

## 10. Discussion

### 10.1 Why the Market Chose Ceremony

Kubernetes won over Paremus Service Fabric not because it was architecturally superior but because it was operationally familiar. Container-based composition mapped onto existing mental models of processes, services, and deployments. Paremus's continuous dynamic resolution was more powerful and more correct, but it required operators to adopt a new conceptual framework. The market chose "good enough and familiar" over "correct and novel."

The same dynamic is visible today. Mediated hierarchies for AI agents map onto existing mental models of managers and workers, orchestrators and tasks. Registry-based discovery maps onto DNS — a metaphor every network engineer already holds. Both are familiar. Neither is correct at scale.

The purpose of this paper is to place the correct architectural argument on record before "good enough and familiar" consolidates. The academic literature is the appropriate venue for that argument: it establishes prior art, provides a citable reference for practitioners who encounter the coordinator trap, and invites empirical challenge.

### 10.2 The Hayek Parallel

The coordinator trap is not a new discovery. Friedrich Hayek described it in 1945 — for economies.

In *"The Use of Knowledge in Society"* [14], Hayek argued that the central planning problem is not computational — it is epistemic. No central planner can possess the distributed, local, tacit knowledge held by individual market participants. Prices are not just numbers; they are signals that aggregate and propagate dispersed knowledge through the economy without anyone needing to understand the whole. Attempts to replace this with a central planning apparatus fail not because planners are incompetent but because the knowledge required for correct decisions is structurally inaccessible from any central point.

The parallel to the coordinator trap is exact. A planned economy and a mediated hierarchy share the same failure mode: a central node that must aggregate knowledge it cannot fully possess, synthesise decisions on behalf of participants who understand their local context better than any coordinator can, and issue commands downward into a system whose ground truth is always more current at the edges than at the centre. A market economy and Mycelium share the same solution: signals propagate local knowledge unconditionally; participants act on signals that match their position; emergent order arises from those local interactions without any node needing a view of the whole.

Hayek's market is a signal/boundary system. He just did not have Holland's vocabulary.

The intuition that a sufficiently intelligent coordinator — with enough information and computing power — could outperform the distributed system is seductive precisely because it *feels* like it should work. The appearance of control is reassuring even when it is structurally impossible. This is why mediated hierarchies keep being built despite their failure modes: the coordinator looks like it is in control. The appearance of coordination is reassuring even as the audit burden accumulates and context is lost on every restart.

The insight is the same in all three cases: **distributed local knowledge, expressed through signals and boundaries, produces emergent order that no coordinator can match.**

### 10.3 The Beinhocker Parallel: Organisations as Complex Adaptive Systems

The tension between centralised control and distributed adaptation is not confined to computer science or economics. Beinhocker's *The Origin of Wealth* [CITE-BEINHOCKER] — applying complexity economics to business strategy — argues that organisations face the same epistemic constraint Hayek identified for economies. Hierarchies that route decisions through central command chains cannot process environmental signals fast enough to remain adaptive: by the time local knowledge has been aggregated upward, synthesised centrally, and directives issued downward, the environment has changed. The hierarchy is always acting on a stale picture of the world.

Beinhocker's prescription is structurally identical to Holland's: successful organisations are not machines with a central controller but ecosystems where agents experiment, adapt, and coordinate through local interactions. Strategy is not a plan issued from the top but an emergent property of many parallel experiments, each agent acting on the signals it receives in its local environment. The organisation that wins is not the one with the best central planner but the one whose agents are best equipped to act on local signals without waiting for central direction.

This is the coordinator trap expressed at organisational scale. The CognitiveEngine coordinating AI agents is the same structure as the central command chain coordinating human organisations — and fails for the same reason. The solution Beinhocker prescribes for firms is the solution Holland prescribes for complex systems and Mycelium implements for agent fleets: move the intelligence to the boundary, let coordination emerge, abandon the fiction of central control.

### 10.4 Limitations

Mycelium assumes a cluster the operator owns. Cross-organisational discovery is NANDA's problem, not Mycelium's. Ephemeral signals are intentionally not durable — a node that misses a signal misses it; durable delivery is a higher-order concern built on the KV layer or consensus, not a substrate property. The gossip substrate assumes eventual connectivity; a fully partitioned cluster cannot converge.

Boundary admission requires agents to declare their boundaries correctly. A misconfigured boundary — too broad or too narrow — produces incorrect routing without any coordinator to catch the error. This places a correctness obligation on the capability declarations that the mediated hierarchy places on the coordinator instead. Neither is strictly easier; the burden is different in character.

### 10.5 Future Work

- **Empirical comparison** against a deployed mediated hierarchy at equivalent agent counts — the placeholder benchmarks in Section 8.
- **Formal verification** of the signal/boundary substrate properties using TLA+ or similar.
- **NANDA integration study** — practical experience deploying a Mycelium cluster as a NANDA-registered entity and measuring discovery latency across organisational boundaries.
- **Signal reorder buffer** — receiver-side per-(sender, kind) HLC-keyed causal delivery for applications requiring strict signal ordering.

---

## 11. Conclusion

The coordinator is not a solution to the multi-agent coordination problem. It is a restatement of the problem at a different scale. Mediated hierarchies make the coordinator responsible for filtering, memory, routing differentiation, and fault tolerance — properties that in a correctly designed substrate belong to the agents themselves. Registry-based systems distribute discovery without eliminating the coordinator; the registry is a coordinator with a narrower mandate.

The failure modes documented by practitioner experience — unbounded audit obligation, context loss across restarts, output format mismatch between heterogeneous consumers — are not bugs in specific implementations. They are structural consequences of the coordinator assumption, predictable from first principles, and irreducible by improving the coordinator.

Holland's signal/boundary model provides the theoretical basis for a different architecture: one in which coordination emerges from substrate properties rather than explicit protocols. Mycelium implements this model as an embeddable library. Each of its three layers — gossip KV store, signal mesh with boundary admission, epidemic consensus — addresses a mirrored failure mode, not by handling the failure gracefully but by making it structurally impossible.

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

[14] F. A. Hayek, "The Use of Knowledge in Society," *American Economic Review*, 35(4):519–530, September 1945.
