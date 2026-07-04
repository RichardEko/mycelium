# Related Work — landscape scan (for §Related Work / §2)

> **Provenance & confidence.** Compiled 2026-07-04 from a web-search + arXiv-fetch snapshot (affiliations
> read from author blocks; maturity from the papers themselves). This is *intel, not an exhaustive
> literature review* — **verify each entry before submission** (titles, author lists, venue/version, and
> especially whether any has since released code). BibTeX keys below are in [`references.bib`](references.bib).

The abstract already frames the coordinator in two forms — **mediated hierarchies** (a central engine
aggregates and commands) and **registry-based discovery** (a global index brokers introductions). The
landscape splits cleanly along that line, and — usefully — a distinct third cluster is now independently
motivating the substrate/emergent alternative *without building it*. That is the ideal shape for §2: the
critique has named targets, and the proposal has fellow-travellers who stop short of the contribution.

## Camp A — the coordinator (the critique's targets)

Named, current instances of the two patterns the paper indicts:

- **Mediated hierarchy — `\cite{cisco-mycelium}`.** Outshift-by-Cisco's *Mycelium* (a **name collision**
  worth a footnote) is the pattern in its purest form: a **CognitiveEngine that mediates negotiation
  "so agents never have to talk directly to each other,"** with rooms + a state machine orchestrating
  multi-issue negotiation (NegMAS). It *names* the coordination problem and answers it with a coordinator
  — the exact move §4 dissects. (This is separate from, and stronger than, the Cisco-internal *audit
  obligation* already cited via Valenti `\cite{...}`.)
- **Mediated hierarchy — `\cite{solace-agent-mesh}`.** Solace's Agent Mesh is A2A-native yet routes task
  breakdown through a specialized **`OrchestratorAgent`** — the coordinator reappearing inside a mesh that
  advertises decentralization.
- **Registry-based discovery** — the A2A/registry meshes (already partly covered in §2) that centralize
  introductions through a global index; the second coordinator "by a different name" of the abstract.

**Use:** these make the critique *concrete and current* — not a straw man. Note the pattern: even systems
that market "mesh"/"swarm" reinstate an orchestrator the moment task decomposition is needed.

## Camp B — the substrate/emergent alternative (fellow-travellers, vision-stage)

Independent work converging on *this paper's* direction (gossip/stigmergy substrate, emergent coherence,
no central orchestrator) — but stopping at vision or a narrow prototype, and **none implementing the
causal-order + consensus + recallable-role machinery** that makes coordination-freeness actually usable.

| Cite key | Origin | Maturity | What it shows / what it lacks |
|---|---|---|---|
| `\cite{geacl}` | **IBM Software, Dublin** (Habiba, *Principal Platform Architect – Agentic AI*; personal capacity) | Vision, no code | Closest sibling: a four-layer **gossip substrate beneath MCP/A2A**, "coherence without central orchestration." *Explicitly* lists failover/roles, hybrid logical clocks, and consensus as **absent** — i.e. this paper's contribution as their open problems. |
| `\cite{habiba-gossip-vision}` | IBM Software, Dublin (same cluster) | Vision | Earlier statement of the same gossip-for-emergent-coordination thesis. |
| `\cite{terrarium}` | **UMass Amherst** (Zilberstein et al.) + **MPI/ELLIS Tübingen** | Framework, security-angled | Revives the **blackboard** for multi-agent safety/privacy — academic kin to the substrate's content-routed sharing; not a coordination-scaling argument. |
| `\cite{evogit}` | **Hong Kong Polytechnic Univ** (Tan, Cheng) | Implemented (narrow) | Coordination **emerges through a shared git version-graph, no central scheduler** — the "substrate not scheduler" instinct, in code, but domain-specific (code evolution). |
| `\cite{pressure-fields-decay}` | **Independent** (Rodriguez, no affiliation) | Conceptual (theorems) | **Pressure fields + temporal decay** = stigmergy + evaporation as formal theory — mirrors the substrate's opacity/load + TTL-evaporation, without a system. |

**Positioning sentence (draft):** *"The substrate direction is not idiosyncratic: an IBM agentic-AI
architect \cite{geacl,habiba-gossip-vision}, a leading multi-agent-systems group \cite{terrarium}, an
evolutionary-computation group \cite{evogit}, and independent theory \cite{pressure-fields-decay} have all,
within a year, motivated gossip/stigmergy substrates as the alternative to mediated coordination. What none
provides — and what this paper contributes — is a working substrate that supplies the causal ordering
(HLC), epidemic consensus, and recallable-role failover such emergence requires to be more than eventual
consistency, together with the composability evidence that the same substrate generates multiple
coordination primitives (§8, the companion crates)."*

## Why this shape helps the paper

- **Prescient, not alone.** Camp B (esp. an *industry* architect at IBM publishing your thesis as future
  work) is strong evidence the field is turning this way — while you shipped it.
- **Sharp differentiation.** The delta is nameable and defensible: *causal order + consensus + recallable
  roles + the recursion*. GEACL literally enumerates the first three as gaps.
- **Concrete critique.** Camp A gives the "coordinator by another name" argument living, dateable targets
  (Cisco Mycelium's CognitiveEngine; Solace's OrchestratorAgent).

## Watch-items (not for the paper — for strategy)

- **IBM.** The closest thinking (Habiba) is an IBM agentic-AI platform architect. Personal-capacity today;
  the one player with resources who's conceptually aligned. Periodically check whether IBM Research
  productizes it.
- **The name.** A Cisco-org project called *Mycelium* in the same space (opposite architecture) is a
  positioning hazard independent of the paper — decide to differentiate loudly or rename.

_Last verified: 2026-07-04 (search snapshot). Re-check for released code / new versions before camera-ready._
