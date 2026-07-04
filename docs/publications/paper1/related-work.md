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
| `\cite{geacl}` | **IBM Software, Dublin** (Habiba, *Principal Platform Architect – Agentic AI*; personal capacity) | Vision, no code | Closest sibling: a four-layer **gossip substrate beneath MCP/A2A**, "coherence without central orchestration." A **state substrate**, not a coordination one — lacks receiver-side admission / scoping / opacity (see *signal/boundary gap* below) as well as HLC, consensus, and roles. |
| `\cite{habiba-gossip-vision}` | IBM Software, Dublin (same cluster) | Vision | Earlier statement of the same gossip-for-emergent-coordination thesis. |
| `\cite{terrarium}` | **UMass Amherst** (Zilberstein et al.) + **MPI/ELLIS Tübingen** | Framework, security-angled | Revives the **blackboard** for multi-agent safety/privacy — academic kin to the substrate's content-routed sharing; not a coordination-scaling argument. |
| `\cite{evogit}` | **Hong Kong Polytechnic Univ** (Tan, Cheng) | Implemented (narrow) | Coordination **emerges through a shared git version-graph, no central scheduler** — the "substrate not scheduler" instinct, in code, but domain-specific (code evolution). |
| `\cite{pressure-fields-decay}` | **Independent** (Rodriguez, solo) | Algorithm + convergence proofs + **simulation benchmarks** (meeting-room scheduling, 1–4 LLM agents) | **Stigmergy + evaporation, genuinely shared** — but an **application-level single-artifact refinement algorithm** (tick-synchronous, a central per-tick selection, single machine), a *different layer* from a distributed substrate. A fellow-traveller / ally in the motivation, not a rival. Full read below. |

**Positioning sentence (draft):** *"The substrate direction is not idiosyncratic: an IBM agentic-AI
architect \cite{geacl,habiba-gossip-vision}, a leading multi-agent-systems group \cite{terrarium}, an
evolutionary-computation group \cite{evogit}, and independent theory \cite{pressure-fields-decay} have all,
within a year, motivated gossip/stigmergy substrates as the alternative to mediated coordination. But these
build a **state substrate** — a coordinator-free layer for agents to *converge on shared knowledge*. What
none provides — and what this paper contributes — is a **coordination substrate**: the signal/boundary
control plane (receiver-side admission, scoped delivery, and opacity-as-emergent-back-pressure) atop
causal ordering (HLC), epidemic consensus, and recallable-role failover, through which coordination
*behaviour* — who acts, when, and how load sheds — emerges rather than being mediated; together with the
composability evidence that the same substrate generates multiple coordination primitives (§8, the
companion crates)."*

### The signal/boundary gap — the load-bearing differentiator

A close read (2026-07-04) of the closest siblings \cite{geacl,habiba-gossip-vision} on *this paper's most
differentiated axis* — the **signal/boundary control plane** (§ Layer II) — is worth stating precisely,
because it moves the contribution from "we added consensus" to "we solve the harder half of the problem."

- **What they have is sender-side.** GEACL has semantic/priority filtering ("safety-critical propagates
  aggressively; routine is throttled") and **peer load *signalling*** (§7.2: agents share workload
  metadata). The vision paper has an *outbound* gossip policy `πf: S→\{0,1\}` and rate limits. All of it
  governs **what a node emits**.
- **What they lack is the receiver-side control plane.** No **admission boundary** (a receiver deciding
  whether to *admit* an incoming signal by local policy — "receiving nodes [do not] reject messages based
  on local policy"); no **scoped delivery** (system/group/individual — they use random peer sampling); no
  **opacity/inhibition** (an overloaded node becoming *unavailable* to shed load — "only load-aware routing
  by senders," never a node refusing admission); and no **stigmergy with evaporation** (no persistent
  markers/gradients).
- **The sharpening for §4/§6.** Their load handling is *load-aware routing by senders* — the sender
  computes a route from a shared view of peer load. That is **coordination-by-mediation in miniature**: a
  decision-maker (the sender) acting on aggregated state. Mycelium's **opacity inverts it** — the
  overloaded node unilaterally goes opaque (a purely *local* decision), and rerouting *emerges* from
  reachability; nobody computes a route. So even the substrate camp, at the exact point it touches
  load-balancing, reaches for a small mediating decision that the boundary/opacity model eliminates. This
  is the paper's *detection-not-prevention / emergent-not-mediated* thesis holding one layer deeper than
  the critique of Camp A.
- **In layer terms:** the IBM vision is essentially **Layer I** (gossip KV convergence) + sender-side
  prioritisation. It does not reach **Layer II** (admission/scoping/opacity) or **Layer III** (consensus).
  The contribution is the two layers they omit — and the evidence that behaviour emerges from them.

_Source: close reading of arXiv:2512.03285v1 and arXiv:2508.01531v1, 2026-07-04. Re-verify their current
versions before camera-ready — a v2 could add ingress admission or opacity._

### The same check across the rest of Camp B

For consistency, the identical signal/boundary interrogation applied to the other three
(2026-07-04). **`~` = partial / different sense; `?` = could not confirm from the fetched text.**

| Work | Receiver-side admission boundary | Scoped delivery | Opacity / self-shed back-pressure | Stigmergy **+ evaporation** | Net characterization |
|---|:--:|:--:|:--:|:--:|---|
| GEACL / Habiba `\cite{geacl}` | ✗ | ✗ | ✗ (sender-side load *routing* only) | ✗ | **state** substrate |
| Terrarium `\cite{terrarium}` | ✗ | `~` (sender sets recipients; blackboard membership via factor graph) | ✗ (treats context-overflow as an **attack to defend**, not load to shed) | ✗ | **instrumented, scoped safety-research blackboard** |
| EvoGit `\cite{evogit}` | ✗ | ✗ | ✗ (agents "operate independently," "no centralized scheduling") | analogy only, **not implemented**; git history is **permanent** — the *opposite* of evaporation | **immutable shared-artifact** coordination |
| Pressure-fields `\cite{pressure-fields-decay}` | ✗ — message-free; a **central per-tick selection** (sort proposals, pick top-κ) replaces admission | ✗ — "regions" are artifact subdivisions, not addressing scopes | ✗ — "inhibition" = *region cooldown*; no node/load model at all (single-machine tick loop) | **✓ core thesis + benchmarks** | **stigmergy kin at a *different layer*** — an application-level single-artifact refinement algorithm, not a distributed substrate |

**Read this honestly — it is where the paper could over-reach.** The pressure-fields work
\cite{pressure-fields-decay} genuinely shares the stigmergy-plus-evaporation core *and* reports
benchmarks, so **do not claim to have invented emergent or stigmergic coordination** — that claim would
not survive review. The defensible differentiation narrows to three things it (and the rest of Camp B) do
not have:

1. **The receiver-side *boundary*, not a global field.** A pressure field is a shared structure agents
   *follow* (attraction/allocation); Mycelium's boundary/opacity is a purely *local* admission decision a
   node makes about *itself* — overload → the node goes opaque → rerouting emerges from reachability, with
   no field to compute or share. Field-following is a softer, still-global shaping; boundary/opacity is
   fully local. (State this carefully — it is a real but subtle distinction, and reviewers will probe it.)
2. **Integration, not a single mechanism.** Camp B each isolates *one* idea — gossip convergence (IBM), a
   scoped blackboard (Terrarium), a shared version-graph (EvoGit), a decay field (pressure-fields).
   Mycelium's claim is the *integrated three layers* — causal KV (HLC) **+** signal/boundary mesh **+**
   epidemic consensus — with recallable-role failover, such that the layers compose.
3. **The recursion.** None demonstrate the substrate *generating multiple distinct coordination primitives*
   on one public API (§8, the five companion crates). That composability evidence is unique to this work.

Two useful contrast lines fall out: **Terrarium** inverts the load story — it treats context-overflow as
an *attack surface to defend*, where Mycelium treats overload as a *signal to route on* (defence vs
metabolism). **EvoGit** inverts the memory story — an *immutable, permanent* git history, where Mycelium's
whole model is *evaporation/TTL* (permanence vs forgetting-as-a-feature).

### Pressure-fields — the full read (2026-07-04, pp. 1–24: abstract, §2 related work, §3 model, §4 method + Algorithm 1, §5 all theorems, §6 setup)

The provisional row above resolved on a proper read, and the finding is *better than "closest competitor"*
— it is **not a competitor at all, but a fellow-traveller one layer up.** Precisely:

- **What it is.** A paradigm for **collaborative artifact refinement by LLM agents**: *one* shared artifact
  of `n` "regions"; a **quality-pressure field** `P(s)=Σ Pᵢ` derived from local signal functions; agents
  greedily propose region patches that lower local pressure; **temporal decay** erodes fitness to force
  continued exploration. Formalised as a potential game with convergence proofs (Thm 5.1/5.3), and
  **evaluated in single-machine simulation** on meeting-room scheduling (qwen2.5:0.5–3B via Ollama, 1–4
  agents) vs conversation/hierarchical/sequential/random baselines.
- **Genuinely shared with Mycelium (state it, credit it):** stigmergy + evaporation as the coordination
  principle; locality (agents see only their region); **role-free**; "coordination from shared state, not
  messages"; and — usefully — its §5.4 makes the *same coordinator critique* (centralized = SPOF,
  hierarchical = manager cascade, message-passing = O(n²)). It is an **empirical ally for the motivation**,
  not a threat to the contribution. **Do not claim to have invented emergent/stigmergic coordination.**
- **Why it is a different artifact, not a rival substrate:** it is **tick-synchronous** (Algorithm 1 is one
  central control loop: Decay → Proposal → **Validation & Selection** → Reinforcement); Phase 3 is an
  explicit **global reduce** ("sort validated patches, greedily select top-κ") — i.e. a *central per-tick
  selection*, the opposite of a local admission boundary; there is **no distribution** — a single shared
  artifact on one machine, no network, no partitions, no replication, no wire, no logical clocks; its
  "consensus" is potential-game convergence (Nedić/Monderer), not epidemic quorum; its "graceful
  degradation" is *stateless-agent* fault tolerance (proposers can come and go), **not** a fault-tolerant
  distributed store. Its "inhibition" is a **region cooldown**, not node opacity/load-shedding.
- **The clean framing for §2.** Pressure-fields is an **application-level coordination *pattern*** (how N
  agents refine one artifact); Mycelium is the **distributed *substrate*** (multi-node gossip KV + signal/
  boundary mesh + epidemic consensus + failover + wire, an embeddable library). They compose rather than
  compete — pressure-field coordination is exactly the kind of pattern that could *run on* a Mycelium-like
  substrate (its "shared artifact" ≈ a replicated KV; its agents ≈ nodes). So the honest contribution
  statement is not "we did stigmergy too" but **"we provide the distributed substrate — causal KV,
  receiver-side boundary/opacity, epidemic consensus, recallable-role failover — that application-level
  emergent-coordination schemes like this one assume but do not supply."**

_Confidence: high for pp. 1–24 (the substance); the appendix proofs (§B) and full result tables (§6.2–6.9)
were not read line-by-line but are not load-bearing for this characterization._

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
