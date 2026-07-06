# domain/pattern-coverage — coordination-pattern coverage vs the substrate

↑ [domain/](domain.md) · positioning artifact (external landscape → link, don't copy). Coverage
claims are **code-anchored**: a `Native` row names the primitive; verify against the cited file.

**Scope (read this first — the matrix is *coordination* patterns, not the whole agentic space).**
This page maps the *coordination / state / interop* patterns — who talks to whom, shared state,
discovery, consensus. It deliberately does **not** claim to cover the *data*, *human*, and *content-
safety* concerns of a full agentic system (RAG/retrieval, human-in-the-loop, content guardrails):
those are **use-case functions**, not substrate patterns — external services a Mycelium group
*accesses through the mesh*, exactly as `mycelium-wiki` accesses a node-independent store. See
*Use-case functions* below. (One safety concern — *structural* guardrails — is a substrate strength,
also below.) Earlier drafts of this page overclaimed "the agentic space"; this scoping is the fix.

**Question this answers:** does the coordinator-free substrate cover the *coordination* patterns the
field has converged on? **Nearly all — *natively* or *by composition of native
primitives*. Exactly one item (ANP wire-protocol conformance) needs genuinely new code; the
orchestrator pattern is a deliberate non-goal, not a gap.** The 2025–26 mainstream is still
orchestrator-centric (Camp A — the [Coordinator Trap](theory/coordinator-trap.md)); the
decentralized/emergent cluster and the MAS-reliability literature are the thesis arriving from two
directions (see [publications.md](publications.md)). Landscape scan: 2026-07 survey + industry
sources (agentic design patterns; MCP/A2A/ACP/ANP interop; stigmergy/pressure-fields/SwarmSys/
AgentNet; MAS reliability failure modes).

**The load-bearing point:** the companions are *themselves* compositions of KV + signals, packaged
ergonomically (blackboard = Linda `rd`/`in`; tuple-space = a pull pipeline). So "not a first-class
primitive" almost never means "not supported" — it means "not yet *packaged*." The Native/Composable
line is about **ergonomics, not capability**.

**Caveat — expressible ≠ validated (do not read this table as a coverage *guarantee*).** A `Native`
row is backed by shipping code (and, for companions, tests). A `Composable` row is a **hypothesis**:
the pattern is expressible on the public API, but *expressible* is not *supported* until it has a
**working, CI-tested example**. The substrate's own history is the standing reminder — the curator
split-brain, the coop flake, and an event-log first mis-classed as a gap were all "should compose"
until built and tested. Converting the Composable rows into a gallery of tested examples is the
[v3.0 work](../../../ROADMAP.md) that *earns* the coverage claim; until then this page states
capability, not proof.

## Native — a primitive or companion provides it

| Pattern | Provided by |
|---|---|
| Mesh (full / partial) | the substrate — partial mesh + **unconditional flood fallback** |
| Blackboard | `mycelium-blackboard` (Linda `rd` / `in`) |
| Pipeline / pull | `mycelium-tuple-space` (`redistribution` example) |
| Swarm / stigmergy / pheromone | coop `stigmergy` demo · tuple-space backpressure pheromone · **evaporating** capability ads (read-side HLC freshness = decay) |
| Shared eventually-consistent state | Layer I KV (LWW + HLC, Merkle anti-entropy) |
| Event-driven messaging / bus | `src/agent/mailbox.rs` — KV-backed, **HLC-ordered durable delivery** |
| **Event-sourced log** (append / replay / tail / compact) | `KvHandle::append` · `scan_log(from,to)` · `subscribe_log(since_hlc)` · `compact_log` — a `log/{stream}/{hlc}` overlay on the gossip KV: replicated + WAL-persisted, **replay from an offset** |
| Capability discovery + demand *pressure* | capability system · `demand()` / `watch_demand()` |
| **Dynamic wiring graph** | `advertise_capability` + `declare_requirement` + `resolve_wiring` — an emergent dependency graph that rewires as capabilities change |
| Elastic membership (by intent) | `MembershipGovernor` + `MembershipIntent` |
| Opt-in strong consistency | Layer III epidemic consensus |
| MCP / A2A interop | native MCP tools + gateway · `a2a` feature + AgentFacts |
| Durable curated memory | `mycelium-wiki` |
| Code mobility | `mycelium-wasm-host` |
| Access governance | `mycelium-wiki` membership-gated access broker + capability authz (WS-D) |
| **Reliability mitigations** (races, stale reads) | eventual consistency + **anti-entropy reconciliation**, WAL replay, opacity back-pressure — the substrate *is* the field's prescribed fix |

## Composable — no packaged primitive, but built from the above (a v3.x *packaging* candidate)

These need **no new substrate capability** — only ergonomic packaging, exactly as blackboard/tuple-space
packaged Linda. Tracked as packaging companions in [`ROADMAP.md`](../../../ROADMAP.md) → *v3.0 Candidates*:

- **Auction / bidding (Contract-Net)** — announce = signal · bids = `kv().append("bids/{auction}")` ·
  clear = a consensus round (linearizable award) **or** the deterministic lowest-wins rule the
  tuple-space/wiki elections already run.
- **DAG self-evolving agent network** (AgentNet-style) — the dynamic wiring graph (above) *is* this;
  "self-evolving" = agents re-advertising capabilities; the LLM-picks-specialization layer rides on top.
- **Governed shared memory / read-set reconstruction** (S-Bus) — governance is Native (access broker +
  authz); the read-set-reconstruction trick is a thin app layer over HLC read-stamps + the wiki's 3-way
  reconcile.
- Orchestrator–Worker / hierarchical (**against the grain** — see non-goal) · Generator–Verifier ·
  pressure-fields / gradient optimization (the ally paper's *application-level* algorithm on a shared KV
  artifact) · emergent conventions (naming-game) · hybrid topologies.

## Genuine gap — needs new code, not composition

- **ANP protocol conformance** — an external *wire-protocol* standard; implemented as an **edge adapter**
  (exactly like the `a2a` adapter — real code, not a composition). AgentFacts is NANDA-adjacent, not ANP.
  The one item not expressible on the existing surface.

A *durable / partitioned* log with consumer-group committed offsets would push the Native event-sourced
log toward full Kafka semantics — a packaging **refinement**, not a missing pattern.

## Use-case functions — accessed *through* the mesh, not implemented *by* it (the wiki precedent)

These are commonly listed as "agentic patterns," but they are **use-case-level functions**, not
substrate concerns. The substrate's job is the same as with `mycelium-wiki`: the store/service is
**node-independent and lives off the cluster**; a Mycelium group *discovers and accesses* it via
capability advertisement + (optionally) the membership-gated access broker. So these are **not
substrate gaps** — they are the wiki's control-plane/data-plane split applied to a different resource.

- **RAG / retrieval / vector search** — Mycelium is not (and need not be) a vector store. A vector/RAG
  **service sits off-cluster** (managed vector DB, embedding service) and is advertised as a capability;
  the group that needs retrieval resolves and calls it — identical to how the wiki reaches an FsStore/S3
  store. Fleet-wide sharing, discovery, and access-gating come from the substrate; the index does not.
- **Human-in-the-loop / approval** — the human is just another participant. Compose from `Suspended`
  (state machine) + a signal/mailbox requesting approval + (for multi-approver) consensus. The waiting,
  routing, and resumption are substrate; the human UI is the use case.
- **Content guardrails** (toxicity / PII / jailbreak / moderation) — an external guardrail service
  (Llama Guard, NeMo, a classifier) accessed via capability, exactly like RAG. Text-safety is
  application-level; the substrate coordinates *access* to it, it does not implement it.

## Structural guardrails — a native strength (distinct from content guardrails)

Separate from *content* guardrails (above), **capability/structural guardrails — what an agent is
allowed to *do*: which tools, data, spend, group — are a substrate strength, and coordinator-free is
a genuine differentiator.** The toolkit already ships: receiver-side signal `Boundary` (a node cannot
act on a signal outside its boundary — admission control at the point of action) · capability authz +
CT revocation (WS-D) · `tool_budget`/`max_turns` policy · the wiki membership-gated access broker ·
mTLS identity + tamper-evident hash-chained audit (WS1/WS2). The point: a coordinator-based system
enforces guardrails at the coordinator — **a chokepoint that is also a single point of bypass** (the
"guardrail proxy in front of the model" *is* a coordinator); coordinator-free, enforcement is at
**every receiver's boundary** with **no central policy engine to bypass**, and audit is per-node and
tamper-evident. Caveats (honest): it is *action*, not *content*, guardrails; it is **promise-strength**
(each node enforces; the substrate makes violations *legible* rather than centrally *prevented* — on
thesis); and it is a **reframe of existing mechanisms** — a strong hypothesis until a worked example
exists (an agent structurally stopped from an out-of-boundary action, with the audit proving it).
Possible v3.0 wedge — arguably stronger than some current candidates.

## A distinct axis — LLM-authoring DX (not coordination)

Coordination-pattern coverage (above) is orthogonal to how *pleasant it is to author the LLM reasoning*
that rides on the mesh. Mycelium has real pieces (`PromptTemplate`, `LlmBackend` + streaming, MCP tools,
the Layer-V `AgentStateMachine` with `max_turns`/`tool_budget`, HLC audit + `/gateway/explain`) but its
design center is the substrate, so the reasoning-framework ergonomics — reasoning-graph authoring,
typed-output + retry, model-call resilience, conversation memory, run-level evals — are **gaps on this
axis**.

**Strategy — build-vs-adopt resolved to three tiers (don't roll a full framework).** The popular DX
(LangGraph, Instructor, Pydantic AI) is almost all **Python** and sits *above* a substrate, so:
- **BUILD** only the un-adoptable, substrate-native differentiators — ① **capability-routed inference**
  (route to a healthy model-advertising node; no central proxy) and ② **fleet-reasoning traces**
  (tamper-evident causal traces via HLC audit + `/gateway/explain`).
- **ADOPT** the commodity layer — typed output via **Instructor** / Pydantic AI, wrapped in `mycelium-py`.
- **INTEROP / be-the-backend** — flagship a **`langgraph-checkpoint-mycelium`** on LangGraph's pluggable
  checkpointer protocol: one-line swap → coordinator-free, resumable-across-nodes agent state (the
  strongest "why not just LangGraph?" rebuttal).

**Sequence: Tier 3 first — to a CI-tested wedge — then Tier 1 ∥ Tier 2** (the differentiator is what
gives the adopt/interop its *pull*; Tier 2 is built to *expose* the wedges). Raises `mycelium-py` to
first-class; the Rust core needs zero changes. Home: the **`mycelium-reason`** DX companion —
[`ROADMAP.md`](../../../ROADMAP.md) → v3.0 · full strategy in the sketch
[`../../plans/mycelium-reason.md`](../../plans/mycelium-reason.md). Same caveat: expressible until each
wedge (and the checkpointer fit) has a tested example.

## Deliberate non-goal (not a gap)

A first-class **orchestrator / coordinator primitive**. The thesis is coordinator-free: you can
*build* one on the public API, but the substrate will not hand you one. Counting it as missing
coverage contradicts [`philosophy.html`](../../philosophy.html) — it is the pattern the project
exists to make unnecessary.
