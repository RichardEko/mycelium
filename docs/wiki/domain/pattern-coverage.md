# domain/pattern-coverage — the agentic-pattern landscape vs the substrate

↑ [domain/](domain.md) · positioning artifact (external landscape → link, don't copy). Coverage
claims are **code-anchored**: a `Native` row names the primitive; verify against the cited file.

**Question this answers:** does the coordinator-free substrate cover the distributed-agentic usage
patterns the field has converged on? **Nearly all — *natively* or *by composition of native
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

## Deliberate non-goal (not a gap)

A first-class **orchestrator / coordinator primitive**. The thesis is coordinator-free: you can
*build* one on the public API, but the substrate will not hand you one. Counting it as missing
coverage contradicts [`philosophy.html`](../../philosophy.html) — it is the pattern the project
exists to make unnecessary.
