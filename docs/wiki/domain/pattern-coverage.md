# domain/pattern-coverage — the agentic-pattern landscape vs the substrate

↑ [domain/](domain.md) · positioning artifact (external landscape → link, don't copy). Coverage
claims are **code-anchored**: a `Native` row names the primitive; verify against the cited file.

**Question this answers:** does the coordinator-free substrate cover the distributed-agentic usage
patterns the field has converged on? **Mostly — *natively* for the decentralized/mesh family,
*expressibly* for the orchestrator family (by design), with five *additive* gaps.** The 2025–26
mainstream is still orchestrator-centric (Camp A — the [Coordinator Trap](theory/coordinator-trap.md));
the decentralized/emergent cluster and the MAS-reliability literature are the thesis arriving from two
directions (see [publications.md](publications.md) → the Paper-1 related-work landscape). Landscape
scan: 2026-07 survey + industry sources (agentic design patterns; MCP/A2A/ACP/ANP interop;
stigmergy/pressure-fields/SwarmSys/AgentNet; MAS reliability failure modes).

## Native — a primitive or companion provides it

| Pattern | Provided by |
|---|---|
| Mesh (full / partial) | the substrate — partial mesh + **unconditional flood fallback** |
| Blackboard | `mycelium-blackboard` (Linda `rd` / `in`) |
| Pipeline / pull | `mycelium-tuple-space` (`redistribution` example) |
| Swarm / stigmergy / pheromone | coop `stigmergy` demo · tuple-space backpressure pheromone · **evaporating** capability ads (read-side HLC freshness = decay) |
| Shared eventually-consistent state | Layer I KV (LWW + HLC, Merkle anti-entropy) |
| Event-driven messaging / bus | `src/agent/mailbox.rs` — KV-backed, **HLC-ordered durable delivery** |
| Capability discovery + demand *pressure* | capability system · `demand()` / `watch_demand()` |
| Elastic membership (by intent) | `MembershipGovernor` + `MembershipIntent` |
| Opt-in strong consistency | Layer III epidemic consensus |
| MCP (agent↔tools) / A2A (agent↔agent) | native MCP tools + gateway · `a2a` feature + AgentFacts `.well-known/agent.json` |
| Durable curated memory | `mycelium-wiki` |
| Code mobility | `mycelium-wasm-host` |
| **Reliability mitigations** (races, stale reads) | eventual consistency + **anti-entropy reconciliation**, WAL replay, opacity back-pressure — the substrate *is* the field's prescribed fix for the shared-state failures the MAS-reliability literature reports |

## Expressible — public API, no packaged primitive (you write it)

Orchestrator–Worker / hierarchical (against the grain — see non-goal) · Generator–Verifier ·
pressure-fields / gradient optimization (the ally paper's *application-level* algorithm rides on a
shared KV artifact) · emergent conventions (naming-game) · hybrid topologies.

## Gap — not there; additive; tracked as v3.0 candidates

Framed as **v3.0 candidates** in [`ROADMAP.md`](../../../ROADMAP.md) → *v3.0 Candidates* (proposed,
demand-driven, not started). None contradicts the coordinator-free model.

1. **Replayable event-sourced log** — the mailbox is durable *delivery* and WALs are *state*
   recovery; there is no app-event log replayable from an offset. *Highest leverage* — HLC total order
   + WAL machinery already exist.
2. **Auction / bidding allocation** — today: demand *pressure* + first-come `claim`; no bid/clear.
3. **DAG-structured self-evolving agent network** (AgentNet-style) — capability groups are flat.
4. **ANP protocol conformance** — AgentFacts is NANDA-adjacent, not ANP.
5. **Governed shared memory / read-set reconstruction** (S-Bus) — access broker + authz cover
   *governance*, not the consistency-reconstruction research.

## Deliberate non-goal (not a gap)

A first-class **orchestrator / coordinator primitive**. The thesis is coordinator-free: you can
*build* one on the public API, but the substrate will not hand you one. Counting it as missing
coverage contradicts [`philosophy.html`](../../philosophy.html) — it is the pattern the project
exists to make unnecessary.
