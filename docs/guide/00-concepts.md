# 00 · Concepts & Vocabulary

Read this first. Mycelium has its own vocabulary (Capability, Skill, Signal,
Requirement) that sits *next to* industry standards it speaks at its edges
(A2A, MCP, NANDA AgentFacts) — and the two are easy to conflate. This chapter
draws the lines, says when to reach for each, and points every term at a
runnable demo.

Every concept below is, ultimately, **a key in the one gossip KV store** — that
is the substrate unity the [guide intro](README.md) describes. The differences
are about *what the key means* and *who reads it*, not about separate systems.

---

## The one distinction that matters: native model vs. edge standards

| | What it is | Examples | Where it lives |
|---|---|---|---|
| **Mycelium-native model** | The substrate's own vocabulary — how nodes describe, find, and run work *inside* the mesh | Capability · Skill · Requirement · Signal · Artifact | KV prefixes (`cap/`, `prompts/`, `req/`, …) + the signal mesh |
| **Edge standards** | Industry protocols Mycelium **speaks at its boundary** so the outside world can interoperate — *export / bridge formats, not the internal model* | A2A AgentCard · MCP tool · NANDA AgentFacts | `/.well-known/*` endpoints + `tools/` |

If you remember one thing: **inside the mesh you think in Capabilities and
Skills; at the edge you speak A2A / MCP / AgentFacts.** The standards are
generated *from* the native model (the A2A card is built from `cap/`; AgentFacts
are signed from live capabilities), never the other way around.

---

## Capability vs. Skill vs. MCP tool vs. A2A vs. AgentFact

This is the headline confusion. Here is the whole thing in one table:

| Term | Native or standard? | One-line definition | When you use it | Demo |
|---|---|---|---|---|
| **Capability** | native | A declarative advertisement: "this node provides `ns/name`." The discovery *atom*. It does nothing by itself — it is found, not called. | Advertise what a node can do; discover providers at call time | [`provisioning`](../../examples/coop/src/bin/provisioning.rs) |
| **Skill** | native | A **Capability + an executable handler**. A *Prompt Skill* is LLM-backed (template in `prompts/`, an `LlmBackend` runs the inference); a *SkillRunner skill* is a hosted process. | Make a capability *invokable* — the unit you actually call | [`mailbox_llm`](../../examples/coop/src/bin/mailbox_llm.rs), [`llm_council`](../../examples/coop/src/bin/llm_council.rs) |
| **MCP tool** | **standard** (Model Context Protocol) | A JSON-schema'd tool registered at `tools/{name}/{node}`, invoked over `mcp.invoke`. Bridges LLM tool-use ↔ the mesh, both directions. | Expose tools to an LLM, or bridge an external MCP server's tools in | [`mcp_toolgrowth`](../../examples/coop/src/bin/mcp_toolgrowth.rs) |
| **A2A AgentCard** | **standard** (Google A2A) | An agent-discovery document served at `/.well-known/agent.json`, **built dynamically from `cap/`**. | Let an *external agent framework* (LangChain, AutoGen) discover & call your agents | [`a2a_langchain`](../../examples/a2a_langchain/) |
| **AgentFacts** | **standard** (NANDA) | A self-certified metadata document at `/.well-known/agent-facts.json` for cross-domain discovery. | Federate discovery across *domains* with no shared trust authority | [`federation_facts`](../../examples/coop/src/bin/federation_facts.rs) |

### Why "Skill" had to exist (it's not just a renamed Capability)

A Capability is *only* an advertisement — `cap/{node}/nlp/summarize` says "I
summarize," but there is nothing behind it to call. `register_prompt_skill`
does three things at once, and that triple is the Skill:

1. writes the prompt **template** to `prompts/{ns}/{name}` (the configuration),
2. **advertises a `Capability`** under `cap/{node}/{ns}/{name}` (so it's discoverable),
3. registers an **`LlmBackend`** as the handler (so it's runnable).

So: **a Skill is a Capability you can invoke.** The Capability is how peers
*find* it; the template + backend are how a call *runs*. Strip the handler and
you're back to a bare Capability; strip the LLM backend and use a process and
you have a SkillRunner skill instead.

### Why A2A / MCP / AgentFacts are *not* the same thing

They are **external interop standards**, each for a different audience:

- **MCP** is for *LLM tool-use*: an LLM (yours or someone else's) discovers and
  calls tools. Mycelium can publish a Skill as an MCP tool, or pull a remote
  MCP server's tools into the mesh.
- **A2A** is for *other agent frameworks*: a LangChain or AutoGen agent treats
  your Mycelium agents as callable tools by reading the AgentCard.
- **AgentFacts (NANDA)** is for *cross-domain federation*: a neighbouring
  cluster discovers your domain's capabilities by pulling a signed facts
  document — no shared CA, trust is the fetcher's decision.

A Capability/Skill is the thing; A2A/MCP/AgentFacts are three different windows
onto it for three different outside callers.

---

## The other concept-pairs people conflate

Short, so you can skim. Each links the demo that makes it concrete.

**Capability vs. Requirement vs. Demand.** A **Capability** (`cap/`) is "I
provide X." A **Requirement** (`req/`) is "I need X." **Demand** is the derived
pressure — requirements with no matching provider — that a provisioner reacts
to. → [`provisioning`](../../examples/coop/src/bin/provisioning.rs),
[`mcp_toolgrowth`](../../examples/coop/src/bin/mcp_toolgrowth.rs).

**Prompt Skill vs. SkillRunner skill vs. MCP tool.** Three "invokable units."
*Prompt Skill* = LLM call behind a capability (in-process backend). *SkillRunner
skill* = a hosted process advertising a capability (the `skillrunner` binary,
TOML manifest). *MCP tool* = the standards-based bridge for LLM tool-use, in the
separate `tools/` namespace. The first two are native Capabilities; the third is
an edge standard.

**Signal vs. KV entry.** A **Signal** (Layer II) is an *ephemeral* scoped event
— miss it and it's gone. A **KV entry** (Layer I) is *durable* state that
gossips and heals via anti-entropy. Use a Signal to *notify*; use KV to *record*.
The mailbox is the bridge: durable, ordered event delivery built on KV. →
[`mailbox_llm`](../../examples/coop/src/bin/mailbox_llm.rs).

**The two "TTL"s — don't conflate them.** The wire frame's **hop-count TTL**
(`u8`, decremented per forward) bounds how far a frame travels. Key
**evaporation** is a *read-side convention*: capability/load entries carry a
`refresh_interval_ms` and readers discard anything older than 3× it. The store
never time-evicts a live key; evaporation is "stop believing a stale
advertisement," not "delete." → see [`01-gossip-kv.md`](01-gossip-kv.md).

**Three kinds of "group."** An **emergent group** forms when nodes self-join by
matching a `CapabilityGroupDef` filter (no coordinator). A **consensus group**
is the set of voters a `group_propose` / `cross_group_propose` requires quorum
from. A **membership-governed group** is an emergent group whose size is bounded
by a `MembershipIntent` the `MembershipGovernor` reconciles toward. →
[`consensus`](../../examples/coop/src/bin/consensus.rs) (consensus group),
[`elastic_intent`](../../examples/coop/src/bin/elastic_intent.rs) (governed).

**Governed vs. ungoverned.** By default a group is **ungoverned**: it just self-organises — nodes
join/leave by capability, and the size is whatever it is. A group becomes **governed** only when
someone publishes a `MembershipIntent` for it; then a `MembershipGovernor` actively holds it to a
`min`/`max` band (spinning members up/down or flagging a `conflict`). Govern a group when you need
a *bounded pool* ("keep 5–10 render workers") or want it observable against a target; leave it
ungoverned when self-organisation is enough (most groups). Governance is additive — the *same*
group is just being watched by a governor. **Why create a group at all** →
[cookbook: "why (and when) create a group"](cookbook.md#why-and-when-would-i-create-a-group-within-a-cluster).

**Consensus vs. LWW vs. tuple-space rendezvous — "how do agents agree?"**
**LWW** (last-write-wins, the default) — no agreement, newest timestamp wins;
use for soft state. **Consensus** (Layer III) — real agreement via quorum; use
when a decision must be singular and durable. **Tuple-space rendezvous** — no
agreement at all; workers *pull* work when ready (the coordinator-free
alternative to predicting who does what). →
[`consensus`](../../examples/coop/src/bin/consensus.rs),
[`llm_pipeline`](../../examples/coop/src/bin/llm_pipeline.rs).

**Tuple space vs. blackboard — "how does a consumer find its work?"** Two
companion crates, two answers, both on the public API. The
[`mycelium-tuple-space`](../../mycelium-tuple-space/) routes by **position**:
named FIFO lanes (stages), topology known up front, O(1) claims — for pipelines
whose stages you know. Fan-in joins by correlation key use its keyed
`take_by_key` (M13). The [`mycelium-blackboard`](../../mycelium-blackboard/)
routes by **content**: a consumer names a **predicate** over fact attributes and
the topology is *emergent per item* — for opportunistic reasoning where *which*
agent acts is decided by the fact, not by where it was put. The blackboard adds
the one primitive the substrate lacks — **competitive destructive
claim-by-predicate** (Linda's `in`); non-destructive shared reads (`rd`) are
already the substrate's (gossiped facts + predicate filters). One line: *known
stages → tuple space; emergent topology over shared facts → blackboard.*

Both of those are **transient** — work found and consumed. The durable sibling is
[`mycelium-wiki`](../../mycelium-wiki/): a group-scoped, **LLM-curated** knowledge
canon (the *maintained-meaning / authoritative-specific* layer, composing with
Postgres metrics + RAG background by a shared id namespace). It answers a
different question — *"where does a group keep what it learns?"* Not in gossiped
KV: the corpus lives in a node-independent store, a single elected **curator**
serialises writes (so concurrent edits need no CRDT) while agents **read directly,
in parallel**, and Mycelium is only the control plane (curator election +
ring-failover, an evaporating proposal queue, MCP + gateway, a membership-gated
access broker). One line: *transient work → tuple space / blackboard; durable
curated knowledge → wiki.*

**Opacity / pheromone / load / backpressure.** All one mechanism: a node writes
its own state under `sys/load/{self}/…`; anything scanning that prefix sees a
consistent picture with no coordination. **Load** is the raw fill; **opacity**
is the boolean "skip me" a governor derives from it; the written entry is the
**pheromone** others read; **backpressure** is the resulting reroute. →
[`stigmergy`](../../examples/coop/src/bin/stigmergy.rs).

**Mailbox vs. Signal vs. RPC vs. Bulk vs. Scatter.** The service patterns, by
shape: **Signal** = fire-and-forget event. **RPC** = request/reply to one
provider. **Mailbox** = durable, HLC-ordered event delivery (survives a restart
within the TTL window). **Bulk** = large payloads over HTTP staging, not the
signal mesh. **Scatter** = query many providers, aggregate replies. →
[`mailbox_llm`](../../examples/coop/src/bin/mailbox_llm.rs).

**Artifact vs. Capability vs. Skill vs. Tool — the deployable-unit ladder.**
An **Artifact** is deployable *bytes* (a WASM component / model) in the
`installable/` catalogue. When a node pulls + verifies + instantiates it, it
**becomes a Capability** the node advertises — and if that capability has a
handler it's a **Skill**, or if exposed over MCP a **Tool**. Artifact is the
package; the rest are what it turns into once running. →
[`provisioning`](../../examples/coop/src/bin/provisioning.rs); the catalogue
itself — where it lives and how to author/publish to it — is the
[`mycelium-wasm-host`](../../mycelium-wasm-host/) crate (a dedicated
`operations/artifacts.md` lands with that workstream).

**Node = Agent = Process.** One Mycelium **node** is one `GossipAgent` is one OS
process is a full peer (registry + bus + scheduler in one). The coop demos name
their nodes "depots" / "agents" for the story — same thing.

**Promise-strength vs. mechanism-strength.** Namespace ownership (e.g. only
`{node}` should write `sys/load/{node}/…`) is **convention**, not enforced: a
rogue write is *applied* per LWW but **flagged** by a tripwire counter
(`commit_conflicts`, `sys_namespace_violations` on `/stats`) — **detection, not
prevention**. Commitments are promise-strength, the only honest strength a
coordinator-free system can offer. → [`consensus`](../../examples/coop/src/bin/consensus.rs)
(an empty bloc can't be coerced), [`federation_facts`](../../examples/coop/src/bin/federation_facts.rs)
(a tampered document fails verification at read).

---

## Layers at a glance

| Layer | What it adds | Native concepts | Chapter |
|---|---|---|---|
| **I — Gossip KV** | eventually-consistent shared state (LWW, HLC, anti-entropy) | KV entry, evaporation | [01](01-gossip-kv.md) |
| **II — Signal mesh** | ephemeral scoped events with admission boundaries | Signal, Boundary, opacity | [03](03-signals.md) |
| **III — Consensus** | opt-in strong consistency on top of I | quorum, leased commit, consistent_set | [04](04-consensus.md) |
| **Capability system** | broker-less discovery (cuts across I–III) | Capability, Requirement, Demand, group | [02](02-capabilities.md) |
| **Application / edge** | the patterns + the standards spoken outward | Skill, Artifact · MCP, A2A, AgentFacts | [05](05-skills.md)–[08](08-a2a-interop.md) |

> The substrate (Layers I + II) is the `mycelium-core` crate; everything above
> is the full `mycelium` crate. See the [guide intro](README.md) for which to
> depend on.

---

## Glossary quick-reference

| Term | Native / standard | One line | Demo |
|---|---|---|---|
| Capability | native | "this node provides `ns/name`" — discovery atom | `provisioning` |
| Skill | native | a Capability + a handler (Prompt = LLM; SkillRunner = process) | `mailbox_llm` |
| Requirement | native | "this node needs `ns/name`" — creates Demand | `mcp_toolgrowth` |
| Demand | native | requirements with no provider — the provisioning trigger | `provisioning` |
| Signal | native | ephemeral scoped event (Layer II) | `mailbox_llm` |
| Boundary | native | a node's receptor set — decides if it *acts* on a signal | `03-signals.md` |
| Mailbox | native | durable, HLC-ordered event delivery on KV | `mailbox_llm` |
| Opacity / pheromone | native | self-written `sys/load/` state others route on | `stigmergy` |
| Group (emergent) | native | nodes self-join by capability filter | `02-capabilities.md` |
| Consensus | native | quorum agreement (Layer III), promise-strength | `consensus` |
| Intent | native | evaporating soft-state desired-state; nodes reconcile locally | `elastic_intent` |
| Artifact | native | deployable bytes in the `installable/` catalogue → becomes a Capability | `provisioning` |
| MCP tool | standard | LLM-tool bridge at `tools/{name}/{node}` | `mcp_toolgrowth` |
| A2A AgentCard | standard | agent discovery for external frameworks (`/.well-known/agent.json`) | `a2a_langchain` |
| AgentFacts | standard | self-certified cross-domain federation (`/.well-known/agent-facts.json`) | `federation_facts` |

**Next:** [01 · Gossip KV](01-gossip-kv.md). For the full design argument behind
this vocabulary, see [philosophy.html](../philosophy.html).

---

## Reference — Skills vs MCP tools: choosing the right primitive

*Moved from the repo README (2026-07-10).*

Mycelium supports two ways to extend what an LLM agent can do. They solve
different problems and compose naturally together.

#### Mental model

> **MCP tool** = a function in the mesh. The LLM calls it to look something
> up, run a calculation, or fetch data. Written in any language.
>
> **Skill** = an LLM agent in the mesh. It has its own identity, prompt, and
> capability declaration. It can be called by any node — including other skills.

#### Comparison

| | MCP Tool | Skill |
|---|---|---|
| What it is | A function registered on a node | An LLM agent node |
| Written in | Any language | TOML manifest — no code |
| Calls an LLM | Optionally | Always |
| Can call other skills | No | Yes — composition |
| Discovered via | `tools/` KV prefix | Capability system (`ns`/`name`) |
| Started with | Any binary / language | `skillrunner --skill manifest.toml` |
| Live chat example | `three_node_demo` — `wiki`, `weather`, `calculate` | `examples/community/` — researcher, writer, orchestrator |
| Guide | [06-tool-discovery.md](06-tool-discovery.md) | [05-skills.md](05-skills.md) |

#### When to use each

Use an **MCP tool** when:
- You need to call an external API (weather, Wikipedia, a database)
- You need deterministic computation (arithmetic, format conversion)
- You want to write the tool in Python, TypeScript, Go, or any language
- The operation is stateless and fast

Use a **Skill** when:
- You need an LLM reasoning step in a pipeline
- You want to compose agents — an orchestrator that calls a researcher that calls a writer
- You want a persistent, named agent role that any node on the mesh can discover and invoke
- You want to scale a reasoning step horizontally (run two researchers; the orchestrator uses both)

#### They compose naturally

The `three_node_demo` LLM node uses MCP tools for external lookups (`wiki`,
`weather`, `sf_lookup`, `book_plot`). The `examples/community/` orchestrator
uses Skills for LLM reasoning steps (`researcher`, `writer`). There is no
conflict — a single planner can have both in scope simultaneously.

---
