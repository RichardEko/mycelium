# Delivery plan — documentation & example-portfolio alignment

**Status:** ✅ **COMPLETE** (2026-06-20). All seven workstreams shipped; the house is in order ahead
of the remaining v2 engineering (WS-D security, WS-F schema-evolution).

| WS | What | PR |
|---|---|---|
| WS0 | Concepts & Glossary (`guide/00-concepts.md`) | #70 |
| WS2 | Patterns & Pitfalls grounded in examples (`guide/14-patterns-and-pitfalls.md`) | #71 |
| WS1 | Retire-but-preserve (`prompt_skill_demo`, `mesh_demo`) | #72 |
| WS5 | Cluster artifact catalogue (example 11 `catalog` + `operations/artifacts.md`) | #73 |
| WS3+WS4 | Developer cookbook + two-audience ops docs | #74 |
| WS6 | Reconcile (CLAUDE.md, plan status, stale-ref sweep) | this |

The original plan follows as the design record.

**Why.** The 10-example Food-Rescue Co-op suite shipped, but the documentation never caught up:
the guide index still points at *old* examples, the coop suite is invisible to `docs/`, no example
or chapter calls out an **anti-pattern**, the **concept vocabulary** is undefined (Capability vs
Skill vs A2A vs MCP vs AgentFacts — easily conflated), there are **no operational docs** for the
questions operators actually ask (deploy, observe, view AgentFacts, dynamic scaling), and the
real **gossip artifact catalogue** (`installable/` + `MeshArtifactSource`) is implemented but
neither documented nor demonstrated (the coop flagship used a node-local `InMemorySource`).

**Two audiences, everywhere.** Every "how do I…" is answered for **both**:
- **Solution/Dev** — a developer *embedding* the library and *building* agents/skills/artifacts.
- **DevOps** — an operator *deploying* and *running* a cluster, *observing* it, *scaling* it.

Where a question matters to both, it gets a section in each track (cross-linked), not one merged
section that half-serves each.

---

## WS0 · Concepts, Ontology & Glossary  *(foundational — leads)*

A new guide chapter **`docs/guide/00-concepts.md`** placed *before* `01`, plus a quick-reference
table. Every other workstream uses this settled vocabulary instead of reinventing it.

**The load-bearing distinction: native concepts vs. edge standards.**

- **Mycelium-native model** (the substrate vocabulary):
  - **Capability** — the discovery atom: a declarative "this node provides `ns/name`", advertised
    under `cap/{node}/{ns}/{name}`, found by capability matching. *Just an advertisement; it does
    nothing by itself.*
  - **Skill** — a **Capability + an executable handler**. Two flavours: a **Prompt Skill**
    (LLM-backed; template in `prompts/`, an `LlmBackend` runs the inference) and a **SkillRunner
    skill** (a hosted process; schemas in `skills/`). *Why Skills exist:* a Capability is
    discoverable but not invokable — the Skill is the unit you can call. `register_prompt_skill`
    literally writes the template, **advertises a `Capability`**, and registers the backend.
  - **Requirement** — the mirror of a Capability: "this node needs `ns/name`"; creates **Demand**.
  - **Signal** — an ephemeral, scoped event (Layer II) — vs a **KV entry** (durable state, Layer I).
  - **Artifact** — deployable bytes (a WASM component / model) in the `installable/` catalogue that
    *becomes a Capability* once a node provisions it.
- **Industry standards spoken at the edge** (export / bridge formats, *not* the internal model):
  - **A2A AgentCard** (Google A2A) — served at `/.well-known/agent.json`, built dynamically *from*
    `cap/`; lets external frameworks (LangChain/AutoGen) discover & call Mycelium agents.
  - **MCP tool** (Model Context Protocol) — `tools/{name}/{node}`; bridges LLM tool-use ↔ mesh,
    both directions (register a tool; or bridge an external MCP server's tools in).
  - **AgentFacts** (NANDA) — self-certified facts at `/.well-known/agent-facts.json`; federated
    discovery across domains.

**When/why each:** advertise & discover inside the mesh → **Capability**; make it callable →
**Skill**; expose/consume LLM tools → **MCP**; interop with external agent frameworks → **A2A**;
federate discovery across domains → **AgentFacts/NANDA**. Native = the model; standards = the edge.

**The 12 concept-pairs the chapter disambiguates** (each: definition · how it differs from the
adjacent concept · when/why · the demo that shows it · for standards, "industry-standard X at the
edge"):

1. Capability vs Skill vs MCP tool vs A2A vs AgentFact  *(the headline)*
2. Capability vs Requirement vs Demand  (provider / consumer / pressure)
3. Prompt Skill vs SkillRunner skill vs MCP tool  (three "invokable unit" notions)
4. Signal vs KV entry  (ephemeral event vs durable state; Layer II vs I)
5. The two "TTL"s  (wire hop-count TTL vs read-side **evaporation** — already conflated internally)
6. Group: emergent vs consensus vs membership-governed  (three "group" meanings)
7. Consensus vs LWW vs tuple-space rendezvous  (agreement vs newest-wins vs pull)
8. Opacity / pheromone / load / backpressure  (the stigmergy vocabulary)
9. Mailbox vs Signal vs RPC vs Bulk vs Scatter  (service patterns; when each)
10. **Artifact vs Capability vs Skill vs Tool**  (the deployable-unit vocabulary — shared with WS5)
11. Node = Agent = Process  (the demos rename nodes "depots"; say so)
12. Promise-strength vs mechanism-strength  (namespace ownership is convention; tripwires *detect*)

Plus a one-page **Layers I/II/III/IV** map (KV / Signal / Consensus / MCP-A2A edge — what lives
where) and a **glossary quick-reference table** (term → one-line def → demo → doc link).

**Deliverable:** `docs/guide/00-concepts.md` + glossary table; every term links to a runnable demo.

---

## WS2 · Patterns & Pitfalls — grounded in the example implementations  *(next, per direction)*

A new guide chapter **`docs/guide/14-patterns-and-pitfalls.md`** that is **grounded in the actual
example code**: each entry says *"`example_X` does it **this** way — not the anti-pattern way —
**because** …"*, with a file/line pointer. These are real lessons from building the suite, not
invented cautionary tales. Each example's source header + README entry also gains a short
**Patterns / Anti-patterns** block pointing into this chapter.

Starter set (all real, example-grounded):

| # | Pattern (the right way) | Anti-pattern (don't) | Grounded in | Why |
|---|---|---|---|---|
| 1 | Host an invokable skill on a **separate** node from its caller | Self-resolve + RPC your *own* capability | `mailbox_llm` | A self-RPC needs ≥1 usable peer for the Individual frame and flakes; a cross-node call has flood-relay fallback |
| 2 | Gate readiness on **capability-visible AND peers-formed** | Poll only `resolve().is_empty()` | `mailbox_llm`, `federation_facts` | KV anti-entropy delivers the cap *before* the peer list populates → RPC races ahead of peering |
| 3 | **Structural polls** (`wait_until` on observable state) | Fixed `sleep(…)` for convergence | every demo | A sleep passes by luck on a fast box, hides the race on a slow one (CLAUDE.md testing convention) |
| 4 | Allocate N ports by **binding N listeners at once** | Call `alloc_port()` N times in a row | `common::bootstrap` | Bind-`:0`/drop/bind-`:0` can hand back the same just-freed port → two agents collide |
| 5 | Model a node's load as its **own backlog** | Flood a *remote* node's queue to make it opaque | `stigmergy` | Cross-node Individual *signals* don't reach a remote `signal_rx` (issue #55); stigmergy is self-reported |
| 6 | Let the **MembershipGovernor own** a group under intent | Expect `max`/`drain` to hold while emergent auto-join is active | `elastic_intent` | The emergent watcher must defer for governed groups (fixed #56→#57) |
| 7 | Use a **faster anti-entropy tick** when reading a freshly-signed fact across startup | Publish a signed fact and assume the peer reads it instantly | `rotation` | A peer drops a signer's frames until it has learned the signer's identity ("unknown signer") |
| 8 | **Single synthesizer** joins partials in app memory | Scale out competing synthesizers expecting correct fan-in | `llm_council` | Keyed-correlation fan-in needs `keyed-exact-match take` (ROADMAP M13) |
| 9 | Bridge an MCP tool by **also advertising a `tool/` capability** | `declare_requirement(tool/…)` and expect `register_mcp_tool` alone to satisfy it | `mcp_toolgrowth` | MCP tools live in `tools/`, separate from the `cap/` demand system |
| 10 | Ship artifacts via the **gossip catalogue** (`installable/` + `MeshArtifactSource`) | Use a node-local `InMemorySource` in a real cluster | `provisioning` → WS5 `catalog` | `InMemorySource` is node-local; other nodes can't pull from it |

**Deliverable:** `docs/guide/14-patterns-and-pitfalls.md` + a Patterns/Anti-patterns block in each
coop example header & README entry, all cross-linked.

---

## WS1 · Example-portfolio rationalization (retire-but-preserve)

**Rule (per direction):** if a coop example is *broader than and subsumes* an earlier example,
**retire the earlier one** — but **first** ensure the replacement example **and** the dev docs
cover the aspects the retired example demonstrated, *alongside* the replacement's new capabilities;
and **repoint every guide/README reference before deleting anything**.

Keep / merge / retire matrix:

| Example | Unique value | Decision | Coverage to preserve / action |
|---|---|---|---|
| `prompt_skill_demo` | **live prompt-template update via KV** (`update_prompt`→peer reads new version), `list_prompts`/`get_prompt` | **RETIRE** → coop | First add a *live template-update* beat to `mailbox_llm` (or a coop demo) + cover it in the Skills chapter; repoint guide ch03 + README index; then delete |
| `mesh_demo` | manifest-driven virgin-agent provisioning + mgmt UI (behaviour by capability) | **RETIRE** → `llm_agent` + coop `provisioning` | Ensure `llm_agent`'s UI + `provisioning` cover capability-selected behaviour; repoint ch02/ch03 header refs; then delete |
| `invoke_skill` | minimal SkillRunner caller / driver | **KEEP (utility)** | Re-label as a *driver/utility*, not a portfolio example |
| `llm_agent` | live **management UI** + probe/health + dynamic provisioning | **KEEP** | The README front-door UI demo; note coop `provisioning` for the headless autonomic version |
| `three_node_demo` | multi-role chat + **consistency overlay** cluster + UI | **KEEP** | README front-door; unique consistency-overlay surface |
| `semantic_coordination` | schema versioning, payload schemas, **sender auth**, FIPA-ACL | **KEEP** | Unique; feature more prominently (ties to WS0 schema concepts) |
| `coordinator_comparison` | Paper 2a push-vs-push staleness gradient | **KEEP (research)** | Research artifact |
| `three_arm_workdist` | Paper 1 §9.5 three-arm experiment | **KEEP (research)** | Research artifact |
| `conway` / `conway-gpu` | gossip-KV convergence visual (ch01) | **KEEP** | Pedagogical; ch01 |
| `fluid_pipeline` | AFN **push-vs-pull** contrast (afn-smoke CI) | **KEEP** | The coordinator-trap contrast |
| `a2a_langchain` · `chat` · `community` · `skills` · `presets` | A2A interop · MCP discovery · skillrunner cluster · infra | **KEEP** | Each unique / load-bearing (guide + CI) |

**Deliverable:** the two retirements executed *after* coverage is preserved + refs repointed; a
one-paragraph "example portfolio" map in the guide README (which example teaches what, native vs
standard).

---

## WS3 · Developer ("Solution/Dev") cookbook — "How do I…", leveraging examples

Refresh the guide index to the coop suite, and add a **`docs/guide/cookbook.md`** answering the
developer questions, each pointing at a runnable demo and the WS0 vocabulary:

- How do I **embed Mycelium** (which crate; `GossipAgent::new` + `start`)? → README + ch01
- How do I **advertise/discover a Capability**, and **build a Skill**? → `mailbox_llm`, Skills ch
- How do I **call a skill / RPC / use the mailbox / scatter**? → `mailbox_llm`, service-patterns
- How do I **make an agent reachable from LangChain (A2A)** / **expose or consume an MCP tool**? →
  `a2a_langchain`, `mcp_toolgrowth`
- How do I **federate across domains / publish AgentFacts**? → `federation_facts`
- How do I **author a deployable artifact** and provision it? → WS5
- How do I **run any example** and **see it running**? → per-demo run + observe steps

---

## WS4 · Operational ("DevOps") docs — the gaps

New `docs/operations/` docs (and a refresh of `tuning.md` against current code):

- **`deployment.md`** — library-embed model (no daemon/control-plane), ports (`bind_port` vs
  `http_port`), TLS/auto-CA + shared `cert_dir`, bootstrap peers, a reference container/compose.
- **`observability.md`** — the endpoints (`/health`, `/ready`, `/stats`, `/metrics`,
  `/.well-known/agent-facts.json`, the dashboards) and **how to view AgentFacts** live; what each
  stat means (incl. the tripwire counters).
- **`dynamic-scaling.md`** — elastic membership via `/gateway/govern` + the `MembershipGovernor`;
  **how to see dynamic scaling** (run `elastic_intent`, watch the band hold + self-heal); the
  intent-evaporation / kill-the-operator behaviour.

Each cross-links its Solution/Dev counterpart in WS3.

---

## WS5 · Dynamic artifacts — deep-dive + a NEW catalogue example  *(both audiences)*

**A new example is needed here** (per direction): **`examples/coop/catalog`** (Step 11), showing
the *real* cluster-wide gossip catalogue end-to-end — answering **what is a catalogue**, **how do I
register something with it**, and **how do I make the catalogue available to a Mycelium cluster**:

- A publisher node `publish_installable(kv, InstallableEntry::new(cap, artifact).signed_by(key))`
  → the entry gossips under `installable/{ns}/{name}/{hex}`.
- Provider nodes build their catalogue with `InstallableCatalog::from_kv(kv)` (the cluster view,
  **not** a node-local `InMemorySource`) and serve/pull bytes via `MeshArtifactSource` /
  `serve_artifacts`.
- A consumer declares demand; a provider resolves the gossiped catalogue, **pulls + verifies
  (provenance) + instantiates**, advertises, serves — the autonomic loop on the *real* catalogue.

**A new doc `docs/operations/artifacts.md` + a Solution/Dev companion** (the two-audience split):
- **DevOps:** where the catalogue lives (`installable/` gossiped KV; no registry server), how to
  stand one up, how artifact **bytes** are distributed (`MeshArtifactSource`), provenance/trust
  (`signed_by` + `require_provenance`), operational concerns (size, GC, eviction).
- **Solution/Dev:** how to **author a deployable artifact** — build a WASM component from the
  `mycelium-wasm-host/tests/fixtures/echo-component/` toolchain (`wit/` + `build.sh`),
  content-address it (`ArtifactId::of`), sign it, publish it (`publish_installable`); the
  Artifact→Capability lifecycle (WS0 entry #10).

**Deliverable:** `examples/coop/catalog` (CI-gated via `coop-smoke`) + `docs/operations/artifacts.md`
+ the Solution/Dev authoring guide; coop `provisioning` gains a note pointing here as the
cluster-scale version of its `InMemorySource` shortcut.

---

## WS6 · Reconcile + keep it honest

- Make every doc "Runnable example" command actually run (the guide index column must all work).
- Keep the `coop-smoke` gate; add `catalog` (Step 11) to it.
- A final pass: README demos point at current examples; `docs/guide/README.md` index current;
  CLAUDE.md already references the suite (PR #68) — extend it to the concepts chapter + catalogue.

---

## Sequencing

1. **WS0** — concepts/glossary (foundational; everything else cites it).
2. **WS2** — Patterns & Pitfalls grounded in the examples (per direction: next; captures lessons
   while fresh; its per-example audit feeds WS1).
3. **WS1** — retire-but-preserve (informed by WS2; repoint refs first).
4. **WS5** — the new `catalog` example + dynamic-artifacts deep-dive (the deepest gap).
5. **WS3 + WS4** — the two-audience cookbook + operational docs.
6. **WS6** — reconcile + CI.

Each workstream lands as its own PR(s), same cadence as the example suite. **All of the above
precedes the remaining v2 WS-D / WS-F engineering.**
