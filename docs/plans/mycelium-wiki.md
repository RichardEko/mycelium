# mycelium-wiki — design sketch

**Status:** 🟢 **Approach revised 2026-07-03 — control-plane / data-plane** (design record done
2026-07-02; two driving use cases reviewed 2026-07-03). Build not started under the new shape.

> **Revision (2026-07-03) — the wiki is NOT in the KV substrate.** Reviewing two real use cases
> (Novus-i2 organisational digital-twin; Transparency-Platform council decisions) with the intended
> owner, we pivoted the whole approach. The wiki is the **maintained-meaning / authoritative-specific
> canon** that *complements* an external metrics/structure store (Postgres) and RAG (background) — and
> for those deployments there is **no reason to push the corpus into gossiped KV** (which floods every
> node; group scope is not a data boundary). Instead: a group node offers a **wiki service** over a
> **node-independent, pluggable store** (a shared FS directory / S3 bucket — the store can be *dumb*),
> the **curator** serialises writes + runs the LLM ingest/lint + **brokers access**, and group agents
> **read the store directly, in parallel** once they obtain the location + a scoped read grant from the
> curator. Mycelium's job is the **control plane** — capability advertisement, group admission, curator
> election + ring-failover, the MCP tool, and the small evaporating **proposal queue** in KV — never
> the storage. This is the wiki pattern's *native* shape (files + an LLM curator + direct reads — how
> Mycelium's own `docs/wiki/` works), so the "one hard problem" (concurrent prose merge) largely
> dissolves into single-writer-curator + the store. The earlier **KV-native section-CRDT** design (§
> the hard problem, and the [design record](../design/wiki-concurrent-edit.md)) is retained as the
> **disconnected / no-external-store variant** — for an edge/autonomous fleet with no store to point
> at — not the primary path. A `WikiStore`-in-KV crate was spiked to clarify this and then retired. This file is rationale + a phased build outline in the shape the
[`mycelium-tuple-space`](mycelium-tuple-space.md) and
[`mycelium-blackboard`](mycelium-blackboard.md) plans took before they shipped; the
canonical per-mechanism decisions (esp. the concurrent-edit merge) will move to a
`docs/design/` record in Phase 0.

*Origin: the LLM-wiki pattern (Karpathy, <https://gist.github.com/karpathy/442a6bf555914893e9891c11519de94f>)
was adopted for the Mycelium **project's own** knowledge base on 2026-07-02
(`docs/wiki/`, schema `docs/wiki/AGENTS.md`). The natural follow-on: Mycelium is a
substrate for LLM agent fleets organised into emergent groups — should a **group** get
the same primitive as a first-class capability? This sketch answers "yes, as a fourth
companion crate, and here is the one hard problem that gates it."*

## What it is

A companion crate rebuilding the **LLM-wiki pattern as a group-scoped distributed
primitive** on Mycelium's public API — the same composability move as the tuple space
(work distribution) and the blackboard (shared working memory). A group of LLM agents
shares one **persistent, compounding, interlinked knowledge base**: agents *ingest*
durable knowledge into it and *query* it instead of re-deriving understanding from raw
event history on every task. It is the **long-term-memory** sibling of the blackboard's
working memory.

The distinction from RAG is the same one that motivates the pattern for humans: instead
of re-deriving from raw sources each query, the group maintains a curated artifact that
gets richer with every ingest. For a *fleet*, "raw sources" = the gossip/event history
and each agent's private context; the wiki is the reconciled shared understanding that
survives agent churn, restart, and membership change.

## Where it sits — the fourth coordination primitive

Mycelium's public-API coordination primitives now form a taxonomy along one axis: **how a
consumer finds what it needs.**

| Primitive | Routes by | Lifetime | Access | Crate |
|---|---|---|---|---|
| Tuple space | lane *position* | transient (consumed) | blocking pull (`take`) | `mycelium-tuple-space` |
| Blackboard | content *predicate* | transient (claimed/acked) | competitive claim (`in`) + shared read (`rd`) | `mycelium-blackboard` |
| **Wiki (proposed)** | *curated path / link* | **durable (compounding)** | shared read + disciplined ingest | `mycelium-wiki` |

The blackboard is *working* memory — facts posted, claimed, consumed, gone. The wiki is
*long-term* memory — knowledge synthesised once, cross-linked, re-read many times, updated
in place. A support-agent fleet's blackboard holds "ticket #4471 is being worked by
agent-7 right now"; its wiki holds "the reconciled resolution pattern for auth-token
expiry incidents." Different point in the design space, real gap between them.

## Worked example — a long-lived support / operations fleet

The pattern earns its keep for **long-lived fleets that accumulate domain knowledge**, so
the example is one, not a short task pipeline. A cooperative of support agents fields
incidents for a fleet of deployments; agents join and leave; no dispatcher.

1. A triage agent resolves a novel incident (a cert-rotation edge case). Rather than only
   closing the ticket (a blackboard/tuple concern), it **ingests the durable lesson**:
   updates `wiki/{group}/incidents/cert-rotation.md` to current state, refreshing the
   cross-link to `wiki/{group}/subsystems/tls.md`.
2. A week later a *different* agent — one that wasn't even in the group during the first
   incident — hits a similar symptom. It **queries the wiki first** (`read` the page),
   onboards from the reconciled resolution, and resolves in one step instead of
   re-deriving from raw logs.
3. Periodically the group's **curator** runs the lint: it flags a page whose cited config
   key no longer exists (the doc-vs-code check, generalised to "cited external fact"),
   and an ingest reconciles it. Staleness is caught by a *group function*, not by luck.
4. The original triage agent leaves the group. The knowledge does **not** leave with it —
   that is the whole point, and it is what the blackboard (consumed on claim) and the
   agent's private context (gone on churn) cannot give you.

The compounding property is the value: each ingest makes the *next* agent's task cheaper,
and the benefit accrues across agent churn. A fleet that re-reads curated knowledge often
enough to amortise the cost of curating it is exactly the fleet shape this serves — and
the demand question below is precisely "is your fleet that shape?"

## Why the existing primitives don't cover it

- **KV store alone** gives you the *transport* (a `wiki/{group}/…` prefix gossips, LWW +
  HLC ordered) but not the *semantics*: no curation, no interlink discipline, no
  ingest/lint workflow, and — critically — LWW **silently drops** concurrent prose edits
  (see the hard problem). You could store pages as KV entries today; you would not have a
  wiki, you would have a lossy shared string map.
- **Blackboard** is content-routed but **ephemeral and claim-destructive** — a fact is
  consumed on claim; a wiki page is re-read indefinitely and edited in place. Opposite
  lifetimes. `rd` is shared read (which a wiki also wants), but the blackboard has no
  notion of a *durable, curated, cross-linked* page or of reconciling an edit into
  existing prose.
- **Schema registry** (`publish_schema` / `get_schema`) is durable and gossiped but holds
  *machine* contracts (typed schemas), not synthesised natural-language knowledge, and has
  no ingest/merge/lint loop.

## Architecture — control plane (Mycelium) / data plane (the store)

The wiki is **not** stored in gossiped KV. A group's canon lives in a **node-independent, pluggable
store** — a shared filesystem directory, an S3 bucket, or a document store the group already runs.
Mycelium supplies the **control plane** around it; the store is the **data plane**. Each layer does
what it is best at:

| | Home | Holds | Access |
|---|---|---|---|
| **Control plane** | Mycelium (KV + capabilities + signals + MCP) | *who* the group's curator is + *where* the store is (a `cap/`-advertised pointer); the short-lived **proposal queue** (evaporating soft-state) | coordinator-free, self-healing |
| **Data plane** | the pluggable store (FS dir / S3 / doc store) | the durable **content** (pages/sections + a manifest) | **curator writes; group agents read directly, in parallel** |

**The flow:**

1. **Propose** — an agent appends a small edit **proposal** to the group's evaporating KV queue
   (`wiki/{group}/proposal/{id}`, `refresh_interval`d, so a crashed author's proposal ages out).
   Coordinator-free; no round-trip to the curator.
2. **Curate** — the elected **curator** drains proposals, runs the LLM ingest/reconcile (and the
   periodic lint), and writes the store — the **single writer of record**, so the same-section
   concurrent-edit problem degrades to a single-writer sequence and **no edit is lost**, with the
   store's own atomicity (S3 PUT / temp-then-rename) doing the rest. It writes section/page objects
   first and the **manifest/index object last**, so a direct reader never sees a half-applied edit.
3. **Read** — a group agent obtains the store **location + a scoped, short-lived read grant** from
   the curator (a pre-signed URL / STS-assumed prefix / FS ACL), then reads the store **directly and
   in parallel**. Group membership (`Boundary::admits`) is what the curator brokers on, so **the
   group boundary is the access gate** — not raw IAM.
4. **Fail over** — the curator is a role advertised as `wiki.{group}.curator`; if its capability
   evaporates, a candidate self-elects (lowest-candidate-id, the ring-as-failure-detector the
   tuple-space/blackboard already use). Because the store is node-independent, the new curator simply
   resumes against the *same* store and re-drains the KV proposal queue — nothing to transfer.

**Why this is the wiki pattern's *native* shape.** The Karpathy LLM-wiki pattern — and Mycelium's own
`docs/wiki/` — is *files in a store, an LLM curator maintains them, everyone reads the files directly*.
The store handles durability and per-object atomicity; the curator handles curation and serialisation;
readers read. There is no distributed-consensus problem to solve, because we stopped inventing one.
The "concurrent prose merge" question **dissolves**: writes are serialised through one curator, so the
only merge is the curator's LLM reconciling a batch of proposals against current text (a 3-way merge —
design record §2), and the store's atomic per-object write commits it.

**The declined alternative — free-for-all inline LLM merge** — stays declined-with-evidence: every
agent LLM-merging in place is non-deterministic, so replicas diverge and never converge. Single-writer
curator is precisely what avoids it. (The KV-native section-CRDT — the *disconnected* variant in the
design record — is the other way to avoid it when there is no external store to point at.)

## Composition — the third layer, not a replacement for the other two

The two driving use cases both draw the same boundary: the wiki is the **specific / authoritative /
maintained** layer; it composes with — does not replace — a metrics/structure store and RAG.

| Layer | Home | Holds | Nature |
|---|---|---|---|
| Structure + metrics | **Postgres** (schema ≈ org / BU / system / domain) | typed nodes, edges, quantitative metrics | relational, *computed* by services/tools + telemetry |
| **Meaning / authoritative records** | **the wiki** (this crate) | the curated, maintained, converged narrative & specific records | prose canon, **keyed to the same id namespace** |
| Background / evidence | **RAG** | the fuzzy corpus | approximate recall |

- **UC1 — Novus-i2 organisational twin.** Management ↔ a domain agent iterate a domain's *meaning*
  canon toward self-consistency; the quantitative twin (couplings, critical path, agility) is
  *computed* into Postgres by services on Mycelium; RAG supplies background. The wiki holds the
  interpretation ("why this coupling is the SPOF, the de-jure-vs-real gap"), **keyed to the Postgres
  node ids** (`node = e_rl_rk`) so the agent joins meaning ↔ metrics. `group` = business-unit/domain;
  many parallel canons. UC1's payoff is the **LLM consistency loop** — the curator's lint generalises
  from structural checks to *semantic self-consistency* (no cross-section contradictions as new org
  info arrives).
- **UC2 — Transparency-Platform council decisions.** A community navigates *specific council
  decisions* surfaced from the wiki, positioned against Climate/Biodiversity evidence via RAG.
  `group` = the council/jurisdiction — **one** authoritative corpus; a community's "scope of interest"
  is a **query** over `{ward, topic, issue}` tags, not its own wiki. (Corpus-boundedness constraint:
  keep the *hot* corpus current/active and archive older decisions out of the served set — a durable
  registry is fine, an unbounded one is a design smell.)

Both need one addition over prose-only: **records carry structured `attributes`** — the shared id +
cross-cutting tags — and the read surface supports **`query(predicate)`** across pages (mirroring the
blackboard's `Fact { attributes, payload }` / `read(predicate)`). Attributes are the *join key and
scope tags*, not typed facets for computation (those are Postgres's).

## How it maps to Capability / Skill / Group — competence is advertised, knowledge is not

The recurring question ("is an agent's knowledge advertised as a capability?") has a sharp
answer that this crate must not blur: **competence and access are capabilities; knowledge
*content* is not — it is group-scoped Layer-I state, and the group is the bridge.** The
native atoms (`docs/guide/00-concepts.md`): a **Capability** is a declarative advertisement
("this node provides `ns/name`" — the discovery atom, found not called); a **Skill** is a
Capability *plus an executable handler*.

| Concept | Layer | Role in the wiki | Prefix |
|---|---|---|---|
| **Group** | II (scope) | The knowledge *community* + boundary — who is in the domain. Self-elected by a `CapabilityGroupDef` filter, no coordinator. | `gcap/{group}/…` |
| **Wiki / domain** | I (state) | The group's durable shared knowledge — long-term memory owned by the *group*, not any node. | `wiki/{group}/…` |
| **Capability — competence** | discovery | "I qualify for / am competent in this domain." The filter that auto-joins the group. | `cap/{node}/…` |
| **Capability — role** | discovery | The wiki role (curator / contributor / reader) for election + failover — same shape as `tuple.{ns}.primary`. | `cap/{node}/wiki.{group}.curator` |
| **Skill** | invocation | The invocable handler that *reads the group wiki* (+ blackboard) and calls the LLM — competence made runnable. | backed by a `cap/` |
| **Knowledge content** | I (state) | **Not a capability.** The prose itself; accessed by group membership. | inside `wiki/{group}/…` |

**The composition (how an agent gets to a domain's knowledge):** advertise a competence
capability → it matches the group's `CapabilityGroupDef` filter → **self-join** (no
coordinator) → group membership makes `Boundary::admits` pass reads of `wiki/{group}/*` →
the agent's skill consumes the wiki. So **access to a specific wiki/domain = group
membership**, and membership is *earned by advertising the qualifying capability*. The
content never enters the `cap/` namespace.

**Access control layers on top of membership** (only when the knowledge is sensitive):
`authorized_callers` restricts *who* may invoke a domain skill (WS-D, enforced where the
skill is served); RBAC clearance (WS1, data-classification-aware L1/L2/L3) can gate an
individual page — an L3 page admits only a caller whose *verified* role claim carries L3.
Both refine the capability→group→boundary chain; neither replaces it.

**Federation boundary:** at the edge, **AgentFacts publishes an agent's capabilities**
(competence) as the outward contract; the **wiki content stays internal to the group**. A
partner discovers "this cluster has domain-D competence," never domain D's pages — the
boundary primitive one level up (advertise *what you can do*, never *what you know*; the
MCB/exit invariant of `docs/wiki/domain/theory/coordinator-free-recursion.md`).

> **Anti-pattern to guard against (normative for the build):** never advertise knowledge
> *content* as capabilities. Capabilities are for "I can" / "I may access" (competence,
> role, qualification); the wiki is for "here is what we know" (state). A capability minted
> per fact collapses the discovery layer into the storage layer and explodes the `cap/`
> namespace. Keep them on opposite layers.

## Control-plane KV namespace (pointers + queue only — never the content)

The substrate holds only small, dynamic, self-healing *control* state; the corpus lives in the store.

- **Curator advertisement:** flat capability `wiki.{group}.curator|candidate` (capability key
  segments must not contain `/` — the flattening the tuple space needed). The winning curator's
  advertisement carries the **store location** (its endpoint / bucket-prefix), so a reader discovers
  *where* the wiki is by resolving the group's curator capability.
- **Proposal queue:** `wiki/{group}/proposal/{id}` — a small **evaporating** edit proposal
  (`refresh_interval`d, so a crashed author's proposal ages out). This is the *only* wiki data in KV,
  and it is bounded (in-flight proposals the curator quickly drains), which is exactly what gossip KV
  is good at — unlike the durable corpus.
- **Metrics / opacity:** `sys/wiki/{node}/{group}/…`; group admission via `Boundary::admits` on
  `SignalScope::Group`; group opacity can hide a wiki's *service* exactly as it hides `gcap/`.

## Roles

`WikiRole`, mirroring `TupleRole` / `BoardRole`:

- **`Curator`** — the group's wiki **service host + gateway**: drains proposals, runs the LLM
  ingest/reconcile and the periodic lint, **writes the store** (single writer of record), and
  **brokers read access** (hands out store location + a scoped, short-lived read grant to group
  members). Exactly one live per group.
- **`Contributor`** — proposes edits (append to the KV queue) and reads the store directly; never
  writes the store.
- **`Reader`** — read-only: obtains location + grant from the curator, reads the store directly (the
  common case; parallel, un-serialised).
- **`Auto`** — self-elects Curator with the lowest-candidate-id tie-break, promotes when the live
  curator's capability evaporates (ring-as-failure-detector, as the other two).

## Durability & failover — the store outlives the node

Durability is the **store's** job (FS dir / S3 / doc store — durable, atomic per object). The curator
is (near-)**stateless w.r.t. durability**: its state is *derivable* — it reconciles from the KV
proposal queue + current store content — so a curator handoff needs **no heartbeat/WAL-cursor**.
Because the store is node-independent, a promoted curator resumes against the *same* store and
re-drains the *same* KV proposals; nothing transfers. This is what makes the failover honest (§the
architecture): the store is where the knowledge lives, the curator is only who serves and writes it.

Ingest is **at-least-once, idempotent** — a proposal may be reconciled twice (curator crash
mid-reconcile, re-elected curator re-drains). Idempotence is the reconcile's contract: re-merging the
same proposals against current store content yields an equivalent write, and the store's atomic
per-object write (manifest last) means a torn intermediate is never observed. Third data point on the
effect contract (`docs/design/exactly-once-effect.md`): tuple = exactly-once blocking take;
blackboard = at-least-once claim/requeue; **wiki = at-least-once idempotent curator write to an
external store**.

## Phased build outline (when the trigger fires)

Mirrors the tuple-space / blackboard phasing. All public-API-only; core unchanged.

*(Re-phased 2026-07-03 for the control-plane/data-plane architecture. The KV-native section-CRDT is
no longer the build spine — it is the disconnected variant in the design record.)*

- **Phase 0 — design.** ✅ the identity/access mapping and the (now *disconnected-variant*) merge
  semantics are in [`docs/design/wiki-concurrent-edit.md`](../design/wiki-concurrent-edit.md). The
  primary control-plane/data-plane architecture is pinned in this plan (above). A short new design
  record for the **store interface + access-brokering + write-ordering** (manifest-last, torn-read
  safety, the scoped-grant scheme) is the remaining Phase-0 slice.
- **Phase 1 — the `Store` trait + a filesystem-dir reference impl.** ✅ **shipped (2026-07-03)** — the
  `mycelium-wiki` crate's **data plane**, deliberately Mycelium-agnostic (control plane behind the
  `control-plane` feature, Phase 2). Ships: the `WikiStore` **trait** (`read`/`query`/`write_page`/
  `list_pages`/`location`); the record model — `Section` (heading + body + join-key/scope
  `attributes`), `Manifest` (order, written **last**), `Page`, `Predicate` (structured attribute
  filter, *not* similarity), stable opaque `mint_section_id`; and **`FsStore`**, the filesystem-dir
  reference impl — atomic per-object writes, **manifest-last** (torn-read safe), manifest-authoritative
  reads, attribute `query` across pages, path-traversal-guarded. Unit-tested on a tempdir, no cluster.
  `S3Store` is a parallel impl (later). *(The earlier in-KV `WikiStore` was spiked and retired.)*
- **Phase 2 — the curator service + control plane.** ✅ **shipped (2026-07-03)** — behind the
  `control-plane` feature: `Wiki<S: WikiStore>` + `WikiRole` (Auto/Curator/Reader) + `WikiConfig`.
  Curator **election** (advertise `wiki.{group}.candidate` → settle → lowest-candidate-id becomes
  `wiki.{group}.curator`, else watch) + **ring-failover** (two empty `curator` resolves → re-elect),
  the **evaporating KV proposal queue** (`wiki/{group}/proposal/{id}` — `propose` writes it), and the
  **single-writer apply** (the curator drains → upserts the section → `write_page` → tombstones the
  proposal; idempotent). Reads go **direct to the store** (any role). Cross-node
  `tests/failover.rs`: two agents share one `FsStore` dir; one elects curator, applies a proposal both
  read; kill it, the survivor promotes and applies against the *same* store (no state transfer —
  passed, 2.8 s). *Remaining:* the **access broker** (scoped read grant → group-membership gate) — a
  store-adapter concern — and TTL-evaporation of abandoned proposals; the Phase-2 apply is a direct
  upsert (the LLM 3-way reconcile is Phase 3).
- **Phase 3 — the LLM reconcile.** ✅ **the reconcile shipped (2026-07-03)**; the lint loop is the
  open remainder. The curator's drain now **groups proposals by target section** and hands each batch to
  a pluggable [`Reconciler`] (a dyn-safe `#[async_trait]` trait), so a same-section conflict is resolved
  by *one* writer holding *all* the proposals — the single-writer dividend, no CRDT, no lost update.
  Two implementations:
  - `DirectReconciler` (default, no LLM) — a deterministic **lossless append-merge**: appends each
    distinct proposal body, skips one already contained. That skip is what makes it **idempotent**, so
    a curator can crash mid-drain and the re-elected one re-drains the same batch to the same result.
  - `LlmReconciler` (feature `llm`) — a real **3-way merge** over a `mycelium::LlmBackend`: the LLM
    curates the section **prose** (resolve conflicts, drop redundancy, keep meaning) while heading and
    attributes are merged **structurally** (code-controlled — the model does not invent join keys). Any
    backend error falls back to the append-merge, so an LLM outage degrades curation, never a write.

  *Verified:* 4 reconcile unit tests (new-section, lossless append, idempotent replay, attribute
  last-wins) + the `EchoBackend`-driven `llm` test proving the backend is called and its completion
  becomes the section body; the cross-node `failover.rs` still passes unchanged (the drain restructure
  preserved single-writer behaviour). *Remaining for Phase 3:* the **periodic lint loop** (generalising
  `/wiki-lint`: dead cross-links, orphans, the cited-fact check, and — for UC1 — **semantic
  self-consistency**, no cross-section contradictions) — a curator background task, separable from the
  reconcile and landing next.
- **Phase 4 — MCP tool + gateway + SDKs.** ✅ **the MCP tools shipped (2026-07-03)**; the bespoke REST
  routes + SDKs are the open remainder. [`Wiki::register_mcp_tools`] publishes three tools —
  `wiki.read` / `wiki.query` (served **directly from the store on the calling node** — the data-plane
  parallel-read property, so any node hosts them) and `wiki.propose` (enqueues to the curator) — over
  Mycelium's **existing** MCP invoke path: registration writes each tool's `inputSchema` to
  `tools/{name}/{node}` in KV for cluster-wide discovery, and dropping the returned [`WikiMcpTools`]
  guard tombstones them. No new transport and no fork of mycelium core — the companion-crate contract
  (public API only) is what makes the MCP path, not bespoke `/gateway/wiki/*` routes, the right shape.
  *Verified:* two single-node tests — the read/query handlers serve a seeded store (page JSON +
  attribute-filtered refs), and a new-section propose mints an id, lands on the KV queue, and
  registration publishes all three discoverable schemas. *Remaining for Phase 4:* the bespoke
  `POST /gateway/wiki/{read,propose}` + `GET /gateway/wiki/query` REST routes (a `gateway`-feature axum
  `Router` the app mounts via `GossipAgent::with_http_routes`, as the blackboard does) and the
  Python/TS `WikiClient` — both additive, and separable from the mesh-native MCP path above.
- **Phase 5 — worked example + CI smoke.** One of the driving use cases (a bounded UC2-style council
  corpus, or a UC1 domain canon) as a runnable example over `FsStore` + `ci_smoke.sh` (Docker-free),
  wired as a CI job — the pattern the other companions established.

## Non-goals

- **Not a human wiki server.** No web UI, no Markdown rendering, no Obsidian. The
  project's *own* `docs/wiki/` is human-and-agent authored via files + git; this crate is
  for **agent-fleet-internal** knowledge, queried over the mesh/gateway. (The recursion is
  intentional but the two are separate artifacts: `docs/wiki/` is the reference
  implementation of the *idea*; this crate is the *runtime primitive*.)
- **Not RAG, and not a metrics store — it *composes* with both.** The wiki is the specific /
  authoritative / maintained layer; the fuzzy background corpus is RAG's, the typed metrics/structure
  are Postgres's (see *Composition*, above). Retrieval is by attribute/tag/id, **not** embedding
  similarity — no vector index in the wiki; the agent joins the three layers by shared id.
- **Not the storage engine.** Durability, indexing, atomicity, backup are the pluggable store's job
  (FS dir / S3 / doc store); the crate supplies the *access/identity/curation control plane*, not a
  bespoke database. (The KV-native section-CRDT store is the disconnected-variant exception.)
- **Not deterministic LLM merge.** The single-writer curator is precisely what avoids the
  non-convergent free-for-all reconcile; do not attempt in-place multi-writer LLM merge.
- **Not a core change.** If a phase needs a core change, that is a signal to re-scope — the
  composability proof is that it rides the public API.

## Trigger to revisit / build — validate demand first

**The gating question is demand, not feasibility** (feasibility is settled: it builds on
the public API like its two siblings). Build when a concrete fleet shape appears that
**re-reads curated knowledge often enough to amortise curating it** — a long-lived agent
group with cross-agent, cross-time knowledge reuse and membership churn (support/ops
fleets, research fleets, a fleet that learns resolutions over time). Do **not** build it
speculatively for short-lived task fleets: they are already served by the blackboard +
KV, and a wiki is pure overhead there. The honest signal to watch: an agent group in a
real deployment (or a coop-suite demo) visibly re-deriving the same knowledge repeatedly
because it has nowhere durable and curated to put it. When that shows up, this sketch
promotes to a phased build plan (`v2-…-wiki.md`), exactly as the blackboard sketch did.

## Relationship to adjacent work

- **`docs/wiki/`** — the project's own LLM wiki; the reference implementation of the idea
  this crate would make a runtime primitive (dogfooding one level up).
- **`mycelium-blackboard`** — the working-memory sibling; a fleet would plausibly run
  both (blackboard for in-flight coordination, wiki for durable lessons).
- **Legible Emergence** (`legible-emergence.md`) — adjacent but distinct: that plan is
  *operator*-facing observability of fleet behaviour; this is *agent*-facing durable
  knowledge the fleet maintains for itself. They could compose (the fleet narrative could
  ingest into the wiki), but neither depends on the other.
- **Management-as-intent** (`docs/wiki/domain/theory/management-as-intent.md`) — the
  curator role is governed by the same litmus (vanishes ⇒ self-heals), which is what keeps
  it a participant and not a coordinator.
