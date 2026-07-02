# mycelium-wiki — design sketch

**Status:** 📋 **Proposed** (2026-07-02) — a design sketch, not yet started. Awaiting
go-ahead. This file is rationale + a phased build outline in the shape the
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

## The one load-bearing hard problem: concurrent prose edits

*(Now formalised — the full section-addressing scheme, merge semantics, curator state machine, and
convergence argument are in the Phase 0 design record
[`docs/design/wiki-concurrent-edit.md`](../design/wiki-concurrent-edit.md). This section is the
summary.)*

This is the crux, and it decides whether this is a weekend crate or a research project.
Two agents ingesting into the **same page** concurrently: LWW keeps one and silently drops
the other. You **cannot LWW-merge prose** the way the KV store merges opaque bytes —
losing half a paragraph is exactly the bookkeeping failure the pattern exists to prevent.
The design space, ordered by how coordinator-shaped each option is:

1. **Section-granular keys** — `wiki/{group}/{page}#{section-id}` as the LWW unit, not the
   whole page. Shrinks the collision blast radius to a paragraph; cheap; no coordinator.
   Necessary but not sufficient — two edits to the *same* section still collide.
2. **A recallable "curator" role** *(recommended, combined with 1)* — mirror `TupleRole` /
   `BoardRole`: agents **propose** edits (append to an ingest queue — a blackboard-style
   `post`, or a `wiki/{group}/proposals/…` prefix); the elected **curator** owns the LLM
   ingest step that reconciles a proposal into the live page, and the periodic lint. This
   is coordinator-shaped **for curation only**, but it passes the management-as-intent
   litmus (`docs/wiki/domain/theory/management-as-intent.md`): if the curator vanishes,
   the wiki stays fully **readable**, pending proposals persist as evaporating soft-state,
   and a new curator self-elects (lowest-candidate-id, the tuple/blackboard failover the
   ring already provides). It is a recallable participant, not a coordinator with a veto.
3. **Free-for-all inline LLM merge** *(rejected — the trap)* — every agent's LLM
   reconciles concurrent edits in place. Tempting and philosophy-flavoured, but it
   **breaks convergence**: LLM merges are non-deterministic, so two replicas reconciling
   the same pair of edits land on *different* text and never converge — violating the LWW
   convergence guarantee the whole substrate rests on
   (`docs/wiki/dev/architecture/runtime-invariants.md`). Only viable if the merge is made
   deterministic (pinned model + pinned prompt + canonical input ordering), which is
   fragile enough to be a research question, not a v1 mechanism. Record it as
   declined-with-evidence.

**Recommended v1:** (1) + (2). Section-granular LWW for the common case (edits to
different parts of a page never collide), and a single recallable curator serialising the
LLM reconcile for the same-section case. Deterministic where it can be (the KV/LWW
substrate does the convergence), LLM-in-the-loop only at the one serialised point (the
curator), which sidesteps the convergence problem because there is one writer of record
per page-group at a time. This is the same "single serving role + ring failover" shape the
other two companions proved; the novelty is only *what* the role does (LLM reconcile +
lint instead of `take`/`claim`).

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

## KV namespace + group scoping (all existing mechanism)

Group scoping is already in the substrate — no core change:

- **Pages:** `wiki/{group}/{path}#{section}` → section body (LWW + HLC, gossiped). The
  crate owns this prefix.
- **Proposals (ingest queue):** `wiki/{group}/proposal/{id}` — evaporating soft-state
  (`refresh_interval`), so a proposal from a crashed agent ages out; the curator drains
  and reconciles them.
- **Curator role / metrics:** flat capability names `wiki.{group}.curator|candidate`
  (capability key segments must not contain `/` — the same flattening the tuple space
  needed), plus `sys/wiki/{node}/{group}/…` for metrics. Read admission uses
  `Boundary::admits` on `SignalScope::Group`; **group opacity** can hide a wiki exactly as
  it hides `gcap/`.
- **Lint state:** `wiki/{group}/.lint/{ts}` — the curator writes lint findings as
  durable entries (the group-function analog of the `docs/wiki/**/.log/` discipline).

## Roles

`WikiRole`, mirroring `TupleRole` / `BoardRole`:

- **`Curator`** — serves the LLM ingest (proposal → reconciled page section) and the
  periodic lint; exactly one live per group.
- **`Contributor`** — proposes edits (queue append) and reads freely; never reconciles.
- **`Reader`** — read-only (query the wiki, no ingest). The common case for most agents.
- **`Auto`** — self-elects Curator with the lowest-candidate-id tie-break, promotes when
  the live curator's capability evaporates (ring-as-failure-detector, as the other two).

## Replication & durability

Pages are ordinary KV entries — gossiped, LWW+HLC, Merkle anti-entropy for free; a late
joiner converges via the (now chunked) `StateResponse` path. The **only** stateful role is
the curator, and its state is *derivable* (it reconciles from the durable proposal queue +
current pages), so — like the blackboard's `Post`/`Ack` model, and *unlike* the tuple
space — a curator handoff needs **no heartbeat/WAL-cursor**: a promoted curator reads the
live pages + pending proposals and continues. Proposals are evaporating soft-state
(at-least-once: a proposal reconciled twice is idempotent because the reconcile is
LWW-into-a-section keyed by proposal id). This is the deliberate simplification the
blackboard's divergence from the tuple space already established.

## The exactly-once question

Ingest is **at-least-once, idempotent** — not exactly-once. A proposal may be reconciled
twice (curator crash mid-reconcile, re-elected curator re-drains); the reconcile writes a
section keyed by content so a re-apply is a no-op LWW. This side-steps the WAL claim/ack
discipline the tuple space and blackboard share
(`docs/design/exactly-once-effect.md`) — the wiki does not need it because a page edit is
idempotent where a `take` is not. Record this as the third data point on that contract:
tuple = exactly-once blocking take; blackboard = at-least-once claim/requeue; wiki =
at-least-once idempotent reconcile.

## Phased build outline (when the trigger fires)

Mirrors the tuple-space / blackboard phasing. All public-API-only; core unchanged.

- **Phase 0 — merge design record.** ✅ **done ahead of build** —
  [`docs/design/wiki-concurrent-edit.md`](../design/wiki-concurrent-edit.md) pins the
  section-granular + recallable-curator scheme, formalises the section-id addressing (stable
  content-independent ids, not headings or line numbers), the three merge cases, the curator
  state machine, and records free-for-all-LLM-merge as declined-with-evidence (breaks the LWW
  convergence invariant). The build plan inherits it; Phase 0 at build time is only its §5 open
  questions (section split/merge, starvation, LLM cost batching).
- **Phase 1 — `BoardStore`-analog core.** `WikiStore` (pure, in-memory + the KV mapping):
  pages, sections, proposal queue, `read`/`propose`/`reconcile`/`lint` over
  `transient()`/`persistent()` test constructors. Unit-tested without a live cluster.
- **Phase 2 — agent-backed roles + failover.** `Wiki` (roles + RPC + curator election),
  `WikiRole`, ring-failover, cross-node `tests/failover.rs`.
- **Phase 3 — the LLM ingest/lint loop.** Curator drains proposals → LLM reconcile into
  sections; periodic lint (staleness, dead cross-links, orphan sections, and the
  cited-fact check generalised from `/wiki-lint`). Gated behind the `llm` feature; a
  no-LLM fallback stores proposals verbatim as append-only sections (still useful, just
  uncurated).
- **Phase 4 — gateway + SDKs.** `POST /gateway/wiki/{read,propose,pages}` +
  `GET /gateway/wiki/lint`; `mycelium-py/src/mycelium/wiki.py`, `mycelium-ts/src/wiki.ts`.
- **Phase 5 — worked example + CI smoke.** The long-lived support/ops fleet above as a
  runnable example + `ci_smoke.sh` (Docker-free), wired as a CI job — the pattern the
  other companions established.

## Non-goals

- **Not a human wiki server.** No web UI, no Markdown rendering, no Obsidian. The
  project's *own* `docs/wiki/` is human-and-agent authored via files + git; this crate is
  for **agent-fleet-internal** knowledge, queried over the mesh/gateway. (The recursion is
  intentional but the two are separate artifacts: `docs/wiki/` is the reference
  implementation of the *idea*; this crate is the *runtime primitive*.)
- **Not RAG / vector search.** Retrieval is by path/link/group, not embedding similarity.
  A vector index over pages is a possible later extension, explicitly out of v1.
- **Not deterministic LLM merge.** The curator serialises reconciles precisely *because*
  concurrent LLM merge doesn't converge; do not attempt free-for-all reconcile.
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
