# Mycelium documentation — map

The front door for `docs/`. Eight categorized areas plus two **root-level stance
anchors**. The convention: **root holds the foundational, cross-cutting "stance"
documents** (what the system *is*, and what an attacker *gains*); **everything else
is categorized by document *type*** — sequencing vs decision vs tutorial vs manual
vs runbook vs paper vs audit vs synthesis. Find the kind of answer you need, then
the area.

> **Agents / maintainers start at [`wiki/`](wiki/wiki.md)** — the LLM-maintained
> knowledge base that synthesises the current state across all the areas below
> (schema: [`wiki/AGENTS.md`](wiki/AGENTS.md); code is canon, the wiki cites it).

## Root anchors

The two documents you read to understand the system's *position* — referenced from
across the tree, deliberately at the root rather than filed under one area.

| Doc | The stance it anchors |
|---|---|
| [`philosophy.html`](philosophy.html) | **Purpose** — what Mycelium *is* and why (the coordinator-free thesis). The authoritative definition of intent. |
| [`threat-model.md`](threat-model.md) | **Security posture** — the crown-jewel blast-radius model: what an attacker gains at each trust boundary, the mitigations, the residual risk an operator owns. A standing posture (updated as the system evolves), not a point-in-time decision. |

## The seven areas

| Area | Document *type* | What lives here |
|---|---|---|
| [`guide/`](guide/) | **Tutorial** — learn the system | The developer guide, chapters [00–17](guide/README.md) (concepts → gossip/KV → capabilities → … → patterns & pitfalls → federation), plus the [cookbook](guide/cookbook.md) ("how do I…?") and [error-handling](guide/error-handling.md). Start at [00 · Concepts](guide/00-concepts.md). |
| [`operations/`](operations/) | **Runbook** — operate the system | DevOps + Solution/Dev runbooks — **start at the [operations index](operations/README.md)** ("Start here" funnel): the [go-live checklist](operations/production-readiness.md), [deployment](operations/deployment.md), [observability](operations/observability.md) + the [metrics reference](operations/metrics.md), [diagnostics](operations/diagnostics.md), [dynamic-scaling](operations/dynamic-scaling.md), [tuning](operations/tuning.md), [artifacts](operations/artifacts.md), [companions](operations/companions.md), [rbac](operations/rbac.md), [sso](operations/sso.md), [audit](operations/audit.md), [cert-rotation](operations/cert-rotation.md), [crown-jewel](operations/crown-jewel.md). |
| [`design/`](design/) | **Decision record / ADR** — *why* a design choice | Point-in-time, cross-cutting design decisions + binding contracts: the [exactly-once-effect contract](design/exactly-once-effect.md) (the discipline the tuple-space + blackboard share), the [OR-Map evaluation](design/or-map-gcap-evaluation.md) ("keep LWW+HLC"), the [group-wiki concurrent-edit record](design/wiki-concurrent-edit.md) (section addressing + curator state machine — retained as the disconnected KV-native variant now that [`mycelium-wiki`](plans/mycelium-wiki.md) has shipped its control-plane/data-plane build), the [legible-emergence taxonomy](design/legible-emergence-taxonomy.md) (Phase 0 of the diagnosability plan — pathology classification + the `ViewConfidence` reframe), and the [artifact-library record](design/artifact-library.md) (✅ adopted & implemented 2026-07-07 — the durable origin tier + librarian discovery for content-addressed artifacts, the artifact-kind/runtime generalization of install (`WasmHost` becomes one runtime among several), and resource-aware install eligibility; implements the shared-store pattern the [artifacts runbook](operations/artifacts.md) prescribes). The home for "we evaluated X, chose Y, here's why." |
| [`plans/`](plans/) | **Sequencing / execution record** — *how/when* something was built | Strategy + phased plans + their completed execution records (the *why-it-wasn't-built-this-way* reasoning too). See the [plans index](plans/README.md). As of 2026-06-21 all engineering plans are shipped. |
| [`reference/`](reference/) | **Manual** — *how to use* a feature | Reference docs, e.g. the [SkillRunner manual](reference/skillrunner.html). |
| [`publications/`](publications/) | **Papers + decks** — the research & pitch track | One directory per paper: [`paper1/`](publications/paper1/) — "The Coordinator Trap" ([paper.md](publications/paper1/paper.md) + LaTeX source); [`paper2a/`](publications/paper2a/) — the substrate-convergence paper (LaTeX + [working draft](publications/paper2a/substrate_convergence.html)). Plus two decks — the architecture/strategy [presentation](publications/presentation.html) (engineer-facing) and the buyer-facing [customer pitch](publications/customer-pitch.html) (value/sovereignty-led). Rendered PDFs are derived, not tracked. Kept honest against shipped reality by the `publication-lint` skill (claims-vs-code + overclaim/framing checks). |
| [`analysis/`](analysis/) | **Audit series** — the project's own report card | **Start at [`analysis/README.md`](analysis/README.md)** — *how Mycelium audits itself* (the self-audit system + calibration ledgers, for technical reviewers). [`ratings.md`](analysis/ratings.md): the periodic 25-dimension M2 self-audit (execution-evidence-gated, with a calibration ledger that scores the scores). Run via the `mycelium-analysis` skill. Plus [`doc-coverage.md`](analysis/doc-coverage.md): the WHAT/WHY/HOW × Dev/Ops documentation-coverage audit (matrix + remediation; a re-run diff target), run via the `doc-coverage` skill. |
| [`wiki/`](wiki/wiki.md) | **Synthesis** — the LLM-maintained knowledge base | The current-state, cross-linked wiki compiled from all the areas above (Karpathy LLM-wiki pattern): [`dev/`](wiki/dev/dev.md) (architecture invariants, concurrency discipline, testing lore, security, companions, delivery history) + [`domain/`](wiki/domain/domain.md) (coordinator-free theory, publications, strategy). Schema: [`AGENTS.md`](wiki/AGENTS.md). Query-first for agents; linted via the `wiki-lint` skill. |

## Which area answers which question?

- *"What is this / why coordinator-free?"* → `philosophy.html`, then [guide 00](guide/00-concepts.md).
- *"How do I build with it?"* → [`guide/`](guide/) + the [cookbook](guide/cookbook.md).
- *"How do I deploy / monitor / scale / secure a cluster?"* → [`operations/`](operations/).
- *"Why was X designed this way (and not Y)?"* → [`design/`](design/) (decisions) + [`plans/`](plans/) (the execution record's reasoning).
- *"What's the security blast radius?"* → [`threat-model.md`](threat-model.md).
- *"How do I use feature Z?"* → [`reference/`](reference/) (+ the relevant guide chapter).
- *"What's the research basis / is it sound?"* → [`publications/`](publications/) + [`analysis/`](analysis/).
- *"What's the current reconciled state of X?" (agent onboarding)* → [`wiki/`](wiki/wiki.md).

> Architecture invariants and the on-ramp for code-assistant sessions live in the
> repo-root [`CLAUDE.md`](../CLAUDE.md); the milestone *design* home is
> [`ROADMAP.md`](../ROADMAP.md). These docs are the elaboration, not a duplicate.
