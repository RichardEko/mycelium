# Mycelium documentation — map

The front door for `docs/`. Seven categorized areas plus two **root-level stance
anchors**. The convention: **root holds the foundational, cross-cutting "stance"
documents** (what the system *is*, and what an attacker *gains*); **everything else
is categorized by document *type*** — sequencing vs decision vs tutorial vs manual
vs runbook vs paper vs audit. Find the kind of answer you need, then the area.

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
| [`guide/`](guide/) | **Tutorial** — learn the system | The developer guide, chapters [00–14](guide/README.md) (concepts → gossip/KV → capabilities → … → patterns & pitfalls), plus the [cookbook](guide/cookbook.md) ("how do I…?") and [error-handling](guide/error-handling.md). Start at [00 · Concepts](guide/00-concepts.md). |
| [`operations/`](operations/) | **Runbook** — operate the system | DevOps + Solution/Dev runbooks (11): [deployment](operations/deployment.md), [observability](operations/observability.md), [dynamic-scaling](operations/dynamic-scaling.md), [tuning](operations/tuning.md), [artifacts](operations/artifacts.md), [rbac](operations/rbac.md), [sso](operations/sso.md), [audit](operations/audit.md), [cert-rotation](operations/cert-rotation.md), [crown-jewel](operations/crown-jewel.md). |
| [`design/`](design/) | **Decision record / ADR** — *why* a design choice | Point-in-time, cross-cutting design decisions + binding contracts: the [exactly-once-effect contract](design/exactly-once-effect.md) (the discipline the tuple-space + blackboard share) and the [OR-Map evaluation](design/or-map-gcap-evaluation.md) ("keep LWW+HLC"). The home for "we evaluated X, chose Y, here's why." |
| [`plans/`](plans/) | **Sequencing / execution record** — *how/when* something was built | Strategy + phased plans + their completed execution records (the *why-it-wasn't-built-this-way* reasoning too). See the [plans index](plans/README.md). As of 2026-06-21 all engineering plans are shipped. |
| [`reference/`](reference/) | **Manual** — *how to use* a feature | Reference docs, e.g. the [SkillRunner manual](reference/skillrunner.html). |
| [`publications/`](publications/) | **Paper** — the research track | The arXiv preprints + working drafts: "The Coordinator Trap" ([paper.md](publications/paper.md) / [arxiv/](publications/arxiv/)), the substrate-convergence paper ([paper2_…html](publications/paper2_substrate_convergence.html)), and a [presentation](publications/presentation.html). |
| [`analysis/`](analysis/) | **Audit series** — the project's own report card | [`ratings.md`](analysis/ratings.md): the periodic 25-dimension M2 self-audit (execution-evidence-gated, with a calibration ledger that scores the scores). Run via the `mycelium-analysis` skill. |

## Which area answers which question?

- *"What is this / why coordinator-free?"* → `philosophy.html`, then [guide 00](guide/00-concepts.md).
- *"How do I build with it?"* → [`guide/`](guide/) + the [cookbook](guide/cookbook.md).
- *"How do I deploy / monitor / scale / secure a cluster?"* → [`operations/`](operations/).
- *"Why was X designed this way (and not Y)?"* → [`design/`](design/) (decisions) + [`plans/`](plans/) (the execution record's reasoning).
- *"What's the security blast radius?"* → [`threat-model.md`](threat-model.md).
- *"How do I use feature Z?"* → [`reference/`](reference/) (+ the relevant guide chapter).
- *"What's the research basis / is it sound?"* → [`publications/`](publications/) + [`analysis/`](analysis/).

> Architecture invariants and the on-ramp for code-assistant sessions live in the
> repo-root [`CLAUDE.md`](../CLAUDE.md); the milestone *design* home is
> [`ROADMAP.md`](../ROADMAP.md). These docs are the elaboration, not a duplicate.
