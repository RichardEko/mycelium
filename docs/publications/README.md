# Publications

A four-paper research corpus on **coordinator-free substrate design** — why
architectures that route coordination through a privileged centre fail under
heterogeneity, latency, and changing state, and what the alternative requires.
Each paper is published on Zenodo (CC BY 4.0) and cites its predecessors by DOI;
together they form one argument built in stages.

## The four-paper sequence

| Read | Paper | What it argues | DOI |
|---|---|---|---|
| **1** | **The Coordinator Trap** | In computing, coordinator-based agent architectures produce three structural failure modes that cannot be fixed by improving the coordinator — they follow from its existence. Derives the coordinator-free alternative (Holland's signal/boundary model) and presents a working implementation. | [10.5281/zenodo.20665238](https://doi.org/10.5281/zenodo.20665238) |
| **2** | **Heterogeneous Local Knowledge Systems (HLKS)** | The computing result is one instance of a cross-domain pattern. Introduces the HLKS abstraction and the (informal) Coordinator Trap Theorem; argues the parallel across economics (Hayek), governance (Ostrom), organisations (Beinhocker), and computing is structurally *homologous*. The contribution: locally evaluated *prediction* only shrinks coordinator error; self-resolved *pull* eliminates the variable in which it is expressed. **Epistemic correctness.** | [10.5281/zenodo.20813058](https://doi.org/10.5281/zenodo.20813058) |
| **3** | **The Capture Problem** | Epistemic correctness is *necessary but not sufficient*. A substrate that resolves all knowledge locally can still be *captured* — power and knowledge are orthogonal. Introduces four further properties (Capture Resistance, Mandate TTL, Epistemic Symmetry, Exit) and confronts the hardest question: does coordinator-freeness prevent capture or relocate it? **The power dimension; closes the sequence.** | [10.5281/zenodo.20813463](https://doi.org/10.5281/zenodo.20813463) |
| **◆** | **Monetary Ecology** | The framework paper the distributive/capture argument draws on: an **MCB/P/S/Î** evaluation of monetary architectures — Moral Circle Breadth (who is counted), Polycentricity (who decides), Surplus-Claim Regime (who benefits), and an Information-Rights diagnostic (Î). Read alongside 2a/2b; the source of the MCB/P/S lens they apply. | [10.5281/zenodo.20811062](https://doi.org/10.5281/zenodo.20811062) |

**Dependency graph** (`A → B` = B cites A):

```
Coordinator Trap ──┐
                   ├──► HLKS (2a) ──► The Capture Problem (2b)
Monetary Ecology ──┴───────────────────► (also cited by 2b)
```

Read 1 → 2 → 3 for the core argument; **Monetary Ecology** can be read before 2a or
alongside it (it defines the evaluative framework 2a and 2b reuse). Sources are one
directory per paper (`paper1/`, `paper2a/`, `paper2b/`, `monetary-ecology/`); build a
PDF with `tectonic main.tex` from inside the relevant directory. Rendered PDFs are
derived artifacts and are not tracked.

## Decks

| File | Audience |
|---|---|
| [`presentation.html`](presentation.html) | Engineer-facing — architecture and strategy. |
| [`customer-pitch.html`](customer-pitch.html) | Buyer-facing — value, data sovereignty, trust. |

---

# Licensing notice

The documents in this directory (papers, the presentation deck, and their
LaTeX/HTML sources) are **not** covered by the repository's AGPL-3.0-only
code license.

Unless a document states otherwise on its own title page:

> © 2026 Tathata Systems Ltd. Licensed under the
> [Creative Commons Attribution 4.0 International License](https://creativecommons.org/licenses/by/4.0/)
> (CC BY 4.0). Author: Dr. Richard Nicholson.

You may share and adapt these documents for any purpose, including
commercially, provided you give appropriate credit (CC BY 4.0 §3).

The AGPL-3.0-only license continues to apply to all source code in this
repository, including code snippets *as they appear in the codebase*. Short
code excerpts reproduced inside the papers are part of the CC-licensed
scholarly work for the purpose of reading and citation; taking code into a
software product remains governed by the repository's code license (or the
commercial embedding license — contact tathatasystems@proton.me).

Cite the lead paper as:

> R. Nicholson, "The Coordinator Trap: Structural Scaling Liabilities in
> Mediated Multi-Agent Architectures and a Substrate-Based Alternative,"
> Tathata Systems Ltd, 2026. DOI: [10.5281/zenodo.20665238](https://doi.org/10.5281/zenodo.20665238)

## Overclaim ledger

Dated record of persuasion-surface claims found drifting from shipped reality — the calibration
signal for the `/publication-lint` skill (is it catching overclaims before humans do?).

- 2026-07-11 (lint run 1): **`presentation.html:926` — unsourced perf number.** "~1 ms overhead"
  for the language-bridge HTTP gateway has no backing benchmark (`benches/` covers kv / scan_prefix
  / signal_fanout / capability_resolve, not gateway overhead). FLAGGED, not auto-edited (author's
  perf copy): either add a gateway-overhead bench or soften to "sub-millisecond / negligible next to
  LLM inference latency." Severity: Major.
- 2026-07-11 (lint run 1): fixed two version/terminology staleness (not overclaims):
  `presentation.html:1298` stale "wire v11" → v12; `:1210` non-canonical "Broadcast" scope →
  "Cluster" (the code/guide/wiki vocabulary is Cluster·Group·Individual).
- 2026-07-13 (lint run 2): **clean re: overclaim — no new finding.** Targeted the delta since run 1
  (this session's wiki section-CAS + `coordination-approaches.md`). Verified honest:
  the two Byzantine references in `paper1` are proper **CFT-not-BFT disclaimers** (§291, §367); the
  `presentation.html:1042` distributed-lock "two Critical bugs (no mutual exclusion; unreleasable)"
  is **self-audit framing of #164 as *found & fixed* with regression gates**, not a live defect; all
  `guarantee` uses are qualified (CAP/at-least-once), and every "platform / control-plane / cluster
  manager" hit is the *"Mycelium is **not** a platform"* contrast, not a self-description. **Avoided a
  bad auto-fix:** `paper1.md` §367/§373 pin `(wire version 10)` / `(wire version 11)` — these are
  **provenance-correct** (signing landed at v10, `hlc_seq` at v11, per `framing.rs`), *not* stale
  current-version claims; "fixing" to v12 would have been wrong. Two carried/notable, neither
  auto-edited:
    - **Carried (Major, still open):** `presentation.html:926` "~1 ms overhead" — still unsourced (no
      gateway-overhead bench in `benches/`); now reads "invisible next to LLM inference" (context
      added) but the bare number persists. Same recommendation as run 1: add a bench or drop the number.
    - **Minor (fixed 2026-07-13, at user request):** `presentation.html:1797` said "the
      concurrent-prose-merge problem dissolves into single-writer-curator + a **dumb store**" — which
      trailed shipped reality after the store gained **section-granular compare-and-swap** on
      2026-07-13 (the eventual-single curator alone had a lost-update window; the CAS is what makes it
      airtight). Updated to "a single-writer curator + *per-section compare-and-swap* on the store — the
      curator serialises edits, and the CAS keeps even a transient dual-curator (mid-failover) from
      losing one." Keeps the slide's "the hard problem dissolves" thrust while naming the real mechanism.
  §6 relative links in both decks + `philosophy.html` all resolve.
- 2026-07-13 (lint run 3): **clean — verified the newly-added persuasion content, not a fresh
  finding.** Diff-gated to the deck edits made since run 2 (the new "The CAP Choice" slide + its
  consistency spectrum + the `dumb store`→section-CAS fix). Verified against code: the slide's **CP
  row** (`consistent_set` / `distributed_lock` / `elect_leader`) is consensus-backed
  (`consensus_handle.rs`, routes through `cluster_propose`/`group_propose`); the **companions row**
  (tuple-space / blackboard / wiki) calls **no** consensus (ring-elected) — the classification is
  correct. Honest ceiling held: the slide says "safe, not linearizable", "blocks ≠ fails",
  "exactly-once *effect*" (not delivery), and "**CAP isn't bypassed**… paying in weaker consistency,
  not availability" — the CAP claim is stated as *relocated, not escaped*, matching
  `coordination-approaches.md` (§5 cross-artifact consistent). Noted-and-kept (defensible, not a
  finding): "a fencing token / CAS makes two writers **harmless**" — strong wording, but true in the
  no-lost-update sense the section-CAS guarantees (never-lose). **Carried (Major, still open):** the
  `presentation.html:926` "~1 ms" gateway number remains unsourced.
