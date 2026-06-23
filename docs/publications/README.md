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
