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
  **RESOLVED 2026-07-13:** added `benches/gateway_overhead.rs` (gateway HTTP path vs. direct
  in-process call, + a `/health` pure-transport floor). Measured on loopback: `/health` **≈40 µs**,
  `POST /gateway/kv` **≈44 µs** vs. direct `kv().set` **≈0.36 µs**, `GET /gateway/kv` **≈42 µs** vs.
  direct `kv().get` **≈0.02 µs** — so the real per-call gateway overhead is **~40 µs (0.04 ms)**, i.e.
  the "~1 ms" placeholder was **~25× pessimistic** (an *undersell*, the safe direction). Slide updated
  to "~40 µs loopback overhead (bench-measured, `benches/gateway_overhead.rs`)". The number is now
  reproducible: `cargo bench --bench gateway_overhead --features gateway,test-util`. Caveat kept
  honest by the bench's own doc — loopback, single node, so it isolates the axum+serde+reqwest
  round-trip, which is exactly what a single-node "overhead" figure should mean.
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
- 2026-07-14 (deck edit, not a lint run): fixed a stale internal count — `presentation.html` said the Food-Rescue Co-op was "eleven-demo" in the interop-slide footnote while its own "Fourteen demos" slide (and the wiki) say fourteen → corrected to `fourteen-demo`. Caught while adding the **Visual showcases** pointer (the four `*_viz` + conway, `:8090`–`:8094`) to the deck + `examples/README.md`. Staleness, not an overclaim.
- 2026-07-14 (lint run 4): **clean — verified the new Visual-showcases deck pointer.** Diff-gated to the one persuasion-surface change since run 3 (the showcase callout on the interop slide). Verified against the built examples: the ports cited (`:8090`–`:8094`) match what `conway`/`microgrid_viz`/`stigmergy_viz`/`redistribution_viz`/`llm_council_viz` actually bind; `llm_council_viz` is `EchoBackend`-only (zero key/Ollama refs) so "no LLM key" holds; no absolute/BFT/platform language introduced; the `examples/README.md` § Browser-showcases reference resolves. The eleven→fourteen count staleness was already caught+fixed in the same edit (`536f360`, logged above). No findings.
- 2026-07-14 (lint run 5): **`presentation.html:1047` — status overclaim (roadmap sold as active).**
  The 100-node scale card claimed *"true 100-node coverage runs nightly on self-hosted hardware."* But
  the self-hosted runner is **"queued until the runner is registered"** (`.github/workflows/scale-
  nightly.yml` header + `docs/wiki/dev/testing/scale-tests.md:43-45`), so the CI nightly does **not
  actually run** — the deck dropped the *awaiting-registration* caveat the wiki carries and presented
  wired infrastructure as active coverage. Survived lint runs 1–4 undetected. Severity: **Major**.
  **FIXED**: reworded to *"a nightly self-hosted scale workflow is wired for full 100-node coverage
  (`scale-nightly.yml`, awaiting runner registration)."* **Calibration (2nd hit of a class):** like
  the wire-`v11` staleness (run 1), the deck restated an internal-doc fact but **shed the internal
  doc's own hedge** — a status verb (`runs`/`gated`/`nightly`) asserted more than the wiki/workflow it
  derives from. **Sharpening folded into §1:** status claims must be reconciled not only against
  `ROADMAP`/`plans` milestones but against the *hedge in the source doc/workflow itself* — if the wiki
  says "queued until registered" and the deck says "runs", that gap is the finding. Cross-checked the
  customer-pitch's "100-node cluster formation … runs on every change" — **defensible, left as-is**:
  "✓ Demonstrated" is honest (`make test-scale` forms 100 nodes) and the "CI-gated with no retries"
  scenarios are the AFN/coop *smokes*, not the 100-node scale suite. Separately confirmed the decks'
  `/gateway/diagnose` + "no control plane to see the fleet" claims (`customer-pitch.html:330-341`,
  `presentation.html:1511`) are now *demonstrable* via the read-only Ops Console (`examples/ops_
  console.rs`) — consistent, no drift. Sole finding fixed; rest of the persuasion surface unchanged
  since run 4.
- 2026-07-14 (lint run 6): **clean — no persuasion-surface change since run 5, and the session's
  shipped work introduced no drift.** Diff-gated: no `docs/publications/` or `philosophy.html` commit
  since run 5. Verified the deltas that *could* have staled a deck claim: (a) the examples audit
  registered `diagnostics` — re-counted the coop suite (16 bins − 2 `*_viz` = **14 non-viz batch
  demos**), so `presentation.html`'s **"fourteen-demo"** is still exact (registration made an existing
  autobin explicit, adding no bin); (b) the deleted `mesh_demo` has **no** deck/paper reference; (c)
  the browser-showcase ports (`:8090`–`:8094`) are unchanged by the audit (the newly dark-themed
  *operator* demos `three_node_demo`/`llm_agent` are `:8080`/`:8100`, correctly *not* in the deck's
  pitch-showcase set); (d) the FAQ↔ch15 langgraph node-survival cross-linking is *consistent with* —
  and now better-demonstrated by — the papers'/philosophy's "no orchestrator" positioning, no
  overclaim. No findings.
- 2026-07-15 (lint run 7): **one Minor staleness — the deck's browser-showcase list fell behind the
  tree.** No `docs/publications/`/`philosophy.html` commit since run 6, but the session shipped a
  **new browser showcase**, `wiki_council_viz` (`:8095`, a live chat over a fleet of wiki-grounded
  specialists, no LLM key), and `presentation.html`'s "Want to watch it?" callout enumerated only
  `:8090`–`:8094` — a *pinned list that goes stale the moment the set grows* (the same class as the
  wiki examples-page by-category drift). Not an overclaim (the deck was accurate for what it listed);
  a **Minor undersell/staleness**. **Fixed**: appended `wiki_council_viz` to the callout. Re-verified
  `"fourteen-demo"` is untouched (the new showcase is a `mycelium-wiki` example, not a coop bin). No
  other drift.
- 2026-07-15 (lint run 8): **clean — deck kept current with the new capability, not caught stale.**
  `wiki_council_viz` gained a real on-mesh local LLM (Ollama served as the `llm/{model}` capability,
  phrased over the mesh; grounded-extraction fallback). The deck's showcase line was updated *in the
  same commit* from "no LLM key" to "each phrasing via a **local model served on the mesh** — Ollama,
  no cloud/key" — verified accurate against the code (`register`/`call_prompt_skill`), and **not an
  overclaim** (the LLM path genuinely works; the fallback is noted in README/examples). Pre-empted the
  "no LLM key" staleness that would otherwise have surfaced at the next lint.
- 2026-07-15 (lint run 9): **clean — deck showcase enumeration kept complete + accurate.** The new
  `guardrail_viz` browser showcase was added to the "Want to watch it?" callout *in its own commit*
  (`:8096` — "watch an agent structurally stopped at a Tier-C gate, with the tamper-evident denial
  proof"); the callout now lists all seven showcases `:8090`–`:8096`. Verified the claim against the
  running demo (unauthorized invocation → STOPPED + a sealed, chain-verified denial) — accurate, not
  an overclaim. The separate showcase-metrics change touched no persuasion surface. No findings.
- 2026-07-15 (lint run 10): **clean — no persuasion-surface change.** The UI-example-contract work
  (the contract page, the concepts box on nine browser examples, the wiki-lint check) touched
  `examples/`, `docs/wiki/`, and the skill — no `docs/publications/` or `philosophy.html` commit since
  run 9. Nothing to reconcile. No findings.
- 2026-07-15 (lint run 11): **clean — no persuasion-surface change.** The examples capability-matrix +
  five new run-doc READMEs touched `examples/`, `docs/wiki/`, and `docs/guide/09-security.md` — no
  `docs/publications/` or `philosophy.html` commit since run 10, and no deck/paper links into the renamed
  `examples/README.md` sections. Nothing to reconcile. No findings.
