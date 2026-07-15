# dev/ui-example-contract — what every browser example must do

↑ [dev/](dev.md)

A **browser (UI) example** — one that serves an HTML dashboard a person opens — is held to a small
contract, so the set is consistent and self-explaining rather than a pile of one-offs. Reference
implementation: [`redistribution_viz`](../../../mycelium-tuple-space/examples/redistribution_viz.rs)
(+ `.html`). CLI examples (no served UI) are exempt.

## The four rules

1. **Build with the UI features — `gateway` + `metrics`.** The dashboard node runs a Mycelium gateway
   (so the Ops Console can target it) *and* installs the Prometheus recorder (so the console's
   **Metrics** tab populates). The example's run command carries `--features …,gateway,metrics` (or
   just `metrics` where gateway is default-on, e.g. the main crate / coop). See
   [examples/README.md § Browser showcases](../../../examples/README.md#browser-showcases) for per-showcase run commands.
2. **Be Ops-Console-present.** After start, advertise the two `ui/viz` KV keys —
   `ui/viz = http://host:port/`, `ui/label = <short name>` — and inject a `⚙ Ops Console` back-link
   into the page (`__OPS_CONSOLE_LINK__`, `cfg!(feature = "gateway")`-gated). This is the two-way
   linking; the reverse `↗ label` link is the console's job.
3. **Audit is an opt-in, not a default.** A UI example *may* expose the tamper-evident audit trail by
   running `compliance` (the guardrails crate uses `metrics-export` alongside for the Metrics tab);
   then the provider's `/gateway/audit` — and the console's **Audit** tab — show the seals. Off by
   default (most demos don't need it), one documented `--features …,compliance` to turn on.
4. **Carry the "what you're seeing" box.** The dashboard shows a panel naming the **Mycelium concepts
   & services this demo exercises** — the per-demo, in-context version of the
   [layer map](examples.md). Data-driven (below), not hand-written prose.

## The concepts box (rule 4) — the mechanism

The concept list is **data in the `.rs`**, injected into the HTML at serve time (like
`__OPS_CONSOLE_LINK__`), and drawn by one shared snippet — so the data lives where the demo knows
what it uses, and the render is identical everywhere.

**In the `.rs`** — a `CONCEPTS` constant (a JSON array; `tag` is a colour-coded layer/service key) and
one extra `.replace`:

```rust
const CONCEPTS: &str = r#"[
  {"tag":"I","name":"gossip-KV","gloss":"the tuple space is KV entries — LWW · HLC · anti-entropy"},
  {"tag":"companion","name":"tuple-space","gloss":"take/complete competitive claims — exactly-once effect"},
  {"tag":"IV","name":"capabilities","gloss":"…"},
  {"tag":"gateway","name":"gateway + metrics","gloss":"/stats · /gateway/fleet · /metrics — the Ops Console"}
]"#;
// … in the HTML-serve block:
let html = include_str!("X.html")
    .replace("__OPS_CONSOLE_LINK__", &console_link)
    .replace("__CONCEPTS__", CONCEPTS);
```

**`tag` vocabulary** (the shared render colour-codes these): `I` gossip-KV · `II` signal-mesh ·
`III` consensus · `IV` capability/agent · `companion` (tuple-space/blackboard/wiki) · `gateway`
(the operational edge) · `audit`/`security` (compliance / guardrails). Pick 3–6 that this demo really
uses; the gloss is one honest line each.

**In the `.html`** — the panel + the shared render snippet (copy verbatim from
`redistribution_viz.html`): the `.explains`/`.concept`/`.ctag`/`.cname`/`.cgloss` CSS, a
`<details class="explains" open>` block containing `<div id="concepts">`, and the `(function(){ const
C = __CONCEPTS__; … })()` renderer with the `COL` tag→colour map.

## The exceptions

- **`conway-gpu`** serves its canvas over a **raw `TcpListener`, not a Mycelium gateway** (the GPU
  stack is deliberately outside the workspace), so it cannot satisfy rules 1–2 without adopting a
  gateway — not Ops-Console-targetable.
- **`three_node_demo`** builds its chat page as an inline `&'static str` (not an `include_str!`'d
  file), so rule 4's *injected* concepts mechanism doesn't fit; it carries an equivalent **static**
  concepts panel instead. Content, not mechanism, is what the rule is really about.
- **`ops_console`** is the **console itself** — the *consumer/observer* of the `ui/viz` convention, not
  a showcase that advertises it. It serves an HTML dashboard (so it matches the `include_str!`
  enumeration) but by nature does not advertise its own `ui/viz`, inject an `⚙ Ops Console` back-link
  into itself, or carry a "what you're seeing" concepts box — it is the thing rules 2 & 4 exist to
  *serve*. Exempt from rules 2–4 (it does use the `gateway` feature — rule 1). Its own docs live in
  [`examples/ops_console/README.md`](../../../examples/ops_console/README.md).

Every other browser example complies with all four rules via the injected mechanism.

## Lint

`/wiki-lint` §4 checks the contract: enumerate browser examples the tree-derived way
(`grep -rl 'include_str!.*\.html' examples mycelium-*/examples`) and **classify every hit** as either
*compliant* (advertises `ui/viz`, injects `__OPS_CONSOLE_LINK__` **and** `__CONCEPTS__`, run command
carries `gateway,metrics`) **or** a *documented exception* above (`conway-gpu` · `three_node_demo` ·
`ops_console`). A hit that is neither — a browser example missing the mechanism **and** absent from the
exceptions list — is a finding (either fix it or classify it). An unclassified hit is the gap the
`ops_console` move exposed (ledger 2026-07-15).
