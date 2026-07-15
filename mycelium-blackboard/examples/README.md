# Community Microgrid — blackboard examples

## Objective

Two runnable demos of the **blackboard companion** crate in *one constructive world*: a
neighbourhood **energy co-op** where a network of agents shares **ONE fact pool with no
dispatcher**. Both show the blackboard's defining split — the coordination primitive that sits
atop Mycelium layers **I** (gossip-KV) + **II** (signal-mesh):

- **Reading is unconditional + concurrent (`rd`)** — any number of readers observe every fact
  non-destructively.
- **Consuming a finite fact is competitive + exactly-once (`in`)** — executors race to claim a
  fact; exactly one wins, and each fact is consumed exactly once.

The two examples differ only in surface: `microgrid` is a CLI batch proof; `microgrid_viz` is the
live browser showcase of the same world.

## How to run

Both share the [repo setup](../../examples/README.md#shared-setup) for the toolchain. Then run
either example (commands per-block below).

### microgrid

**Objective.** A CLI batch demo (no browser) that proves the `rd`/`in` split end to end and exits
`0` on success — the minimal, assertable form of the co-op world.

**How to run.**

```bash
cargo run -p mycelium-blackboard --example microgrid
```

Prints the inverter posting surplus facts, two readers seeing the shared pool non-destructively,
and two storage executors competing to claim each surplus exactly once.

**What it demonstrates.** An inverter posts finite `surplus` facts (feeder 4, varying kWh),
gossiped to all. A forecaster and a tariff agent each `read` the whole pool concurrently and
non-destructively — both see every fact. Two storage executors then compete via `in`: each surplus
is claimed by exactly one, and the demo asserts every surplus is consumed exactly once. See the
module doc at the top of [`microgrid.rs`](microgrid.rs).

### microgrid_viz

**Objective.** The **browser showcase** of the same co-op world — it runs continuously and serves a
live animated canvas so you can *watch* the `rd`/`in` split happen, rather than reading batch output.

**How to run.**

```bash
cargo run -p mycelium-blackboard --example microgrid_viz --features gateway,metrics
```

Then open **http://127.0.0.1:8091/** (no external dependencies, works offline). Ctrl-C to stop.

**What it demonstrates.** An inverter posts a new `surplus` fact several times a second; posting and
drain rates fluctuate, so a visible pool builds and drains as glowing dots on the feeder bus. A
`forecaster` and a `tariff` agent track the shared, concurrent `rd` view ("sees N" counters); two
storage executors (`community-battery`, `ev-charger`) race to claim + ack each surplus, and the
dashboard shows a green **exactly-once** badge (red if a fact is ever claimed twice). Following the
repo's UI-example contract, the page carries a "what you're seeing" concepts box and a **⚙ Ops
Console** button (it self-advertises its UI via `ui/viz` KV keys); the `metrics` feature makes the
Ops Console **Metrics** tab populate live. See the module doc at the top of
[`microgrid_viz.rs`](microgrid_viz.rs).
