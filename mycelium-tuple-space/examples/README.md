# Tuple-space redistribution — example pair

## Objective

Two runnable demos for the **tuple-space companion** crate — the food-redistribution sorting
pipeline built on the tuple-space primitive. A community redistribution hub moves donated produce
through three stages — `intake` → `sorted` → `routed` — with **no dispatcher**: workers *pull*,
never get pushed to, and the queue depth is the only coordination signal. The primitive underneath
is `take`/`complete` — competitive single-copy claims that give an **exactly-once** effect — layered
on Mycelium I (gossip-KV: LWW · HLC · anti-entropy) + II (signal-mesh). One demo is a CLI batch run;
the other is the same world as a live browser showcase.

## How to run

Both share the [repo toolchain setup](../../examples/README.md#shared-setup). Neither needs Docker or
external services. Then run either example below.

### `redistribution`

**Objective.** A finite batch of 12 donations flows `intake` → `sorted` → `routed` and exits 0 —
the shortest end-to-end proof of the tuple space's two defining moves.

**How to run.**

```bash
cargo run -p mycelium-tuple-space --example redistribution
```

Prints the dock posting donations, then per-stage worker progress; exits 0 on success.

**What it demonstrates.**

- **Single-copy competitive `take`** — several workers park on the same stage and each queued item
  is handed to exactly ONE of them (the Linda `in` primitive; the pipeline counterpart to the
  blackboard's `rd`/`in` split). Add or remove a sorter and throughput changes with no config.
- **Atomic stage advance (`complete`)** — acking an item and posting its successor to the next stage
  is ONE WAL record, so there is no crash window between stages: every donation advances
  at-most-once per stage and the pipeline delivers each exactly once. Workers drain each stage until
  they see consecutive empty timeouts. See the header of
  [`redistribution.rs`](redistribution.rs) for the full walkthrough.

### `redistribution_viz`

**Objective.** The same pipeline as a **live browser showcase** you can *watch* — two sorters race
on `intake`, two routers race on `sorted`, a dispatch collector consumes the terminal `routed`
stage, and a dock feeds donations on a burst/lull cycle so a backlog visibly builds then drains. It
runs **continuously** (Ctrl-C to stop). This is the **reference implementation** of the UI-example
contract's concepts-box mechanism.

**How to run.**

```bash
cargo run -p mycelium-tuple-space --example redistribution_viz --features gateway,metrics
```

Then open **http://127.0.0.1:8093/** — no external dependencies, works offline. Ctrl-C to exit.

**What it demonstrates.**

- The animated canvas shows competitive `take` and atomic `complete` happening live; the dashboard
  asserts no id is delivered twice and shows a green **"exactly-once"** badge (red if ever violated).
- **UI-example contract compliance** (`docs/wiki/dev/ui-example-contract.md`): built with
  `gateway,metrics`, it self-advertises via `ui/viz` KV keys, links to the Ops Console via a
  **⚙ Ops Console** button, and carries a "what you're seeing" concepts box mapping each visible
  behaviour to the layer/service it exercises.
- The **`metrics` feature** makes the Ops Console **Metrics** tab populate live while the demo runs.

See the header of [`redistribution_viz.rs`](redistribution_viz.rs) for the design notes.

## Deployment launchers (not showcases)

`scrape_fleet_node.rs` / `scrape_worker_node.rs` are **operational launchers** for an external
scraper-fleet deployment (a WAL-backed Primary and per-worker Client sidecars for a real workload),
not pedagogical examples — they demonstrate nothing the pair above doesn't, so they are deliberately
absent from the [capability matrix](../../examples/README.md#the-capability-matrix). Deployment
knobs come from `GossipConfig` env overrides (`GOSSIP_CLUSTER_NAME` et al.); both require
`--features gateway` (registered in `Cargo.toml` so feature-less builds skip them).
