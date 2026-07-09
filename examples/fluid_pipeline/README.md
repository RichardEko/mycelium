# Agentic Flow Networks — Fluid Pipeline Demo

## Objective

A tuple-space **PULL** pipeline with stigmergic backpressure, and the **PUSH**
baseline it refined away — run side by side, no dispatcher. A fixed pool of
identical workers carries the full stage repertoire and *flows* to where demand
is: if stage C is slow, the pool masses on stage C. No static assignment, no
rebalancing job, no configuration change — every distribution decision is made
by a worker, not a coordinator. See the concept in
[`docs/guide/07-pipelines.md`](../../docs/guide/07-pipelines.md) and the
push→pull refinement essay [`flow_networks.html`](flow_networks.html).

## How to run

See [shared setup](../README.md#shared-setup) for Docker Compose.

```bash
cd examples/fluid_pipeline
docker compose up --build --scale worker=10                    # pull (default)
PIPELINE_MODE=push docker compose up --build --scale worker=10 # baseline
```

**Expected seeder output (pull):**

```
seeder: seeding 200 articles into tuple stage-a (ns=pipeline)
seeder: seed complete — workers are already draining (no dispatch step exists)
seeder:   pressure: stage-a=142(+8 inflight)  stage-b=31(+6)  stage-c=9(+4)  stage-d=0(+0)   done=0/200
seeder:   pressure: stage-a=0(+0)  stage-b=12(+8)  stage-c=88(+10)  stage-d=21(+6)   done=61/200
seeder: === pipeline complete: 200/200 articles in 41.3s (4.8 items/s) ===
```

Scale up mid-run — new workers join the gossip mesh and start taking within
seconds; in pull mode there is nothing to tell them, they just start pulling:

```bash
docker compose up --scale worker=15 --no-recreate
```

Query final results:

```bash
docker exec afn-postgres psql -U pipeline -d pipeline \
  -c "SELECT id, composite_score, topics FROM articles \
      ORDER BY composite_score DESC LIMIT 10;"
```

## What it demonstrates

The flow `A>B>C>D` is **data topology, not node topology**: it exists as
tuple-space stages, and a worker is "at stage B" only for the duration of one
`take()` → process → `complete()` cycle.

```mermaid
graph LR
    SE["Seeder / Sink<br/>put 200 → watch done markers<br/>(edge client, decides nothing)"]

    subgraph "Tuple space (ns=pipeline, primary on seeder node)"
        SA["stage-a"]
        SB["stage-b"]
        SC["stage-c"]
        SD["stage-d"]
    end

    subgraph "Worker pool (10 identical, full repertoire)"
        W["each flow-loop:<br/>read depths → take() deepest stage<br/>→ process → complete() into next"]
    end

    SE -->|put| SA
    W -->|take| SA
    W -->|take| SB
    W -->|take| SC
    W -->|take| SD
    W -->|complete| SB
    W -->|complete| SC
    W -->|complete| SD
    W -->|"ack + done marker"| SE
```

**What to watch — the pressure front migrating.** Run with `STAGE_C_SLEEP=1.0`
and watch the seeder's `pressure:` lines: stage-c accumulates while a–b drain,
then the whole pool's inflight count concentrates on stage-c — no one told the
workers to move. Then run the *identical* workload under push and compare
wall-clock, dispatch retries, and coordinator log volume. Same stages, same
workers, same data — the only variable is who decides.

```bash
STAGE_C_SLEEP=1.0 docker compose up --build --scale worker=10
PIPELINE_MODE=push STAGE_C_SLEEP=1.0 docker compose up --build --scale worker=10
```

**Pull mode (the canonical AFN pattern).** The seeder puts items into stage-a
and counts done markers — it is not a coordinator
([`coordinator/coordinator.py`](coordinator/coordinator.py) runs the poll loop
here). Each worker ([`worker/worker.py`](worker/worker.py)) runs
`WORKER_CONCURRENCY` flow loops of take-deepest → process → complete:

- **Readiness is self-announcing** — a worker `take()`s when it is actually
  free, so push dispatch's staleness/misroute failure mode does not exist.
- **Fluidity is self-selection against pressure** — each loop reads the
  per-stage depths and takes from the deepest, massing on the bottleneck
  automatically (front migrates A→B→C→D).
- **Stage transitions are atomic** — `complete()` acks the input and puts the
  output in one WAL record; a crash cannot half-replay a hop. Unacked items
  re-queue after the in-flight deadline (at-least-once; handlers are idempotent).

**How stages "filter" their work — they don't.** One tuple space, but it is
lane-addressed, not content-matched: classic Linda retrieves tuples by template
matching over one flat bag; this space keeps Linda's generative decoupling and
blocking pull but replaces matching with **named per-stage FIFO lanes**
([`mycelium-tuple-space/src/store.rs`](../../mycelium-tuple-space/src/store.rs) —
per-stage queues + parked waiters). Payloads are opaque; an item is "at stage B"
because it sits in the `stage-b` lane, and `complete(id, "stage-c", out)` moves
it atomically to the next lane. A B-worker never searches for A-output — it just
`take("stage-b")`s, parking a waiter that fires the instant an upstream
`complete()` lands. The per-lane depth counters this gives for free are exactly
the pressure signal the fluid workers route on.

**Push mode (the pre-refinement baseline).** The original architecture, kept
runnable as the contrast case: workers advertise four stage capabilities and
serve RPCs; a **coordinator** resolves free workers and dispatches every item
through its own decision loop — the architecture the project's Paper 1 names
*the coordinator trap*. The KV ring is the buffer
(`pipeline/stage-{a,b,c,d}/{id}`), claim keys prevent double-dispatch, and the
coordinator drains stages one at a time. It works — and comparing it against
pull under stage skew is exactly how the refinement earns its keep.

| Property | Pull (canonical) | Push (baseline) |
|----------|------------------|-----------------|
| Who decides | Each worker, by taking when ready | Coordinator, by predicting who is free |
| Buffer | Tuple-space stages (single-copy, WAL-durable) | KV ring (replicated everywhere) |
| Stage transition | One atomic `complete()` record | Worker KV write + delete + claim cleanup |
| Crash recovery | In-flight deadline re-queues automatically | Claim-key TTL + coordinator retry |
| Failure surface | Tuple primary (secondary promotes via capability evaporation) | The coordinator itself |
| Topology emergence | Workers appear in the capability ring for observability | Coordinator resolves workers per dispatch |

**Tuple space hosting.** The seeder's sidecar Mycelium node runs with
`MYCELIUM_TUPLE_ROLE=primary` for namespace `pipeline`; worker nodes run as
`client`, so their gateway routes tuple ops to the primary via RPC. (For primary
failover — a secondary mirror promoting when the primary's capability evaporates
— see integration scenario 13 and the `mycelium-tuple-space` crate docs.)

**Pipeline stages** live in [`worker/stages/`](worker/stages/) (shared by both modes):

| Stage | File | What it does |
|-------|------|-------------|
| A — Parse | `parse.py` | Extract title, body, source, publication date |
| B — Enrich | `enrich.py` | Add tags, named entities, reading-time estimate |
| C — Score | `score.py` | Compute composite quality score (configurable sleep) |
| D — Aggregate | `aggregate.py` | Write final record to PostgreSQL |

**Demo assumption vs real deployment.** This demo uses *identical* workers that
each carry all four stages — that makes fluid allocation vivid, but it is the
demo's assumption, not Mycelium's. Nothing requires a monolithic worker:

- **Specialist workers** — some nodes serve only `stage-c` (the expensive LLM
  step), others only `stage-a`. In pull mode that's just a smaller `PIPELINE`
  dict: a worker takes only from stages it can serve.
- **Heterogeneous pools** — different repertoires deployed independently; the
  pipeline self-assembles from whatever is running.
- **Incremental rollout** — deploy `score_v2` workers alongside `v1`; they start
  taking from the same stage immediately. Drain `v1` by stopping its workers —
  items simply stop being taken. No flags, no orchestrator.

Each capability a node lacks is a stage it cannot flow to, so fluidity is
maximal when repertoires are uniform — a sizing dial, not a substrate constraint.

## Dev notes

**CI smoke harness.** [`ci_smoke.sh`](ci_smoke.sh) runs **both modes**
end-to-end without Docker: it starts a seeder node (tuple primary) plus two
worker nodes as local processes, drives a reduced workload (24 items) through
each mode with fresh nodes per phase, and asserts `pipeline complete` with the
full item count. Wired into CI as the `afn-smoke` job.

```bash
./examples/fluid_pipeline/ci_smoke.sh            # both modes
./examples/fluid_pipeline/ci_smoke.sh pull       # one mode
```

**Adding a stage.** Add a handler in `worker/stages/`, add one entry to
`PIPELINE` in `worker.py` (and `STAGES` in `coordinator.py` for push mode). The
tuple space creates stages on first use — no other changes.

**Plugging in a real LLM.** Replace the simulated sleep in `score.py` with an
LLM call; `STAGE_C_SLEEP` exists precisely to stand in for that latency.

**In-flight deadline sizing (pull).** Items taken but not acked re-queue after
the tuple space's `worker_timeout_secs` (default 300 s). Keep it 2–3× the
slowest stage's expected duration.

**Without Docker.** `ci_smoke.sh` is the reference for running everything as
plain processes; the short version:

```bash
cargo build --example three_node_demo
MYCELIUM_ROLE=node MYCELIUM_PORT=57400 MYCELIUM_HTTP_PORT=58400 \
  MYCELIUM_TUPLE_ROLE=primary MYCELIUM_TUPLE_NS=pipeline \
  ./target/debug/examples/three_node_demo &
# … worker nodes with MYCELIUM_TUPLE_ROLE=client, then worker.py / coordinator.py
```
