# Agentic Flow Networks — Fluid Pipeline Demo

A minimal, runnable demonstration of the **fluid pool** pattern:
a fixed pool of workers that dynamically flows through pipeline stages,
using the Mycelium KV ring as the distributed buffer.

## What this shows

| Property | Mechanism |
|---|---|
| **Substrate Unity** | KV ring = buffer; capability ring = scheduler. One gossip substrate. |
| **Fluid Allocation** | Same 10 workers handle all 4 stages. No static assignment. |
| **Topology Emergence** | Workers appear in `resolve_capability()` once their Mycelium node joins. |
| **TTL-Native Cleanup** | Abandoned claims expire automatically via gossip TTL. |

## Quick start

```bash
# From the Mycelium repository root:
cd examples/fluid_pipeline
docker compose up --build --scale worker=10
```

Watch the coordinator log — the pool visibly shifts from Parse → Enrich → Score → Aggregate.

## Architecture

```
                 coordinator
                     │
          ┌──────────┼──────────┐
          │  seed 200 articles   │
          │  into KV ring        │
          │  pipeline/stage-a/*  │
          └──────────┼──────────┘
                     │
          ┌──────────▼──────────┐
          │   drain_stage loop   │
          │  resolve_capability  │
          │  → rpc_call workers  │
          └──────────────────────┘

worker-1 … worker-10 (identical containers)
  ├── Mycelium gossip node (sidecar)
  │       peers → coordinator:57000
  └── Python worker
          advertises: stage_a/worker, stage_b/worker,
                      stage_c/worker, stage_d/worker
          serves:     stage_a.parse, stage_b.enrich,
                      stage_c.score, stage_d.aggregate
```

## Tuning the bottleneck

Stage C (`score`) simulates LLM latency with a configurable sleep:

```bash
STAGE_C_SLEEP=1.0 docker compose up --scale worker=10
```

Set it to `1.0`+ to clearly see all workers accumulate at the score stage
before draining through aggregate.

## Scaling

```bash
# Add 5 more workers while the pipeline is running:
docker compose up --scale worker=15 --no-recreate

# Remove workers:
docker compose up --scale worker=5 --no-recreate
```

New workers are discovered automatically via capability gossip.
Removed workers have their capability TTL expire (~15s) — the coordinator
stops routing to them.

## PostgreSQL results

```bash
docker exec afn-postgres psql -U pipeline -d pipeline -c \
  "SELECT id, composite_score, topics FROM articles ORDER BY composite_score DESC LIMIT 10;"
```
