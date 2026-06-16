# WS-B scale baseline — the "before" curve

This directory holds the Phase 0 baseline measurements for WS-B (Scale &
Transport). See [`docs/plans/v2-wsb-scale-transport.md`](../../../docs/plans/v2-wsb-scale-transport.md)
§Phase 0. The numbers here are captured on **current `main`** (pre-M4/M5) so every
later WS-B phase can be measured against them — that is what makes the Definition
of Done falsifiable rather than vibes.

## How to record / refresh

```sh
make test-scale-baseline                       # default curve: 30 50 70 100 workers
make test-scale-baseline BASELINE_WORKERS="30" # a single point
```

Runs **on the Docker host** (not inside a container) — the test-runner container
cannot see the seed's network namespace or the host FORWARD chain. Each point
brings the scale cluster up, waits for convergence, measures, and tears down.
Appends one CSV row per worker count to `scale-baseline.csv`.

## Columns

| Column | Source | Meaning |
|---|---|---|
| `timestamp`, `git_sha` | host | when / which commit produced the row |
| `n_workers`, `total_nodes` | harness | `total = workers + 1` (seed) |
| `converged` | mgmt `/api/state` | `yes` if mgmt saw all nodes before the timeout |
| `seed_established` | `docker exec seed` → `/proc/net/tcp{,6}` state 01 | **G1 metric** — persistent TCP connections terminating on seed. Expected to grow ~linearly with N today. |
| `host_conntrack_count` / `host_conntrack_max` | privileged `--net=host` probe → procfs | Docker-VM netfilter conntrack table size — the quantity that actually saturates the bridge. |
| `forward_rules` | privileged probe → `iptables -S FORWARD \| wc -l` | **G2 proxy** — FORWARD-chain rule count (O(N²) in the bridge). `na` if iptables isn't in the probe image. |
| `seed_task_count` | seed `/stats` | per-peer-writer fan-out proxy (one writer task per outbound connection). |
| `seed_store_entries`, `seed_dropped_frames` | seed `/stats` | store size + gossip backpressure at convergence. |

## Reading the curve

The WS-B thesis is that `seed_established`, `host_conntrack_count`, and
`forward_rules` all climb super-linearly toward the iptables ceiling well before
100 nodes. **M4** (bounded fan-out) should flatten `seed_established` /
`seed_task_count` (Gate G1); **M5** (UDP/TCP SWIM) should collapse persistent
heartbeat connections to ~O(1) and let `test-scale-resilience RESILIENCE_WORKERS=50`
pass (Gate G3). Re-run this baseline after each phase and diff the columns.
