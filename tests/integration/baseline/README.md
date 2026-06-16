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

**M4** (bounded fan-out) should flatten `seed_established` (Gate G1); **M5**
(UDP/TCP SWIM) should collapse persistent heartbeat connections to ~O(1) and let
`test-scale-resilience RESILIENCE_WORKERS=50` pass (Gate G3). Re-run this baseline
after each phase and diff the columns.

## Recorded baseline — `main` @ `81858ba`, 2026-06-16 (pre-M4/M5)

| N workers | total nodes | seed_established | host_conntrack_count | forward_rules | seed_task_count | dropped_frames |
|---:|---:|---:|---:|---:|---:|---:|
| 30  | 31  | 62  | 1956 | 3 | 11 | 0 |
| 50  | 51  | 102 | 3399 | 3 | 11 | 0 |
| 70  | 71  | 142 | 5701 | 3 | 11 | 81 |
| 100 | 101 | 202 | 8256 | 3 | 11 | 0 |

**Headline finding — `seed_established = 2 × total_nodes`, exactly linear.** Every
node holds two persistent TCP connections to seed (inbound + peer-exchange
outbound), so seed's connection table — and the cluster-wide total at O(N²) — grows
without bound. This is the precise quantity M4 must flatten to ~`2 × fanout`
(constant) and M5 must drive toward zero for the heartbeat path. `host_conntrack_count`
climbs in lockstep (1956 → 8256). The G1 target after M4: this column goes flat.

**Caveats for this platform / binary:**
- `forward_rules` is constant at 3 on Docker Desktop (the FORWARD chain holds only
  jump rules to DOCKER sub-chains; the O(N²) rule growth is a native-Linux-bridge
  artifact). On this platform **`host_conntrack_count` is the better G2 saturation
  proxy** — it is the table that actually fills.
- `seed_task_count` is flat at 11 (the demo binary does not track per-peer writers
  in its JoinSet) — **not** a fan-out proxy; use host-measured `seed_established`.
- `dropped_frames` is transiently non-zero (81 at N=70 here) — gossip backpressure
  during formation, not a steady-state signal.
