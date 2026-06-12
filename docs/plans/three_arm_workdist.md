# Three-arm work-distribution experiment — harness design

**Status:** In progress (started 2026-06-12). This is the experiment Paper 1
§9.5 names "Completing the three-arm work-distribution experiment" and Paper
2a § "What Remains Open Empirically" sets up. It exists to test a specific,
falsifiable prediction — not to produce a benchmark.

## The predictions under test (Paper 2a)

1. The outcome gap between the two *prediction* arms (broker, gossip) and the
   *pull* arm widens **monotonically with worker heterogeneity H**.
2. The gap widens **monotonically with drift δ̄** (the rate at which worker
   capability changes, making any observed state stale).
3. **Homogenisation corollary:** the prediction arms approach pull's outcomes
   only as H → 0 (workers made clone-like). At H = 0 the arms should be
   near-indistinguishable — that null result is part of the test.

## The three arms (identical substrate, identical workload)

| Arm | Who decides | Decision input |
|---|---|---|
| `broker` | One designated node answers `pick_worker` RPCs | Broker's gossip view of `wkr/*/load` |
| `gossip` | The submitting client itself | Client's *own* gossip view of `wkr/*/load` |
| `pull` | Each worker, by taking when free | None — readiness is the claim itself (tuple-space `take`) |

Both prediction arms use the same policy (lowest perceived queue depth); the
only difference between them is *where* the stale view lives (centre vs
edge). The pull arm uses `mycelium-tuple-space` with the primary on a
**dedicated space node** (the same topology slot as the broker arm's broker)
and submitter + workers as clients, so puts and takes both cross the same
real RPC machinery the push arms use — no colocation shortcut.

## Workload model

- **Open-loop Poisson arrivals** at rate λ (jobs/s). Open-loop is essential:
  latency, backlog, and idle-while-work-exists are only meaningful when the
  arrival process does not adapt to service capacity.
- **One job = `sleep(job_size / speed_i(t))`** on the assigned worker. Workers
  are single-server queues (one job at a time + FIFO local queue in push
  arms); queue depth is the advertised load.
- **Heterogeneity H** = coefficient of variation of worker speeds. Speeds are
  drawn lognormal and **normalised so aggregate capacity is constant across
  H** — H varies the *shape* of the fleet, never its total power (otherwise
  the sweep confounds heterogeneity with capacity).
- **Drift δ̄**: each worker's speed performs a random walk in log-space
  (relative step δ̄ per second, reflected at ±2σ bounds, re-normalised to
  preserve aggregate capacity). Drift is what makes any advertised or
  observed speed stale; δ̄ = 0 disables it.
- λ is set to ~65% of aggregate capacity by default — enough queueing for
  the dynamics to matter, stable enough to reach steady state.

## Outcome metrics (decision-level metrics deliberately absent)

Pull has no staleness/misroute vocabulary, so arms are compared only on what
the *jobs* and the *fleet* experience:

| Metric | Definition |
|---|---|
| **Latency** | `t_done − t_submit` per job; report mean / p50 / p95 / p99 over the post-warmup window |
| **Throughput** | completed jobs ÷ measurement window (with offered λ for reference) |
| **Idle-while-work-exists (IWWE)** | sampled every 25 ms: `Σ idle_workers(t)·Δt` over samples where ≥1 job is submitted-but-unstarted, normalised by `N × window` → a fraction in [0,1]. The signature failure of misrouting: capacity idle while work waits |
| **Fairness** | Jain's index over per-worker *utilisation* (busy-time fraction). Under heterogeneity the work-conserving ideal equalises utilisation, not job counts — fast workers do more jobs in the same busy time |

## Sweep

`examples/three_arm_runner.sh` sweeps:

- arms × H ∈ {0, 0.25, 0.5, 1.0} × δ̄ ∈ {0, 0.05, 0.20}/s, N = 20 default
  (env-overridable), ≥3 seeds per cell.
- Output: one CSV row per job (latency) + one summary row per run
  (throughput, IWWE, Jain) into `docs/publications/arxiv/paper2a/data/three_arm/`.
- Figure pipeline: `three_arm_plot.py` (gap-vs-H and gap-vs-δ̄ panels).

## Fairness-of-comparison rules

1. Same substrate config, same load-advertisement rate (10 Hz), same
   dispatch concurrency budget in both push arms.
2. The broker's serialisation of decisions is **not** mitigated (no broker
   sharding) — centralisation cost is the phenomenon, not a nuisance.
3. The pull arm gets no private shortcut: jobs flow client → primary lane →
   worker `take` over the same gossip/RPC substrate.
4. Aggregate capacity identical across arms and across H (normalisation
   above); the *same seed* produces the same worker-speed trajectory in all
   three arms of a cell.

## Scope note: this sweep is push's best case (deliberately)

The phase-1 sweep grants the push arms two advantages real fleets do not
have, so its results are a **lower bound on the push–pull gap**:

1. **Single submission source ⇒ complete outstanding ledger.** The
   least-outstanding-requests baseline sees every dispatch, so only the
   gossiped progress component can be stale. With S independent sources the
   per-source ledgers do not compose and the shared signal degrades to the
   gossip view alone (the herd returns at the seams); the broker arm retains
   ledger completeness only by serialising every decision — the bottleneck
   itself. Pull is indifferent to S: the lane serialises claims at the data
   path, not the decision path.
2. **No hidden local consumption.** Workers do only assigned work, so
   advertised queue depth is an honest state proxy. Local variability —
   background contention or work arriving at workers directly — consumes
   capacity invisibly to any remote view; pull absorbs it by definition.

**Phase 2 axes** (after the phase-1 sweep ships):

- `SOURCES=k` — k independent submitters with private outstanding ledgers
  (gossip arm), k clients sharing the broker (broker arm), k putters (pull).
  Prediction: gossip-arm degradation grows with k; broker pays serialisation;
  pull flat.
- `LOCAL_FRAC` — a per-worker private Poisson stream of local jobs entering
  the worker's queue *without* appearing in its advertised load until the
  next advert tick (and counted in busy/IWWE/fairness like any work).
  Prediction: push degrades with local fraction; pull flat.

Paper framing: report phase 1 as "even in the push paradigm's best case —
single source, complete outstanding ledger, no hidden local state — the
outcome gap exists and widens monotonically with H and δ̄"; phase 2 then
shows the gap's growth as the best-case assumptions are relaxed one at a
time.
