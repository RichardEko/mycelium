# Three-arm work-distribution sweep — dataset notes

Produced by `examples/three_arm_runner.sh` driving
`examples/three_arm_workdist.rs` (design: `docs/plans/three_arm_workdist.md`).
108 runs: {broker, gossip, pull} × H ∈ {0, 0.25, 0.5, 1.0} × δ̄ ∈ {0, 0.05,
0.2}/s × seeds {1, 2, 3}; N = 20 workers, λ = 65% of aggregate capacity
(162.5 jobs/s), 40 s arrival window with the first 5 s (ramp) excluded from
all metrics. Same seed ⇒ identical worker-speed trajectory and arrival
timetable in all three arms (verified: submitted counts match ±1 across arms
in every cell; the +1 is the pull arm's warmup probe item).

Collected 2026-06-12 on a single quiet host (macOS, Apple Silicon), runs
sequential, fresh loopback port range per run.

## Anomaly exclusions (3 of 108 runs re-run)

Three late-sweep pull runs (all seed 3) exhibited host-transient anomalies:
two stall-and-drain signatures (mean latency > 5× cell median, throughput >
offered during drain) and one arrival shortfall (~14% under the seed's fixed
timetable — impossible under absolute-schedule pacing except by host
starvation). Criterion applied: each was re-run once with the identical
seed/config on a quiet host; **none reproduced** (re-run means 83.5 / 95.3 /
106.9 ms, matching seed-1/2 neighbours; determinism cross-check: three
repeats of one config agreed within 0.04 ms). The re-runs replace the
anomalous rows. The originals are preserved in version control history; no
reproducing anomaly was excluded.

## Push arms are the strongest practical baseline (deliberately)

Least-outstanding-requests: job RPCs respond at completion and the decider
adds its exact local outstanding ledger to the gossiped load view (the broker
keeps its own ledger via completion callbacks). Single submission source ⇒
the ledger is complete, and workers do no hidden local work — both gifts
real fleets do not give, so these gaps are a **lower bound** (see the design
doc's scope note and the planned phase-2 axes).
