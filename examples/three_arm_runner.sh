#!/usr/bin/env bash
# Three-arm work-distribution sweep — drives examples/three_arm_workdist.rs
# across arms × heterogeneity (H) × drift (δ̄) × seeds and collects one
# summary CSV. Design doc: docs/plans/three_arm_workdist.md.
#
# Usage:
#   ./examples/three_arm_runner.sh                  # full sweep
#   HETS="0 0.5" DRIFTS="0 0.2" SEEDS="1 2" N=8 DURATION_SECS=15 \
#     ./examples/three_arm_runner.sh                # reduced pilot
#
# Tunables (env):
#   ARMS          — default "pull gossip broker"
#   HETS          — default "0 0.25 0.5 1.0"
#   DRIFTS        — default "0 0.05 0.2"
#   SEEDS         — default "1 2 3"
#   N             — default 20
#   DURATION_SECS — default 40 (RAMP_SECS=5 excluded from metrics)
#   OUT_DIR       — default docs/publications/arxiv/paper2a/data/three_arm

set -euo pipefail

ARMS="${ARMS:-pull gossip broker}"
HETS="${HETS:-0 0.25 0.5 1.0}"
DRIFTS="${DRIFTS:-0 0.05 0.2}"
SEEDS="${SEEDS:-1 2 3}"
N="${N:-20}"
DURATION_SECS="${DURATION_SECS:-40}"
RAMP_SECS="${RAMP_SECS:-5}"
WARMUP_SECS="${WARMUP_SECS:-12}"
OUT_DIR="${OUT_DIR:-docs/publications/arxiv/paper2a/data/three_arm}"

mkdir -p "$OUT_DIR"
SUMMARY="$OUT_DIR/summary.csv"
echo "arm,n,het,drift,seed,lambda_hz,submitted,completed,thr_hz,mean_ms,p50_ms,p95_ms,p99_ms,iwwe,jain" > "$SUMMARY"

echo "▶ Building release binary…"
cargo build --release --example three_arm_workdist 2>/dev/null

PORT=31000
total=0; for _ in $ARMS; do for _ in $HETS; do for _ in $DRIFTS; do for _ in $SEEDS; do total=$((total+1)); done; done; done; done
i=0
for seed in $SEEDS; do
  for het in $HETS; do
    for drift in $DRIFTS; do
      for arm in $ARMS; do
        i=$((i+1))
        echo "▶ [$i/$total] arm=$arm H=$het δ̄=$drift seed=$seed (ports $PORT+)"
        MODE="$arm" N="$N" HET="$het" DRIFT="$drift" SEED="$seed" \
        DURATION_SECS="$DURATION_SECS" RAMP_SECS="$RAMP_SECS" WARMUP_SECS="$WARMUP_SECS" \
        PORT_BASE="$PORT" \
          ./target/release/examples/three_arm_workdist 2>>"$OUT_DIR/run.log" >> "$SUMMARY"
        # Fresh port range per run: no TIME_WAIT interaction between clusters.
        PORT=$((PORT + N + 8))
        if [ "$PORT" -gt 63000 ]; then PORT=31000; fi
      done
    done
  done
done

echo "▶ Sweep complete → $SUMMARY"
column -t -s, "$SUMMARY" | head -20
