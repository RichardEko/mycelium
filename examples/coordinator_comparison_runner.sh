#!/usr/bin/env bash
# Coordinator-comparison runner — sweeps (mode, N) and collects per-decision
# CSV plus a summary line per run. Outputs to docs/publications/paper2a/data/.
#
# Usage:
#   ./examples/coordinator_comparison_runner.sh
#
# Tunables (env vars):
#   N_VALUES         — space-separated cluster sizes (default "10 20 40")
#   DURATION_SECS    — per-run measurement window (default 20)
#   DECISION_RATE_HZ — decisions per second (default 50)
#   OUT_DIR          — output directory (default docs/publications/paper2a/data)

set -euo pipefail

N_VALUES="${N_VALUES:-10 20 40}"
DURATION_SECS="${DURATION_SECS:-20}"
DECISION_RATE_HZ="${DECISION_RATE_HZ:-50}"
OUT_DIR="${OUT_DIR:-docs/publications/paper2a/data}"

mkdir -p "$OUT_DIR"
SUMMARY_FILE="$OUT_DIR/summary.csv"
echo "mode,n,decisions,considered,nones,mean_us,p50_us,p95_us,p99_us,mean_staleness,misroute_rate" > "$SUMMARY_FILE"

# Build once.
echo "▶ Building release binary…"
cargo build --release --example coordinator_comparison 2>/dev/null

PORT_BASE=29100
for n in $N_VALUES; do
    for mode in gossip broker; do
        CSV="$OUT_DIR/${mode}_n${n}.csv"
        echo ""
        echo "▶ mode=$mode N=$n duration=${DURATION_SECS}s rate=${DECISION_RATE_HZ}Hz"
        MODE=$mode N=$n DURATION_SECS=$DURATION_SECS DECISION_RATE_HZ=$DECISION_RATE_HZ \
            PORT_BASE=$PORT_BASE \
            ./target/release/examples/coordinator_comparison > "$CSV" 2>&1 \
        || { echo "  RUN FAILED"; continue; }

        # Extract the summary line from CSV (last line starts with "# SUMMARY").
        SUMMARY_LINE=$(grep "^# SUMMARY" "$CSV" | tail -1 | sed 's/^# SUMMARY //')
        echo "  $SUMMARY_LINE"

        # Parse the summary line into CSV columns.
        decisions=$(echo "$SUMMARY_LINE"      | sed -n 's/.*decisions=\([0-9]*\).*/\1/p')
        considered=$(echo "$SUMMARY_LINE"     | sed -n 's/.*considered=\([0-9]*\).*/\1/p')
        nones=$(echo "$SUMMARY_LINE"          | sed -n 's/.*nones=\([0-9]*\).*/\1/p')
        mean_us=$(echo "$SUMMARY_LINE"        | sed -n 's/.*mean_us=\([0-9.]*\).*/\1/p')
        p50_us=$(echo "$SUMMARY_LINE"         | sed -n 's/.*p50_us=\([0-9]*\).*/\1/p')
        p95_us=$(echo "$SUMMARY_LINE"         | sed -n 's/.*p95_us=\([0-9]*\).*/\1/p')
        p99_us=$(echo "$SUMMARY_LINE"         | sed -n 's/.*p99_us=\([0-9]*\).*/\1/p')
        mean_staleness=$(echo "$SUMMARY_LINE" | sed -n 's/.*mean_staleness=\([0-9.]*\).*/\1/p')
        misroute_rate=$(echo "$SUMMARY_LINE"  | sed -n 's/.*misroute_rate=\([0-9.]*\).*/\1/p')

        echo "$mode,$n,$decisions,$considered,$nones,$mean_us,$p50_us,$p95_us,$p99_us,$mean_staleness,$misroute_rate" \
            >> "$SUMMARY_FILE"

        # Cycle ports so successive runs don't collide on TIME_WAIT.
        PORT_BASE=$(( PORT_BASE + 200 ))

        # Brief inter-run pause to let kernel reclaim sockets.
        sleep 2
    done
done

echo ""
echo "════════════════════════════════════════════════════════════════"
echo "Summary written to $SUMMARY_FILE:"
echo "════════════════════════════════════════════════════════════════"
column -t -s, "$SUMMARY_FILE"
