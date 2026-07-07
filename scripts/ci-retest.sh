#!/usr/bin/env bash
# The CI flake tier (Run-38 floor fix — docs/wiki/dev/testing/testing.md §The CI flake tier).
#
# Socket-binding / multi-node test suites are timing-sensitive by nature; before this tier a
# single wall-clock flake redded main (2026-07-07: a wiki `AddrInUse` port race), and the
# reactive alternative — widening timeouts — is exactly how a Major liveness bug hid for ten
# analysis runs (Run 37's lesson). This wrapper is the structural compromise:
#
#   * run the suite once; all green → done.
#   * on failure, re-run ONLY the failed tests, individually, once.
#       - rerun fails      → REAL failure → exit 1 (a deterministic bug always reds the build:
#                            it fails twice).
#       - rerun passes     → the build stays green BUT the flake is loudly recorded — a GitHub
#                            warning annotation + step-summary line per test. A flake is a bug
#                            report with visibility, never noise: recurring annotations must get
#                            a root-cause issue, not a timeout widen.
#   * compile errors / suite-level failures with no parseable test list → exit 1 unchanged.
#
# Deterministic unit gates should stay on bare `cargo test`; route only the port-binding suites
# through this tier.
set -uo pipefail

log="$(mktemp)"
trap 'rm -f "$log"' EXIT

if cargo test "$@" 2>&1 | tee "$log"; then
  exit 0
fi

# Collect failed test names from every `failures:` block in the combined output.
mapfile -t failed < <(awk '/^failures:$/{f=1;next} f&&/^    [a-zA-Z_]/{print $1} f&&!/^    /{f=0}' "$log" | sort -u)

if [ "${#failed[@]}" -eq 0 ]; then
  echo "── ci-retest: failure with no parseable failed-test list (compile error?) — real failure ──"
  exit 1
fi

echo "── ci-retest: re-running ${#failed[@]} failed test(s) individually (flake tier) ──"
rc=0
for t in "${failed[@]}"; do
  if cargo test "$@" -- --exact "$t" 2>&1 | tee -a "$log"; then
    msg="$t failed once, passed on isolated retry (cargo test $*). A flake is a bug report — root-cause it (testing.md §The CI flake tier); never 'fix' it by widening a timeout (Run-37 lesson)."
    echo "::warning title=FLAKY TEST::${msg}"
    if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then
      echo "⚠️ **FLAKY**: \`$t\` (\`cargo test $*\`)" >> "$GITHUB_STEP_SUMMARY"
    fi
  else
    echo "── ci-retest: $t failed twice — real failure ──"
    rc=1
  fi
done
exit $rc
