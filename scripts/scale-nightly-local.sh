#!/usr/bin/env bash
# Local nightly scale-suite runner for macOS / Apple Silicon (Option B — no GitHub runner).
# Fired by a launchd LaunchAgent (scripts/launchd/com.mycelium.scale-nightly.plist), or run
# by hand. Starts Colima if Docker isn't reachable, runs the scale suites, tees each to a
# timestamped log, appends one result line per suite to a local CSV, and always tears the
# clusters down. Docker Desktop is deliberately NOT used — it needs a GUI session; Colima is
# headless and works for an unattended 02:00 run.
#
#   scripts/scale-nightly-local.sh            # all three suites
#   scripts/scale-nightly-local.sh resilience # one suite: all|scale|resilience|entries
#
# Results land in $MYCELIUM_SCALE_RESULTS (default ~/mycelium-scale-results): per-suite logs
# plus results.csv (timestamp,suite,exit_code,result,note,log).

set -uo pipefail

# launchd runs with a minimal PATH — put Homebrew (arm64), cargo, and system bins on it.
export PATH="/opt/homebrew/bin:/opt/homebrew/sbin:/usr/local/bin:${HOME}/.cargo/bin:/usr/bin:/bin:/usr/sbin:/sbin"

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RESULTS="${MYCELIUM_SCALE_RESULTS:-${HOME}/mycelium-scale-results}"
STAMP="$(date +%Y%m%d-%H%M%S)"
SUITE="${1:-all}"
CSV="${RESULTS}/results.csv"

mkdir -p "${RESULTS}"
cd "${REPO}"

log() { printf '%s  %s\n' "$(date +%H:%M:%S)" "$*"; }

# Keep this runner checkout current with the committed state (best-effort — if the box is offline
# or git auth isn't available to the background session, just run whatever is checked out).
git -C "${REPO}" pull --ff-only >/dev/null 2>&1 \
  && log "runner checkout updated to $(git -C "${REPO}" rev-parse --short HEAD)" \
  || log "git pull skipped (offline / no auth) — running current checkout"

# ── Docker runtime (Colima; headless, unlike Docker Desktop) ────────────────────────────────
# A 100-node run saturates the VM's conntrack/iptables; the documented remedy is to restart the VM
# between rounds (docs/wiki/dev/testing/scale-tests.md) — otherwise the daemon can go unreachable and
# the *next* suite fails to even start ("Cannot connect to the Docker daemon", make Error 1). So we
# bring Colima up once, then hand each SUBSEQUENT suite a freshly-restarted VM.
COLIMA_ARGS="--cpu 8 --memory 16 --disk 60"
DOCKER_UP=0
wait_docker() { local i; for i in $(seq 1 45); do docker info >/dev/null 2>&1 && return 0; sleep 2; done; return 1; }
ensure_docker() {   # arg "fresh" ⇒ restart the VM to clear between-round fatigue
  if [ "${1:-}" = "fresh" ] && [ "${DOCKER_UP}" = "1" ]; then
    log "Restarting Colima for a clean VM (clears between-round conntrack/iptables fatigue)…"
    colima restart >/dev/null 2>&1 || colima start ${COLIMA_ARGS} >/dev/null 2>&1
  elif ! docker info >/dev/null 2>&1; then
    log "Starting Colima (${COLIMA_ARGS})…"
    colima start ${COLIMA_ARGS} >/dev/null 2>&1 || true
  fi
  docker context use colima >/dev/null 2>&1 || true
  wait_docker || { log "Docker unreachable — aborting"; exit 1; }
  DOCKER_UP=1
}

# CSV header once.
[ -f "${CSV}" ] || echo "timestamp,suite,exit_code,result,note,log" > "${CSV}"

run_suite() {
  local name="$1" target="$2"
  local logf="${RESULTS}/${STAMP}-${name}.log"
  log "▶ ${name}  (make ${target}) → ${logf}"
  make "${target}" >"${logf}" 2>&1
  local rc=$?
  local result; [ "${rc}" -eq 0 ] && result=PASS || result=FAIL
  # Best-effort: pull a formation/summary hint from the log for the CSV note column.
  local note
  note="$(grep -aoiE 'formed [0-9]+/[0-9]+|[0-9]+/[0-9]+ nodes|converged|integrity|PASS|FAIL' "${logf}" | tail -1 | tr ',' ' ')"
  echo "${STAMP},${name},${rc},${result},${note:-},${logf}" >> "${CSV}"
  log "  ${name} → ${result} (rc=${rc})"
}

case "${SUITE}" in
  all)        SUITES="scale resilience entries" ;;
  scale)      SUITES="scale" ;;
  resilience) SUITES="resilience" ;;
  entries)    SUITES="entries" ;;
  *) log "unknown suite '${SUITE}' — use: all|scale|resilience|entries"; exit 2 ;;
esac

FIRST=1
for s in ${SUITES}; do
  # First suite: bring Docker up. Subsequent suites: restart the VM first (clears the 100-node
  # conntrack/iptables fatigue that otherwise leaves the daemon unreachable).
  [ "${FIRST}" = 1 ] && ensure_docker || ensure_docker fresh
  FIRST=0
  case "${s}" in
    scale)      run_suite scale      test-scale ;;
    resilience) run_suite resilience test-scale-resilience ;;
    entries)    run_suite entries    test-scale-entries ;;
  esac
done

# Always leave the box clean (the make targets clean on success; this catches a mid-run abort).
log "Tearing down any leftover clusters…"
make test-scale-clean test-scale-resilience-clean test-scale-entries-clean >/dev/null 2>&1 || true

log "Done. Results appended to ${CSV}"
