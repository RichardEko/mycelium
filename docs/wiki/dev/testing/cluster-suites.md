# Docker cluster suites — the CI-gated cross-node tests

↑ [testing](testing.md) · sibling: [scale-tests](scale-tests.md)

The two multi-container suites cover what in-process tests structurally cannot: real TCP,
anti-entropy over a Docker bridge, container restarts, late joiners, and cross-node RPC under
genuine network latency. **Both are CI-gated since 2026-07-10** (`.github/workflows/
cluster-suites.yml`) — wiring them into CI is what surfaced five substrate/companion defects
in one week (#149 exact-once, #150 flood-relay + spurious-promotion split-brain, #159
succession data loss, #160 restart AddrInUse panic). None were reproducible on fast local
hardware alone.

## The suites

| Suite | Command | Shape | Covers |
|---|---|---|---|
| Integration | `make test` | 4 nodes (a, b, late-joiner c, mgmt) + runner | 13 scenarios: KV convergence/persistence, restarts, signals, capabilities, scatter-gather, AFN, prompt skills, tuple space (`tests/integration/scenarios/`) |
| Overlay | `make test-overlay` | 3 nodes + runner | S11 task auction (exact-once), S12 leader election + consensus config, S13 shared reasoning log (`tests/overlay/`) |

Note the trap that cost a day: **overlay "S13" (shared log) and integration scenario 13
(tuple space) are different tests.** Execution evidence must name the suite that tests the
claim (calibration ledger, 2026-07-10).

## The CI gate (`cluster-suites.yml`)

- Runs on substrate PRs (path-filtered: `src/`, the crates, harnesses, `docker/`, the demo),
  merges to main (same filter — docs-only pushes skip), nightly 05:00 UTC, and
  `workflow_dispatch`.
- **No retries, by design.** A red gate is signal; both historic flakes were real bugs fixed
  at the substrate layer, not papered over (#155/#158). Do not add retry loops to make it
  green — diagnose (the harness self-diagnoses, below).
- The 100-node **scale** suites run separately: `.github/workflows/scale-nightly.yml`,
  nightly 06:00 UTC on a **self-hosted** runner labelled `mycelium-scale` (hosted 2-core
  runners hit the Docker-bridge iptables ceiling ~50 nodes — [scale-tests](scale-tests.md)).

## Harness diagnostics (a red gate names its own cause)

Added 2026-07-10 after a bare "FAIL" with empty stderr cost several diagnosis rounds:

- **ERR trap + errtrace** (`tests/integration/lib/helpers.sh`): every scenario runs
  `set -euo pipefail` with `curl -sf`, which dies silently — the trap prints
  `script:line: command exited N` for any unguarded failure (`if`/`&&`/`||`-guarded helper
  internals stay silent). `set -o errtrace` is load-bearing: without it the trap is NOT
  inherited by shell *functions*, so a curl inside a helper (e.g. `gw_kv_set`) still died
  silently — exactly the undiagnosable AFN failure (#161). Any new harness lib must set both.
- **Node-log dump**: `make test` dumps the last 200 log lines per node when the runner fails
  — this is what named the spurious-promotion split-brain and the AddrInUse panic directly.
- **Phase-0 data-plane barrier** (`tests/integration/run.sh`): health + mgmt visibility prove
  the control plane only; the barrier proves one KV round-trip in each direction (bounded
  60 s, warn-not-block) before scenarios run, so scenario windows measure convergence, not
  bring-up lag.
- **S13 take-loop instrumentation** (`13_tuple_space.sh`): each take reports iteration + HTTP
  code + body on failure (408 vs 5xx tell different stories).

## The operating lesson

A CPU-starved 2-core runner is a *feature*: it stretches timing windows (cap propagation,
TIME_WAIT, connection warmup) into ranges fast local hardware never exhibits. Every "hosted
CI is flaky" episode this suite has produced so far decomposed into a real defect. Diagnose
from captured node logs, never from code-reading — four wrong hypotheses in the #150 arc were
each killed by data (full narrative: `.log/2026-07-09-connect-peer-s13.md`,
`.log/2026-07-10-spurious-promotion-s13.md` in [dev](../dev.md)'s log).
