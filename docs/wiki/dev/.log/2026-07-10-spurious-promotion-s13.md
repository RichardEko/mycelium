# 2026-07-10 — S13's second root cause: spurious promotion split-brain (#158); yesterday's entry corrected

Yesterday's entry (`2026-07-09-connect-peer-s13.md`) recorded S13 as fixed by `connect_peer`
with a 5/5 verification. **That verification ran the wrong suite** — `make test-overlay`'s
"S13" (consensus shared-log), not integration scenario 13 (`make test`, the tuple-space pull
pipeline #150 is actually about). The hosted cluster-suites gate (PR #156) caught this: three
hosted runs failed integration S13 identically while overlay passed and local passed 3/3.

- **Second root cause (ground truth from the instrumented hosted run's node logs):** node-b
  logged "primary evaporated — promoting" **2 s after becoming secondary** while node-a served
  the whole time. The promotion watch treated *never-saw-a-primary* as *evaporated*; on a
  CPU-starved 2-core runner the cap advertisement outruns 2×cap_refresh, and a promoted node
  never demotes → **permanent split-brain**: takes 408 off the impostor's empty mirror, puts
  land on the real primary (the exact curl-exit-22 signature). Fast local hardware propagates
  in ms — unreproducible locally by construction.
- **Fix (#158, merged):** "evaporated" requires prior sight; never-seen promotes only after a
  10-interval orphan grace. Canary verified: `secondary_startup_lag_is_not_evaporation` fails
  on pre-fix code. Companion page updated with the generalized lesson: **in a gossip-visibility
  failure detector, absence-at-birth is not failure.**
- **Harness hardening that produced the ground truth** (all on the gate branch, PR #156):
  ERR trap in `helpers.sh` (a red scenario names its dying line), take-loop HTTP-code
  instrumentation in `13_tuple_space.sh`, node-log dump on runner failure (`make test`), and a
  Phase-0 data-plane readiness barrier in `run.sh` (scenario 01 had failed on bring-up lag).
- **Result:** first fully green hosted run (Integration 13/13 + Overlay 3/3) on the build with
  both fixes; stability reruns in progress before #156 merges.
- So #150 was **two stacked defects**: flood-relay RPC latency (#155) *and* promotion
  split-brain (#158). Methodology note for the record: the wrong-suite verification is exactly
  the class of error the execution-evidence rule exists for — the evidence must name the suite
  *that tests the claim*, not a suite with a similar name.

Pages touched: `dev/companions/tuple-space.md` (promotion semantics + lesson),
`dev/architecture/runtime-invariants.md` (regression-gate correction — earlier note cited the
wrong suite).
