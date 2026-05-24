Run the full Mycelium test suite across all three levels and report a structured summary.

## Levels

**Level 1 — Unit tests** `cargo test --lib`
Fast. Runs the 200+ in-process tests. Should finish in under 10 s.

**Level 2 — Lint** `cargo clippy --lib --tests -- -D warnings`
Fast. Treat any new warning as a failure. The baseline is 61 pre-existing
`field_reassign_with_default` warnings in test code; if the count is ≤ 61 and
no new error-level diagnostics appear, this level passes.

**Level 3 — Integration** `make test`
Slow (~5 min if Docker build cache is warm, longer on first run).
Builds Docker containers, starts a 5-node cluster, runs 10 unattended scenarios.
The Docker build layer cache is keyed on source files, so re-runs after a small
code change are fast.

## Execution order

Run Level 1 first, then Level 2, then Level 3. Run all three regardless of
intermediate failures so the full picture is available — unless $ARGUMENTS
contains `--fast`, in which case stop at the first failure.

For Level 3, stream `docker compose logs -f runner` output so progress is visible
while the cluster runs. The Makefile target handles this automatically.

## Reporting

After all levels complete, print a results table and an overall verdict:

```
╔══════════════════════════════════════════════════════╗
║              Mycelium Test Suite Results             ║
╠═══╦════════════════════╦══════════╦══════════════════╣
║ # ║ Level              ║ Status   ║ Detail           ║
╠═══╬════════════════════╬══════════╬══════════════════╣
║ 1 ║ Unit tests         ║ PASS/FAIL║ N passed, N fail ║
║ 2 ║ Lint (clippy)      ║ PASS/FAIL║ N warnings       ║
║ 3 ║ Integration        ║ PASS/FAIL║ N/10 scenarios   ║
╚═══╩════════════════════╩══════════╩══════════════════╝
Overall: PASS / FAIL
```

For any failing level include the specific failures:
- Unit tests: list the failing test names
- Clippy: list each diagnostic (file:line, message)
- Integration: list each failing scenario name and its error line from stderr

If a level was skipped due to `--fast`, show `SKIP` in the Status column.

$ARGUMENTS
