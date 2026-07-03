## [2026-07-03] ingest | developer-experience fixes (analysis Run 30 DX finding)

Run 30 (M2) scored **Developer Experience 7** (a floor dimension) on two concrete pains this
session. Both addressed.

**Pain 1 — the feature-gated dead-code trap turned three commits CI-red.** Root cause: no
one-command local gate mirroring CI, so it was easy to skip the `--no-default-features` clippy that
catches the trap. Fix: **`make check`** (Makefile) runs clippy across the feature matrix CI enforces
(feature-matrix + `--no-default-features` + core) in ~3 min with no wasmtime — the pre-push gate.
The insight baked into it: the fast `cargo clippy --lib --no-default-features` lints the *same*
gateway/metrics-off mycelium lib that CI's slow "Gateway-free" + "WASM host" jobs compile, so it is
the trap-catcher and you rarely need the wasmtime build. `make check-full` adds the test suites +
wasm-host clippy. CLAUDE.md + dev/testing/testing.md now point at `make check` as the pre-push gate.

**Pain 2 — the coop crate pulled wasmtime into *every* binary** (a pathological cold
`--bin diagnostics` build). Root cause: `mycelium-wasm-host` (→ wasmtime/cranelift) was an
*unconditional* dep of `examples/coop`, and Cargo compiles it for any bin. Fix: made it **optional
behind a `wasm` feature**; only `provisioning` + `catalog` use it, so they carry
`required-features = ["wasm"]`. Verified with `cargo tree`: mycelium (llm,tls) does **not** pull
wasmtime (so gating is not moot), coop *without* `wasm` has **no wasmtime in its graph**, and *with*
`wasm` it appears via wasm-host. Net: `cargo run --bin diagnostics` (and the other nine non-wasm
demos) now build without compiling wasmtime — the heavy cold build is gone for local iteration.
`ci_smoke.sh` enables `--features wasm` for the two gated demos; the CI pre-build warms everything
with `--features wasm`; the coop-smoke job renamed "11 demos" → "12 demos" (the diagnostics demo).

Pages: CLAUDE.md, dev/testing/testing.md (the `make check` gate + the coop-wasm note). No code
behavior change — pure build/CI/tooling ergonomics.
