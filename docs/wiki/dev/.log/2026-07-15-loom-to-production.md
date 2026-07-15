# 2026-07-15 — loom spike → production (two more models + CI job + docs)

Took the `loom-spike/` concurrency-model-checker spike to production. It was one model
(`once_guard.rs`, the exactly-once `AtomicBool` init guard); now three, each a faithful,
tokio-free loom model citing the real code it mirrors, plus a CI job and wiki docs.

**Two new models** (`loom-spike/tests/`, each `#![cfg(loom)]`):
- `unique_id.rs` — the monotonic `fetch_add` unique-ID allocator (`next_pred_watcher_id`,
  `mycelium-core/src/ops.rs:237` + `kv_handle.rs:183`). CORRECT `fetch_add` hands 3 threads
  distinct ids (set.len() == N) and PASSES; the `#[ignore]`d broken `load`-then-`store` twin
  COLLIDES (two threads get the same id) and FAILS under loom.
- `publish.rs` — publish-then-observe release/acquire (`soft_state_advertised.store(true,
  Release)` at `kv_persist.rs:57`, paired with the `Acquire` load in `is_ready()`,
  `introspect.rs:125`). CORRECT Release/Acquire PASSES. **Honest outcome recorded in the
  file:** loom's weak-memory model DOES surface the all-`Relaxed` variant — the `#[ignore]`d
  twin FAILS with "reader saw flag=ready with stale data (0)". So the broken twin is real
  bug-catch proof, not a forced failure.

Both broken twins are `#[ignore]`d (matching `once_guard`), so the default loom run stays green
while each race stays one `-- --ignored` command from a printed failing schedule.

**CI:** new `loom:` job in `.github/workflows/ci.yml` (ubuntu-latest, `@1.96.0`, rust-cache),
runs `RUSTFLAGS="--cfg loom" cargo test -p loom-spike --release` with `LOOM_MAX_PREEMPTIONS: 3`.
Runs CORRECT tests only ⇒ green. YAML validated (no tabs; job parses).

**Docs:** new "Loom: permutation model-checking of the atomic patterns" subsection in
`docs/wiki/dev/testing/testing.md` (why a sibling crate, the three patterns, the run command,
the `#[ignore]`d-twins-as-proof convention). Updated the pointer comment in
`mycelium-core/Cargo.toml` (§loom spike) from one model to all three.

Invariant confirmed: without `--cfg loom` the crate is empty/zero-dep, so `cargo build -p
loom-spike`, `cargo build -p mycelium-core`, and normal `cargo test` are all unaffected. This is
the calibration ledger's recurring "act on a stale read" bug family, now model-checked in CI.
