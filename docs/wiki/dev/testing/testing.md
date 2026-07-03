# dev/testing — conventions

↑ [dev/](../dev.md) · child page: [scale-tests.md](scale-tests.md)

## Run the full feature matrix before pushing

`cargo test --lib` alone misses `#[cfg(feature = …)]` code. CI's set:

```bash
cargo test --lib --features tls,metrics,a2a,llm
cargo clippy --lib --tests --features tls,metrics,a2a,llm -- -D warnings
cargo test --lib --features compliance          # WS1 RBAC + WS2 audit + WS4 OIDC + WS5 rotation
cargo test --lib --no-default-features --features gateway   # consensus-free embed
cargo clippy -p mycelium-core --lib --tests -- -D warnings  # core's own tests are a separate lint scope
cargo clippy --lib --no-default-features -- -D warnings     # minimal embed — catches feature-gated dead code
cargo clippy -p mycelium-wasm-host --all-targets -- -D warnings   # wasm-host embeds mycelium default-features=false
```

**Feature-gated dead code is a real trap** (bit the diagnostics work, 2026-07-03): an item used
only under `gateway`/`metrics` is *dead* in a `--no-default-features` build (the CI "Gateway-free"
+ "WASM host" jobs run exactly that), and `-D warnings` fails there even though the default and
feature-matrix gates pass. Run the last two lines above before pushing anything with a
feature-conditional consumer.

CI additionally gates `tsc --noEmit` (mycelium-ts), the AFN smoke (pull+push), the coop
smoke, time-boxed fuzz (skipped on PRs), and `cargo audit` (RUSTSEC). **Don't trust a
memorised test count** — the counts grow every PR; run the suites for the live total (the
CLAUDE.md count bullet drifted twice before this rule).

## Toolchain is pinned — bump it deliberately

`rust-toolchain.toml` pins `channel = "1.96.0"` (was floating `stable`), and the CI jobs pin
`dtolnay/rust-toolchain@1.96.0` (the fuzz job stays on `@nightly` by necessity). This exists
because a new stable ships new clippy lints that redden `-D warnings` on unrelated PRs the
moment a runner picks up a newer stable than a dev has locally — it bit twice in one session
(`int_plus_one`, `manual_is_multiple_of`; analysis Runs 28–29). To upgrade: bump the file
**and** the 10 CI `@1.96.0` refs together, in their own PR, after running the full
`-D warnings` matrix on the new version — never let it float again.

## Multi-node consensus tests need listeners everywhere

`system_propose`/`consistent_set` compute quorum from live peers; peers without
`start_consensus_listener` never vote and every ballot times out. A test omitting this
passes only via accidental single-node quorum. Pattern (and the peer-ready poll) in
`src/lib_tests.rs::consensus_pair`.

## Structural polling, not fixed sleeps

Assert cluster state with a predicate poll (`poll_until(|| !a.peers().is_empty(), …)`), not
`sleep(300ms)`. A fixed sleep passes by luck on fast machines and hides the race on slow
ones; the structural poll converts a timing race into a deterministic failure.

## Env-var tests serialise on a lock

`apply_env_overrides` reads **all** `GOSSIP_*` vars, so any test that mutates one races
every other env test in parallel threads. Hold `config::tests::env_test_lock()` for the
guard's lifetime (added Run 28 after exactly this race).

## Ports

Use `crate::test_util::alloc_port` (process-unique, bind-verified, confined below the OS
ephemeral floor — PR #110 retired the parallel-suite flake family). Never hardcode.
