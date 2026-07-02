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
```

CI additionally gates `tsc --noEmit` (mycelium-ts), the AFN smoke (pull+push), the coop
smoke, time-boxed fuzz (skipped on PRs), and `cargo audit` (RUSTSEC). **Don't trust a
memorised test count** — the counts grow every PR; run the suites for the live total (the
CLAUDE.md count bullet drifted twice before this rule).

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
