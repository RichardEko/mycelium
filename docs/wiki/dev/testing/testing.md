# dev/testing — conventions

↑ [dev/](../dev.md) · child page: [scale-tests.md](scale-tests.md)

## Run the full feature matrix before pushing

**`make check` is the one-command pre-push gate** — clippy across the feature matrix CI enforces
(feature-matrix + `--no-default-features` + core), in ~3 min with no wasmtime compile. Run it before
every push. `make check-full` adds the test suites + the (slow) wasm-host clippy; run it before a
release or when you have touched wasm-host / a feature-conditional path.

`make check` expands to CI's clippy set; the full CI gate (for reference) is:

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
feature-matrix gates pass. **The fast catcher is `cargo clippy --lib --no-default-features`** (in
`make check`) — it lints the same gateway/metrics-off mycelium lib the slow wasm-host job compiles,
so you rarely need the wasmtime build to catch the trap.

## Coop demos: wasm is opt-in (fast non-wasm builds)

`examples/coop` gates `mycelium-wasm-host` (→ wasmtime/cranelift) behind a `wasm` feature. Only
`provisioning` + `catalog` need it (`required-features = ["wasm"]`); the other ten demos — e.g.
`cargo run --bin diagnostics` — build **without** compiling wasmtime. `ci_smoke.sh` enables
`--features wasm` for the two that need it; a dev iterating on any non-wasm demo skips the heavy
build entirely.

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

Companion-crate integration tests that can't reach `test_util` and bind real agents must
**retry the bind, never bare-`unwrap` it**: the bind-`:0`-read-drop idiom (`free_port()`)
opens a TOCTOU window against parallel test binaries (`AddrInUse` flaked
`mycelium-wiki/tests/failover.rs` in CI, 2026-07-07). Retry at the granularity the topology
forces: per-agent with fresh ports when nodes join one at a time (the wasm-host tests'
16-attempt loop), **per-pair** when mutual bootstrap fixes both ports before either agent
starts (`start_pair()` in the wiki tests — shut the half-started survivor down before
re-attempting, and shut a discarded `Wiki` down explicitly, the Run-32 task-leak lesson).
