# dev/testing — conventions

↑ [dev/](../dev.md) · child pages: [scale-tests.md](scale-tests.md) ·
[cluster-suites.md](cluster-suites.md)

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
cargo test -p mycelium-core                                 # the substrate suite (codec/framing/hlc/store/swim) — RUNS as of 2026-07-11
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

**mycelium-core's suite runs in CI as of 2026-07-11.** Before that it was clippy-*compiled*
(`clippy -p mycelium-core --lib --tests`) but never *run*: `cargo test --lib` tests only the root
`mycelium` package (core is a compiled dependency there, its `#[cfg(test)]` invisible), and there was
no `-p mycelium-core` test job — every *companion* crate had one, core didn't. So the whole substrate
suite (codec/framing/hlc/store/swim, 131 tests), including the wire back-compat tests, was unenforced.
Same class of gap as the decoder mini-fuzz that sat uncaught until M2 Run-20. Now in the CI `Test`
job + `make check-full`.

**Wire back-compat gate.** `codec::tests::decode_wire_v11_agrees_with_v12_on_every_shared_variant`
proves the current decoder reads a **PREV-version (v11)** frame for every shared `WireMessage` variant
— the rolling-upgrade contract (`StateRequest`'s deliberate Merkle-digest change is covered separately
by `decode_wire_v11_downgrades_state_request`). **Corpus discipline** on a `WIRE_VERSION` bump:
regenerate `GOLDENS`, freeze the *outgoing* version's bytes as `V{N}_*` fixtures, add a
`decode_wire_v{N}`, and extend the gate so new code still decodes vN frames. A live two-binary
mixed-version *cluster* test remains a documented (unbuilt) nightly-tier follow-up.

## Coop demos: wasm is opt-in (fast non-wasm builds)

`examples/coop` gates `mycelium-wasm-host` (→ wasmtime/cranelift) behind a `wasm` feature. Four
bins need it (`required-features = ["wasm"]`): `provisioning`, `catalog`, `mcp_toolgrowth`, and
the manual `model_deploy`; the other demos — e.g. `cargo run --bin diagnostics` — build
**without** compiling wasmtime. `ci_smoke.sh` enables `--features wasm` for the three CI demos
that need it; a dev iterating on any non-wasm demo skips the heavy build entirely.

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

`cluster_propose`/`consistent_set` compute quorum from live peers; peers without
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

## The CI flake tier (structural, Run-38 floor fix)

Socket-binding / multi-node suites run in CI through `scripts/ci-retest.sh`, not bare
`cargo test`: on failure the wrapper re-runs **only the failed tests, individually, once**. A
test that fails twice is a real failure and reds the build; a test that passes on isolated
retry keeps the build green **but emits a loud per-test flake annotation + step-summary line**.
The policy that makes this safe against the Run-37 masking failure mode: **a flake annotation
is a bug report** — recurring annotations get a root-cause dig (the wiki port race and the
opacity shed bug were both found that way), and "fixing" a flake by widening a timeout is
forbidden. Deterministic unit gates stay on bare `cargo test`. This is the class-level
prevention Run 37 asked for: a wall-clock flake can no longer red main *or* hide silently.

Companion integration tests now reach the same allocator: `alloc_port` is exposed under the
core's `test-util` cargo feature (Run-39 floor fix), and every companion with a real-agent
`tests/` suite pulls `mycelium = { path = "..", features = ["test-util"] }` as a **dev-dependency**
and calls `mycelium::test_util::alloc_port()` in place of its old `free_port()`. That is the
class-level prevention — the bind-`:0`-read-drop idiom is gone from the `tests/` surface, so no
companion re-opens the TOCTOU window (only the `examples/` `free_port` remain, a follow-up).

The bind retry stays as defense-in-depth for the residual case — an agent under test binds a port
`alloc_port` returned but a foreign process grabbed it first: **retry the bind, never bare-`unwrap`
it**. The old bind-`:0`-read-drop idiom (`free_port()`) opened a TOCTOU window against parallel
test binaries (`AddrInUse` flaked `mycelium-wiki/tests/failover.rs` in CI, 2026-07-07). Retry at
the granularity the topology
forces: per-agent with fresh ports when nodes join one at a time (the wasm-host tests'
16-attempt loop), **per-pair** when mutual bootstrap fixes both ports before either agent
starts (`start_pair()` in the wiki tests — shut the half-started survivor down before
re-attempting, and shut a discarded `Wiki` down explicitly, the Run-32 task-leak lesson).
