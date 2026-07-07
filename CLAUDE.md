# CLAUDE.md — Mycelium session on-ramp

Fast on-ramp for code-assistant sessions. This file is deliberately thin: it holds the
**workflow rules, the build/test gates, and the hottest invariants** — everything else
lives in the **LLM wiki** and the code canon it cites.

## What this is

Mycelium is an embedded, broker-less Rust library — a three-layer substrate for AI agent
fleets and storage replication: **I** gossip KV (LWW + HLC, Merkle anti-entropy) ·
**II** signal mesh (scoped events, admission boundaries, opacity) · **III** epidemic
consensus. Layers I+II are the `mycelium-core` crate; `mycelium` adds III, capabilities,
services, gateway, tls. It is a **library, not a platform** — no daemon, no control plane;
a cluster is emergent from network reachability. v2.0 complete (all 16 milestones,
2026-06-21); wire **v12** (`PREV = 11`).

## The wiki workflow (non-negotiable)

1. **Query first.** Start any task by reading [`docs/wiki/wiki.md`](docs/wiki/wiki.md) →
   the relevant section/pages. Don't re-derive what the wiki already states.
2. **Ingest on completion.** When finished work produces durable knowledge (new invariant,
   root-caused bug family, shipped workstream, revised position): update the page(s),
   refresh folder-notes, add one dated file to the section's `.log/`.
3. **Schema:** [`docs/wiki/AGENTS.md`](docs/wiki/AGENTS.md). **Code is canon** — the wiki
   cites `src/` rather than paraphrasing it; on conflict trust the code, then fix the page.
4. **Lint periodically** via the `/wiki-lint` skill — doc-vs-code verification first (the
   check that catches drifted claims like the Run-28 lock-table finding).

Private memory (`~/.claude/.../memory/`) holds user preferences and session state only —
promote durable project knowledge to the wiki.

## Where to read what

| For | Read |
|---|---|
| Public API + KV-namespace ownership | `src/lib.rs` crate doc |
| Wire format + version policy | `mycelium-core/src/framing.rs` (top) |
| HLC design + limits | `mycelium-core/src/hlc.rs` module doc |
| Capability model | `src/capability.rs` |
| Purpose / roadmap | `docs/philosophy.html` · `ROADMAP.md` |
| Docs map (guide, operations, design, plans, publications, analysis) | `docs/README.md` |
| Architecture, concurrency, testing lore, security, companions, history | the wiki: [`docs/wiki/`](docs/wiki/wiki.md) |

## Build & test gates (run before pushing)

**`make check`** is the one-command pre-push gate — clippy across the feature matrix CI enforces
(feature-matrix + `--no-default-features` + core), ~3 min, no wasmtime. The `--no-default-features`
clippy is the catcher for the *feature-gated dead-code trap* (an item live only under
`gateway`/`metrics` is dead in a minimal build — CI's Gateway-free + WASM-host jobs). `make
check-full` adds the test suites + wasm-host clippy. The underlying set:

```bash
cargo test --lib --features tls,metrics,a2a,llm
cargo clippy --lib --tests --features tls,metrics,a2a,llm -- -D warnings
cargo test --lib --features compliance
cargo test --lib --no-default-features --features gateway
cargo clippy -p mycelium-core --lib --tests -- -D warnings
cargo clippy --lib --no-default-features -- -D warnings   # catches the feature-gated dead-code trap
```

Companion crates: `cargo test -p mycelium-tuple-space --features gateway`, same for
`mycelium-blackboard` (+ clippy `--all-targets`). CI also gates `tsc --noEmit`, the AFN +
coop smokes, fuzz (non-PR), and `cargo audit`. **Never trust a memorised test count** — run
the suite. Scale suites: `make test-scale` (100 nodes), `test-scale-resilience`,
`test-scale-entries` — read the wiki's
[scale-tests page](docs/wiki/dev/testing/scale-tests.md) before interpreting failures
(Docker-bridge iptables ceiling, VM fatigue).

## Hot invariants (the ones that ship regressions when forgotten)

- **One lock per function**, flat acquisitions only — the [lock-order
  table](docs/wiki/dev/concurrency/lock-order.md) claims completeness: adding any
  `Mutex`/`RwLock` field means adding a row.
- **papaya:** `compute` closures retry-safe; never act on a stale read — the whole
  recurring race family, rules + reference impls in
  [lock-free-and-atomics](docs/wiki/dev/concurrency/lock-free-and-atomics.md).
- **Individual-scope forwarding is unconditional** (flood fallback); only *admission* is
  scoped. Do not "optimize" it away —
  [runtime-invariants](docs/wiki/dev/architecture/runtime-invariants.md).
- **Detection, not prevention:** never teach Layer I a higher-layer law (no prefix write
  guards in `apply_and_notify`) — tripwires + counters instead.
- **Consensus listeners register synchronously**; multi-node consensus tests need a
  listener on every node + a structural peer-ready poll (never fixed sleeps) —
  [testing](docs/wiki/dev/testing/testing.md).
- **KV writes are size-gated** (`framing::MAX_KV_WRITE_BYTES`); anti-entropy is chunked;
  a `FrameTooLarge` frame is dropped without tearing down the connection.
- Ports via `test_util::alloc_port`; env-var tests hold `config::tests::env_test_lock()`.

## Active work

All engineering plans shipped as of 2026-06-21 (`docs/plans/README.md`); **Legible Emergence**
completed 2026-07-03 (phases 0–5 — `docs/plans/legible-emergence.md`). Open: the
**artifact library** (steps 1–5 shipped 2026-07-07 — durable library + librarian + kind/runtime
generalization + honest demos; remaining: object-store source + async-trait revisit,
`docs/design/artifact-library.md` §10).
Research-track: the three-arm work-distribution experiment (Paper 1) and the monetary-
ecology article revision ([wiki](docs/wiki/domain/publications.md)). Delivery ledger:
[dev/history](docs/wiki/dev/history.md). Self-audit series: `docs/analysis/ratings.md`
(run via `/mycelium-analysis`).
