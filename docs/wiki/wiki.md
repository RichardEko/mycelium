# Mycelium — wiki root

The project's LLM Wiki (schema: [AGENTS.md](AGENTS.md) — read it before editing; **code is
canon, the wiki cites it**). Start here, follow links down.

Mycelium is an embedded, broker-less Rust library: a three-layer substrate (gossip KV /
signal mesh / epidemic consensus) for AI agent fleets and storage replication, built on the
thesis that a coordinator is not just slower but *epistemically incapable* for heterogeneous
fleets. Purpose anchor: `docs/philosophy.html`. Version state: **v2.0.0 released** (tag `v2.0.0`) —
all 16 milestones (2026-06-21) + the v1.x production-readiness workstreams, now on a unified
2.0.0 workspace version train; wire v12.

## Sections

- **[dev/](dev/dev.md)** — how the substrate is built and verified: [architecture
  invariants](dev/architecture/architecture.md), [concurrency
  discipline](dev/concurrency/concurrency.md), [testing & scale
  lore](dev/testing/testing.md), [security workstreams](dev/security.md), [companion
  crates](dev/companions/companions.md), [operational surface](dev/operations.md),
  [example suites](dev/examples.md), [delivery history](dev/history.md).
- **[domain/](domain/domain.md)** — the coordinator-free thesis and its world:
  [theory](domain/theory/theory.md) (Coordinator Trap, scale-invariant boundaries,
  management-as-intent), [publications corpus](domain/publications.md) (4 papers, all
  published), [commercial strategy](domain/strategy/strategy.md).

## The other knowledge stores (link, don't fork)

| Store | Role |
|---|---|
| `src/lib.rs`, `mycelium-core/src/{framing,hlc}.rs`, `src/capability.rs` | Code canon (API, wire, HLC, capability model) |
| `docs/README.md` | Map of the seven docs areas + root anchors |
| `docs/plans/README.md` | Execution-record index (all engineering plans shipped as of 2026-06-21) |
| `docs/publications/README.md` | Paper corpus index (read order, DOIs, dependency graph) |
| `docs/analysis/ratings.md` | The M2 self-audit series + calibration ledger |
| `docs/analysis/doc-coverage-2026-07-10.md` | Documentation-coverage audit (WHAT/WHY/HOW × Dev/Ops matrix + remediation; a re-run diff target) |
| `CLAUDE.md` | Session on-ramp: build/test gates + hot invariants + pointers here |
