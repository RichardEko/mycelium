# dev/companions — crates built on the public API

↑ [dev/](../dev.md)

Each companion depends on `mycelium` **only through its public API** — the composability
proof. Workspace members; scope builds with `-p` (a workspace-wide build pulls `wasmtime`
via wasm-host).

- **[tuple-space.md](tuple-space.md)** — `mycelium-tuple-space/`: pull-based pipeline buffer
  (Linda-style lanes). The load-bearing artifact for Paper 2a's pull-vs-push argument.
- **[blackboard.md](blackboard.md)** — `mycelium-blackboard/`: content-routed shared working
  memory (`claim(predicate)`).
- **`mycelium-wasm-host/`** — WS-E code mobility: the coordinator-free
  requirement→resolve→pull→advertise→serve→self-heal loop, Ed25519 provenance, mesh artifact
  pull, gossiped catalog, fuel limits (restart ≡ provisioning). PRs #32–#42; runbook
  `docs/operations/artifacts.md`. Security note: wasmtime is this crate's sandbox — keep
  `cargo audit` green on it (RUSTSEC-2026-0188 was found+fixed via audit, Run 28).
- **`mycelium-agentfacts/`** — WS-F/M16 federation edge: self-certified NANDA AgentFacts
  document (superset of the A2A AgentCard), CRDT-assembled domain endpoint, schema
  migrations. PRs #44–#49, #83–#88. Domain positioning:
  [coordinator-free-recursion](../../domain/theory/coordinator-free-recursion.md).

Both tuple-space and blackboard implement the **exactly-once-effect contract** — the shared
artifact is the *contract*, not code (`docs/design/exactly-once-effect.md`; a shared overlay
was examined and declined-with-evidence).
