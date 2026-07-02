# dev/ — how the substrate is built and verified

↑ [wiki root](../wiki.md) · schema: [AGENTS.md](../AGENTS.md)

System knowledge only (routing test → *no*: about our code, tests, infra). Code is canon —
pages here cite `src/` / `mycelium-core/src/` rather than paraphrasing it.

## Areas

- **[architecture/](architecture/architecture.md)** — the three layers, the crate split,
  runtime invariants that keep recurring in review.
- **[concurrency/](concurrency/concurrency.md)** — the lock-order table, lock-free (papaya)
  mutation rules, atomics ordering policy. The discipline that the calibration ledger shows
  is this codebase's recurring bug family.
- **[testing/](testing/testing.md)** — test conventions, the feature matrix, scale-test +
  Docker-bridge lore, the SWIM divergence saga.
- **[companions/](companions/companions.md)** — the companion crates built on the public API
  (tuple-space, blackboard, wasm-host, agentfacts).

## Leaf pages

- **[security.md](security.md)** — the v1.x WS1–WS5 security surface + crown-jewel posture.
- **[operations.md](operations.md)** — diagnostics endpoints, task-count reference, feature
  gates, the ops label.
- **[examples.md](examples.md)** — the coop suite, AFN pipeline, A2A community demos.
- **[history.md](history.md)** — the delivery ledger: v1.x + v2.0 workstreams, PR ranges,
  what was declined-with-evidence.
