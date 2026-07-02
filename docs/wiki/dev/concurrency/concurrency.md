# dev/concurrency — the discipline

↑ [dev/](../dev.md)

The calibration ledger (`docs/analysis/ratings.md`) shows concurrency is this codebase's
recurring bug family — four ledger entries and counting, all reducible to one shape:
**a lock-free operation followed by an unserialised derived effect**. Pages:

- **[lock-order.md](lock-order.md)** — every `Mutex`/`RwLock` site, the flat-acquisition
  invariant, and the keep-it-honest rule.
- **[lock-free-and-atomics.md](lock-free-and-atomics.md)** — the papaya mutation rules and
  the memory-ordering policy for atomics.

The `AgentStateMachine` commit discipline (validate-and-swap with budget reserve under the
state lock — Run 28 Finding 2) is documented at `src/agent/state_machine.rs::try_commit`.
