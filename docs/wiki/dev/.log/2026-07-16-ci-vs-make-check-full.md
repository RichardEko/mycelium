# ingest — the `make check-full` ≠ CI-green gap (2026-07-16)

**Durable lesson added to** [`dev/testing/testing.md`](../testing/testing.md) (after the CI-gate block).

**What happened.** A pass-1 audit fix (core BUG 4) salted the `kv().append` on-disk key with the node
id (`log/{stream}/{hlc:016x}/{node}`) — a change to a **shared core contract**. `mycelium-reason::replay`,
a separate crate with its own `log/`-key parser, then read the HLC from the wrong segment and silently
dropped every trace event. **`make check-full` stayed green** (reason's unit tests covered only
formatting, never a `record→replay` round-trip), so the regression sat **red in CI for ~25 commits**
while each was reported "green" off the local gate — only CI's live **Reason (v3.0 Tier-3)** + **Python
SDK + LangGraph** jobs exercised it. A related intermittent flake (coop `07 · consensus` demo) turned
out to be a *correct* substrate change (stricter quorum + TLS-signed-consensus identity timing) meeting
a demo whose startup gate predated it.

**Rules ingested:**
1. `make check-full` is the Rust lib+clippy set only; CI **also** runs live-node/cross-language jobs
   (Reason, Python SDK, Food-Rescue coop suite, Blackboard, AFN, Docker cluster suites, nightly fuzz)
   that no `cargo test --lib` reaches. **After pushing, check `gh run list` — CI green is the real bar.**
2. Changing a shared contract (on-disk/wire key layout, KV-prefix convention, encode format) → **grep
   the whole workspace for other consumers**; companion crates parse these too and may have no lib test.
3. A *correct* substrate change can make a timing-sensitive **demo** flakier — fix the demo's readiness
   gate, not the substrate.

**Gates added:** `mycelium-reason` `tests/reason.rs::trace_record_replay_round_trips` (fails on the old
parser); the coop `07 · consensus` demo now waits for `sys/identity/` propagation before proposing.
