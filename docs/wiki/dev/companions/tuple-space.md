# mycelium-tuple-space — the pull-based pipeline buffer

↑ [companions](companions.md) · sibling: [blackboard](blackboard.md)

Linda-style **generative decoupling + blocking pull**, NOT associative template matching.
Design: `docs/plans/mycelium-tuple-space.md`. Key facts:

- **Pattern:** workers `take()` when ready — readiness is self-announcing, so push-predict
  staleness/misroute cannot occur. The space removes the central *decision-maker*; the data
  path keeps its own failover. This crate is Paper 2a's pull-vs-push evidence.
- **Lanes, not matching:** named per-stage FIFO lanes; payloads opaque; `complete` is an
  atomic lane-to-lane move; per-lane depth is the pressure signal; content-style routing is
  encoded in lane names (`stage-b.high`). **Fan-in joins** are expressible via keyed
  exact-match take (M13): `put_keyed`/`take_by_key`/`complete_keyed` — O(1) keyed index +
  keyed-waiter map, WAL v2 records, gateway + py/ts SDKs. Template matching stays the
  blackboard's territory.
- **Roles** (`TupleRole`): `Primary` serves; `Secondary` mirrors (replicate RPCs +
  heartbeat) and promotes when the primary's capability evaporates — the ring *is* the
  failure detector; `Auto` elects (lowest-candidate-id tie-break); `Client` never serves.
  **"Evaporated" requires prior sight** (#158): an empty resolve before the watch has ever
  *seen* the primary's advertisement is startup propagation lag, not failure — on a
  CPU-starved host the first sighting can take many intervals, and a promoted node never
  demotes, so promoting on lag is *permanent split-brain* (takes 408 off the impostor's
  empty mirror while puts land on the real primary — the hosted-CI S13 signature, #150).
  Seen-then-empty×2 promotes (unchanged failover latency); never-seen promotes only after
  a 10-interval orphan grace (bounded availability for a primary that died first). Gates:
  `secondary_startup_lag_is_not_evaporation` (fails on pre-fix code),
  `never_seen_primary_promotes_after_orphan_grace` (both `tests/failover.rs`). The reusable
  lesson generalizes: **in a gossip-visibility failure detector, absence-at-birth is not
  failure** — require one positive observation before treating absence as evaporation.
- **Durability:** single-lock hot path (no waiter/store TOCTOU); WAL with indivisible
  `Complete` records; compaction bumps a WAL *epoch* so a secondary's byte-offset cursor
  can't dangle.
- **Naming/prefixes:** capability segments must not contain `/` → flat
  `{ns}.primary|secondary|candidate`. Owns `tuple/inflight/{ns}/{id}` +
  `sys/tuple/{node}/{ns}/…` (backpressure pheromone — deliberately NOT `sys/load/` opacity:
  hiding the primary from `resolve` under load would false-trigger promotion).
- **Gates:** `cargo test -p mycelium-tuple-space --features gateway` (+ clippy
  `--all-targets -D warnings`); integration scenario 13; `redistribution` example
  (staged `intake→sorted→routed` pipeline — single-copy `take` + atomic `complete`,
  exactly-once assertion); SDKs `mycelium-py/src/mycelium/tuple.py`,
  `mycelium-ts/src/tuple.ts`.
