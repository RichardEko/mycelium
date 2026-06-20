# Delivery plan — WS-G · Coordination patterns

**Status:** proposed (2026-06-20). Executes the v2.0 plan's WS-G *"richer data rendezvous on the
existing substrate."* Two items: **M13** (keyed-exact-match `take` — fan-in joins) and the
**`mycelium-blackboard`** companion crate, plus the WS-G "done-when" overlay (extract the
exactly-once-effect dedup discipline).

**Done when** (from v2.0 §WS-G): fan-in pipelines are expressible without one-lane-per-key
degeneration, and the exactly-once-effect dedup discipline is extracted as a reusable overlay
shared by the tuple space, mailbox, supervision (M14), and the blackboard.

**House-style constraint:** M13 is **exact-match only** (an O(1) hash lookup, lane depth/backpressure
intact) — *not* template matching, which is the blackboard companion's territory. The blackboard is
built **entirely on the public API** (the same composability proof as `mycelium-tuple-space` /
`mycelium-wasm-host` / `mycelium-agentfacts`).

---

## G1 — M13 · keyed-exact-match `take` (fan-in joins) ✅ COMPLETE

**Shipped** (G1a #90 in-memory rendezvous + public API/RPC, G1b #91 WAL v2 + replication/promotion
durability, G1c gateway + py/ts SDKs + cross-node join integration). Fan-in joins are now expressible
without one-lane-per-key degeneration; exact-match only.

`put` gains an optional **correlation key**; `take_by_key(stage, key)` claims the item on `stage`
whose key matches, parking a *keyed waiter* when absent — the two-stream rendezvous ("an invoice AND
its matching purchase order") that exact lane names cannot express without degenerating to one lane
per key. (ROADMAP M13; the WAL is the real work.)

### G1a · In-memory keyed rendezvous (the core primitive)

- Per-`StageState`: a keyed index (`key → item_id`) alongside the FIFO, and a **keyed-waiter map**
  (`key → oneshot`). `put` with a key registers it and wakes a parked keyed waiter; `take_by_key`
  does an O(1) lookup, claiming the item (into in-flight) or parking a keyed waiter on miss.
- Per-lane depth / waiters / in-flight accounting stays intact (keyed waiters count too).
- Public API: `TupleSpace::put_keyed(stage, key, payload)` and `take_by_key(stage, key, timeout)`;
  `complete` gains a keyed variant for the next stage.
- **Gate G-G1a:** a keyed two-stream join — `take_by_key("po", id)` parks until `put_keyed("po", id, …)`
  arrives, then rendezvouses; an unkeyed FIFO `take` on the same lane is unaffected; depth counters
  are correct.

### G1b · Durability — WAL v2 + replication + promotion

- Optional `key` field on the `Put` WAL record ⇒ **WAL format v2**, with **v1 replay accepted**
  (the documented `mirror_payload_limit` / epoch-cursor invariants hold). `complete`'s keyed next
  stage is one indivisible record (no half-replay).
- The key is carried on **secondary replication** and through **promotion replay**, so a keyed
  in-flight item survives a primary crash and re-queues under its key.
- **Gate G-G1b:** kill the primary with a keyed in-flight item live → a standby promotes and the
  item re-queues under its key (a keyed `take_by_key` after promotion still rendezvouses);
  acknowledged keyed items do not resurrect; v1 WALs replay.

### G1c · Edge — gateway endpoint + SDKs

- `POST /gateway/tuple/{ns}/put` gains an optional key; `/gateway/tuple/{ns}/take_by_key`; py/ts SDK
  methods mirroring `put_keyed` / `take_by_key`.
- **Gate G-G1c:** the join drives across nodes through the HTTP gateway (extend integration
  scenario 13's shape).

---

## G2 — Exactly-once-effect dedup overlay (the WS-G "done-when") ✅ DONE (documented contract)

**Resolved as a documented shared contract** ([`docs/design/exactly-once-effect.md`](../design/exactly-once-effect.md)),
code-extraction **deferred-with-rationale** per the sequencing note below. The Rule-of-Three check
during G1 was decisive: only **one** real implementation of this mechanism exists today (the
tuple-space store); the **mailbox uses a genuinely different mechanism** (KV + HLC keys +
read-side tombstoning, no in-flight claim/WAL/requeue); supervision (M14) and the blackboard (G3)
are not built. Extracting an `ExactlyOnce` overlay across one real user + a different-mechanism user
+ two non-existent users would be speculative abstraction. The contract names the invariants + the
canonical reference (`mycelium-tuple-space/src/store.rs`); **G3's claim-by-predicate is the second
real user that will drive the extraction.**

The tuple space, mailbox, supervision (M14), and the blackboard each implement an
"effect-happens-once" discipline (in-flight claim + ack + crash-requeue). Extract the **shared
overlay** so it is one tested mechanism, not four re-implementations.

- A small `ExactlyOnce` overlay (claim-by-id, ack, in-flight timeout + requeue, WAL-record shape)
  the tuple-space store already embodies — lift its invariants into a reusable unit and have the
  mailbox/supervision reference it (or document the shared contract if a full extraction is
  premature — Rule of Three).
- **Gate G-G2:** the extracted overlay's claim/ack/requeue invariants are unit-tested independently,
  and the tuple-space + mailbox use it (or are documented as the same contract).

> Sequencing note: G2 may be **light** (a documented shared contract + a small extracted helper)
> rather than a heavy refactor, if the Rule of Three says the three+ users don't yet justify a full
> abstraction. Decide during G1, per the same posture as the elastic `IntentReconciler` extraction.

---

## G3 — `mycelium-blackboard` companion crate

Promote the existing design **sketch** ([`mycelium-blackboard.md`](mycelium-blackboard.md)) to a
built crate: opportunistic shared-working-memory + **competitive destructive claim-by-predicate**
(the one new primitive), reusing the tuple space's WAL/in-flight exactly-once discipline (G2) and
M13's keyed claim (G1) for fan-in. Built entirely on the public API.

- Facts as KV writes; trigger predicates as signal boundaries; the new primitive is
  *claim-by-predicate* with exactly-once discipline (contract-net / blackboard, **not** fan-in joins
  — those are G1's keyed `take`).
- This is a **large** increment (a whole companion crate); **its phased build plan is now written**:
  [`v2-wsg-g3-blackboard.md`](v2-wsg-g3-blackboard.md) — Phase 1 core claim-by-predicate → Phase 2
  WAL durability (built against the G2 contract) → Phase 3 roles/failover → Phase 4 gateway+SDK →
  Phase 5 worked example + integration scenario 14 → Phase 6 extract the exactly-once overlay (G3 is
  the second real user, closing G2's deferred half).
- **Gate G-G3:** a worked example (the community-microgrid fact pool from the sketch) drives
  claim-by-predicate end to end with exactly-once effect; integration scenario added.

---

## Sequencing & PRs

1. **G1a** — in-memory keyed rendezvous (the foundational primitive).
2. **G1b** — WAL v2 + replication + promotion (the durability the keyed primitive needs).
3. **G1c** — gateway + SDK + integration.
4. **G2** — dedup overlay (light extraction or documented contract; decided during G1).
5. **G3** — the `mycelium-blackboard` crate (its own phased sub-plan).

Each is its own PR. M13 (G1) is the high-value, self-contained start — it closes the Paper 1 §9.4
fan-in contract and is what the blackboard's keyed claim reuses. The blackboard (G3) is the largest
piece and is sequenced last.
