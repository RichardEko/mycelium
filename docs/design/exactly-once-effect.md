# The exactly-once-effect discipline — shared contract (WS-G / G2 + G3·P6)

**Status:** ✅ **resolved** (G2 documented the contract 2026-06-20; G3 Phase 6 made the code-extraction
decision 2026-06-21 with the second user in hand → **declined-with-evidence**, see below). This is
the v2.0 §WS-G "done-when" item: *"the exactly-once-effect dedup discipline is extracted as a reusable
overlay shared by the tuple space, mailbox, supervision (M14), and the blackboard."* The resolution:
the shared artifact is **this contract**; a code overlay is declined because the two real
implementations diverge on a load-bearing axis (persisted/cross-node ms vs in-process `Instant`).

## What the discipline is

A coordinator-free way to make an *effect happen once* despite at-least-once delivery and crashes,
**without** a distributed transaction. The shape:

1. **Claim** — an item is moved from "available" to "in-flight" atomically (a single owner holds it).
2. **Indivisible transition** — any state advance that must not partially apply is written as **one
   record** (never two that could half-replay).
3. **Ack** — terminal completion removes the in-flight claim; a duplicate ack is a no-op (idempotent).
4. **Crash-requeue (at-least-once)** — an in-flight claim that is not acked within a timeout returns
   to "available" and is re-claimed. The *effect* is exactly-once because the consumer's terminal ack
   is idempotent and the claim is single-owner; the *delivery* is at-least-once.

The load-bearing invariants:

- **Single-owner claim.** Two consumers never both hold the same item in-flight at once.
- **Idempotent terminal ack.** Re-acking an already-acked id changes nothing (the dedup point).
- **Indivisible multi-step records.** A stage→stage advance is one record, so replay never applies
  half of it (the duplicate hazard the split encoding reintroduces).
- **Bounded in-flight.** An unacked claim is reclaimed after a timeout — no item is lost to a dead
  consumer.

> This discipline is the tuple-space/blackboard half of a broader coordination story: a **capability-ring
> single-writer** made safe by a fencing discipline instead of a distributed lock. For when to use this
> vs the wiki's store-CAS vs the (CP) `LockService`, see
> [design/coordination-approaches.md](coordination-approaches.md).

## The reference implementation

**`mycelium-tuple-space/src/store.rs` is the canonical implementation.** Build new consumers against
*these* invariants rather than re-deriving them:

| Invariant | Reference site |
|---|---|
| Single-owner claim | `TupleStore::{take, take_by_key}` — move `entries`/`keyed_entries` → `inflight` under the stage lock (no TOCTOU between waiter-check and store) |
| Idempotent terminal ack | `TupleStore::ack` — `inflight.remove(id)`; absent ⇒ `NotFound`, never a double effect |
| Indivisible multi-step record | `Record::Complete` (+ `complete_keyed`) — old-ack + new-put in **one** WAL record; `open()` replay applies it atomically |
| Bounded in-flight (crash-requeue) | `TupleStore::requeue_expired` — re-dispatch unacked claims past `worker_timeout` (keyed items re-queue under their key) |
| Crash durability | WAL v2 (`Record` encode/decode, `open()` replay) + secondary replication (`apply_records`) + promotion replay |

## Code extraction: examined with the second user, **declined-with-evidence** (WS-G / G3 · Phase 6)

The G2 plan deferred the *code* extraction until a genuine second user of this exact mechanism
existed — explicitly **G3's blackboard** — so the abstraction would be driven by two concrete shapes,
not one shape + anticipation. The blackboard now exists (`mycelium-blackboard`, all phases shipped),
so the deferred decision is now **made with both implementations in hand**.

| User | Status | In-flight mechanism |
|---|---|---|
| **Tuple space** (`TupleStore`) | shipped | `Inflight.taken_at_ms: u64` — **wall-clock ms**, **WAL-persisted** (in `Record::Take`) and written into the cross-node advisory `tuple/inflight/{id}` visibility key. Also feeds `inflight_by_stage` (metrics), `inflight_snapshot` (compaction), and keyed dispatch. Exactly-once that is *persisted + cross-node*. |
| **Blackboard** (`BoardStore`) | shipped | `Inflight.claimed_at: Instant` — **monotonic, in-process only**, no timestamp in the WAL `Claim` record. Exactly-once whose in-flight *deadline* is in-process. |
| **Mailbox** (Actor/Event) | shipped | A genuinely different mechanism — KV + HLC keys + drain-then-tombstone; no in-flight claim/WAL/requeue. |
| **Supervision (M14)** | not built | — |

**Verdict: the shared artifact is the contract above, not a code overlay.** Examining the two real
implementations reveals a **load-bearing divergence** the surface similarity hid: the tuple space's
in-flight timestamp is **wall-clock-ms + persisted + cross-node** (it lives in WAL records and a
gossiped visibility key precisely because its exactly-once spans nodes and restarts), whereas the
blackboard's is a **monotonic `Instant` + in-process** deadline. A shared `InflightTracker<T>` would
have to be generic over the clock, and the tuple space additionally reaches into its in-flight set
through three crate-specific surfaces (`inflight_by_stage`, `inflight_snapshot`, keyed dispatch).
Forcing a shared overlay over a ~15-line kernel would couple two crates with **divergent evolution**
(a change for the tuple space's persisted/cross-node needs would ripple into the blackboard's
in-process path) — the exact failure mode the Rule of Three exists to prevent, just on the
over-abstraction side. So both crates implement the *same contract*, validated by the *same gate
shape* (claim single-owner, ack idempotent, expired re-queues), with **no code coupling**.

This is the intended outcome of the deferral, not a dodge: the second concrete user was the
*evidence* the decision waited for, and the evidence says the divergence is real. The contract
remains the single source of truth; a future third user with a *persisted, cross-node* in-flight set
(not the in-process blackboard) would be the trigger to revisit a shared persisted-claim overlay.
M14 supervision adopts whichever shape its capability-presence-invariant semantics need — decided
when M14 is specified.

> This mirrors the project's standing posture (Core Principle: detection-not-prevention; the WS-E
> "deferred-with-rationale" niceties; the elastic `IntentReconciler` sequencing): name the invariant,
> point at the reference, let the concrete users decide — including the decision *not* to couple them.
