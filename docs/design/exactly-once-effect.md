# The exactly-once-effect discipline — shared contract (WS-G / G2)

**Status:** documented contract (2026-06-20). Code-level extraction **deferred-with-rationale**
(Rule of Three — see below). This is the v2.0 §WS-G "done-when" item: *"the exactly-once-effect
dedup discipline is extracted as a reusable overlay shared by the tuple space, mailbox, supervision
(M14), and the blackboard."*

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

## Why the code is not yet extracted (Rule of Three)

The done-when names four prospective users; **only one exists as this mechanism today**:

| Prospective user | Status | Mechanism |
|---|---|---|
| **Tuple space** | shipped | This discipline (WAL claim/ack/requeue). The reference. |
| **Mailbox** (Actor/Event) | shipped | **A different mechanism** — KV-backed, HLC-ordered keys under `mailbox/{target}/{kind}/{hlc}`, drain-then-**tombstone** (idempotent read-side eviction). At-least-once delivery with idempotent tombstoning; **no in-flight claim, no WAL, no requeue**. Sharing an `ExactlyOnce` claim/ack overlay would not fit it. |
| **Supervision (M14)** | not built | — |
| **Blackboard (G3)** | not built | — |

Extracting a shared `ExactlyOnce` abstraction now would mean abstracting across **one** real user, a
second user whose mechanism is genuinely different, and two that do not exist — speculative
abstraction with no second concrete shape to generalise from. The disciplined call (and the one the
WS-G plan explicitly authorized) is to **document the contract now** and **extract the code when a
genuine second user of this exact mechanism lands** — which is **G3 (the blackboard), whose
competitive destructive claim-by-predicate is precisely this discipline.** At that point there are
two real implementations to factor a correct overlay from, and the extraction is driven by demand,
not anticipation.

**Trigger to extract:** G3's claim-by-predicate (the second real in-flight-claim/ack/requeue user).
Build it against this contract; then lift the shared core out of the two and have both reference it.
M14 supervision adopts whichever shape (this overlay, or the mailbox's tombstone pattern) its
capability-presence-invariant semantics actually need — decided when M14 is specified, not now.

> This mirrors the project's standing posture (Core Principle: detection-not-prevention; the WS-E
> "deferred-with-rationale" niceties; the elastic `IntentReconciler` extraction sequencing): name the
> invariant, point at the reference, and let the second concrete user drive the abstraction.
