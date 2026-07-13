# Coordination approaches — when to reach for the distributed lock (and why the companions don't)

*Design note / decision guide (2026-07-13). Not a new mechanism — a guide that unifies what four
existing docs each say in part. The payload: Mycelium ships a consensus-backed distributed lock, yet
**all three flagship companions coordinate a single writer without it — deliberately**. This is the
note a developer/architect choosing a coordination approach should read first; the mechanism detail
lives in the linked sources.*

## The question

You need coordination — one writer for a resource, one owner of a shard, one leader for a group.
Mycelium gives you a **`LockService`** (a leased, consensus-backed distributed lock with a fencing
token — [`src/agent/lock_service.rs`](../../src/agent/lock_service.rs)). Reach for it by reflex and
you may quietly build a **CP** service on an **AP** substrate — losing partition availability for no
reason. The tuple space, blackboard, and wiki each need a single writer and each **route around** the
lock. This note says which primitive fits which job, and why those three chose as they did.

## The deciding axis is CP vs AP

Everything turns on one question: *when the cluster loses quorum, must this coordination keep working,
or is it acceptable to block?*

- The **consensus overlay** (and the lock built on it) is **CP** — it *blocks*, not fails, without a
  quorum ([`04-consensus.md` §Dev Notes](../guide/04-consensus.md)). Correct where you need agreement;
  unavailable under partition.
- The **capability ring** (peer-exchange + advertised capabilities) is **AP** — a node self-elects
  from what it can see, so it **always** produces a writer, even partitioned. Each side of a split
  keeps serving.

So "lock or ring?" is really "can this tolerate blocking when quorum is lost, or must it stay
available?" For a work pipeline, a fact pool, or a shared wiki, the answer is *stay available* — which
is why none of the three uses the lock.

## The decision matrix

| Approach | Primitive | Availability | Dual-writer window closed by | Reach for it when |
|---|---|---|---|---|
| **Ring election + local mutex + id-fencing + idempotent WAL replay** | capability ring, **no quorum** | **AP** — always elects | id-fencing (`fetch_max` on ids) + idempotent replay → [exactly-once effect](exactly-once-effect.md) | A fast, always-available **single-writer of record** over in-process state (tuple space, blackboard) |
| **Ring election + store compare-and-swap** | capability ring + conditional write | **AP** | per-object CAS (`WikiError::Conflict` → re-reconcile), idempotent reconcile → exactly-once *effect* | A single-writer over a **shared/external store**, where an idempotent merge exists ([wiki](wiki-concurrent-edit.md)) |
| **`LockService`** (leased lock + monotonic fencing token) | epidemic **consensus** | **CP** — blocks without quorum | the lease (auto-expiry) + the fencing token surviving a GC pause | **Coarse ownership** where correctness needs a token that outlives a paused holder: leader election, shard/config ownership, migrations |

The first two rows are the *same idea* at different scales: a ring elects one writer (an availability
choice), and a **fencing discipline** — id-fencing for the WAL companions, version-CAS for the wiki —
makes the eventual-single election *safe* rather than merely convergent. The election is a **liveness**
property; correctness rides on the fencing, not on the election being instantaneous.

## Why the three companions reject the lock

**Tuple space + blackboard** — lowest-node-id ring self-election of a **primary** + a local
`Mutex` that serialises within it + a WAL, with **id-fencing** (`put_with_id`/`fetch_max`) so a
promoted secondary never re-issues an id the old primary assigned, and idempotent replay so a doubled
apply is harmless (the [exactly-once-effect contract](exactly-once-effect.md);
[`mycelium-tuple-space`](../../mycelium-tuple-space/src/lib.rs) · [`mycelium-blackboard`](../../mycelium-blackboard/src/lib.rs)).
They do **not** use the lock because a pipeline needs a *fast, always-available* single-writer — and a
lock **serialises** where a queue must **distribute** ([04-consensus.md](../guide/04-consensus.md)
says exactly this: *"don't build a queue from one lock"*). A quorum-blocking acquire would stall the
pipeline the moment a partition costs quorum — the opposite of what a work buffer is for.

**Wiki** — lowest-node-id ring self-election of a **curator** + **section-granular compare-and-swap**
on the store (`read_versioned` → reconcile → `write_section`/`update_manifest`, each keyed on the
version read; a stale write returns `WikiError::Conflict` and the curator re-reads + re-reconciles;
the idempotent reconcile makes the retry lossless — [wiki-concurrent-edit.md §3.5](wiki-concurrent-edit.md),
[`mycelium-wiki/src/agent.rs`](../../mycelium-wiki/src/agent.rs)). It does **not** use the lock for the
same reason, sharpened: the wiki's *entire purpose* is disconnected, KV-native operation — a
partition that briefly runs two curators must stay **safe and available**, which the CAS guarantees
(at-least-once/never-lose) and a quorum-blocking lock would forbid.

In all three, a transient split-brain (the ring hasn't reconciled yet) is **safe** — not because two
writers are prevented, but because the fencing discipline makes a doubled write lossless. That is the
crucial inversion: **you don't need a lock to prevent two writers; you need a fencing token or a CAS
to make two writers harmless.**

## When the lock *is* the right tool

Reach for `LockService` when **both** hold:

1. **You need a fencing token that survives a paused-but-alive holder.** A GC pause can outlast a
   lease; the only real protection is a monotonic token (`LockGuard::token`, the commit HLC — #164)
   that the *resource* enforces by rejecting a lower token than it has seen (Kleppmann — the two rules
   in [04-consensus.md](../guide/04-consensus.md)). The ring gives you a writer; it does not give you a
   token to fence a *foreign* resource you don't control.
2. **You can tolerate quorum-blocking (CP).** The acquire is a consensus round (~1s to converge) and
   blocks without quorum — fine for *coarse, infrequent* ownership: **leader election, shard/config
   ownership, schema migrations**. Not for high-rate or partition-sensitive paths.

If you only need one of these — a fast single-writer with no foreign resource to fence — the ring is
the better tool, and cheaper.

## The rule (and a note for a fourth companion)

> **Default to the ring. Reach for the lock only when you need a fencing token to survive a paused
> holder *and* can tolerate quorum-blocking.**

Building a new coordinated service on Mycelium? Start from the companion pattern, not the lock:

1. **Elect a single writer on the capability ring** (advertise a `{ns}.primary`/`.owner` capability;
   lowest-node-id self-elects; a watcher promotes when it evaporates — [companions.md](../operations/companions.md)).
   Coordinator-free and AP.
2. **Make the eventual-single election safe with a fencing discipline**, not with a lock — id-fencing
   + idempotent replay if you hold your own state (the [exactly-once-effect contract](exactly-once-effect.md)),
   or per-object CAS + idempotent merge if you write a shared store (the [wiki's model](wiki-concurrent-edit.md)).
3. **Only if a foreign, quorum-tolerant resource must be fenced** across a GC pause, take a
   `LockService` lock and stamp its token on every write.

This keeps the write path coordinator-free and partition-available — the substrate's whole thesis —
and reserves the (CP) consensus overlay for the operations that genuinely need agreement.

## See also — where each piece is detailed

- **The lock itself** (API, the two rules, "which primitive?" table, coarse/CP nature):
  [guide/04-consensus.md](../guide/04-consensus.md) · [`src/agent/lock_service.rs`](../../src/agent/lock_service.rs).
- **The at-least-once + idempotent = exactly-once-effect contract** (tuple space, blackboard):
  [design/exactly-once-effect.md](exactly-once-effect.md).
- **The wiki's section-granular CAS** (curator, `read_versioned`/`write_section`, never-lose):
  [design/wiki-concurrent-edit.md](wiki-concurrent-edit.md).
- **The per-companion coordination model, operationally** (ring failover, promotion latency, "no
  distributed lock"): [operations/companions.md](../operations/companions.md).
