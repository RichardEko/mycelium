# Group-wiki concurrent edits — section addressing, merge semantics, curator state machine

**Status:** 📐 **design record, pre-build** (2026-07-02). This is the Phase 0 artifact the
[`mycelium-wiki` sketch](../plans/mycelium-wiki.md) calls for — it formalises the two areas the
sketch flagged as load-bearing: (1) the **section addressing + merge unit** and (2) the **curator
role state machine**. Written *before* the crate exists so the mechanism is settled and reviewable;
promotes to the crate's `lib.rs` doc + a `v2-…-wiki.md` build plan when the demand trigger fires.
Nothing here changes core — it is a discipline over the public KV/signal/capability API, the way
[`exactly-once-effect.md`](exactly-once-effect.md) is.

The whole record exists to answer one question: **how does a group of LLM agents edit a shared prose
wiki concurrently without either losing edits (naive LWW on whole pages) or diverging forever
(free-for-all LLM merge)?** The answer is *make the common case a convergent CRDT and the rare case a
serialised single-writer*, and prove each half.

---

## 0. Notation & invariants to preserve

- The substrate gives **per-key LWW + HLC**: for one key, all replicas converge to the write with the
  greatest packed-HLC timestamp (deterministic byte tiebreak on equal stamps —
  `store.rs::lww_wins`). This is the *only* convergence primitive we get, and everything below is
  built to stay inside it.
- **I1 (convergence):** any two replicas that have seen the same set of writes hold the same wiki.
  Non-negotiable — it is the substrate's core guarantee; a design that breaks it is rejected.
- **I2 (no lost durable edit):** an accepted contribution is never silently dropped by a concurrent
  contribution. (Naive whole-page LWW violates I2; that is the entire problem.)
- **I3 (curator-optional readability):** with no live curator, the wiki is fully **readable** and
  contributions **persist** (as evaporating proposals); only *reconciliation* pauses. This is the
  management-as-intent litmus ([`../wiki/domain/theory/management-as-intent.md`](../wiki/domain/theory/management-as-intent.md)).

---

## 1. Section addressing — the merge unit

A page is not one key. A page is an **ordered list of sections**, each an independently-keyed LWW
value. The page is the reconciliation of its sections at read time.

### 1.1 Keys

```
wiki/{group}/page/{page-path}/meta                 → PageMeta   (section order + page-level fields)
wiki/{group}/page/{page-path}/sec/{section-id}     → SectionBody (the prose of one section)
```

- `{page-path}` is a kebab path (`incidents/cert-rotation`). `{section-id}` is a **stable opaque
  id**, *not* a heading text and *not* a line/ordinal number (see §1.3 — this is the crux of
  stability under concurrent edits).
- `PageMeta.order: Vec<SectionId>` gives the render order; section bodies are addressed by id, so a
  reorder is a `meta` write that touches no body, and a body edit is a `sec/{id}` write that touches
  no `meta`. **Different concerns → different keys → no false LWW collision.** This is the same move
  the KV store already relies on (secondary indices keyed separately from data) generalised to prose.

### 1.2 Why section-granular satisfies I2 for the common case

Two contributors editing *different* sections of the same page write *different keys* — both writes
survive, LWW never sees a conflict, I2 holds trivially, and I1 holds because each key converges
independently. The empirical claim behind "common case": in a curated knowledge base, near-all
concurrent edits touch different sections (an incident page's "symptoms" vs its "resolution"), the
same way near-all KV writes touch different keys. Section granularity converts the *default* editing
pattern into a lost-update-free CRDT with zero LLM involvement.

### 1.3 Section ids must be stable and content-independent

A section id is minted **once**, at section creation, and never changes — it is content-addressed at
birth but identity-stable after (`sid = base32(hash(group ‖ page-path ‖ mint-hlc ‖ nonce))`,
truncated). It is **not** derived from the heading or the body, because:

- deriving from the heading means renaming a heading re-keys the section → the old key tombstones,
  the new key inserts, and a concurrent body edit to the "same" section lands on the *old* key and is
  lost (I2 violation);
- deriving from ordinal position means inserting a section above shifts every id below → mass
  spurious re-key, same failure.

Stable ids are what make "edit section X" and "rename/move section X" independent, non-conflicting
operations. The id lives in `PageMeta.order` and in the `sec/{id}` key; a heading is just a field of
`SectionBody`, freely editable.

### 1.4 Section create / delete / reorder

- **Create:** contributor mints `sid`, writes `sec/{sid}`, and proposes a `meta` update inserting
  `sid` into `order` at a position. Two concurrent creates → two `sec/` keys (both survive) + two
  concurrent `meta` writes (§2.2 handles the `order` merge).
- **Delete:** tombstone `sec/{sid}` **and** propose `sid`'s removal from `order`. A body edit racing a
  delete: LWW on `sec/{sid}` decides (delete = tombstone; a later body write with a greater HLC
  resurrects — deliberate: "someone kept editing while I deleted" resolves to keep, the safe default
  for knowledge, and it converges because it is ordinary LWW).
- **Reorder:** a pure `meta.order` permutation, no body writes.

---

## 2. Merge semantics

Three cases, in increasing cost. The design goal is to keep the expensive case rare and *serialised*.

### 2.1 Different sections (the common case) — pure CRDT, no curator

Covered by §1.2. Convergent by the substrate; no reconciliation needed; works with **no live
curator** (I3). Contributors write section bodies directly for this case — a contribution that its
author knows targets a distinct/new section need not queue (it can't lose-update). The crate's
`propose` API decides "direct write vs queue" by whether the target section id already exists and is
being concurrently touched — conservatively, a *new* section is a direct write; an edit to an
*existing* section is a queued proposal (§2.2).

### 2.2 Same section, or `meta.order` (the rare case) — serialised reconcile

Two contributions to the *same* `sec/{sid}` (or two `meta.order` mutations) genuinely conflict on one
key — LWW would drop one (I2). These do **not** write the live key directly. They append a
**proposal**:

```
wiki/{group}/proposal/{proposal-id}   → Proposal { target: SectionRef|Meta, base_hlc, edit, author, mint_hlc }
```

Proposals are **evaporating soft-state** (short `refresh_interval` — a crashed author's proposal ages
out; I3). The **curator** (§3) drains proposals for a given target, reconciles them into the live key,
and the reconcile is the *only* writer of that key while the curator is live. Because there is **one
writer of record per section at a time**, the same-section case degrades to a single-writer sequence —
no concurrent LWW conflict on the live key, I2 preserved (every proposal is either merged or explicitly
superseded, never silently dropped), I1 preserved (the live key is written by one node in HLC order).

`base_hlc` is the HLC the author last read for that section — it lets the curator detect "this
proposal was written against a stale version" and hand the LLM both the base and the current text to
do a **3-way** reconcile (base + current + proposed), the same shape as a git 3-way merge, rather than
a blind overwrite.

### 2.3 Why free-for-all LLM merge is rejected (the declined alternative)

The tempting shortcut — every agent's LLM reconciles concurrent edits inline, no curator — **violates
I1**. LLM reconciliation is non-deterministic: two replicas that both observe proposals {P, Q} and
reconcile locally produce *different* prose, and since each writes its own result to `sec/{sid}`, the
key now has two different values racing on LWW — they never converge to the *same* text, only to
"whichever HLC happened to be larger," which is not a reconciliation, it is a coin-flip that also
threw away content. Determinising it (pinned model + pinned prompt + canonical proposal ordering) is
possible in principle but (a) fragile across model/version drift, (b) still forces every replica to
run the LLM on every conflict (cost O(replicas × conflicts) vs the curator's O(conflicts)), and (c)
turns a model upgrade into a convergence-breaking event. **Declined-with-evidence.** The curator makes
the LLM step *serialised at one node*, which is exactly what buys back I1: one writer ⇒ one value ⇒
convergence, regardless of the LLM's non-determinism.

This is the direct analogue of the substrate's own posture: the KV store never lets two winning
writers interleave a derived effect (the M2 Run-18 stripe-lock finding —
[`../wiki/dev/concurrency/lock-free-and-atomics.md`](../wiki/dev/concurrency/lock-free-and-atomics.md));
here the "derived effect" is an LLM reconcile and the "stripe lock" is the single-curator role.

---

## 3. The curator role state machine

Exactly one **live** curator serves a group's reconcile + lint; `WikiRole::Auto` nodes elect and fail
over. The role is *recallable* (I3): losing it pauses reconciliation, nothing else.

### 3.1 States

```
        ┌─────────┐  advertise wiki.{group}.candidate
        │  Reader │──────────────┐
        └─────────┘              ▼
                          ┌────────────┐  lowest live candidate-id?
                          │ Candidate  │───────────── no ──┐
                          └─────┬──────┘                   │
                        yes │   ▲  higher candidate appears │ (stay)
                            ▼   │ & I'm not lowest          │
                       ┌─────────────┐                      │
                  ┌───▶│   Curator   │◀─────────────────────┘
                  │    └─────┬───────┘
     reconcile /  │          │  own capability evaporates (crash) OR
     lint tick    │          │  a strictly-lower candidate-id appears (yield)
     (self-loop)  └──────────┤
                             ▼
                       ┌────────────┐
                       │  Stepping  │  drain in-flight reconcile, retract wiki.{group}.curator
                       │   Down     │
                       └─────┬──────┘
                             ▼  back to Candidate/Reader
```

- **Reader** — read-only; never advertises curator candidacy. Most agents.
- **Candidate** — advertises `wiki.{group}.candidate` (evaporating capability). Watches the candidate
  set; if it holds the **lowest candidate-id** among live candidates, transitions to Curator.
  (Lowest-id tie-break = the tuple-space/blackboard `Auto` election, reused verbatim — no new
  mechanism, and it is the coordinator-free election the ring already does.)
- **Curator** — advertises `wiki.{group}.curator`; runs the reconcile loop (§3.2) and the lint loop
  (§3.3). Yields if a strictly-lower candidate-id appears (deterministic, converges to a single
  curator without flapping because the order is total).
- **Stepping Down** — finishes any in-flight reconcile (a reconcile is a single LWW write, so
  "in-flight" is sub-millisecond), retracts the curator capability, returns to Candidate/Reader.

### 3.2 The reconcile loop (idempotent, at-least-once)

```
loop while Curator:
  proposals = scan(wiki/{group}/proposal/*)            # evaporating; crashed authors' age out
  group proposals by target (section-id or meta)
  for each target with ≥1 proposal:
     base    = proposal.base_hlc's text (or current if absent)
     current = live sec/{sid} (or meta)
     merged  = if single proposal and base==current:  apply directly (no LLM)
               else:                                   LLM 3-way reconcile(base, current, [edits])
     write  sec/{sid} = merged        # ONE LWW write, HLC-stamped, keyed by section id
     tombstone each consumed proposal # idempotent: a re-drained proposal re-reconciles to the same text
  sleep(reconcile_interval)
```

**Idempotence / at-least-once (the exactly-once-effect contract, third instance —
[`exactly-once-effect.md`](exactly-once-effect.md)):** a curator that crashes mid-drain is re-elected
(or a peer promotes); the new curator re-scans the *still-present* proposals (they were tombstoned
only after the write, and a proposal reconciled-but-not-yet-tombstoned simply reconciles again to the
same section text — LWW-idempotent because the merged content is a deterministic function of
(base, current, edits) *for the no-LLM fast path*, and for the LLM path a second reconcile of the same
inputs produces *a* valid merge that overwrites the first; either way the key ends single-valued and
convergent because there is still only one writer). Delivery is at-least-once; the *effect* (the
section reaches a merged state incorporating every proposal) is once, because proposals are only
tombstoned after they are incorporated.

Note the LLM path is **not** claimed to be deterministic — it does not need to be. The convergence
guarantee (I1) comes from *single-writer*, not from merge determinism: whatever text the one live
curator writes is what every replica converges to. This is the precise reason the curator exists.

### 3.3 The lint loop (the group-function generalisation of `/wiki-lint`)

On a slower tick the curator runs the fleet analog of the project's `/wiki-lint`: dead cross-links
(a `[[...]]` to a non-existent page/section), orphan sections (in a `sec/` key but absent from any
`meta.order`), staleness (a page whose cited external fact — a config key, a capability name — the
curator can probe and no longer finds), and coverage gaps. Findings are written durably to
`wiki/{group}/.lint/{hlc}` (the group analog of `docs/wiki/**/.log/`), where any agent can read them
and file a corrective proposal. Lint is detection-not-prevention (it never blocks a write), matching
the substrate's posture everywhere else.

### 3.4 Failover has no heartbeat / WAL cursor

Unlike the tuple space (whose secondary needs a replicated log + heartbeat), the curator holds **no
authoritative state** — pages and proposals are durable KV; the curator's "state" is entirely
*derivable* by scanning them. So a promoted curator just starts its reconcile loop; there is nothing
to replay or hand off. This is the blackboard's `Post`/`Ack` simplification (snapshot-derivable
mirror, no heartbeat) applied one level up — recorded here as the reason `mycelium-wiki` will *not*
copy the tuple space's WAL-cursor machinery.

---

## 4. What this buys, restated against the invariants

| Case | Mechanism | I1 (converge) | I2 (no lost edit) | I3 (curator-optional) |
|---|---|---|---|---|
| Different sections | independent LWW keys | ✓ per-key | ✓ different keys | ✓ direct write, no curator |
| New section | new `sec/` key + `meta` insert | ✓ | ✓ | ✓ direct write |
| Same section | proposal queue → serialised curator reconcile | ✓ single writer | ✓ every proposal merged or superseded | ⏸ reconcile pauses, reads + proposals persist |
| `meta.order` conflict | proposal → curator reconcile of the order vector | ✓ single writer | ✓ | ⏸ |
| Curator crash | re-election + re-drain of live proposals | ✓ | ✓ at-least-once idempotent | ✓ self-heals |

The design's one sentence: **section granularity turns the common case into a lock-free CRDT that
needs no curator, and the curator turns the rare same-section case into a single-writer sequence —
so the LLM's non-determinism is quarantined at one serialised node and never threatens convergence.**

## 5. Open questions for the build plan (not decided here)

- **Section-split/merge edits** (an author wants to split one section into two, or fuse two): these
  touch multiple `sec/` keys + `meta.order` atomically-ish. Proposed as a single multi-key proposal
  the curator applies in one reconcile pass; the *degenerate* interleaving with a concurrent
  same-target edit needs a worked example before v1.
- **Proposal starvation / ordering fairness** when one hot section accumulates proposals faster than
  the curator drains — bounded by proposal evaporation, but a backpressure pheromone
  (`sys/wiki/{node}/{group}/…`, mirroring the tuple space's) may be warranted.
- **LLM cost governance** — the reconcile is the only LLM call; rate/lint intervals are governor
  knobs (management-as-intent). Whether reconcile should batch multiple same-section proposals per
  LLM call (cheaper, but larger blast radius on a bad merge) is a tuning decision for the build.
- **Read-time vs write-time page assembly** — this record assembles a page from its sections at
  *read* time (cheap keys, assembly on demand). A cached assembled `page/{path}/rendered` is a
  possible optimisation, explicitly deferred (it reintroduces a derived-effect-to-serialise, so it
  would be curator-written if added).
