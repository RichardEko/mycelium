# OR-Map CRDT for the `gcap/` registry projection — evaluation

*WS-F design note (2026-06-19). Per ROADMAP §16 [Quilt-DD #3] this is a **design note, not a
rewrite**: evaluate observed-remove (OR-Set / OR-Map) semantics for the group-capability registry
where concurrent add/remove is framed as the norm — and decide whether to adopt it or keep the
existing LWW + HLC convergence. **Conclusion: keep LWW + HLC.***

## The question

An **OR-Map / OR-Set** (observed-remove) CRDT exists to fix one specific anomaly that last-writer-
wins has: when **two actors concurrently add and remove the _same_ element**, LWW resolves it by a
single timestamp and can let a `remove` erase a causally-concurrent `add` (the "remove wins over a
concurrent add" surprise). OR-semantics instead tag every `add` with a unique token and let a
`remove` retract only the tokens it *observed* — so a concurrent `add` (a token the remover never
saw) **survives**. The cost is metadata: per-element add-token sets that must be gossiped and GC'd.

The plan flags `gcap/` — the projection of group members' capabilities — as a place "where
concurrent add/remove is the norm," which is the textbook trigger for considering OR-semantics.

## What `gcap/` actually is

Group-capability projections are keyed **`gcap/{group}/{ns}/{name}/{contributor}`** (see
`emergent_groups.rs` and `wiring.rs::parse_gcap_key`). The **contributor (the node) is part of the
key**. Each node:

- **adds** by writing `gcap/{group}/{ns}/{name}/{self}` for each capability it `provides`, and
  **re-asserts** it on a ticker (the evaporation/refresh convention);
- **removes** by tombstoning **its own** `gcap/…/{self}` entries on leave/shutdown.

So every `gcap/` element is **single-writer**: only the contributor named in the key ever writes or
tombstones that key. There is no key that two different nodes both add/remove.

## Why LWW + HLC is sufficient here

The OR-Map's reason to exist — **concurrent add and remove of the same element by _different_
actors** — **cannot arise** in `gcap/`, because the element identity *includes* the actor:

1. **Cross-node churn is adds/removes of _different_ keys.** "Membership churns constantly" means
   many nodes joining/leaving — but each touches `gcap/…/{itself}`, a distinct key. Distinct keys
   never conflict under LWW; they simply coexist or independently tombstone. No anomaly.
2. **The only same-key sequence is one owner's add↔remove, which is causal, not concurrent.** A
   node's re-assert (`add`) and its leave-tombstone (`remove`) are operations by the *same* node,
   ordered by that node's own HLC. `Hlc::tick` guarantees a node's later write has a strictly
   greater timestamp, and the leave path cancels the re-assert ticker *before* tombstoning — so the
   tombstone is causally last and LWW correctly lets it win. There is no second writer to be
   "concurrent" with.
3. **A re-join after leave is a new, later write** — a fresh `add` with a greater HLC than the
   prior tombstone, so it correctly resurrects the entry (this is the *intended* "add-wins" outcome,
   achieved by causal ordering, not by OR tokens).

In OR-Map terms: a map whose keys are `(element, actor)` and whose only writer per key is that
actor is **already** an OR-Map — the actor id *is* the unique add-tag, and "observed-remove"
degenerates to "the owner removes its own tag," which plain LWW-by-HLC implements exactly. Adopting
an explicit OR-Set would add per-element token-set metadata and GC for a concurrency case the keying
scheme has already designed away.

## Where OR-semantics _would_ be warranted (the boundary)

OR-Map earns its metadata cost only for a **shared, multi-writer set** — one where several nodes
concurrently add/remove the **same** element (a key *not* partitioned by writer). Mycelium does not
have such a set today; if a future feature introduces one (e.g. a collaboratively-edited tag set, or
a membership roster written by peers *about* a node rather than by the node itself), revisit OR-Set
**for that structure specifically** — not as a wholesale replacement of LWW + HLC.

Note the `sys/quorum/` precedent already in the tree: it is deliberately excluded from the
self-owned-namespace tripwire *because* peers legitimately attest about an observed node — i.e. it
*is* multi-writer. That is the shape that would merit OR-semantics; `gcap/`, single-writer by
construction, is not.

## Decision

- **Keep LWW + HLC for `gcap/`.** The contributor-keyed projection is single-writer per element, so
  the OR-Map's only advantage (surviving a concurrent add against a remove by a *different* actor)
  has nothing to resolve. LWW-by-HLC already yields the correct, convergent, anti-entropy-healed
  result with zero extra metadata.
- **Record the trigger.** Reconsider OR-Set/OR-Map only if a genuinely multi-writer shared set is
  introduced (the `sys/quorum/` shape), and scope it to that structure.

This closes the WS-F OR-Map line as the design note it was specified to be.
