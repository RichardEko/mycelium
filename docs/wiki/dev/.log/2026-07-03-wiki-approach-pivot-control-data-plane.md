## [2026-07-03] ingest | mycelium-wiki approach pivot — control-plane / data-plane

Reviewing two driving use cases with the intended owner (Novus-i2 organisational digital-twin;
Transparency-Platform council decisions) pivoted the whole `mycelium-wiki` approach **away from a
KV-native section-CRDT** toward **control-plane / data-plane**.

**The realisation:** the wiki is the *maintained-meaning / authoritative-specific* layer that
**composes** with an external metrics/structure store (Postgres) and RAG (background), joined by a
**shared id namespace** — it is not a replacement for either. And for those deployments there is **no
reason to push the corpus into gossiped KV**: KV floods every cluster node (group scope is
access/namespacing, *not* replication isolation — [runtime-invariants](../architecture/runtime-invariants.md)),
so a durable per-group corpus would replicate everywhere. The other two layers are already external
stores reached by tools; making the wiki the third is uniform, and it dissolves the corpus-scaling
problem.

**New architecture:** the corpus lives in a **node-independent, pluggable store** (shared FS dir / S3
bucket / doc store — it can be *dumb*). A group node runs a **curator** service that (1) serialises
writes — the single writer of record, so concurrent same-section edits degrade to a single-writer
sequence, no CRDT needed; (2) runs the LLM ingest/reconcile + lint; and (3) **brokers access** —
hands out the store location + a scoped, short-lived read grant, so **group membership
(`Boundary::admits`) is the access gate**. Group agents **read the store directly, in parallel**.
Mycelium is the **control plane**: `wiki.{group}.curator` capability advertisement (carrying the store
location) + ring-failover, the small **evaporating proposal queue** in KV, and the MCP tool — never
the storage. Writes are ordered *objects first, manifest last* so a direct reader never sees a torn
edit.

**Why it's sympathetic:** this is the wiki pattern's *native* shape — files in a store, an LLM curator
maintains them, everyone reads the files directly — exactly how this very `docs/wiki/` works. We
stopped inventing a distributed-consensus problem; the concurrent-prose-merge "hard problem" reduces
to single-writer-curator + the store's per-object atomicity. A group-scoped, self-healing curator
role is a *role with failover*, not the coordinator trap.

**What carries over / what's retired:** the identity model ("competence is a capability, knowledge is
not"), the curator state machine (§3), and the curator `lib.rs` surface (§6) carry over unchanged. The
KV-native section-CRDT (design record §1–§2) is **retained as the disconnected / no-external-store
variant** (edge/autonomous fleet with nowhere to point a store). A `WikiStore`-in-KV crate was spiked
to clarify the design, then **retired** (removed from the workspace; uncommitted).

**Docs updated:** `docs/plans/mycelium-wiki.md` (rewritten — Architecture, Composition, control-plane
KV namespace, roles, durability/failover, re-phased build), `docs/design/wiki-concurrent-edit.md`
(header reframed as the disconnected variant), `docs/plans/README.md`, `companions.md`,
`runtime-invariants.md` (the cross-reference). Build not started under the new shape.
