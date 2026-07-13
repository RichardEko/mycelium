# 2026-07-13 — the wiki store is now CAS-airtight (strict-single per object)

**What shipped.** Closed a real (if narrow) lost-update window in `mycelium-wiki`'s curator write
path. The store no longer *assumes* a single writer — it *enforces* one **per object** via
compare-and-swap. Two curators (a transient split-brain the ring hasn't reconciled yet) can no longer
clobber each other's edits.

**The bug (root cause).** `apply_group` did a read-modify-write over the **whole page**: `store.read`
→ reconcile one section → `store.write_page(all_sections)`. `write_page` rewrote *every* section from
the curator's in-memory snapshot, so during a dual-curator blip two curators editing **different
sections of the same page** clobbered each other — A writes `{X=v1, Y=v0}`, B (snapshot taken before
A's write) writes `{X=v0, Y=v1}`, reverting X. The lost proposal was already tombstoned →
unrecoverable. `FsStore::write_page` was already atomic *per object* (temp+rename, manifest-last), so
readers were safe — but atomic rename prevents *torn writes*, not *lost updates* in a R-M-W. The old
"eventual-single, doubled-drain-is-harmless" posture leaned entirely on the idempotent reconcile,
which handles a re-applied *same* edit, not two curators interleaving R-M-W on *different* sections.

**The fix (both halves the user asked for).**
1. **Section-granular writes** — the curator writes only the section(s) in the batch, not the whole
   page. Different sections are independent slots → zero false contention between two curators.
2. **Compare-and-swap** — `read_versioned` returns per-section + manifest versions; `write_section` /
   `update_manifest` take an `expected` version and return `WikiError::Conflict` if it moved. On
   conflict the curator re-reads + re-reconciles (idempotent → lossless). Bounded retry (8); on
   exhaustion it returns `Err` and leaves the proposals un-tombstoned for the next drain.

**Why not the distributed lock (`LockService`).** It would close the window too, but it is
consensus-backed → **blocks with no quorum**, costing the wiki its partition availability — the wiki's
whole reason to exist is disconnected, KV-native operation. CAS keeps the write path coordinator-free.

**`FsStore` mechanism.** Immutable versioned objects `{base}.v{N}.json`; a write publishes the next
version with `std::fs::hard_link`, which is atomic and **fails if the version exists** → true CAS with
**no lock files, so no stale-lock deadlock** on a writer crash (a crashed writer leaves at worst an
ignored temp). Highest version wins; older GC'd once a newer commits; a head-check rejects a stale
writer filling a GC gap below the real head (which would otherwise be silently shadowed). `S3Store`'s
conditional `PUT`/`If-Match` is the identical contract — airtightness rides on the **trait**, so it
holds for any conditional-write backend, not just the local FS.

**Consequence for the design record.** The ring's eventual-single *election* is now a
liveness/efficiency property, **not** a correctness dependency. Updated
`docs/design/wiki-concurrent-edit.md` §3.5 (which had accepted "eventual-single, not strict-single")
and `docs/wiki/dev/companions/wiki.md`.

**At-least-once, not store-level exactly-once — surfaced by a concurrency stress test.** The first
multithreaded test asserted store-level exactly-once (a non-idempotent counter of `THREADS*INCS`
increments) and reliably *over*shot (321–326 vs 320) under load — never *under* (min 321 over 30 runs).
Root cause: `hard_link` publishes a version that is immediately readable, so a follower can consume it
(build the next version on top) *before* the original writer's head-check reports `Conflict`; the
writer then retries and re-applies. That is **at-least-once/never-lose**, not a bug — it is the *same*
contract the tuple space and blackboard run on (`docs/design/exactly-once-effect.md`): the store is
at-least-once, and exactly-once **effect** comes from the reconcile being idempotent (append-merge
skips an already-contained body). Bounded disk (GC of old versions) is why the store cannot promise
exactly-once at its own layer — closing the consume-before-confirm window would need an unbounded,
gap-free version history. The lesson: the deterministic contract tests proved the *shape* of the CAS
but asserted the wrong bar; only a real racing test caught the exactly-once-vs-at-least-once
distinction. Fixed the test to assert the real property (idempotent appends → every edit present
exactly once) and tightened the store/model docs, which had over-implied store-level exactly-once.

**Regression gates** (`mycelium-wiki/src/fs/tests.rs`): deterministic — `concurrent_curators_editing_different_sections_dont_clobber`,
`same_section_stale_write_is_rejected_not_silently_lost`, `creating_a_section_that_already_exists_conflicts`,
`manifest_membership_is_compare_and_swap`, `a_stale_write_into_a_gc_gap_is_rejected_not_shadowed` (the
head-check/GC-gap branch); multithreaded — `concurrent_idempotent_appends_deliver_every_edit_exactly_once`,
`many_threads_editing_different_sections_all_survive` (0/25 full-suite runs failed under load). Full
crate suite 26 lib + 2 access + 2 failover green; clippy `--all-targets` clean on default and
`control-plane,gateway`.

**How it surfaced.** A design question — "we introduced a lock-manager service; should the wiki /
tuple-space adopt it?" The answer was *no for all three* (the tuple space + blackboard already close
their failover window with id-fencing + idempotent WAL replay; adopting the quorum-blocking lock would
be a CP-for-AP regression). But the audit of the wiki's coordination found `write_page`'s whole-page
R-M-W was the one genuine correctness exposure — reading-for-coverage surfacing a real bug again.
