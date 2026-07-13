# mycelium-wiki — group-scoped, LLM-curated wiki (a store, not a service)

↑ [companions](companions.md) · siblings: [tuple-space](tuple-space.md) · [blackboard](blackboard.md)

The **durable, curated** third coordination primitive: where the tuple space routes by lane
**position** (transient) and the blackboard by content **predicate** (transient/competitive), the wiki
is the **compounding, re-read** slot — the long-term-memory sibling of the blackboard's working memory.
Build phases 1–5 shipped 2026-07-03, all CI-green (wiki job). Plan: `docs/plans/mycelium-wiki.md`;
design record `docs/design/wiki-concurrent-edit.md`.

## The load-bearing shape: control plane / data plane

The corpus is **not** in gossiped KV (KV floods every node unconditionally — group scope is
access/namespacing, never replication isolation; that invariant is what drove the pivot out of KV). It
lives in a **node-independent, pluggable store**. Two planes, each used the way the architecture
intends — and the worked example demonstrates both:

- **Data plane** (`WikiStore` trait + `FsStore` ref impl): the pluggable backing store, deliberately
  **Mycelium-agnostic**. `read`/`read_versioned`/`query`/`write_section`/`update_manifest`/`write_page`/
  `list_pages`/`location`. The concurrent write path is a **section-granular compare-and-swap**
  (`read_versioned` → reconcile → `write_section`/`update_manifest`, each keyed on the version read);
  `write_page` is a non-CAS full-replace convenience. Torn-read-safe for readers; `read` is
  manifest-authoritative (a stray unreferenced section is invisible). **Readers go direct to the store,
  in parallel — no node, no curator.** That node-independence is what makes the wiki a *store, not a
  service*.
- **Control plane** (feature `control-plane`, the Mycelium side): a single elected **curator**
  discovered on the capability ring, the single writer of record.

## Curator election + failover (the recallable role, not the coordinator trap)

`WikiRole` Auto/Curator/Reader. Election mirrors the blackboard's: advertise
`wiki.{group}.candidate` → settle → **lowest candidate id** self-elects `wiki.{group}.curator`, else
become a reader that watches. **Ring-failover:** two consecutive empty `curator` resolves (one refresh
apart — split-brain guard) → re-elect. Because the store is node-independent, **failover transfers
nothing**: a promoted curator resumes against the *same* store and re-drains the *same* proposals. The
litmus: *if the curator vanishes, the wiki stays readable and a new curator self-elects.*

**Step-down (split-brain reconciliation, PR #127):** the election settles on a *fixed* window
(`(cap_refresh*2).max(2s)`), so a lost gossip race can leave two nodes self-elected — both writing the
shared store, with no recovery (this flaked `tests/failover.rs::curator_elects_…` once in CI). A curator
**sentinel** now applies *lowest-id-wins continuously*, not just at election: a curator that sees a
lower-id peer curator **resigns** — stops *just* its own drain/lint/broker loops (held in a separate
`curator_tasks` list so the sentinel that triggered it survives), retracts the ad, and returns to the
reader watch. Deterministic: the lowest stays, every other steps down; self-healing. Canary:
`tests/failover.rs::dual_curators_reconcile_to_a_single_writer` (fails at 30 s without the sentinel).

**Lifecycle (learned the hard way — analysis Run 32):** the curator's background loops (drain / lint /
election / watch) each hold an `Arc<Self>` and loop unconditionally, and they use raw `tokio::spawn`
(not the agent's tracked `spawn_task`, so agent shutdown does **not** reap them). Call **`Wiki::shutdown`**
to reclaim a wiki — it aborts the tasks (releasing their `Arc<Self>`, breaking the strong-ref cycle) and
retracts the cap ads. Mirrors `Blackboard::shutdown`; without it a discarded `Wiki` leaks its tasks until
the runtime ends. Canary: `agent::tests::shutdown_breaks_the_task_cycle_and_frees_the_wiki`.

## The access broker (membership-gated store grant)

`Wiki::request_store_access` is a **one-time** handshake: an agent RPCs the curator, which — if its
`Membership` gate (`Open` | `Allowlist`, set on the `CuratorBrain`) permits the requester's
transport-authenticated node id — replies with a `StoreGrant` (the store `location`; a real object-store
adapter would add a scoped credential). After the grant the agent opens the store and reads **directly**,
so the broker is **not on the read path** — node-independence holds. RPC (not KV) so a grant/credential
goes point-to-point, never floods the cluster. The curator self-grants; `AccessError::{NoCurator,Denied}`
on the retry/deny paths. Cross-node `tests/access.rs`: an allowlisted reader is granted its store's
location; one outside the allowlist is denied.

## The write path: propose → drain → reconcile → apply

- **Evaporating proposal queue:** any agent (any role) `propose`s → an evaporating
  `wiki/{group}/proposal/{id}` KV entry. Coordinator-free.
- **Single-writer apply:** only the curator drains, **grouping proposals by target section** so a
  same-section conflict reaches the reconcile as *one* batch — no CRDT. Tombstones the proposal only
  after the store write lands.
- **CAS-airtight, not single-writer-*assuming* (2026-07-13):** the apply is a **section-granular
  compare-and-swap**, so **no lost update even during a dual-curator blip** — not merely as the
  single-writer dividend. The old whole-page `write_page` let two transient curators editing *different
  sections of one page* clobber each other (B's full-page snapshot reverting A's section, proposal
  already tombstoned → unrecoverable). Now each section is an independent CAS slot: a stale-based write
  returns `WikiError::Conflict`, the curator re-reads + re-reconciles, and different sections don't
  contend. `FsStore` implements the CAS with immutable versioned objects published by atomic `hard_link`
  (no lock files → **no stale-lock deadlock**); `S3Store`'s conditional `PUT`/`If-Match` is the same
  contract. The ring's eventual-single *election* is now liveness/efficiency, **not** a correctness
  dependency — a partition running two curators is safe (that is what keeps the write path
  coordinator-free, no distributed lock).
- **The store CAS is at-least-once/never-lose, *not* store-level exactly-once** — the important nuance a
  concurrency stress test (`concurrent_idempotent_appends_deliver_every_edit_exactly_once`) surfaced. A
  version can be consumed by a follower and then re-applied on the original writer's retry, so a
  `Conflict` caller may apply an edit more than once; nothing is ever *lost*. Exactly-once **effect**
  comes from the reconcile being **idempotent** (the append-merge skips an already-contained body) —
  the *same* at-least-once + idempotent-merge = exactly-once-effect contract the tuple space and
  blackboard use (`docs/design/exactly-once-effect.md`). Bounded disk (old versions GC'd) is why the
  store cannot promise exactly-once at its own layer. Regression gates (`fs/tests.rs`):
  `concurrent_curators_editing_different_sections_dont_clobber`, `same_section_stale_write_is_rejected_not_silently_lost`,
  `manifest_membership_is_compare_and_swap`, `a_stale_write_into_a_gc_gap_is_rejected_not_shadowed`,
  and the multithreaded `concurrent_idempotent_appends_deliver_every_edit_exactly_once` +
  `many_threads_editing_different_sections_all_survive`.
- **Reconcile** (`Reconciler`, dyn-safe `#[async_trait]`): `DirectReconciler` (default, no LLM) is a
  **lossless append-merge** that skips an already-contained body → **idempotent**, so crash-mid-drain
  re-drains to the same result. `LlmReconciler` (feature `llm`) is a real **3-way merge** over
  `mycelium::LlmBackend` — the LLM curates the **prose**, heading + attributes merge **structurally**
  (code-controlled; the model never invents join keys). Backend error → append-merge fallback (an LLM
  outage degrades curation, never a write). Injected via `CuratorBrain`.

## The lint loop (the group function — detection, not prevention)

Only the curator lints (one lint of record); findings are **advisory** (`Wiki::last_lint()` + warn
log), never auto-applied. It is **change-driven** (Run-32 scalability fix): a whole-corpus pass runs
only when the store changed since the last one (a `lint_dirty` flag set by `apply_group`), so an idle
wiki does zero lint work — cost is proportional to change, not to elapsed time. `lint_pass_count()`
exposes how many passes have run. `structural_lint` (always-on, deterministic):
dead cross-links (`[[page]]` / `[[page#section]]`) + empty sections, pure over the pages. `SemanticLinter`
(feature `llm`, `LlmSemanticLinter`): cross-section self-consistency (the UC1 org-twin must not assert
contradictory facts).

## MCP tools + the worked example

- **MCP tools** (`Wiki::register_mcp_tools` → `WikiMcpTools` guard): `wiki.read` / `wiki.query` (served
  direct from the store on the calling node) + `wiki.propose` (enqueues). Over Mycelium's **existing**
  MCP invoke path — schema to `tools/{name}/{node}` for discovery. Public API only; no core fork — the
  mesh-native surface for agents already on the mesh.
- **HTTP gateway** (feature `gateway`, `Wiki::http_router`): `POST /gateway/wiki/{read,query,propose}`
  mounted via `GossipAgent::with_http_routes` — the JSON edge for **non-mesh** callers, spoken by the
  Python (`mycelium.wiki.Wiki`) + TypeScript (`mycelium-ts` `Wiki`) clients. `query` is
  POST-with-predicate (a GET can't carry an attribute map — the blackboard's read precedent).
  `tests/gateway.rs` drives propose → curator-apply → read/query over HTTP.
- **Worked example** (`examples/wiki_chat.rs`): **one template, both use cases** — import documents
  then chat grounded in the wiki. `import` = writer (control plane); `ask`/`chat` = reader (data plane,
  no node). Retrieval is keyword-overlap over the exact curated text (RAG's similarity is the separate
  background layer). Corpora `examples/corpus/{council,org-twin}` = UC2 / UC1.

## Composition

Joined to **Postgres** (typed metrics/structure) + **RAG** (background) by a **shared id namespace**;
`attributes` on a section are **join keys + scope tags**, not computational facets. The wiki is the
*specific / authoritative / maintained-meaning* layer — not a replacement for either.

## Gates

`cargo test -p mycelium-wiki` (data plane) · `--features control-plane` (curator + `tests/failover.rs`)
· `--features llm` (reconcile + semantic-lint wiring, EchoBackend) · `--features gateway`
(`tests/gateway.rs` — the `/gateway/wiki/*` lifecycle) · `tests/access.rs` (the membership-gated broker,
under `control-plane`) · `./mycelium-wiki/ci_smoke.sh` (the worked example, Docker-free) · clippy
`--features control-plane|llm|gateway --all-targets -D warnings`. Wired as the CI **Wiki** job. **Only
open remainder (additive):** the disconnected KV-native section-CRDT variant for the no-external-store
case (design record `docs/design/wiki-concurrent-edit.md`).
