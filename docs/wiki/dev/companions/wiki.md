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
  **Mycelium-agnostic**. `read`/`query`/`write_page`/`list_pages`/`location`. Torn-read-safe for
  readers via **manifest-last** writes + per-object atomicity; `read` is manifest-authoritative (a
  stray unreferenced section is invisible). **Readers go direct to the store, in parallel — no node,
  no curator.** That node-independence is what makes the wiki a *store, not a service*.
- **Control plane** (feature `control-plane`, the Mycelium side): a single elected **curator**
  discovered on the capability ring, the single writer of record.

## Curator election + failover (the recallable role, not the coordinator trap)

`WikiRole` Auto/Curator/Reader. Election mirrors the blackboard's: advertise
`wiki.{group}.candidate` → settle → **lowest candidate id** self-elects `wiki.{group}.curator`, else
become a reader that watches. **Ring-failover:** two consecutive empty `curator` resolves (one refresh
apart — split-brain guard) → re-elect. Because the store is node-independent, **failover transfers
nothing**: a promoted curator resumes against the *same* store and re-drains the *same* proposals. The
litmus: *if the curator vanishes, the wiki stays readable and a new curator self-elects.*

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
  same-section conflict reaches the reconcile as *one* batch held by *one* writer — the single-writer
  dividend: **no CRDT, no lost update**. Tombstones the proposal only after the store write lands.
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
