# dev/companions — crates built on the public API

↑ [dev/](../dev.md)

Each companion depends on `mycelium` **only through its public API** — the composability
proof. Workspace members; scope builds with `-p` (a workspace-wide build pulls `wasmtime`
via wasm-host).

- **[tuple-space.md](tuple-space.md)** — `mycelium-tuple-space/`: pull-based pipeline buffer
  (Linda-style lanes). The load-bearing artifact for Paper 2a's pull-vs-push argument.
- **[blackboard.md](blackboard.md)** — `mycelium-blackboard/`: content-routed shared working
  memory (`claim(predicate)`).
- **[wiki.md](wiki.md)** — `mycelium-wiki/`: group-scoped, LLM-curated wiki — the durable, curated
  third primitive (a **store, not a service**: node-independent store + a recallable curator). Build
  phases 1–5 shipped 2026-07-03.
- **`mycelium-wasm-host/`** — WS-E code mobility: the coordinator-free
  requirement→resolve→pull→advertise→serve→self-heal loop, Ed25519 provenance, mesh artifact
  pull, gossiped catalog, fuel limits (restart ≡ provisioning). PRs #32–#42; runbook
  `docs/operations/artifacts.md`. **Extended 2026-07-07 (artifact library, steps 1–5 —
  `docs/design/artifact-library.md`):** durable `FsLibrarySource` + signed manifest, the
  **librarian** role (serve + `artifact/librarian` cap + signature-scoped manifest→KV reconcile),
  capability-resolved pulls (`MeshArtifactSource::resolving`), and the **kind/runtime
  generalization** — `ArtifactRuntime`/`Installed` with `WasmHost` as one engine and `BlobRuntime`
  (streamed place-and-probe for models/data) as another; provenance binds the whole entry.
  Security note: wasmtime is this crate's sandbox — keep
  `cargo audit` green on it (RUSTSEC-2026-0188 was found+fixed via audit, Run 28).
- **`mycelium-agentfacts/`** — WS-F/M16 federation edge: self-certified NANDA AgentFacts
  document (superset of the A2A AgentCard), CRDT-assembled domain endpoint, schema
  migrations. PRs #44–#49, #83–#88. Domain positioning:
  [coordinator-free-recursion](../../domain/theory/coordinator-free-recursion.md).

Both tuple-space and blackboard implement the **exactly-once-effect contract** — the shared
artifact is the *contract*, not code (`docs/design/exactly-once-effect.md`; a shared overlay
was examined and declined-with-evidence).

## The coordination-primitive taxonomy

The public-API coordination primitives form one axis — *how a consumer finds what it needs*:
tuple-space routes by lane **position** (transient, blocking `take`), blackboard by content
**predicate** (transient, competitive `claim`). The **durable** slot is filled by
**[`mycelium-wiki`](wiki.md)** — a group-scoped LLM-curated wiki (curated, compounding, re-read), the
long-term-memory sibling of the blackboard's working memory. **Build phases 1–5 shipped 2026-07-03**
(page: [wiki.md](wiki.md)); the design shape below is now the implemented one. Plan:
`docs/plans/mycelium-wiki.md`.
**Control-plane / data-plane:** the corpus is **not** in gossiped KV —
it lives in a **node-independent, pluggable store** (shared FS dir / S3 / doc store); a group node
runs a **curator** service that serialises writes, runs the LLM ingest/lint, and **brokers access**,
while group agents **read the store directly, in parallel**. Mycelium is the control plane — curator
election + ring-failover, the store-location pointer, the small evaporating proposal queue in KV, the
MCP tool — never the storage. This is the wiki pattern's *native* shape (files + an LLM curator +
direct reads, exactly how this very `docs/wiki/` works), so the concurrent-prose-merge problem
dissolves into single-writer-curator + the store. The earlier KV-native section-CRDT is retained as
the **disconnected / no-external-store variant** (design record `docs/design/wiki-concurrent-edit.md`
§1–§2); the identity model + curator state machine carry over. Composes with Postgres (metrics) + RAG
(background) by a shared id namespace — it is the specific/authoritative/maintained layer, not a
replacement for either.
