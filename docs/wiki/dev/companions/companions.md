# dev/companions — crates built on the public API

↑ [dev/](../dev.md)

Each companion depends on `mycelium` **only through its public API** — the composability
proof. Workspace members; scope builds with `-p` (a workspace-wide build pulls `wasmtime`
via wasm-host).

> These pages are maintainer-facing (design + rationale + gates). The **operator** runbook —
> durability/WAL, capability-ring failover, the wiki's node-independent store, teardown — is
> [operations/companions.md](../../../operations/companions.md). The **cross-cutting coordination
> decision** all three share — a ring-elected single-writer made safe by id-fencing / section-CAS,
> deliberately *not* the (CP, quorum-blocking) distributed lock, and the CAP framing (AP, paying in
> weaker consistency not availability) — is
> [design/coordination-approaches.md](../../../design/coordination-approaches.md).

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
  (streamed place-and-probe for models/data) as another; provenance binds the whole entry;
  the probe is *consumed* — a per-round health pass withdraws failing installs (restart ≡
  provisioning is the health protocol). Real-model proof: the coop `model_deploy` demo.
  Security note: wasmtime is this crate's sandbox — keep
  `cargo audit` green on it (RUSTSEC-2026-0188 was found+fixed via audit, Run 28).
- **`mycelium-agentfacts/`** — WS-F/M16 federation edge: self-certified NANDA AgentFacts
  document (superset of the A2A AgentCard), CRDT-assembled domain endpoint, schema
  migrations. PRs #44–#49, #83–#88. Domain positioning:
  [coordinator-free-recursion](../../domain/theory/coordinator-free-recursion.md).
- **`mycelium-reason/`** — the **v3.0 LLM-authoring DX companion** (a *different axis* from the
  coordination crates above — see [pattern-coverage](../../domain/pattern-coverage.md) → the LLM-DX
  axis). Three Tier-3 wedges: ① **capability-routed inference** (`InferenceRouter`: resolve → drop
  opaque → rank by `peer_load` fill → failover — resolution is load-blind, so the routing is a real
  layer, not a byproduct; `serve_model` = model-is-a-prompt-skill `llm/{model}` + attributed
  `llm-meta/{model}`), ② **fleet-reasoning traces** (`TraceRecorder`/`replay`/`narrate` on per-writer
  log substreams `reason/{run_id}/{node}` — a shared stream collides same-ms HLC keys; optional WS2
  audit-chain anchoring under `compliance`), ③ **artifact-aware resume** (`require_model` demand half;
  install half is wasm-host's `model_deploy`). Plus a content-addressed **blob tier**
  (`FsBlobStore`/`MeshBlobStore`/`spawn_blob_server`, ≤ 8 MiB v1) + `/gateway/reason/{blob,trace,route}`
  routes (`route` is the load-aware routing surface, #132). Zero core changes, zero new locks. **Python tier** (separate packages): the
  **`langgraph-checkpoint-mycelium`** `BaseCheckpointSaver` (index rows in KV `ckpt/`/`ckptw/`,
  payloads in the blob tier, cross-node `StateGraph` resume proven in CI) and `mycelium.call_typed`.
  **COMPLETE (PRs #130–#136, 2026-07-08):** the crate + Python tier, the LangGraph example ladder
  (`examples/langgraph/` rungs 0–6 incl. the echo-CI **deploy/reheal flagship** + a router-robustness
  fix it surfaced — live-SWIM filter + fast failover, #134), the repo's first Python CI job, and guide
  chapter 15 + an Ollama-manual real-model variant. Plans: `docs/plans/mycelium-reason.md` +
  `…-examples.md`.
- **`mycelium-guardrails/`** — the **v3.0 structural-guardrails companion** (the second primary;
  *different axis* again — [pattern-coverage](../../domain/pattern-coverage.md) → Structural guardrails).
  *What an agent may do*, one tier-labelled `Policy` → `apply()` compiling to **Tier A** boundary
  (self-imposed prevention) · **Tier B** `AgentPolicy` (self-imposed at state transitions) · **Tier C**
  `authorized_callers` (hard prevention — unauthorized invoke rejected at the provider + the denial
  **sealed** into the tamper-evident chain). `Policy::strength_report()` discloses each clause's tier —
  the design's honesty; the **`prove_denials` verification tool** reconstructs the chain and proves the
  guardrail fired (*provable-stopping*, not global negative proof). Self-imposed (no remote policy
  authority — the chokepoint non-goal). Examples `guardrail_wedge`/`guardrail_fleet`, guide chapter 16.
  PRs #137–#139; zero new locks. Plan: `docs/plans/mycelium-guardrails.md`.

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
