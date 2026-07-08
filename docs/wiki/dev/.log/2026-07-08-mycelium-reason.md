# 2026-07-08 — ingest: mycelium-reason shipped (Tier-3 wedges + Python tier)

The first *built* v3.0 deliverable. `mycelium-reason` turned from PROPOSED to shipped-and-tested for
its Tier-3/1/2 tranche across two CI-green PRs; `mycelium-guardrails` (the other v3.0 primary) stays
PROPOSED.

**Preceded by a code-verified pre-implementation reassessment** (five bindings written into
`docs/plans/mycelium-reason.md`, 2026-07-08 addendum). Two corrected earlier overstatements in the
plan's own 2026-07-07 addenda:
- The attributed `cap/{node}/llm/inference` convention **did not exist** — `model_deploy` advertises a
  plain `llm/storyteller` with no attributes. Convention now defined: **model = prompt skill**
  (`llm/{model}`) + a parallel attributed `llm-meta/{model}` ad (second ad avoids LWW-churn against the
  skill's own persist task).
- Capability **resolution is load-blind** — `resolve` ranks by freshness/attributes/locality only;
  opacity/load influence routing only indirectly (evaporation) or via `suggest_leader*`. So wedge ① is a
  real routing layer, not a byproduct.

**PR #130 — the crate** (public-API-only companion, no `mycelium-wasm-host` dep): ① `InferenceRouter`
(resolve → drop opaque → rank by `peer_load` fill → failover) + `serve_model`; ② `TraceRecorder`/`replay`/
`narrate` on the log overlay, optional WS2 audit-chain anchoring (`compliance`); ③ `require_model`
(demand half). Plus the content-addressed **blob tier** (`FsBlobStore`/`MeshBlobStore`/`spawn_blob_server`,
SHA-256 verify-on-read, verified peer fetch, ≤ 8 MiB v1) + `/gateway/reason/{blob,trace}` routes.

**Implementation caught a real plan error:** a single shared trace stream `reason/{run_id}` collides
same-millisecond HLC keys across writers (the HLC's 16-bit per-node logical counter → two nodes both mint
`(ms,0)` → identical `log/…/{hlc:016x}` keys → LWW drops a record). Fixed with **per-writer substreams**
`reason/{run_id}/{node}`, merged on HLC at replay. Binding #3 in the plan corrected to match.

**PR #131 — the Python tier** (Tiers 1+2): `langgraph-checkpoint-mycelium` (a `BaseCheckpointSaver` —
index rows in gossiped KV `ckpt/`/`ckptw/` with metadata inline for payload-free `list`, payloads in the
blob tier one-blob-per-channel-value for free dedup; sync + async; **cross-node `StateGraph` resume proven
in CI**) and `mycelium.call_typed` (through-the-mesh pydantic validation + feedback retry, `typed` extra).
The repo's **first Python CI job** (`python-sdk`: `reason_node` example → two-node mesh → 14 pytest). A
checkpointer edge exposed + fixed the crate's empty-blob path (typed `None` → zero bytes = `SHA-256("")`;
empty fetch reply = miss, so `MeshBlobStore::get` answers it from the address alone).

**Reserved claims:** KV `ckpt/`·`ckptw/`·`log/reason/`, capability `reason/blob-cache`, RPC
`reason.blob.fetch` (added to `docs/guide/building-on-mycelium.md`). Zero new locks (no lock-order rows).

**Pages updated:** `dev/history.md` (ledger entry), `domain/pattern-coverage.md` (the LLM-DX axis — the
"expressible until tested" caveat discharged for what shipped; remaining: conversation memory, run-level
evals, harder Tier-3 demos with a real LLM backend + chunked blob transfer). Open: the `mycelium-reason`
crate-naming question, shared with the artifact library.
