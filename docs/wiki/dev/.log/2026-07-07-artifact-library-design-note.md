# 2026-07-07 — artifact-library: design note + steps 1–5 SHIPPED same day

**Implementation status (end of day): steps 1–5 of the note's sequencing shipped**, commits
`910c1ff`…`22ac02b` — FsLibrarySource + manifest + kind encoding + entry-binding provenance;
ArtifactRuntime/Installed + kind-dispatching Provisioner (async reservations, eligibility,
tripwire); librarian role + resolving MeshArtifactSource; BlobRuntime + RangedArtifactSource +
real loading tiers; honest catalog/mcp_toolgrowth demos (+ the new committed
unit-convert-component fixture) and the llm_agent labelled-simulation decision. 45 crate tests;
lock-order rows 20–22; full coop smoke + make check green. **Open: step 6 (object-store source
via PrefetchingSource — needs a per-node credentials story) and step 7 (async ArtifactSource
revisit).** Ledger entry: [dev/history](../history.md); companion entry updated.

**Step 4b added later the same day** (requirements review with Richard): **resource-aware
eligibility** (design §4.4) — signed `requires: {disk, mem}` in the entry (safety claims inside
provenance; hints stay unsigned), `ResourceProbe` + headroom fraction (default system probe at
0.8), in-flight reservations counted (two 6 GB models can't both pass a 10 GB check),
unmeasurable→permissive, `BlobRuntime` fail-fast disk check. Explicitly rejected: resource
gossip + best-fit ranking — resource-aware self-election *is* the placement algorithm.

Original note (morning), for the record:

A session probing "dynamic component install at runtime" verified a gap in the artifact pipeline
and produced [`docs/design/artifact-library.md`](../../../design/artifact-library.md) (📐 proposed,
not started; indexed in `docs/README.md`, cross-linked from the artifacts runbook, listed in
CLAUDE.md Active work).

**Verified facts (2026-07-07):**

- Only two real `ArtifactSource` impls exist: `InMemorySource` + `MeshArtifactSource` (verified
  cache). **No durable source** — artifact bytes survive only as long as some serving node's
  process; the `installable/` catalogue entry persists in KV regardless (a stale announcement is
  possible).
- **No holder discovery**: `MeshArtifactSource::new` takes a hardcoded provider list; nothing maps
  `ArtifactId` → live holders. The `catalog` demo knows the publisher's node-id a priori.
- **Doc drift**: `mycelium-wasm-host/src/artifact.rs:140` claims "v0 ships in-memory + local-dir" —
  the local-dir source does not exist. (Fix rides item 1 of the design's sequencing.)
- The ops guide (`operations/artifacts.md` §cross-cluster) already *prescribes* the shared-store +
  bridge/librarian pattern — so the note fills a documented-but-unimplemented hole, not a new
  design direction.
- Demo caveats worth remembering: `catalog`/`provisioning` origin bytes are `include_bytes!`
  (build-time constant, CI convenience — the documented flow is runtime `fs::read`);
  `mcp_toolgrowth` is dynamic *activation* of compiled-in tool logic, not code arrival;
  `llm_agent`'s install-progress tiers are simulated.

**The design (invariants L1–L5):** catalogue never names a location (content address only);
library = origin tier, never a mandatory read path (peers verify identically → interchangeable);
librarian = a role (a node running `serve_artifacts` over a durable source + one
`artifact/librarian` capability — **not** per-hash ads, which would flood the cap namespace);
detection-not-prevention for unresolvable entries; substrate stays kind-agnostic (all changes
app-layer, core untouched).

**Revised same day (v2), after requirements review with Richard:**

- **Library format = blobs + manifest** (serialized `InstallableEntry`s, signed at
  publish-to-library time — publisher keys stay in CI). Librarian sync is **one-way,
  signature-scoped** (manifest → KV; publishes new, tombstones removed, *only* own-signer
  entries — never clobbers another publisher).
- **Cardinality decision:** one library per *system*, not per group — principle: *a store's
  cardinality follows the scope of what it stores* (wiki corpus = group knowledge → per-group;
  artifacts = cluster-visible competence → per-system). Same control-plane/data-plane pattern as
  the wiki either way; per-group possible via the wiki's access-broker template.
- **Artifact kinds + runtime generalization** (the big one): `ArtifactKind` (WasmComponent,
  Blob/model, …) in a **clean-slate versioned entry encoding** (leading format-version byte +
  explicit kind byte — the installable catalogue has no field deployments, confirmed by Richard
  2026-07-07, so no compat shims; a v1 flag-byte-bitfield idea was rejected as unnecessary
  cleverness). `ArtifactRuntime`/`Installed` traits;
  `Provisioner` gains a kind registry, eligibility checks (kind + size budget) before
  self-election, and async install reservations (`Installing → Live → Withdrawing`).
  **`WasmHost` is unchanged — it becomes the engine inside one runtime.** The demand/presence/
  shed loops in `provision_round` were already kind-agnostic.
- **Large artifacts (LLM/ONNX):** every node pulls **direct from the store, async, chunked** —
  no group-leader routing (would overload it; excluded by L3). Per-node read creds + the same
  `EgressPolicy` gate as LLM backends; peer bulk-serving is an optimization, never a route.
  `llm_agent`'s simulated percent tiers become real progress from real bytes.
- **Examples/tests promoted to acceptance criteria** — the honest-install matrix: `catalog`
  loses `include_bytes!` (runtime `FsLibrarySource` + publisher-dies act), `mcp_toolgrowth`
  gains real code arrival (activation kept as a labelled contrast), `llm_agent` de-simulated.
- Open question deferred: crate naming (`mycelium-wasm-host` will undersell its contents).
