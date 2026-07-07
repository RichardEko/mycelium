# The durable artifact library — origin tier, artifact kinds, and node-level install lifecycle

**Status:** ✅ **adopted & implemented** (proposed, revised, and shipped 2026-07-07: sequencing
steps 1–6 landed in commits `910c1ff`…; §4.4 resource-aware eligibility added and shipped the
same day; step 7 resolved **declined-with-evidence** — see §10. Open remainder: the crate-naming
question only). Decision record for closing the gap between what
[`operations/artifacts.md`](../operations/artifacts.md) *prescribes* ("point `serve_artifacts` at a
common object store — S3 / OCI registry / artifact server") and what `mycelium-wasm-host` *ships*
(no durable `ArtifactSource`, no holder discovery, one hard-coded artifact kind). Nothing here
changes core or the wire protocol — the work is additive on the `ArtifactSource` seam plus one
app-layer generalization inside `mycelium-wasm-host`.

The record answers three questions:

1. **Where do artifact bytes durably live**, and how does a pulling node find a holder — without
   reintroducing a registry server the cluster depends on?
2. **What is "install" for artifacts that are not WASM components** (an LLM, an ONNX net, a data
   pack) — and how does a node manage the lifecycle of each kind?
3. **How do large artifacts reach every member of a group** without funnelling downloads through
   any single node?

---

## 0. Current state (verified against code, 2026-07-07)

The install pipeline separates *announcement* from *bytes*:

- **Catalogue** — an `InstallableEntry` per artifact in gossiped KV under `installable/{ns}/{name}/{hex}`
  (`mycelium-wasm-host/src/catalog.rs:25`). The entry holds the declared-provide `Capability`, a
  32-byte **content address** (`ArtifactId`), cost hints, and optional Ed25519 provenance
  (`catalog.rs:31`). It contains **no location** — no URL, path, or holder node-id — and **no
  kind**: every entry is implicitly a WASM component. Every node holds the whole catalogue; there
  is no registry server.
- **Bytes** — fetched through the `ArtifactSource` trait (`artifact.rs:141`, one *synchronous*
  method: `fetch(&ArtifactId) -> Option<Bytes>`). Exactly two real implementations exist:
  `InMemorySource` (`artifact.rs:149`, a heap map in the holding process) and
  `MeshArtifactSource` (`mesh_source.rs:71`, pulls from peers **listed at construction** via the
  `artifact.fetch` RPC, verifies on arrival, caches in memory; sync `fetch` forces an async
  `prefetch` two-step — `mesh_source.rs:67–70`).
- **Install** — `Provisioner::bring_live` (`provisioner.rs:168`) hard-codes the whole step:
  `WasmHost::provision` (pull + verify + instantiate, `host.rs:236`) → register an RPC serve loop
  on `cap_invoke_kind(ns, name)` → advertise the capability. One kind, one serving model, one
  synchronous call inside the `provision_round` tick.

**The gaps this record addresses:**

1. **No durability.** Bytes live only in serving processes' memory. If every node holding
   artifact X dies, X is gone — while its catalogue entry persists in KV, advertising an artifact
   nobody can produce. The demos sidestep this with `include_bytes!` (a build-time constant,
   `examples/coop/src/bin/catalog.rs:33`), which cannot demonstrate installing code that didn't
   exist at build time.
2. **No holder discovery.** Nothing maps `ArtifactId` → live holders. `MeshArtifactSource::new`
   takes a hardcoded provider list (`vec![publisher_id]` in the demo, `catalog.rs:112–113`).
3. **One artifact kind.** "Install" is defined as "instantiate in wasmtime." An LLM or ONNX
   model is not instantiable by `WasmHost`; it is *placed* and served by a node-local runtime.
   The pipeline has no vocabulary for that.
4. **Install is synchronous and unreported.** `bring_live` blocks a tick; a multi-GB pull cannot
   run that way, and there is no real progress signal (the `llm_agent` example's `loading →
   ready` percent tiers are a *simulation* — a counter loop, no bytes).
5. **Examples overstate what ships.** `mcp_toolgrowth` is dynamic *activation* (the tool logic
   was compiled into the tool-host; demand switches it on — no new code arrives);
   `llm_agent`'s install progress is simulated; only `catalog` moves real bytes between real
   processes, and its origin is `include_bytes!`.

---

## 1. Invariants to preserve

- **L1 — the catalogue never names a location.** Content address only. Location-addressing
  (URLs, bucket keys, holder ids in the entry) would make holders non-interchangeable and bind
  the catalogue to a topology; the hash is what lets any holder — librarian or peer cache —
  serve identical, verifiable bytes. (The deliberate divergence from OSGi/OBR, which bakes
  bundle URLs into resolver metadata.)
- **L2 — the library is an origin tier, never a mandatory read path.** A node must be able to
  provision from any live holder; a peer-served copy re-hashes identically to the library copy.
  Making every node read one store directly *as the only path* would reintroduce a soft
  coordinator — but see §5: for large artifacts, direct store pulls are the *baseline* and peer
  serving the *optimization*, never the reverse requirement.
- **L3 — a librarian is a role, not a daemon.** "The library" is a regular node running
  `serve_artifacts` over a durable `ArtifactSource`, discovered like any other capability.
  Multiple librarians may front the same store; none is elected or special. **No download ever
  routes through a leader** — there is no leader in this design at all.
- **L4 — detection, not prevention.** A catalogue entry with no reachable holder, or with a kind
  this node cannot host, is a *detectable, counted* condition — never one the substrate prevents
  or garbage-collects on its own.
- **L5 — the substrate stays kind-agnostic.** Artifact kinds, runtimes, and lifecycle live in
  the app-layer companion (`mycelium-wasm-host` today), keyed off the entry — core Mycelium
  never learns what a "model" is. Library, not platform.

---

## 2. The library format: blobs + manifest

A library is a durable store containing **content-addressed blobs plus a manifest** — the
manifest is the library's own catalogue file: a serialized list of `InstallableEntry`s (they
already carry exactly the right fields: declared-provide, content address, kind, cost hints,
signature). The manifest is what makes a library **self-describing and portable**: a directory
of hex-named blobs says nothing about what each provides.

- **Signing happens at publish-to-library time** (CI signs the entry when it adds the blob), so
  the publisher key lives in CI/KMS only — librarian nodes hold no signing keys; they serve
  pre-signed entries.
- **Sync is one-way and signature-scoped: manifest → gossip KV.** The manifest is the *library's*
  source of truth; the gossiped `installable/` catalogue is the *cluster's view* — and the KV
  catalogue stays open (any node may `publish_installable` its own entries; a dev node
  publishing a one-off is legitimate). When the librarian detects a manifest change (mtime/hash
  poll — an external concern, as is updating the manifest itself), it publishes new entries and
  tombstones removed ones — **but only entries carrying the library's publisher signature**, so
  a library removal never clobbers another publisher's announcement.
- **Restart = re-publish.** A restarted librarian re-drives the KV catalogue from the manifest;
  no external re-publish step, no state transfer.
- Every group's "current view of the library" then costs nothing extra: the KV catalogue *is*
  the always-current view on every node, kept fresh by the librarian's diff and delivered by the
  same anti-entropy as all KV state.

## 3. Cardinality: one library per system, not per group

**Decision: one library per Mycelium system (cluster) is the default.** Per-group libraries are
an allowed *policy* choice, not the architecture.

The forcing function is the substrate invariant that drove the `mycelium-wiki` pivot: **KV floods
every node unconditionally — group scope is access/namespacing, never replication isolation.** A
per-group catalogue would still replicate every group's entries to every node; nothing is
isolated, only prefixed. And artifacts are content-addressed — the same bytes in two group
libraries are provably identical, so duplication buys nothing.

The wiki comparison, stated at the right altitude, *supports* this rather than contradicting it.
The shared principle is: **a store's cardinality follows the scope of what it stores.**

| Store | Content | Scope of content | Cardinality |
|---|---|---|---|
| `mycelium-wiki` corpus | group knowledge ("competence is a capability, knowledge is not") | group-private | per **group** |
| artifact library | competence-in-waiting (an artifact exists to become a capability, and capabilities are cluster-visible) | cluster-visible | per **system** |

What *is* identical in both is the pattern: node-independent external store · a Mycelium
control-plane role in front of it (curator / librarian) · data-plane reads that never route
through that role · role failover that transfers nothing. If a group ever genuinely needs a
private library (a sovereign sub-fleet, restricted models), the wiki's membership-gated access
broker is the template to copy — namespaced entries + per-group trusted-publisher keys. Where a
group must not *run* an artifact, the control is the provisioner's provenance policy
(`require_provenance`), not library partitioning.

## 4. Artifact kinds and node runtimes (the generalization)

"Install" is not one operation. The pipeline gains a **kind** axis, and the node gains a
**runtime registry** — `WasmHost` becomes the engine behind *one* runtime, not the definition of
install.

### 4.1 `ArtifactKind`

```rust
#[non_exhaustive]
pub enum ArtifactKind {
    WasmComponent,   // 0 — pull → verify → instantiate (wasmtime); RPC serve loop
    Blob,            // 1 — pull → verify → place (a file a local runtime consumes: LLM/ONNX
                     //     weights, data pack); capability advertisement is probe-gated
    // future: OciImage, SkillPackage, …
}
```

**Encoding: clean slate.** The installable catalogue has no field deployments (confirmed
2026-07-07), so the entry encoding is redesigned outright rather than evolved — no
compatibility shims. `InstallableEntry::encode` (`catalog.rs:98`) is replaced with an explicit,
versioned layout:

```text
[1B format-version = 1][1B kind][32B artifact][8B size][8B est_install][1B signed]
[32B signer + 64B sig, if signed][Capability bytes]
```

One leading version byte buys all future evolution honestly (a decoder rejects versions it
doesn't know — a counted event, L4), and `kind` is a first-class byte, not bits scavenged from
a flag. (A v1 draft of this note proposed a flag-byte bitfield for backward compatibility;
rejected as unnecessary cleverness once clean-slate was confirmed — see §7.) The `installable/`
encoding is app-layer, owned by this crate — not the core wire protocol.

**Provenance binds the whole entry, not just the bytes.** The clean slate also fixed a
weakness in the original scheme: the Ed25519 signature covered *only* the content address, so a
signed artifact could be re-published under a different capability or kind with provenance
still verifying (re-labeling `llm/summarize` as `admin/root-shell`). The signature now covers a
domain-separated message over `version ‖ kind ‖ artifact ‖ capability`
(`InstallableEntry::provenance_message`). Cost hints stay outside the signature — they are
ranking hints, not security claims, and may be updated without re-signing.

### 4.2 `ArtifactRuntime` — install + lifecycle per kind

```rust
#[async_trait]
pub trait ArtifactRuntime: Send + Sync {
    fn kind(&self) -> ArtifactKind;
    /// Pull (via the provided source), verify, and bring the artifact live.
    /// Long-running; reports progress (bytes_fetched / bytes_total) via `progress`.
    async fn install(&self, entry: &InstallableEntry, source: &dyn ArtifactSource,
                     ctx: RuntimeCtx, progress: ProgressFn) -> Result<Box<dyn Installed>, InstallError>;
}

/// A live installation — the node-level lifecycle handle.
pub trait Installed: Send {
    /// Health probe: is the capability actually servable right now?
    fn probe(&self) -> bool;
    /// Cooperative teardown: stop serving, clean up placed bytes/instances.
    fn uninstall(self: Box<Self>);
}
```

- **`WasmComponentRuntime`** wraps today's behaviour unchanged: `WasmHost::provision` →
  `rpc_rx(cap_invoke_kind(ns, name))` serve loop → advertise. `probe()` is trivially true while
  the serve task lives; `uninstall` = today's `withdraw` (drop cap reg, abort serve task).
- **`BlobRuntime`** implements the model case: chunked pull → verify → **place** at a configured
  path (or hand to a local runtime — an Ollama pull, an onnxruntime session) → advertise the
  declared-provide **probe-gated**: the capability is asserted only while the runtime actually
  answers (exactly the `llm_agent` Ollama-probe pattern, now attached to real bytes). `uninstall`
  deletes the placed file / unloads the model.
- **Progress is real state, not a simulation.** During `install`, the runtime advertises the
  loading tier (`{ns}/loading` with a percent attribute, re-asserted — the existing `llm_agent`
  tier convention) driven by actual `bytes_fetched / bytes_total`. The simulated percent loop in
  `llm_agent` becomes this, for real.

### 4.3 The `Provisioner` becomes kind-dispatching

- Gains a registry: `HashMap<ArtifactKind, Arc<dyn ArtifactRuntime>>`, populated by the embedder
  (a node with no GPU registers no model runtime — **eligibility is node-local truth**).
- **Self-election gains eligibility checks**: skip an entry when (a) no runtime is registered
  for its kind, or (b) its `size_bytes` exceeds this node's configured install budget. Both are
  silent non-participation (some *other* node elects), plus a tripwire counter for "entry
  resolvable by no runtime on any live node" (L4).
- **`bring_live` becomes an async reservation.** Today it blocks the tick and inserts into
  `hosted` only on success. With multi-GB installs, `provision_round` must instead insert an
  `Installing` reservation *first* (so subsequent rounds don't double-start), spawn the
  runtime's `install` as a background task, and transition the entry to `Live(Box<dyn
  Installed>)` on completion / remove it on failure. The `Hosted` map becomes a small state
  machine: `Installing → Live → Withdrawing`.
- The **shed path** (`max_providers`) and **supervision** loops are already kind-agnostic and
  survive unchanged — `withdraw` delegates to `Installed::uninstall`.
- **The probe is consumed, not just exposed** (wired 2026-07-07, coverage review): every
  `provision_round` opens with a health pass — a `Live` install whose `Installed::probe()`
  fails is withdrawn (ad retracted, runtime torn down), and the demand/presence passes
  reinstall it once the retracted ad clears the local view. Probe-gated advertisement thus
  degenerates into the existing restart ≡ provisioning machinery instead of needing its own
  health protocol. Probes run under the hosted lock: cheap and non-blocking by contract
  (file-exists, a cached health bit — slow checks live in a background task that flips a
  flag the probe reads).

### 4.3.1 Composite deployments: the profile is an artifact too (added 2026-07-07)

A model deployment has two governed halves: the **weights** and the **profile** (system
prompt, parameters — the configuration a runtime loads the weights *with*). Both travel the
library as ordinary signed Blob entries; the profile references the weights **by content
address** (`FROM artifact:{hex}` in the demo's Modelfile dialect), never by path or node.
Activation resolves the reference against the local placement dir — content-addressed
placement makes the weights' path derivable from their hash. **There is no dependency
resolver** (the M15 one-hop contract stands): a profile whose referenced artifact isn't
placed yet fails activation, drops its reservation, and a later round retries — the ordering
mechanism *is* restart ≡ provisioning. Updating a prompt/parameter set = publishing a new
profile artifact (new content address, re-signed); the weights entry is untouched. Proven
live by the `model_deploy` demo, which asserts `ollama show` carries the SYSTEM prompt that
arrived in the signed profile.

### 4.4 Resource-aware eligibility (added 2026-07-07, requirements review)

A node must not elect to install an artifact it cannot fit. The static byte budget (§4.3) is an
operator *guess*; the node also needs a view of its **actual free resources**:

- **Requirements travel in the entry, signed.** `InstallableEntry.requires` (`disk_bytes` at the
  placement root, `mem_bytes` once live; `0` = undeclared) is publisher-declared metadata —
  only the publisher knows that a 4 GB quantised model wants 5 GB+ of RAM activated. Unlike the
  cost *hints* (ranking inputs, deliberately unsigned), requirements are **install-safety
  claims** and sit inside `provenance_message`: a tampered requirement (mem lowered to 0) would
  make trusting nodes OOM themselves, so it must break the signature.
- **The node side is a probe + a headroom fraction.** `ResourceProbe` (available memory;
  available disk at a path — implementations measure the nearest existing ancestor of a
  not-yet-created placement dir) with a `sysinfo`-backed default, and a policy on the
  provisioner: eligible only if `requirement ≤ headroom × available − reserved`, headroom
  clamped to `(0, 1]`, default `0.8`. Disk is measured at the kind's runtime `resource_root()`
  (`None` for memory-resident kinds like WASM components — then only memory is checked).
- **In-flight installs count.** The `Installing` reservation carries the entry's declared
  requirements; eligibility subtracts the sum of in-flight reservations — two 6 GB models must
  not both pass a 10 GB-free check. Live installs need no virtual accounting: their placed
  bytes and activated memory are visible to the probe directly.
- **Unmeasurable = permissive, undeclared = unchecked** (detection, not prevention). Blocking
  every install on a platform the probe can't read would freeze a fleet silently; the runtime's
  real failure is the signal, and the tripwire counter (`ineligible_skips`) records every
  resource-based non-election.
- **This is the fleet's placement algorithm — deliberately not a scheduler.** Eligibility is
  node-local, binary, and *silent*: a node that can't fit simply doesn't elect, one that can
  does (herd-damped by the existing probabilistic self-election), and if *no* live node can,
  the tripwire family says so. Two things are explicitly rejected: **gossiping per-node
  resource state** (nobody needs global resource knowledge — the stigmergy lesson: model load
  as the node's *own* backlog) and **best-fit ranking** (contended capacity self-corrects via
  the shed band; ranking would require exactly the cross-node comparisons we refuse).
- **Time-of-check honesty:** disk can fill between election and rename. `BlobRuntime` adds a
  fail-fast absolute check before starting a multi-GB pull, but correctness never depended on
  it — a mid-pull ENOSPC fails cleanly (complete-or-absent), the reservation drops, a later
  round retries, possibly on a node that fits.

## 5. Transport: large artifacts pull direct, async, per-node

For an LLM or ONNX net, the mesh RPC fetch is out (`artifact.fetch` rides the gossip frame,
`MAX_FRAME_BYTES` = 10 MiB). The large-artifact path:

- **Every member node pulls directly from the durable store, asynchronously, on its own
  connection.** No group leader, no relay node — routing downloads through any single node would
  overload it and reintroduce a coordinator (L3). Concretely: the object-store/fs source is
  handed to each node's provisioner; N nodes = N independent pulls.
- **Chunked/ranged transfer with incremental verification** (hash the stream, compare at the
  end; a failed chunk retries without restarting). Progress feeds §4.2's loading tier.
- **Per-node credentials and egress.** Direct pulls mean read credentials on every pulling node
  — that is the accepted price of no-intermediary (the v1 draft's "credentials on librarians
  only" holds only for the small-artifact mesh path). Every outbound pull is gated by the node's
  own `EgressPolicy` (`config.egress.permits_url`) — the same WS3 gate the LLM backends pass
  through before dispatch.
- **Peer serving is an optimization, never a route.** A node that has pulled may serve peers over
  the **bulk transport** (`ServiceHandle::bulk_serve` — HTTP staging, built for
  beyond-frame-cap payloads), so 100 nodes wanting the same 4 GB can swarm instead of issuing
  100 store egresses. Opt-in, load-shedding, and its absence must never block an install (L2).

Small artifacts (typical WASM components) keep the existing mesh RPC path unchanged.

## 6. Holder discovery

- A node serving artifacts durably advertises **one** capability: `artifact/librarian` (normal
  30 s heartbeat — evaporates with the node). A puller resolves
  `CapFilter::new("artifact", "librarian")` and tries holders in order; `pull_artifact` already
  returns `None` cleanly on a miss, so misses cost one RPC.
- **Rejected: per-artifact advertisement** (`serves/{hash}`) — a librarian fronting hundreds of
  artifacts would flood the capability namespace with per-hash heartbeats. The librarian
  population is small, misses are cheap, and peer caches absorb hot-artifact load.
- `MeshArtifactSource` gains a constructor taking a `CapFilter` instead of `Vec<NodeId>`,
  resolving providers at `prefetch` time. The static-list constructor stays for tests and fixed
  topologies.

### End-to-end flow

```text
publish:   CI → sign entry → add blob + manifest row to the library store
           librarian: manifest diff → publish_installable / tombstone (own-signer entries only)

install:   resolve catalogue (from_kv) → provenance check → kind/size eligibility check
           small artifact: resolve artifact/librarian caps → mesh prefetch (verified)
           large artifact: direct chunked pull from the store (per-node creds, egress-gated)
           runtime.install(kind) → loading tier (real %) → probe-gated advertise → serve
```

---

## 7. Alternatives considered

| Alternative | Why rejected |
|---|---|
| Nodes read the object store as the *only* path (no mesh tier) | Every cold-start depends on one external service — a soft coordinator/SPOF (L2). Kept as the *baseline for large artifacts* where the mesh frame cap rules RPC out — but peer bulk-serving remains available, and small artifacts keep the mesh path. |
| Route large downloads through a group leader / relay | Overloads that node, reintroduces a coordinator, and adds a failure domain for zero benefit — the store already serves ranged reads concurrently. Explicitly excluded (L3). |
| Location (URL/holder-id) in `InstallableEntry` | Violates L1: binds the announcement to a topology, makes holders non-interchangeable, entries go stale on re-homing. The hash names the bytes everywhere at once. |
| Per-artifact holder capabilities (`serves/{hash}`) | Capability-namespace flood at library scale (§6). |
| Per-group libraries by default | KV floods every node — no isolation gained, only prefixes; content-addressed artifacts dedup to nothing; group *execution* control belongs to provenance policy. Per-group remains a policy option via the wiki's access-broker pattern (§3). |
| A kind field via new KV prefix (`installable-v2/`) | Splits the catalogue into two namespaces every resolver must scan forever; a version byte inside the value (§4.1) evolves in place. |
| Kind smuggled into the signed-flag byte as a bitfield (v1 of this note) | Was motivated purely by backward compatibility; with no field deployments (confirmed 2026-07-07) it is unnecessary cleverness. An explicit version byte + kind byte is self-describing and boring — correctly so. |
| Async `ArtifactSource` trait now | Right long-term shape, wrong sequencing: a breaking trait revision to unblock what the prefetch pattern already delivers. Chunked pulls live inside runtime `install`; the trait decision is taken later, on its own merits (`mesh_source.rs:70`). |
| One kind-specific provisioner per crate (WasmProvisioner, ModelProvisioner…) | Duplicates the demand/presence/shed loops — which are already kind-agnostic — per kind. The loops are the invariant machinery; only install/lifecycle varies. Hence one provisioner + a runtime registry (§4.3). |
| A registry server (Docker-registry style) | The line the feature was built to avoid — see the ops guide's "There is no registry server." A librarian is optional on the read path and interchangeable with any peer cache. |
| Gossip per-node resource state / best-fit install ranking | Resource-aware **self-election is already the placement algorithm** (§4.4): local truth + silent non-participation needs no global resource knowledge, and contention self-corrects via herd damping + the shed band. A resource gossip layer plus ranking is a scheduler wearing a costume. |

---

## 8. Failure modes & litmus tests

- **Library store unreachable, peers hold the bytes** → small artifacts provision from peer
  caches (litmus: kill the librarian after one peer has pulled; a second peer must still
  provision). Large artifacts: peer bulk-serving covers it if opted in; otherwise installs of
  *uncached* large artifacts pause — and nothing else does.
- **All holders of X gone** → `prefetch`/pull exhausts holders; the provisioner skips the round;
  a tripwire counter (`artifact_unresolvable_total`) records a catalogue entry that resolved but
  could not be fetched (L4). Recovery is a librarian returning — the entry is still correct.
- **Kind nobody can host** → entries whose kind has no registered runtime on any live node:
  same tripwire family, distinct label (`artifact_kind_unhostable_total`).
- **Crash mid-install (large blob)** → the `Installing` reservation dies with the process
  (in-memory); on restart the same resolve-and-pull path re-fires (restart ≡ provisioning).
  Placed partial files are the runtime's cleanup concern: `BlobRuntime` writes to a temp path
  and renames on verify — the placed file is complete-or-absent (manifest-last discipline,
  same as the wiki's `FsStore`).
- **Lying holder / corrupted chunk** → verified against the content address (whole-artifact
  hash; chunk retry on ranged pulls); a mismatch is a miss, next holder is tried.
- **Manifest removal races an in-flight install** → the tombstone reaches the installer's KV
  after `provision_round` resolved the entry: install completes and serves; the *next* round
  no longer sees the entry, and supervision won't re-provision it. Acceptable — detection, not
  prevention; an operator wanting hard revocation revokes the capability/provenance, not the
  catalogue row.

---

## 9. Code impact map

All changes live in `mycelium-wasm-host` (+ examples/tests). Core Mycelium: **zero changes** (L5).

| Where | Change |
|---|---|
| `artifact.rs` | **New `FsLibrarySource`** (dir of hex-named blobs; sync `fetch` = file read; `store(&bytes)` write-verify helper). **`ArtifactKind` enum.** **`PrefetchingSource`** wrapper (async prefetch → verified in-memory cache → sync `fetch`), generalising the pattern `MeshArtifactSource` proved. Fix the doc drift at `:140` ("v0 ships in-memory + local-dir" — becomes true). |
| `catalog.rs` | `InstallableEntry.kind` + the clean versioned encoding (§4.1 — replaces the current layout outright; no compat shims). **Manifest module**: serialize/deserialize entry lists; diff(old, new) → (publish, tombstone) sets; sync scoped to entries matching the library's signer (§2). |
| `mesh_source.rs` | `serve_artifacts` gains an opt-in **`artifact/librarian` advertisement**. `MeshArtifactSource` gains the **resolving constructor** (`CapFilter`-based, resolved at prefetch). Bulk-transport serve/pull path for beyond-frame-cap artifacts (peer-serving optimization, §5). |
| `provisioner.rs` | **The generalization.** `ArtifactRuntime` + `Installed` traits (§4.2); runtime registry on `Provisioner`; `bring_live` → async reservation + kind dispatch (`Hosted` becomes `Installing → Live → Withdrawing`); eligibility checks (kind registered, size budget) before self-election; `withdraw` delegates to `Installed::uninstall`; tripwire counters. The demand/presence/shed loops in `provision_round` are **unchanged** — they were always kind-agnostic. |
| `host.rs` | **Unchanged.** `WasmHost` becomes the engine inside `WasmComponentRuntime`; `provision`/`provision_for` keep their signatures (the runtime calls them). The current `serve_loop` and `cap_invoke_kind` move into (or are wrapped by) `WasmComponentRuntime`. |
| New: `runtime/` module | `WasmComponentRuntime` (today's behaviour, relocated), `BlobRuntime` (place + probe-gate + temp-file/rename discipline + fail-fast disk check), progress plumbing to the loading tier; `ArtifactRuntime::resource_root()` for the §4.4 disk check. |
| New: `resources.rs` | `ResourceProbe` trait + `SystemResourceProbe` (sysinfo-backed memory + per-mount disk); the provisioner's resource policy (`set_resource_policy(probe, headroom)`, default system probe at 0.8) and in-flight reservation accounting (§4.4). `InstallableEntry.requires` (signed) carries the publisher-declared footprint. |
| Examples | `catalog`: origin becomes a runtime-read `FsLibrarySource` directory (delete the `include_bytes!`); publisher dies after a librarian mirrors; a late-joining installer still provisions. `mcp_toolgrowth`: the converter **arrives** as a WASM component pulled from the library, provisioned, *then* bridged (`register_mcp_tool` + `tool/` cap); current activation behaviour retained as an explicitly-labelled contrast (activation vs installation is worth teaching). `llm_agent`: **kept simulated, now explicitly labelled** — de-simulating it would need `mycelium-wasm-host` as a root dev-dependency, and `cargo test --lib` compiles dev-deps, so wasmtime would enter `make check` (whose contract is *no wasmtime, ~3 min*). The real chunked-pull-drives-loading-tier path is proven instead by the provisioner blob e2e test and the coop `catalog`/`mcp_toolgrowth` demos; `llm_agent`'s percent loops carry SIMULATED markers pointing there. (Decided 2026-07-07 during implementation.) |
| Tests (acceptance criteria, not afterthoughts) | Cross-node provision knowing *only* the catalogue (no hardcoded provider). Librarian death after peer cache → second peer provisions. Manifest add/remove → publish/tombstone, scoped to own signer (another publisher's entry survives). Provenance rejection. Kind dispatch: wasm instantiates; blob places + probe-gates; unregistered kind → skip + counter. Large-artifact path: fixture > frame cap via bulk/ranged pull, progress observed, crash-mid-pull leaves no partial placed file. Encoding: round-trip across all kinds signed/unsigned; unknown format-version rejected (and counted). |
| Docs | Ops guide gains the manifest/librarian runbook section; guide ch. 14 gains the activation-vs-installation pitfall; this record's §3 principle cross-linked from the wiki's companions pages. |

**Open question (deliberately deferred): crate naming.** After this, `mycelium-wasm-host`
contains the artifact/catalogue/provisioner/runtime machinery with WASM as one runtime — the
name undersells it. Options: rename/split (`mycelium-artifacts` with the wasm engine as a
feature) vs. keep-and-redocument. Recommendation: keep the crate in place for this work (churn
minimization; everything is additive), take the naming as its own small decision when the dust
settles.

---

## 10. Sequencing

1. **`FsLibrarySource` + manifest format + doc-drift fix** (sync, no design risk; the library is
   usable single-host immediately).
2. **Kind axis**: `ArtifactKind`, entry encoding bitfield, `ArtifactRuntime`/`Installed` traits,
   `WasmComponentRuntime` (pure relocation of today's behaviour — behaviour-identical tests),
   provisioner registry + async reservation.
3. **Librarian**: `artifact/librarian` advertisement, resolving `MeshArtifactSource`
   constructor, manifest→KV sync loop. Cross-node test: install from catalogue knowledge only.
4. **`BlobRuntime` + large-artifact transport**: chunked direct pull, temp-file/rename, real
   loading tiers, bulk-transport peer serving. `llm_agent` de-simulated.
5. **Examples honest-ified + acceptance tests** (§9 rows); `catalog` gains the
   publisher-dies-installer-still-provisions act; `mcp_toolgrowth` gains real code arrival.
6. **Object-store source** — ✅ shipped as three pieces: the **`BlobFetcher`** async trait
   (the vendor-SDK extension point — native SigV4 is deliberately out of scope; an AWS/OCI
   client implements `BlobFetcher` and plugs in), **`PrefetchingSource`** (the verified-cache
   bridge to the sync face, with `prefetch_all` as a librarian's mirror step), and
   **`HttpLibrarySource`** (GET `{base}/{artifact-hex}` over any HTTP(S) blob store —
   S3-compatible, nginx, CDN — with static credential headers, gated by the node's
   `EgressPolicy` **before** dispatch: the same WS3 posture as the LLM backends, per-node
   creds, no relay).
7. Revisit async `ArtifactSource` once 1–6 are in use. **Resolved 2026-07-07:
   declined-with-evidence.** By step 6 the crate ships three async faces that together serve
   every consumer: `MeshArtifactSource::prefetch` (peer pulls), `BlobFetcher` +
   `PrefetchingSource` (remote stores, vendor-extensible), and `RangedArtifactSource`
   (streaming for large blobs). An async `ArtifactSource` would unify those aesthetically but
   break `WasmHost::provision` and every source implementation for no new capability — the
   same shape as the exactly-once-effect record's declined code-extraction: the shared
   artifact is the *pattern* (async pull → verified cache → sync face), not a trait revision.
   Reopen only if a consumer appears that genuinely needs transparent on-demand async fetch
   inside `provision` itself.

**Non-goals:** cross-cluster catalogue sync (stays publish-per-cluster + shared store — ops
guide §"Sharing a catalogue across clusters"); artifact GC/eviction (operator concern; entries
tombstone like any KV); changing core (L5); a transitive install closure (the M15 one-hop
contract stands — `catalog.rs:5–13`: service dependencies are runtime-mesh-resolved, never
frozen into a deployment set).
