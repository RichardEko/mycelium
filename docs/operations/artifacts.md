# Dynamically deployable artifacts

How a Mycelium cluster distributes and installs **artifacts** — deployable bytes
(a WASM component, a model) that a node pulls, verifies, instantiates, and then
advertises as a **Capability**. See [00 · Concepts](../guide/00-concepts.md) for
the Artifact → Capability → Skill → Tool vocabulary.

Two audiences:

- **[DevOps](#devops--operating-the-catalogue)** — where the catalogue lives,
  how to stand one up, how bytes are distributed, trust/provenance.
- **[Solution/Dev](#solutiondev--authoring--publishing-an-artifact)** — how to
  author a deployable artifact and publish it.

Runnable reference: [`examples/coop/src/bin/catalog.rs`](../../examples/coop/src/bin/catalog.rs)
(`cargo run -p mycelium-coop-examples --features wasm --bin catalog`).

---

## DevOps — operating the catalogue

### Where is the catalogue kept? There is no registry server.

The catalogue **is the gossip KV store**. An installable artifact is announced
by writing one entry under the `installable/{ns}/{name}/{hex}` prefix; that key
replicates to every node by the same anti-entropy that heals all KV state.
*Every node holds the whole catalogue.* There is no Docker-registry-style server
to deploy, secure, or keep available — the catalogue's availability is the
cluster's availability.

```text
installable/{ns}/{name}/{artifact-hex}   →  InstallableEntry { provides, kind, artifact_id, cost hints, requires{disk,mem}, signature }
```

Two distinct things travel separately:

| | What | How it's distributed |
|---|---|---|
| **Catalogue entry** | the *announcement*: capability → content address (+ cost, signature) | gossiped KV (`installable/`) — every node sees it |
| **Artifact bytes** | the actual component | pulled peer-to-peer over an `artifact.fetch` RPC, on demand |

### How do I make the catalogue available to a cluster?

You don't stand anything up — you **publish to it** and **serve the bytes**:

1. A node holding the bytes calls `serve_artifacts(agent, source)` — it answers
   `artifact.fetch` RPCs (payload = 32-byte content address) with the bytes.
2. The same (or any) node calls `publish_installable(kv, &entry)` — the entry
   gossips cluster-wide.

That's it for an **ad-hoc publish** (a dev node offering a one-off). For
production, use the **durable library** instead: CI publishes blob + signed
manifest row into an `FsLibrarySource` directory (or an HTTP blob store), and a
node takes the librarian role — `spawn_librarian(agent, source, config)` serves
the bytes, advertises `artifact/librarian`, and keeps the catalogue reconciled
to the manifest (publishes additions, tombstones removals, scoped to the
library's publisher key). Any node then discovers the entry
(`InstallableCatalog::from_kv`) and pulls from a *discovered* holder
(`MeshArtifactSource::resolving(agent, librarian_filter(), timeout)` — no
hardcoded node-ids). The [`catalog`](../../examples/coop/src/bin/catalog.rs)
example runs the whole durable flow, including the origin dying.

### Trust & provenance

The byte source is **untrusted**: `pull_artifact` / `MeshArtifactSource` verify
the returned bytes against the requested **content address** (`ArtifactId`, a
hash) on arrival — a peer cannot substitute different bytes. That covers
*integrity*.

For *origin*, sign the catalogue entry. A publisher signs with a key
(`InstallableEntry::signed_by(&key)`); an installer that sets
`Provisioner::require_provenance([trusted_pubkey])` (or calls
`entry.verify_provenance(&[trusted_pubkey])`) refuses any entry not signed by a
trusted publisher — even if its bytes hash correctly. Publisher keys are an
operator concern (wrap a KMS); the demo uses a fixed seed.

### Operational notes

- **Metrics.** The provisioner/librarian emit Prometheus-ready counters through the `metrics`
  facade (visible on `/metrics` when the `metrics` feature's recorder is installed):
  `mycelium_artifact_installs_{started,completed}_total`,
  `mycelium_artifact_installs_failed_total{stage=fetch|verify|place|activation|resources|host}`,
  `mycelium_artifact_ineligible_skips_total{reason=no_runtime|budget|memory|disk}` (the
  resource-election tripwire — a storm here means the fleet can't fit an artifact),
  `mycelium_artifact_probe_withdrawals_total`, and
  `mycelium_artifact_librarian_{published,tombstoned}_total`.

- **Size ceiling.** `artifact.fetch` rides the gossip frame (`MAX_FRAME_BYTES` =
  10 MiB). Larger artifacts want the bulk transport
  (`ServiceHandle::bulk_serve`). Typical WASM components are well under.
- **The prefetch step.** `MeshArtifactSource::fetch` serves from a local cache;
  bytes must be `prefetch`ed (async, verified) before a synchronous `provision`
  reads them. The autonomic `Provisioner` and the `catalog` example both account
  for this; if you wire it by hand, prefetch first.
- **Eviction / GC.** Catalogue entries are ordinary KV; tombstone an entry to
  withdraw it. Held artifact bytes live as long as the serving node holds them.
- **Durability — the library tier.** For bytes that must outlive any serving node,
  use the durable library: `FsLibrarySource` (a content-addressed blob directory +
  signed `Manifest`) or any HTTP(S) blob store via `HttpLibrarySource` +
  `PrefetchingSource` (egress-gated, credential headers; implement `BlobFetcher`
  for vendor SDKs). A node takes the **librarian** role with `spawn_librarian` —
  it serves the bytes, advertises `artifact/librarian` for discovery
  (`MeshArtifactSource::resolving` finds it; no hardcoded node-ids), and keeps the
  catalogue reconciled to the manifest. The library is an *origin tier*, never a
  mandatory read path — peers that pulled an artifact serve the same verified
  bytes. Design record: [design/artifact-library.md](../design/artifact-library.md);
  live demos: the coop `catalog` example (origin dies, installs continue) and
  `model_deploy` (manual — a real GGUF deployed end-to-end into Ollama, generating
  real tokens).

### Sharing a catalogue across clusters

The catalogue is the **per-cluster** gossip KV — it does *not* automatically span
clusters. But because an `InstallableEntry` is **content-addressed and
Ed25519-signed**, the *same* artifact is byte-identical and verifies identically in
every cluster, so an org-wide catalogue is a publishing pattern, not missing
machinery:

1. **Publish the same signed entry into each cluster** (CI runs `publish_installable`
   against each cluster's gateway/seed). The hash + signature mean a runtime in any
   cluster pulls and verifies the exact same bytes.
2. **Serve the bytes from a shared store.** Point `serve_artifacts` at a common
   object store (S3 / OCI registry / artifact server) that all clusters can reach,
   or let one bridge node in each cluster mirror from it — so you publish bytes once
   and every cluster's mesh fetch resolves them.

What Mycelium deliberately does *not* provide is a cross-cluster catalogue **sync
daemon** — that would re-introduce a coordinator. Cross-*domain* capability
*discovery* (which domain offers what) is the federation story instead — see the
[AgentFacts](observability.md#viewing-agentfacts) edge and the `federation_facts`
demo; artifact-byte distribution stays the publish-per-cluster + shared-store
pattern above.

---

## Solution/Dev — authoring & publishing an artifact

### 1 · Author the deployable component

Artifacts are WASM components built against the host's WIT world. The reference
fixture is [`mycelium-wasm-host/tests/fixtures/echo-component/`](../../mycelium-wasm-host/tests/fixtures/echo-component/)
(the `wit/` interface + a `build.sh`); the host boundary is documented in the
[`mycelium-wasm-host`](../../mycelium-wasm-host/) crate. A component exports an
`invoke(payload) -> result` and may import the host's confined `kv` (scoped to
its `comp/{node}/{ns}/` subtree). Build it to a `.wasm` and you have your bytes.

```bash
# in mycelium-wasm-host/tests/fixtures/echo-component/
./build.sh        # → echo_component.wasm
```

### 2 · Declare the footprint, sign, and publish to the library

The publish step is CI code (no node, no mesh): store the bytes in the library,
describe them — **kind**, cost hints, and the **resource footprint** — sign, and
append to the manifest. A running librarian picks the change up on its next
reconcile pass and the catalogue updates cluster-wide.

```rust
use mycelium_wasm_host::{ArtifactKind, FsLibrarySource, InstallableEntry, Manifest,
                         MANIFEST_FILE};
use mycelium::Capability;

let bytes = std::fs::read("my_component.wasm")?;
let library = FsLibrarySource::open("/srv/mycelium-library")?;
let id = library.store(&bytes)?;                        // content address = hash of the bytes

let entry = InstallableEntry::new(Capability::new("route", "optimize"), id)
    .with_kind(ArtifactKind::WasmComponent)             // install dispatch (Blob for models)
    .with_cost(bytes.len() as u64, /* est_install_secs */ 1)
    .with_requirements(/* disk */ 0, /* mem */ 64 << 20) // the resource footprint — see below
    .signed_by(&publisher_key);                         // ed25519 SigningKey (KMS in prod)
Manifest::append_entry(&std::path::Path::new("/srv/mycelium-library").join(MANIFEST_FILE), entry)?;
```

**Declaring `with_requirements(disk_bytes, mem_bytes)`** — what hosting the
artifact *consumes*, as opposed to `with_cost` (transfer-size ranking hints):

- `disk_bytes` — the placed footprint at the runtime's placement root. For a
  `Blob` this is the file size; for a `WasmComponent` it is `0` (memory-resident).
- `mem_bytes` — the working set once live. For a model: the loaded size, not the
  file size (a 4 GB quantised model typically wants 5 GB+ activated — weights +
  KV-cache/session overhead; measure with your runtime). For a WASM component:
  the instance memory bound you run it with.
- `0` = undeclared = unchecked. Declare honestly: nodes **refuse to self-elect**
  for entries whose requirements exceed their headroom
  (`headroom × available − reserved`, default 0.8 — `Provisioner::set_resource_policy`),
  so an undeclared 5 GB model will be attempted by nodes that cannot host it,
  and an over-declared one will be installed by nobody (the
  `ineligible_skips` tripwire tells you which is happening).
- Requirements are **inside the provenance signature** — they are install-safety
  claims. Changing them means re-signing (unlike the cost hints); tampered
  requirements fail `verify_provenance`.

Sign **after** `with_kind`/`with_requirements`. For an ad-hoc publish without a
library (a dev one-off), the `InMemorySource` + `publish_installable` flow above
still works — it just dies with the process.

**Remote blob stores:** blobs can live in any HTTP(S) store (consumed via
`HttpLibrarySource` + `PrefetchingSource`, egress-gated, credential headers;
implement `BlobFetcher` for a vendor SDK). The *manifest* is a local file path
(`LibrarianConfig::manifest_path`), so for a remote store, sync the manifest file
down to the librarian node (CI artifact, cron `curl`, or a mounted volume) while
the bytes stay remote — the librarian mirrors what its manifest names
(`PrefetchingSource::prefetch_all`).

### 3 · How another node installs it

The installing node needs no configuration beyond being in the cluster:

```rust
use mycelium_wasm_host::{librarian_filter, InstallableCatalog, MeshArtifactSource,
                         WasmHost, HostState};
use mycelium::CapFilter;

let filter = CapFilter::new("route", "optimize");
let catalog = InstallableCatalog::from_kv(&agent.kv());   // the gossiped catalogue
let entry = catalog.resolve_best(&filter).expect("in catalogue").clone();
assert!(entry.verify_provenance(&[trusted_pub]));         // origin check

// Holders are DISCOVERED via the capability ring — no node-ids in installer code.
let src = MeshArtifactSource::resolving(agent.clone(), librarian_filter(), timeout);
src.prefetch(&entry.artifact).await;                       // pull bytes over the mesh (verified)

let host = WasmHost::new()?;
let state = HostState::new(agent.node_id().clone(), entry.provides.namespace.clone(),
                           agent.kv(), agent.mesh());
let mut instance = host.provision(&src, &entry.artifact, state)?;  // verify + instantiate
// register an rpc handler on `cap_invoke_kind(ns, name)`, advertise `entry.provides`, serve.
```

The fully wired version (publisher + installer + caller, with the serve loop) is
[`examples/coop/src/bin/catalog.rs`](../../examples/coop/src/bin/catalog.rs).

### Doing it autonomically

Hand-wiring is for understanding; production nodes run the `Provisioner`
(register a `BlobRuntime` for model artifacts alongside the built-in WASM
runtime). It applies everything above automatically per round: provenance
policy, kind dispatch, **and the resource-eligibility check** — a node whose
free memory/disk (minus in-flight reservations) can't fit an entry's declared
requirements silently never elects; some node that fits does.

The [`provisioning`](../../examples/coop/src/bin/provisioning.rs) flagship runs
this loop *on demand*: a `Provisioner` watches for unmet demand and installs the
matching artifact automatically. That demo uses a node-local `InMemorySource`
shortcut (single process); point its `catalog` at `InstallableCatalog::from_kv`
and its `source` at a (prefetched) `MeshArtifactSource` for the cluster-wide
version shown here — see the [patterns chapter](../guide/14-patterns-and-pitfalls.md)
§10.
