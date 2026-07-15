//! Example 11 — **the cluster-wide artifact catalogue** (where deployables durably live,
//! registering them, and installing them — including after the publisher is gone).
//!
//! Demo 04 (`provisioning`) used a node-local `InMemorySource` shortcut — fine in one process, but
//! no *other* node can pull from it. This demo shows the **real** path end to end, with no
//! build-time embedding anywhere: the component bytes are **read from disk at runtime**, stored
//! in a durable **library** (an `FsLibrarySource` directory + signed manifest), served by a
//! **librarian** node, and installed by nodes that know *only the catalogue* — no hardcoded
//! provider ids. Then the librarian dies **and the library directory is deleted**, and a
//! late-joining node still installs, from a peer's verified cache.
//!
//! There is **no registry server**. The catalogue *is* the gossip KV store: the librarian
//! reconciles the library's manifest into `installable/{ns}/{name}/{hex}`, which replicates to
//! every node like any other key. The artifact *bytes* travel peer-to-peer over an
//! `artifact.fetch` RPC and are verified against their content address on arrival — so *any*
//! holder (librarian or peer cache) is interchangeable and untrusted.
//!
//!   • CI (plain code, no node) — reads the component from disk, stores it in the library,
//!     writes the **signed manifest** (the publisher key never touches any node).
//!   • `librarian` — `spawn_librarian`: serves the library's bytes, advertises
//!     `artifact/librarian`, and syncs manifest → catalogue.
//!   • `installer` — discovers the entry via the catalogue, verifies provenance, pulls via
//!     `MeshArtifactSource::resolving` (holder found through the capability ring), provisions,
//!     serves `route/optimize` — then **re-serves its verified cache** as a peer holder.
//!   • `caller` — invokes the installed capability.
//!   • `late` — joins **after the librarian is dead and the library deleted**; still installs.
//!
//! ## Loads
//! - **Content** — route/optimize — a WASM component (the echo fixture stands in for a router)
//! - **Type** — `ArtifactKind::WasmComponent`
//! - **From** — FsLibrarySource (disk) → signed manifest → librarian → gossip catalogue → MeshArtifactSource (peer pull, content-verified); survives publisher death via a peer cache
//!
//! Run:  cargo run -p mycelium-coop-examples --features wasm --bin catalog
use std::sync::Arc;
use std::time::Duration;

use coop::common::{alloc_ports, announce_loads, spawn_depot, DepotOpts, Loads};
use ed25519_dalek::SigningKey;
use mycelium::{CapFilter, Capability};
use mycelium_wasm_host::{
    cap_invoke_kind, librarian_filter, spawn_librarian, FsLibrarySource, HostState,
    InstallableCatalog, InstallableEntry, LibrarianConfig, Manifest, MeshArtifactSource,
    WasmHost, serve_artifacts, LIBRARIAN_NAME, LIBRARIAN_NS, MANIFEST_FILE,
};

/// Read at **runtime** from the repo — nothing is compiled into this binary. A real deployment
/// reads freshly-built component bytes; the committed echo fixture stands in so CI needs no
/// wasm toolchain.
const OPTIMIZER_WASM_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../mycelium-wasm-host/tests/fixtures/echo_component.wasm"
);

async fn wait_until(secs: u64, mut cond: impl FnMut() -> bool) -> bool {
    let deadline = std::time::Instant::now() + Duration::from_secs(secs);
    while std::time::Instant::now() < deadline {
        if cond() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
    cond()
}

const LOADS: &[Loads] = &[Loads {
    content: "route/optimize — a WASM component (the echo fixture stands in for a router)",
    kind: "ArtifactKind::WasmComponent",
    from: "FsLibrarySource (disk) → signed manifest → librarian → gossip catalogue → \
           MeshArtifactSource (peer pull, content-verified); survives publisher death via a peer cache",
}];

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    announce_loads(LOADS);
    tracing_subscriber::fmt().with_max_level(tracing::Level::ERROR).init();

    let cert_dir = std::env::temp_dir().join(format!("coop-catalog-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&cert_dir);
    let p = alloc_ports(8); // librarian, installer, caller, late

    // ── Phase 0 — CI publishes into the durable library (no node involved) ───────
    // The publisher key signs the *entry* (kind + content address + capability) when the
    // artifact is added to the library; librarian nodes serve pre-signed entries and never
    // hold signing keys. Demo-only fixed seed; production keys come from a KMS.
    let lib_dir = std::env::temp_dir().join(format!("coop-catalog-lib-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&lib_dir);
    let publisher_key = SigningKey::from_bytes(&[42u8; 32]);
    let publisher_pub = publisher_key.verifying_key().to_bytes();

    let optimizer_wasm = std::fs::read(OPTIMIZER_WASM_PATH)?; // runtime read — no include_bytes!
    let library = Arc::new(FsLibrarySource::open(&lib_dir)?);
    let artifact_id = library.store(&optimizer_wasm)?;
    let entry = InstallableEntry::new(Capability::new("route", "optimize"), artifact_id)
        .with_cost(optimizer_wasm.len() as u64, 1)
        .signed_by(&publisher_key);
    Manifest::from_entries(vec![entry]).save(&lib_dir.join(MANIFEST_FILE))?;
    println!("[ci] stored the optimizer in the library + wrote the signed manifest (no node yet)");

    // ── librarian: serve the library + advertise + sync manifest → catalogue ─────
    let librarian = spawn_depot(DepotOpts {
        name: "librarian".into(), gossip_port: p[0], http_port: p[1],
        zone: "hub".into(), bootstrap: vec![], cert_dir: cert_dir.clone(), health_secs: Some(2),
    }).await?;
    let seed = librarian.gossip_port;
    let librarian_role = spawn_librarian(
        Arc::clone(&librarian.agent),
        Arc::clone(&library) as Arc<_>,
        LibrarianConfig {
            manifest_path: lib_dir.join(MANIFEST_FILE),
            publisher: publisher_pub,
            sync_interval: Duration::from_millis(500),
        },
    );
    println!("[librarian] serving the library + advertising artifact/librarian + syncing the manifest");

    // ── installer + caller ────────────────────────────────────────────────────────
    let mk = |name: &str, gp: u16, hp: u16, boot: u16| DepotOpts {
        name: name.into(), gossip_port: gp, http_port: hp,
        zone: "depot".into(), bootstrap: vec![boot], cert_dir: cert_dir.clone(), health_secs: Some(2),
    };
    let installer = spawn_depot(mk("installer", p[2], p[3], seed)).await?;
    let caller    = spawn_depot(mk("caller",    p[4], p[5], seed)).await?;
    println!("[installer|caller] up");

    wait_until(20, || !installer.agent.peers().is_empty() && !caller.agent.peers().is_empty()).await;

    // ── Phase 1 — the catalogue is cluster-wide: the installer sees the entry ────
    let filter = CapFilter::new("route", "optimize");
    let seen = wait_until(20, || {
        InstallableCatalog::from_kv(&installer.agent.kv()).resolve_best(&filter).is_some()
    }).await;
    assert!(seen, "the installer discovers the entry via the gossiped catalogue (no registry server)");
    let catalog = InstallableCatalog::from_kv(&installer.agent.kv());
    let entry = catalog.resolve_best(&filter).expect("entry").clone();
    println!("[installer] found route/optimize in the catalogue (synced from the library's manifest)");

    // ── Phase 2 — verify provenance, then pull via a *discovered* holder ──────────
    assert!(entry.verify_provenance(&[publisher_pub]),
        "the entry is signed by the trusted publisher key (provenance)");
    assert!(!entry.verify_provenance(&[[0u8; 32]]), "an untrusted key would be rejected");
    println!("[installer] provenance verified (signed into the library by CI)");

    // No node-id anywhere: the holder is resolved through the capability ring.
    let mesh_source = Arc::new(MeshArtifactSource::resolving(
        Arc::clone(&installer.agent), librarian_filter(), Duration::from_secs(3)));
    // Retry the fetch: the artifact.fetch RPC can race peer-path establishment (the same
    // Individual-frame-vs-peering timing the mailbox lesson covers). prefetch is idempotent.
    let mut pulled = false;
    for _ in 0..40 {
        if mesh_source.prefetch(&entry.artifact).await {
            pulled = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    assert!(pulled, "the bytes pull from a librarian discovered via the capability ring");
    println!("[installer] pulled the bytes from a discovered librarian (verified against the content address)");

    // ── Phase 3 — provision + advertise & serve the capability ───────────────────
    let host = WasmHost::new()?;
    let state = HostState::new(
        installer.node_id(), entry.provides.namespace.clone(),
        installer.agent.kv(), installer.agent.mesh());
    let mut instance = host.provision(&*mesh_source, &entry.artifact, state)
        .expect("provision (fetch from cache + verify + instantiate)");

    let invoke_kind: Arc<str> = Arc::from(cap_invoke_kind("route", "optimize").as_str());
    let mut rx = installer.agent.service().rpc_rx(Arc::clone(&invoke_kind));
    let _cap = installer.agent.capabilities()
        .advertise_capability(entry.provides.clone(), Duration::from_secs(30));
    let serve_agent = Arc::clone(&installer.agent);
    let serve = tokio::spawn(async move {
        while let Some(req) = rx.recv().await {
            let out = instance.invoke("invoke", req.payload().to_vec())
                .ok().and_then(|r| r.ok()).unwrap_or_default();
            serve_agent.service().rpc_respond(&req, out);
        }
    });
    println!("[installer] provisioned + serving route/optimize (installed from the catalogue)");

    // The installer also joins the byte-serving tier: its verified cache is as good as the
    // library (content addressing makes holders interchangeable), so it serves + advertises.
    let _peer_serve = serve_artifacts(
        Arc::clone(&installer.agent), Arc::clone(&mesh_source) as Arc<_>);
    let _peer_cap = installer.agent.capabilities().advertise_capability(
        Capability::new(LIBRARIAN_NS, LIBRARIAN_NAME), Duration::from_secs(5));
    println!("[installer] re-serving its verified cache as a peer holder");

    // ── Phase 4 — the caller invokes the catalogue-installed capability ──────────
    let visible = wait_until(20, || {
        !caller.agent.capabilities().resolve(&filter).is_empty()
    }).await;
    assert!(visible, "the provisioned capability advertises cluster-wide");
    let (provider, _) = caller.agent.capabilities().resolve(&filter).into_iter().next().unwrap();
    let reply = caller.agent.service()
        .rpc_call(provider, Arc::clone(&invoke_kind), b"optimize-this-route".to_vec(), Duration::from_secs(5))
        .await?;
    println!("[caller] invoked route/optimize → {}", String::from_utf8_lossy(&reply));
    assert_eq!(reply.as_ref(), b"optimize-this-route", "the catalogue-installed component runs");

    // ── Phase 5 — kill the librarian AND delete the library ──────────────────────
    // The origin tier is gone entirely: no serving node, no durable store. The catalogue
    // entry survives (it is ordinary KV), and the installer's verified cache still holds
    // the bytes.
    drop(librarian_role);          // retracts artifact/librarian + stops serve/sync
    librarian.shutdown().await;
    std::fs::remove_dir_all(&lib_dir)?;
    assert!(!lib_dir.exists(), "the durable library directory is deleted");
    println!("[cluster] librarian dead + library deleted — origin tier is gone");

    // ── Phase 6 — a late joiner still installs, from the peer cache ──────────────
    let late = spawn_depot(mk("late", p[6], p[7], installer.gossip_port)).await?;
    wait_until(20, || !late.agent.peers().is_empty()).await;

    let seen_late = wait_until(20, || {
        InstallableCatalog::from_kv(&late.agent.kv()).resolve_best(&filter).is_some()
    }).await;
    assert!(seen_late, "the late joiner still sees the catalogue entry (it is ordinary KV)");
    let late_entry = InstallableCatalog::from_kv(&late.agent.kv())
        .resolve_best(&filter).expect("entry").clone();
    assert!(late_entry.verify_provenance(&[publisher_pub]));

    let late_source = MeshArtifactSource::resolving(
        Arc::clone(&late.agent), librarian_filter(), Duration::from_secs(3));
    let mut late_pulled = false;
    for _ in 0..40 {
        if late_source.prefetch(&late_entry.artifact).await {
            late_pulled = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    assert!(late_pulled,
        "the late joiner pulls from the installer's cache — same hash, same verify, no origin needed");

    let late_state = HostState::new(
        late.node_id(), late_entry.provides.namespace.clone(),
        late.agent.kv(), late.agent.mesh());
    let mut late_instance = WasmHost::new()?
        .provision(&late_source, &late_entry.artifact, late_state)
        .expect("late node provisions from peer-cached bytes");
    let out = late_instance.invoke("invoke", b"late-route".to_vec())
        .expect("invoke").expect("component ok");
    assert_eq!(out, b"late-route");
    println!("[late] joined after the origin died — installed from a peer cache and ran it");

    println!("\nAll assertions passed — runtime-read bytes → durable library → librarian → \
        discovered pull → provisioned → origin killed → late joiner installed from a peer cache. \
        No registry server, no build-time embedding, no hardcoded providers.");

    serve.abort();
    for d in [late, caller, installer] {
        d.shutdown().await;
    }
    let _ = std::fs::remove_dir_all(&cert_dir);
    Ok(())
}
