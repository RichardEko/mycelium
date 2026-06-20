//! Example 11 — **the cluster-wide artifact catalogue** (where deployables live, registering, and
//! making them available to a cluster).
//!
//! Demo 04 (`provisioning`) used a node-local `InMemorySource` shortcut — fine in one process, but
//! no *other* node can pull from it. This demo shows the **real** path: a gossiped catalogue any
//! node can discover, and artifact bytes any node can pull over the mesh.
//!
//! There is **no registry server**. The catalogue *is* the gossip KV store: an entry published to
//! `installable/{ns}/{name}/{hex}` replicates to every node like any other key. The artifact *bytes*
//! are served peer-to-peer over an `artifact.fetch` RPC and verified against their content address
//! on arrival (so the byte source is untrusted).
//!
//!   • `publisher` — holds the route-optimizer bytes; **serves** them (`serve_artifacts`) and
//!     **registers** a signed catalogue entry (`publish_installable`).
//!   • `installer` — discovers the entry via `InstallableCatalog::from_kv` (the gossiped catalogue),
//!     verifies its provenance, **pulls** the bytes over the mesh (`MeshArtifactSource`, verified),
//!     provisions the WASM component, and advertises + serves the `route/optimize` capability.
//!   • `caller`    — invokes the now-available capability.
//!
//! Run:  cargo run -p mycelium-coop-examples --bin catalog

use std::sync::Arc;
use std::time::Duration;

use coop::common::{alloc_ports, spawn_depot, DepotOpts};
use ed25519_dalek::SigningKey;
use mycelium::{CapFilter, Capability};
use mycelium_wasm_host::{
    cap_invoke_kind, ArtifactId, HostState, InMemorySource, InstallableCatalog, InstallableEntry,
    MeshArtifactSource, WasmHost, publish_installable, serve_artifacts,
};

const OPTIMIZER_WASM: &[u8] =
    include_bytes!("../../../../mycelium-wasm-host/tests/fixtures/echo_component.wasm");

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

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt().with_max_level(tracing::Level::ERROR).init();

    let cert_dir = std::env::temp_dir().join(format!("coop-catalog-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&cert_dir);
    let p = alloc_ports(6); // publisher, installer, caller

    // ── publisher: serves the bytes + registers a signed catalogue entry ────────
    let publisher = spawn_depot(DepotOpts {
        name: "publisher".into(), gossip_port: p[0], http_port: p[1],
        zone: "hub".into(), bootstrap: vec![], cert_dir: cert_dir.clone(), health_secs: Some(2),
    }).await?;
    let seed = publisher.gossip_port;

    // The publisher signs the artifact's provenance with a publisher key (independent of the node
    // identity — "this publisher vouches for these bytes"). The content address is the hash of the
    // bytes; the catalogue entry binds capability → content address, signed.
    // Demo-only fixed seed; a production publisher key comes from a KMS.
    let publisher_key = SigningKey::from_bytes(&[42u8; 32]);
    let publisher_pub = publisher_key.verifying_key().to_bytes();
    let artifact_id = ArtifactId::of(OPTIMIZER_WASM);
    let entry = InstallableEntry::new(Capability::new("route", "optimize"), artifact_id)
        .with_cost(OPTIMIZER_WASM.len() as u64, 1)
        .signed_by(&publisher_key);

    // Serve the bytes to the cluster (artifact.fetch RPC) and register the entry in the catalogue.
    let source = Arc::new({
        let mut s = InMemorySource::new();
        let id = s.insert(OPTIMIZER_WASM.to_vec());
        assert_eq!(id, artifact_id, "content address is deterministic");
        s
    });
    let _serve = serve_artifacts(Arc::clone(&publisher.agent), Arc::clone(&source) as Arc<_>);
    assert!(publish_installable(&publisher.agent.kv(), &entry), "register the entry in the catalogue");
    println!("[publisher] serving bytes + registered route/optimize in the catalogue (installable/)");

    // ── installer + caller ──────────────────────────────────────────────────────
    let mk = |name: &str, gp: u16, hp: u16| DepotOpts {
        name: name.into(), gossip_port: gp, http_port: hp,
        zone: "depot".into(), bootstrap: vec![seed], cert_dir: cert_dir.clone(), health_secs: Some(2),
    };
    let installer = spawn_depot(mk("installer", p[2], p[3])).await?;
    let caller    = spawn_depot(mk("caller",    p[4], p[5])).await?;
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
    println!("[installer] found route/optimize in the catalogue (gossiped from the publisher)");

    // ── Phase 2 — verify provenance, then pull the bytes over the mesh ──────────
    assert!(entry.verify_provenance(&[publisher_pub]),
        "the entry is signed by the trusted publisher key (provenance)");
    assert!(!entry.verify_provenance(&[[0u8; 32]]), "an untrusted key would be rejected");
    println!("[installer] provenance verified (signed by the publisher)");

    let publisher_id = publisher.node_id();
    let mesh_source = MeshArtifactSource::new(
        Arc::clone(&installer.agent), vec![publisher_id], Duration::from_secs(10));
    // Retry the fetch: the artifact.fetch RPC can race peer-path establishment to the publisher
    // (the same Individual-frame-vs-peering timing the mailbox lesson covers). prefetch is
    // idempotent — a cached id short-circuits — so retrying is safe.
    let mut pulled = false;
    for _ in 0..40 {
        if mesh_source.prefetch(&entry.artifact).await {
            pulled = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    assert!(pulled, "the bytes pull over the mesh and verify against the content address");
    println!("[installer] pulled the artifact bytes over the mesh (verified against the content address)");

    // ── Phase 3 — provision the WASM component + advertise & serve the capability ─
    let host = WasmHost::new()?;
    let state = HostState::new(
        installer.node_id(), entry.provides.namespace.clone(),
        installer.agent.kv(), installer.agent.mesh());
    let mut instance = host.provision(&mesh_source, &entry.artifact, state)
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

    // ── Phase 4 — the caller invokes the catalogue-installed capability ─────────
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

    println!("\nAll assertions passed — registered to the gossip catalogue, discovered cluster-wide, pulled over the mesh, provisioned, and invoked. No registry server.");

    serve.abort();
    for d in [caller, installer, publisher] {
        d.shutdown().await;
    }
    let _ = std::fs::remove_dir_all(&cert_dir);
    Ok(())
}
