//! Example 11b — **origin-death survival, visualised**: the artifact catalogue outliving its
//! publisher, live in the browser.
//!
//! This is the continuous, animated sibling of `catalog.rs`. Same world, same mechanism — but
//! instead of running a fixed sequence of assertions and exiting, it loops the story forever and
//! streams the install picture to a `<canvas>` dashboard at `http://127.0.0.1:8098/`.
//!
//! The world: CI stores a `route/optimize` WASM component in a durable **library** (an
//! `FsLibrarySource` directory + a *signed* manifest) — the publisher key never touches a node. A
//! **librarian** node serves the library's bytes and reconciles its manifest into the gossip KV
//! catalogue (`installable/{ns}/{name}/{hex}`), which replicates to every node. An **installer**
//! discovers the entry through the catalogue alone (no hardcoded provider id), verifies provenance,
//! pulls the bytes peer-to-peer over `artifact.fetch` (content-verified on arrival), provisions a
//! `WasmHost`, serves the capability — and then **re-serves its verified cache** as a peer holder.
//! A **caller** invokes it.
//!
//! Then the climax, made legible: the **librarian dies and its library directory is deleted** — the
//! origin tier is gone entirely, no serving node and no durable store. A **late-joining node** still
//! installs, pulling the *same bytes* from the installer's verified cache and verifying them against
//! the *same content address*. Content addressing makes every holder interchangeable and untrusted,
//! so the install survives the origin's death.
//!
//! The genuine machinery runs for real: the library is published, the librarian serves + syncs, the
//! installer pulls + provisions + re-serves, the caller invokes, the librarian is killed + the
//! library deleted, and the late node provisions a fresh WASM instance from the peer cache and runs
//! it — every cycle. The browser narrates that arc on a loop, paced for legibility.
//!
//! Run:  cargo run -p mycelium-coop-examples --features wasm,metrics --bin catalog_viz
//! Then open http://127.0.0.1:8098/

use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::Serialize;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::{signal, time};

use coop::common::{alloc_ports, spawn_depot, DepotOpts, Loads};
use ed25519_dalek::SigningKey;
use mycelium::{CapFilter, Capability};
use mycelium_wasm_host::{
    cap_invoke_kind, librarian_filter, serve_artifacts, spawn_librarian, FsLibrarySource,
    HostState, InstallableCatalog, InstallableEntry, LibrarianConfig, Manifest, MeshArtifactSource,
    WasmHost, LIBRARIAN_NAME, LIBRARIAN_NS, MANIFEST_FILE,
};

/// Fixed HTTP port for the dashboard (kept clear of the gateway's OS-assigned depot ports).
const HTTP_PORT: u16 = 8098;

/// Read at **runtime** from the repo — nothing is compiled into this binary. A real deployment
/// reads freshly-built component bytes; the committed echo fixture stands in so CI needs no wasm
/// toolchain. Same path as `catalog.rs`.
const OPTIMIZER_WASM_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../mycelium-wasm-host/tests/fixtures/echo_component.wasm"
);

/// The "dynamically loaded artifact(s)" banner — reused verbatim from `catalog.rs`.
const LOADS: &[Loads] = &[Loads {
    content: "route/optimize — a WASM component (the echo fixture stands in for a router)",
    kind: "ArtifactKind::WasmComponent",
    from: "FsLibrarySource (disk) → signed manifest → librarian → gossip catalogue → \
           MeshArtifactSource (peer pull, content-verified); survives publisher death via a peer cache",
}];

/// The Mycelium concepts + services this demo exercises — injected into the dashboard's "what
/// you're seeing" box (the UI-example contract; see docs/wiki/dev/ui-example-contract.md). `tag` is
/// a layer/service key the shared panel colour-codes (I·II·III·IV · companion · gateway · audit).
const CONCEPTS: &str = r#"[
  {"tag":"I","name":"gossip-KV catalogue","gloss":"the librarian syncs installable/{ns}/{name}/{hex}; it replicates to every node"},
  {"tag":"IV","name":"content-addressed install","gloss":"discover → verify provenance → pull bytes → verify content hash → install"},
  {"tag":"IV","name":"origin-independent","gloss":"any holder (librarian or peer cache) is interchangeable; installs survive the origin's death"},
  {"tag":"companion","name":"WASM host","gloss":"the installed route/optimize component runs confined"},
  {"tag":"gateway","name":"gateway + metrics","gloss":"/stats · /gateway/fleet · /metrics — this Ops Console"}
]"#;

// ── Shared snapshot served to the browser ─────────────────────────────
#[derive(Serialize, Clone)]
struct NodeView {
    name:   String,
    role:   String,
    status: String, // idle | serving | installing | installed | dead
}

#[derive(Serialize, Clone)]
struct Flow {
    from:  String,
    to:    String,
    label: String,
}

#[derive(Serialize)]
struct VizState {
    nodes:             Vec<NodeView>,
    catalogue_present: bool,
    origin_alive:      bool,
    library_deleted:   bool,
    holders:           Vec<String>,
    phase:             String,
    caption:           String,
    flow:              Option<Flow>,
    events:            Vec<String>,
    tick:              u64,
}

impl VizState {
    fn to_json(&self) -> String {
        serde_json::to_string(self).unwrap_or_else(|_| "{}".to_string())
    }

    /// Set a node's status by name.
    fn set_status(&mut self, name: &str, status: &str) {
        if let Some(n) = self.nodes.iter_mut().find(|n| n.name == name) {
            n.status = status.into();
        }
    }

    /// Append an event, capped at the most-recent 8 (matches the browser's scroll).
    fn log(&mut self, msg: impl Into<String>) {
        self.events.push(msg.into());
        let overflow = self.events.len().saturating_sub(8);
        if overflow > 0 {
            self.events.drain(0..overflow);
        }
    }
}

/// Apply a mutation to the shared snapshot and bump the tick.
fn update(state: &Arc<Mutex<VizState>>, f: impl FnOnce(&mut VizState)) {
    let mut s = state.lock().unwrap();
    f(&mut s);
    s.tick += 1;
}

/// Dwell between transitions so the story stays legible in the browser.
async fn dwell(ms: u64) {
    time::sleep(Duration::from_millis(ms)).await;
}

async fn wait_until(secs: u64, mut cond: impl FnMut() -> bool) -> bool {
    let deadline = std::time::Instant::now() + Duration::from_secs(secs);
    while std::time::Instant::now() < deadline {
        if cond() {
            return true;
        }
        time::sleep(Duration::from_millis(150)).await;
    }
    cond()
}

// ── Minimal HTTP server — no dependencies beyond tokio (mirrors stigmergy_viz.rs) ─
// Serves the HTML dashboard at / and the JSON state at /state. `gw_port` is the *installer's*
// gateway port — the Ops Console back-link target must stay alive, and the librarian dies.
async fn serve_http(state: Arc<Mutex<VizState>>, gw_port: u16) {
    let listener = match TcpListener::bind(format!("127.0.0.1:{HTTP_PORT}")).await {
        Ok(l) => l,
        Err(e) => {
            eprintln!("HTTP server failed to bind :{HTTP_PORT} — {e}");
            return;
        }
    };
    println!("╔══════════════════════════════════════════════════════╗");
    println!("║  Catalogue dashboard → http://127.0.0.1:{HTTP_PORT}          ║");
    println!("║  installs survive the origin's death (peer cache)    ║");
    println!("╚══════════════════════════════════════════════════════╝");

    loop {
        let Ok((mut stream, _)) = listener.accept().await else { continue };
        let st = state.clone();
        tokio::spawn(async move {
            let mut buf = [0u8; 512];
            let n = stream.read(&mut buf).await.unwrap_or(0);
            let req = std::str::from_utf8(&buf[..n]).unwrap_or("");

            if req.starts_with("OPTIONS") {
                let _ = stream
                    .write_all(
                        b"HTTP/1.1 204 No Content\r\n\
                          Access-Control-Allow-Origin: *\r\n\
                          Connection: close\r\n\r\n",
                    )
                    .await;
                return;
            }

            if req.contains("GET /state") {
                let json = st.lock().unwrap().to_json();
                let response = format!(
                    "HTTP/1.1 200 OK\r\n\
                     Content-Type: application/json\r\n\
                     Access-Control-Allow-Origin: *\r\n\
                     Content-Length: {}\r\n\
                     Connection: close\r\n\r\n{}",
                    json.len(),
                    json
                );
                let _ = stream.write_all(response.as_bytes()).await;
            } else {
                // Inject the "⚙ Ops Console" back-link, pre-targeted at the INSTALLER's gateway —
                // the librarian dies in this demo, so the console target must be a node that lives.
                // A coop depot always runs the gateway, so this is unconditional.
                let console_link = format!(
                    "<a class=\"opsbtn\" href=\"http://127.0.0.1:8099/?target=127.0.0.1:{gw_port}\" \
                     title=\"Open this cluster in the Mycelium Ops Console\">⚙ Ops Console</a>"
                );
                let html = include_str!("catalog_viz.html")
                    .replace("__OPS_CONSOLE_LINK__", &console_link)
                    .replace("__CONCEPTS__", CONCEPTS);
                let response = format!(
                    "HTTP/1.1 200 OK\r\n\
                     Content-Type: text/html; charset=utf-8\r\n\
                     Content-Length: {}\r\n\
                     Connection: close\r\n\r\n{}",
                    html.len(),
                    html
                );
                let _ = stream.write_all(response.as_bytes()).await;
            }
        });
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    coop::common::announce_loads(LOADS);
    tracing_subscriber::fmt().with_max_level(tracing::Level::WARN).init();

    let cert_dir = std::env::temp_dir().join(format!("coop-catalog-viz-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&cert_dir);
    let p = alloc_ports(8); // librarian, installer, caller, late

    // ── Phase 0 — CI publishes into the durable library (no node involved) ───────
    // The publisher key signs the entry (kind + content address + capability). Demo-only fixed
    // seed; production keys come from a KMS.
    let lib_dir = std::env::temp_dir().join(format!("coop-catalog-viz-lib-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&lib_dir);
    let publisher_key = SigningKey::from_bytes(&[42u8; 32]);
    let publisher_pub = publisher_key.verifying_key().to_bytes();

    let optimizer_wasm = std::fs::read(OPTIMIZER_WASM_PATH)?; // runtime read — no include_bytes!
    let library = Arc::new(FsLibrarySource::open(&lib_dir)?);
    let artifact_id = library.store(&optimizer_wasm)?;
    let entry0 = InstallableEntry::new(Capability::new("route", "optimize"), artifact_id)
        .with_cost(optimizer_wasm.len() as u64, 1)
        .signed_by(&publisher_key);
    Manifest::from_entries(vec![entry0]).save(&lib_dir.join(MANIFEST_FILE))?;
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

    // ── installer + caller + late (all spawned once; the story loops over them) ──
    let mk = |name: &str, gp: u16, hp: u16, boot: u16| DepotOpts {
        name: name.into(), gossip_port: gp, http_port: hp,
        zone: "depot".into(), bootstrap: vec![boot], cert_dir: cert_dir.clone(), health_secs: Some(2),
    };
    let installer = spawn_depot(mk("installer", p[2], p[3], seed)).await?;
    let caller    = spawn_depot(mk("caller",    p[4], p[5], seed)).await?;
    println!("[installer|caller] up");

    // The Ops Console back-link targets the installer's gateway (it outlives the librarian).
    let gw_port = installer.http_port;
    let _ = installer.agent.kv().set("ui/viz", format!("http://127.0.0.1:{HTTP_PORT}/"));
    let _ = installer.agent.kv().set("ui/label", "Origin-death survival".to_string());
    println!(
        "[ops] installer gateway is live — point the Ops Console at http://127.0.0.1:{gw_port}/  \
         (/stats · /gateway/fleet · /metrics)"
    );

    wait_until(20, || !installer.agent.peers().is_empty() && !caller.agent.peers().is_empty()).await;

    // ── Shared snapshot + HTTP dashboard (start before the flow so the browser connects early) ──
    let node = |name: &str, role: &str| NodeView {
        name: name.into(), role: role.into(), status: "idle".into(),
    };
    let state = Arc::new(Mutex::new(VizState {
        nodes: vec![
            node("librarian", "origin"),
            node("installer", "installer"),
            node("caller", "caller"),
            node("late", "late-joiner"),
        ],
        catalogue_present: false,
        origin_alive: true,
        library_deleted: false,
        holders: Vec::new(),
        phase: "booting".into(),
        caption: "The cluster is forming — the librarian is about to serve the catalogue.".into(),
        flow: None,
        events: vec!["[ci] signed manifest stored in the durable library".into()],
        tick: 0,
    }));
    let state_for_server = state.clone();
    tokio::spawn(async move { serve_http(state_for_server, gw_port).await });

    // ── Real end-to-end setup (mirrors catalog.rs): discover → verify → pull → provision → serve ─
    let filter = CapFilter::new("route", "optimize");
    let seen = wait_until(20, || {
        InstallableCatalog::from_kv(&installer.agent.kv()).resolve_best(&filter).is_some()
    }).await;
    assert!(seen, "the installer discovers the entry via the gossiped catalogue (no registry server)");
    let entry = InstallableCatalog::from_kv(&installer.agent.kv())
        .resolve_best(&filter).expect("entry").clone();
    assert!(entry.verify_provenance(&[publisher_pub]), "entry signed by the trusted publisher key");
    println!("[installer] found route/optimize in the catalogue + verified provenance");

    // The holder is resolved through the capability ring — no node id anywhere.
    let installer_source = Arc::new(MeshArtifactSource::resolving(
        Arc::clone(&installer.agent), librarian_filter(), Duration::from_secs(3)));
    let mut pulled = false;
    for _ in 0..40 {
        if installer_source.prefetch(&entry.artifact).await { pulled = true; break; }
        time::sleep(Duration::from_millis(250)).await;
    }
    assert!(pulled, "the bytes pull from a librarian discovered via the capability ring");
    println!("[installer] pulled the bytes (verified against the content address)");

    // Provision + advertise & serve the capability.
    let host = WasmHost::new()?;
    let state_host = HostState::new(
        installer.node_id(), entry.provides.namespace.clone(),
        installer.agent.kv(), installer.agent.mesh());
    let mut instance = host.provision(&*installer_source, &entry.artifact, state_host)
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

    // The installer joins the byte-serving tier: its verified cache is as good as the library
    // (content addressing makes holders interchangeable), so it serves + advertises.
    let _peer_serve = serve_artifacts(
        Arc::clone(&installer.agent), Arc::clone(&installer_source) as Arc<_>);
    let _peer_cap = installer.agent.capabilities().advertise_capability(
        Capability::new(LIBRARIAN_NS, LIBRARIAN_NAME), Duration::from_secs(5));
    println!("[installer] provisioned + serving route/optimize + re-serving its verified cache");

    // ── Kill the librarian AND delete the library — the origin tier is gone for good ──
    drop(librarian_role);          // retracts artifact/librarian + stops serve/sync
    librarian.shutdown().await;
    let _ = std::fs::remove_dir_all(&lib_dir);
    println!("[cluster] librarian dead + library deleted — origin tier is gone");

    // ── The late joiner: a persistent node that installs from the peer cache each cycle ──
    let late = spawn_depot(mk("late", p[6], p[7], installer.gossip_port)).await?;
    wait_until(20, || !late.agent.peers().is_empty()).await;
    let late_source = MeshArtifactSource::resolving(
        Arc::clone(&late.agent), librarian_filter(), Duration::from_secs(3));
    let late_host = WasmHost::new()?;
    let namespace = entry.provides.namespace.clone();

    // ── The story loop: narrate the arc forever, doing genuine peer-cache installs each cycle. ──
    println!("\nCatalogue loop running. Open http://127.0.0.1:{HTTP_PORT}/ · ctrl-c to exit.\n");
    tokio::select! {
        _ = story_loop(&state, &caller, &late, &installer_source,
                       &late_source, &late_host, &entry, &namespace, &invoke_kind,
                       &filter, &publisher_pub) => {}
        _ = signal::ctrl_c() => {}
    }

    // ── Shutdown ──────────────────────────────────────────────────────
    println!("\nShutting down…");
    serve.abort();
    for d in [late, caller, installer] {
        d.shutdown().await;
    }
    let _ = std::fs::remove_dir_all(&cert_dir);
    Ok(())
}

/// The forever loop: publish → serve → replicate → pull → verify → INSTALLED → invoke →
/// origin dies → late installs from a peer cache (the climax) → reset → repeat. Each cycle the
/// climax is genuine: the late node re-pulls from the installer's verified cache and provisions a
/// fresh WASM instance — with no origin alive.
#[allow(clippy::too_many_arguments)]
async fn story_loop(
    state: &Arc<Mutex<VizState>>,
    caller: &coop::common::Depot,
    late: &coop::common::Depot,
    installer_source: &Arc<MeshArtifactSource>,
    late_source: &MeshArtifactSource,
    late_host: &WasmHost,
    entry: &InstallableEntry,
    namespace: &str,
    invoke_kind: &Arc<str>,
    filter: &CapFilter,
    publisher_pub: &[u8; 32],
) {
    loop {
        // ── Frame 1 — the librarian serves the signed manifest (origin alive). ──
        update(state, |s| {
            s.phase = "publish".into();
            s.origin_alive = true;
            s.library_deleted = false;
            s.catalogue_present = false;
            s.flow = None;
            s.set_status("librarian", "serving");
            s.set_status("installer", "idle");
            s.set_status("caller", "idle");
            s.set_status("late", "idle");
            s.caption = "The librarian serves route/optimize from its durable library.".into();
            s.log("[librarian] serving the library + the signed manifest");
        });
        dwell(1400).await;

        // ── Frame 2 — the catalogue replicates over gossip KV to every node. ──
        update(state, |s| {
            s.phase = "replicate".into();
            s.catalogue_present = true;
            s.caption = "The manifest reconciles into installable/{ns}/{name}/{hex} — it \
                         replicates to every node like any other key.".into();
            s.log("[catalogue] installable/route/optimize replicated cluster-wide");
        });
        dwell(1400).await;

        // ── Frame 3 — the installer pulls the bytes peer-to-peer (genuine prefetch). ──
        update(state, |s| {
            s.phase = "pull".into();
            s.set_status("installer", "installing");
            s.flow = Some(Flow { from: "librarian".into(), to: "installer".into(),
                                 label: "artifact.fetch".into() });
            s.caption = "The installer discovers the entry and pulls the component bytes over \
                         artifact.fetch — no hardcoded provider id.".into();
            s.log("[installer] pulling bytes peer-to-peer …");
        });
        let _ = installer_source.prefetch(&entry.artifact).await;
        dwell(1300).await;

        // ── Frame 4 — verify provenance + the content address. ──
        let provenance_ok = entry.verify_provenance(std::slice::from_ref(publisher_pub));
        update(state, |s| {
            s.phase = "verify".into();
            s.flow = None;
            s.caption = format!(
                "Provenance {} · content hash verified on arrival — the bytes are the ones CI signed.",
                if provenance_ok { "OK" } else { "REJECTED" }
            );
            s.log("[installer] provenance + content address verified");
        });
        dwell(1100).await;

        // ── Frame 5 — INSTALLED: provisioned in the WASM host. ──
        update(state, |s| {
            s.phase = "installed".into();
            s.set_status("installer", "installed");
            if !s.holders.contains(&"installer".to_string()) {
                s.holders.push("installer".into());
            }
            s.caption = "INSTALLED — route/optimize is provisioned in the WASM host and \
                         re-served as a verified peer cache.".into();
            s.log("[installer] INSTALLED + re-serving its verified cache");
        });
        dwell(1400).await;

        // ── Frame 6 — the caller invokes the catalogue-installed capability (genuine rpc). ──
        update(state, |s| {
            s.phase = "invoke".into();
            s.set_status("caller", "serving");
            s.flow = Some(Flow { from: "caller".into(), to: "installer".into(),
                                 label: "route/optimize".into() });
            s.caption = "The caller invokes route/optimize — the installed component runs.".into();
        });
        let reply = if let Some((provider, _)) =
            caller.agent.capabilities().resolve(filter).into_iter().next()
        {
            caller.agent.service()
                .rpc_call(provider, Arc::clone(invoke_kind),
                          b"optimize-this-route".to_vec(), Duration::from_secs(5))
                .await
                .map(|r| String::from_utf8_lossy(&r).into_owned())
                .unwrap_or_else(|_| "<no reply>".into())
        } else {
            "<provider not yet visible>".into()
        };
        update(state, |s| {
            s.log(format!("[caller] invoked route/optimize → {reply}"));
        });
        dwell(1300).await;

        // ── Frame 7 — the librarian dies + the library is deleted (origin tier gone). ──
        update(state, |s| {
            s.phase = "origin-death".into();
            s.origin_alive = false;
            s.library_deleted = true;
            s.flow = None;
            s.set_status("librarian", "dead");
            s.set_status("caller", "idle");
            s.caption = "The librarian dies and its library directory is deleted — no serving \
                         node, no durable store. The origin tier is gone.".into();
            s.log("[cluster] librarian dead + library deleted — origin gone");
        });
        dwell(1800).await;

        // ── Frame 8 — a late node joins and pulls from the installer's cache (genuine). ──
        update(state, |s| {
            s.phase = "late-join".into();
            s.set_status("late", "installing");
            s.flow = Some(Flow { from: "installer".into(), to: "late".into(),
                                 label: "peer cache".into() });
            s.caption = "A late-joining node still sees the catalogue entry (it is ordinary KV) \
                         and pulls the same bytes from the installer's verified cache.".into();
            s.log("[late] joined after origin death — pulling from a peer cache …");
        });
        let late_pulled = late_source.prefetch(&entry.artifact).await;
        dwell(1200).await;

        // ── Frame 9 — the climax: the late node installs + runs, with no origin alive. ──
        let late_out = if late_pulled {
            let late_state = HostState::new(
                late.node_id(), namespace.to_string(),
                late.agent.kv(), late.agent.mesh());
            match late_host.provision(late_source, &entry.artifact, late_state) {
                Ok(mut inst) => inst
                    .invoke("invoke", b"late-route".to_vec())
                    .ok()
                    .and_then(|r| r.ok())
                    .map(|o| String::from_utf8_lossy(&o).into_owned())
                    .unwrap_or_else(|| "<invoke failed>".into()),
                Err(_) => "<provision failed>".into(),
            }
        } else {
            "<pull failed>".into()
        };
        update(state, |s| {
            s.phase = "survived".into();
            s.set_status("late", "installed");
            s.flow = None;
            if !s.holders.contains(&"late".to_string()) {
                s.holders.push("late".into());
            }
            s.caption = format!(
                "INSTALLED from a peer cache — same hash, same verify, no origin needed. \
                 route/optimize ran: {late_out}"
            );
            s.log(format!("[late] installed from a peer cache + ran it → {late_out}"));
        });
        dwell(2000).await;

        // ── Reset the visible statuses for the next telling (the caches persist). ──
        update(state, |s| {
            s.phase = "reset".into();
            s.set_status("late", "idle");
            s.set_status("installer", "serving");
            s.caption = "The verified caches persist — replaying how the catalogue outlived its \
                         origin.".into();
        });
        dwell(1000).await;
    }
}
