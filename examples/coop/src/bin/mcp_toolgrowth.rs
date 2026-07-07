//! Example 09 — **an LLM agent grows the fabric's toolset at runtime** (MCP + demand + a real
//! code arrival).
//!
//! An LLM agent, mid-task, finds it needs a tool the fabric doesn't yet offer — a unit
//! converter. It **declares the requirement**; a tool-host node, watching demand, **installs the
//! tool**: the converter's arithmetic lives in a WASM component that **arrives over the mesh**
//! (discovered via the catalogue, pulled by content address from a librarian, verified,
//! instantiated) and is then **bridged** as an MCP tool (`register_mcp_tool` →
//! `tools/unit-convert/{host}` + a `tool/` capability so the demand resolves). The agent
//! discovers and invokes it over the MCP path and finishes its task.
//!
//! **Activation vs installation — the distinction this demo teaches.** The tool-host also has a
//! trivial `ping` tool *compiled in*, which it merely **activates** (registers) at startup:
//! turning on code you already shipped is *activation*. The converter is *installation*: no node
//! has its logic compiled in — `grep` this file for arithmetic; there is none — the code arrives
//! as verified bytes at runtime. Both are legitimate; don't mistake the first for the second.
//!
//!   • CI (plain code, no node) — stores the converter component in a durable library + signed
//!     manifest (see the `catalog` demo for the library pattern in full).
//!   • `library` — a librarian node: serves the bytes, advertises `artifact/librarian`, syncs
//!     the manifest into the catalogue.
//!   • `tool-host` — runs **dark** (only `ping` activated) until demand appears; then installs
//!     the arrived component and offers it as an MCP tool.
//!   • `llm-agent` — declares the requirement, waits, invokes the tool, composes the result.
//!
//! Run:  cargo run -p mycelium-coop-examples --features wasm --bin mcp_toolgrowth

use std::sync::Arc;
use std::time::Duration;

use coop::common::{alloc_ports, spawn_depot, DepotOpts};
use ed25519_dalek::SigningKey;
use mycelium::{Capability, CapFilter, EchoBackend, LlmBackend, signal_kind};
use mycelium_wasm_host::{
    librarian_filter, spawn_librarian, FsLibrarySource, HostState, InstallableCatalog,
    InstallableEntry, LibrarianConfig, Manifest, MeshArtifactSource, WasmHost, MANIFEST_FILE,
};
use serde_json::json;

const TOOL: &str = "unit-convert";

/// Read at **runtime** — the converter's logic is in this component, not in this binary.
const CONVERTER_WASM_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../mycelium-wasm-host/tests/fixtures/unit_convert_component.wasm"
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

/// Does this node's gossip view yet show a provider offering the MCP `TOOL`? (Tools live under
/// `tools/{name}/{node}` — separate from the `cap/` namespace.)
fn tool_offered(agent: &mycelium::GossipAgent) -> Option<mycelium::NodeId> {
    agent.kv().scan_prefix(&format!("tools/{TOOL}/")).into_iter().find_map(|(key, _)| {
        key.strip_prefix(&format!("tools/{TOOL}/")).and_then(|n| n.parse().ok())
    })
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt().with_max_level(tracing::Level::WARN).init();

    let cert_dir = std::env::temp_dir().join(format!("coop-mcp-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&cert_dir);
    let p = alloc_ports(6);

    // ── Phase 0 — CI publishes the converter component into the library ──────────
    let lib_dir = std::env::temp_dir().join(format!("coop-mcp-lib-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&lib_dir);
    let publisher_key = SigningKey::from_bytes(&[43u8; 32]);
    let publisher_pub = publisher_key.verifying_key().to_bytes();

    let converter_wasm = std::fs::read(CONVERTER_WASM_PATH)?; // runtime read — no include_bytes!
    let library_src = Arc::new(FsLibrarySource::open(&lib_dir)?);
    let artifact_id = library_src.store(&converter_wasm)?;
    let entry = InstallableEntry::new(Capability::new("tool", TOOL), artifact_id)
        .with_cost(converter_wasm.len() as u64, 1)
        .signed_by(&publisher_key);
    Manifest::from_entries(vec![entry]).save(&lib_dir.join(MANIFEST_FILE))?;
    println!("[ci] converter component stored in the library (its arithmetic is NOT in this binary)");

    // ── library node: the librarian role ─────────────────────────────────────────
    let library = spawn_depot(DepotOpts {
        name: "library".into(), gossip_port: p[0], http_port: p[1],
        zone: "hub".into(), bootstrap: vec![], cert_dir: cert_dir.clone(), health_secs: Some(2),
    }).await?;
    let seed = library.gossip_port;
    let _librarian = spawn_librarian(
        Arc::clone(&library.agent),
        Arc::clone(&library_src) as Arc<_>,
        LibrarianConfig {
            manifest_path: lib_dir.join(MANIFEST_FILE),
            publisher: publisher_pub,
            sync_interval: Duration::from_millis(500),
        },
    );
    println!("[library] librarian up — serving bytes + syncing the manifest into the catalogue");

    // ── tool-host + llm-agent ─────────────────────────────────────────────────────
    let host = spawn_depot(DepotOpts {
        name: "tool-host".into(), gossip_port: p[2], http_port: p[3],
        zone: "depot-a".into(), bootstrap: vec![seed], cert_dir: cert_dir.clone(), health_secs: Some(2),
    }).await?;
    // CONTRAST — activation: `ping` was compiled into this binary; registering it just turns
    // existing code on. No new code arrives. (Installation is what happens to the converter.)
    let _ping = host.agent.mcp().register_mcp_tool(
        "ping",
        json!({"name": "ping", "description": "liveness echo (compiled-in)"}),
        |_args| async move { Ok(json!({"pong": true})) },
    );
    println!("[tool-host] up — 'ping' ACTIVATED (code was already here); '{TOOL}' not present in any form");

    let agent = spawn_depot(DepotOpts {
        name: "llm-agent".into(), gossip_port: p[4], http_port: p[5],
        zone: "depot-b".into(), bootstrap: vec![seed], cert_dir: cert_dir.clone(), health_secs: Some(2),
    }).await?;
    println!("[llm-agent] up — needs to report a donation weight in tonnes");

    wait_until(20, || !agent.agent.peers().is_empty() && !host.agent.peers().is_empty()).await;

    // ── tool-host watches demand; INSTALLS the tool when the fabric asks ─────────
    let host_agent = Arc::clone(&host.agent);
    let host_loop = tokio::spawn(async move {
        let filter = CapFilter::new("tool", TOOL);
        let mut _tool_handle = None;
        let mut _cap = None;
        loop {
            let demand = host_agent.capabilities().demand(&filter);
            let unmet = demand.providers.is_empty() && !demand.demanding_nodes.is_empty();
            if unmet && _tool_handle.is_none() {
                // 1. Resolve the *catalogue*: is there an installable artifact providing tool/unit-convert?
                let Some(entry) =
                    InstallableCatalog::from_kv(&host_agent.kv()).resolve_best(&filter).cloned()
                else {
                    tokio::time::sleep(Duration::from_millis(300)).await;
                    continue; // catalogue entry not gossiped yet
                };
                // 2. Provenance: only install what a trusted publisher vouched for.
                if !entry.verify_provenance(&[publisher_pub]) {
                    tokio::time::sleep(Duration::from_millis(300)).await;
                    continue;
                }
                // 3. Pull the bytes from a *discovered* holder (capability ring), verified.
                let mesh = MeshArtifactSource::resolving(
                    Arc::clone(&host_agent), librarian_filter(), Duration::from_secs(3));
                if !mesh.prefetch(&entry.artifact).await {
                    tokio::time::sleep(Duration::from_millis(300)).await;
                    continue; // holder not reachable yet — retry
                }
                // 4. Instantiate the arrived component: the converter's code is now on this node.
                let state = HostState::new(
                    host_agent.node_id().clone(), entry.provides.namespace.clone(),
                    host_agent.kv(), host_agent.mesh());
                let instance = match WasmHost::new().and_then(|h| h.provision(&mesh, &entry.artifact, state)) {
                    Ok(i) => i,
                    Err(e) => {
                        eprintln!("[tool-host] provision failed ({e}) — retrying");
                        tokio::time::sleep(Duration::from_millis(300)).await;
                        continue;
                    }
                };
                println!("[tool-host] unmet demand for tool/{TOOL} → the converter code arrived over the mesh (pulled, verified, instantiated)");

                // 5. Bridge it as an MCP tool: the handler is a thin shim — the arithmetic runs
                //    inside the sandboxed component that just arrived.
                let schema = json!({
                    "name": TOOL,
                    "description": "Convert a mass in kilograms to tonnes (runs in an installed WASM component).",
                    "inputSchema": {"type": "object", "properties": {"kg": {"type": "number"}}},
                });
                let inst = Arc::new(std::sync::Mutex::new(instance));
                _tool_handle = Some(host_agent.mcp().register_mcp_tool(TOOL, schema, move |args| {
                    let inst = Arc::clone(&inst);
                    async move {
                        let out = {
                            let mut i = inst.lock().unwrap();
                            i.invoke("invoke", args.to_string().into_bytes())
                                .map_err(|e| format!("host: {e}"))?
                                .map_err(|e| format!("component: {e}"))?
                        };
                        serde_json::from_slice::<serde_json::Value>(&out)
                            .map_err(|e| format!("bad component json: {e}"))
                    }
                }));
                // 6. Advertise the matching capability so the requirement resolves (demand relieved).
                _cap = Some(host_agent.capabilities()
                    .advertise_capability(Capability::new("tool", TOOL), Duration::from_secs(30)));
                println!("[tool-host] INSTALLED + offered tool/{TOOL} (bridged over mcp.invoke)");
            }
            tokio::time::sleep(Duration::from_millis(300)).await;
        }
    });

    // ── Phase 1 — the agent finds the tool missing and declares the requirement ─
    assert!(tool_offered(&agent.agent).is_none(), "the tool is not offered before it's needed");
    println!("[llm-agent] '{TOOL}' is not in the fabric yet — declaring the requirement");
    let _req = agent.agent.capabilities()
        .declare_requirement(CapFilter::new("tool", TOOL), Duration::from_secs(60));

    // ── Phase 2 — the tool appears (installed on demand); the agent invokes it ──
    let appeared = wait_until(30, || tool_offered(&agent.agent).is_some()).await;
    assert!(appeared, "the tool-host must install + offer the tool once demand appears");
    let provider = tool_offered(&agent.agent).expect("a tool provider node");
    println!("[llm-agent] '{TOOL}' is now offered by {provider} — invoking it over MCP");

    let call = json!({
        "jsonrpc": "2.0", "id": 1, "method": "tools/call",
        "params": {"name": TOOL, "arguments": {"kg": 5000.0}},
    });
    let reply = agent.agent.service()
        .rpc_call(provider, signal_kind::MCP_INVOKE, call.to_string().into_bytes(), Duration::from_secs(5))
        .await?;
    let resp: serde_json::Value = serde_json::from_slice(&reply)?;
    let tool_text = resp["result"]["content"][0]["text"].as_str().unwrap_or("");
    println!("[llm-agent] MCP tool returned: {tool_text}");
    assert!(tool_text.contains("5") && tool_text.contains("tonnes"),
        "the converter must report 5 tonnes for 5000 kg — got {tool_text}");

    // ── Phase 3 — the agent uses its model to compose the final report ──────────
    let backend: Arc<dyn LlmBackend> = Arc::new(EchoBackend);
    let summary = backend.complete(
        "You write a one-line donation receipt.",
        &format!("Donation accepted; converted weight = {tool_text}"), 64, 0.0).await?;
    println!("[llm-agent] composed report: {}", summary.output);
    assert!(summary.output.contains("tonnes"), "the agent's report incorporates the tool result");

    println!("\nAll assertions passed — an LLM agent grew the fabric's toolset at runtime: declared a need, the tool's code ARRIVED (catalogue → pull → verify → instantiate), was bridged over MCP, and invoked. Activation ≠ installation: ping was activated; the converter was installed.");

    host_loop.abort();
    agent.shutdown().await;
    host.shutdown().await;
    library.shutdown().await;
    let _ = std::fs::remove_dir_all(&cert_dir);
    let _ = std::fs::remove_dir_all(&lib_dir);
    Ok(())
}
