//! Example 09 — **an LLM agent grows the fabric's toolset at runtime** (MCP + demand).
//!
//! An LLM agent, mid-task, finds it needs a tool the fabric doesn't yet offer — a unit converter.
//! It **declares the requirement**; a tool-host node, watching demand, **loads the MCP tool into
//! itself and offers it out** (`register_mcp_tool` → `tools/unit-convert/{host}`); the agent then
//! **discovers and invokes** the freshly-loaded tool over the MCP path and finishes its task.
//!
//! This is the agentic self-extension loop: the fabric's capability surface grows because an agent
//! asked for it — no operator wired the tool in advance, no coordinator decided who hosts it.
//!
//!   • `tool-host` — can provide a `unit-convert` MCP tool, but runs **dark** until demand appears.
//!   • `llm-agent` — processes a donation; needs kg→tonnes; declares the requirement, waits for the
//!                   tool to be offered, invokes it, then uses its model to compose the result.
//!
//! Run:  cargo run -p mycelium-coop-examples --bin mcp_toolgrowth

use std::sync::Arc;
use std::time::Duration;

use coop::common::{alloc_ports, spawn_depot, DepotOpts};
use mycelium::{Capability, CapFilter, EchoBackend, LlmBackend, signal_kind};
use serde_json::json;

const TOOL: &str = "unit-convert";

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
    let p = alloc_ports(4);

    // ── tool-host: can offer a unit-convert MCP tool, but runs dark for now ──────
    let host = spawn_depot(DepotOpts {
        name: "tool-host".into(), gossip_port: p[0], http_port: p[1],
        zone: "depot-a".into(), bootstrap: vec![], cert_dir: cert_dir.clone(), health_secs: Some(2),
    }).await?;
    println!("[tool-host] up — can provide '{TOOL}', running dark (nothing registered yet)");

    // ── llm-agent: an LLM-backed depot processing a donation ────────────────────
    let agent = spawn_depot(DepotOpts {
        name: "llm-agent".into(), gossip_port: p[2], http_port: p[3],
        zone: "depot-b".into(), bootstrap: vec![p[0]], cert_dir: cert_dir.clone(), health_secs: Some(2),
    }).await?;
    println!("[llm-agent] up — needs to report a donation weight in tonnes");

    wait_until(20, || !agent.agent.peers().is_empty() && !host.agent.peers().is_empty()).await;

    // ── tool-host watches demand for tool/unit-convert; loads the MCP tool on demand ──
    let host_agent = Arc::clone(&host.agent);
    let host_loop = tokio::spawn(async move {
        let filter = CapFilter::new("tool", TOOL);
        let mut _tool_handle = None;
        let mut _cap = None;
        loop {
            let demand = host_agent.capabilities().demand(&filter);
            let unmet = demand.providers.is_empty() && !demand.demanding_nodes.is_empty();
            if unmet && _tool_handle.is_none() {
                // Load the MCP tool INTO this node and offer it out (writes tools/unit-convert/{self}).
                let schema = json!({
                    "name": TOOL,
                    "description": "Convert a mass in kilograms to tonnes.",
                    "inputSchema": {"type": "object", "properties": {"kg": {"type": "number"}}},
                });
                _tool_handle = Some(host_agent.mcp().register_mcp_tool(TOOL, schema, |args| async move {
                    let kg = args["kg"].as_f64().unwrap_or(0.0);
                    Ok(json!({"tonnes": kg / 1000.0}))
                }));
                // Advertise the matching capability so the requirement resolves (demand relieved).
                _cap = Some(host_agent.capabilities()
                    .advertise_capability(Capability::new("tool", TOOL), Duration::from_secs(30)));
                println!("[tool-host] saw unmet demand for tool/{TOOL} → loaded the MCP tool and offered it out");
            }
            tokio::time::sleep(Duration::from_millis(300)).await;
        }
    });

    // ── Phase 1 — the agent finds the tool missing and declares the requirement ─
    assert!(tool_offered(&agent.agent).is_none(), "the tool is not offered before it's needed");
    println!("[llm-agent] '{TOOL}' is not in the fabric yet — declaring the requirement");
    let _req = agent.agent.capabilities()
        .declare_requirement(CapFilter::new("tool", TOOL), Duration::from_secs(60));

    // ── Phase 2 — the tool appears (loaded on demand); the agent invokes it ─────
    let appeared = wait_until(25, || tool_offered(&agent.agent).is_some()).await;
    assert!(appeared, "the tool-host must load + offer the tool once demand appears");
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

    println!("\nAll assertions passed — an LLM agent grew the fabric's toolset at runtime: declared a need, the tool was loaded on demand, then invoked.");

    host_loop.abort();
    agent.shutdown().await;
    host.shutdown().await;
    let _ = std::fs::remove_dir_all(&cert_dir);
    Ok(())
}
