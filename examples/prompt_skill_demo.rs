//! Prompt Skills demo — two in-process nodes, EchoBackend, no LLM required.
//!
//! What it demonstrates:
//!   1. Node A registers a `demo/echo` skill backed by EchoBackend
//!   2. Node B discovers it via the capability ring
//!   3. Node B calls the skill — output echoes the rendered user_template
//!   4. Node A updates the template live; Node B's next call reads the new version from KV
//!   5. `list_prompts()` and `get_prompt()` reflect the KV snapshot on any node
//!
//! Usage:
//!   cargo run --example prompt_skill_demo --features llm

use mycelium::{CapFilter, EchoBackend, GossipAgent, GossipConfig, LlmBackend, NodeId, PromptTemplate};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt().with_max_level(tracing::Level::INFO).init();

    // ── Node A — skill host (port 7960) ────────────────────────────────────────
    let id_a = NodeId::new("127.0.0.1", 7960)?;
    let mut cfg_a = GossipConfig::default();
    cfg_a.bind_port = 7960;
    let agent_a = Arc::new(GossipAgent::new(id_a, cfg_a));
    agent_a.start().await?;

    // Template stored in cluster KV (TTL = 1 week).
    // No `model` field — model availability is node-local knowledge baked into the backend.
    let template = PromptTemplate {
        system: "You are a demo assistant.".into(),
        user_template: "Input was: {{input}}".into(),
        max_tokens: 128,
        temperature: 0.7,
        metadata: HashMap::new(),
    };
    // EchoBackend returns "echo: <rendered user prompt>" — no Ollama / API key required.
    let backend: Arc<dyn LlmBackend> = Arc::new(EchoBackend);
    let _skill = agent_a.register_prompt_skill("demo", "echo", template, backend).await?;
    println!("A: registered demo/echo (EchoBackend, port 7960)");

    // ── Node B — caller (port 7961, bootstrapped off A) ────────────────────────
    let id_b = NodeId::new("127.0.0.1", 7961)?;
    let mut cfg_b = GossipConfig::default();
    cfg_b.bind_port = 7961;
    cfg_b.bootstrap_peers = vec![NodeId::new("127.0.0.1", 7960)?];
    let agent_b = Arc::new(GossipAgent::new(id_b, cfg_b));
    agent_b.start().await?;
    println!("B: started on port 7961, waiting for demo/echo on the mesh…");

    // Poll until the capability gossips from A to B (typically < 1 s).
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if !agent_b.resolve(&CapFilter::new("demo", "echo")).is_empty() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }).await.map_err(|_| "timed out: demo/echo capability never appeared on B")?;
    println!("B: demo/echo visible in capability ring");

    // ── Call 1 ─────────────────────────────────────────────────────────────────
    // B resolves a provider (→ A), sends llm.invoke RPC, A dispatches to EchoBackend.
    let out1 = agent_b.call_prompt_skill(
        "demo", "echo",
        "Hello, Mycelium!",
        HashMap::new(),
        Duration::from_secs(5),
    ).await?;
    println!("B → A call 1:  {out1:?}");
    assert!(out1.contains("Hello, Mycelium!"), "EchoBackend should echo the input");

    // ── Template visible on B's KV snapshot ───────────────────────────────────
    // Template propagates via gossip KV — readable without an extra RPC.
    tokio::time::sleep(Duration::from_millis(300)).await;

    let list = agent_b.list_prompts();
    println!("B: list_prompts()  → {list:?}");
    assert!(list.iter().any(|(ns, n)| ns == "demo" && n == "echo"));

    let tpl = agent_b.get_prompt("demo", "echo").expect("template should be in KV");
    println!("B: user_template   = {:?}", tpl.user_template);

    // ── Live template update ──────────────────────────────────────────────────
    // A writes a new template to KV — the serving dispatch loop reads KV on every
    // invocation, so the next call picks up the change immediately, no restart needed.
    agent_a.update_prompt("demo", "echo", PromptTemplate {
        system: "Updated assistant.".into(),
        user_template: "v2: {{input}}".into(),
        max_tokens: 64,
        temperature: 0.0,
        metadata: HashMap::new(),
    })?;
    println!("A: template updated  (user_template → \"v2: {{{{input}}}}\")");

    // Give gossip a moment to propagate the KV write to B.
    tokio::time::sleep(Duration::from_millis(400)).await;
    let updated = agent_b.get_prompt("demo", "echo").expect("template still present");
    assert_eq!(updated.user_template, "v2: {{input}}");
    println!("B: sees live update  user_template = {:?}", updated.user_template);

    // ── Call 2 — dispatch reads updated template from KV ──────────────────────
    let out2 = agent_b.call_prompt_skill(
        "demo", "echo",
        "world",
        HashMap::new(),
        Duration::from_secs(5),
    ).await?;
    println!("B → A call 2:  {out2:?}");
    assert!(out2.contains("world"), "call 2 should echo the input");

    println!("\nAll assertions passed.");

    agent_a.shutdown().await;
    agent_b.shutdown().await;
    Ok(())
}
