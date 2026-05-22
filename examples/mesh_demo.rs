//! Generic Mesh Demo — manifest-driven virgin-agent provisioning.
//!
//! One persistent management agent (port 56000) serves HTTP on port 8100.
//! On preset apply, fresh `GossipAgent` instances are provisioned per manifest
//! group. Each node's behavior loop is selected entirely by its capability
//! `(ns, name)` — no hardcoded demo topology.
//!
//! # Run
//! ```sh
//! MOCK_LLM=1 cargo run --example mesh_demo   # no Ollama needed
//! cargo run --example mesh_demo               # real Ollama
//! ```
//! Open **http://127.0.0.1:8100**

use bytes::Bytes;
use mycelium::{
    AgentPolicy, AgentStateMachine, Capability, CapabilityHandle,
    CapFilter, CapValue, ExecutionState, GossipAgent, GossipConfig,
    McpToolHandle, MeshManifest, NodeId, manifest_keys, semver_gt,
    signal_kind, SignalScope,
};
use serde_json::{json, Value};
use std::{
    sync::{
        atomic::{AtomicBool, AtomicU16, AtomicU64, Ordering},
        Arc, Mutex, RwLock,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{task::AbortHandle, time};

// ── Port allocation ───────────────────────────────────────────────────────────

const MGMT_PORT: u16 = 56000;
const HTTP_PORT: u16 = 8100;
const SETTLE_MS: u64 = 1_200;

static NEXT_PORT: AtomicU16 = AtomicU16::new(56001);
fn alloc_port() -> u16 { NEXT_PORT.fetch_add(1, Ordering::Relaxed) }

// ── Helpers ───────────────────────────────────────────────────────────────────

fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64
}

fn fmt_time(ms: u64) -> String {
    let s = ms / 1000;
    format!("{:02}:{:02}:{:02}.{:03}", s / 3600 % 24, s / 60 % 60, s % 60, ms % 1000)
}

fn make_agent(port: u16, peers: &[u16]) -> Arc<GossipAgent> {
    let nid = NodeId::new("127.0.0.1", port).unwrap();
    let mut cfg = GossipConfig::default();
    cfg.bind_address               = "127.0.0.1".to_string();
    cfg.bind_port                  = port;
    cfg.default_ttl                = 20;
    cfg.reconnect_backoff_secs     = 1;
    cfg.gossip_shards              = 1;
    cfg.health_check_max_jitter_ms = 100;
    cfg.bootstrap_peers = peers.iter()
        .map(|&p| NodeId::new("127.0.0.1", p).unwrap())
        .collect();
    Arc::new(GossipAgent::new(nid, cfg))
}

// ── Shared log ────────────────────────────────────────────────────────────────

#[derive(Clone)]
struct LogEntry { time_ms: u64, event: String, detail: String }

type SharedLog = Arc<Mutex<Vec<LogEntry>>>;

fn push_log(log: &SharedLog, event: impl Into<String>, detail: impl Into<String>) {
    let mut l = log.lock().unwrap();
    if l.len() > 200 { l.drain(0..50); }
    l.push(LogEntry { time_ms: now_ms(), event: event.into(), detail: detail.into() });
}

// ── Traffic events ─────────────────────────────────────────────────────────────

#[derive(Clone)]
struct TrafficEvent { from: String, to: String, ts_ms: u64, kind: String }

type SharedTraffic = Arc<Mutex<Vec<TrafficEvent>>>;

fn push_traffic(t: &SharedTraffic, from: &str, to: &str, kind: &str) {
    let mut v = t.lock().unwrap();
    if v.len() > 80 { v.drain(0..30); }
    v.push(TrafficEvent { from: from.to_string(), to: to.to_string(),
                          ts_ms: now_ms(), kind: kind.to_string() });
}

// ── Tool handlers ─────────────────────────────────────────────────────────────

async fn handle_weather(args: Value) -> Value {
    let city = args["city"].as_str().unwrap_or("Unknown");
    let temp = 15 + (city.len() as i32 % 20);
    json!({ "city": city, "temp_c": temp,
            "condition": if temp > 20 { "sunny" } else if temp > 10 { "cloudy" } else { "rainy" } })
}

async fn handle_ping(args: Value) -> Value {
    json!({ "host": args["host"].as_str().unwrap_or("localhost"), "latency_ms": 12, "status": "reachable" })
}

async fn handle_search(args: Value) -> Value {
    let q = args["query"].as_str().unwrap_or("");
    json!({ "query": q, "results": [
        { "title": format!("{q} — Overview"), "url": "https://example.com/1" },
        { "title": format!("{q} — Deep dive"), "url": "https://example.com/2" },
    ]})
}

async fn handle_calculate(args: Value) -> Value {
    let expr = args["expression"].as_str().unwrap_or("0");
    let result: f64 = (|| {
        let p: Vec<&str> = expr.split_whitespace().collect();
        if p.len() == 3 {
            let a: f64 = p[0].parse().ok()?;
            let b: f64 = p[2].parse().ok()?;
            match p[1] {
                "+" => Some(a + b), "-" => Some(a - b),
                "*" => Some(a * b),
                "/" => if b != 0.0 { Some(a / b) } else { None },
                _   => None,
            }
        } else { None }
    })().unwrap_or(f64::NAN);
    json!({ "expression": expr, "result": result })
}

// ── Tool registration helpers ─────────────────────────────────────────────────

struct ToolDef { name: &'static str, description: &'static str, params: Value }

fn register_tool(
    agent:   &Arc<GossipAgent>,
    def:     &ToolDef,
    handler: impl Fn(Value) -> std::pin::Pin<Box<dyn std::future::Future<Output = Value> + Send + 'static>>
             + Send + Sync + 'static,
) -> McpToolHandle {
    let schema  = json!({ "description": def.description, "inputSchema": def.params });
    let handler = Arc::new(handler);
    agent.register_mcp_tool(def.name, schema, move |args: Value| {
        let h = Arc::clone(&handler);
        Box::pin(async move { Ok::<Value, String>(h(args).await) })
    })
}

fn register_realtime_tools(agent: &Arc<GossipAgent>) -> Vec<McpToolHandle> {
    vec![
        register_tool(agent, &ToolDef {
            name: "weather", description: "Get current weather for a city",
            params: json!({"type":"object","properties":{"city":{"type":"string"}},"required":["city"]}),
        }, |args| Box::pin(handle_weather(args))),
        register_tool(agent, &ToolDef {
            name: "ping", description: "Ping a host and return latency",
            params: json!({"type":"object","properties":{"host":{"type":"string"}},"required":["host"]}),
        }, |args| Box::pin(handle_ping(args))),
    ]
}

fn register_compute_tools(agent: &Arc<GossipAgent>) -> Vec<McpToolHandle> {
    vec![
        register_tool(agent, &ToolDef {
            name: "search", description: "Search the web for information",
            params: json!({"type":"object","properties":{"query":{"type":"string"}},"required":["query"]}),
        }, |args| Box::pin(handle_search(args))),
        register_tool(agent, &ToolDef {
            name: "calculate", description: "Evaluate a simple arithmetic expression",
            params: json!({"type":"object","properties":{"expression":{"type":"string"}},"required":["expression"]}),
        }, |args| Box::pin(handle_calculate(args))),
    ]
}

// ── LLM config + planning ─────────────────────────────────────────────────────

struct LlmConfig {
    base_url: String,
    api_key:  String,
    model:    String,
    mock:     bool,
}

impl LlmConfig {
    fn from_env() -> Self {
        Self {
            base_url: std::env::var("OPENAI_BASE_URL")
                .unwrap_or_else(|_| "http://localhost:11434/v1".into()),
            api_key:  std::env::var("OPENAI_API_KEY").unwrap_or_else(|_| "ollama".into()),
            model:    std::env::var("OPENAI_MODEL").unwrap_or_else(|_| "llama3.2".into()),
            mock:     std::env::var("MOCK_LLM").map(|v| v == "1").unwrap_or(false),
        }
    }
}

const TASK_TEXT: &str =
    "What is the weather in London? Then search for 'mycelium networking' and calculate 42 * 7.";

fn discover_tools(agent: &GossipAgent) -> Vec<(String, NodeId, Value)> {
    let mut tools = Vec::new();
    for (key, schema_bytes) in agent.scan_prefix("tools/") {
        let parts: Vec<&str> = key.splitn(3, '/').collect();
        if parts.len() != 3 { continue; }
        let tool_name = parts[1].to_string();
        let Ok(node_id) = parts[2].parse::<NodeId>() else { continue };
        let Ok(schema)  = serde_json::from_slice::<Value>(&schema_bytes) else { continue };
        let input_schema = schema.get("inputSchema").cloned()
            .unwrap_or_else(|| json!({"type":"object","properties":{}}));
        let description = schema["description"].as_str().unwrap_or("").to_string();
        tools.push((tool_name.clone(), node_id, json!({
            "type": "function",
            "function": { "name": tool_name, "description": description, "parameters": input_schema }
        })));
    }
    let mut seen = std::collections::HashSet::new();
    tools.retain(|(name, _, _)| seen.insert(name.clone()));
    tools
}

async fn invoke_tool(agent: &GossipAgent, tool_name: &str, args: Value) -> Result<Value, String> {
    let entries = agent.scan_prefix(&format!("tools/{tool_name}/"));
    let (key, _) = entries.into_iter().next()
        .ok_or_else(|| format!("no provider for {tool_name}"))?;
    let parts: Vec<&str> = key.splitn(3, '/').collect();
    let node_id: NodeId = parts[2].parse()
        .map_err(|e: mycelium::GossipError| e.to_string())?;
    let rpc_req = json!({
        "jsonrpc": "2.0", "id": 1, "method": "tools/call",
        "params": { "name": tool_name, "arguments": args }
    });
    let reply = agent.rpc_call(
        node_id, signal_kind::MCP_INVOKE,
        Bytes::from(rpc_req.to_string()), Duration::from_secs(10),
    ).await.map_err(|e| e.to_string())?;
    let resp: Value = serde_json::from_slice(&reply).map_err(|e| e.to_string())?;
    if let Some(err) = resp.get("error") {
        return Err(err["message"].as_str().unwrap_or("tool error").to_string());
    }
    Ok(resp["result"].clone())
}

fn mock_plan_step(turn: usize, tools: &[(String, NodeId, Value)]) -> Option<(String, Value)> {
    let names: Vec<&str> = tools.iter().map(|(n, _, _)| n.as_str()).collect();
    match turn {
        0 => names.iter().find(|&&n| n == "weather")
                .map(|&n| (n.to_string(), json!({"city":"London"}))),
        1 => names.iter().find(|&&n| n == "search")
                .map(|&n| (n.to_string(), json!({"query":"mycelium networking"}))),
        2 => names.iter().find(|&&n| n == "calculate")
                .map(|&n| (n.to_string(), json!({"expression":"42 * 7"}))),
        _ => None,
    }
}

async fn llm_plan_step(
    agent: &GossipAgent, cfg: &LlmConfig, messages: &[Value], tools: &[(String, NodeId, Value)],
) -> Result<Option<(String, Value)>, String> {
    let endpoint = agent.resolve(&CapFilter::new("llm", "inference")).first()
        .and_then(|(_, cap)| cap.attributes.get("endpoint"))
        .and_then(|v| if let CapValue::Text(s) = v { Some(s.as_ref().to_string()) } else { None })
        .unwrap_or_else(|| cfg.base_url.clone());
    let tool_defs: Vec<Value> = tools.iter().map(|(_, _, def)| def.clone()).collect();
    let resp = reqwest::Client::new()
        .post(format!("{endpoint}/chat/completions"))
        .bearer_auth(&cfg.api_key)
        .json(&json!({ "model": cfg.model, "messages": messages,
                        "tools": tool_defs, "tool_choice": "auto" }))
        .send().await.map_err(|e| e.to_string())?
        .json::<Value>().await.map_err(|e| e.to_string())?;
    let choice = resp["choices"].get(0).ok_or("empty choices")?;
    if choice["finish_reason"].as_str().unwrap_or("") == "tool_calls" {
        let tc   = &choice["message"]["tool_calls"][0];
        let name = tc["function"]["name"].as_str().unwrap_or("").to_string();
        let args: Value = serde_json::from_str(
            tc["function"]["arguments"].as_str().unwrap_or("{}"),
        ).unwrap_or(json!({}));
        Ok(Some((name, args)))
    } else {
        Ok(None)
    }
}

// ── Preset list ───────────────────────────────────────────────────────────────

struct Preset { id: &'static str, name: &'static str, description: &'static str, toml: &'static str }

const PRESETS: &[Preset] = &[
    Preset { id: "llm-agent",
             name: "LLM Agent Demo",
             description: "Three nodes: real-time data, compute tools, LLM inference",
             toml: include_str!("presets/llm_agent.toml") },
    Preset { id: "signal-suppress",
             name: "Signal Suppression",
             description: "Workers suppress after each invocation — emergent load balancing with no dispatcher",
             toml: include_str!("presets/compute_cluster.toml") },
    Preset { id: "multi-agent-mesh",
             name: "Multi-Agent Mesh",
             description: "Planners, fact-checkers, and synthesizers collaborate via gossip RPC",
             toml: include_str!("presets/mcp_mesh.toml") },
    Preset { id: "consensus-cluster",
             name: "Consensus Cluster",
             description: "Voters + rotating proposers — epidemic two-phase ballot matrix",
             toml: include_str!("presets/consensus_cluster.toml") },
    Preset { id: "watchdog-cluster",
             name: "Watchdog Cluster",
             description: "Heartbeat services monitored by a quorum_persistent circuit-breaker",
             toml: include_str!("presets/watchdog_cluster.toml") },
    Preset { id: "dispatch-pool",
             name: "Dispatch Pool",
             description: "Fast and slow worker tiers with adaptive load-balancing dispatchers",
             toml: include_str!("presets/dispatch_pool.toml") },
    Preset { id: "epidemic-ring",
             name: "Epidemic Ring",
             description: "Alpha/beta ring partitions for signal propagation demos",
             toml: include_str!("presets/epidemic_ring.toml") },
    Preset { id: "minimal",
             name: "Minimal Mesh",
             description: "Single data node — development and testing",
             toml: include_str!("presets/minimal.toml") },
    Preset { id: "emergent-pool",
             name: "Emergent GPU Pool",
             description: "GPU workers self-assemble; render jobs route via signal_wired_via",
             toml: include_str!("presets/emergent_pool.toml") },
    Preset { id: "capability-market",
             name: "Capability Market",
             description: "Four capability kinds — demand pressure and dynamic advertisement",
             toml: include_str!("presets/capability_market.toml") },
    Preset { id: "locality-mesh",
             name: "Locality Mesh",
             description: "East/west render providers — resolve_with_locality routes to nearest",
             toml: include_str!("presets/locality_mesh.toml") },
];

// ── Core types ────────────────────────────────────────────────────────────────

struct MeshNode {
    agent:        Arc<GossipAgent>,
    label:        String,
    group:        String,
    ns:           String,
    cap_name:     String,
    behavior:     Option<AbortHandle>,
    cap_handles:  Vec<CapabilityHandle>,
    tool_handles: Vec<McpToolHandle>,
    pause_flag:   Arc<AtomicBool>,
}

struct MeshInstance {
    nodes: Vec<MeshNode>,
}

impl Drop for MeshInstance {
    fn drop(&mut self) {
        for node in &mut self.nodes {
            if let Some(h) = node.behavior.take() { h.abort(); }
            node.cap_handles.clear();
            node.tool_handles.clear();
            // node.agent dropped here via MeshNode — sends shutdown_tx signal
        }
    }
}

struct MgmtState {
    mgmt_agent:        Arc<GossipAgent>,
    manifest:          RwLock<MeshManifest>,
    instance:          Mutex<Option<MeshInstance>>,
    log:               SharedLog,
    traffic:           SharedTraffic,
    llm_cfg:           Arc<LlmConfig>,
    sm:                Mutex<Option<Arc<AgentStateMachine>>>,
    trigger_requested: Arc<AtomicBool>,
    call_count:        Arc<AtomicU64>,
}

// ── Behavior functions ────────────────────────────────────────────────────────

async fn run_worker_behavior(agent: Arc<GossipAgent>, suppress_secs: u64, log: SharedLog) {
    let label = agent.node_id().to_string();
    let mut rx = agent.signal_rx("compute.invoke");
    while let Some(_sig) = rx.recv().await {
        push_log(&log, "Worker", format!("{label} processing invoke"));
        agent.suppress("compute.invoke", Duration::from_secs(suppress_secs));
        push_log(&log, "Suppressed", format!("{label} refractory {suppress_secs}s"));
        time::sleep(Duration::from_millis(200)).await;
    }
}

async fn run_emitter_behavior(agent: Arc<GossipAgent>, log: SharedLog) {
    let label = agent.node_id().to_string();
    loop {
        time::sleep(Duration::from_millis(500)).await;
        let _ = agent.emit("compute.invoke", SignalScope::System, Bytes::from_static(b"invoke"));
        push_log(&log, "Emitting", format!("{label} burst → compute.invoke"));
    }
}

async fn run_dispatcher_behavior(agent: Arc<GossipAgent>, log: SharedLog) {
    let label = agent.node_id().to_string();
    loop {
        time::sleep(Duration::from_millis(800)).await;
        let workers = agent.resolve(&CapFilter::new("compute", "cpu")).len()
            + agent.resolve(&CapFilter::new("compute", "cpu-heavy")).len();
        if workers > 0 {
            let _ = agent.emit("compute.invoke", SignalScope::System, Bytes::from_static(b"dispatch"));
            push_log(&log, "Dispatch", format!("{label} routed to {workers} worker(s)"));
        }
    }
}

async fn run_coalition_planner(agent: Arc<GossipAgent>, log: SharedLog,
                                traffic: SharedTraffic) {
    let label = agent.node_id().to_string();
    loop {
        time::sleep(Duration::from_secs(3)).await;
        push_log(&log, "Planner", format!("{label} decomposing goal → delegating claim"));
        let _ = agent.emit("reasoning.task", SignalScope::System, Bytes::from_static(b"claim"));
        push_traffic(&traffic, &label, "fact-checkers", "delegate");
        // wait briefly for synthesis result
        let mut rx = agent.signal_rx("reasoning.result");
        match time::timeout(Duration::from_secs(5), rx.recv()).await {
            Ok(Some(_)) => push_log(&log, "Planner", format!("{label} received synthesis result")),
            _ => push_log(&log, "Planner", format!("{label} synthesis pending")),
        }
    }
}

async fn run_coalition_fact_checker(agent: Arc<GossipAgent>, log: SharedLog,
                                     traffic: SharedTraffic) {
    let label = agent.node_id().to_string();
    let mut rx = agent.signal_rx("reasoning.task");
    while let Some(_sig) = rx.recv().await {
        push_log(&log, "FactCheck", format!("{label} verifying claim"));
        push_traffic(&traffic, &label, "synthesizers", "verified");
        time::sleep(Duration::from_millis(800)).await;
        let _ = agent.emit("reasoning.verified", SignalScope::System, Bytes::from_static(b"evidence"));
    }
}

async fn run_coalition_synthesizer(agent: Arc<GossipAgent>, log: SharedLog,
                                    traffic: SharedTraffic) {
    let label = agent.node_id().to_string();
    let mut rx = agent.signal_rx("reasoning.verified");
    while let Some(_sig) = rx.recv().await {
        push_log(&log, "Synthesizer", format!("{label} aggregating outputs"));
        push_traffic(&traffic, &label, "planners", "result");
        time::sleep(Duration::from_millis(400)).await;
        let _ = agent.emit("reasoning.result", SignalScope::System, Bytes::from_static(b"summary"));
    }
}

async fn run_voter_behavior(agent: Arc<GossipAgent>, log: SharedLog) {
    let label = agent.node_id().to_string();
    let mut rx = agent.signal_rx("consensus.ballot");
    while let Some(_sig) = rx.recv().await {
        push_log(&log, "Vote", format!("{label} casting ballot"));
        time::sleep(Duration::from_millis(100)).await;
        let _ = agent.emit("consensus.vote", SignalScope::System, Bytes::from_static(b"aye"));
    }
}

async fn run_proposer_behavior(agent: Arc<GossipAgent>, log: SharedLog,
                                traffic: SharedTraffic) {
    let label = agent.node_id().to_string();
    loop {
        time::sleep(Duration::from_secs(4)).await;
        push_log(&log, "Proposer", format!("{label} broadcasting prepare"));
        push_traffic(&traffic, &label, "voters", "propose");
        let _ = agent.emit("consensus.ballot", SignalScope::System, Bytes::from_static(b"prepare"));
        // Collect votes
        let mut votes = 0usize;
        let mut rx = agent.signal_rx("consensus.vote");
        let deadline = time::sleep(Duration::from_secs(2));
        tokio::pin!(deadline);
        loop {
            tokio::select! {
                Some(_) = rx.recv() => {
                    votes += 1;
                    push_log(&log, "Vote", format!("{label} received ballot — total {votes}"));
                }
                _ = &mut deadline => break,
            }
        }
        if votes > 0 {
            push_log(&log, "Commit", format!("{label} quorum {votes} — committed"));
            push_traffic(&traffic, &label, "voters", "commit");
        }
    }
}

async fn run_propagation_behavior(agent: Arc<GossipAgent>, log: SharedLog) {
    let label = agent.node_id().to_string();
    let mut rx = agent.signal_rx("epidemic.wave");
    while let Some(_sig) = rx.recv().await {
        let jitter = (now_ms() % 100) as u64;
        time::sleep(Duration::from_millis(50 + jitter)).await;
        push_log(&log, "Propagate", format!("{label} relaying epidemic wave"));
        let _ = agent.emit("epidemic.wave", SignalScope::System, Bytes::from_static(b"wave"));
    }
}

async fn run_heartbeat_behavior(agent: Arc<GossipAgent>, log: SharedLog,
                                 traffic: SharedTraffic) {
    let label = agent.node_id().to_string();
    loop {
        time::sleep(Duration::from_secs(1)).await;
        let _ = agent.emit("health.heartbeat", SignalScope::System, Bytes::from_static(b"ping"));
        push_log(&log, "Heartbeat", format!("{label} ♥"));
        push_traffic(&traffic, &label, "supervisors", "heartbeat");
    }
}

async fn run_watchdog_behavior(agent: Arc<GossipAgent>, log: SharedLog) {
    let label = agent.node_id().to_string();
    let mut rx = agent.signal_rx("health.heartbeat");
    let mut last_beat = now_ms();
    loop {
        match time::timeout(Duration::from_secs(3), rx.recv()).await {
            Ok(Some(_)) => { last_beat = now_ms(); }
            Ok(None) => break,
            Err(_) => {
                let gap_s = (now_ms() - last_beat) / 1000;
                push_log(&log, "Alert", format!("{label} circuit-breaker: no heartbeat for {gap_s}s"));
                let _ = agent.emit("health.alert", SignalScope::System, Bytes::from_static(b"breach"));
            }
        }
    }
}

async fn run_render_worker(agent: Arc<GossipAgent>, log: SharedLog) {
    let label = agent.node_id().to_string();
    let mut rx = agent.signal_rx("render.job");
    while let Some(_sig) = rx.recv().await {
        let ms = 50 + (now_ms() % 150);
        push_log(&log, "Render", format!("{label} processing job (~{ms}ms)"));
        time::sleep(Duration::from_millis(ms)).await;
        push_log(&log, "Render", format!("{label} job done"));
    }
}

async fn run_render_consumer(agent: Arc<GossipAgent>, log: SharedLog) {
    let label = agent.node_id().to_string();
    loop {
        time::sleep(Duration::from_secs(2)).await;
        if !agent.resolve(&CapFilter::new("render", "job")).is_empty() {
            push_log(&log, "Consumer", format!("{label} dispatching render job"));
            let _ = agent.emit("render.job", SignalScope::System, Bytes::from_static(b"frame"));
        }
    }
}

async fn run_ai_agent_behavior(agent: Arc<GossipAgent>, log: SharedLog) {
    let label = agent.node_id().to_string();
    loop {
        time::sleep(Duration::from_secs(3)).await;
        push_log(&log, "AI", format!("{label} emitting request"));
        let _ = agent.emit("ai.request", SignalScope::System, Bytes::from_static(b"task"));
    }
}

async fn run_llm_planner(
    agent:   Arc<GossipAgent>,
    sm:      Arc<AgentStateMachine>,
    trigger: Arc<AtomicBool>,
    cfg:     Arc<LlmConfig>,
    log:     SharedLog,
    traffic: SharedTraffic,
    call_count: Arc<AtomicU64>,
) {
    loop {
        if trigger.swap(false, Ordering::Relaxed) {
            push_log(&log, "Task", "Manual trigger — starting planning cycle now");
        } else {
            time::sleep(Duration::from_secs(3)).await;
        }

        let label = agent.node_id().to_string();
        push_log(&log, "Task", TASK_TEXT);

        match sm.transition(ExecutionState::Planning).await {
            Ok(())  => push_log(&log, "State", "Planning"),
            Err(e)  => { push_log(&log, "PolicyViolation", e.to_string()); continue; }
        }
        if cfg.mock { time::sleep(Duration::from_millis(900)).await; }

        let tools = discover_tools(&agent);
        if tools.is_empty() {
            push_log(&log, "Warning", "no tools on mesh yet — retrying");
            sm.transition(ExecutionState::Failed { reason: "no tools".into() }).await.ok();
            time::sleep(Duration::from_secs(5)).await;
            continue;
        }
        push_log(&log, "Tools",
            tools.iter().map(|(n, _, _)| n.as_str()).collect::<Vec<_>>().join(", "));

        let mut messages = vec![
            json!({"role":"system","content":"Use available tools to answer the user question step by step."}),
            json!({"role":"user","content": TASK_TEXT}),
        ];

        let mut turn = 0usize;
        'inner: loop {
            if turn >= 8 {
                sm.transition(ExecutionState::Failed { reason: "max turns".into() }).await.ok();
                break 'inner;
            }

            let step = if cfg.mock {
                Ok(mock_plan_step(turn, &tools))
            } else {
                llm_plan_step(&agent, &cfg, &messages, &tools).await
            };

            match step {
                Err(e) => {
                    push_log(&log, "LLM error", &e);
                    sm.transition(ExecutionState::Failed { reason: e }).await.ok();
                    break 'inner;
                }
                Ok(None) => {
                    sm.transition(ExecutionState::Reflecting).await.ok();
                    if cfg.mock { time::sleep(Duration::from_millis(700)).await; }
                    sm.transition(ExecutionState::Done).await.ok();
                    push_log(&log, "State", "Done");
                    if cfg.mock { time::sleep(Duration::from_millis(800)).await; }
                    break 'inner;
                }
                Ok(Some((tool_name, args))) => {
                    match sm.transition(ExecutionState::Invoking { tool: tool_name.clone() }).await {
                        Err(e) => {
                            push_log(&log, "PolicyViolation", e.to_string());
                            sm.transition(ExecutionState::Failed { reason: e.to_string() }).await.ok();
                            break 'inner;
                        }
                        Ok(()) => push_log(&log, "Invoking", format!("{tool_name}({args})")),
                    }
                    if cfg.mock { time::sleep(Duration::from_millis(900)).await; }
                    let result = invoke_tool(&agent, &tool_name, args.clone()).await;
                    push_traffic(&traffic, &label, "compute", "tool-call");
                    sm.transition(ExecutionState::Reflecting).await.ok();
                    if cfg.mock { time::sleep(Duration::from_millis(700)).await; }
                    match result {
                        Err(e) => {
                            push_log(&log, "ToolError", format!("{tool_name}: {e}"));
                            messages.push(json!({"role":"assistant","content":null,"tool_calls":[{"id":"c0","type":"function","function":{"name":&tool_name,"arguments":args.to_string()}}]}));
                            messages.push(json!({"role":"tool","tool_call_id":"c0","content":format!("Error: {e}")}));
                        }
                        Ok(res) => {
                            push_log(&log, "Result", format!("{tool_name} → {}", serde_json::to_string_pretty(&res).unwrap_or_default()));
                            call_count.fetch_add(1, Ordering::Relaxed);
                            messages.push(json!({"role":"assistant","content":null,"tool_calls":[{"id":"c0","type":"function","function":{"name":&tool_name,"arguments":args.to_string()}}]}));
                            messages.push(json!({"role":"tool","tool_call_id":"c0","content":res.to_string()}));
                        }
                    }
                    sm.transition(ExecutionState::Planning).await.ok();
                    if cfg.mock { time::sleep(Duration::from_millis(900)).await; }
                    turn += 1;
                }
            }
        }

        sm.transition(ExecutionState::Idle).await.ok();
        push_log(&log, "State", "Idle — waiting for next task");
        time::sleep(Duration::from_secs(6)).await;
    }
}

// ── Behavior dispatch ─────────────────────────────────────────────────────────

fn spawn_behavior(
    agent:      Arc<GossipAgent>,
    ns:         &str,
    name:       &str,
    log:        SharedLog,
    traffic:    SharedTraffic,
    llm_cfg:    Arc<LlmConfig>,
    sm:         Option<Arc<AgentStateMachine>>,
    trigger:    Arc<AtomicBool>,
    call_count: Arc<AtomicU64>,
) -> Option<AbortHandle> {
    let handle = match (ns, name) {
        ("compute", "cpu") | ("compute", "cpu-heavy") | ("compute", "gpu") => {
            let secs = match name { "cpu-heavy" => 5u64, "gpu" => 2, _ => 3 };
            tokio::spawn(run_worker_behavior(agent, secs, log)).abort_handle()
        }
        ("routing", "emitter") =>
            tokio::spawn(run_emitter_behavior(agent, log)).abort_handle(),
        ("routing", "dispatcher") =>
            tokio::spawn(run_dispatcher_behavior(agent, log)).abort_handle(),
        ("llm", "inference") => {
            let sm = sm?;
            tokio::spawn(run_llm_planner(agent, sm, trigger, llm_cfg, log, traffic, call_count))
                .abort_handle()
        }
        ("reasoning", "planner") =>
            tokio::spawn(run_coalition_planner(agent, log, traffic)).abort_handle(),
        ("reasoning", "fact-checker") =>
            tokio::spawn(run_coalition_fact_checker(agent, log, traffic)).abort_handle(),
        ("reasoning", "synthesizer") =>
            tokio::spawn(run_coalition_synthesizer(agent, log, traffic)).abort_handle(),
        ("consensus", "voter") =>
            tokio::spawn(run_voter_behavior(agent, log)).abort_handle(),
        ("consensus", "proposer") =>
            tokio::spawn(run_proposer_behavior(agent, log, traffic)).abort_handle(),
        ("signal", "propagate") =>
            tokio::spawn(run_propagation_behavior(agent, log)).abort_handle(),
        ("health", "heartbeat") =>
            tokio::spawn(run_heartbeat_behavior(agent, log, traffic)).abort_handle(),
        ("health", "watchdog") =>
            tokio::spawn(run_watchdog_behavior(agent, log)).abort_handle(),
        ("render", "job") =>
            tokio::spawn(run_render_worker(agent, log)).abort_handle(),
        ("render", "consumer") =>
            tokio::spawn(run_render_consumer(agent, log)).abort_handle(),
        ("ai", "agent") =>
            tokio::spawn(run_ai_agent_behavior(agent, log)).abort_handle(),
        _ => return None,
    };
    Some(handle)
}

// ── Provisioning ──────────────────────────────────────────────────────────────

async fn provision_from_manifest(state: Arc<MgmtState>, mut manifest: MeshManifest) {
    // Bump version so the preset is always > whatever was stored previously
    let cur_ver = state.mgmt_agent.get(manifest_keys::VERSION)
        .and_then(|b| String::from_utf8(b.to_vec()).ok())
        .unwrap_or_else(|| "0.0.0".into());
    if !semver_gt(&manifest.mesh.version, &cur_ver) {
        let parse = |s: &str| -> (u64, u64, u64) {
            let mut p = s.trim_start_matches('v').split('.');
            ( p.next().and_then(|x| x.parse().ok()).unwrap_or(0),
              p.next().and_then(|x| x.parse().ok()).unwrap_or(0),
              p.next().and_then(|x| x.parse().ok()).unwrap_or(0) )
        };
        let (mj, mn, pt) = parse(&cur_ver);
        manifest.mesh.version = format!("{mj}.{mn}.{}", pt + 1);
    }

    // 1. Tear down old instance (outside the mutex to allow async shutdown if needed)
    let old = state.instance.lock().unwrap().take();
    drop(old); // MeshInstance::drop aborts tasks, drops handles/agents

    // Small settle to let OS ports release (agents use monotonic ports so not strictly needed)
    time::sleep(Duration::from_millis(50)).await;

    // 2. Create SM for llm-agent-demo
    let mesh_name = manifest.mesh.name.clone();
    let sm: Option<Arc<AgentStateMachine>> = if mesh_name == "llm-agent-demo" {
        let s = state.mgmt_agent.agent_state_machine(AgentPolicy {
            max_turns:   Some(10),
            tool_budget: Some(20),
            ..Default::default()
        });
        *state.sm.lock().unwrap() = Some(Arc::clone(&s));
        Some(s)
    } else {
        *state.sm.lock().unwrap() = None;
        None
    };

    // 3. Provision nodes per group
    let mut nodes = Vec::new();
    for group in &manifest.groups {
        let cap = group.capabilities.first().expect("group must have at least one capability");
        for i in 0..group.min_agents {
            let port  = alloc_port();
            let label = format!("{}-{i}", group.name);
            let agent = make_agent(port, &[MGMT_PORT]);
            agent.start().await.expect("provisioned agent start failed");

            // Register tools for data/compute roles
            let tool_handles = match (cap.ns.as_str(), cap.name.as_str()) {
                ("data", "realtime") => register_realtime_tools(&agent),
                ("compute", "cpu") if mesh_name == "llm-agent-demo" =>
                    register_compute_tools(&agent),
                _ => vec![],
            };

            // Advertise primary capability
            let cap_h = agent.advertise_capability(
                Capability::new(cap.ns.as_str(), cap.name.as_str()),
                Duration::from_secs(30),
            );

            // Decide whether to spawn a behavior loop
            let run_loop = !(
                (cap.ns == "data" && cap.name == "realtime") ||
                (cap.ns == "compute" && cap.name == "cpu" && mesh_name == "llm-agent-demo")
            );

            let behavior = if run_loop {
                spawn_behavior(
                    Arc::clone(&agent),
                    cap.ns.as_str(), cap.name.as_str(),
                    state.log.clone(), state.traffic.clone(),
                    Arc::clone(&state.llm_cfg), sm.clone(),
                    Arc::clone(&state.trigger_requested),
                    Arc::clone(&state.call_count),
                )
            } else {
                None
            };

            push_log(&state.log, "Provision",
                format!("{label} → {}/{}", cap.ns, cap.name));

            nodes.push(MeshNode {
                agent,
                label,
                group:        group.name.clone(),
                ns:           cap.ns.clone(),
                cap_name:     cap.name.clone(),
                behavior,
                cap_handles:  vec![cap_h],
                tool_handles,
                pause_flag:   Arc::new(AtomicBool::new(false)),
            });
        }
    }

    // 4. Push manifest to gossip KV
    if let Ok(toml_str) = manifest.to_toml() {
        let ver = manifest.mesh.version.clone();
        let _ = state.mgmt_agent.set(manifest_keys::CURRENT,
                                      Bytes::from(toml_str.into_bytes()));
        let _ = state.mgmt_agent.set(manifest_keys::VERSION,
                                      Bytes::from(ver.into_bytes()));
    }

    // 5. Update manifest + store instance
    *state.manifest.write().unwrap() = manifest;
    *state.instance.lock().unwrap() = Some(MeshInstance { nodes });
    push_log(&state.log, "Mesh", format!("preset '{mesh_name}' provisioned"));
}

// ── State JSON ────────────────────────────────────────────────────────────────

fn build_state_json(state: &MgmtState) -> String {
    let manifest    = state.manifest.read().unwrap();
    let mesh_status = manifest.check_status(&state.mgmt_agent);
    drop(manifest);

    let inst_guard = state.instance.lock().unwrap();
    let sm_guard   = state.sm.lock().unwrap();

    let nodes: Vec<Value> = inst_guard.as_ref().map(|inst| {
        inst.nodes.iter().map(|node| {
            let alive  = !node.pause_flag.load(Ordering::Relaxed);
            let paused = !alive;
            let state_str: String = if paused {
                "Offline".into()
            } else if node.ns == "llm" && node.cap_name == "inference" {
                sm_guard.as_ref()
                    .map(|sm| {
                        let s = sm.state().to_kv_str();
                        if s == "Idle" { "Ready".into() } else { s }
                    })
                    .unwrap_or_else(|| "Running".into())
            } else if node.ns == "routing" && node.cap_name == "emitter" {
                "Emitting".into()
            } else if node.agent.is_suppressed("compute.invoke") {
                "Suppressed".into()
            } else {
                "Running".into()
            };
            json!({
                "label":    node.label,
                "group":    node.group,
                "ns":       node.ns,
                "cap_name": node.cap_name,
                "alive":    alive,
                "paused":   paused,
                "state":    state_str,
            })
        }).collect()
    }).unwrap_or_default();

    let groups: Vec<Value> = mesh_status.groups.iter().map(|g| json!({
        "name":        g.name,
        "description": g.description,
        "min_agents":  g.min_agents,
        "max_agents":  g.max_agents,
        "actual":      g.actual,
        "satisfied":   g.satisfied,
        "deficit":     g.deficit,
    })).collect();

    let log: Vec<Value> = {
        let l = state.log.lock().unwrap();
        l.iter().rev().take(30)
            .map(|e| json!({"time": fmt_time(e.time_ms), "event": e.event, "detail": e.detail}))
            .collect()
    };

    let traffic: Vec<Value> = {
        let t = state.traffic.lock().unwrap();
        t.iter().rev().take(20)
            .map(|e| json!({"from": e.from, "to": e.to, "ts": e.ts_ms, "kind": e.kind}))
            .collect()
    };

    let manifest = state.manifest.read().unwrap();
    json!({
        "preset":       manifest.mesh.name,
        "version":      manifest.mesh.version,
        "healthy":      mesh_status.is_healthy(),
        "total_calls":  state.call_count.load(Ordering::Relaxed),
        "nodes":        nodes,
        "groups":       groups,
        "log":          log,
        "traffic":      traffic,
    }).to_string()
}

// ── HTTP handlers ─────────────────────────────────────────────────────────────

fn handle_presets_list() -> (u16, &'static str, String) {
    let list: Vec<Value> = PRESETS.iter().map(|p| {
        let ver = MeshManifest::from_toml_bytes(p.toml.as_bytes())
            .map(|m| m.mesh.version).unwrap_or_else(|| "0.1".into());
        json!({ "id": p.id, "name": p.name, "description": p.description, "version": ver })
    }).collect();
    (200, "application/json", json!(list).to_string())
}

async fn handle_preset_apply(state: Arc<MgmtState>, preset_id: &str) -> (u16, &'static str, String) {
    let Some(preset) = PRESETS.iter().find(|p| p.id == preset_id) else {
        return (404, "application/json", json!({"error":"preset not found"}).to_string());
    };
    let Some(manifest) = MeshManifest::from_toml_bytes(preset.toml.as_bytes()) else {
        return (500, "application/json", json!({"error":"preset parse failed"}).to_string());
    };
    let ver = manifest.mesh.version.clone();
    provision_from_manifest(state, manifest).await;
    (200, "application/json", json!({"ok":true,"version":ver}).to_string())
}

async fn handle_manifest_post(state: Arc<MgmtState>, body: &[u8]) -> (u16, &'static str, String) {
    let Some(new_m) = MeshManifest::from_toml_bytes(body) else {
        return (400, "application/json", json!({"error":"invalid TOML"}).to_string());
    };
    let cur_ver = state.mgmt_agent.get(manifest_keys::VERSION)
        .and_then(|b| String::from_utf8(b.to_vec()).ok())
        .unwrap_or_else(|| "0.0.0".into());
    if !semver_gt(&new_m.mesh.version, &cur_ver) {
        return (409, "application/json", json!({
            "error": format!("version {} is not > current {cur_ver}", new_m.mesh.version)
        }).to_string());
    }
    // Archive old
    if let Some(old) = state.mgmt_agent.get(manifest_keys::CURRENT) {
        let _ = state.mgmt_agent.set(manifest_keys::history(&cur_ver), old);
    }
    let new_ver = new_m.mesh.version.clone();
    push_log(&state.log, "Manifest", format!("uploaded v{new_ver} (was v{cur_ver})"));
    provision_from_manifest(state, new_m).await;
    (200, "application/json", json!({"ok":true,"version":new_ver}).to_string())
}

fn handle_node_kill(state: &MgmtState, label: &str) -> (u16, &'static str, String) {
    let mut inst_guard = state.instance.lock().unwrap();
    let Some(inst) = inst_guard.as_mut() else {
        return (404, "application/json", json!({"error":"no active instance"}).to_string());
    };
    let Some(node) = inst.nodes.iter_mut().find(|n| n.label == label) else {
        return (404, "application/json", json!({"error":"node not found"}).to_string());
    };
    node.pause_flag.store(true, Ordering::Relaxed);
    if let Some(h) = node.behavior.take() { h.abort(); }
    node.cap_handles.clear();
    node.tool_handles.clear();
    push_log(&state.log, "NodeKill", format!("{label} capabilities tombstoned"));
    (200, "application/json", json!({"ok":true}).to_string())
}

fn handle_node_start(state: &MgmtState, label: &str) -> (u16, &'static str, String) {
    let mesh_name = state.manifest.read().unwrap().mesh.name.clone();
    let mut inst_guard = state.instance.lock().unwrap();
    let Some(inst) = inst_guard.as_mut() else {
        return (404, "application/json", json!({"error":"no active instance"}).to_string());
    };
    let Some(node) = inst.nodes.iter_mut().find(|n| n.label == label) else {
        return (404, "application/json", json!({"error":"node not found"}).to_string());
    };

    node.pause_flag.store(false, Ordering::Relaxed);

    // Re-register tools
    node.tool_handles = match (node.ns.as_str(), node.cap_name.as_str()) {
        ("data", "realtime") => register_realtime_tools(&node.agent),
        ("compute", "cpu") if mesh_name == "llm-agent-demo" =>
            register_compute_tools(&node.agent),
        _ => vec![],
    };

    // Re-advertise capability
    node.cap_handles = vec![node.agent.advertise_capability(
        Capability::new(node.ns.as_str(), node.cap_name.as_str()),
        Duration::from_secs(30),
    )];

    // Re-spawn behavior loop if applicable
    let run_loop = !(
        (node.ns == "data" && node.cap_name == "realtime") ||
        (node.ns == "compute" && node.cap_name == "cpu" && mesh_name == "llm-agent-demo")
    );
    if run_loop {
        let sm = state.sm.lock().unwrap().clone();
        let ns       = node.ns.clone();
        let cap_name = node.cap_name.clone();
        node.behavior = spawn_behavior(
            Arc::clone(&node.agent),
            &ns, &cap_name,
            state.log.clone(), state.traffic.clone(),
            Arc::clone(&state.llm_cfg), sm,
            Arc::clone(&state.trigger_requested),
            Arc::clone(&state.call_count),
        );
    }

    push_log(&state.log, "NodeStart", format!("{label} re-advertised {}/{}", node.ns, node.cap_name));
    (200, "application/json", json!({"ok":true}).to_string())
}

async fn handle_system_stop(state: &MgmtState) -> (u16, &'static str, String) {
    let _ = state.mgmt_agent.set(manifest_keys::CONTROL_SYSTEM, Bytes::from_static(b"stopped"));
    let inst_guard = state.instance.lock().unwrap();
    if let Some(inst) = inst_guard.as_ref() {
        for node in &inst.nodes {
            node.pause_flag.store(true, Ordering::Relaxed);
        }
    }
    push_log(&state.log, "Control", "system stopped");
    (200, "application/json", json!({"ok":true}).to_string())
}

async fn handle_system_start(state: &MgmtState) -> (u16, &'static str, String) {
    let _ = state.mgmt_agent.set(manifest_keys::CONTROL_SYSTEM, Bytes::from_static(b"running"));
    let inst_guard = state.instance.lock().unwrap();
    if let Some(inst) = inst_guard.as_ref() {
        for node in &inst.nodes {
            node.pause_flag.store(false, Ordering::Relaxed);
        }
    }
    push_log(&state.log, "Control", "system started");
    (200, "application/json", json!({"ok":true}).to_string())
}

async fn handle_group_control(state: &MgmtState, group: &str, stop: bool) -> (u16, &'static str, String) {
    let inst_guard = state.instance.lock().unwrap();
    let Some(inst) = inst_guard.as_ref() else {
        return (404, "application/json", json!({"error":"no active instance"}).to_string());
    };
    let matching: Vec<&MeshNode> = inst.nodes.iter().filter(|n| n.group == group).collect();
    if matching.is_empty() {
        return (404, "application/json", json!({"error":"group not found"}).to_string());
    }
    for node in &matching { node.pause_flag.store(stop, Ordering::Relaxed); }
    push_log(&state.log, "Control",
        format!("group '{group}' {}", if stop { "stopped" } else { "started" }));
    (200, "application/json", json!({"ok":true,"group":group}).to_string())
}

fn handle_demo_trigger(state: &MgmtState) -> (u16, &'static str, String) {
    state.trigger_requested.store(true, Ordering::Relaxed);
    push_log(&state.log, "Demo", "Task triggered");
    (200, "application/json", json!({"ok":true}).to_string())
}

// ── HTTP server ───────────────────────────────────────────────────────────────

async fn handle_http(mut stream: tokio::net::TcpStream, state: Arc<MgmtState>) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut buf = [0u8; 16384];
    let Ok(n) = stream.read(&mut buf).await else { return };
    let req = std::str::from_utf8(&buf[..n]).unwrap_or("");

    let mut lines = req.lines();
    let req_line  = lines.next().unwrap_or("");
    let mut parts = req_line.split_whitespace();
    let method    = parts.next().unwrap_or("GET");
    let path      = parts.next().unwrap_or("/");

    let body_start = req.find("\r\n\r\n").map(|i| i + 4)
                        .or_else(|| req.find("\n\n").map(|i| i + 2))
                        .unwrap_or(n);
    let body = &buf[body_start..n];
    let is_post = method == "POST";

    let (status, ct, body_str) = match (method, path) {
        (_, "/state") =>
            (200, "application/json", build_state_json(&state)),
        ("GET", "/presets") =>
            handle_presets_list(),
        _ if is_post && path.starts_with("/presets/") && path.ends_with("/apply") => {
            let id = path.trim_start_matches("/presets/").trim_end_matches("/apply");
            handle_preset_apply(Arc::clone(&state), id).await
        }
        ("GET", "/manifest") => {
            let m = state.manifest.read().unwrap();
            let g: Vec<Value> = m.groups.iter().map(|g| json!({
                "name":        g.name,
                "description": g.description,
                "min_agents":  g.min_agents,
                "max_agents":  g.max_agents,
            })).collect();
            let body = json!({"name":m.mesh.name,"version":m.mesh.version,"groups":g}).to_string();
            drop(m);
            (200, "application/json", body)
        }
        ("POST", "/manifest") =>
            handle_manifest_post(Arc::clone(&state), body).await,
        ("POST", "/system/stop") =>
            handle_system_stop(&state).await,
        ("POST", "/system/start") =>
            handle_system_start(&state).await,
        ("POST", "/demo/trigger") =>
            handle_demo_trigger(&state),
        _ if is_post && path.starts_with("/nodes/") && path.ends_with("/kill") => {
            let label = path.trim_start_matches("/nodes/").trim_end_matches("/kill");
            handle_node_kill(&state, label)
        }
        _ if is_post && path.starts_with("/nodes/") && path.ends_with("/start") => {
            let label = path.trim_start_matches("/nodes/").trim_end_matches("/start");
            handle_node_start(&state, label)
        }
        _ if is_post && path.starts_with("/system/groups/")
             && (path.ends_with("/stop") || path.ends_with("/start")) => {
            let is_stop = path.ends_with("/stop");
            let group = path.trim_start_matches("/system/groups/")
                .trim_end_matches("/stop").trim_end_matches("/start");
            handle_group_control(&state, group, is_stop).await
        }
        _ =>
            (200, "text/html; charset=utf-8",
             include_str!("../docs/mesh_demo.html").to_string()),
    };

    let response = format!(
        "HTTP/1.1 {status}\r\nContent-Type: {ct}\r\nContent-Length: {}\r\n\
         Access-Control-Allow-Origin: *\r\nConnection: close\r\n\r\n{body_str}",
        body_str.len()
    );
    let _ = stream.write_all(response.as_bytes()).await;
}

// ── Main ──────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _ = tracing_subscriber::fmt().with_env_filter("warn").try_init();

    let llm_cfg = LlmConfig::from_env();
    println!("LLM mode  : {}", if llm_cfg.mock { "MOCK" } else { "Ollama" });
    println!("Endpoint  : {}", llm_cfg.base_url);

    // Management agent — persistent, never killed
    let mgmt_agent = make_agent(MGMT_PORT, &[]);
    mgmt_agent.start().await?;
    println!("Mgmt agent: 127.0.0.1:{MGMT_PORT}");

    let state = Arc::new(MgmtState {
        mgmt_agent:        Arc::clone(&mgmt_agent),
        manifest:          RwLock::new(MeshManifest::default()),
        instance:          Mutex::new(None),
        log:               Arc::new(Mutex::new(Vec::new())),
        traffic:           Arc::new(Mutex::new(Vec::new())),
        llm_cfg:           Arc::new(llm_cfg),
        sm:                Mutex::new(None),
        trigger_requested: Arc::new(AtomicBool::new(false)),
        call_count:        Arc::new(AtomicU64::new(0)),
    });

    // Bootstrap with the default LLM-agent preset
    let default_preset = PRESETS.iter().find(|p| p.id == "llm-agent").unwrap();
    let default_manifest = MeshManifest::from_toml_bytes(default_preset.toml.as_bytes())
        .expect("default preset parseable");
    println!("Provisioning default preset: {}", default_manifest.mesh.name);

    provision_from_manifest(Arc::clone(&state), default_manifest).await;

    println!("\nWaiting {SETTLE_MS}ms for mesh to settle…");
    time::sleep(Duration::from_millis(SETTLE_MS)).await;

    {
        let manifest = state.manifest.read().unwrap();
        let status   = manifest.check_status(&state.mgmt_agent);
        println!("Mesh: {} (deficit: {})", manifest.mesh.name, status.total_deficit());
        for g in &status.groups {
            println!("  {:22} {}/{} {}", g.name, g.actual, g.min_agents,
                     if g.satisfied { "✓" } else { "✗" });
        }
        println!();
    }

    // HTTP server
    let listener = tokio::net::TcpListener::bind(format!("127.0.0.1:{HTTP_PORT}")).await?;
    println!("Serving   : http://127.0.0.1:{HTTP_PORT}");
    println!("Ready.\n");

    loop {
        let (stream, _) = listener.accept().await?;
        let state2 = Arc::clone(&state);
        tokio::spawn(handle_http(stream, state2));
    }
}
