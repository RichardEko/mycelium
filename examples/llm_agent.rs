//! LLM Agent — Config-driven capabilities · Probe/Health · Dynamic Provisioning
//!
//! Three nodes share a gossip mesh. Each node loads its capability declarations
//! from a TOML file (`examples/node_n*.toml`). The probe loop advertises
//! capabilities to the mesh when their services respond, and tombstones them
//! when they go offline.
//!
//! n-2's `llm/inference` capability is probe-gated on a live Ollama instance
//! (or bypassed in mock mode). All capability advertisement is config-driven —
//! no capability is hardcoded in the application.
//!
//! # Topology (all localhost)
//! ```text
//!   n-0 · 56000  fixed tools: weather, ping       caps: from node_n0.toml
//!                health loop with simulated failure at T+35 s
//!
//!   n-1 · 56001  fixed tools: search, calculate   caps: from node_n1.toml
//!                dynamic tool: vector-search       data/installable→loading→ready
//!                handles "cap.provision" RPC
//!
//!   n-2 · 56002  LLM agent node                   caps: from node_n2.toml
//!                llm/inference probed via Ollama /api/tags
//!                planning loop resolves endpoint from cap KV (not from config)
//!                accepts "cap.provision" for model pulls
//!                on startup: requests vector-search from n-1
//! ```
//!
//! # Config files
//! | File                    | Node | Contents                               |
//! |-------------------------|------|----------------------------------------|
//! | `examples/node_n0.toml` | n-0  | `data/realtime` (always-alive)         |
//! | `examples/node_n1.toml` | n-1  | `compute/cpu`, `data/installable`      |
//! | `examples/node_n2.toml` | n-2  | `llm/inference` (probed), installables |
//!
//! # Env vars
//! | Var               | Default                      | Purpose                   |
//! |-------------------|------------------------------|---------------------------|
//! | `OPENAI_BASE_URL` | `http://localhost:11434/v1`  | Ollama endpoint           |
//! | `OPENAI_API_KEY`  | `ollama`                     | API key                   |
//! | `OPENAI_MODEL`    | `llama3.2`                   | Model name                |
//! | `MOCK_LLM`        | *(unset)*                    | `1` = skip Ollama probe   |
//!
//! # Run
//! ```sh
//! MOCK_LLM=1 cargo run --example llm_agent   # no Ollama needed
//! cargo run --example llm_agent               # real Ollama
//! ```
//! Then open **http://127.0.0.1:8100**

use bytes::Bytes;
use mycelium::{
    AgentPolicy, AgentStateMachine, Capability, CapabilityEvent, CapabilityHandle,
    CapConstraint, CapFilter, CapValue, ExecutionState, GossipAgent, GossipConfig,
    McpToolHandle, MeshManifest, NodeCapabilityConfig, NodeId, ProbeEvent, ProbeState,
    TomlCapValue, manifest_keys, semver_gt, run_capability_probes, signal_kind,
};
use serde_json::{json, Value};
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex, RwLock,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::time;

// ── Constants ─────────────────────────────────────────────────────────────────

const PORT_N0:         u16 = 56000;
const PORT_N1:         u16 = 56001;
const PORT_N2:         u16 = 56002;
const HTTP_PORT_N0:    u16 = 8100;
const HTTP_PORT_N1:    u16 = 8101;
const HTTP_PORT_N2:    u16 = 8102;
const SETTLE_MS:       u64 = 2_000;
const HEALTH_SECS:     u64 = 10;
const FAILURE_ONSET_S: u64 = 35;
const FAILURE_HOLD_S:  u64 = 15;
const TASK_TEXT: &str = "What is the weather in London? Then search for 'mycelium networking' and calculate 42 * 7.";

// ── Helpers ───────────────────────────────────────────────────────────────────

fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64
}

fn fmt_time(ms: u64) -> String {
    let s = ms / 1000;
    let m = (ms % 1_000) as u32;
    format!("{:02}:{:02}:{:02}.{:03}", s / 3600 % 24, s / 60 % 60, s % 60, m)
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

/// Build an `on_event` closure for [`run_capability_probes`] that forwards
/// probe state changes into the shared log.
fn probe_logger(log: SharedLog, node_label: &'static str) -> impl Fn(ProbeEvent) + Send + 'static {
    move |e: ProbeEvent| {
        let state = match e.state { ProbeState::Up => "up", ProbeState::Down => "down" };
        push_log(&log, "Probe", format!("[{node_label}] {}/{} → {state}", e.ns, e.name));
    }
}

// ── Tool handlers ─────────────────────────────────────────────────────────────

async fn handle_weather(args: Value) -> Value {
    let city = args["city"].as_str().unwrap_or("Unknown");
    let temp = 15 + (city.len() as i32 % 20);
    json!({ "city": city, "temp_c": temp,
            "condition": if temp > 20 { "sunny" } else if temp > 10 { "cloudy" } else { "rainy" } })
}

async fn handle_ping(args: Value) -> Value {
    json!({ "host": args["host"].as_str().unwrap_or("localhost"),
            "latency_ms": 12, "status": "reachable" })
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

async fn handle_vector_search(args: Value) -> Value {
    let query = args["query"].as_str().unwrap_or("");
    json!({
        "query": query,
        "results": [
            { "score": 0.97, "text": format!("Mycelium architecture: {query}") },
            { "score": 0.91, "text": format!("Semantic match for: {query}") },
        ]
    })
}

// ── Tool registration helper ──────────────────────────────────────────────────

struct ToolDef {
    name:        &'static str,
    description: &'static str,
    params:      Value,
}

fn register_tool(
    agent:   &Arc<GossipAgent>,
    def:     &ToolDef,
    handler: impl Fn(Value) -> std::pin::Pin<Box<dyn std::future::Future<Output = Value> + Send + 'static>>
             + Send + Sync + 'static,
) -> McpToolHandle {
    let schema = json!({ "description": def.description, "inputSchema": def.params });
    agent.register_mcp_tool(def.name, schema, move |args: Value| {
        let fut = handler(args);
        Box::pin(async move { Ok::<Value, String>(fut.await) })
    })
}

// ── Fixed-tool node — probe + health loop ─────────────────────────────────────
//
// Capabilities are advertised separately via run_capability_probes (config-driven).
// This task only manages tool registration/deregistration based on the fail_flag.

fn probe_tool(_name: &str, fail_flag: &Arc<AtomicBool>) -> bool {
    !fail_flag.load(Ordering::Relaxed)
}

struct FixedToolNode {
    agent:     Arc<GossipAgent>,
    tools:     Vec<ToolDef>,
    fail_flag: Arc<AtomicBool>,
    log:       SharedLog,
    handlers:  Vec<fn(Value) -> std::pin::Pin<Box<dyn std::future::Future<Output = Value> + Send + 'static>>>,
}

struct ToolHandle { _tool: McpToolHandle }

async fn run_fixed_tool_node(node: Arc<FixedToolNode>) {
    let mut handles: Vec<Option<ToolHandle>> = node.tools.iter().map(|_| None).collect();

    for (i, def) in node.tools.iter().enumerate() {
        if probe_tool(def.name, &node.fail_flag) {
            handles[i] = Some(ToolHandle { _tool: register_tool(&node.agent, def, node.handlers[i]) });
            push_log(&node.log, "Probe", format!("{} → registered", def.name));
        } else {
            push_log(&node.log, "Probe", format!("{} → failed — will retry", def.name));
        }
    }

    loop {
        time::sleep(Duration::from_secs(HEALTH_SECS)).await;
        for (i, def) in node.tools.iter().enumerate() {
            match (handles[i].is_some(), probe_tool(def.name, &node.fail_flag)) {
                (true, false) => {
                    handles[i] = None;
                    push_log(&node.log, "Health", format!("{} → offline — tombstoned", def.name));
                }
                (false, true) => {
                    handles[i] = Some(ToolHandle { _tool: register_tool(&node.agent, def, node.handlers[i]) });
                    push_log(&node.log, "Health", format!("{} → back online — re-registered", def.name));
                }
                _ => {}
            }
        }
    }
}

// ── n-1 vector-search provision handler ──────────────────────────────────────

async fn run_vector_search_provision_handler(agent: Arc<GossipAgent>, log: SharedLog) {
    let mut rx = agent.signal_rx("cap.provision");
    let mut _tool_h:  Option<McpToolHandle>    = None;
    let mut _ready_h: Option<CapabilityHandle> = None;

    while let Some(sig) = rx.recv().await {
        if sig.payload.len() < 8 { continue; }
        let Ok(body) = serde_json::from_slice::<Value>(&sig.payload[8..]) else { continue };
        if body["ns"].as_str() != Some("data") || body["name"].as_str() != Some("vector-search") {
            continue;
        }
        agent.rpc_respond(&sig, Bytes::from_static(b"accepted"));
        push_log(&log, "Provision", "vector-search accepted");

        // loading tier — 2 s re-assertion so progress updates stay fresh
        let mut loading_h = agent.advertise_capability(
            Capability::new("data", "loading")
                .with("name",     CapValue::Text("vector-search".into()))
                .with("progress", CapValue::Integer(0)),
            Duration::from_secs(2),
        );
        for pct in [20i64, 40, 60, 80, 100] {
            time::sleep(Duration::from_secs(3)).await;
            loading_h = agent.advertise_capability(
                Capability::new("data", "loading")
                    .with("name",     CapValue::Text("vector-search".into()))
                    .with("progress", CapValue::Integer(pct)),
                Duration::from_secs(2),
            );
            push_log(&log, "Provision", format!("vector-search install {pct}%"));
        }
        drop(loading_h);

        let tool = register_tool(
            &agent,
            &ToolDef {
                name:        "vector-search",
                description: "Semantic search over a local vector store",
                params: json!({"type":"object","properties":{"query":{"type":"string"}},"required":["query"]}),
            },
            |args| Box::pin(handle_vector_search(args)),
        );
        let ready = agent.advertise_capability(
            Capability::new("data", "ready")
                .with("name", CapValue::Text("vector-search".into())),
            Duration::from_secs(120),
        );
        push_log(&log, "Provision", "vector-search ready on mesh");
        _tool_h  = Some(tool);
        _ready_h = Some(ready);
    }
}

// ── n-2 LLM provision handler (model pulls) ───────────────────────────────────

async fn run_llm_provision_handler(agent: Arc<GossipAgent>, base_url: String, log: SharedLog) {
    let mut rx = agent.signal_rx("cap.provision");
    let mut _extra_inference: Vec<CapabilityHandle> = Vec::new();

    while let Some(sig) = rx.recv().await {
        if sig.payload.len() < 8 { continue; }
        let Ok(body) = serde_json::from_slice::<Value>(&sig.payload[8..]) else { continue };
        if body["ns"].as_str() != Some("llm") { continue; }
        let model = body["model"].as_str().unwrap_or("").to_string();
        if model.is_empty() { continue; }

        agent.rpc_respond(&sig, Bytes::from_static(b"pulling"));
        push_log(&log, "LLM Provision", format!("pulling model={model}"));

        let mut loading_h = agent.advertise_capability(
            Capability::new("llm", "loading")
                .with("model",    CapValue::Text(model.clone().into()))
                .with("progress", CapValue::Integer(0)),
            Duration::from_secs(2),
        );
        for pct in [20i64, 40, 60, 80, 100] {
            time::sleep(Duration::from_secs(4)).await;
            loading_h = agent.advertise_capability(
                Capability::new("llm", "loading")
                    .with("model",    CapValue::Text(model.clone().into()))
                    .with("progress", CapValue::Integer(pct)),
                Duration::from_secs(2),
            );
            push_log(&log, "LLM Provision", format!("{model} pull {pct}%"));
        }
        drop(loading_h);

        let inf_h = agent.advertise_capability(
            Capability::new("llm", "inference")
                .with("model",    CapValue::Text(model.clone().into()))
                .with("context",  CapValue::Integer(8192))
                .with("backend",  CapValue::Text("ollama".into()))
                .with("endpoint", CapValue::Text(base_url.clone().into())),
            Duration::from_secs(30),
        );
        push_log(&log, "LLM Provision", format!("model {model} now hot on mesh"));
        _extra_inference.push(inf_h);
    }
}

// ── n-2 vector-search consumer ────────────────────────────────────────────────

async fn request_vector_search(agent_n2: &Arc<GossipAgent>, n1_id: NodeId, log: &SharedLog) {
    // Subscribe before sending to avoid race
    let mut ready_rx = agent_n2.watch_capabilities(
        CapFilter::new("data", "ready")
            .with("name", CapConstraint::Eq(CapValue::Text("vector-search".into()))),
    );

    let req = json!({"ns": "data", "name": "vector-search"}).to_string();
    match agent_n2.rpc_call(n1_id, "cap.provision", Bytes::from(req), Duration::from_secs(10)).await {
        Ok(_)  => push_log(log, "Provision", "vector-search requested from n-1"),
        Err(e) => {
            push_log(log, "Provision", format!("vector-search request failed: {e}"));
            return;
        }
    }

    match time::timeout(Duration::from_secs(120), async {
        loop {
            match ready_rx.recv().await {
                Some(CapabilityEvent::Added { .. }) => break,
                Some(_) => continue,
                None    => break,
            }
        }
    }).await {
        Ok(_)  => push_log(log, "Provision", "vector-search is now available on mesh"),
        Err(_) => push_log(log, "Provision", "vector-search did not become ready in 120 s"),
    }
}

// ── Tool discovery and invocation ─────────────────────────────────────────────

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
    let node_id: NodeId = parts[2].parse().map_err(|e: mycelium::GossipError| e.to_string())?;
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

// ── LLM planning ──────────────────────────────────────────────────────────────

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

fn mock_plan_step(turn: usize, tools: &[(String, NodeId, Value)]) -> Option<(String, Value)> {
    let names: Vec<&str> = tools.iter().map(|(n, _, _)| n.as_str()).collect();
    match turn {
        0 => names.iter().find(|&&n| n == "weather")
                .map(|&n| (n.to_string(), json!({"city":"London"}))),
        1 => names.iter().find(|&&n| n == "vector-search")
                .map(|&n| (n.to_string(), json!({"query":"mycelium networking"}))),
        2 => names.iter().find(|&&n| n == "calculate")
                .map(|&n| (n.to_string(), json!({"expression":"42 * 7"}))),
        _ => None,
    }
}

/// Real LLM step. Resolves the inference endpoint from mesh cap KV so the
/// planning loop is not coupled to `LlmConfig::base_url`.
async fn llm_plan_step(
    agent:    &GossipAgent,
    cfg:      &LlmConfig,
    messages: &[Value],
    tools:    &[(String, NodeId, Value)],
) -> Result<Option<(String, Value)>, String> {
    let providers = agent.resolve(
        &CapFilter::new("llm", "inference")
            .with("model", CapConstraint::Eq(CapValue::Text(cfg.model.clone().into()))),
    );
    let endpoint = providers.first()
        .and_then(|(_, cap)| cap.attributes.get("endpoint" as &str))
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

// ── Preset manifests ──────────────────────────────────────────────────────────

struct Preset {
    id:          &'static str,
    name:        &'static str,
    description: &'static str,
    toml:        &'static str,
}

const PRESETS: &[Preset] = &[
    // ── Current-generation examples ───────────────────────────────────────────
    Preset {
        id:          "llm-agent",
        name:        "LLM Agent Demo",
        description: "Three nodes: real-time data, compute tools, LLM inference",
        toml:        include_str!("presets/llm_agent.toml"),
    },
    Preset {
        id:          "mcp-mesh",
        name:        "MCP Tool Mesh",
        description: "Tool providers, data sources, and LLM reasoning nodes",
        toml:        include_str!("presets/mcp_mesh.toml"),
    },
    Preset {
        id:          "compute-cluster",
        name:        "Compute Cluster",
        description: "Parallel compute workers with real-time data feed",
        toml:        include_str!("presets/compute_cluster.toml"),
    },
    Preset {
        id:          "minimal",
        name:        "Minimal Mesh",
        description: "Single data node — ideal for development and testing",
        toml:        include_str!("presets/minimal.toml"),
    },
    // ── Topology presets (from archived standalone demos) ─────────────────────
    Preset {
        id:          "epidemic-ring",
        name:        "Epidemic Ring",
        description: "16-node ring split into alpha/beta partitions for signal propagation demos",
        toml:        include_str!("presets/epidemic_ring.toml"),
    },
    Preset {
        id:          "consensus-cluster",
        name:        "Consensus Cluster",
        description: "7 voters + rotating proposers — epidemic two-phase ballot matrix",
        toml:        include_str!("presets/consensus_cluster.toml"),
    },
    Preset {
        id:          "dispatch-pool",
        name:        "Dispatch Pool",
        description: "Fast and slow worker tiers with adaptive load-balancing dispatchers",
        toml:        include_str!("presets/dispatch_pool.toml"),
    },
    Preset {
        id:          "emergent-pool",
        name:        "Emergent GPU Pool",
        description: "20 GPU workers self-assemble via cap-groups; render jobs route via signal_wired_via",
        toml:        include_str!("presets/emergent_pool.toml"),
    },
    Preset {
        id:          "capability-market",
        name:        "Capability Market",
        description: "12 nodes across 4 capability kinds — demand pressure and dynamic advertisement",
        toml:        include_str!("presets/capability_market.toml"),
    },
    Preset {
        id:          "locality-mesh",
        name:        "Locality Mesh",
        description: "East/west render providers — resolve_with_locality routes to nearest first",
        toml:        include_str!("presets/locality_mesh.toml"),
    },
    Preset {
        id:          "watchdog-cluster",
        name:        "Watchdog Cluster",
        description: "6 heartbeat services monitored by a quorum_persistent circuit-breaker supervisor",
        toml:        include_str!("presets/watchdog_cluster.toml"),
    },
];

// ── Emergent manager election ─────────────────────────────────────────────────
//
// Every node permanently advertises cap/{self}/system/manager with its HTTP
// port. The active manager is the node with the lexicographically smallest
// node-id among all live candidates — a deterministic function every node
// computes independently from shared gossip state (no consensus needed).
// When the current minimum's TTL expires (node died), all survivors converge
// to the new minimum within one gossip round.

fn compute_manager_port(candidates: &[(NodeId, Capability)]) -> Option<u16> {
    candidates.iter()
        .min_by_key(|(nid, _)| nid.to_string())
        .and_then(|(_, cap)| {
            cap.attributes.get("http_port")
                .and_then(|v| if let CapValue::Integer(p) = v { Some(*p as u16) } else { None })
        })
}

fn port_to_node_label(port: u16) -> Option<&'static str> {
    match port {
        HTTP_PORT_N0 => Some("n-0"),
        HTTP_PORT_N1 => Some("n-1"),
        HTTP_PORT_N2 => Some("n-2"),
        _ => None,
    }
}

/// Advertise every capability declared in `manifest` on the three nodes.
/// Groups are assigned round-robin: group[i % 3] → agents[i % 3].
/// Returns the resulting handles; caller stores them in `AppState::manifest_caps`
/// and drops the previous Vec to tombstone the old advertisements.
fn advertise_manifest_caps(
    manifest: &MeshManifest,
    agents:   [&Arc<GossipAgent>; 3],
) -> Vec<CapabilityHandle> {
    manifest.groups.iter().enumerate().flat_map(|(i, group)| {
        let agent = agents[i % 3];
        group.capabilities.iter().map(move |cap| {
            agent.advertise_capability(
                Capability::new(cap.ns.as_str(), cap.name.as_str()),
                Duration::from_secs(30),
            )
        }).collect::<Vec<_>>()
    }).collect()
}

// ── Agent planning loop ───────────────────────────────────────────────────────

struct AppState {
    agent_n0:    Arc<GossipAgent>,
    agent_n1:    Arc<GossipAgent>,
    agent_n2:    Arc<GossipAgent>,
    sm:          Arc<AgentStateMachine>,
    log:         SharedLog,
    call_count:  Arc<AtomicU64>,
    last_tool:   Arc<Mutex<String>>,
    last_result: Arc<Mutex<Value>>,
    manifest:    Arc<RwLock<MeshManifest>>,
    pause_n0:    Arc<AtomicBool>,
    pause_n1:    Arc<AtomicBool>,
    pause_n2:    Arc<AtomicBool>,
    /// group name → which node's pause flag to flip
    group_node:   Arc<HashMap<String, &'static str>>,
    /// current elected manager's HTTP port (None while electing)
    manager_port: Arc<Mutex<Option<u16>>>,
    /// Dynamic capability handles derived from the live manifest.
    /// Replaced atomically whenever the manifest changes so the mesh always
    /// reflects the current topology intent.
    manifest_caps: Arc<Mutex<Vec<CapabilityHandle>>>,
}

async fn run_agent_loop(app: Arc<AppState>, cfg: LlmConfig) {
    let sm    = Arc::clone(&app.sm);
    let agent = Arc::clone(&app.agent_n2);

    loop {
        // Respect system/group pause from manifest control
        if app.pause_n2.load(Ordering::Relaxed) {
            time::sleep(Duration::from_secs(2)).await;
            continue;
        }

        time::sleep(Duration::from_secs(3)).await;
        push_log(&app.log, "Task", TASK_TEXT);

        match sm.transition(ExecutionState::Planning).await {
            Ok(())  => push_log(&app.log, "State", "Idle → Planning"),
            Err(e)  => { push_log(&app.log, "PolicyViolation", e.to_string()); continue; }
        }

        let tools = discover_tools(&agent);
        if tools.is_empty() {
            push_log(&app.log, "Warning", "no tools on mesh yet — retrying");
            sm.transition(ExecutionState::Failed { reason: "no tools".into() }).await.ok();
            time::sleep(Duration::from_secs(5)).await;
            continue;
        }
        push_log(&app.log, "Tools", tools.iter().map(|(n,_,_)| n.as_str()).collect::<Vec<_>>().join(", "));

        let mut messages = vec![
            json!({"role":"system","content":"Use available tools to answer the user's question step by step. Stop once you have all information."}),
            json!({"role":"user","content": TASK_TEXT}),
        ];
        let mut turn = 0usize;

        loop {
            if turn >= 8 {
                sm.transition(ExecutionState::Failed { reason: "max turns".into() }).await.ok();
                push_log(&app.log, "State", "Failed: max turns");
                break;
            }

            let step = if cfg.mock {
                Ok(mock_plan_step(turn, &tools))
            } else {
                llm_plan_step(&agent, &cfg, &messages, &tools).await
            };

            match step {
                Err(e) => {
                    push_log(&app.log, "LLM error", &e);
                    sm.transition(ExecutionState::Failed { reason: e }).await.ok();
                    break;
                }
                Ok(None) => {
                    sm.transition(ExecutionState::Reflecting).await.ok();
                    sm.transition(ExecutionState::Done).await.ok();
                    push_log(&app.log, "State", "Done");
                    break;
                }
                Ok(Some((tool_name, args))) => {
                    match sm.transition(ExecutionState::Invoking { tool: tool_name.clone() }).await {
                        Err(e) => {
                            push_log(&app.log, "PolicyViolation", e.to_string());
                            sm.transition(ExecutionState::Failed { reason: e.to_string() }).await.ok();
                            break;
                        }
                        Ok(()) => push_log(&app.log, "Invoking", format!("{tool_name}({args})")),
                    }
                    let result = invoke_tool(&agent, &tool_name, args.clone()).await;
                    sm.transition(ExecutionState::Reflecting).await.ok();
                    match result {
                        Err(e) => {
                            push_log(&app.log, "ToolError", format!("{tool_name}: {e}"));
                            messages.push(json!({"role":"assistant","content":null,"tool_calls":[{"id":"c0","type":"function","function":{"name":tool_name,"arguments":args.to_string()}}]}));
                            messages.push(json!({"role":"tool","tool_call_id":"c0","content":format!("Error: {e}")}));
                        }
                        Ok(res) => {
                            push_log(&app.log, "Result", format!("{tool_name} → {}", serde_json::to_string_pretty(&res).unwrap_or_default()));
                            *app.last_tool.lock().unwrap()   = tool_name.clone();
                            *app.last_result.lock().unwrap() = res.clone();
                            app.call_count.fetch_add(1, Ordering::Relaxed);
                            messages.push(json!({"role":"assistant","content":null,"tool_calls":[{"id":"c0","type":"function","function":{"name":tool_name,"arguments":args.to_string()}}]}));
                            messages.push(json!({"role":"tool","tool_call_id":"c0","content":res.to_string()}));
                        }
                    }
                    sm.transition(ExecutionState::Planning).await.ok();
                    turn += 1;
                }
            }
        }
        sm.transition(ExecutionState::Idle).await.ok();
        push_log(&app.log, "State", "Idle — waiting for next task");
        time::sleep(Duration::from_secs(8)).await;
    }
}

// ── Dynamic /state JSON from live mesh KV ─────────────────────────────────────

fn capvalue_to_json(v: &CapValue) -> Value {
    match v {
        CapValue::Text(s)    => Value::String(s.as_ref().to_string()),
        CapValue::Integer(n) => json!(n),
        CapValue::Float(f)   => json!(f),
        CapValue::Bool(b)    => json!(b),
        CapValue::Version(v) => Value::String(format!("{}.{}.{}", v[0], v[1], v[2])),
    }
}

fn caps_for_node(reporter: &GossipAgent, target: &NodeId) -> Vec<Value> {
    let prefix = format!("cap/{target}/");
    reporter.scan_prefix(&prefix).into_iter().filter_map(|(_, v)| {
        let cap = Capability::decode(&v)?;
        Some(json!({
            "ns":   cap.namespace,
            "name": cap.name,
            "attrs": cap.attributes.iter()
                .map(|(k, v)| (k.as_ref().to_string(), capvalue_to_json(v)))
                .collect::<serde_json::Map<_, _>>(),
        }))
    }).collect()
}

fn tools_for_node(reporter: &GossipAgent, target: &NodeId) -> Vec<String> {
    let target_str = target.to_string();
    reporter.scan_prefix("tools/").into_iter().filter_map(|(k, _)| {
        let parts: Vec<&str> = k.splitn(3, '/').collect();
        if parts.len() == 3 && parts[2] == target_str { Some(parts[1].to_string()) } else { None }
    }).collect()
}

fn build_state_json(app: &AppState) -> String {
    let reporter = &app.agent_n2;
    let nodes: Vec<Value> = [
        (&app.agent_n0, "n-0"),
        (&app.agent_n1, "n-1"),
        (&app.agent_n2, "n-2"),
    ].iter().map(|(agent, label)| {
        let nid   = agent.node_id();
        let alive = agent.peers().len() > 0 || *label == "n-2";
        let paused = match *label {
            "n-0" => app.pause_n0.load(Ordering::Relaxed),
            "n-1" => app.pause_n1.load(Ordering::Relaxed),
            _     => app.pause_n2.load(Ordering::Relaxed),
        };
        let state: String = if *label == "n-2" {
            // n-2 runs the planning loop — show the real SM state
            let s = app.sm.state().to_kv_str();
            // "Idle" between cycles means "waiting for next task", not "off"
            if s == "Idle" { "Ready".into() } else { s }
        } else if !alive {
            "Offline".into()
        } else if paused {
            "Paused".into()
        } else {
            "Running".into()
        };
        json!({
            "label": label, "state": state, "alive": alive,
            "tools": tools_for_node(reporter, nid),
            "caps":  caps_for_node(reporter, nid),
        })
    }).collect();

    let log: Vec<Value> = {
        let l = app.log.lock().unwrap();
        l.iter().rev().take(30).map(|e| json!({
            "time": fmt_time(e.time_ms), "event": e.event, "detail": e.detail,
        })).collect()
    };

    let ms = app.manifest.read().unwrap().check_status(&app.agent_n2);
    let mesh_groups: Vec<Value> = ms.groups.iter().map(|g| json!({
        "name":        g.name,
        "description": g.description,
        "min_agents":  g.min_agents,
        "max_agents":  g.max_agents,
        "actual":      g.actual,
        "satisfied":   g.satisfied,
        "deficit":     g.deficit,
    })).collect();

    let mgr_port = *app.manager_port.lock().unwrap();
    json!({
        "nodes":       nodes,
        "log":         log,
        "total_calls": app.call_count.load(Ordering::Relaxed),
        "last_tool":   *app.last_tool.lock().unwrap(),
        "last_result": *app.last_result.lock().unwrap(),
        "mesh_status": {
            "healthy":        ms.is_healthy(),
            "total_deficit":  ms.total_deficit(),
            "groups":         mesh_groups,
        },
        "manager": {
            "port": mgr_port,
            "node": mgr_port.and_then(port_to_node_label),
        },
    }).to_string()
}

// ── Manifest control HTTP handlers ───────────────────────────────────────────

fn handle_manifest_get(app: &AppState) -> (u16, &'static str, String) {
    match app.agent_n2.get(manifest_keys::CURRENT) {
        Some(b) => match MeshManifest::from_toml_bytes(&b) {
            Some(m) => {
                let groups: Vec<Value> = m.groups.iter().map(|g| json!({
                    "name":        g.name,
                    "description": g.description,
                    "min_agents":  g.min_agents,
                    "max_agents":  g.max_agents,
                })).collect();
                let body = json!({
                    "name":    m.mesh.name,
                    "version": m.mesh.version,
                    "groups":  groups,
                }).to_string();
                (200, "application/json", body)
            }
            None => (500, "application/json", json!({"error":"manifest decode failed"}).to_string()),
        },
        None => (404, "application/json", json!({"error":"no manifest in KV"}).to_string()),
    }
}

async fn handle_manifest_post(app: &AppState, body: &[u8]) -> (u16, &'static str, String) {
    let Some(new_m) = MeshManifest::from_toml_bytes(body) else {
        return (400, "application/json", json!({"error":"invalid TOML"}).to_string());
    };
    let cur_ver = app.agent_n2.get(manifest_keys::VERSION)
        .and_then(|b| String::from_utf8(b.to_vec()).ok())
        .unwrap_or_else(|| "0.0.0".into());

    if !semver_gt(&new_m.mesh.version, &cur_ver) {
        return (409, "application/json", json!({
            "error": format!("version {} is not greater than current {cur_ver}", new_m.mesh.version)
        }).to_string());
    }

    // Archive old manifest
    if let Some(old) = app.agent_n2.get(manifest_keys::CURRENT) {
        let _ = app.agent_n2.set(manifest_keys::history(&cur_ver), old);
    }
    let new_ver = new_m.mesh.version.clone();
    let Ok(toml_str) = new_m.to_toml() else {
        return (500, "application/json", json!({"error":"manifest re-serialization failed"}).to_string());
    };
    let _ = app.agent_n2.set(manifest_keys::CURRENT,
                              Bytes::from(toml_str.into_bytes()));
    let _ = app.agent_n2.set(manifest_keys::VERSION,
                              Bytes::from(new_ver.clone().into_bytes()));

    // Update local manifest reference so check_status() reflects the new manifest immediately
    if let Ok(mut w) = app.manifest.write() {
        *w = new_m;
    }

    push_log(&app.log, "Manifest", format!("uploaded v{new_ver} (was v{cur_ver})"));
    (200, "application/json", json!({"ok": true, "version": new_ver}).to_string())
}

fn handle_system_status(app: &AppState) -> (u16, &'static str, String) {
    let ms = app.manifest.read().unwrap().check_status(&app.agent_n2);
    let groups: Vec<Value> = ms.groups.iter().map(|g| json!({
        "name":        g.name,
        "description": g.description,
        "min_agents":  g.min_agents,
        "max_agents":  g.max_agents,
        "actual":      g.actual,
        "satisfied":   g.satisfied,
        "deficit":     g.deficit,
    })).collect();
    let mgr_port = *app.manager_port.lock().unwrap();
    let body = json!({
        "healthy":        ms.is_healthy(),
        "total_deficit":  ms.total_deficit(),
        "groups":         groups,
        "manager_port":   mgr_port,
        "manager_node":   mgr_port.and_then(port_to_node_label),
    }).to_string();
    (200, "application/json", body)
}

async fn handle_system_stop(app: &AppState) -> (u16, &'static str, String) {
    let _ = app.agent_n2.set(manifest_keys::CONTROL_SYSTEM, Bytes::from_static(b"stopped"));
    push_log(&app.log, "Control", "system stop requested via HTTP");
    (200, "application/json", json!({"ok": true}).to_string())
}

async fn handle_system_start(app: &AppState) -> (u16, &'static str, String) {
    let _ = app.agent_n2.set(manifest_keys::CONTROL_SYSTEM, Bytes::from_static(b"running"));
    push_log(&app.log, "Control", "system start requested via HTTP");
    (200, "application/json", json!({"ok": true}).to_string())
}

async fn handle_group_control(app: &AppState, path: &str, is_stop: bool) -> (u16, &'static str, String) {
    // path is like /system/groups/compute/stop
    let group = path
        .trim_start_matches("/system/groups/")
        .trim_end_matches("/stop")
        .trim_end_matches("/start");
    if group.is_empty() {
        return (400, "application/json", json!({"error":"missing group name"}).to_string());
    }
    let key = manifest_keys::control_group(group);
    let val = if is_stop { Bytes::from_static(b"stopped") } else { Bytes::from_static(b"running") };
    let _ = app.agent_n2.set(key, val);
    push_log(&app.log, "Control",
        format!("group {group} {} via HTTP", if is_stop { "stop" } else { "start" }));
    (200, "application/json", json!({"ok": true, "group": group}).to_string())
}

// ── Preset handlers ──────────────────────────────────────────────────────────

fn handle_presets_list() -> (u16, &'static str, String) {
    let list: Vec<Value> = PRESETS.iter().map(|p| {
        let ver = MeshManifest::from_toml_bytes(p.toml.as_bytes())
            .map(|m| m.mesh.version)
            .unwrap_or_else(|| "0.1".into());
        json!({ "id": p.id, "name": p.name, "description": p.description, "version": ver })
    }).collect();
    (200, "application/json", json!(list).to_string())
}

async fn handle_preset_apply(app: &AppState, preset_id: &str) -> (u16, &'static str, String) {
    let Some(preset) = PRESETS.iter().find(|p| p.id == preset_id) else {
        return (404, "application/json", json!({"error":"preset not found"}).to_string());
    };
    let Some(mut new_m) = MeshManifest::from_toml_bytes(preset.toml.as_bytes()) else {
        return (500, "application/json", json!({"error":"preset parse failed"}).to_string());
    };
    let cur_ver = app.agent_n2.get(manifest_keys::VERSION)
        .and_then(|b| String::from_utf8(b.to_vec()).ok())
        .unwrap_or_else(|| "0.0.0".into());

    // Use the preset version if it's greater; otherwise bump the current patch
    let new_ver = if semver_gt(&new_m.mesh.version, &cur_ver) {
        new_m.mesh.version.clone()
    } else {
        let parse = |s: &str| -> (u64, u64, u64) {
            let mut p = s.trim_start_matches('v').split('.');
            (p.next().and_then(|x| x.parse().ok()).unwrap_or(0),
             p.next().and_then(|x| x.parse().ok()).unwrap_or(0),
             p.next().and_then(|x| x.parse().ok()).unwrap_or(0))
        };
        let (maj, min, pat) = parse(&cur_ver);
        format!("{maj}.{min}.{}", pat + 1)
    };
    new_m.mesh.version = new_ver.clone();

    if let Some(old) = app.agent_n2.get(manifest_keys::CURRENT) {
        let _ = app.agent_n2.set(manifest_keys::history(&cur_ver), old);
    }
    let Ok(toml_str) = new_m.to_toml() else {
        return (500, "application/json", json!({"error":"re-serialization failed"}).to_string());
    };
    let _ = app.agent_n2.set(manifest_keys::CURRENT, Bytes::from(toml_str.into_bytes()));
    let _ = app.agent_n2.set(manifest_keys::VERSION,  Bytes::from(new_ver.clone().into_bytes()));
    if let Ok(mut w) = app.manifest.write() { *w = new_m; }
    push_log(&app.log, "Preset", format!("'{}' applied as v{new_ver}", preset_id));
    (200, "application/json", json!({"ok":true,"version":new_ver}).to_string())
}

// ── HTTP server ───────────────────────────────────────────────────────────────

async fn handle_http(mut stream: tokio::net::TcpStream, app: Arc<AppState>, my_port: u16) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut buf = [0u8; 16384];
    let Ok(n) = stream.read(&mut buf).await else { return };
    let req = std::str::from_utf8(&buf[..n]).unwrap_or("");

    // Parse method and path from request line
    let mut lines = req.lines();
    let req_line = lines.next().unwrap_or("");
    let mut parts = req_line.split_whitespace();
    let method = parts.next().unwrap_or("GET");
    let path   = parts.next().unwrap_or("/");

    // Parse body (after blank line)
    let body_start = req.find("\r\n\r\n").map(|i| i + 4)
                        .or_else(|| req.find("\n\n").map(|i| i + 2))
                        .unwrap_or(n);
    let body_bytes = &buf[body_start..n];

    let is_post = method == "POST";

    // Non-manager: 307-redirect all requests to the elected manager
    let manager_port = *app.manager_port.lock().unwrap();
    if manager_port != Some(my_port) {
        let response = if let Some(port) = manager_port {
            format!(
                "HTTP/1.1 307 Temporary Redirect\r\nLocation: http://127.0.0.1:{port}{path}\r\nContent-Length: 0\r\nCache-Control: no-cache\r\nConnection: close\r\n\r\n"
            )
        } else {
            "HTTP/1.1 503 Service Unavailable\r\nContent-Type: text/plain\r\nContent-Length: 25\r\nRetry-After: 2\r\nConnection: close\r\n\r\nElecting management node.".to_string()
        };
        stream.write_all(response.as_bytes()).await.ok();
        return;
    }

    let (status, ct, body) = match (method, path) {
        (_, "/state") =>
            (200, "application/json", build_state_json(&app)),
        ("GET",  "/manifest") =>
            handle_manifest_get(&app),
        ("POST", "/manifest") =>
            handle_manifest_post(&app, body_bytes).await,
        ("GET",  "/system/status") =>
            handle_system_status(&app),
        ("POST", "/system/stop") =>
            handle_system_stop(&app).await,
        ("POST", "/system/start") =>
            handle_system_start(&app).await,
        ("GET",  "/presets") =>
            handle_presets_list(),
        _ if is_post && path.starts_with("/presets/") && path.ends_with("/apply") => {
            let id = path.trim_start_matches("/presets/").trim_end_matches("/apply");
            handle_preset_apply(&app, id).await
        }
        _ if is_post && path.starts_with("/system/groups/") => {
            let is_stop = path.ends_with("/stop");
            handle_group_control(&app, path, is_stop).await
        }
        _ =>
            (200, "text/html; charset=utf-8",
             include_str!("../docs/mesh_control.html").to_string()),
    };

    let response = format!(
        "HTTP/1.1 {status}\r\nContent-Type: {ct}\r\nContent-Length: {}\r\nAccess-Control-Allow-Origin: *\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    let _ = stream.write_all(response.as_bytes()).await;
}

// ── Main ──────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _ = tracing_subscriber::fmt().with_env_filter("warn").try_init();

    let llm_cfg = LlmConfig::from_env();
    println!("LLM mode : {}", if llm_cfg.mock { "MOCK" } else { "Ollama" });
    println!("Endpoint  : {}", llm_cfg.base_url);
    println!("Model     : {}", llm_cfg.model);

    // ── Load manifests ────────────────────────────────────────────────────────
    let manifest = MeshManifest::load_from_file("examples/mesh.toml")?;
    manifest.print_anatomy();
    println!();

    let cfg_n0 = NodeCapabilityConfig::load_from_file("examples/node_n0.toml")?;
    let cfg_n1 = NodeCapabilityConfig::load_from_file("examples/node_n1.toml")?;
    let mut cfg_n2 = NodeCapabilityConfig::load_from_file("examples/node_n2.toml")?;

    println!("Loaded node_n0.toml — {} capabilities", cfg_n0.capabilities.len());
    println!("Loaded node_n1.toml — {} capabilities", cfg_n1.capabilities.len());
    println!("Loaded node_n2.toml — {} capabilities", cfg_n2.capabilities.len());

    // Mock override: remove probe and set backend=mock on llm/inference so the
    // capability is advertised immediately without a live Ollama instance.
    if llm_cfg.mock {
        for entry in &mut cfg_n2.capabilities {
            if entry.ns == "llm" && entry.name == "inference" {
                entry.probe_url = None;
                entry.attrs.insert("backend".into(), TomlCapValue::Text("mock".into()));
            }
        }
        println!("Mock mode: llm/inference probe disabled");
    }

    // ── Spawn nodes ───────────────────────────────────────────────────────────
    let agent_n0 = make_agent(PORT_N0, &[PORT_N1, PORT_N2]);
    let agent_n1 = make_agent(PORT_N1, &[PORT_N0, PORT_N2]);
    let agent_n2 = make_agent(PORT_N2, &[PORT_N0, PORT_N1]);
    agent_n0.start().await?;
    agent_n1.start().await?;
    agent_n2.start().await?;

    let shared_log: SharedLog = Arc::new(Mutex::new(Vec::new()));

    // ── Pause flags (shared with probe loops + control watcher) ──────────────
    let pause_n0 = Arc::new(AtomicBool::new(false));
    let pause_n1 = Arc::new(AtomicBool::new(false));
    let pause_n2 = Arc::new(AtomicBool::new(false));

    // ── Manager candidacy handles ─────────────────────────────────────────────
    // Every node advertises itself as a system/manager candidate (TTL 15 s).
    // The elected manager is the node with the lexicographically smallest
    // node-id among all live candidates — a deterministic rule every node
    // computes independently from gossip state; no consensus or coordinator.
    //
    // n-0's handle is held behind Arc<Mutex<Option>> so the failure injector
    // can drop it (tombstoning the capability) and restore it on recovery.
    // n-1 and n-2 never fail in this demo so plain handles suffice.
    let mgr_h_n0 = Arc::new(Mutex::new(Some(agent_n0.advertise_capability(
        Capability::new("system", "manager")
            .with("http_port", CapValue::Integer(HTTP_PORT_N0 as i64)),
        Duration::from_secs(15),
    ))));
    let _mgr_h_n1 = agent_n1.advertise_capability(
        Capability::new("system", "manager")
            .with("http_port", CapValue::Integer(HTTP_PORT_N1 as i64)),
        Duration::from_secs(15),
    );
    let _mgr_h_n2 = agent_n2.advertise_capability(
        Capability::new("system", "manager")
            .with("http_port", CapValue::Integer(HTTP_PORT_N2 as i64)),
        Duration::from_secs(15),
    );

    // ── n-0: probe loop + fixed tools + simulated failure ─────────────────────
    let fail_flag_n0 = Arc::new(AtomicBool::new(false));
    {
        // Config-driven capability advertisement
        tokio::spawn(run_capability_probes(
            Arc::clone(&agent_n0), cfg_n0,
            Arc::clone(&pause_n0),
            probe_logger(Arc::clone(&shared_log), "n-0"),
        ));

        // Tool registration / health (separate from cap advertisement)
        let node = Arc::new(FixedToolNode {
            agent:     Arc::clone(&agent_n0),
            tools:     vec![
                ToolDef {
                    name: "weather",
                    description: "Get current weather for a city",
                    params: json!({"type":"object","properties":{"city":{"type":"string"}},"required":["city"]}),
                },
                ToolDef {
                    name: "ping",
                    description: "Ping a host and return latency",
                    params: json!({"type":"object","properties":{"host":{"type":"string"}},"required":["host"]}),
                },
            ],
            fail_flag: Arc::clone(&fail_flag_n0),
            log:       Arc::clone(&shared_log),
            handlers:  vec![
                |args| Box::pin(handle_weather(args)),
                |args| Box::pin(handle_ping(args)),
            ],
        });
        tokio::spawn(run_fixed_tool_node(Arc::clone(&node)));

        // Failure injector: flip fail_flag at T+35s for 15s.
        // Also drops n-0's manager candidacy so n-1 wins the election during
        // the outage, then restores it on recovery so n-0 reclaims the role.
        let flag  = Arc::clone(&fail_flag_n0);
        let log   = Arc::clone(&shared_log);
        let mgr   = Arc::clone(&mgr_h_n0);
        let agent = Arc::clone(&agent_n0);
        tokio::spawn(async move {
            time::sleep(Duration::from_secs(FAILURE_ONSET_S)).await;
            flag.store(true, Ordering::Relaxed);
            *mgr.lock().unwrap() = None; // drop candidacy → gossip tombstone
            push_log(&log, "Failure", "n-0 failure injected — n-1 will become manager");

            time::sleep(Duration::from_secs(FAILURE_HOLD_S)).await;
            flag.store(false, Ordering::Relaxed);
            *mgr.lock().unwrap() = Some(agent.advertise_capability(
                Capability::new("system", "manager")
                    .with("http_port", CapValue::Integer(HTTP_PORT_N0 as i64)),
                Duration::from_secs(15),
            ));
            push_log(&log, "Failure", "n-0 recovered — reclaiming manager role");
        });
    }

    // ── n-1: config caps + fixed tools + vector-search provision handler ──────
    {
        tokio::spawn(run_capability_probes(
            Arc::clone(&agent_n1), cfg_n1,
            Arc::clone(&pause_n1),
            probe_logger(Arc::clone(&shared_log), "n-1"),
        ));

        let node = Arc::new(FixedToolNode {
            agent:     Arc::clone(&agent_n1),
            tools:     vec![
                ToolDef {
                    name: "search",
                    description: "Search the web for information",
                    params: json!({"type":"object","properties":{"query":{"type":"string"}},"required":["query"]}),
                },
                ToolDef {
                    name: "calculate",
                    description: "Evaluate a simple arithmetic expression like '3 + 4'",
                    params: json!({"type":"object","properties":{"expression":{"type":"string"}},"required":["expression"]}),
                },
            ],
            fail_flag: Arc::new(AtomicBool::new(false)),
            log:       Arc::clone(&shared_log),
            handlers:  vec![
                |args| Box::pin(handle_search(args)),
                |args| Box::pin(handle_calculate(args)),
            ],
        });
        tokio::spawn(run_fixed_tool_node(Arc::clone(&node)));

        tokio::spawn(run_vector_search_provision_handler(
            Arc::clone(&agent_n1),
            Arc::clone(&shared_log),
        ));
    }

    // ── n-2: config caps (Ollama probe) + LLM provision handler ──────────────
    {
        tokio::spawn(run_capability_probes(
            Arc::clone(&agent_n2), cfg_n2,
            Arc::clone(&pause_n2),
            probe_logger(Arc::clone(&shared_log), "n-2"),
        ));

        tokio::spawn(run_llm_provision_handler(
            Arc::clone(&agent_n2),
            llm_cfg.base_url.clone(),
            Arc::clone(&shared_log),
        ));
    }

    // ── Push initial manifest to gossip KV ───────────────────────────────────
    {
        let toml_str = manifest.to_toml().expect("manifest serializable");
        let _ = agent_n2.set(manifest_keys::CURRENT,
                              Bytes::from(toml_str.into_bytes()));
        let _ = agent_n2.set(manifest_keys::VERSION,
                              Bytes::from(manifest.mesh.version.clone().into_bytes()));
        println!("Manifest v{} pushed to KV", manifest.mesh.version);
    }

    // ── Wait for mesh to settle ───────────────────────────────────────────────
    println!("\nWaiting {SETTLE_MS}ms for mesh to settle…");
    time::sleep(Duration::from_millis(SETTLE_MS)).await;

    // ── Print live mesh status from manifest ──────────────────────────────────
    {
        let status = manifest.check_status(&agent_n2);  // manifest is still local here, before AppState
        println!(
            "Mesh status   : {} (deficit: {})",
            if status.is_healthy() { "HEALTHY" } else { "DEGRADED" },
            status.total_deficit(),
        );
        for g in &status.groups {
            println!(
                "  {:22} {}/{} agents  {}",
                g.name, g.actual, g.min_agents,
                if g.satisfied { "✓" } else { "✗ needs more agents" },
            );
        }
        println!();
    }

    // ── n-2 requests vector-search from n-1 ──────────────────────────────────
    {
        let n1_id = agent_n1.node_id().clone();
        let a2    = Arc::clone(&agent_n2);
        let log   = Arc::clone(&shared_log);
        tokio::spawn(async move { request_vector_search(&a2, n1_id, &log).await });
    }

    // ── State machine for n-2 ─────────────────────────────────────────────────
    let sm = agent_n2.agent_state_machine(AgentPolicy {
        max_turns:   Some(10),
        tool_budget: Some(20),
        ..Default::default()
    });

    // ── Shared app state ──────────────────────────────────────────────────────
    // Map group name → which node's pause flag to flip when that group is stopped
    let group_node: HashMap<String, &'static str> = [
        ("realtime-data".to_string(), "n0"),
        ("compute".to_string(),       "n1"),
        ("llm-inference".to_string(), "n2"),
    ].into_iter().collect();

    let app = Arc::new(AppState {
        agent_n0:    Arc::clone(&agent_n0),
        agent_n1:    Arc::clone(&agent_n1),
        agent_n2:    Arc::clone(&agent_n2),
        sm:          Arc::clone(&sm),
        log:         Arc::clone(&shared_log),
        call_count:  Arc::new(AtomicU64::new(0)),
        last_tool:   Arc::new(Mutex::new(String::new())),
        last_result: Arc::new(Mutex::new(Value::Null)),
        manifest:    Arc::new(RwLock::new(manifest.clone())),
        pause_n0:    Arc::clone(&pause_n0),
        pause_n1:    Arc::clone(&pause_n1),
        pause_n2:    Arc::clone(&pause_n2),
        group_node:    Arc::new(group_node),
        manager_port:  Arc::new(Mutex::new(None)),
        manifest_caps: Arc::new(Mutex::new(Vec::new())),
    });

    // ── Seed initial manifest capabilities ───────────────────────────────────
    // Advertise the capabilities declared in the initial manifest so the mesh
    // shows HEALTHY immediately rather than waiting for the first manifest upload.
    {
        let m = app.manifest.read().unwrap().clone();
        let handles = advertise_manifest_caps(&m, [&agent_n0, &agent_n1, &agent_n2]);
        *app.manifest_caps.lock().unwrap() = handles;
    }

    // ── Manifest content watcher ──────────────────────────────────────────────
    // When manifest/current changes (new upload or preset apply), refresh the
    // dynamic capability handles so the mesh immediately reflects the new topology.
    tokio::spawn({
        let app        = Arc::clone(&app);
        let agent      = Arc::clone(&agent_n2);
        let agent_n0_w = Arc::clone(&agent_n0);
        let agent_n1_w = Arc::clone(&agent_n1);
        let agent_n2_w = Arc::clone(&agent_n2);
        async move {
            let mut rx = agent.subscribe_prefix("manifest/current");
            loop {
                if rx.changed().await.is_err() { break; }
                let m = app.manifest.read().unwrap().clone();
                let new_handles = advertise_manifest_caps(
                    &m, [&agent_n0_w, &agent_n1_w, &agent_n2_w],
                );
                *app.manifest_caps.lock().unwrap() = new_handles;
                push_log(&app.log, "Manifest", format!("topology v{} deployed to mesh", m.mesh.version));
            }
        }
    });

    // ── Manifest control watcher ──────────────────────────────────────────────
    // Watches manifest/control/ prefix via gossip KV subscription.
    // System stop/start flips all node pause flags; group stop/start targets
    // only the node(s) serving that group.
    tokio::spawn({
        let agent = Arc::clone(&agent_n2);
        let app   = Arc::clone(&app);
        async move {
            let mut rx = agent.subscribe_prefix("manifest/control/");
            loop {
                // Wait for any change under manifest/control/
                if rx.changed().await.is_err() { break; }
                let entries = agent.scan_prefix("manifest/control/");

                // Pass 1 — system-level control establishes the baseline for all nodes.
                // Must run before group overrides so alphabetic scan order ('g' < 's')
                // doesn't cause a system=running entry to clobber a group=stopped entry.
                let sys_stopped = entries.iter()
                    .find(|(k, _)| k.as_ref() == manifest_keys::CONTROL_SYSTEM)
                    .map(|(_, v)| v.as_ref() == b"stopped");
                if let Some(stopped) = sys_stopped {
                    app.pause_n0.store(stopped, Ordering::Relaxed);
                    app.pause_n1.store(stopped, Ordering::Relaxed);
                    app.pause_n2.store(stopped, Ordering::Relaxed);
                    push_log(&app.log, "Control",
                        if stopped { "system stopped" } else { "system started" });
                }

                // Pass 2 — per-group overrides, applied only when system is running.
                if !sys_stopped.unwrap_or(false) {
                    for (key, val) in &entries {
                        if let Some(group) = key.strip_prefix(manifest_keys::CONTROL_GROUP_PREFIX) {
                            let stopped = val.as_ref() == b"stopped";
                            let flag = match app.group_node.get(group).copied().unwrap_or("") {
                                "n0" => Some(Arc::clone(&app.pause_n0)),
                                "n1" => Some(Arc::clone(&app.pause_n1)),
                                "n2" => Some(Arc::clone(&app.pause_n2)),
                                _    => None,
                            };
                            if let Some(f) = flag {
                                f.store(stopped, Ordering::Relaxed);
                            }
                            push_log(&app.log, "Control",
                                format!("group {} {}", group,
                                        if stopped { "stopped" } else { "started" }));
                        }
                    }
                }
            }
        }
    });

    // ── Manager election watcher ──────────────────────────────────────────────
    // Watches live system/manager candidates in gossip. Re-computes the elected
    // manager (lexicographically smallest node-id) whenever the candidate set
    // changes (TTL expiry, new advertiser, recovery).
    tokio::spawn({
        let app   = Arc::clone(&app);
        let agent = Arc::clone(&agent_n0);
        async move {
            // Bootstrap: compute initial state before entering the watch loop
            {
                let candidates = agent.resolve(&CapFilter::new("system", "manager"));
                *app.manager_port.lock().unwrap() = compute_manager_port(&candidates);
            }
            let mut rx = agent.watch_capabilities(CapFilter::new("system", "manager"));
            loop {
                match rx.recv().await {
                    None    => break,
                    Some(_) => {}
                }
                let candidates = agent.resolve(&CapFilter::new("system", "manager"));
                let new_port   = compute_manager_port(&candidates);
                let mut guard  = app.manager_port.lock().unwrap();
                if *guard != new_port {
                    *guard = new_port;
                    match new_port {
                        Some(p) => push_log(&app.log, "Manager",
                            format!("elected → :{p} ({})", port_to_node_label(p).unwrap_or("?"))),
                        None    => push_log(&app.log, "Manager", "no candidates — re-electing"),
                    }
                }
            }
        }
    });

    tokio::spawn({
        let app2 = Arc::clone(&app);
        async move { run_agent_loop(app2, llm_cfg).await }
    });

    // ── HTTP servers (one per node) ───────────────────────────────────────────
    // Each node listens on its own port. Non-manager nodes 307-redirect to the
    // elected manager. All three must listen so redirects resolve correctly
    // even when n-0 (port 8100) fails and n-1 (8101) becomes the manager.
    for port in [HTTP_PORT_N0, HTTP_PORT_N1, HTTP_PORT_N2] {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", port)).await?;
        println!("Serving http://127.0.0.1:{port}");
        let app = Arc::clone(&app);
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else { continue };
                let app = Arc::clone(&app);
                tokio::spawn(async move { handle_http(stream, app, port).await });
            }
        });
    }

    // Keep main alive
    tokio::signal::ctrl_c().await?;
    println!("\nShutting down.");
    Ok(())
}
