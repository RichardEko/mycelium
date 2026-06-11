//! A2A (Agent-to-Agent) protocol adapter — `a2a` feature.
//!
//! Exposes two HTTP endpoints:
//! - `GET  /.well-known/agent.json` — A2A discovery (AgentCard)
//! - `POST /a2a`                    — JSON-RPC 2.0 task dispatch
//!
//! The card is built dynamically from the live `cap/` KV prefix so late-joining
//! nodes are immediately discoverable without re-configuration.
//!
//! ## JSON-RPC methods
//! | Method               | Behaviour                                                  |
//! |----------------------|------------------------------------------------------------|
//! | `tasks/send`         | Synchronous: resolve skill, RPC call, return completed task|
//! | `tasks/sendSubscribe`| SSE stream: submitted → working → completed/failed         |
//! | `tasks/get`          | Retrieve a previously-created task by `id`                 |
//! | `tasks/cancel`       | Cancel a pending task; error if already completed          |

use axum::{
    Router,
    extract::State,
    response::{IntoResponse, Json, Sse},
    response::sse::{Event, KeepAlive},
    routing::{get, post},
};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    convert::Infallible,
    sync::Arc,
    time::{Duration, Instant},
};
use tracing::warn;

use crate::agent::TaskCtx;
use crate::capability::CapFilter;
use crate::store::scan_kv_prefix;
use super::capability_ops::{is_cap_locality_key, parse_cap_key_or_warn};

// ── Wire types ────────────────────────────────────────────────────────────────

#[derive(Serialize)]
pub(crate) struct AgentCard {
    pub name:         String,
    pub url:          String,
    pub version:      String,
    pub capabilities: A2aCapabilities,
    pub skills:       Vec<AgentSkill>,
}

#[derive(Serialize)]
pub(crate) struct A2aCapabilities {
    pub streaming: bool,
}

#[derive(Serialize)]
pub(crate) struct AgentSkill {
    pub id:          String,
    pub name:        String,
    pub description: String,
    /// Machine-readable JSON Schema for the skill's input, taken from the
    /// gossiped `skills/{ns}/{name}/{node}/input` entry when present.
    /// Additive extension to the A2A card: tool-calling frameworks use it to
    /// build properly-typed tools instead of guessing field names from prose.
    #[serde(rename = "inputSchema", skip_serializing_if = "Option::is_none", default)]
    pub input_schema: Option<Value>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub(crate) struct Task {
    pub id:        String,
    pub status:    TaskStatus,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub artifacts: Vec<Artifact>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub(crate) struct TaskStatus {
    pub state: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub(crate) struct Artifact {
    pub parts: Vec<Part>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[allow(dead_code)]
pub(crate) struct Message {
    pub role:  String,
    pub parts: Vec<Part>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(tag = "type", rename_all = "lowercase")]
pub(crate) enum Part {
    Text { text: String },
}

// ── In-memory task store ──────────────────────────────────────────────────────

pub(crate) struct A2aTask {
    pub task:       Task,
    pub created_at: Instant,
}

// ── Router context ────────────────────────────────────────────────────────────

#[derive(Clone)]
pub(crate) struct A2aState {
    pub task_ctx: Arc<TaskCtx>,
    pub tasks:    Arc<papaya::HashMap<String, A2aTask>>,
}

// ── Public constructor ────────────────────────────────────────────────────────

/// Spawns a background task that evicts A2A tasks older than 5 minutes.
/// Call this once after creating the shared `tasks` map.
pub(crate) fn spawn_cleanup(tasks: Arc<papaya::HashMap<String, A2aTask>>) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(60));
        loop {
            interval.tick().await;
            evict_stale_tasks(&tasks, Instant::now());
        }
    });
}

const A2A_TASK_TTL: Duration = Duration::from_secs(300);

/// One eviction sweep over `tasks`: removes entries older than
/// [`A2A_TASK_TTL`] as of `now`. The removal is a conditional `compute`:
/// a status update can re-insert a task id with a fresh `created_at`
/// between collect and removal, and an unconditional `remove()` would evict
/// the live task — clients polling it would get NotFound (M2 Run-18 sweep
/// finding, same family as the tombstone-GC conditional remove).
fn evict_stale_tasks(tasks: &papaya::HashMap<String, A2aTask>, now: Instant) {
    let to_remove: Vec<String> = tasks.pin().iter()
        .filter(|(_, v)| now.duration_since(v.created_at) > A2A_TASK_TTL)
        .map(|(k, _)| k.clone().to_string())
        .collect();
    let guard = tasks.pin();
    for k in to_remove {
        guard.compute(k, |existing| match existing {
            Some((_, v)) if now.duration_since(v.created_at) > A2A_TASK_TTL =>
                papaya::Operation::Remove,
            _ => papaya::Operation::Abort(()),
        });
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn new_task_id(hlc: &crate::hlc::Hlc) -> String {
    format!("{:016x}-{:016x}", hlc.tick(), fastrand::u64(1..))
}

fn text_from_message(msg: &Value) -> String {
    msg.get("parts")
        .and_then(|p| p.as_array())
        .and_then(|arr| arr.first())
        .and_then(|part| part.get("text"))
        .and_then(|t| t.as_str())
        .unwrap_or("")
        .to_string()
}

fn completed_task(id: String, reply: Bytes) -> Task {
    let text = String::from_utf8_lossy(&reply).into_owned();
    Task {
        id,
        status:    TaskStatus { state: "completed".into() },
        artifacts: vec![Artifact { parts: vec![Part::Text { text }] }],
    }
}

fn jsonrpc_error(id: Option<Value>, code: i32, message: &str) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": { "code": code, "message": message }
    })
}

fn jsonrpc_ok(id: Option<Value>, result: Value) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "result": result })
}

/// Resolves the first node for `skill_id` ("ns/name") from the live cap/ KV prefix.
fn resolve_skill(ctx: &TaskCtx, skill_id: &str) -> Option<crate::node_id::NodeId> {
    let parts: Vec<&str> = skill_id.splitn(2, '/').collect();
    if parts.len() != 2 { return None; }
    let filter = CapFilter::new(parts[0], parts[1]);
    let providers = resolve_providers(ctx, &filter);
    providers.into_iter().next().map(|(n, _)| n)
}

fn resolve_providers(
    ctx:    &TaskCtx,
    filter: &CapFilter,
) -> Vec<(crate::node_id::NodeId, crate::capability::Capability)> {
    use crate::capability::Capability;
    let mut out = Vec::new();
    for (key, bytes) in scan_kv_prefix(&ctx.kv_state, "cap/") {
        if is_cap_locality_key(&key) { continue; }
        let Some((node_id, _ns, _name)) = parse_cap_key_or_warn("cap/", &key) else { continue };
        let Some(cap) = Capability::decode(&bytes) else { continue };
        if filter.matches(&cap) {
            out.push((node_id, cap));
        }
    }
    out
}

/// Builds a human/LLM-readable description for a skill from its gossiped
/// input schema (`skills/{ns}/{name}/{node}/input`, published by SkillRunner).
/// Tool-calling frameworks read the card's `description` to decide how to
/// invoke a skill — an empty description left LangChain agents passing plain
/// text to skills that require a JSON object (observed live: the orchestrator
/// rejected `"CRDTs"` with a JSON parse error and the agent silently fell
/// back to answering from its own weights).
fn skill_input_description(kv: &crate::store::KvState, skill_id: &str) -> (String, Option<Value>) {
    let prefix = format!("skills/{skill_id}/");
    for (key, bytes) in scan_kv_prefix(kv, &prefix) {
        if !key.ends_with("/input") { continue; }
        let Ok(schema) = serde_json::from_slice::<Value>(&bytes) else { continue };
        let required: Vec<&str> = schema.get("required")
            .and_then(|r| r.as_array())
            .map(|a| a.iter().filter_map(|v| v.as_str()).collect())
            .unwrap_or_default();
        let props = schema.get("properties").and_then(|p| p.as_object());
        let mut fields: Vec<String> = Vec::new();
        if let Some(props) = props {
            for (name, spec) in props {
                let ty   = spec.get("type").and_then(|t| t.as_str()).unwrap_or("any");
                let desc = spec.get("description").and_then(|d| d.as_str()).unwrap_or("");
                let req  = if required.contains(&name.as_str()) { "required" } else { "optional" };
                if desc.is_empty() {
                    fields.push(format!("{name} ({ty}, {req})"));
                } else {
                    fields.push(format!("{name} ({ty}, {req}): {desc}"));
                }
            }
        }
        let text = if fields.is_empty() {
            format!("Mycelium mesh skill {skill_id}. Input: a JSON object.")
        } else {
            format!(
                "Mycelium mesh skill {skill_id}. Input MUST be a JSON object with: {}.",
                fields.join("; "),
            )
        };
        return (text, Some(schema));
    }
    (String::new(), None)
}

// ── Handlers ──────────────────────────────────────────────────────────────────

async fn agent_card_handler(State(state): State<A2aState>) -> impl IntoResponse {
    let kv = &state.task_ctx.kv_state;
    let mut skill_ids: std::collections::HashSet<String> = std::collections::HashSet::new();
    for (key, bytes) in scan_kv_prefix(kv, "cap/") {
        if is_cap_locality_key(&key) { continue; }
        let Some((_node_id, ns, name)) = parse_cap_key_or_warn("cap/", &key) else { continue };
        use crate::capability::Capability;
        if Capability::decode(&bytes).is_some() {
            skill_ids.insert(format!("{}/{}", ns, name));
        }
    }
    let skills: Vec<AgentSkill> = skill_ids.into_iter().map(|id| {
        let (description, input_schema) = skill_input_description(kv, &id);
        AgentSkill { name: id.clone(), id, description, input_schema }
    }).collect();

    let node_addr = state.task_ctx.node_id.to_string();
    let card = AgentCard {
        name:         format!("Mycelium/{}", node_addr),
        url:          format!("http://{}", node_addr),
        version:      "1.0.0".into(),
        capabilities: A2aCapabilities { streaming: true },
        skills,
    };
    Json(card)
}

async fn handle_tasks_send(
    state:  &A2aState,
    id:     Option<Value>,
    params: &Value,
) -> Value {
    let task_id  = params.get("id")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .unwrap_or_else(|| new_task_id(&state.task_ctx.hlc));
    let skill_id = params.get("skillId")
        .or_else(|| params.get("skill_id"))
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let message  = params.get("message").cloned().unwrap_or(Value::Null);
    let text     = text_from_message(&message);

    let Some(target) = resolve_skill(&state.task_ctx, skill_id) else {
        return jsonrpc_error(id, -32001, "skill not found");
    };

    // Generous: A2A skills are frequently multi-step LLM pipelines
    // (orchestrator → researcher → writer takes ~90 s on local Ollama);
    // a 30 s cap made every such composition fail with -32603 while the
    // pipeline was still working. Clients enforce their own HTTP timeouts.
    let timeout = Duration::from_secs(120);
    match super::rpc::rpc_call_ctx(
        &state.task_ctx, target,
        "skill.invoke".into(), Bytes::from(text.into_bytes()), timeout,
    ).await {
        Ok(reply) => {
            let task = completed_task(task_id.clone(), reply);
            state.tasks.pin().insert(task_id, A2aTask { task: task.clone(), created_at: Instant::now() });
            jsonrpc_ok(id, serde_json::to_value(&task).unwrap_or(Value::Null))
        }
        Err(e) => {
            warn!("A2A tasks/send rpc_call failed for skill {}: {:?}", skill_id, e);
            jsonrpc_error(id, -32603, "rpc call failed")
        }
    }
}

fn handle_tasks_get(state: &A2aState, id: Option<Value>, params: &Value) -> Value {
    let task_id = params.get("id").and_then(|v| v.as_str()).unwrap_or("");
    match state.tasks.pin().get(task_id) {
        Some(entry) => jsonrpc_ok(id, serde_json::to_value(&entry.task).unwrap_or(Value::Null)),
        None        => jsonrpc_error(id, -32001, "task not found"),
    }
}

fn handle_tasks_cancel(state: &A2aState, id: Option<Value>, params: &Value) -> Value {
    let task_id = params.get("id").and_then(|v| v.as_str()).unwrap_or("");
    let guard = state.tasks.pin();
    match guard.get(task_id) {
        Some(entry) if entry.task.status.state == "completed" => {
            jsonrpc_error(id, -32002, "task already completed")
        }
        Some(_) => {
            guard.remove(task_id);
            jsonrpc_ok(id, json!({ "id": task_id, "status": { "state": "canceled" } }))
        }
        None => jsonrpc_error(id, -32001, "task not found"),
    }
}

// ── tasks/sendSubscribe SSE ───────────────────────────────────────────────────

/// A2A SSE handler for `tasks/sendSubscribe`. Unlike the JSON-RPC handler,
/// this is called via a separate route when the request body specifies
/// `"method": "tasks/sendSubscribe"`. We expose it as a plain `async fn`
/// so the router can dispatch to it after the JSON body is parsed.
pub(crate) async fn tasks_send_subscribe(
    state:    A2aState,
    id:       Option<Value>,
    task_id:  String,
    skill_id: String,
    text:     String,
) -> impl IntoResponse {
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<Event, Infallible>>(8);

    // Emit "submitted" immediately.
    let _ = tx.try_send(Ok(Event::default()
        .event("task_status_update")
        .data(json!({ "id": &task_id, "status": { "state": "submitted" } }).to_string())));

    let state2   = state.clone();
    let task_id2 = task_id.clone();
    let id2      = id.clone();
    tokio::spawn(async move {
        let target = resolve_skill(&state2.task_ctx, &skill_id);
        let Some(target) = target else {
            let _ = tx.send(Ok(Event::default()
                .event("task_status_update")
                .data(json!({ "id": &task_id2, "status": { "state": "failed" }, "error": "skill not found" }).to_string()))).await;
            return;
        };

        // Emit "working" after a short delay while the RPC runs.
        let tx2      = tx.clone();
        let task_id3 = task_id2.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let _ = tx2.try_send(Ok(Event::default()
                .event("task_status_update")
                .data(json!({ "id": &task_id3, "status": { "state": "working" } }).to_string())));
        });

        let timeout = Duration::from_secs(30);
        match super::rpc::rpc_call_ctx(
            &state2.task_ctx, target,
            "skill.invoke".into(), Bytes::from(text.into_bytes()), timeout,
        ).await {
            Ok(reply) => {
                let task = completed_task(task_id2.clone(), reply);
                state2.tasks.pin().insert(task_id2.clone(), A2aTask { task: task.clone(), created_at: Instant::now() });
                let _ = tx.send(Ok(Event::default()
                    .event("task_status_update")
                    .data(serde_json::to_string(&task).unwrap_or_default()))).await;
            }
            Err(e) => {
                warn!("A2A sendSubscribe rpc_call failed for skill {}: {:?}", skill_id, e);
                let _ = tx.send(Ok(Event::default()
                    .event("task_status_update")
                    .data(json!({ "id": &task_id2, "status": { "state": "failed" } }).to_string()))).await;
            }
        }
        drop(id2); // suppress unused warning
    });

    let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
    Sse::new(stream).keep_alive(KeepAlive::default())
}

// ── Full JSON-RPC handler with SSE dispatch ───────────────────────────────────

pub(crate) async fn a2a_jsonrpc_full(
    State(state): State<A2aState>,
    Json(body):   Json<Value>,
) -> axum::response::Response {
    let id     = body.get("id").cloned();
    let method = body.get("method").and_then(|m| m.as_str()).unwrap_or("").to_string();
    let params = body.get("params").cloned().unwrap_or(Value::Null);

    if method == "tasks/sendSubscribe" {
        let task_id  = params.get("id")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_else(|| new_task_id(&state.task_ctx.hlc));
        let skill_id = params.get("skillId")
            .or_else(|| params.get("skill_id"))
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let text     = text_from_message(params.get("message").unwrap_or(&Value::Null));
        return tasks_send_subscribe(state, id, task_id, skill_id, text).await.into_response();
    }

    let result: Value = match method.as_str() {
        "tasks/send"   => handle_tasks_send(&state, id.clone(), &params).await,
        "tasks/get"    => handle_tasks_get(&state, id.clone(), &params),
        "tasks/cancel" => handle_tasks_cancel(&state, id.clone(), &params),
        _              => jsonrpc_error(id, -32601, "method not found"),
    };
    Json(result).into_response()
}

// ── Router (updated to use full handler with SSE) ─────────────────────────────

/// Returns the A2A router. Use this variant when `tasks/sendSubscribe` SSE support is needed.
pub(crate) fn a2a_router_full(
    task_ctx: Arc<TaskCtx>,
    tasks:    Arc<papaya::HashMap<String, A2aTask>>,
) -> Router {
    let state = A2aState { task_ctx, tasks };
    Router::new()
        .route("/.well-known/agent.json", get(agent_card_handler))
        .route("/a2a",                    post(a2a_jsonrpc_full))
        .with_state(state)
}

// ── Unit tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{GossipAgent, GossipConfig, NodeId};

    fn make_ctx() -> Arc<TaskCtx> {
        let agent = GossipAgent::new(
            NodeId::new("127.0.0.1", 0).unwrap(),
            GossipConfig::default(),
        );
        Arc::clone(&agent.task_ctx)
    }

    #[test]
    fn agent_card_from_empty_caps() {
        let ctx   = make_ctx();
        let kv    = &ctx.kv_state;
        let pairs = scan_kv_prefix(kv, "cap/");
        assert!(pairs.is_empty(), "fresh agent has no caps");
    }

    #[test]
    fn agent_card_deduplicates_skills() {
        use crate::capability::Capability;
        use crate::framing::make_gossip_update;
        use crate::store::apply_and_notify;

        let ctx  = make_ctx();
        let node = ctx.node_id.clone();
        let cap  = Capability::new("compute", "gpu");
        let bytes = cap.encode();

        // Two cap/ entries for the same (ns, name) from different nodes.
        for suffix in ["cap/127.0.0.1:9001/compute/gpu", "cap/127.0.0.1:9002/compute/gpu"] {
            let upd = make_gossip_update(
                &node, 5, std::sync::Arc::from(suffix),
                bytes.clone(), false, &ctx.hlc,
            );
            apply_and_notify(&ctx.kv_state, &upd);
        }

        let mut skill_ids: std::collections::HashSet<String> = std::collections::HashSet::new();
        for (key, kv_bytes) in scan_kv_prefix(&ctx.kv_state, "cap/") {
            if is_cap_locality_key(&key) { continue; }
            let Some((_n, ns, name)) = parse_cap_key_or_warn("cap/", &key) else { continue };
            if Capability::decode(&kv_bytes).is_some() {
                skill_ids.insert(format!("{}/{}", ns, name));
            }
        }
        assert_eq!(skill_ids.len(), 1, "two nodes with same cap must produce one skill");
    }

    #[test]
    fn tasks_get_unknown_returns_error() {
        let tasks = Arc::new(papaya::HashMap::<String, A2aTask>::new());
        let state = A2aState { task_ctx: make_ctx(), tasks };
        let result = handle_tasks_get(&state, None, &json!({ "id": "no-such-id" }));
        assert_eq!(result["error"]["code"], -32001);
    }

    /// The agent card must surface each skill's gossiped input schema as a
    /// description — tool-calling frameworks decide how to invoke from it,
    /// and an empty description left agents passing plain text to
    /// JSON-expecting skills.
    #[test]
    fn agent_card_description_renders_input_schema() {
        let kv = crate::store::KvState::new(0);
        let schema = serde_json::json!({
            "type": "object",
            "required": ["topic"],
            "properties": {
                "topic":      { "type": "string", "description": "Topic to write about" },
                "max_points": { "type": "integer" },
            },
        });
        kv.store.pin().insert(
            Arc::from("skills/llm/orchestrator/127.0.0.1:7950/input"),
            crate::store::StoreEntry {
                data:      Some(bytes::Bytes::from(serde_json::to_vec(&schema).unwrap())),
                timestamp: 1,
            },
        );
        let (desc, schema) = skill_input_description(&kv, "llm/orchestrator");
        assert!(schema.is_some(), "raw schema is exposed on the card as inputSchema");
        assert!(desc.contains("JSON object"), "tells callers the payload shape: {desc}");
        assert!(desc.contains("topic (string, required): Topic to write about"), "{desc}");
        assert!(desc.contains("max_points (integer, optional)"), "{desc}");
        // No schema on the mesh → empty description, never a panic.
        let (empty_desc, no_schema) = skill_input_description(&kv, "llm/unknown");
        assert_eq!(empty_desc, "");
        assert!(no_schema.is_none());
    }

    /// M2 Run-18 race-family sweep regression: the cleanup sweep removes only
    /// entries that are still stale at removal time — a task re-inserted with
    /// a fresh `created_at` (status update) survives the sweep.
    #[test]
    fn cleanup_sweep_evicts_stale_keeps_fresh() {
        let mk = |id: &str| Task {
            id: id.into(),
            status: TaskStatus { state: "working".into() },
            artifacts: vec![],
        };
        let tasks = Arc::new(papaya::HashMap::<String, A2aTask>::new());
        let now = Instant::now();
        let old = now.checked_sub(Duration::from_secs(600)).expect("clock supports -600s");
        tasks.pin().insert("stale".into(), A2aTask { task: mk("stale"), created_at: old });
        tasks.pin().insert("fresh".into(), A2aTask { task: mk("fresh"), created_at: now });
        evict_stale_tasks(&tasks, now);
        assert!(tasks.pin().get("stale").is_none(), "stale task evicted");
        assert!(tasks.pin().get("fresh").is_some(), "fresh task survives the sweep");
    }

    #[test]
    fn tasks_cancel_completed_returns_error() {
        let tasks = Arc::new(papaya::HashMap::<String, A2aTask>::new());
        let task  = Task {
            id:        "t1".into(),
            status:    TaskStatus { state: "completed".into() },
            artifacts: vec![],
        };
        tasks.pin().insert("t1".into(), A2aTask { task, created_at: Instant::now() });
        let state = A2aState { task_ctx: make_ctx(), tasks };
        let result = handle_tasks_cancel(&state, None, &json!({ "id": "t1" }));
        assert_eq!(result["error"]["code"], -32002);
    }
}
