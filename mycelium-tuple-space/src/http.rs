//! HTTP gateway endpoints (`gateway` feature): the five tuple operations
//! under `/gateway/tuple/*` plus the cluster-wide `/api/tuple` monitoring
//! aggregation. Register with the agent's embedded gateway:
//!
//! ```rust,ignore
//! let ts = TupleSpace::new(Arc::clone(&agent), cfg).await?;
//! agent.with_http_routes(ts.clone().http_router());
//! agent.start().await?; // routes must be registered before start
//! ```
//!
//! Base64 appears only here — the internal RPC path stays binary.

use axum::Json;
use axum::extract::{Query, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as B64;
use bytes::Bytes;
use serde::Deserialize;
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use crate::{TupleError, TupleSpace};

impl TupleSpace {
    /// Returns an axum `Router` with the tuple gateway endpoints, ready for
    /// [`GossipAgent::with_http_routes`](mycelium::GossipAgent::with_http_routes).
    pub fn http_router(self: Arc<Self>) -> axum::Router {
        axum::Router::new()
            .route("/gateway/tuple/put", post(gw_put))
            .route("/gateway/tuple/take", post(gw_take))
            .route("/gateway/tuple/take_by_key", post(gw_take_by_key))
            .route("/gateway/tuple/complete", post(gw_complete))
            .route("/gateway/tuple/ack", post(gw_ack))
            .route("/gateway/tuple/depth", get(gw_depth))
            .route("/api/tuple", get(api_tuple))
            .with_state(self)
    }
}

fn err_response(e: &TupleError) -> Response {
    let (status, mut headers) = match e {
        TupleError::Backpressure { .. } | TupleError::NoProvider => {
            let mut h = HeaderMap::new();
            h.insert("Retry-After", HeaderValue::from_static("1"));
            (StatusCode::SERVICE_UNAVAILABLE, h)
        }
        TupleError::Timeout => (StatusCode::REQUEST_TIMEOUT, HeaderMap::new()),
        TupleError::NotFound => (StatusCode::NOT_FOUND, HeaderMap::new()),
        _ => (StatusCode::BAD_GATEWAY, HeaderMap::new()),
    };
    headers.insert("Content-Type", HeaderValue::from_static("application/json"));
    (status, headers, json!({ "error": e.to_string() }).to_string()).into_response()
}

fn bad_request(msg: &str) -> Response {
    (StatusCode::BAD_REQUEST, Json(json!({ "error": msg }))).into_response()
}

/// `ns` is optional in every body; when present it must match this space.
fn ns_mismatch(ts: &TupleSpace, ns: &Option<String>) -> bool {
    ns.as_ref().is_some_and(|n| n.as_str() != ts.namespace().as_ref())
}

#[derive(Deserialize)]
struct PutBody {
    ns: Option<String>,
    stage: String,
    payload_b64: String,
    /// M13 (WS-G): when present, the item is put under this correlation key and is claimable only by
    /// a matching `take_by_key`. Omit for an ordinary FIFO put.
    #[serde(default)]
    key: Option<String>,
}

async fn gw_put(State(ts): State<Arc<TupleSpace>>, Json(b): Json<PutBody>) -> Response {
    if ns_mismatch(&ts, &b.ns) {
        return bad_request("unknown namespace");
    }
    let Ok(payload) = B64.decode(&b.payload_b64) else {
        return bad_request("payload_b64 is not valid base64");
    };
    let result = match &b.key {
        Some(key) => ts.put_keyed(&b.stage, key, Bytes::from(payload)).await,
        None => ts.put(&b.stage, Bytes::from(payload)).await,
    };
    match result {
        Ok(id) => Json(json!({ "id": id })).into_response(),
        Err(e) => err_response(&e),
    }
}

#[derive(Deserialize)]
struct TakeByKeyBody {
    ns: Option<String>,
    stage: String,
    key: String,
    #[serde(default = "default_take_timeout")]
    timeout_secs: u64,
}

async fn gw_take_by_key(State(ts): State<Arc<TupleSpace>>, Json(b): Json<TakeByKeyBody>) -> Response {
    if ns_mismatch(&ts, &b.ns) {
        return bad_request("unknown namespace");
    }
    match ts.take_by_key(&b.stage, &b.key, Duration::from_secs(b.timeout_secs)).await {
        Ok((id, payload)) => Json(json!({
            "id": id,
            "stage": b.stage,
            "key": b.key,
            "payload_b64": B64.encode(&payload),
        }))
        .into_response(),
        Err(e) => err_response(&e),
    }
}

#[derive(Deserialize)]
struct TakeBody {
    ns: Option<String>,
    stage: String,
    #[serde(default = "default_take_timeout")]
    timeout_secs: u64,
}

fn default_take_timeout() -> u64 {
    30
}

async fn gw_take(State(ts): State<Arc<TupleSpace>>, Json(b): Json<TakeBody>) -> Response {
    if ns_mismatch(&ts, &b.ns) {
        return bad_request("unknown namespace");
    }
    match ts.take(&b.stage, Duration::from_secs(b.timeout_secs)).await {
        Ok((id, payload)) => Json(json!({
            "id": id,
            "stage": b.stage,
            "payload_b64": B64.encode(&payload),
        }))
        .into_response(),
        Err(e) => err_response(&e),
    }
}

#[derive(Deserialize)]
struct CompleteBody {
    ns: Option<String>,
    id: u64,
    next_stage: String,
    next_payload_b64: String,
}

async fn gw_complete(
    State(ts): State<Arc<TupleSpace>>,
    Json(b): Json<CompleteBody>,
) -> Response {
    if ns_mismatch(&ts, &b.ns) {
        return bad_request("unknown namespace");
    }
    let Ok(payload) = B64.decode(&b.next_payload_b64) else {
        return bad_request("next_payload_b64 is not valid base64");
    };
    match ts.complete(b.id, &b.next_stage, Bytes::from(payload)).await {
        Ok(next_id) => Json(json!({ "next_id": next_id })).into_response(),
        Err(e) => err_response(&e),
    }
}

#[derive(Deserialize)]
struct AckBody {
    ns: Option<String>,
    id: u64,
}

async fn gw_ack(State(ts): State<Arc<TupleSpace>>, Json(b): Json<AckBody>) -> Response {
    if ns_mismatch(&ts, &b.ns) {
        return bad_request("unknown namespace");
    }
    match ts.ack(b.id).await {
        Ok(()) => Json(json!({ "ok": true })).into_response(),
        Err(e) => err_response(&e),
    }
}

#[derive(Deserialize)]
struct DepthQuery {
    ns: Option<String>,
    stage: Option<String>,
}

async fn gw_depth(
    State(ts): State<Arc<TupleSpace>>,
    Query(q): Query<DepthQuery>,
) -> Response {
    if ns_mismatch(&ts, &q.ns) {
        return bad_request("unknown namespace");
    }
    match ts.depth(q.stage.as_deref()).await {
        Ok(depths) => {
            let stages: Vec<Value> = depths
                .iter()
                .map(|d| {
                    json!({
                        "stage": d.stage.as_ref(),
                        "depth": d.depth,
                        "waiters": d.waiters,
                        "inflight": d.inflight,
                    })
                })
                .collect();
            Json(json!({ "stages": stages })).into_response()
        }
        Err(e) => err_response(&e),
    }
}

/// Aggregates every `sys/tuple/{node}/{ns}/…` key in the local gossip view
/// into one cluster-wide monitoring document (all namespaces, all nodes).
async fn api_tuple(State(ts): State<Arc<TupleSpace>>) -> Response {
    // node → ns → (role, wal_bytes, stage → metric → value, pressure set)
    type StageMap = BTreeMap<String, BTreeMap<String, Value>>;
    #[derive(Default)]
    struct NodeNs {
        role: Option<String>,
        wal_bytes: Option<u64>,
        stages: StageMap,
        pressure: Vec<String>,
    }
    let mut acc: BTreeMap<(String, String), NodeNs> = BTreeMap::new();

    for (key, value) in ts.agent().kv().scan_prefix("sys/tuple/") {
        let rest = &key["sys/tuple/".len()..];
        let mut parts = rest.splitn(3, '/');
        let (Some(node), Some(ns), Some(tail)) =
            (parts.next(), parts.next(), parts.next())
        else {
            continue;
        };
        let entry = acc.entry((node.to_string(), ns.to_string())).or_default();
        let text = String::from_utf8_lossy(&value).to_string();
        if tail == "role" {
            entry.role = Some(text);
        } else if tail == "wal_bytes" {
            entry.wal_bytes = text.parse().ok();
        } else if let Some(p) = tail.strip_prefix("pressure/") {
            entry.pressure.push(p.to_string());
        } else if let Some(rest) = tail.strip_prefix("stage/")
            && let Some((stage, metric)) = rest.rsplit_once('/')
        {
            let v = text.parse::<u64>().map_or(Value::String(text), Value::from);
            entry
                .stages
                .entry(stage.to_string())
                .or_default()
                .insert(metric.to_string(), v);
        }
    }

    let nodes: Vec<Value> = acc
        .into_iter()
        .map(|((node, ns), e)| {
            let stages: Vec<Value> = e
                .stages
                .into_iter()
                .map(|(stage, metrics)| {
                    let mut obj = serde_json::Map::new();
                    obj.insert("stage".into(), Value::from(stage));
                    obj.extend(metrics);
                    Value::Object(obj)
                })
                .collect();
            json!({
                "node_id": node,
                "ns": ns,
                "role": e.role,
                "wal_bytes": e.wal_bytes,
                "stages": stages,
                "pressure": e.pressure,
            })
        })
        .collect();
    Json(json!({ "nodes": nodes })).into_response()
}
