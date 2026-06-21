//! HTTP gateway endpoints (WS-G / G3 · Phase 4) — `/gateway/bb/*`.
//!
//! Registered onto the Mycelium embedded gateway via
//! [`GossipAgent::with_http_routes`](mycelium::GossipAgent::with_http_routes). Base64/JSON is the
//! edge boundary; the intra-cluster RPC path stays compact binary. Mirrors what the Python/TS SDKs
//! send over the wire.

use std::collections::BTreeMap;
use std::sync::Arc;

use axum::Json;
use axum::extract::{Query, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use base64::Engine as _;
use bytes::Bytes;
use serde::Deserialize;
use serde_json::json;

use crate::{Blackboard, BlackboardError, Fact, Predicate};

const B64: base64::engine::GeneralPurpose = base64::engine::general_purpose::STANDARD;

impl Blackboard {
    /// An axum `Router` with the board gateway endpoints, ready for `GossipAgent::with_http_routes`.
    pub fn http_router(self: Arc<Self>) -> axum::Router {
        axum::Router::new()
            .route("/gateway/bb/post", post(gw_post))
            .route("/gateway/bb/read", post(gw_read))
            .route("/gateway/bb/claim", post(gw_claim))
            .route("/gateway/bb/ack", post(gw_ack))
            .route("/gateway/bb/release", post(gw_release))
            .route("/gateway/bb/depth", get(gw_depth))
            .with_state(self)
    }
}

fn err_response(e: &BlackboardError) -> Response {
    let (status, mut headers) = match e {
        BlackboardError::NoProvider => {
            let mut h = HeaderMap::new();
            h.insert("Retry-After", HeaderValue::from_static("1"));
            (StatusCode::SERVICE_UNAVAILABLE, h)
        }
        BlackboardError::NotFound => (StatusCode::NOT_FOUND, HeaderMap::new()),
        _ => (StatusCode::BAD_GATEWAY, HeaderMap::new()),
    };
    headers.insert("Content-Type", HeaderValue::from_static("application/json"));
    (status, headers, json!({ "error": e.to_string() }).to_string()).into_response()
}

fn bad_request(msg: &str) -> Response {
    (StatusCode::BAD_REQUEST, Json(json!({ "error": msg }))).into_response()
}

fn ns_mismatch(bb: &Blackboard, ns: &Option<String>) -> bool {
    ns.as_ref().is_some_and(|n| n.as_str() != bb.namespace().as_ref())
}

fn fact_json(f: &Fact) -> serde_json::Value {
    json!({ "id": f.id, "attributes": f.attributes, "payload_b64": B64.encode(&f.payload) })
}

/// Build a [`Predicate`] from the JSON `eq` map + `present` list.
fn build_predicate(eq: &BTreeMap<String, String>, present: &[String]) -> Predicate {
    let mut p = Predicate::new();
    for (k, v) in eq {
        p = p.eq(k.clone(), v.clone());
    }
    for k in present {
        p = p.present(k.clone());
    }
    p
}

#[derive(Deserialize)]
struct PostBody {
    ns: Option<String>,
    #[serde(default)]
    attributes: BTreeMap<String, String>,
    payload_b64: String,
}

async fn gw_post(State(bb): State<Arc<Blackboard>>, Json(b): Json<PostBody>) -> Response {
    if ns_mismatch(&bb, &b.ns) {
        return bad_request("unknown namespace");
    }
    let Ok(payload) = B64.decode(&b.payload_b64) else {
        return bad_request("payload_b64 is not valid base64");
    };
    match bb.post(b.attributes, Bytes::from(payload)).await {
        Ok(id) => Json(json!({ "id": id })).into_response(),
        Err(e) => err_response(&e),
    }
}

#[derive(Deserialize)]
struct PredicateBody {
    ns: Option<String>,
    #[serde(default)]
    eq: BTreeMap<String, String>,
    #[serde(default)]
    present: Vec<String>,
}

async fn gw_read(State(bb): State<Arc<Blackboard>>, Json(b): Json<PredicateBody>) -> Response {
    if ns_mismatch(&bb, &b.ns) {
        return bad_request("unknown namespace");
    }
    let pred = build_predicate(&b.eq, &b.present);
    match bb.read(&pred).await {
        Ok(facts) => {
            let facts: Vec<_> = facts.iter().map(fact_json).collect();
            Json(json!({ "facts": facts })).into_response()
        }
        Err(e) => err_response(&e),
    }
}

async fn gw_claim(State(bb): State<Arc<Blackboard>>, Json(b): Json<PredicateBody>) -> Response {
    if ns_mismatch(&bb, &b.ns) {
        return bad_request("unknown namespace");
    }
    let pred = build_predicate(&b.eq, &b.present);
    match bb.claim(&pred).await {
        Ok(Some(f)) => Json(json!({ "claimed": true, "fact": fact_json(&f) })).into_response(),
        Ok(None) => Json(json!({ "claimed": false })).into_response(),
        Err(e) => err_response(&e),
    }
}

#[derive(Deserialize)]
struct IdBody {
    ns: Option<String>,
    id: u64,
}

async fn gw_ack(State(bb): State<Arc<Blackboard>>, Json(b): Json<IdBody>) -> Response {
    if ns_mismatch(&bb, &b.ns) {
        return bad_request("unknown namespace");
    }
    match bb.ack(b.id).await {
        Ok(()) => Json(json!({ "ok": true })).into_response(),
        Err(e) => err_response(&e),
    }
}

async fn gw_release(State(bb): State<Arc<Blackboard>>, Json(b): Json<IdBody>) -> Response {
    if ns_mismatch(&bb, &b.ns) {
        return bad_request("unknown namespace");
    }
    match bb.release(b.id).await {
        Ok(()) => Json(json!({ "ok": true })).into_response(),
        Err(e) => err_response(&e),
    }
}

#[derive(Deserialize)]
struct DepthQuery {
    ns: Option<String>,
}

async fn gw_depth(State(bb): State<Arc<Blackboard>>, Query(q): Query<DepthQuery>) -> Response {
    if ns_mismatch(&bb, &q.ns) {
        return bad_request("unknown namespace");
    }
    match bb.depth().await {
        Ok(d) => Json(json!({ "available": d.available, "inflight": d.inflight })).into_response(),
        Err(e) => err_response(&e),
    }
}
