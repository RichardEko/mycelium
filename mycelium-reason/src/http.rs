//! HTTP gateway endpoints (feature `gateway`) — `/gateway/reason/*`.
//!
//! Registered onto the Mycelium embedded gateway via
//! [`GossipAgent::with_http_routes`](mycelium::GossipAgent::with_http_routes) (routers
//! merge; `/gateway/…` routes pass the gateway auth middleware). This is the boundary
//! the Python LangGraph checkpointer speaks: blob PUT/GET carry **raw bytes** (checkpoint
//! payloads — no base64 inflation), the trace endpoint returns JSON events + narrative.

use std::sync::Arc;
use std::time::Duration;

use axum::Json;
use axum::extract::{DefaultBodyLimit, Path, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, put};
use serde_json::json;

use mycelium::GossipAgent;

use crate::blob::{BlobId, FsBlobStore, MAX_BLOB_BYTES, MeshBlobStore};
use crate::trace::{narrate, replay};

/// Shared route state: the agent (trace replay) + the local-first/mesh-fallback store.
#[derive(Clone)]
struct ReasonState {
    agent: Arc<GossipAgent>,
    blobs: MeshBlobStore,
}

/// An axum `Router` with the reason gateway endpoints, ready for
/// `GossipAgent::with_http_routes`. Mesh blob fetches (a GET whose id is not local)
/// use a 10 s per-provider timeout.
pub fn reason_router(agent: Arc<GossipAgent>, store: Arc<FsBlobStore>) -> axum::Router {
    let state = ReasonState {
        blobs: MeshBlobStore::new(Arc::clone(&agent), store, Duration::from_secs(10)),
        agent,
    };
    axum::Router::new()
        .route(
            "/gateway/reason/blob",
            // Axum's default body cap (2 MiB) is under the blob ceiling; lift it to the
            // ceiling + 1 KiB slack so *our* 413 fires with the JSON error body.
            put(gw_blob_put).layer(DefaultBodyLimit::max(MAX_BLOB_BYTES + 1024)),
        )
        .route("/gateway/reason/blob/{id}", get(gw_blob_get))
        .route("/gateway/reason/trace/{run_id}", get(gw_trace_get))
        .with_state(state)
}

fn error_json(status: StatusCode, error: &str) -> Response {
    (status, Json(json!({ "error": error }))).into_response()
}

/// `PUT /gateway/reason/blob` — raw body in, `{"id":"<hex>"}` out. 413 over the ceiling.
async fn gw_blob_put(State(s): State<ReasonState>, body: bytes::Bytes) -> Response {
    if body.len() > MAX_BLOB_BYTES {
        return error_json(StatusCode::PAYLOAD_TOO_LARGE, "too_large");
    }
    match s.blobs.put(&body) {
        Ok(id) => Json(json!({ "id": id.to_hex() })).into_response(),
        Err(e) => {
            tracing::warn!(error = %e, "gateway blob put failed");
            error_json(StatusCode::INTERNAL_SERVER_ERROR, "store_error")
        }
    }
}

/// `GET /gateway/reason/blob/{id}` — local-then-mesh; body = verified blob bytes.
async fn gw_blob_get(State(s): State<ReasonState>, Path(id): Path<String>) -> Response {
    let Some(id) = BlobId::from_hex(&id) else {
        return error_json(StatusCode::BAD_REQUEST, "bad_id");
    };
    match s.blobs.get(&id).await {
        Some(bytes) => (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "application/octet-stream")],
            bytes,
        )
            .into_response(),
        None => error_json(StatusCode::NOT_FOUND, "not_found"),
    }
}

/// `GET /gateway/reason/trace/{run_id}` — the replayed run + its narrative, from this
/// node's KV view (gossip-replicated: any node can serve any run's trace).
async fn gw_trace_get(State(s): State<ReasonState>, Path(run_id): Path<String>) -> Response {
    let events = replay(&s.agent, &run_id);
    let narrative = narrate(&events);
    let events_json: Vec<_> = events
        .iter()
        .map(|e| json!({ "hlc": e.hlc, "node": e.node, "kind": e.kind, "detail": e.detail }))
        .collect();
    Json(json!({ "run_id": run_id, "events": events_json, "narrative": narrative })).into_response()
}
