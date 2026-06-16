//! Embedded HTTP server — Layer 3 + Layer 4 gateway.
//!
//! ## Library-level endpoints
//! - `GET  /health`                — liveness probe
//! - `GET  /ready`                 — readiness probe (caps advertised + no dead shards)
//! - `GET  /stats`                 — KV store metrics (node_id, store_entries, dropped_frames, task_count)
//! - `GET  /consensus/{slot}`      — inspect committed value + ballot for a consensus slot
//! - `GET  /signals/{kind}`        — SSE stream of admitted signals
//! - `POST /mcp`                   — JSON-RPC 2.0 MCP protocol bridge
//!
//! ## Language-bridge gateway endpoints (`/gateway/*`)
//! These endpoints let Python/TypeScript agents participate in the mesh
//! without a Rust dependency. The gateway is the HTTP sidecar described in
//! the Layer 4 architecture.
//!
//! - `POST   /gateway/capability/advertise`    — advertise a capability; returns handle_id
//! - `DELETE /gateway/capability/{handle_id}`  — retract (tombstone) a capability
//! - `GET    /gateway/capability/resolve`      — filter-match with optional caller_id scoping
//! - `POST   /gateway/signal/emit`             — fire a signal into the mesh
//! - `GET    /gateway/signal/sse/{kind}`       — SSE stream for a signal kind
//! - `GET    /gateway/demand`                  — demand pressure for a capability filter
//! - `POST   /gateway/rpc/call`               — blocking RPC call to a named node
//! - `GET    /gateway/rpc/serve/{kind}`        — SSE stream of incoming RPC requests
//! - `POST   /gateway/rpc/respond`             — send reply to an in-flight RPC request
//! - `POST   /gateway/scatter`                 — scatter-gather RPC to multiple targets
//! - `GET    /gateway/kv?key=K`                — read a KV key
//! - `POST   /gateway/kv`                      — write a KV key
//! - `DELETE /gateway/kv?key=K`                — delete (tombstone) a KV key
//! - `GET    /gateway/kv/keys?prefix=P`        — list live keys (optionally filtered)
//! - `POST   /gateway/kv/quorum`               — write + wait for N peer ACKs
//! - `GET    /gateway/mailbox/{kind}`          — SSE stream of mailbox events for this node
//! - `POST   /gateway/mailbox/deliver`         — deliver an event to a target's mailbox
//! - `GET    /gateway/shard/{ns}/{name}?key=K` — deterministic shard owner for a key
//! - `POST   /gateway/shard/emit`              — emit signal to consistent-hash owner
//! - `POST   /gateway/consensus/cross_group_propose` — multi-group independent-quorum proposal
//!
//! Started when `GossipConfig::http_port` is `Some(port)`. Shuts down cleanly
//! when the agent's broadcast shutdown signal fires.

use axum::{
    Router,
    extract::{Path, Query, Request, State},
    http::{StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Json, Response, Sse},
    response::sse::{Event, KeepAlive},
    routing::{delete, get, post},
};
#[cfg(feature = "compliance")]
use axum::extract::MatchedPath;
use bytes::{BufMut, Bytes, BytesMut};
use serde::Deserialize;
use serde_json::json;
use std::{
    collections::HashMap,
    convert::Infallible,
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::{oneshot, watch};
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt as _;
use tracing::info;

use super::kv_handle::LogEntry;
#[cfg(feature = "consensus")]
use super::kv_handle::SubscribeHandle;
#[cfg(feature = "consensus")]
use super::overlay_consistent::LockGuard;

use super::TaskCtx;

/// Shared state passed to every HTTP handler.
struct HttpCtx {
    agent_ctx:       Arc<TaskCtx>,
    /// Capability handle table for the language gateway.
    /// Key: opaque handle_id string returned to the caller.
    /// Value: cancel sender — dropping it tombstones the capability.
    gateway_caps:    Arc<Mutex<HashMap<String, oneshot::Sender<()>>>>,
    /// Distributed lock guards held on behalf of HTTP clients.
    /// Key: opaque guard_id returned to the caller.
    /// Drop-on-remove tombstones `lock/{name}` in the gossip KV.
    #[cfg(feature = "consensus")]
    lock_guards:     Arc<Mutex<HashMap<String, LockGuard>>>,
    /// Shutdown receiver used when spawning gateway advertisement tasks.
    shutdown_rx:     watch::Receiver<bool>,
    /// Prometheus scrape handle (only present when the `metrics` feature is enabled).
    #[cfg(feature = "metrics")]
    prometheus:      metrics_exporter_prometheus::PrometheusHandle,
    /// OIDC verifier (WS4) — present when `GossipConfig::oidc` is set. Validates
    /// JWT bearers against the IdP JWKS and maps groups to gateway scopes.
    #[cfg(feature = "compliance")]
    oidc:            Option<Arc<super::oidc::OidcVerifier>>,
}

/// Returns the process-wide Prometheus scrape handle, installing the recorder
/// the first time it is called. Safe to call from multiple agents in the same
/// process (e.g. in tests) — subsequent calls return a clone of the same handle.
#[cfg(feature = "metrics")]
fn prometheus_handle() -> metrics_exporter_prometheus::PrometheusHandle {
    use std::sync::OnceLock;
    static HANDLE: OnceLock<metrics_exporter_prometheus::PrometheusHandle> = OnceLock::new();
    HANDLE.get_or_init(|| {
        metrics_exporter_prometheus::PrometheusBuilder::new()
            .install_recorder()
            .expect("Prometheus recorder install failed")
    }).clone()
}

/// Starts the axum HTTP server on `addr`. Returns when the agent shuts down
/// (shutdown_rx fires) or if the listener fails to bind.
///
/// `extra_routes` is an optional `Router<()>` (state already attached by the
/// caller) that is merged into the library router so application-level
/// handlers share the same port without a second TCP listener.
pub(super) async fn run_http_server(
    addr:         SocketAddr,
    ctx:          Arc<TaskCtx>,
    shutdown_rx:  watch::Receiver<bool>,
    extra_routes: Option<axum::Router>,
) -> Result<(), std::io::Error> {
    #[cfg(feature = "metrics")]
    let prometheus = prometheus_handle();

    #[cfg(feature = "compliance")]
    let oidc = ctx
        .config
        .oidc
        .clone()
        .map(|c| Arc::new(super::oidc::OidcVerifier::new(c)));

    let state = Arc::new(HttpCtx {
        agent_ctx:    ctx,
        gateway_caps: Arc::new(Mutex::new(HashMap::new())),
        #[cfg(feature = "consensus")]
        lock_guards:  Arc::new(Mutex::new(HashMap::new())),
        shutdown_rx:  shutdown_rx.clone(),
        #[cfg(feature = "metrics")]
        prometheus,
        #[cfg(feature = "compliance")]
        oidc,
    });

    // ── Language-bridge gateway routes (optionally auth-protected) ────────────
    // Nested under /gateway so the auth middleware applies to all of them while
    // leaving /health, /ready, /stats, /metrics, /signals, and /mcp public.
    // route_layer is applied once at the end so all routes (including
    // cfg-gated llm routes) are covered by a single middleware instance.
    let gateway = Router::new()
        .route("/capability/advertise",   post(gw_cap_advertise))
        .route("/capability/{handle_id}", delete(gw_cap_drop))
        .route("/capability/resolve",     get(gw_cap_resolve))
        .route("/signal/emit",            post(gw_signal_emit))
        .route("/signal/sse/{kind}",      get(gw_signal_sse))
        .route("/demand",                 get(gw_demand))
        .route("/rpc/call",               post(gw_rpc_call))
        .route("/rpc/serve/{kind}",       get(gw_rpc_serve))
        .route("/rpc/respond",            post(gw_rpc_respond))
        .route("/scatter",                post(gw_scatter))
        .route("/kv",                     get(gw_kv_get).post(gw_kv_set).delete(gw_kv_delete))
        .route("/kv/keys",                get(gw_kv_keys))
        .route("/kv/quorum",              post(gw_kv_quorum))
        .route("/mailbox/deliver",        post(gw_mailbox_deliver))
        .route("/mailbox/{kind}",         get(gw_mailbox_subscribe))
        // ── Overlay: ordered log ──────────────────────────────────────────
        .route("/overlay/log/append",             post(gw_overlay_log_append))
        .route("/overlay/log/scan",               get(gw_overlay_log_scan))
        .route("/overlay/log/compact",            post(gw_overlay_log_compact))
        .route("/overlay/log/subscribe",          get(gw_overlay_log_subscribe))
        // ── Overlay: reliable delivery ────────────────────────────────────
        .route("/overlay/emit_reliable",          post(gw_overlay_emit_reliable))
        // ── Cluster sharding ──────────────────────────────────────────────
        .route("/shard/{ns}/{name}",               get(gw_shard_owner))
        .route("/shard/emit",                     post(gw_shard_emit));

    // ── Consensus + the consistency/lock/election overlays built on it ────────
    // (v2 M2 feature gate). The ordered-log and reliable-delivery overlays above
    // are KV/anti-entropy based, not consensus, so they stay unconditional.
    #[cfg(feature = "consensus")]
    let gateway = gateway
        .route("/overlay/consistent/set",         post(gw_overlay_consistent_set))
        .route("/overlay/consistent/get",         get(gw_overlay_consistent_get))
        .route("/overlay/lock/acquire",           post(gw_overlay_lock_acquire))
        .route("/overlay/lock/{guard_id}",         delete(gw_overlay_lock_release))
        .route("/overlay/elect",                  post(gw_overlay_elect))
        // log/group/subscribe uses the distributed-lock claim (consensus overlay)
        .route("/overlay/log/group/subscribe",    get(gw_overlay_log_group_subscribe))
        .route("/consensus/cross_group_propose",  post(gw_cross_group_propose));

    #[cfg(feature = "llm")]
    let gateway = gateway
        .route("/prompts",             get(gw_prompts_list))
        .route("/prompts/{ns}/{name}", get(gw_prompt_get).put(gw_prompt_put).delete(gw_prompt_delete))
        .route("/llm/call",            post(gw_llm_call))
        .route("/llm/stream",          post(gw_llm_stream));

    // WS2 audit trail query + verification (compliance feature).
    #[cfg(feature = "compliance")]
    let gateway = gateway.route("/audit", get(gw_audit));

    // Apply auth middleware to all gateway routes in one shot.
    let gateway = gateway
        .route_layer(middleware::from_fn_with_state(Arc::clone(&state), gateway_auth));

    // ── Main router ───────────────────────────────────────────────────────────
    let app = Router::new()
        // Library endpoints — always public
        .route("/health",               get(health_handler))
        .route("/ready",                get(ready_handler))
        .route("/stats",                get(stats_handler))
        .route("/metrics",              get(metrics_handler))
        .route("/signals/{kind}",       get(signal_sse_handler))
        .route("/mcp",                  post(mcp_handler))
        .route("/bulk/{corr_id}",       get(bulk_staging_handler))
        // Gateway — auth-protected when gateway_auth_token is set
        .nest("/gateway", gateway);

    // Consensus slot inspection — public, but consensus-gated (v2 M2).
    #[cfg(feature = "consensus")]
    let app = app.route("/consensus/{slot}", get(consensus_slot_handler));

    let app = app.with_state(state);

    let app = if let Some(extra) = extra_routes {
        app.merge(extra)
    } else {
        app
    };

    let listener = tokio::net::TcpListener::bind(addr).await?;
    info!(addr = %listener.local_addr().unwrap(), "HTTP server listening");

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal(shutdown_rx))
        .await
        .map_err(|e| std::io::Error::other(e.to_string()))
}

async fn shutdown_signal(mut rx: watch::Receiver<bool>) {
    let _ = rx.wait_for(|v| *v).await;
}

/// Axum middleware applied to every `/gateway/**` route.
///
/// Two layers, the second feature-gated:
///
/// 1. **Authentication** (always): when `gateway_auth_token` is set, or
///    (compliance) any `gateway_scoped_tokens` are configured, every gateway
///    request must carry a valid `Authorization: Bearer <token>`. With neither
///    set the gateway is open (loopback-only deployments). `/health`, `/ready`,
///    `/stats`, `/metrics`, and the descriptor path are NOT under `/gateway`
///    and stay public regardless.
///
/// 2. **OAuth2 scope authorization** (`compliance` feature): the presented
///    token resolves to a scope grant — `gateway_auth_token` ⇒ the `"*"`
///    wildcard (full access, unchanged behaviour), or a `gateway_scoped_tokens`
///    entry ⇒ its scopes. The matched route requires a `resource:verb` scope
///    ([`required_scope`]); the request is admitted only if the grant holds it
///    or `"*"`. Deny-by-default: an unmapped gateway route requires `admin`.
async fn gateway_auth(
    State(ctx): State<Arc<HttpCtx>>,
    request: Request,
    next: Next,
) -> Response {
    let cfg = &ctx.agent_ctx.config;
    let legacy = cfg.gateway_auth_token.as_deref();

    #[cfg(feature = "compliance")]
    let have_scoped = !cfg.gateway_scoped_tokens.is_empty();
    #[cfg(not(feature = "compliance"))]
    let have_scoped = false;

    #[cfg(feature = "compliance")]
    let have_oidc = ctx.oidc.is_some();
    #[cfg(not(feature = "compliance"))]
    let have_oidc = false;

    // Open gateway: no token model and no OIDC configured.
    if legacy.is_none() && !have_scoped && !have_oidc {
        return next.run(request).await;
    }

    let presented = request.headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "));

    let Some(presented) = presented else {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": "authentication required"})),
        ).into_response();
    };

    // Resolve scopes: try OIDC first (a JWT bearer from the IdP → groups → scopes),
    // then fall back to the static token table. A JWT that fails OIDC validation
    // won't match a static token either, so it correctly ends in 401.
    #[cfg(feature = "compliance")]
    let resolved: Option<Vec<String>> = {
        let mut s = None;
        if let Some(verifier) = &ctx.oidc
            && let Some(principal) = verifier.verify(presented).await
        {
            s = Some(principal.scopes);
        }
        s.or_else(|| resolve_token_scopes(cfg, presented))
    };
    #[cfg(not(feature = "compliance"))]
    let resolved: Option<Vec<String>> = resolve_token_scopes(cfg, presented);

    let Some(scopes) = resolved else {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": "authentication required"})),
        ).into_response();
    };

    #[cfg(feature = "compliance")]
    {
        let required = request
            .extensions()
            .get::<MatchedPath>()
            .map(|m| required_scope(request.method(), m.as_str()))
            .unwrap_or("admin");
        if !scope_admits(&scopes, required) {
            return (
                StatusCode::FORBIDDEN,
                Json(json!({"error": "insufficient scope", "required_scope": required})),
            ).into_response();
        }
    }
    // Without compliance the token is authenticated but not scope-gated.
    #[cfg(not(feature = "compliance"))]
    let _ = &scopes;

    next.run(request).await
}

/// Map a presented bearer token to its scope grant, or `None` if unrecognised.
///
/// The legacy `gateway_auth_token` is the superuser case: it grants `["*"]`, so
/// deployments that only set it behave exactly as before. Scoped tokens are only
/// consulted under the `compliance` feature.
fn resolve_token_scopes(cfg: &crate::config::GossipConfig, presented: &str) -> Option<Vec<String>> {
    if let Some(legacy) = cfg.gateway_auth_token.as_deref()
        && presented == legacy
    {
        return Some(vec!["*".to_string()]);
    }
    #[cfg(feature = "compliance")]
    {
        for t in &cfg.gateway_scoped_tokens {
            if t.token == presented {
                return Some(t.scopes.clone());
            }
        }
    }
    None
}

/// True if `scopes` grants `required` (exact match or the `"*"` wildcard).
#[cfg(feature = "compliance")]
fn scope_admits(scopes: &[String], required: &str) -> bool {
    scopes.iter().any(|s| s == "*" || s == required)
}

/// The OAuth2 scope a gateway route requires, keyed on its matched-path pattern
/// and method. This is the gateway ACL policy table; deny-by-default — any route
/// not listed requires `admin`. Scopes are coarse `resource:verb` families so the
/// vocabulary stays small (`kv`, `cap`, `mesh`, `consensus`, `llm` × `read`/`write`,
/// plus `llm:invoke`).
#[cfg(feature = "compliance")]
fn required_scope(method: &axum::http::Method, matched_path: &str) -> &'static str {
    use axum::http::Method;
    let read = method == Method::GET;
    match matched_path {
        // KV
        "/gateway/kv"          => if read { "kv:read" } else { "kv:write" },
        "/gateway/kv/keys"     => "kv:read",
        "/gateway/kv/quorum"   => "kv:write",
        // Capabilities
        "/gateway/capability/advertise"   => "cap:write",
        "/gateway/capability/{handle_id}" => "cap:write",
        "/gateway/capability/resolve"     => "cap:read",
        "/gateway/shard/{ns}/{name}"      => "cap:read",
        // Layer II mesh messaging
        "/gateway/signal/emit"     => "mesh:write",
        "/gateway/signal/sse/{kind}" => "mesh:read",
        "/gateway/demand"          => "mesh:read",
        "/gateway/rpc/call"        => "mesh:write",
        "/gateway/rpc/serve/{kind}" => "mesh:read",
        "/gateway/rpc/respond"     => "mesh:write",
        "/gateway/scatter"         => "mesh:write",
        "/gateway/mailbox/deliver" => "mesh:write",
        "/gateway/mailbox/{kind}"  => "mesh:read",
        "/gateway/shard/emit"      => "mesh:write",
        // Layer III consensus / consistency overlay
        "/gateway/overlay/consistent/set"      => "consensus:write",
        "/gateway/overlay/consistent/get"      => "consensus:read",
        "/gateway/overlay/lock/acquire"        => "consensus:write",
        "/gateway/overlay/lock/{guard_id}"     => "consensus:write",
        "/gateway/overlay/elect"               => "consensus:write",
        "/gateway/overlay/log/append"          => "consensus:write",
        "/gateway/overlay/log/scan"            => "consensus:read",
        "/gateway/overlay/log/compact"         => "consensus:write",
        "/gateway/overlay/log/subscribe"       => "consensus:read",
        "/gateway/overlay/log/group/subscribe" => "consensus:read",
        "/gateway/overlay/emit_reliable"       => "consensus:write",
        "/gateway/consensus/cross_group_propose" => "consensus:write",
        // LLM / prompt skills
        "/gateway/prompts"             => "llm:read",
        "/gateway/prompts/{ns}/{name}" => if read { "llm:read" } else { "llm:write" },
        "/gateway/llm/call"            => "llm:invoke",
        "/gateway/llm/stream"          => "llm:invoke",
        // Audit trail (WS2)
        "/gateway/audit"               => "audit:read",
        // Deny-by-default.
        _ => "admin",
    }
}

// ── Handlers ─────────────────────────────────────────────────────────────────

/// `GET /metrics` — Prometheus text-format scrape endpoint.
///
/// Available when the `metrics` cargo feature is enabled. Returns
/// `text/plain; version=0.0.4` as expected by Prometheus scrapers.
/// When the feature is disabled, returns 404.
async fn metrics_handler(State(ctx): State<Arc<HttpCtx>>) -> impl IntoResponse {
    #[cfg(feature = "metrics")]
    {
        let body = ctx.prometheus.render();
        (
            StatusCode::OK,
            [(axum::http::header::CONTENT_TYPE, "text/plain; version=0.0.4")],
            body,
        ).into_response()
    }
    #[cfg(not(feature = "metrics"))]
    {
        let _ = ctx;
        (StatusCode::NOT_FOUND, "metrics feature not enabled").into_response()
    }
}

async fn health_handler(State(ctx): State<Arc<HttpCtx>>) -> impl IntoResponse {
    Json(json!({
        "status":  "ok",
        "node_id": ctx.agent_ctx.node_id.to_string(),
    }))
}

/// Readiness probe: returns 200 once soft-state keys (capabilities, locality)
/// have been written to the local store after startup or restart.
/// Returns 503 while WAL replay is still pending or the first advertisement
/// tick has not yet fired.
///
/// Use `/health` for liveness; use `/ready` before sending traffic that
/// depends on accurate capability or membership state.
async fn ready_handler(State(ctx): State<Arc<HttpCtx>>) -> impl IntoResponse {
    if ctx.agent_ctx.soft_state_advertised.load(std::sync::atomic::Ordering::Acquire) {
        (StatusCode::OK, Json(json!({ "status": "ready", "node_id": ctx.agent_ctx.node_id.to_string() }))).into_response()
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, Json(json!({ "status": "starting", "node_id": ctx.agent_ctx.node_id.to_string() }))).into_response()
    }
}

/// `GET /bulk/{corr_id}`
///
/// Serves a staged bulk-call payload by nonce (hex-encoded 16-char string).
/// Used by the `bulk_serve` target to fetch the caller's staged data over HTTP.
/// Returns 200 + raw bytes on hit, 404 when the nonce is not found.
async fn bulk_staging_handler(
    Path(corr_id): Path<String>,
    State(ctx):    State<Arc<HttpCtx>>,
) -> impl IntoResponse {
    let nonce = match u64::from_str_radix(corr_id.trim_start_matches("0x"), 16) {
        Ok(n)  => n,
        Err(_) => return StatusCode::BAD_REQUEST.into_response(),
    };
    match ctx.agent_ctx.bulk_transport.get(nonce) {
        Some(bytes) => (StatusCode::OK, bytes.to_vec()).into_response(),
        None        => StatusCode::NOT_FOUND.into_response(),
    }
}

/// `GET /consensus/{slot}` — inspect the committed value and current ballot for a slot.
///
/// Returns `{"slot": "…", "committed": "<base64>" | null, "ballot": <u64>,
/// "lease_ms": <u64> | null, "lease_expired": <bool>}`.
/// `committed` is the **live** value: `null` when nothing has been committed
/// yet *or* when an epoch lease has expired (the slot has reopened —
/// `lease_expired: true` distinguishes the two). `ballot` reflects the highest
/// ballot number seen for that slot (0 = never proposed).
///
/// This endpoint is public (no auth) and is intended for operational debugging.
#[cfg(feature = "consensus")]
async fn consensus_slot_handler(
    Path(slot):   Path<String>,
    State(ctx):   State<Arc<HttpCtx>>,
) -> impl IntoResponse {
    use base64::Engine as _;
    let committed_key = format!("{}{}", crate::consensus::consensus_ns::COMMITTED, slot);
    let lease_key     = format!("{}{}", crate::consensus::consensus_ns::LEASE,     slot);
    let ballot_key    = format!("{}{}", crate::consensus::consensus_ns::BALLOT,    slot);
    let live = crate::consensus::live_committed_value(
        &ctx.agent_ctx.kv_state, &slot, crate::consensus::wall_now_ms(),
    );
    let store = ctx.agent_ctx.kv_state.store.pin();
    let raw_present = store.get(committed_key.as_str())
        .and_then(|e| e.data.as_ref())
        .is_some();
    let committed_b64 = live
        .map(|b| base64::engine::general_purpose::STANDARD.encode(&b));
    let lease_expired = raw_present && committed_b64.is_none();
    let lease_ms = store.get(lease_key.as_str())
        .and_then(|e| e.data.clone())
        .and_then(|b| crate::consensus::decode_lease_ms(&b));
    let ballot: u64 = store.get(ballot_key.as_str())
        .and_then(|e| e.data.clone())
        .map(|b| crate::consensus::decode_ballot(&b))
        .unwrap_or(0);
    Json(json!({
        "slot":          slot,
        "committed":     committed_b64,
        "ballot":        ballot,
        "lease_ms":      lease_ms,
        "lease_expired": lease_expired,
    }))
}

async fn stats_handler(State(ctx): State<Arc<HttpCtx>>) -> impl IntoResponse {
    let kv = &ctx.agent_ctx.kv_state;
    let task_count = ctx.agent_ctx.task_handles
        .lock().unwrap_or_else(|e| e.into_inner())
        .len();
    Json(json!({
        "node_id":       ctx.agent_ctx.node_id.to_string(),
        "store_entries": kv.store.pin().len(),
        "dropped_frames": kv.dropped_frames.load(std::sync::atomic::Ordering::Relaxed),
        "individual_flood_fallbacks": kv.individual_flood_fallbacks.load(std::sync::atomic::Ordering::Relaxed),
        "task_count":    task_count,
        "commit_conflicts": ctx.agent_ctx.commit_conflicts
            .load(std::sync::atomic::Ordering::Relaxed),
        "sys_namespace_violations": ctx.agent_ctx.sys_namespace_violations
            .load(std::sync::atomic::Ordering::Relaxed),
    }))
}

/// `GET /gateway/audit` — query the tamper-evident audit trail (compliance, scope
/// `audit:read`). Optional `?node=` selects one stream (default: all known
/// streams); optional `?limit=` caps the records returned per stream (most
/// recent), while verification always runs over the full stream. Each stream
/// reports `verified` + any `verify_error`, the chain-tip `head_hash`, and each
/// record's stable `content_hash` (the M16-citable identifier).
#[cfg(feature = "compliance")]
#[derive(Deserialize)]
struct AuditQuery {
    node:  Option<String>,
    limit: Option<usize>,
}

#[cfg(feature = "compliance")]
fn hex32(bytes: &[u8; 32]) -> String {
    use std::fmt::Write;
    let mut s = String::with_capacity(64);
    for b in bytes {
        let _ = write!(s, "{b:02x}");
    }
    s
}

#[cfg(feature = "compliance")]
async fn gw_audit(
    State(ctx): State<Arc<HttpCtx>>,
    Query(q): Query<AuditQuery>,
) -> Response {
    let tc = &ctx.agent_ctx;
    let nodes: Vec<crate::node_id::NodeId> = match q.node.as_deref() {
        Some(s) => match s.parse() {
            Ok(n) => vec![n],
            Err(_) => {
                return (StatusCode::BAD_REQUEST, Json(json!({"error": "invalid node id"})))
                    .into_response();
            }
        },
        None => super::audit::stream_nodes(tc),
    };
    let limit = q.limit.unwrap_or(usize::MAX);

    let mut streams = Vec::with_capacity(nodes.len());
    for node in nodes {
        let records = super::audit::read_stream(tc, &node);
        let verify  = super::audit::verify_stream(tc, &node);
        let head_hash = records.last().map(|sr| hex32(&sr.record.content_hash()));
        let shown: Vec<_> = records
            .iter()
            .rev()
            .take(limit)
            .rev()
            .map(|sr| {
                let r = &sr.record;
                json!({
                    "seq":          r.seq,
                    "hlc":          r.hlc,
                    "principal":    r.principal,
                    "action":       format!("{:?}", r.action),
                    "target":       r.target,
                    "outcome":      format!("{:?}", r.outcome),
                    "detail":       r.detail,
                    "content_hash": hex32(&r.content_hash()),
                })
            })
            .collect();
        streams.push(json!({
            "node":         node.to_string(),
            "count":        records.len(),
            "verified":     verify.is_ok(),
            "verify_error": verify.err().map(|e| format!("{e:?}")),
            "head_hash":    head_hash,
            "records":      shown,
        }));
    }
    Json(json!({ "streams": streams })).into_response()
}

/// SSE endpoint — streams admitted signals of the requested `kind`.
///
/// Each event carries:
/// - `event` field: the signal kind
/// - `data` field: JSON `{"sender":"<node_id>","payload":"<base64>"}`
///
/// The subscription is torn down automatically when the client disconnects.
async fn signal_sse_handler(
    Path(kind):  Path<String>,
    State(ctx):  State<Arc<HttpCtx>>,
) -> Sse<impl futures_util::Stream<Item = Result<Event, Infallible>>> {
    let rx = ctx.agent_ctx.signal_handlers.register_with_capacity(
        std::sync::Arc::from(kind.as_str()),
        256,
    );

    let stream = ReceiverStream::new(rx).map(|sig: crate::signal::Signal| {
        use base64::Engine as _;
        let payload_b64 = base64::engine::general_purpose::STANDARD.encode(&sig.payload);
        let data = json!({
            "sender":  sig.sender.to_string(),
            "payload": payload_b64,
        });
        Ok(Event::default()
            .event(sig.kind.as_ref())
            .data(data.to_string()))
    });

    Sse::new(stream).keep_alive(KeepAlive::default())
}

/// JSON-RPC 2.0 handler for the MCP protocol (`POST /mcp`).
///
/// Dispatches on `method`:
/// - `initialize`   — returns server capabilities.
/// - `tools/list`   — scans `tools/` prefix and returns registered tools.
/// - `tools/call`   — locates a provider and proxies the call via `rpc_call_ctx`.
async fn mcp_handler(
    State(ctx): State<Arc<HttpCtx>>,
    body: axum::body::Bytes,
) -> impl IntoResponse {
    let req: serde_json::Value = match serde_json::from_slice(&body) {
        Ok(v)  => v,
        Err(_) => {
            return Json(json!({
                "jsonrpc": "2.0", "id": null,
                "error": {"code": -32700, "message": "parse error"},
            })).into_response();
        }
    };

    let id     = req.get("id").cloned().unwrap_or(serde_json::Value::Null);
    let method = req["method"].as_str().unwrap_or("");

    match method {
        "initialize" => Json(json!({
            "jsonrpc": "2.0",
            "id": id,
            "result": {
                "protocolVersion": "2024-11-05",
                "capabilities": {"tools": {}},
                "serverInfo": {
                    "name": "mycelium",
                    "version": env!("CARGO_PKG_VERSION"),
                },
            },
        })).into_response(),

        "tools/list" => {
            let mut tool_map: std::collections::HashMap<String, serde_json::Value>
                = Default::default();
            for (key, bytes) in
                crate::store::scan_kv_prefix(&ctx.agent_ctx.kv_state, "tools/")
            {
                let rest = key.strip_prefix("tools/").unwrap_or_default();
                let Some((name, _node_id)) = rest.split_once('/') else { continue };
                if tool_map.contains_key(name) { continue; }
                let schema: serde_json::Value =
                    serde_json::from_slice(&bytes).unwrap_or(json!({}));
                tool_map.insert(name.to_string(), schema);
            }
            let tools: Vec<serde_json::Value> = tool_map.into_iter().map(|(name, schema)| {
                let description = schema.get("description")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());
                let mut entry = json!({"name": name, "inputSchema": schema});
                if let Some(desc) = description {
                    entry["description"] = json!(desc);
                }
                entry
            }).collect();
            Json(json!({
                "jsonrpc": "2.0",
                "id": id,
                "result": {"tools": tools},
            })).into_response()
        }

        "tools/call" => {
            let name = req["params"]["name"].as_str().unwrap_or("").to_string();
            let arguments = req["params"]["arguments"].clone();

            if name.is_empty() {
                return Json(json!({
                    "jsonrpc": "2.0", "id": id,
                    "error": {"code": -32602, "message": "invalid params: missing tool name"},
                })).into_response();
            }

            let prefix = format!("tools/{name}/");
            let provider = crate::store::scan_kv_prefix(&ctx.agent_ctx.kv_state, &prefix)
                .into_iter()
                .find_map(|(key, _)| {
                    let rest = key.strip_prefix(&prefix)?;
                    rest.parse::<crate::node_id::NodeId>().ok()
                });

            let Some(provider_node_id) = provider else {
                return Json(json!({
                    "jsonrpc": "2.0", "id": id,
                    "error": {"code": -32601, "message": format!("tool not found: {name}")},
                })).into_response();
            };

            let tool_req = json!({
                "jsonrpc": "2.0",
                "id": req["id"],
                "method": "tools/call",
                "params": {"name": name, "arguments": arguments},
            });

            match super::rpc::rpc_call_ctx(
                &ctx.agent_ctx,
                provider_node_id,
                std::sync::Arc::from(crate::signal::signal_kind::MCP_INVOKE),
                Bytes::from(tool_req.to_string().into_bytes()),
                Duration::from_secs(30),
            ).await {
                Ok(reply_bytes) => {
                    let resp: serde_json::Value = serde_json::from_slice(&reply_bytes)
                        .unwrap_or_else(|_| json!({
                            "jsonrpc": "2.0", "id": id,
                            "error": {"code": -32603, "message": "tool returned invalid JSON"},
                        }));
                    Json(resp).into_response()
                }
                Err(super::rpc::RpcError::Timeout) => Json(json!({
                    "jsonrpc": "2.0", "id": id,
                    "error": {"code": -32000, "message": "tool invocation timed out"},
                })).into_response(),
            }
        }

        _ => Json(json!({
            "jsonrpc": "2.0", "id": id,
            "error": {"code": -32601, "message": format!("method not found: {method}")},
        })).into_response(),
    }
}

// ── Language-bridge gateway handlers ─────────────────────────────────────────
//
// These seven endpoints form the HTTP sidecar API for Python/TypeScript agents.
// All inputs and outputs use JSON. Binary payloads are base64-encoded.

/// `POST /gateway/capability/advertise`
///
/// Advertises a capability on behalf of a language-bridge agent. The
/// returned `handle_id` must be supplied to `DELETE /gateway/capability/{id}`
/// to retract the advertisement (tombstone the KV entry).
///
/// Request body:
/// ```json
/// { "ns": "compute", "name": "gpu",
///   "interval_secs": 30,
///   "attributes": { "model": "A100" },
///   "authorized_callers": ["orchestrator"] }
/// ```
/// Response: `{ "handle_id": "<uuid>" }`
async fn gw_cap_advertise(
    State(ctx): State<Arc<HttpCtx>>,
    Json(body):  Json<serde_json::Value>,
) -> impl IntoResponse {
    use crate::capability::{Capability, CapValue};

    let ns   = match body["ns"].as_str()   { Some(s) => s.to_string(), None => return (StatusCode::BAD_REQUEST, Json(json!({"error":"missing ns"}))).into_response() };
    let name = match body["name"].as_str() { Some(s) => s.to_string(), None => return (StatusCode::BAD_REQUEST, Json(json!({"error":"missing name"}))).into_response() };
    let interval_secs = body["interval_secs"].as_u64().unwrap_or(30);

    let mut cap = Capability::new(ns.as_str(), name.as_str());

    if let Some(attrs) = body["attributes"].as_object() {
        for (k, v) in attrs {
            let cv = match v {
                serde_json::Value::String(s) => CapValue::Text(Arc::from(s.as_str())),
                serde_json::Value::Number(n) => {
                    if let Some(i) = n.as_i64() { CapValue::Integer(i) }
                    else if let Some(f) = n.as_f64() { CapValue::Float(f) }
                    else { continue }
                }
                serde_json::Value::Bool(b) => CapValue::Bool(*b),
                _ => continue,
            };
            cap = cap.with(k.as_str(), cv);
        }
    }

    if let Some(callers) = body["authorized_callers"].as_array() {
        let list: Vec<Arc<str>> = callers.iter()
            .filter_map(|v| v.as_str())
            .map(Arc::from)
            .collect();
        cap = cap.with_authorized_callers(list);
    }

    let interval = Duration::from_secs(interval_secs.max(1));
    let kv_key: Arc<str> = Arc::from(
        format!("cap/{}/{}/{}", ctx.agent_ctx.node_id, cap.namespace, cap.name).as_str()
    );
    let cap_arc = Arc::new(cap);
    let payload_fn: super::kv::PersistPayloadFn = {
        let cap = Arc::clone(&cap_arc);
        Arc::new(move || cap.encode())
    };

    let (cancel_tx, cancel_rx) = oneshot::channel::<()>();
    let shutdown_rx = ctx.shutdown_rx.clone();
    tokio::spawn(super::kv::run_kv_persist_task(
        Arc::clone(&ctx.agent_ctx.core), cancel_rx, shutdown_rx, kv_key, interval, payload_fn, None,
    ));

    let handle_id = format!("{:x}", fastrand::u128(..));
    ctx.gateway_caps.lock().unwrap_or_else(|e| e.into_inner()).insert(handle_id.clone(), cancel_tx);

    Json(json!({ "handle_id": handle_id })).into_response()
}

/// `DELETE /gateway/capability/{handle_id}`
///
/// Retracts a previously-advertised capability. Drops the cancel sender,
/// which causes the persist task to tombstone the KV entry.
async fn gw_cap_drop(
    Path(handle_id): Path<String>,
    State(ctx):      State<Arc<HttpCtx>>,
) -> impl IntoResponse {
    let removed = ctx.gateway_caps.lock().unwrap_or_else(|e| e.into_inner()).remove(&handle_id).is_some();
    if removed {
        Json(json!({ "ok": true })).into_response()
    } else {
        (StatusCode::NOT_FOUND, Json(json!({ "error": "handle not found" }))).into_response()
    }
}

/// `GET /gateway/capability/resolve?ns=X&name=Y[&caller_id=Z]`
///
/// Snapshot filter-match over the local `cap/` KV view. If `caller_id` is
/// supplied, capabilities with non-empty `authorized_callers` are filtered
/// to only those that list the caller's identity.
#[derive(Deserialize)]
struct ResolveQuery {
    ns:        String,
    name:      String,
    caller_id: Option<String>,
}

async fn gw_cap_resolve(
    Query(q):   Query<ResolveQuery>,
    State(ctx): State<Arc<HttpCtx>>,
) -> impl IntoResponse {
    use crate::capability::{CallerContext, CapFilter, Capability};

    let filter     = CapFilter::new(q.ns.as_str(), q.name.as_str());
    let caller_ctx = match q.caller_id {
        Some(id) => CallerContext::for_caller(id.as_str()),
        None     => CallerContext::unrestricted(),
    };

    let mut results = Vec::new();
    for (key, bytes) in crate::store::scan_kv_prefix(&ctx.agent_ctx.kv_state, "cap/") {
        if super::capability_ops::is_cap_locality_key(&key) { continue; }
        let Some((node_id, _ns, _name)) =
            super::capability_ops::parse_cap_key_or_warn("cap/", &key)
            else { continue };
        let Some(cap) = Capability::decode(&bytes) else { continue };
        if filter.matches(&cap) && caller_ctx.can_see(&cap) {
            let attrs: serde_json::Map<String, serde_json::Value> = cap.attributes.iter()
                .map(|(k, v)| (k.as_ref().to_string(), capvalue_to_json(v)))
                .collect();
            results.push(json!({
                "node_id":    node_id.to_string(),
                "ns":         cap.namespace.as_ref(),
                "name":       cap.name.as_ref(),
                "attributes": attrs,
            }));
        }
    }

    Json(json!({ "providers": results })).into_response()
}

fn capvalue_to_json(v: &crate::capability::CapValue) -> serde_json::Value {
    use crate::capability::CapValue;
    match v {
        CapValue::Text(s)    => serde_json::Value::String(s.as_ref().to_string()),
        CapValue::Integer(n) => json!(n),
        CapValue::Float(f)   => json!(f),
        CapValue::Bool(b)    => json!(b),
        CapValue::Version(v) => serde_json::Value::String(format!("{}.{}.{}", v[0], v[1], v[2])),
    }
}

/// `POST /gateway/signal/emit`
///
/// Fires a signal into the mesh. `scope` is `"system"`, `"group:NAME"`, or
/// `"node:IP:PORT"`. `payload_b64` is the base64-encoded signal payload.
async fn gw_signal_emit(
    State(ctx): State<Arc<HttpCtx>>,
    Json(body):  Json<serde_json::Value>,
) -> impl IntoResponse {
    use base64::Engine as _;
    use crate::signal::SignalScope;

    let kind = match body["kind"].as_str() {
        Some(k) => Arc::from(k),
        None    => return (StatusCode::BAD_REQUEST, Json(json!({"error":"missing kind"}))).into_response(),
    };

    let scope_str = body["scope"].as_str().unwrap_or("system");
    let scope = if scope_str == "system" {
        SignalScope::System
    } else if let Some(rest) = scope_str.strip_prefix("group:") {
        SignalScope::Group(Arc::from(rest))
    } else if let Some(rest) = scope_str.strip_prefix("node:") {
        match rest.parse::<crate::node_id::NodeId>() {
            Ok(nid) => SignalScope::Individual(nid),
            Err(_)  => return (StatusCode::BAD_REQUEST, Json(json!({"error":"invalid node id"}))).into_response(),
        }
    } else {
        SignalScope::System
    };

    let payload = if let Some(b64) = body["payload_b64"].as_str() {
        match base64::engine::general_purpose::STANDARD.decode(b64) {
            Ok(v)  => Bytes::from(v),
            Err(_) => return (StatusCode::BAD_REQUEST, Json(json!({"error":"invalid base64 payload"}))).into_response(),
        }
    } else {
        Bytes::new()
    };

    // Same code path as GossipAgent::emit — local delivery + gossip fan-out
    let ok = super::helpers::emit_signal(&ctx.agent_ctx, kind, scope, payload);
    Json(json!({ "ok": ok })).into_response()
}

/// `GET /gateway/signal/sse/{kind}` — SSE stream of admitted signals for a kind.
///
/// Each event has `event: <kind>` and `data: {"sender":"…","payload_b64":"…","nonce":…}`.
async fn gw_signal_sse(
    Path(kind):  Path<String>,
    State(ctx):  State<Arc<HttpCtx>>,
) -> Sse<impl futures_util::Stream<Item = Result<Event, Infallible>>> {
    let rx = ctx.agent_ctx.signal_handlers.register_with_capacity(
        Arc::from(kind.as_str()),
        256,
    );

    let stream = ReceiverStream::new(rx).map(|sig: crate::signal::Signal| {
        use base64::Engine as _;
        let payload_b64 = base64::engine::general_purpose::STANDARD.encode(&sig.payload);
        let data = json!({
            "sender":      sig.sender.to_string(),
            "payload_b64": payload_b64,
            "nonce":       sig.nonce,
        });
        Ok(Event::default()
            .event(sig.kind.as_ref())
            .data(data.to_string()))
    });

    Sse::new(stream).keep_alive(KeepAlive::default())
}

/// `GET /gateway/demand?ns=X&name=Y`
///
/// Returns the demand-pressure snapshot for a capability filter.
#[derive(Deserialize)]
struct DemandQuery { ns: String, name: String }

async fn gw_demand(
    Query(q):   Query<DemandQuery>,
    State(ctx): State<Arc<HttpCtx>>,
) -> impl IntoResponse {
    use crate::capability::CapFilter;

    let filter   = CapFilter::new(q.ns.as_str(), q.name.as_str());
    let kv       = &ctx.agent_ctx.kv_state;

    let providers = crate::store::scan_kv_prefix(kv, "cap/")
        .into_iter()
        .filter(|(k, v)| {
            if super::capability_ops::is_cap_locality_key(k) { return false; }
            crate::capability::Capability::decode(v)
                .map(|c| filter.matches(&c))
                .unwrap_or(false)
        })
        .count();

    let requirers = crate::store::scan_kv_prefix(kv, "req/")
        .into_iter()
        .filter(|(_, v)| {
            crate::capability::CapFilter::decode(v)
                .map(|f| f.namespace == filter.namespace && f.name == filter.name)
                .unwrap_or(false)
        })
        .count();

    let pressure = (requirers as f64) / (providers.max(1) as f64);

    Json(json!({
        "ns":              q.ns,
        "name":            q.name,
        "providers":       providers,
        "requirers":       requirers,
        "demand_pressure": pressure,
    })).into_response()
}

/// `POST /gateway/rpc/call`
///
/// Sends a blocking RPC call to a named node. `payload_b64` is base64.
/// Returns `{ "ok": true, "result_b64": "…" }` or `{ "ok": false, "error": "timeout" }`.
async fn gw_rpc_call(
    State(ctx): State<Arc<HttpCtx>>,
    Json(body):  Json<serde_json::Value>,
) -> impl IntoResponse {
    use base64::Engine as _;

    let target_str = match body["target"].as_str() {
        Some(s) => s.to_string(),
        None    => return (StatusCode::BAD_REQUEST, Json(json!({"error":"missing target"}))).into_response(),
    };
    let target: crate::node_id::NodeId = match target_str.parse() {
        Ok(n)  => n,
        Err(_) => return (StatusCode::BAD_REQUEST, Json(json!({"error":"invalid target node id"}))).into_response(),
    };

    let method = match body["method"].as_str() {
        Some(m) => Arc::from(m),
        None    => return (StatusCode::BAD_REQUEST, Json(json!({"error":"missing method"}))).into_response(),
    };

    let payload = if let Some(b64) = body["payload_b64"].as_str() {
        match base64::engine::general_purpose::STANDARD.decode(b64) {
            Ok(v)  => Bytes::from(v),
            Err(_) => return (StatusCode::BAD_REQUEST, Json(json!({"error":"invalid base64 payload"}))).into_response(),
        }
    } else {
        Bytes::new()
    };

    let timeout_secs = body["timeout_secs"].as_u64().unwrap_or(30);
    let timeout      = Duration::from_secs(timeout_secs.clamp(1, 300));

    match super::rpc::rpc_call_ctx(&ctx.agent_ctx, target, method, payload, timeout).await {
        Ok(result) => {
            let result_b64 = base64::engine::general_purpose::STANDARD.encode(&result);
            Json(json!({ "ok": true, "result_b64": result_b64 })).into_response()
        }
        Err(super::rpc::RpcError::Timeout) => {
            (StatusCode::GATEWAY_TIMEOUT, Json(json!({ "ok": false, "error": "timeout" }))).into_response()
        }
    }
}

// ── KV gateway handlers ───────────────────────────────────────────────────────

#[derive(Deserialize)]
struct KvKeyQuery { key: String }

/// `GET /gateway/kv?key=K` — read a single KV entry.
///
/// Returns `{"found": true, "value_b64": "…"}` or `{"found": false}`.
async fn gw_kv_get(
    Query(q):   Query<KvKeyQuery>,
    State(ctx): State<Arc<HttpCtx>>,
) -> impl IntoResponse {
    match ctx.agent_ctx.kv_state.store.pin().get(q.key.as_str()).and_then(|e| e.data.clone()) {
        Some(bytes) => {
            use base64::Engine as _;
            let v = base64::engine::general_purpose::STANDARD.encode(&bytes);
            Json(json!({ "found": true, "value_b64": v })).into_response()
        }
        None => Json(json!({ "found": false })).into_response(),
    }
}

/// `POST /gateway/kv` — write a KV entry.
///
/// Body: `{"key": "…", "value_b64": "…"}`. Returns `{"ok": true}`.
async fn gw_kv_set(
    State(ctx): State<Arc<HttpCtx>>,
    Json(body):  Json<serde_json::Value>,
) -> impl IntoResponse {
    use base64::Engine as _;

    let key = match body["key"].as_str() {
        Some(k) => Arc::from(k),
        None    => return (StatusCode::BAD_REQUEST, Json(json!({"error":"missing key"}))).into_response(),
    };
    let value = if let Some(b64) = body["value_b64"].as_str() {
        match base64::engine::general_purpose::STANDARD.decode(b64) {
            Ok(v)  => Bytes::from(v),
            Err(_) => return (StatusCode::BAD_REQUEST, Json(json!({"error":"invalid base64"}))).into_response(),
        }
    } else {
        Bytes::new()
    };

    kv_write(&ctx.agent_ctx, key, value, false);
    Json(json!({ "ok": true })).into_response()
}

/// `DELETE /gateway/kv?key=K` — tombstone a KV entry.
///
/// Returns `{"ok": true}`.
async fn gw_kv_delete(
    Query(q):   Query<KvKeyQuery>,
    State(ctx): State<Arc<HttpCtx>>,
) -> impl IntoResponse {
    kv_write(&ctx.agent_ctx, Arc::from(q.key.as_str()), Bytes::new(), true);
    Json(json!({ "ok": true })).into_response()
}

#[derive(Deserialize)]
struct KvKeysQuery { prefix: Option<String> }

/// `GET /gateway/kv/keys?prefix=P` — list live KV keys, optionally filtered by prefix.
///
/// Returns `{"keys": ["key1", "key2", …]}`.
async fn gw_kv_keys(
    Query(q):   Query<KvKeysQuery>,
    State(ctx): State<Arc<HttpCtx>>,
) -> impl IntoResponse {
    let keys: Vec<String> = if let Some(ref pfx) = q.prefix {
        crate::store::scan_kv_prefix(&ctx.agent_ctx.kv_state, pfx.as_str())
            .into_iter()
            .map(|(k, _)| k.as_ref().to_string())
            .collect()
    } else {
        ctx.agent_ctx.kv_state.store.pin()
            .iter()
            .filter(|(_, v)| v.data.is_some())
            .map(|(k, _)| k.as_ref().to_string())
            .collect()
    };
    Json(json!({ "keys": keys })).into_response()
}

/// `POST /gateway/kv/quorum` — write + wait for peer durability acknowledgements.
///
/// Request body:
/// ```json
/// { "key": "...", "value_b64": "<base64>", "min_acks": 2, "timeout_secs": 5.0 }
/// ```
/// Success: `{ "ok": true, "acks_received": 2 }`
/// Timeout: `{ "ok": false, "error": "timeout", "acks_received": 0 }`
#[derive(Deserialize)]
struct KvQuorumBody {
    key:         String,
    #[serde(default)]
    value_b64:   String,
    min_acks:    usize,
    #[serde(default = "default_quorum_timeout")]
    timeout_secs: f64,
}

fn default_quorum_timeout() -> f64 { 5.0 }

async fn gw_kv_quorum(
    State(ctx): State<Arc<HttpCtx>>,
    Json(body): Json<KvQuorumBody>,
) -> impl IntoResponse {
    use base64::Engine as _;
    use super::kv_quorum::QuorumAckTracker;

    let value = match base64::engine::general_purpose::STANDARD.decode(&body.value_b64) {
        Ok(v)  => Bytes::from(v),
        Err(_) => return (StatusCode::BAD_REQUEST,
            Json(json!({ "error": "invalid base64" }))).into_response(),
    };

    let key: Arc<str> = Arc::from(body.key.as_str());
    let timeout        = Duration::from_secs_f64(body.timeout_secs);
    let tc             = Arc::clone(&ctx.agent_ctx);

    if body.min_acks == 0 {
        kv_write(&tc, key, value, false);
        return Json(json!({ "ok": true, "acks_received": 0 })).into_response();
    }

    let write_ts_min = tc.hlc.tick();
    let self_hash    = tc.node_id.id_hash();
    let (tracker, mut rx) = QuorumAckTracker::new(write_ts_min, self_hash);
    super::kv_quorum::install_tracker(&tc.kv_state.quorum_trackers, Arc::clone(&key), &tracker);

    kv_write(&tc, Arc::clone(&key), value, false);

    let result = tokio::time::timeout(timeout, async {
        loop {
            let n = *rx.borrow();
            if n >= body.min_acks { return n; }
            if rx.changed().await.is_err() { return *rx.borrow(); }
        }
    })
    .await;

    super::kv_quorum::remove_tracker(&tc.kv_state.quorum_trackers, &key, &tracker);

    match result {
        Ok(n)  => Json(json!({ "ok": true, "acks_received": n })).into_response(),
        Err(_) => {
            let n = *rx.borrow();
            Json(json!({ "ok": false, "error": "timeout", "acks_received": n })).into_response()
        }
    }
}

/// Applies a KV write (set or delete) and fans out to gossip peers.
fn kv_write(ctx: &Arc<TaskCtx>, key: Arc<str>, value: Bytes, tombstone: bool) -> bool {
    use crate::framing::{dispatch_gossip_try_send, make_gossip_update, ForwardHint, WireMessage};
    use crate::store::apply_and_notify;
    let update = make_gossip_update(&ctx.node_id, ctx.default_ttl, key, value, tombstone, &ctx.hlc);
    if let Some(wal) = ctx.wal.get() {
        wal.append_try(crate::framing::sync_entry_from(&update));
    }
    apply_and_notify(&ctx.kv_state, &update);
    dispatch_gossip_try_send(
        &ctx.gossip_txs,
        WireMessage::Data(update),
        ctx.node_id.id_hash(),
        ForwardHint::All,
        &ctx.kv_state.dropped_frames,
    )
}

// ── RPC serve / respond gateway handlers ─────────────────────────────────────

/// `GET /gateway/rpc/serve/{kind}` — SSE stream of incoming RPC requests.
///
/// Streams requests as `{"nonce_hex": "…", "sender": "IP:PORT", "payload_b64": "…"}`.
/// The receiver must call `POST /gateway/rpc/respond` with the same `nonce_hex` and
/// `sender` to complete the round-trip.
async fn gw_rpc_serve(
    Path(kind):  Path<String>,
    State(ctx):  State<Arc<HttpCtx>>,
) -> Sse<impl futures_util::Stream<Item = Result<Event, Infallible>>> {
    let rx = ctx.agent_ctx.signal_handlers.register_with_capacity(
        Arc::from(kind.as_str()),
        256,
    );

    let stream = ReceiverStream::new(rx).filter_map(|sig: crate::signal::Signal| {
        use base64::Engine as _;
        if sig.payload.len() < 8 { return None; }
        let nonce = u64::from_le_bytes(sig.payload[..8].try_into().expect("infallible: payload.len() >= 8 checked above"));
        let app_payload = sig.payload.slice(8..);
        let payload_b64 = base64::engine::general_purpose::STANDARD.encode(&app_payload);
        let data = json!({
            "nonce_hex":   format!("{:016x}", nonce),
            "sender":      sig.sender.to_string(),
            "payload_b64": payload_b64,
        });
        Some(Ok(Event::default()
            .event(sig.kind.as_ref())
            .data(data.to_string())))
    });

    Sse::new(stream).keep_alive(KeepAlive::default())
}

/// `POST /gateway/rpc/respond` — send a reply to an in-flight RPC request.
///
/// Body: `{"nonce_hex": "…", "sender": "IP:PORT", "result_b64": "…"}`.
/// Returns `{"ok": true}`.
async fn gw_rpc_respond(
    State(ctx): State<Arc<HttpCtx>>,
    Json(body):  Json<serde_json::Value>,
) -> impl IntoResponse {
    use base64::Engine as _;
    use crate::signal::SignalScope;

    let nonce_hex = match body["nonce_hex"].as_str() {
        Some(s) => s,
        None    => return (StatusCode::BAD_REQUEST, Json(json!({"error":"missing nonce_hex"}))).into_response(),
    };
    let nonce = match u64::from_str_radix(nonce_hex, 16) {
        Ok(n)  => n,
        Err(_) => return (StatusCode::BAD_REQUEST, Json(json!({"error":"invalid nonce_hex"}))).into_response(),
    };
    let sender: crate::node_id::NodeId = match body["sender"].as_str().and_then(|s| s.parse().ok()) {
        Some(n) => n,
        None    => return (StatusCode::BAD_REQUEST, Json(json!({"error":"missing or invalid sender"}))).into_response(),
    };
    let result = if let Some(b64) = body["result_b64"].as_str() {
        match base64::engine::general_purpose::STANDARD.decode(b64) {
            Ok(v)  => Bytes::from(v),
            Err(_) => return (StatusCode::BAD_REQUEST, Json(json!({"error":"invalid base64 result"}))).into_response(),
        }
    } else {
        Bytes::new()
    };

    let mut buf = BytesMut::with_capacity(8 + result.len());
    buf.put_u64_le(nonce);
    buf.put(result);
    super::helpers::emit_signal(
        &ctx.agent_ctx,
        Arc::from(crate::signal::signal_kind::RPC_RESULT),
        SignalScope::Individual(sender),
        buf.freeze(),
    );

    Json(json!({ "ok": true })).into_response()
}

// ── Scatter-gather gateway handler ────────────────────────────────────────────

/// `POST /gateway/scatter` — fan-out RPC to multiple targets, collect replies.
///
/// Body:
/// ```json
/// {
///   "targets":       ["IP:PORT", …],
///   "method":        "signal-kind",
///   "payload_b64":   "…",
///   "timeout_secs":  10,
///   "min_ok":        1
/// }
/// ```
/// Returns `{"ok": true, "replies": [{"sender": "…", "result_b64": "…"}, …]}` once
/// `min_ok` replies arrive, or `{"ok": false, "error": "…", "replies": […]}` on timeout.
async fn gw_scatter(
    State(ctx): State<Arc<HttpCtx>>,
    Json(body):  Json<serde_json::Value>,
) -> impl IntoResponse {
    use base64::Engine as _;

    let targets: Vec<crate::node_id::NodeId> = match body["targets"].as_array() {
        Some(arr) => arr.iter()
            .filter_map(|v| v.as_str()?.parse().ok())
            .collect(),
        None => return (StatusCode::BAD_REQUEST, Json(json!({"error":"missing targets"}))).into_response(),
    };
    let method: Arc<str> = match body["method"].as_str() {
        Some(m) => Arc::from(m),
        None    => return (StatusCode::BAD_REQUEST, Json(json!({"error":"missing method"}))).into_response(),
    };
    let payload = if let Some(b64) = body["payload_b64"].as_str() {
        match base64::engine::general_purpose::STANDARD.decode(b64) {
            Ok(v)  => Bytes::from(v),
            Err(_) => return (StatusCode::BAD_REQUEST, Json(json!({"error":"invalid base64 payload"}))).into_response(),
        }
    } else {
        Bytes::new()
    };
    let timeout_secs = body["timeout_secs"].as_u64().unwrap_or(10).clamp(1, 300);
    let timeout      = Duration::from_secs(timeout_secs);
    let min_ok       = body["min_ok"].as_u64().unwrap_or(1) as usize;

    let mut js: tokio::task::JoinSet<(crate::node_id::NodeId, Result<Bytes, super::rpc::RpcError>)>
        = tokio::task::JoinSet::new();
    for target in targets {
        let c = Arc::clone(&ctx.agent_ctx);
        let k = Arc::clone(&method);
        let p = payload.clone();
        let t = target.clone();
        js.spawn(async move {
            let res = super::rpc::rpc_call_ctx(&c, t.clone(), k, p, timeout).await;
            (t, res)
        });
    }

    let mut replies: Vec<serde_json::Value> = Vec::new();
    while let Some(res) = js.join_next().await {
        if let Ok((nid, Ok(bytes))) = res {
            let b64 = base64::engine::general_purpose::STANDARD.encode(&bytes);
            replies.push(json!({ "sender": nid.to_string(), "result_b64": b64 }));
            if replies.len() >= min_ok {
                js.abort_all();
                break;
            }
        }
    }

    if replies.len() >= min_ok {
        Json(json!({ "ok": true, "replies": replies })).into_response()
    } else {
        (StatusCode::GATEWAY_TIMEOUT,
         Json(json!({ "ok": false, "error": "insufficient replies", "replies": replies })))
            .into_response()
    }
}

// ── Mailbox gateway handlers ──────────────────────────────────────────────────

/// `GET /gateway/mailbox/{kind}` — SSE stream of mailbox events for this node.
///
/// Streams events as `{"sender": "IP:PORT", "kind": "…", "payload_b64": "…"}`.
/// The subscription is torn down when the client disconnects.
async fn gw_mailbox_subscribe(
    Path(kind):  Path<String>,
    State(ctx):  State<Arc<HttpCtx>>,
) -> Sse<impl futures_util::Stream<Item = Result<Event, Infallible>>> {
    let kind_arc: Arc<str> = Arc::from(kind.as_str());
    let (handle, rx) = super::mailbox::open_mailbox_ctx(
        Arc::clone(&ctx.agent_ctx),
        &ctx.agent_ctx.node_id,
        Arc::clone(&kind_arc),
        256,
        ctx.shutdown_rx.clone(),
    );

    let stream = ReceiverStream::new(rx).map(move |event: super::mailbox::MeshEvent| {
        use base64::Engine as _;
        let _ = &handle; // keep the MailboxHandle alive for the duration of the stream
        let payload_b64 = base64::engine::general_purpose::STANDARD.encode(&event.payload);
        let data = json!({
            "sender":      event.sender.to_string(),
            "kind":        event.kind.as_ref(),
            "payload_b64": payload_b64,
        });
        Ok(Event::default()
            .event(event.kind.as_ref())
            .data(data.to_string()))
    });

    Sse::new(stream).keep_alive(KeepAlive::default())
}

/// `POST /gateway/mailbox/deliver` — deliver an event to a target node's mailbox.
///
/// Body: `{"target": "IP:PORT", "kind": "…", "payload_b64": "…"}`.
/// Returns `{"ok": true}`.
async fn gw_mailbox_deliver(
    State(ctx): State<Arc<HttpCtx>>,
    Json(body):  Json<serde_json::Value>,
) -> impl IntoResponse {
    use base64::Engine as _;

    let target: crate::node_id::NodeId = match body["target"].as_str().and_then(|s| s.parse().ok()) {
        Some(n) => n,
        None    => return (StatusCode::BAD_REQUEST, Json(json!({"error":"missing or invalid target"}))).into_response(),
    };
    let kind: Arc<str> = match body["kind"].as_str() {
        Some(k) => Arc::from(k),
        None    => return (StatusCode::BAD_REQUEST, Json(json!({"error":"missing kind"}))).into_response(),
    };
    let payload = if let Some(b64) = body["payload_b64"].as_str() {
        match base64::engine::general_purpose::STANDARD.decode(b64) {
            Ok(v)  => Bytes::from(v),
            Err(_) => return (StatusCode::BAD_REQUEST, Json(json!({"error":"invalid base64 payload"}))).into_response(),
        }
    } else {
        Bytes::new()
    };

    super::mailbox::deliver_event_ctx(
        &ctx.agent_ctx,
        &ctx.agent_ctx.node_id,
        &target,
        kind,
        payload,
    );

    Json(json!({ "ok": true })).into_response()
}

// ── Overlay gateway helpers ───────────────────────────────────────────────────

/// Build a `ConsensusEngine` from `TaskCtx`, skipping the opacity/load-balance
/// heuristics used by `GossipAgent::system_propose` — those are performance
/// hints, not correctness requirements, and are not available from `TaskCtx`.
#[cfg(feature = "consensus")]
fn overlay_make_engine(ctx: &Arc<TaskCtx>) -> crate::consensus::ConsensusEngine {
    crate::consensus::ConsensusEngine {
        task_ctx:            Arc::clone(ctx),
        abstain_when_opaque: false,
        use_trust_slices:    false,
        max_abstain_ballots: 3,
        self_locality:       None,
        topology_policy:     None,
    }
}

/// Thin system-wide propose from `TaskCtx` (quorum = floor(N/2)+1 over live peers).
#[cfg(feature = "consensus")]
async fn overlay_system_propose(
    ctx:    &Arc<TaskCtx>,
    slot:   &str,
    value:  Bytes,
    config: crate::consensus::ConsensusConfig,
) -> crate::consensus::ConsensusResult {
    let n_nodes = (ctx.peers.len() + 1).max(1);
    let quorum  = super::helpers::compute_quorum_size(config.quorum_size, n_nodes);
    overlay_make_engine(ctx)
        .propose(
            crate::signal::SignalScope::System,
            Arc::from(slot),
            value,
            quorum,
            config,
            None,
        )
        .await
}

/// Thin group propose from `TaskCtx`.
#[cfg(feature = "consensus")]
async fn overlay_group_propose(
    ctx:    &Arc<TaskCtx>,
    group:  &str,
    slot:   &str,
    value:  Bytes,
    config: crate::consensus::ConsensusConfig,
) -> crate::consensus::ConsensusResult {
    let prefix  = crate::signal::grp_prefix(group);
    let members = crate::store::scan_kv_prefix(ctx.kv_state.as_ref(), &prefix);
    let n       = (members.len() + 1).max(1);
    let quorum  = super::helpers::compute_quorum_size(config.quorum_size, n);
    overlay_make_engine(ctx)
        .propose(
            crate::signal::SignalScope::Group(Arc::from(group)),
            Arc::from(slot),
            value,
            quorum,
            config,
            None,
        )
        .await
}

// ── Cross-group consensus ─────────────────────────────────────────────────────

#[cfg(feature = "consensus")]
#[derive(serde::Deserialize)]
struct CrossGroupProposeBody {
    slot:      String,
    value_b64: Option<String>,
    groups:    Vec<crate::consensus::GroupQuorum>,
}

/// `POST /gateway/consensus/cross_group_propose` — multi-group proposal.
///
/// Body: `{"slot": "S", "value_b64": "...", "groups": [{"group":"G","quorum":0.5,"veto":false}]}`
/// Returns `{"ok":true}` on commit, or an error status.
#[cfg(feature = "consensus")]
async fn gw_cross_group_propose(
    State(ctx): State<Arc<HttpCtx>>,
    Json(body):  Json<CrossGroupProposeBody>,
) -> impl IntoResponse {
    use base64::Engine as _;
    let value = if let Some(b64) = body.value_b64.as_deref() {
        match base64::engine::general_purpose::STANDARD.decode(b64) {
            Ok(v)  => Bytes::from(v),
            Err(_) => return (StatusCode::BAD_REQUEST, Json(json!({"error":"invalid base64 value"}))).into_response(),
        }
    } else {
        Bytes::new()
    };
    if body.groups.is_empty() {
        return (StatusCode::BAD_REQUEST, Json(json!({"error":"groups must not be empty"}))).into_response();
    }

    let engine = overlay_make_engine(&ctx.agent_ctx);
    let result = engine.cross_propose(
        Arc::from(body.slot.as_str()),
        value,
        &body.groups,
        crate::consensus::ConsensusConfig::default(),
    ).await;

    match result {
        crate::consensus::ConsensusResult::Committed { .. } =>
            Json(json!({ "ok": true })).into_response(),
        crate::consensus::ConsensusResult::Timeout { ballots_tried, .. } =>
            (StatusCode::GATEWAY_TIMEOUT, Json(json!({ "ok": false, "error": format!("consensus timed out after {ballots_tried} ballot(s)") }))).into_response(),
        crate::consensus::ConsensusResult::Superseded { .. } =>
            (StatusCode::CONFLICT, Json(json!({ "ok": false, "error": "superseded" }))).into_response(),
        crate::consensus::ConsensusResult::TopologyUnsatisfied { .. } =>
            (StatusCode::CONFLICT, Json(json!({ "ok": false, "error": "topology_unsatisfied" }))).into_response(),
    }
}

// ── Overlay: consistent KV ────────────────────────────────────────────────────

/// `POST /gateway/overlay/consistent/set` — consensus-durable KV write (ballot-serialized).
///
/// Body: `{"key": "K", "value_b64": "V"}`.
#[cfg(feature = "consensus")]
async fn gw_overlay_consistent_set(
    State(ctx): State<Arc<HttpCtx>>,
    Json(body):  Json<serde_json::Value>,
) -> impl IntoResponse {
    use base64::Engine as _;
    let key = match body["key"].as_str() {
        Some(k) => k.to_string(),
        None    => return (StatusCode::BAD_REQUEST, Json(json!({"error":"missing key"}))).into_response(),
    };
    let value = if let Some(b64) = body["value_b64"].as_str() {
        match base64::engine::general_purpose::STANDARD.decode(b64) {
            Ok(v)  => Bytes::from(v),
            Err(_) => return (StatusCode::BAD_REQUEST, Json(json!({"error":"invalid base64 value"}))).into_response(),
        }
    } else {
        Bytes::new()
    };

    let slot = format!("consistent/{key}");
    let result = overlay_system_propose(
        &ctx.agent_ctx, &slot, value.clone(),
        crate::consensus::ConsensusConfig::default(),
    ).await;

    match result {
        crate::consensus::ConsensusResult::Committed { .. } => {
            let key_arc: Arc<str> = Arc::from(key.as_str());
            let update = crate::framing::make_gossip_update(
                &ctx.agent_ctx.node_id, ctx.agent_ctx.default_ttl,
                key_arc, value, false, &ctx.agent_ctx.hlc,
            );
            crate::store::apply_and_notify(&ctx.agent_ctx.kv_state, &update);
            crate::framing::dispatch_gossip_try_send(
                &ctx.agent_ctx.gossip_txs,
                crate::framing::WireMessage::Data(update),
                ctx.agent_ctx.node_id.id_hash(),
                crate::framing::ForwardHint::All,
                &ctx.agent_ctx.kv_state.dropped_frames,
            );
            Json(json!({ "ok": true })).into_response()
        }
        crate::consensus::ConsensusResult::Timeout { ballots_tried, .. } =>
            (StatusCode::GATEWAY_TIMEOUT, Json(json!({ "ok": false, "error": format!("consensus timed out after {ballots_tried} ballot(s)") }))).into_response(),
        crate::consensus::ConsensusResult::Superseded { .. } =>
            (StatusCode::CONFLICT, Json(json!({ "ok": false, "error": "superseded" }))).into_response(),
        crate::consensus::ConsensusResult::TopologyUnsatisfied { .. } =>
            (StatusCode::CONFLICT, Json(json!({ "ok": false, "error": "topology_unsatisfied" }))).into_response(),
    }
}

/// `GET /gateway/overlay/consistent/get?key=K` — read latest ballot-committed value (local, eventually consistent).
#[cfg(feature = "consensus")]
async fn gw_overlay_consistent_get(
    Query(q):   Query<KvKeyQuery>,
    State(ctx): State<Arc<HttpCtx>>,
) -> impl IntoResponse {
    use base64::Engine as _;
    let committed_key = format!("consensus/committed/consistent/{}", q.key);
    let value = ctx.agent_ctx.kv_state.store.pin()
        .get(committed_key.as_str())
        .and_then(|e| e.data.clone())
        .or_else(|| {
            ctx.agent_ctx.kv_state.store.pin()
                .get(q.key.as_str())
                .and_then(|e| e.data.clone())
        });
    match value {
        Some(v) => Json(json!({ "found": true, "value_b64": base64::engine::general_purpose::STANDARD.encode(&v) })).into_response(),
        None    => Json(json!({ "found": false })).into_response(),
    }
}

// ── Overlay: distributed lock ─────────────────────────────────────────────────

#[derive(Deserialize)]
#[cfg(feature = "consensus")]
struct LockAcquireBody { name: String, ttl_secs: Option<u64> }

/// `POST /gateway/overlay/lock/acquire` — acquire a named distributed lock.
///
/// Body: `{"name": "N", "ttl_secs": 30}`.
/// Returns `{"guard_id": "…", "token": N}`.
#[cfg(feature = "consensus")]
async fn gw_overlay_lock_acquire(
    State(ctx): State<Arc<HttpCtx>>,
    Json(body):  Json<LockAcquireBody>,
) -> impl IntoResponse {
    use std::time::{SystemTime, UNIX_EPOCH};
    let ttl_secs = body.ttl_secs.unwrap_or(30).clamp(1, 3600);
    let now_ms   = SystemTime::now().duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64).unwrap_or(0);
    let lock_json = serde_json::json!({
        "holder":     ctx.agent_ctx.node_id.to_string(),
        "expires_ms": now_ms + ttl_secs * 1000,
    });
    let value = Bytes::from(serde_json::to_vec(&lock_json).unwrap_or_default());
    let slot  = format!("lock/{}", body.name);

    let result = overlay_system_propose(
        &ctx.agent_ctx, &slot, value,
        crate::consensus::ConsensusConfig::default(),
    ).await;

    match result {
        crate::consensus::ConsensusResult::Committed { ballot, .. } => {
            let guard = LockGuard {
                ctx:      Arc::clone(&ctx.agent_ctx),
                name:     Arc::from(body.name.as_str()),
                token:    ballot,
                released: false,
            };
            let guard_id = format!("{:016x}", fastrand::u64(..));
            ctx.lock_guards.lock().unwrap_or_else(|e| e.into_inner()).insert(guard_id.clone(), guard);
            Json(json!({ "ok": true, "guard_id": guard_id, "token": ballot })).into_response()
        }
        crate::consensus::ConsensusResult::Timeout { ballots_tried, .. } =>
            (StatusCode::GATEWAY_TIMEOUT, Json(json!({ "ok": false, "error": format!("timeout after {ballots_tried} ballot(s)") }))).into_response(),
        crate::consensus::ConsensusResult::Superseded { .. } =>
            (StatusCode::CONFLICT, Json(json!({ "ok": false, "error": "superseded" }))).into_response(),
        crate::consensus::ConsensusResult::TopologyUnsatisfied { .. } =>
            (StatusCode::CONFLICT, Json(json!({ "ok": false, "error": "topology_unsatisfied" }))).into_response(),
    }
}

/// `DELETE /gateway/overlay/lock/:guard_id` — release a held lock.
#[cfg(feature = "consensus")]
async fn gw_overlay_lock_release(
    Path(guard_id): Path<String>,
    State(ctx):     State<Arc<HttpCtx>>,
) -> impl IntoResponse {
    let removed = ctx.lock_guards.lock().unwrap_or_else(|e| e.into_inner()).remove(&guard_id);
    if removed.is_some() {
        Json(json!({ "ok": true })).into_response()
    } else {
        (StatusCode::NOT_FOUND, Json(json!({ "ok": false, "error": "guard_not_found" }))).into_response()
    }
}

// ── Overlay: leader election ──────────────────────────────────────────────────

#[derive(Deserialize)]
#[cfg(feature = "consensus")]
struct ElectBody { group: String }

/// `POST /gateway/overlay/elect` — elect a leader for `group`.
///
/// Body: `{"group": "G"}`.
/// Returns `{"leader": "IP:PORT"}` on success.
#[cfg(feature = "consensus")]
async fn gw_overlay_elect(
    State(ctx): State<Arc<HttpCtx>>,
    Json(body):  Json<ElectBody>,
) -> impl IntoResponse {
    let slot  = format!("leader/{}", body.group);
    let value = Bytes::from(ctx.agent_ctx.node_id.to_string().into_bytes());

    let result = overlay_group_propose(
        &ctx.agent_ctx, &body.group, &slot, value,
        crate::consensus::ConsensusConfig::default(),
    ).await;

    match result {
        crate::consensus::ConsensusResult::Committed { .. } =>
            Json(json!({ "ok": true, "leader": ctx.agent_ctx.node_id.to_string() })).into_response(),
        crate::consensus::ConsensusResult::Superseded { .. } => {
            let committed_key = format!("consensus/committed/{slot}");
            if let Some(raw) = ctx.agent_ctx.kv_state.store.pin().get(committed_key.as_str()).and_then(|e| e.data.clone())
                && let Ok(s) = std::str::from_utf8(&raw) {
                    return Json(json!({ "ok": true, "leader": s.to_string() })).into_response();
                }
            (StatusCode::CONFLICT, Json(json!({ "ok": false, "error": "superseded" }))).into_response()
        }
        crate::consensus::ConsensusResult::Timeout { ballots_tried, .. } =>
            (StatusCode::GATEWAY_TIMEOUT, Json(json!({ "ok": false, "error": format!("timeout after {ballots_tried} ballot(s)") }))).into_response(),
        crate::consensus::ConsensusResult::TopologyUnsatisfied { .. } =>
            (StatusCode::CONFLICT, Json(json!({ "ok": false, "error": "topology_unsatisfied" }))).into_response(),
    }
}

// ── Overlay: ordered log ──────────────────────────────────────────────────────

#[derive(Deserialize)]
struct LogAppendBody { stream: String, value_b64: Option<String> }

/// `POST /gateway/overlay/log/append` — append to `stream`.
///
/// Body: `{"stream": "S", "value_b64": "V"}`.
/// Returns `{"hlc": N}`.
async fn gw_overlay_log_append(
    State(ctx): State<Arc<HttpCtx>>,
    Json(body):  Json<LogAppendBody>,
) -> impl IntoResponse {
    use base64::Engine as _;
    let value = if let Some(b64) = body.value_b64.as_deref() {
        match base64::engine::general_purpose::STANDARD.decode(b64) {
            Ok(v)  => Bytes::from(v),
            Err(_) => return (StatusCode::BAD_REQUEST, Json(json!({"error":"invalid base64 value"}))).into_response(),
        }
    } else {
        Bytes::new()
    };

    let hlc = ctx.agent_ctx.hlc.tick();
    let key: Arc<str> = Arc::from(format!("log/{}/{hlc:016x}", body.stream).as_str());
    let update = crate::framing::make_gossip_update(
        &ctx.agent_ctx.node_id, ctx.agent_ctx.default_ttl,
        key, value, false, &ctx.agent_ctx.hlc,
    );
    crate::store::apply_and_notify(&ctx.agent_ctx.kv_state, &update);
    crate::framing::dispatch_gossip_try_send(
        &ctx.agent_ctx.gossip_txs,
        crate::framing::WireMessage::Data(update),
        ctx.agent_ctx.node_id.id_hash(),
        crate::framing::ForwardHint::All,
        &ctx.agent_ctx.kv_state.dropped_frames,
    );
    Json(json!({ "hlc": hlc })).into_response()
}

#[derive(Deserialize)]
struct LogScanQuery { stream: String, from: Option<u64>, to: Option<u64> }

/// `GET /gateway/overlay/log/scan?stream=S&from=0&to=MAX` — range scan.
///
/// Returns `[{"hlc": N, "value_b64": "…"}]`.
async fn gw_overlay_log_scan(
    Query(q):   Query<LogScanQuery>,
    State(ctx): State<Arc<HttpCtx>>,
) -> impl IntoResponse {
    use base64::Engine as _;
    let from = q.from.unwrap_or(0);
    let to   = q.to.unwrap_or(u64::MAX);
    let prefix = format!("log/{}/", q.stream);
    let mut entries: Vec<LogEntry> = crate::store::scan_kv_prefix(&ctx.agent_ctx.kv_state, &prefix)
        .into_iter()
        .filter_map(|(k, v)| {
            let suffix = k.strip_prefix(&prefix)?;
            let hlc    = u64::from_str_radix(suffix, 16).ok()?;
            if hlc >= from && hlc < to { Some(LogEntry { hlc, value: v }) } else { None }
        })
        .collect();
    entries.sort_by_key(|e| e.hlc);
    let arr: Vec<serde_json::Value> = entries.iter().map(|e| json!({
        "hlc":       e.hlc,
        "value_b64": base64::engine::general_purpose::STANDARD.encode(&e.value),
    })).collect();
    Json(arr).into_response()
}

#[derive(Deserialize)]
struct LogCompactBody { stream: String, before_hlc: u64 }

/// `POST /gateway/overlay/log/compact` — tombstone entries with HLC < `before_hlc`.
async fn gw_overlay_log_compact(
    State(ctx): State<Arc<HttpCtx>>,
    Json(body):  Json<LogCompactBody>,
) -> impl IntoResponse {
    let prefix = format!("log/{}/", body.stream);
    for (k, _) in crate::store::scan_kv_prefix(&ctx.agent_ctx.kv_state, &prefix) {
        let suffix = k.strip_prefix(&prefix).unwrap_or("");
        if let Ok(hlc) = u64::from_str_radix(suffix, 16)
            && hlc < body.before_hlc {
                let update = crate::framing::make_gossip_update(
                    &ctx.agent_ctx.node_id, ctx.agent_ctx.default_ttl,
                    k, Bytes::new(), true, &ctx.agent_ctx.hlc,
                );
                crate::store::apply_and_notify(&ctx.agent_ctx.kv_state, &update);
                crate::framing::dispatch_gossip_try_send(
                    &ctx.agent_ctx.gossip_txs,
                    crate::framing::WireMessage::Data(update),
                    ctx.agent_ctx.node_id.id_hash(),
                    crate::framing::ForwardHint::All,
                    &ctx.agent_ctx.kv_state.dropped_frames,
                );
            }
    }
    Json(json!({ "ok": true })).into_response()
}

#[derive(Deserialize)]
struct LogSubscribeQuery { stream: String, since: Option<u64> }

/// `GET /gateway/overlay/log/subscribe?stream=S&since=0` — SSE stream of log entries.
async fn gw_overlay_log_subscribe(
    Query(q):   Query<LogSubscribeQuery>,
    State(ctx): State<Arc<HttpCtx>>,
) -> Sse<impl futures_util::Stream<Item = Result<Event, Infallible>>> {
    let prefix      = format!("log/{}/", q.stream);
    let prefix_arc: Arc<str> = Arc::from(prefix.as_str());
    let mut watcher  = super::capability_ops::subscribe_prefix_on_kv(&ctx.agent_ctx.kv_state, Arc::clone(&prefix_arc));
    let stream_name  = q.stream.clone();
    let kv_state     = Arc::clone(&ctx.agent_ctx.kv_state);
    let mut last_seen = q.since.unwrap_or(0);

    let (tx, rx) = tokio::sync::mpsc::channel::<Event>(256);
    tokio::spawn(async move {
        loop {
            let entries = {
                let mut es: Vec<LogEntry> = crate::store::scan_kv_prefix(&kv_state, &prefix)
                    .into_iter()
                    .filter_map(|(k, v)| {
                        let suffix = k.strip_prefix(&prefix)?;
                        let hlc    = u64::from_str_radix(suffix, 16).ok()?;
                        if hlc >= last_seen { Some(LogEntry { hlc, value: v }) } else { None }
                    })
                    .collect();
                es.sort_by_key(|e| e.hlc);
                es
            };
            for entry in entries {
                use base64::Engine as _;
                last_seen = entry.hlc + 1;
                let data  = json!({
                    "stream":    stream_name,
                    "hlc":       entry.hlc,
                    "value_b64": base64::engine::general_purpose::STANDARD.encode(&entry.value),
                });
                if tx.send(Event::default().data(data.to_string())).await.is_err() { return; }
            }
            if watcher.changed().await.is_err() { return; }
        }
    });

    Sse::new(ReceiverStream::new(rx).map(Ok::<_, Infallible>)).keep_alive(KeepAlive::default())
}

#[derive(Deserialize)]
#[cfg(feature = "consensus")]
struct LogGroupSubscribeQuery { stream: String, group: String }

/// `GET /gateway/overlay/log/group/subscribe?stream=S&group=G` — consumer-group SSE.
#[cfg(feature = "consensus")]
async fn gw_overlay_log_group_subscribe(
    Query(q):   Query<LogGroupSubscribeQuery>,
    State(ctx): State<Arc<HttpCtx>>,
) -> Sse<impl futures_util::Stream<Item = Result<Event, Infallible>>> {
    let stream_name = q.stream.clone();
    let group_name  = q.group.clone();
    let kv_state    = Arc::clone(&ctx.agent_ctx.kv_state);
    let task_ctx    = Arc::clone(&ctx.agent_ctx);

    let (tx, rx) = tokio::sync::mpsc::channel::<Event>(64);
    tokio::spawn(async move {
        let handle = SubscribeHandle::from_task_ctx(Arc::clone(&task_ctx));
        loop {
            let lock_name  = format!("clog/{stream_name}/{group_name}/claim");
            let _guard = match handle.distributed_lock(&lock_name, Duration::from_secs(30)).await {
                Ok(g)  => g,
                Err(_) => {
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    continue;
                }
            };
            let offset_key = format!("clog/{stream_name}/{group_name}/offset");
            let offset: u64 = kv_state.store.pin().get(offset_key.as_str())
                .and_then(|e| e.data.clone())
                .and_then(|b| std::str::from_utf8(&b).ok().and_then(|s| u64::from_str_radix(s, 16).ok()))
                .unwrap_or(0);

            let prefix = format!("log/{stream_name}/");
            let mut entries: Vec<LogEntry> = crate::store::scan_kv_prefix(&kv_state, &prefix)
                .into_iter()
                .filter_map(|(k, v)| {
                    let suffix = k.strip_prefix(&prefix)?;
                    let hlc    = u64::from_str_radix(suffix, 16).ok()?;
                    if hlc > offset { Some(LogEntry { hlc, value: v }) } else { None }
                })
                .collect();
            entries.sort_by_key(|e| e.hlc);

            if let Some(entry) = entries.into_iter().next() {
                let new_offset = format!("{:016x}", entry.hlc);
                let offset_key_arc: Arc<str> = Arc::from(offset_key.as_str());
                let update = crate::framing::make_gossip_update(
                    &task_ctx.node_id, task_ctx.default_ttl,
                    offset_key_arc, Bytes::from(new_offset.into_bytes()), false, &task_ctx.hlc,
                );
                crate::store::apply_and_notify(&task_ctx.kv_state, &update);
                crate::framing::dispatch_gossip_try_send(
                    &task_ctx.gossip_txs,
                    crate::framing::WireMessage::Data(update),
                    task_ctx.node_id.id_hash(),
                    crate::framing::ForwardHint::All,
                    &task_ctx.kv_state.dropped_frames,
                );
                use base64::Engine as _;
                let data = json!({
                    "stream":    stream_name,
                    "hlc":       entry.hlc,
                    "value_b64": base64::engine::general_purpose::STANDARD.encode(&entry.value),
                });
                if tx.send(Event::default().data(data.to_string())).await.is_err() { return; }
            } else {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    });

    Sse::new(ReceiverStream::new(rx).map(Ok::<_, Infallible>)).keep_alive(KeepAlive::default())
}

// ── Overlay: reliable delivery ────────────────────────────────────────────────

#[derive(Deserialize)]
struct EmitReliableBody {
    target:       String,
    kind:         String,
    payload_b64:  Option<String>,
    timeout_secs: Option<u64>,
}

/// `POST /gateway/overlay/emit_reliable` — send with explicit ACK.
///
/// Body: `{"target": "IP:PORT", "kind": "K", "payload_b64": "V", "timeout_secs": 5}`.
/// Returns `{"ack": "acknowledged" | "timeout"}`.
async fn gw_overlay_emit_reliable(
    State(ctx): State<Arc<HttpCtx>>,
    Json(body):  Json<EmitReliableBody>,
) -> impl IntoResponse {
    use base64::Engine as _;
    let target: crate::node_id::NodeId = match body.target.parse() {
        Ok(n)  => n,
        Err(_) => return (StatusCode::BAD_REQUEST, Json(json!({"error":"invalid target node id"}))).into_response(),
    };
    let payload = if let Some(b64) = body.payload_b64.as_deref() {
        match base64::engine::general_purpose::STANDARD.decode(b64) {
            Ok(v)  => Bytes::from(v),
            Err(_) => return (StatusCode::BAD_REQUEST, Json(json!({"error":"invalid base64 payload"}))).into_response(),
        }
    } else {
        Bytes::new()
    };
    let timeout = Duration::from_secs(body.timeout_secs.unwrap_or(5).clamp(1, 300));
    let kind: Arc<str> = Arc::from(body.kind.as_str());

    match super::rpc::rpc_call_ctx(&ctx.agent_ctx, target, kind, payload, timeout).await {
        Ok(_)                              => Json(json!({ "ack": "acknowledged" })).into_response(),
        Err(super::rpc::RpcError::Timeout) => Json(json!({ "ack": "timeout" })).into_response(),
    }
}

// ── Cluster sharding ──────────────────────────────────────────────────────────

/// `GET /gateway/shard/{ns}/{name}?key=<shard_key>`
///
/// Returns the consistent-hash owner NodeId for `shard_key` among providers of
/// capability `ns/name`. The result is deterministic: every node with the same
/// provider view returns the same owner for the same key.
///
/// 200 `{"owner":"ip:port"}` — owner found.
/// 404 `{"error":"no providers"}` — no live providers match the filter.
#[derive(Deserialize)]
struct ShardOwnerQuery { key: String }

async fn gw_shard_owner(
    Path((ns, name)): Path<(String, String)>,
    Query(q):         Query<ShardOwnerQuery>,
    State(ctx):       State<Arc<HttpCtx>>,
) -> impl IntoResponse {
    use crate::capability::CapFilter;
    use super::sharding::shard_owner;

    let filter = CapFilter::new(ns.as_str(), name.as_str());
    let providers = resolve_cap_providers(&ctx.agent_ctx.kv_state, &filter);

    match shard_owner(&q.key, &providers) {
        Some(owner) => Json(json!({ "owner": owner.to_string() })).into_response(),
        None        => (StatusCode::NOT_FOUND, Json(json!({ "error": "no providers" }))).into_response(),
    }
}

/// `POST /gateway/shard/emit`
///
/// Emits `kind` signal to the consistent-hash owner for `shard_key` among
/// providers of `ns/name`. Equivalent to calling `emit_sharded` from Rust.
///
/// Request body:
/// ```json
/// { "kind": "actor.msg", "ns": "actor", "name": "user",
///   "shard_key": "user-12345", "payload_b64": "<base64>" }
/// ```
/// Response 200: `{"ok":true,"owner":"ip:port"}`
/// Response 404: `{"ok":false,"error":"no providers"}`
async fn gw_shard_emit(
    State(ctx): State<Arc<HttpCtx>>,
    Json(body):  Json<serde_json::Value>,
) -> impl IntoResponse {
    use base64::Engine as _;
    use crate::capability::CapFilter;
    use super::sharding::shard_owner;
    use crate::signal::SignalScope;

    let kind = match body["kind"].as_str() {
        Some(k) => Arc::from(k),
        None    => return (StatusCode::BAD_REQUEST, Json(json!({"error":"missing kind"}))).into_response(),
    };
    let ns   = match body["ns"].as_str()   { Some(s) => s.to_string(), None => return (StatusCode::BAD_REQUEST, Json(json!({"error":"missing ns"}))).into_response() };
    let name = match body["name"].as_str() { Some(s) => s.to_string(), None => return (StatusCode::BAD_REQUEST, Json(json!({"error":"missing name"}))).into_response() };
    let shard_key = match body["shard_key"].as_str() {
        Some(s) => s.to_string(),
        None    => return (StatusCode::BAD_REQUEST, Json(json!({"error":"missing shard_key"}))).into_response(),
    };
    let payload = if let Some(b64) = body["payload_b64"].as_str() {
        match base64::engine::general_purpose::STANDARD.decode(b64) {
            Ok(b)  => Bytes::from(b),
            Err(_) => return (StatusCode::BAD_REQUEST, Json(json!({"error":"invalid base64"}))).into_response(),
        }
    } else {
        Bytes::new()
    };

    let filter    = CapFilter::new(ns.as_str(), name.as_str());
    let providers = resolve_cap_providers(&ctx.agent_ctx.kv_state, &filter);

    match shard_owner(&shard_key, &providers) {
        Some(owner) => {
            super::helpers::emit_signal_async(
                &ctx.agent_ctx, kind, SignalScope::Individual(owner.clone()), payload,
            ).await;
            Json(json!({ "ok": true, "owner": owner.to_string() })).into_response()
        }
        None => (StatusCode::NOT_FOUND, Json(json!({ "ok": false, "error": "no providers" }))).into_response(),
    }
}

/// Shared helper: scan `cap/` KV and return providers matching `filter`.
/// Mirrors the scan in `gw_cap_resolve` (no freshness check — same as the HTTP resolve endpoint).
fn resolve_cap_providers(
    kv_state: &crate::store::KvState,
    filter:   &crate::capability::CapFilter,
) -> Vec<(crate::node_id::NodeId, crate::capability::Capability)> {
    use crate::capability::Capability;
    use crate::store::scan_kv_prefix;
    use super::capability_ops::{is_cap_locality_key, parse_cap_key_or_warn};

    let mut out = Vec::new();
    for (key, bytes) in scan_kv_prefix(kv_state, "cap/") {
        if is_cap_locality_key(&key) { continue; }
        let Some((node_id, _ns, _name)) = parse_cap_key_or_warn("cap/", &key) else { continue };
        let Some(cap) = Capability::decode(&bytes) else { continue };
        if filter.matches(&cap) {
            out.push((node_id, cap));
        }
    }
    out
}

// ── LLM / Prompt Skills gateway handlers ─────────────────────────────────────

#[cfg(feature = "llm")]
fn llm_get_prompt_from_kv(
    kv_state: &crate::store::KvState,
    ns: &str,
    name: &str,
) -> Option<crate::agent::prompt::PromptTemplate> {
    use crate::signal::kv_ns;
    let key = format!("{}{}/{}", kv_ns::PROMPTS, ns, name);
    let bytes = kv_state.store.pin().get(key.as_str())
        .and_then(|e| e.data.clone())?;
    serde_json::from_slice(&bytes).ok()
}

#[cfg(feature = "llm")]
async fn gw_prompts_list(
    State(ctx): State<Arc<HttpCtx>>,
) -> impl IntoResponse {
    use crate::signal::kv_ns;
    let entries: Vec<serde_json::Value> = crate::store::scan_kv_prefix(
        &ctx.agent_ctx.kv_state, kv_ns::PROMPTS,
    )
    .into_iter()
    .filter_map(|(k, _v)| {
        let rest = k.strip_prefix(kv_ns::PROMPTS)?;
        let mut parts = rest.splitn(2, '/');
        let ns   = parts.next()?.to_owned();
        let name = parts.next()?.to_owned();
        if name.is_empty() { return None; }
        llm_get_prompt_from_kv(&ctx.agent_ctx.kv_state, &ns, &name).map(|t| {
            serde_json::json!({
                "ns":          ns,
                "name":        name,
                "max_tokens":  t.max_tokens,
                "temperature": t.temperature,
                "metadata":    t.metadata,
            })
        })
    })
    .collect();
    axum::Json(entries)
}

#[cfg(feature = "llm")]
async fn gw_prompt_get(
    State(ctx): State<Arc<HttpCtx>>,
    axum::extract::Path((ns, name)): axum::extract::Path<(String, String)>,
) -> impl IntoResponse {
    match llm_get_prompt_from_kv(&ctx.agent_ctx.kv_state, &ns, &name) {
        Some(t) => axum::Json(serde_json::to_value(t).unwrap_or_default())
                       .into_response(),
        None    => (StatusCode::NOT_FOUND, "not found").into_response(),
    }
}

#[cfg(feature = "llm")]
async fn gw_prompt_put(
    State(ctx): State<Arc<HttpCtx>>,
    axum::extract::Path((ns, name)): axum::extract::Path<(String, String)>,
    axum::Json(body): axum::Json<crate::agent::prompt::PromptTemplate>,
) -> impl IntoResponse {
    use crate::signal::kv_ns;
    let kv_key = format!("{}{}/{}", kv_ns::PROMPTS, ns, name);
    match serde_json::to_vec(&body) {
        Ok(bytes) => {
            kv_write(&ctx.agent_ctx, Arc::from(kv_key.as_str()), Bytes::from(bytes), false);
            axum::Json(serde_json::json!({"ok": true})).into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

#[cfg(feature = "llm")]
async fn gw_prompt_delete(
    State(ctx): State<Arc<HttpCtx>>,
    axum::extract::Path((ns, name)): axum::extract::Path<(String, String)>,
) -> impl IntoResponse {
    use crate::signal::kv_ns;
    let key = format!("{}{}/{}", kv_ns::PROMPTS, ns, name);
    kv_write(&ctx.agent_ctx, Arc::from(key.as_str()), Bytes::new(), true);
    axum::Json(serde_json::json!({"ok": true}))
}

#[cfg(feature = "llm")]
#[derive(serde::Deserialize)]
struct LlmCallBody {
    ns:         String,
    name:       String,
    input:      String,
    #[serde(default)]
    context:    std::collections::HashMap<String, String>,
    #[serde(default = "default_timeout_ms")]
    timeout_ms: u64,
}

#[cfg(feature = "llm")]
fn default_timeout_ms() -> u64 { 30_000 }

#[cfg(feature = "llm")]
async fn gw_llm_call(
    State(ctx): State<Arc<HttpCtx>>,
    axum::Json(body): axum::Json<LlmCallBody>,
) -> impl IntoResponse {
    use crate::capability::CapFilter;
    use crate::signal::signal_kind;

    let timeout = std::time::Duration::from_millis(body.timeout_ms);
    let filter  = CapFilter::new(body.ns.as_str(), body.name.as_str());
    let providers = resolve_cap_providers(&ctx.agent_ctx.kv_state, &filter);

    let provider_str = providers.first()
        .map(|(id, _)| id.to_string())
        .unwrap_or_default();

    let (target, _) = match providers.into_iter().next() {
        Some(p) => p,
        None => {
            return (StatusCode::NOT_FOUND,
                axum::Json(serde_json::json!({"error":"no_provider","detail":""})))
                .into_response();
        }
    };

    let req = serde_json::json!({
        "prompt":  format!("{}/{}", body.ns, body.name),
        "input":   body.input,
        "context": body.context,
    });
    let payload = Bytes::from(req.to_string().into_bytes());

    match super::rpc::rpc_call_ctx(
        &ctx.agent_ctx,
        target,
        Arc::from(signal_kind::LLM_INVOKE),
        payload,
        timeout,
    ).await {
        Ok(reply) => {
            let v: serde_json::Value = serde_json::from_slice(&reply)
                .unwrap_or_else(|_| serde_json::json!({"error":"parse_error","detail":""}));
            if v.get("error").is_some() {
                // Provider-side failure forwarded to the caller: upstream error.
                return (StatusCode::BAD_GATEWAY, axum::Json(v)).into_response();
            }
            axum::Json(serde_json::json!({
                "output":   v["output"],
                "provider": provider_str,
            })).into_response()
        }
        Err(super::rpc::RpcError::Timeout) =>
            (StatusCode::GATEWAY_TIMEOUT,
                axum::Json(serde_json::json!({"error":"timeout","detail":""})))
                .into_response(),
    }
}

#[cfg(feature = "llm")]
#[derive(serde::Deserialize)]
struct LlmStreamBody {
    ns:      String,
    name:    String,
    input:   String,
    #[serde(default)]
    context: std::collections::HashMap<String, String>,
}

#[cfg(feature = "llm")]
async fn gw_llm_stream(
    State(ctx): State<Arc<HttpCtx>>,
    axum::Json(body): axum::Json<LlmStreamBody>,
) -> impl IntoResponse {
    use axum::response::sse::Event;
    use crate::capability::CapFilter;
    use crate::signal::signal_kind;
    use futures_util::stream;

    // v1: buffer full response via RPC, emit as single "done" event.
    // Errors are reported as in-stream `{"type":"error",...}` events, not HTTP
    // status codes: SSE commits the status line before the body, so this is
    // the only legible channel once streaming starts (deliberate asymmetry
    // with gw_llm_call, which uses 404/502/504).
    let timeout = std::time::Duration::from_secs(30);
    let filter  = CapFilter::new(body.ns.as_str(), body.name.as_str());
    let providers = resolve_cap_providers(&ctx.agent_ctx.kv_state, &filter);

    let event = match providers.into_iter().next() {
        None => {
            let data = serde_json::json!({"type":"error","error":"no_provider"}).to_string();
            Event::default().data(data)
        }
        Some((target, _)) => {
            let req = serde_json::json!({
                "prompt":  format!("{}/{}", body.ns, body.name),
                "input":   body.input,
                "context": body.context,
            });
            let payload = Bytes::from(req.to_string().into_bytes());
            match super::rpc::rpc_call_ctx(
                &ctx.agent_ctx,
                target,
                Arc::from(signal_kind::LLM_INVOKE),
                payload,
                timeout,
            ).await {
                Ok(reply) => {
                    let v: serde_json::Value = serde_json::from_slice(&reply)
                        .unwrap_or_else(|_| serde_json::json!({"error":"parse_error"}));
                    let output = v["output"].as_str().unwrap_or("").to_owned();
                    let data = serde_json::json!({"type":"done","output":output}).to_string();
                    Event::default().data(data)
                }
                Err(_) => {
                    let data = serde_json::json!({"type":"error","error":"timeout"}).to_string();
                    Event::default().data(data)
                }
            }
        }
    };

    Sse::new(stream::once(async move { Ok::<_, std::convert::Infallible>(event) }))
}

#[cfg(test)]
mod tests {
    use crate::{GossipAgent, GossipConfig, NodeId};
    use std::{sync::Arc, time::Duration};

    fn alloc_port() -> u16 {
        std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port()
    }

    #[tokio::test]
    async fn test_http_health_responds() {
        let gossip_port = alloc_port();
        let http_port   = alloc_port();

        let id  = NodeId::new("127.0.0.1", gossip_port).unwrap();
        let mut cfg = GossipConfig::default();
        cfg.bind_port  = gossip_port;
        cfg.http_port  = Some(http_port);
        cfg.http_addr  = "127.0.0.1".to_string();

        let agent = Arc::new(GossipAgent::new(id, cfg));
        agent.start().await.unwrap();
        // Brief pause for the HTTP server to bind and accept.
        tokio::time::sleep(Duration::from_millis(50)).await;

        let url = format!("http://127.0.0.1:{http_port}/health");
        let resp = reqwest::get(&url).await.expect("HTTP request failed");
        assert_eq!(resp.status(), 200);

        let body: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(body["status"], "ok");
        assert!(body["node_id"].as_str().unwrap().contains("127.0.0.1"));

        agent.shutdown().await;
    }

    #[tokio::test]
    async fn test_http_stats_responds() {
        let gossip_port = alloc_port();
        let http_port   = alloc_port();

        let id  = NodeId::new("127.0.0.1", gossip_port).unwrap();
        let mut cfg = GossipConfig::default();
        cfg.bind_port = gossip_port;
        cfg.http_port = Some(http_port);

        let agent = Arc::new(GossipAgent::new(id, cfg));
        agent.start().await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;

        let url = format!("http://127.0.0.1:{http_port}/stats");
        let resp = reqwest::get(&url).await.expect("stats request failed");
        assert_eq!(resp.status(), 200);

        let body: serde_json::Value = resp.json().await.unwrap();
        assert!(body["store_entries"].is_number());
        assert!(body["dropped_frames"].is_number());

        agent.shutdown().await;
    }

    /// Operational-readiness invariant: shutdown must actually close the
    /// gateway port. A load balancer drains a node by observing connection
    /// refusal; a zombie listener that keeps accepting after shutdown() would
    /// answer health checks from a dead agent (M2 Run-22 probe).
    #[tokio::test]
    async fn test_gateway_port_closes_on_shutdown() {
        let gossip_port = alloc_port();
        let http_port   = alloc_port();

        let id  = NodeId::new("127.0.0.1", gossip_port).unwrap();
        let mut cfg = GossipConfig::default();
        cfg.bind_port = gossip_port;
        cfg.http_port = Some(http_port);

        let agent = Arc::new(GossipAgent::new(id, cfg));
        agent.start().await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;

        let url = format!("http://127.0.0.1:{http_port}/health");
        let resp = reqwest::get(&url).await.expect("health request failed");
        assert_eq!(resp.status(), 200);

        agent.shutdown().await;

        // Poll briefly: the server task abort is asynchronous, but the port
        // must stop accepting within the shutdown grace window.
        let mut closed = false;
        for _ in 0..40 {
            if reqwest::Client::builder()
                .timeout(Duration::from_millis(250))
                .build()
                .unwrap()
                .get(&url)
                .send()
                .await
                .is_err()
            {
                closed = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(closed, "gateway port still accepting after shutdown");
    }

    /// gw_llm_call reports failures via HTTP status codes (the gateway-wide
    /// convention), not a 200 + error-JSON envelope: a no-provider miss is a
    /// 404 so plain `curl -f` / `raise_for_status()` callers see the failure.
    #[cfg(feature = "llm")]
    #[tokio::test]
    async fn test_llm_call_no_provider_returns_404() {
        let gossip_port = alloc_port();
        let http_port   = alloc_port();

        let id  = NodeId::new("127.0.0.1", gossip_port).unwrap();
        let mut cfg = GossipConfig::default();
        cfg.bind_port = gossip_port;
        cfg.http_port = Some(http_port);

        let agent = Arc::new(GossipAgent::new(id, cfg));
        agent.start().await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;

        let url = format!("http://127.0.0.1:{http_port}/gateway/llm/call");
        let resp = reqwest::Client::new()
            .post(&url)
            .json(&serde_json::json!({"ns":"nobody","name":"provides-this","input":"x"}))
            .send()
            .await
            .expect("llm/call request failed");
        assert_eq!(resp.status(), 404, "no provider must surface as HTTP 404");

        let body: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(body["error"], "no_provider", "error JSON body is kept alongside the status");

        agent.shutdown().await;
    }

    #[tokio::test]
    async fn test_sse_delivers_signals() {
        use crate::signal::SignalScope;
        use bytes::Bytes;

        let gossip_port = alloc_port();
        let http_port   = alloc_port();

        let id  = NodeId::new("127.0.0.1", gossip_port).unwrap();
        let mut cfg = GossipConfig::default();
        cfg.bind_port = gossip_port;
        cfg.http_port = Some(http_port);
        // Must be in the "test-sse" group to admit the signal.
        let agent = Arc::new(GossipAgent::new(id, cfg));
        agent.mesh().join_group("test-sse");
        agent.start().await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Connect SSE client.
        let url = format!("http://127.0.0.1:{http_port}/signals/sse-probe");
        let mut resp = reqwest::Client::new()
            .get(&url)
            .send()
            .await
            .expect("SSE connect failed");
        assert_eq!(resp.status(), 200);

        // Emit a signal to self.
        let _ = agent.mesh().emit("sse-probe", SignalScope::System, Bytes::from_static(b"payload"));

        // Read SSE chunks until we see the expected event or timeout.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        let mut found = false;
        while tokio::time::Instant::now() < deadline {
            match tokio::time::timeout(Duration::from_millis(200), resp.chunk()).await {
                Ok(Ok(Some(chunk))) => {
                    let text = String::from_utf8_lossy(&chunk);
                    if text.contains("sse-probe") {
                        found = true;
                        break;
                    }
                }
                _ => break,
            }
        }
        assert!(found, "SSE event for 'sse-probe' was not received within timeout");

        agent.shutdown().await;
    }

    // ── MCP endpoint tests ────────────────────────────────────────────────────

    fn mcp_agent(http_port: u16) -> Arc<GossipAgent> {
        let gossip_port = alloc_port();
        let id = NodeId::new("127.0.0.1", gossip_port).unwrap();
        let mut cfg = GossipConfig::default();
        cfg.bind_port = gossip_port;
        cfg.http_port = Some(http_port);
        Arc::new(GossipAgent::new(id, cfg))
    }

    #[tokio::test]
    async fn test_mcp_initialize() {
        let http_port = alloc_port();
        let agent = mcp_agent(http_port);
        agent.start().await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;

        let resp = reqwest::Client::new()
            .post(format!("http://127.0.0.1:{http_port}/mcp"))
            .json(&serde_json::json!({
                "jsonrpc": "2.0", "id": 0, "method": "initialize",
                "params": {
                    "protocolVersion": "2024-11-05",
                    "capabilities": {},
                    "clientInfo": {"name": "test", "version": "1.0"},
                },
            }))
            .send()
            .await
            .expect("initialize request failed");

        assert_eq!(resp.status(), 200);
        let body: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(body["result"]["protocolVersion"], "2024-11-05");
        assert!(body["result"]["serverInfo"]["name"].as_str().is_some());

        agent.shutdown().await;
    }

    #[tokio::test]
    async fn test_mcp_tools_list() {
        let http_port = alloc_port();
        let agent = mcp_agent(http_port);
        agent.start().await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;

        let _handle = agent.mcp().register_mcp_tool(
            "greet",
            serde_json::json!({
                "type": "object",
                "description": "Greets a person",
                "properties": {"name": {"type": "string"}},
                "required": ["name"],
            }),
            |args| async move {
                Ok(serde_json::json!(format!("hello, {}", args["name"].as_str().unwrap_or("?"))))
            },
        );

        let resp = reqwest::Client::new()
            .post(format!("http://127.0.0.1:{http_port}/mcp"))
            .json(&serde_json::json!({
                "jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {},
            }))
            .send()
            .await
            .expect("tools/list request failed");

        assert_eq!(resp.status(), 200);
        let body: serde_json::Value = resp.json().await.unwrap();
        let tools = body["result"]["tools"].as_array().unwrap();
        assert!(
            tools.iter().any(|t| t["name"] == "greet"),
            "tool 'greet' not in list: {body}"
        );

        agent.shutdown().await;
    }

    #[tokio::test]
    async fn test_mcp_tools_call_round_trip() {
        let http_port = alloc_port();
        let agent = mcp_agent(http_port);
        agent.start().await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;

        let _handle = agent.mcp().register_mcp_tool(
            "square",
            serde_json::json!({
                "type": "object",
                "properties": {"n": {"type": "number"}},
                "required": ["n"],
            }),
            |args| async move {
                let n = args["n"].as_f64().unwrap_or(0.0);
                Ok(serde_json::json!(n * n))
            },
        );

        let resp = reqwest::Client::new()
            .post(format!("http://127.0.0.1:{http_port}/mcp"))
            .json(&serde_json::json!({
                "jsonrpc": "2.0", "id": 2, "method": "tools/call",
                "params": {"name": "square", "arguments": {"n": 5.0}},
            }))
            .send()
            .await
            .expect("tools/call request failed");

        assert_eq!(resp.status(), 200);
        let body: serde_json::Value = resp.json().await.unwrap();
        assert!(body.get("error").is_none(), "unexpected error: {body}");
        let text = body["result"]["content"][0]["text"].as_str().unwrap();
        assert!(text.contains("25"), "expected 25, got '{text}'");

        agent.shutdown().await;
    }

    #[tokio::test]
    async fn test_mcp_tools_call_not_found() {
        let http_port = alloc_port();
        let agent = mcp_agent(http_port);
        agent.start().await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;

        let resp = reqwest::Client::new()
            .post(format!("http://127.0.0.1:{http_port}/mcp"))
            .json(&serde_json::json!({
                "jsonrpc": "2.0", "id": 3, "method": "tools/call",
                "params": {"name": "no-such-tool", "arguments": {}},
            }))
            .send()
            .await
            .expect("tools/call request failed");

        assert_eq!(resp.status(), 200);
        let body: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(body["error"]["code"], -32601);
        assert!(
            body["error"]["message"].as_str().unwrap().contains("no-such-tool"),
            "unexpected error message: {body}"
        );

        agent.shutdown().await;
    }

    // ── WS1 gateway OAuth2 scope ACLs (compliance feature) ────────────────

    #[cfg(feature = "compliance")]
    #[test]
    fn required_scope_table_maps_families_and_denies_by_default() {
        use axum::http::Method;
        use super::required_scope;
        // read/write split on the same path keys off the method.
        assert_eq!(required_scope(&Method::GET,    "/gateway/kv"), "kv:read");
        assert_eq!(required_scope(&Method::POST,   "/gateway/kv"), "kv:write");
        assert_eq!(required_scope(&Method::DELETE, "/gateway/kv"), "kv:write");
        // resource families.
        assert_eq!(required_scope(&Method::GET,  "/gateway/capability/resolve"), "cap:read");
        assert_eq!(required_scope(&Method::POST, "/gateway/signal/emit"), "mesh:write");
        assert_eq!(required_scope(&Method::POST, "/gateway/overlay/consistent/set"), "consensus:write");
        assert_eq!(required_scope(&Method::GET,  "/gateway/overlay/consistent/get"), "consensus:read");
        assert_eq!(required_scope(&Method::POST, "/gateway/llm/call"), "llm:invoke");
        // deny-by-default: anything unmapped requires admin.
        assert_eq!(required_scope(&Method::POST, "/gateway/some/future/route"), "admin");
    }

    #[cfg(feature = "compliance")]
    #[test]
    fn scope_admits_exact_and_wildcard_only() {
        use super::scope_admits;
        let ro = vec!["kv:read".to_string()];
        assert!(scope_admits(&ro, "kv:read"));
        assert!(!scope_admits(&ro, "kv:write"));
        let star = vec!["*".to_string()];
        assert!(scope_admits(&star, "kv:write"));
        assert!(scope_admits(&star, "admin"));
        // Empty grant admits nothing.
        assert!(!scope_admits(&[], "kv:read"));
    }

    #[cfg(feature = "compliance")]
    #[test]
    fn resolve_token_scopes_legacy_is_wildcard() {
        use super::resolve_token_scopes;
        let mut cfg = GossipConfig::default();
        cfg.gateway_auth_token = Some("legacy-tok".to_string());
        cfg.gateway_scoped_tokens = vec![crate::GatewayToken {
            token:  "ro-tok".to_string(),
            scopes: vec!["kv:read".to_string()],
        }];
        // Legacy token → superuser wildcard (unchanged upgrade path).
        assert_eq!(resolve_token_scopes(&cfg, "legacy-tok"), Some(vec!["*".to_string()]));
        // Scoped token → its grant.
        assert_eq!(resolve_token_scopes(&cfg, "ro-tok"), Some(vec!["kv:read".to_string()]));
        // Unknown token → None (unauthenticated).
        assert_eq!(resolve_token_scopes(&cfg, "nope"), None);
    }

    /// End-to-end: a scoped token is admitted on routes within its grant,
    /// denied (403) on routes outside it, the wildcard token passes scope
    /// gating everywhere, an unknown token is 401, and public routes stay open
    /// with no credentials at all.
    #[cfg(feature = "compliance")]
    #[tokio::test]
    async fn test_gateway_scoped_token_acl_end_to_end() {
        use axum::http::header::AUTHORIZATION;

        let gossip_port = alloc_port();
        let http_port   = alloc_port();
        let id  = NodeId::new("127.0.0.1", gossip_port).unwrap();
        let mut cfg = GossipConfig::default();
        cfg.bind_port = gossip_port;
        cfg.http_port = Some(http_port);
        cfg.gateway_scoped_tokens = vec![
            crate::GatewayToken { token: "ro".into(),    scopes: vec!["kv:read".into()] },
            crate::GatewayToken { token: "super".into(), scopes: vec!["*".into()] },
        ];

        let agent = Arc::new(GossipAgent::new(id, cfg));
        agent.start().await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;

        let client = reqwest::Client::new();
        let base = format!("http://127.0.0.1:{http_port}");

        // Public route: open, no credentials.
        let r = client.get(format!("{base}/health")).send().await.unwrap();
        assert_eq!(r.status(), 200, "public /health must stay open");

        // No token on a protected route → 401.
        let r = client.get(format!("{base}/gateway/kv/keys")).send().await.unwrap();
        assert_eq!(r.status(), 401, "missing token must be unauthorized");

        // Unknown token → 401.
        let r = client.get(format!("{base}/gateway/kv/keys"))
            .header(AUTHORIZATION, "Bearer bogus").send().await.unwrap();
        assert_eq!(r.status(), 401, "unknown token must be unauthorized");

        // ro token on a kv:read route → admitted (not 401/403).
        let r = client.get(format!("{base}/gateway/kv/keys"))
            .header(AUTHORIZATION, "Bearer ro").send().await.unwrap();
        assert_eq!(r.status(), 200, "kv:read token must reach kv/keys");

        // ro token on a kv:write route → 403 (authenticated, insufficient scope).
        let r = client.post(format!("{base}/gateway/kv"))
            .header(AUTHORIZATION, "Bearer ro")
            .json(&serde_json::json!({"key": "k", "value": "v"}))
            .send().await.unwrap();
        assert_eq!(r.status(), 403, "kv:read token must be forbidden on kv:write");
        let body: serde_json::Value = r.json().await.unwrap();
        assert_eq!(body["required_scope"], "kv:write");

        // super (wildcard) token on the same write route → passes scope gating.
        let r = client.post(format!("{base}/gateway/kv"))
            .header(AUTHORIZATION, "Bearer super")
            .json(&serde_json::json!({"key": "k", "value": "v"}))
            .send().await.unwrap();
        assert_ne!(r.status(), 401, "wildcard token must authenticate");
        assert_ne!(r.status(), 403, "wildcard token must pass scope gating");

        agent.shutdown().await;
    }

    /// WS4: an OIDC JWT from a (mock) IdP is validated at the gateway and its
    /// groups are mapped to scopes — a `readers`-group token reaches a `kv:read`
    /// route but is forbidden on a `kv:write` route; no token is 401.
    #[cfg(feature = "compliance")]
    #[tokio::test]
    async fn test_gateway_oidc_jwt_maps_groups_to_scopes() {
        use axum::{routing::get, Router};
        use axum::http::header::AUTHORIZATION;
        use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
        use std::time::{SystemTime, UNIX_EPOCH};

        const TEST_PRIV: &str = include_str!("../../tests/fixtures/oidc_test.key");
        let jwks_body = include_str!("../../tests/fixtures/oidc_jwks.json");

        // ── Mock IdP: discovery + JWKS ───────────────────────────────────────
        let idp_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let idp_port = idp_listener.local_addr().unwrap().port();
        let issuer = format!("http://127.0.0.1:{idp_port}");
        let disco = serde_json::json!({
            "issuer": issuer,
            "jwks_uri": format!("{issuer}/jwks"),
        }).to_string();
        let idp = Router::new()
            .route("/.well-known/openid-configuration", get(move || {
                let disco = disco.clone();
                async move { ([("content-type", "application/json")], disco) }
            }))
            .route("/jwks", get(move || {
                async move { ([("content-type", "application/json")], jwks_body) }
            }));
        let _idp = tokio::spawn(async move { axum::serve(idp_listener, idp).await.unwrap(); });

        // ── Mycelium node with OIDC configured ───────────────────────────────
        let gossip_port = alloc_port();
        let http_port   = alloc_port();
        let id = NodeId::new("127.0.0.1", gossip_port).unwrap();
        let mut group_scopes = std::collections::HashMap::new();
        group_scopes.insert("readers".to_string(), vec!["kv:read".to_string()]);
        let mut cfg = GossipConfig::default();
        cfg.bind_port = gossip_port;
        cfg.http_port = Some(http_port);
        cfg.oidc = Some(crate::OidcConfig {
            issuer: issuer.clone(),
            audience: "mycelium-cluster".into(),
            group_claim: "groups".into(),
            group_scopes,
            jwks_uri: None, // exercise discovery
        });
        let agent = Arc::new(GossipAgent::new(id, cfg));
        agent.start().await.unwrap();
        tokio::time::sleep(Duration::from_millis(80)).await;

        // ── Mint a JWT for a "readers" user ──────────────────────────────────
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
        let claims = serde_json::json!({
            "sub": "alice", "iss": issuer, "aud": "mycelium-cluster",
            "exp": now + 3600, "groups": ["readers"],
        });
        let mut header = Header::new(Algorithm::RS256);
        header.kid = Some("test-kid".to_string());
        let jwt = encode(&header, &claims, &EncodingKey::from_rsa_pem(TEST_PRIV.as_bytes()).unwrap()).unwrap();

        let client = reqwest::Client::new();
        let base = format!("http://127.0.0.1:{http_port}");

        // No token → 401.
        let r = client.get(format!("{base}/gateway/kv/keys")).send().await.unwrap();
        assert_eq!(r.status(), 401, "no token must be unauthorized");

        // OIDC JWT with kv:read → admitted on the kv:read route.
        let r = client.get(format!("{base}/gateway/kv/keys"))
            .header(AUTHORIZATION, format!("Bearer {jwt}")).send().await.unwrap();
        assert_eq!(r.status(), 200, "readers JWT must reach kv/keys (kv:read)");

        // Same JWT on a kv:write route → 403 (group grants only kv:read).
        let r = client.post(format!("{base}/gateway/kv"))
            .header(AUTHORIZATION, format!("Bearer {jwt}"))
            .json(&serde_json::json!({"key":"k","value":"v"}))
            .send().await.unwrap();
        assert_eq!(r.status(), 403, "readers JWT must be forbidden on kv:write");

        agent.shutdown().await;
    }

    /// WS2: the `/gateway/audit` endpoint returns the node's verified audit
    /// stream to a token holding `audit:read`, and 403s a token without it.
    #[cfg(feature = "compliance")]
    #[tokio::test]
    async fn test_gateway_audit_endpoint_verifies_and_scope_gates() {
        use crate::config::TlsConfig;
        use crate::{AuditAction, AuditOutcome, GatewayToken};
        use axum::http::header::AUTHORIZATION;

        let gossip_port = alloc_port();
        let http_port   = alloc_port();
        let id = NodeId::new("127.0.0.1", gossip_port).unwrap();
        let cert_dir = std::env::temp_dir().join(format!("myc-audit-ep-{gossip_port}"));
        let _ = std::fs::remove_dir_all(&cert_dir);

        let mut cfg = GossipConfig::default();
        cfg.bind_port = gossip_port;
        cfg.http_port = Some(http_port);
        cfg.tls = Some(TlsConfig { auto_cert_dir: cert_dir.clone(), ..TlsConfig::default() });
        cfg.gateway_scoped_tokens = vec![
            GatewayToken { token: "auditor".into(), scopes: vec!["audit:read".into()] },
            GatewayToken { token: "noaudit".into(), scopes: vec!["kv:read".into()] },
        ];

        let agent = Arc::new(GossipAgent::new(id.clone(), cfg));
        agent.start().await.unwrap();
        tokio::time::sleep(Duration::from_millis(80)).await;

        // Seal two events into this node's stream.
        agent.audit(AuditAction::Invoke, "10.0.0.1:9000", "skill/a", AuditOutcome::Success, None).unwrap();
        agent.audit(AuditAction::Read, "10.0.0.2:9000", "kv/secret", AuditOutcome::Denied, None).unwrap();

        let client = reqwest::Client::new();
        let url = format!("http://127.0.0.1:{http_port}/gateway/audit");

        // Wrong scope → 403.
        let r = client.get(&url).header(AUTHORIZATION, "Bearer noaudit").send().await.unwrap();
        assert_eq!(r.status(), 403, "kv:read token must not reach the audit trail");

        // Correct scope → 200, verified stream with both records.
        let r = client.get(&url).header(AUTHORIZATION, "Bearer auditor").send().await.unwrap();
        assert_eq!(r.status(), 200, "audit:read token must reach the audit trail");
        let body: serde_json::Value = r.json().await.unwrap();
        let streams = body["streams"].as_array().expect("streams array");
        let mine = streams.iter()
            .find(|s| s["node"] == id.to_string())
            .expect("this node's stream present");
        assert_eq!(mine["verified"], true, "honest stream must verify");
        assert!(mine["count"].as_u64().unwrap() >= 2, "both sealed records counted");
        assert!(mine["head_hash"].is_string(), "chain tip hash present");
        let recs = mine["records"].as_array().unwrap();
        assert!(recs.iter().all(|r| r["content_hash"].is_string()),
            "every record carries a citable content_hash");

        agent.shutdown().await;
        let _ = std::fs::remove_dir_all(&cert_dir);
    }
}
