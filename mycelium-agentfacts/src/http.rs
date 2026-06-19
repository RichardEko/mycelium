//! The **edge endpoint** — M16-A's serve half. Mounts a public, un-gated
//! `GET /.well-known/agent-facts.json` on the agent's embedded gateway that returns the freshly
//! built, self-signed [`SignedFacts`](crate::SignedFacts) document. The NANDA-style quilt **pulls**
//! it at the domain boundary; the `Cache-Control: max-age` realises the TTL-scoped `facts_url`.
//!
//! Deliberately **outside the `/gateway` scope wall** — AgentFacts are meant to be *publicly
//! fetchable and cryptographically verified*, never token-gated (ROADMAP §16 precursor criterion).
//!
//! ```rust,ignore
//! let router = mycelium_agentfacts::agent_facts_router(Arc::clone(&agent), opts);
//! agent.with_http_routes(router);   // must be registered before start
//! agent.start().await?;
//! // GET http://<node>/.well-known/agent-facts.json
//! ```

use std::sync::Arc;

use axum::extract::State;
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use mycelium::GossipAgent;

use crate::{signed_agent_facts, FactsOptions};

#[derive(Clone)]
struct FactsState {
    agent: Arc<GossipAgent>,
    opts:  Arc<FactsOptions>,
}

/// Build the AgentFacts edge router (run-dark: nothing is published until the operator mounts this
/// via [`GossipAgent::with_http_routes`] and starts the node). The route is public and un-gated.
pub fn agent_facts_router(agent: Arc<GossipAgent>, opts: FactsOptions) -> axum::Router {
    axum::Router::new()
        // PULL of this node's fresh-built, whole-doc-signed facts (M16-A).
        .route("/.well-known/agent-facts.json", get(serve_facts))
        // PULL of the converged, multi-author CRDT board — every node's per-field-signed facts as
        // they've gossiped intra-domain (M16-B), each field independently verifiable. Ties PUSH→PULL.
        .route("/.well-known/agent-facts/domain.json", get(serve_domain))
        .with_state(FactsState { agent, opts: Arc::new(opts) })
}

async fn serve_domain(State(s): State<FactsState>) -> Response {
    let nodes = crate::domain_facts(&s.agent, s.opts.ttl_secs.saturating_mul(1000));
    (
        [
            (header::CONTENT_TYPE, "application/json".to_string()),
            (header::CACHE_CONTROL, format!("public, max-age={}", s.opts.ttl_secs)),
        ],
        serde_json::json!({ "nodes": nodes }).to_string(),
    )
        .into_response()
}

async fn serve_facts(State(s): State<FactsState>) -> Response {
    match signed_agent_facts(&s.agent, &s.opts) {
        Some(facts) => (
            [
                (header::CONTENT_TYPE, "application/ld+json".to_string()),
                (header::CACHE_CONTROL, format!("public, max-age={}", s.opts.ttl_secs)),
            ],
            facts.to_json(),
        )
            .into_response(),
        // No tls identity ⇒ nothing to self-certify. 503 (not 500): the node is up but cannot emit.
        None => (StatusCode::SERVICE_UNAVAILABLE, "agent-facts require a tls node identity").into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mycelium::{Capability, GossipConfig, NodeId};
    use std::time::Duration;

    fn alloc_port() -> u16 {
        std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
    }

    #[tokio::test]
    async fn edge_endpoint_serves_a_verifiable_signed_document() {
        let gossip_port = alloc_port();
        let http_port = alloc_port();
        let id = NodeId::new("127.0.0.1", gossip_port).unwrap();
        let cert_dir = std::env::temp_dir().join(format!("myc-af-http-{gossip_port}"));
        let _ = std::fs::remove_dir_all(&cert_dir);
        let cfg = GossipConfig {
            bind_port: gossip_port,
            http_port: Some(http_port),
            tls: Some(mycelium::config::TlsConfig {
                auto_cert_dir: cert_dir.clone(),
                ..mycelium::config::TlsConfig::default()
            }),
            ..Default::default()
        };
        let agent = Arc::new(GossipAgent::new(id, cfg));
        let _reg = agent.capabilities().advertise_capability(
            Capability::new("nlp", "summarize"),
            Duration::from_secs(5),
        );
        let opts = FactsOptions { ttl_secs: 120, ..Default::default() };
        agent.with_http_routes(agent_facts_router(Arc::clone(&agent), opts));
        agent.start().await.unwrap();

        let url = format!("http://127.0.0.1:{http_port}/.well-known/agent-facts.json");
        let client = reqwest::Client::builder().timeout(Duration::from_millis(500)).build().unwrap();

        // Poll the (public, un-gated) endpoint until it serves the doc with the advertised cap.
        let mut body: Option<serde_json::Value> = None;
        for _ in 0..100 {
            if let Ok(r) = client.get(&url).send().await
                && r.status() == 200
            {
                let v: serde_json::Value = r.json().await.unwrap();
                if v["document"]["capabilities"].as_array().map(|a| !a.is_empty()).unwrap_or(false) {
                    body = Some(v);
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let body = body.expect("edge endpoint serves facts with the advertised capability");

        // Reconstruct SignedFacts from the served JSON and verify the self-signature.
        let signed = crate::SignedFacts {
            document:       body["document"].clone(),
            alg:            "ed25519",
            public_key_b64: body["public_key_b64"].as_str().unwrap().to_string(),
            signature_b64:  body["signature_b64"].as_str().unwrap().to_string(),
        };
        assert!(signed.verify(), "the publicly-served document verifies");
        assert_eq!(signed.document["certification"]["scheme"], "self-certified");

        agent.shutdown_with_timeout(Duration::from_secs(5)).await;
        let _ = std::fs::remove_dir_all(&cert_dir);
    }

    #[tokio::test]
    async fn edge_endpoint_serves_the_crdt_domain_board() {
        let gossip_port = alloc_port();
        let http_port = alloc_port();
        let id = NodeId::new("127.0.0.1", gossip_port).unwrap();
        let cert_dir = std::env::temp_dir().join(format!("myc-af-dom-{gossip_port}"));
        let _ = std::fs::remove_dir_all(&cert_dir);
        let cfg = GossipConfig {
            bind_port: gossip_port,
            http_port: Some(http_port),
            tls: Some(mycelium::config::TlsConfig {
                auto_cert_dir: cert_dir.clone(),
                ..mycelium::config::TlsConfig::default()
            }),
            ..Default::default()
        };
        let agent = Arc::new(GossipAgent::new(id, cfg));
        agent.with_http_routes(agent_facts_router(Arc::clone(&agent), FactsOptions { ttl_secs: 120, ..Default::default() }));
        agent.start().await.unwrap();

        // Publish per-field CRDT facts (M16-B); they should surface on the domain board at the edge.
        assert!(crate::publish_field(&agent, "status", serde_json::json!("ready")));

        let url = format!("http://127.0.0.1:{http_port}/.well-known/agent-facts/domain.json");
        let client = reqwest::Client::builder().timeout(Duration::from_millis(500)).build().unwrap();
        let me = agent.node_id().to_string();
        let mut board = None;
        for _ in 0..100 {
            if let Ok(r) = client.get(&url).send().await
                && r.status() == 200
            {
                let v: serde_json::Value = r.json().await.unwrap();
                let has = v["nodes"].as_array().map(|ns| {
                    ns.iter().any(|n| n["node"] == me
                        && n["fields"].as_array().map(|f| !f.is_empty()).unwrap_or(false))
                }).unwrap_or(false);
                if has {
                    board = Some(v);
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let board = board.expect("domain board serves this node's published fields");
        let node = board["nodes"].as_array().unwrap().iter().find(|n| n["node"] == me).unwrap();
        assert!(node["public_key_b64"].as_str().is_some(), "board carries the node's identity key");
        assert!(
            node["fields"].as_array().unwrap().iter().any(|f| f["field"] == "status"),
            "the published field is on the board"
        );

        agent.shutdown_with_timeout(Duration::from_secs(5)).await;
        let _ = std::fs::remove_dir_all(&cert_dir);
    }
}
