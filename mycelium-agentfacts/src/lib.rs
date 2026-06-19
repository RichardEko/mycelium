//! `mycelium-agentfacts` — WS-F **M16-A**: self-certified AgentFacts emission for a Mycelium
//! domain, the "sovereign patch" a NANDA-style discovery quilt **pulls** at the edge.
//!
//! A domain self-elects to publish (run-dark by default); a [`SignedFacts`] document is built from
//! **live substrate state** (this node's capabilities, locality, identity) and **self-signed by
//! the node Ed25519 identity** — no issuer/TRS authority (Core Principle 1; trust is the fetcher's
//! to decide). It is a superset of the A2A Agent Card Mycelium already serves.
//!
//! ## Decoupled from NANDA's churning field names (the load-bearing rule)
//!
//! [`AgentFacts`] is a **stable, substrate-shaped** struct (our names: capabilities, locality,
//! identity, ttl). The NANDA JSON-LD mapping lives in **one place** — [`to_nanda_jsonld`]. When the
//! spec renames a field (it is a moving v0.3 RFC; AgentFacts may even become "Agent Metadata
//! Layer"), only that serializer changes; the substrate-derived core never does.
//!
//! Built **entirely on Mycelium's public API** (companion-crate contract, same as
//! `mycelium-tuple-space` / `mycelium-wasm-host`).

use std::time::{SystemTime, UNIX_EPOCH};

use base64::Engine as _;
use mycelium::{Capability, GossipAgent};
use serde::Serialize;
use serde_json::{json, Value};

mod crdt;
mod http;
pub use crdt::{publish_field, read_verified_fields, SignedField, FACTS_PREFIX};
pub use http::agent_facts_router;

fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0)
}

fn b64() -> base64::engine::general_purpose::GeneralPurpose {
    base64::engine::general_purpose::STANDARD
}

/// One capability this domain advertises (a skill the quilt can discover).
#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct CapabilityFact {
    pub namespace:     String,
    pub name:          String,
    pub schema_id:     Option<String>,
    pub input_schema:  Option<String>,
    pub output_schema: Option<String>,
}

/// Stable, **substrate-shaped** agent facts (deliberately NOT NANDA field names — see crate docs).
#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct AgentFacts {
    pub node_id:         String,
    /// Ed25519 identity public key — the self-certification anchor.
    pub identity_pubkey: [u8; 32],
    pub capabilities:    Vec<CapabilityFact>,
    pub locality:        Option<String>,
    /// Edge endpoint URLs the quilt can reach this domain at.
    pub endpoints:       Vec<String>,
    /// Endpoint freshness (seconds) — the quilt re-pulls after this (`facts_url` TTL).
    pub ttl_secs:        u64,
    pub issued_at_ms:    u64,
}

/// Operator-supplied facets the substrate doesn't itself know: the public edge URLs, an optional
/// jurisdiction/locality string, and the publish TTL.
#[derive(Clone, Debug, Default)]
pub struct FactsOptions {
    pub endpoints: Vec<String>,
    pub locality:  Option<String>,
    pub ttl_secs:  u64,
}

impl AgentFacts {
    /// Build from a node's live substrate state (public API only). `None` if the node has no `tls`
    /// identity — AgentFacts are self-certified and require one. `capabilities` are this node's own
    /// advertised `cap/{self}/…` entries (entries that don't decode as a [`Capability`], e.g.
    /// locality keys, are skipped); they are sorted for a deterministic document.
    pub fn from_agent(agent: &GossipAgent, opts: &FactsOptions) -> Option<Self> {
        let identity_pubkey = agent.identity_public_key()?;
        let node_id = agent.node_id().to_string();

        let mut capabilities = Vec::new();
        for (_key, bytes) in agent.kv().scan_prefix(&format!("cap/{node_id}/")) {
            let Some(cap) = Capability::decode(&bytes) else { continue };
            capabilities.push(CapabilityFact {
                namespace:     cap.namespace.to_string(),
                name:          cap.name.to_string(),
                schema_id:     cap.schema_id.as_ref().map(|s| s.to_string()),
                input_schema:  cap.input_schema.as_ref().map(|s| s.to_string()),
                output_schema: cap.output_schema.as_ref().map(|s| s.to_string()),
            });
        }
        capabilities.sort_by(|a, b| (&a.namespace, &a.name).cmp(&(&b.namespace, &b.name)));
        capabilities.dedup();

        Some(Self {
            node_id,
            identity_pubkey,
            capabilities,
            locality: opts.locality.clone(),
            endpoints: opts.endpoints.clone(),
            ttl_secs: opts.ttl_secs,
            issued_at_ms: now_ms(),
        })
    }
}

/// Map the stable [`AgentFacts`] to a NANDA-style AgentFacts JSON-LD document. **This is the only
/// place NANDA's (churning) field names appear** — a spec rename changes this function alone.
pub fn to_nanda_jsonld(facts: &AgentFacts) -> Value {
    let capabilities: Vec<Value> = facts
        .capabilities
        .iter()
        .map(|c| {
            let id = format!("{}/{}", c.namespace, c.name);
            let mut o = json!({ "id": id, "name": id });
            if let Some(s) = &c.schema_id {
                o["schemaId"] = json!(s);
            }
            if let Some(s) = &c.input_schema {
                o["inputSchema"] = serde_json::from_str(s).unwrap_or_else(|_| json!(s));
            }
            if let Some(s) = &c.output_schema {
                o["outputSchema"] = serde_json::from_str(s).unwrap_or_else(|_| json!(s));
            }
            o
        })
        .collect();

    let mut doc = json!({
        "@context":     "https://projectnanda.org/agentfacts/v0",
        "id":           format!("did:mycelium:{}", facts.node_id),
        "capabilities": capabilities,
        "endpoints":    { "static": facts.endpoints, "adaptive_resolver": facts.endpoints },
        "certification": {
            "scheme":    "self-certified",
            "alg":       "ed25519",
            "publicKey": b64().encode(facts.identity_pubkey),
        },
        "ttl":          facts.ttl_secs,
        "issuedAt":     facts.issued_at_ms,
    });
    if let Some(loc) = &facts.locality {
        doc["jurisdiction"] = json!(loc);
    }
    doc
}

/// A self-signed AgentFacts document ready to publish at the edge. The signature is Ed25519 over
/// the canonical JSON bytes of `document`, by the node identity; a fetcher verifies it against
/// `public_key_b64` (self-certified — trust is the fetcher's decision).
#[derive(Clone, Debug, Serialize)]
pub struct SignedFacts {
    pub document:       Value,
    pub alg:            &'static str,
    pub public_key_b64: String,
    pub signature_b64:  String,
}

/// Canonical bytes that are signed and verified: serde_json's deterministic (sorted-key)
/// serialization of the document. A fetcher re-serialises the `document` the same way to verify.
fn canonical(document: &Value) -> Vec<u8> {
    serde_json::to_vec(document).unwrap_or_default()
}

impl SignedFacts {
    /// Verify the self-signature: the signature is a valid Ed25519 signature over the canonical
    /// document bytes by the embedded public key. (Whether to *trust* that key is the caller's.)
    pub fn verify(&self) -> bool {
        use ed25519_dalek::{Signature, Verifier, VerifyingKey};
        let (Ok(pk), Ok(sig)) = (b64().decode(&self.public_key_b64), b64().decode(&self.signature_b64))
        else {
            return false;
        };
        let (Ok(pk), Ok(sig)) =
            (<[u8; 32]>::try_from(pk.as_slice()), <[u8; 64]>::try_from(sig.as_slice()))
        else {
            return false;
        };
        let Ok(vk) = VerifyingKey::from_bytes(&pk) else { return false };
        vk.verify(&canonical(&self.document), &Signature::from_bytes(&sig)).is_ok()
    }

    /// The document serialised as a JSON string (what an edge endpoint serves).
    pub fn to_json(&self) -> String {
        serde_json::to_string(self).unwrap_or_default()
    }
}

/// Build **and** self-sign this node's AgentFacts (the one-shot edge document). `None` if the node
/// has no `tls` identity. Run-dark: nothing is published until an operator calls this and serves
/// the result.
pub fn signed_agent_facts(agent: &GossipAgent, opts: &FactsOptions) -> Option<SignedFacts> {
    let facts = AgentFacts::from_agent(agent, opts)?;
    let document = to_nanda_jsonld(&facts);
    let signature = agent.sign_with_identity(&canonical(&document))?;
    let public_key = agent.identity_public_key()?;
    Some(SignedFacts {
        document,
        alg: "ed25519",
        public_key_b64: b64().encode(public_key),
        signature_b64: b64().encode(signature),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use mycelium::{Capability, GossipAgent, GossipConfig, NodeId};
    use std::sync::Arc;
    use std::time::Duration;

    fn alloc_port() -> u16 {
        std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
    }

    async fn tls_agent() -> (Arc<GossipAgent>, std::path::PathBuf) {
        let port = alloc_port();
        let id = NodeId::new("127.0.0.1", port).unwrap();
        let cert_dir = std::env::temp_dir().join(format!("myc-af-{port}"));
        let _ = std::fs::remove_dir_all(&cert_dir);
        let cfg = GossipConfig {
            bind_port: port,
            tls: Some(mycelium::config::TlsConfig {
                auto_cert_dir: cert_dir.clone(),
                ..mycelium::config::TlsConfig::default()
            }),
            ..Default::default()
        };
        let agent = Arc::new(GossipAgent::new(id, cfg));
        agent.start().await.unwrap();
        (agent, cert_dir)
    }

    #[tokio::test]
    async fn builds_signs_and_verifies_self_certified_facts() {
        let (agent, cert_dir) = tls_agent().await;

        // Advertise a capability so the facts have something to report.
        let _reg = agent.capabilities().advertise_capability(
            Capability::new("nlp", "summarize"),
            Duration::from_secs(5),
        );

        let opts = FactsOptions {
            endpoints: vec!["https://edge.example/.well-known/agent-facts.json".into()],
            locality: Some("eu-west".into()),
            ttl_secs: 300,
        };

        // Poll until the advertised cap is visible, then build the signed document.
        let mut signed = None;
        for _ in 0..100 {
            if let Some(s) = signed_agent_facts(&agent, &opts)
                && s.document["capabilities"].as_array().map(|a| !a.is_empty()).unwrap_or(false)
            {
                signed = Some(s);
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let signed = signed.expect("facts built with the advertised capability");

        // Self-signature verifies.
        assert!(signed.verify(), "self-signed facts verify against the embedded key");

        // The NANDA-shaped document carries the mapped fields.
        assert_eq!(signed.document["certification"]["scheme"], "self-certified");
        assert_eq!(signed.document["jurisdiction"], "eu-west");
        assert_eq!(signed.document["ttl"], 300);
        let caps = signed.document["capabilities"].as_array().unwrap();
        assert!(caps.iter().any(|c| c["id"] == "nlp/summarize"));

        // Tampering the document breaks the signature.
        let mut tampered = signed.clone();
        tampered.document["jurisdiction"] = json!("us-east");
        assert!(!tampered.verify(), "a tampered document fails verification");

        agent.shutdown_with_timeout(Duration::from_secs(5)).await;
        let _ = std::fs::remove_dir_all(&cert_dir);
    }

    #[test]
    fn stable_struct_is_decoupled_from_nanda_names() {
        // The mapping is the only place NANDA names appear; the stable struct uses substrate names.
        let facts = AgentFacts {
            node_id: "127.0.0.1:9000".into(),
            identity_pubkey: [3u8; 32],
            capabilities: vec![CapabilityFact {
                namespace: "vision".into(),
                name: "detect".into(),
                schema_id: Some("acme/v1".into()),
                input_schema: None,
                output_schema: None,
            }],
            locality: None,
            endpoints: vec!["https://x/".into()],
            ttl_secs: 60,
            issued_at_ms: 1,
        };
        let doc = to_nanda_jsonld(&facts);
        assert_eq!(doc["@context"], "https://projectnanda.org/agentfacts/v0");
        assert_eq!(doc["id"], "did:mycelium:127.0.0.1:9000");
        assert_eq!(doc["capabilities"][0]["id"], "vision/detect");
        assert_eq!(doc["capabilities"][0]["schemaId"], "acme/v1");
        assert!(doc.get("jurisdiction").is_none(), "absent locality omits the field");
    }
}
