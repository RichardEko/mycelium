//! M16-B — the **CRDT AgentFacts update layer** (intra-domain; PUSH).
//!
//! The NANDA abstract names a *"CRDT-based update protocol"* its v0.3 body does not deliver: it
//! falls back to whole-document VC + host-at-URL + TTL re-fetch, because **whole-document signing
//! is in tension with field-level merge** — you cannot merge two independently-signed documents
//! field-by-field and preserve either signature.
//!
//! Mycelium's substrate *is* that missing protocol. Each AgentFacts **field** is an independently
//! **node-signed** KV entry at `facts/{node}/{field}`; **LWW + HLC + anti-entropy** is the
//! convergent, concurrent-safe merge — concurrent edits to *different* fields both survive (distinct
//! keys), same-field edits LWW by HLC, late joiners catch up via anti-entropy, freshness is the
//! evaporation convention. Per-**entry** signatures are exactly the precondition that makes
//! field-level merge possible — so Mycelium is *better-suited* to a CRDT AgentFacts than NANDA's
//! own whole-doc VC. **Push intra-domain (here); pull at the edge** (the M16-A endpoint).

use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

use mycelium::GossipAgent;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

/// KV prefix owned by the per-field AgentFacts CRDT layer: `facts/{node}/{field}`.
pub const FACTS_PREFIX: &str = "facts/";

fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0)
}

fn field_key(node: &str, field: &str) -> String {
    format!("{FACTS_PREFIX}{node}/{field}")
}

/// One independently-signed AgentFacts field — the unit of CRDT merge. The signature binds the
/// `(field, value, issued_at)` tuple under the node identity, so a reader can verify each field on
/// its own (no whole-document signature to break under field-level merge).
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct SignedField {
    pub field:        String,
    pub value:        Value,
    pub issued_at_ms: u64,
    /// 64-byte Ed25519 signature over [`canonical`](Self::canonical) by the node identity.
    pub signature:    Vec<u8>,
}

impl SignedField {
    /// Canonical bytes signed/verified — deterministic (sorted-key) JSON of the tuple.
    fn canonical(field: &str, value: &Value, issued_at_ms: u64) -> Vec<u8> {
        serde_json::to_vec(&json!({ "f": field, "t": issued_at_ms, "v": value })).unwrap_or_default()
    }

    /// Verify the field's signature against `pubkey` (the node's identity verifying key).
    pub fn verify(&self, pubkey: &[u8; 32]) -> bool {
        use ed25519_dalek::{Signature, Verifier, VerifyingKey};
        let Ok(sig) = <[u8; 64]>::try_from(self.signature.as_slice()) else { return false };
        let Ok(vk) = VerifyingKey::from_bytes(pubkey) else { return false };
        vk.verify(&Self::canonical(&self.field, &self.value, self.issued_at_ms), &Signature::from_bytes(&sig))
            .is_ok()
    }

    /// Verify against any key in a node's retained set (current key + rotation history). `true` if
    /// the signature checks out under *some* key the node has published — so a field signed before
    /// an identity rotation still verifies (the WS5 retained-key-set posture). Empty set ⇒ `false`.
    pub fn verify_any(&self, pubkeys: &[[u8; 32]]) -> bool {
        pubkeys.iter().any(|pk| self.verify(pk))
    }

    fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(self).unwrap_or_default()
    }

    fn decode(bytes: &[u8]) -> Option<Self> {
        serde_json::from_slice(bytes).ok()
    }
}

/// Publish (or update) one self-signed AgentFacts `field` for **this** node. Writes
/// `facts/{self}/{field}` — gossiped like any KV value, LWW-merged by HLC. `false` if the node has
/// no `tls` identity to sign with. Updating the same field again supersedes it (newest HLC wins);
/// distinct fields never conflict.
pub fn publish_field(agent: &GossipAgent, field: &str, value: Value) -> bool {
    let issued_at_ms = now_ms();
    let canonical = SignedField::canonical(field, &value, issued_at_ms);
    let Some(sig) = agent.sign_with_identity(&canonical) else { return false };
    let sf = SignedField { field: field.to_string(), value, issued_at_ms, signature: sig.to_vec() };
    agent.kv().set(field_key(&agent.node_id().to_string(), field), sf.encode())
}

/// This node's view of `node`'s verifying-key **history**, read from the gossiped
/// `sys/identity/{node}` entry: a concatenation of 32-byte keys (`32 × N`) — current key first,
/// retained priors after (WS5 multi-key archival, same layout `helpers::parse_identity_keys`
/// produces). Empty if not yet learned or malformed (empty / non-multiple-of-32). Every verify
/// path tries the whole set so a field signed before an identity rotation still verifies — the
/// retained-key-set posture the connection/consensus/rbac/audit paths already use.
fn peer_identity_keys(agent: &GossipAgent, node: &str) -> Vec<[u8; 32]> {
    let Some(bytes) = agent.kv().get(&format!("sys/identity/{node}")) else { return Vec::new() };
    if bytes.is_empty() || !bytes.len().is_multiple_of(32) {
        return Vec::new();
    }
    bytes
        .chunks_exact(32)
        .map(|c| {
            let mut a = [0u8; 32];
            a.copy_from_slice(c);
            a
        })
        .collect()
}

/// Read and **verify** `node`'s published AgentFacts fields from the local gossip view, returning
/// the merged `{field: value}` map. Drops any field whose signature does not verify against the
/// node's identity key, or that is staler than `ttl_ms` (the evaporation/freshness convention) —
/// so a forged `facts/{node}/…` write (LWW-accepted by the substrate, detection-not-prevention)
/// reads back as absent. `None`-key (identity not yet learned) yields an empty map.
pub fn read_verified_fields(agent: &GossipAgent, node: &str, ttl_ms: u64) -> BTreeMap<String, Value> {
    let mut out = BTreeMap::new();
    let keys = peer_identity_keys(agent, node);
    if keys.is_empty() {
        return out;
    }
    let now = now_ms();
    for (_key, bytes) in agent.kv().scan_prefix(&format!("{FACTS_PREFIX}{node}/")) {
        let Some(sf) = SignedField::decode(&bytes) else { continue };
        if now.saturating_sub(sf.issued_at_ms) > ttl_ms {
            continue; // stale
        }
        if sf.verify_any(&keys) {
            out.insert(sf.field.clone(), sf.value);
        }
    }
    out
}

/// One node's slice of the domain-wide CRDT facts board: its identity key plus its **verified**,
/// fresh signed fields. A puller re-verifies each field against `public_key_b64`.
#[derive(Clone, Debug, Serialize)]
pub struct NodeFacts {
    pub node:           String,
    pub public_key_b64: String,
    pub fields:         Vec<SignedField>,
}

/// Assemble the **domain-wide** CRDT facts board from this node's local gossip view: every node
/// that has published facts (`facts/{node}/…`), with its identity key and its verified, fresh
/// fields. This is the converged, multi-author view to serve at the edge — the quilt pulls one URL
/// and gets the whole patch, each field independently verifiable. Nodes whose identity key isn't
/// yet known locally, and forged/stale fields, are dropped.
pub fn domain_facts(agent: &GossipAgent, ttl_ms: u64) -> Vec<NodeFacts> {
    use base64::Engine as _;
    let now = now_ms();
    let mut by_node: BTreeMap<String, Vec<SignedField>> = BTreeMap::new();
    for (key, bytes) in agent.kv().scan_prefix(FACTS_PREFIX) {
        let Some(rest) = key.strip_prefix(FACTS_PREFIX) else { continue };
        let Some((node, _field)) = rest.split_once('/') else { continue };
        let Some(sf) = SignedField::decode(&bytes) else { continue };
        if now.saturating_sub(sf.issued_at_ms) > ttl_ms {
            continue;
        }
        by_node.entry(node.to_string()).or_default().push(sf);
    }
    let mut out = Vec::new();
    for (node, fields) in by_node {
        let keys = peer_identity_keys(agent, &node);
        // The published key is the node's *current* identity (first in the history); fields are
        // verified against the whole retained set so pre-rotation signatures still pass.
        let Some(current) = keys.first().copied() else { continue };
        let verified: Vec<SignedField> = fields.into_iter().filter(|sf| sf.verify_any(&keys)).collect();
        if verified.is_empty() {
            continue;
        }
        out.push(NodeFacts {
            node,
            public_key_b64: base64::engine::general_purpose::STANDARD.encode(current),
            fields: verified,
        });
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use mycelium::{GossipAgent, GossipConfig, NodeId};
    use std::sync::Arc;
    use std::time::Duration;

    fn alloc_port() -> u16 {
        std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
    }

    // Nodes that must peer over mTLS share one `cert_dir` (a shared auto-CA — separate dirs give
    // separate CAs and the handshake fails).
    async fn tls_agent(bootstrap: Option<u16>, cert_dir: &std::path::Path) -> (Arc<GossipAgent>, u16) {
        let port = alloc_port();
        let id = NodeId::new("127.0.0.1", port).unwrap();
        let cfg = GossipConfig {
            bind_port: port,
            bootstrap_peers: bootstrap.map(|b| vec![NodeId::new("127.0.0.1", b).unwrap()]).unwrap_or_default(),
            health_check_interval_secs: 1,
            tls: Some(mycelium::config::TlsConfig {
                auto_cert_dir: cert_dir.to_path_buf(),
                ..mycelium::config::TlsConfig::default()
            }),
            ..Default::default()
        };
        let agent = Arc::new(GossipAgent::new(id, cfg));
        agent.start().await.unwrap();
        (agent, port)
    }

    async fn wait_identity(agent: &GossipAgent, node: &str) {
        for _ in 0..100 {
            if agent.kv().get(&format!("sys/identity/{node}")).is_some() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("identity never gossiped");
    }

    #[tokio::test]
    async fn per_field_merge_lww_and_forgery_rejection() {
        let cert_dir = std::env::temp_dir().join(format!("myc-crdt-1-{}", alloc_port()));
        let _ = std::fs::remove_dir_all(&cert_dir);
        let (agent, _p) = tls_agent(None, &cert_dir).await;
        let me = agent.node_id().to_string();
        wait_identity(&agent, &me).await;

        // Two distinct fields both survive (different keys — no conflict).
        assert!(publish_field(&agent, "status", json!("ready")));
        assert!(publish_field(&agent, "region", json!("eu-west")));
        let fields = read_verified_fields(&agent, &me, 60_000);
        assert_eq!(fields.get("status"), Some(&json!("ready")));
        assert_eq!(fields.get("region"), Some(&json!("eu-west")));

        // Same-field update supersedes (LWW newest wins).
        tokio::time::sleep(Duration::from_millis(2)).await;
        assert!(publish_field(&agent, "status", json!("draining")));
        assert_eq!(read_verified_fields(&agent, &me, 60_000).get("status"), Some(&json!("draining")));

        // A forged field (direct KV write, no valid signature) is LWW-accepted by the substrate
        // (detection-not-prevention) but read_verified_fields drops it.
        let forged = SignedField {
            field: "status".into(),
            value: json!("HACKED"),
            issued_at_ms: now_ms() + 1_000_000, // even "newer"
            signature: vec![0u8; 64],
        };
        let _ = agent.kv().set(field_key(&me, "status"), forged.encode());
        assert_ne!(
            read_verified_fields(&agent, &me, 60_000).get("status"),
            Some(&json!("HACKED")),
            "a forged field never reads back as verified"
        );

        agent.shutdown_with_timeout(Duration::from_secs(5)).await;
        let _ = std::fs::remove_dir_all(&cert_dir);
    }

    #[tokio::test]
    async fn intra_domain_field_gossips_and_verifies_cross_node() {
        // A publishes a field; B reads + verifies it after intra-domain gossip — the PUSH path.
        // Both share a cert_dir so mTLS peering succeeds (shared auto-CA).
        let cert_dir = std::env::temp_dir().join(format!("myc-crdt-2-{}", alloc_port()));
        let _ = std::fs::remove_dir_all(&cert_dir);
        let (a, a_port) = tls_agent(None, &cert_dir).await;
        let (b, _b_port) = tls_agent(Some(a_port), &cert_dir).await;
        let a_id = a.node_id().to_string();

        assert!(publish_field(&a, "model", json!("acme-1")));

        // B catches up via gossip/anti-entropy: A's identity + A's facts field.
        let mut seen = None;
        for _ in 0..200 {
            let fields = read_verified_fields(&b, &a_id, 120_000);
            if let Some(v) = fields.get("model") {
                seen = Some(v.clone());
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        assert_eq!(seen, Some(json!("acme-1")), "B verifies A's field cross-node");

        a.shutdown_with_timeout(Duration::from_secs(5)).await;
        b.shutdown_with_timeout(Duration::from_secs(5)).await;
        let _ = std::fs::remove_dir_all(&cert_dir);
    }

    #[tokio::test]
    async fn field_signed_before_rotation_still_verifies_after_rotation() {
        // The retained-key-set gap (Run-26 fix): a field signed by the OLD identity key must still
        // read back as verified after the node rotates its identity, because `sys/identity/{node}`
        // retains `new ‖ old` and verification tries the whole set. Without the fix, only the
        // current key is tried and the pre-rotation field silently vanishes from the board.
        let cert_dir = std::env::temp_dir().join(format!("myc-crdt-rot-{}", alloc_port()));
        let _ = std::fs::remove_dir_all(&cert_dir);
        let (agent, _p) = tls_agent(None, &cert_dir).await;
        let me = agent.node_id().to_string();
        wait_identity(&agent, &me).await;

        // Publish a field under the original identity key.
        assert!(publish_field(&agent, "model", json!("acme-1")));
        assert_eq!(
            read_verified_fields(&agent, &me, 120_000).get("model"),
            Some(&json!("acme-1")),
            "field verifies under the original key"
        );
        let old_key = agent.identity_public_key().unwrap();

        // Rotate the identity. `sys/identity/{me}` becomes `new ‖ old`; the field is NOT republished.
        let new_key = agent
            .rotate_identity(Duration::from_millis(50))
            .await
            .expect("rotation succeeds");
        assert_ne!(new_key, old_key, "rotation produced a fresh key");

        // The history entry now carries both keys (current first).
        let keys = peer_identity_keys(&agent, &me);
        assert!(keys.first() == Some(&new_key) && keys.contains(&old_key), "new‖old retained");

        // The pre-rotation field (still signed by the old key) must still verify against the set.
        assert_eq!(
            read_verified_fields(&agent, &me, 120_000).get("model"),
            Some(&json!("acme-1")),
            "old-key-signed field survives the rotation via the retained key set"
        );
        // And it appears on the assembled board, advertised under the *current* key.
        let board = domain_facts(&agent, 120_000);
        let entry = board.iter().find(|n| n.node == me).expect("node on the board");
        use base64::Engine as _;
        let advertised: [u8; 32] = base64::engine::general_purpose::STANDARD
            .decode(&entry.public_key_b64).unwrap().try_into().unwrap();
        assert_eq!(advertised, new_key, "board advertises the current key");
        assert!(entry.fields.iter().any(|f| f.field == "model"), "field present on the board");

        agent.shutdown_with_timeout(Duration::from_secs(5)).await;
        let _ = std::fs::remove_dir_all(&cert_dir);
    }

    #[tokio::test]
    async fn domain_facts_assembles_verified_per_node_board() {
        let cert_dir = std::env::temp_dir().join(format!("myc-crdt-3-{}", alloc_port()));
        let _ = std::fs::remove_dir_all(&cert_dir);
        let (agent, _p) = tls_agent(None, &cert_dir).await;
        let me = agent.node_id().to_string();
        wait_identity(&agent, &me).await;

        assert!(publish_field(&agent, "model", json!("acme-1")));
        assert!(publish_field(&agent, "region", json!("eu")));
        // A forged field must not appear on the board.
        let forged = SignedField {
            field: "evil".into(), value: json!("x"),
            issued_at_ms: now_ms(), signature: vec![0u8; 64],
        };
        let _ = agent.kv().set(field_key(&me, "evil"), forged.encode());

        let board = domain_facts(&agent, 60_000);
        let entry = board.iter().find(|n| n.node == me).expect("this node on the board");
        let names: Vec<&str> = entry.fields.iter().map(|f| f.field.as_str()).collect();
        assert!(names.contains(&"model") && names.contains(&"region"), "verified fields present");
        assert!(!names.contains(&"evil"), "forged field excluded from the board");
        // Each served field verifies against the published key.
        use base64::Engine as _;
        let pk: [u8; 32] = base64::engine::general_purpose::STANDARD
            .decode(&entry.public_key_b64).unwrap().try_into().unwrap();
        assert!(entry.fields.iter().all(|f| f.verify(&pk)), "every board field verifies");

        agent.shutdown_with_timeout(Duration::from_secs(5)).await;
        let _ = std::fs::remove_dir_all(&cert_dir);
    }
}
