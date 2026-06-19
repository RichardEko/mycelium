//! Falsification probes (mycelium-analysis Run 26, M2). Adversarial inputs against the WS-F
//! self-certification surface: `verify()` on hostile/malformed bytes must return `false`, never
//! panic, and never accept a key/signature substitution. Kept as a permanent regression test.

use mycelium_agentfacts::{read_verified_fields, SignedField};
use serde_json::json;

/// `SignedField::verify` must reject malformed signatures and keys without panicking.
#[test]
fn signed_field_verify_survives_hostile_inputs() {
    // Wrong-length signature (not 64 bytes) → false, no panic.
    let f = SignedField {
        field: "x".into(),
        value: json!("v"),
        issued_at_ms: 1,
        signature: vec![0u8; 7],
    };
    assert!(!f.verify(&[0u8; 32]), "short signature rejected");

    // Empty signature → false.
    let f0 = SignedField { signature: vec![], ..f.clone() };
    assert!(!f0.verify(&[0u8; 32]), "empty signature rejected");

    // Correct-length but all-zero signature against an all-zero (and a random) key → false.
    let fz = SignedField { signature: vec![0u8; 64], ..f.clone() };
    assert!(!fz.verify(&[0u8; 32]), "zero signature against zero key rejected");
    assert!(!fz.verify(&[7u8; 32]), "zero signature against arbitrary key rejected");

    // Oversized signature (65 bytes) → false (try_into to [u8;64] fails cleanly).
    let fo = SignedField { signature: vec![1u8; 65], ..f };
    assert!(!fo.verify(&[1u8; 32]), "oversized signature rejected");
}

/// `read_verified_fields` for an unknown node (no learned identity key) yields an empty map —
/// never a panic, never an unverified field. Runs without a live agent only insofar as the API
/// is total; here we just assert the no-identity branch is safe by constructing a throwaway agent.
#[tokio::test]
async fn read_verified_fields_for_unknown_node_is_empty_not_panic() {
    use mycelium::{GossipAgent, GossipConfig, NodeId};
    use std::sync::Arc;
    use std::time::Duration;

    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let cert_dir = std::env::temp_dir().join(format!("myc-fals26-{port}"));
    let _ = std::fs::remove_dir_all(&cert_dir);
    let cfg = GossipConfig {
        bind_port: port,
        tls: Some(mycelium::config::TlsConfig {
            auto_cert_dir: cert_dir.clone(),
            ..mycelium::config::TlsConfig::default()
        }),
        ..Default::default()
    };
    let agent = Arc::new(GossipAgent::new(NodeId::new("127.0.0.1", port).unwrap(), cfg));
    agent.start().await.unwrap();

    // A node we have never heard of: no identity key learned ⇒ empty map, no panic.
    let fields = read_verified_fields(&agent, "10.255.255.1:65000", 60_000);
    assert!(fields.is_empty(), "unknown-node fields are empty, not forged-through");

    agent.shutdown_with_timeout(Duration::from_secs(5)).await;
    let _ = std::fs::remove_dir_all(&cert_dir);
}
