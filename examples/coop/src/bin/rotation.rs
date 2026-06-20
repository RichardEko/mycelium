//! Example 06 — **zero-disruption identity rotation** (WS5 + retained-key verification).
//!
//! A depot rotates its Ed25519 identity mid-operation (routine hygiene). Its peers keep verifying
//! everything it signed — **including AgentFacts fields signed by the now-retired key** — across the
//! rotation, with no dropout. This is the runnable form of the retained-key-set fix: a node's
//! `sys/identity/{node}` entry retains `new ‖ old`, and every verify path tries the whole set.
//!
//!   • `depot-a` — publishes a CRDT AgentFacts field (`status`), then rotates its identity.
//!   • `depot-b` — a peer; reads + verifies A's field before the rotation, after the rotation (the
//!                 *old-key-signed* field still verifies), and a fresh field A signs with the new key.
//!
//! Run:  cargo run -p mycelium-coop-examples --bin rotation

use std::time::Duration;

use coop::common::{alloc_ports, spawn_depot, DepotOpts};
use mycelium_agentfacts::{publish_field, read_verified_fields};
use serde_json::json;

async fn wait_until(secs: u64, mut cond: impl FnMut() -> bool) -> bool {
    let deadline = std::time::Instant::now() + Duration::from_secs(secs);
    while std::time::Instant::now() < deadline {
        if cond() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    cond()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt().with_max_level(tracing::Level::WARN).init();

    // One shared cert dir (shared auto-CA) so the two depots peer over mTLS.
    let cert_dir = std::env::temp_dir().join(format!("coop-rotation-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&cert_dir);
    let p = alloc_ports(4);

    // Faster health/anti-entropy tick (2 s): at startup a peer can briefly drop a signer's signed
    // KV frame before it has processed that signer's `sys/identity` ("SignedData from unknown
    // signer"); anti-entropy re-delivers it on the next sweep, so a short interval closes the gap
    // quickly instead of waiting out the 10 s default.
    let depot_a = spawn_depot(DepotOpts {
        name: "depot-a".into(), gossip_port: p[0], http_port: p[1],
        zone: "camden".into(), bootstrap: vec![], cert_dir: cert_dir.clone(), health_secs: Some(2),
    }).await?;
    let depot_b = spawn_depot(DepotOpts {
        name: "depot-b".into(), gossip_port: p[2], http_port: p[3],
        zone: "hackney".into(), bootstrap: vec![p[0]], cert_dir: cert_dir.clone(), health_secs: Some(2),
    }).await?;
    println!("[depot-a|depot-b] up — peered over a shared auto-CA");

    let a_id = depot_a.node_id().to_string();
    // Wait until BOTH directions are established and B has firmly learned A's identity — so A's
    // signed facts frame won't be dropped as "unknown signer".
    wait_until(20, || !depot_a.agent.peers().is_empty() && !depot_b.agent.peers().is_empty()
        && depot_b.agent.kv().get(&format!("sys/identity/{a_id}")).is_some()).await;

    // ── Phase 1 — A publishes a CRDT fact (signed by its CURRENT key) ───────────
    assert!(publish_field(&depot_a.agent, "status", json!("accepting-donations")),
        "depot-a publishes a signed facts field");
    let old_key = depot_a.agent.identity_public_key().expect("a has a tls identity");

    let seen_before = wait_until(20, || {
        read_verified_fields(&depot_b.agent, &a_id, 120_000).get("status") == Some(&json!("accepting-donations"))
    }).await;
    assert!(seen_before, "depot-b verifies A's field before the rotation");
    println!("[phase 1] depot-b verified A's 'status' field (signed by A's original key) ✓");

    // ── Phase 2 — A rotates its identity (sys/identity/{a} becomes new ‖ old) ────
    println!("[phase 2] depot-a rotating its Ed25519 identity …");
    let new_key = depot_a.agent.rotate_identity(Duration::from_millis(500)).await?;
    assert_ne!(new_key, old_key, "rotation produced a fresh key");

    // depot-b learns the rotated identity history (new ‖ old) via gossip.
    let learned_rotation = wait_until(20, || {
        depot_b.agent.kv().get(&format!("sys/identity/{a_id}")).map(|b| b.len() >= 64).unwrap_or(false)
    }).await;
    assert!(learned_rotation, "depot-b learns A's new‖old identity history");

    // The field signed BEFORE the rotation (by the now-retired key) must still verify — the
    // retained-key-set posture. Without it, this field would silently vanish from B's view.
    let still_verifies = wait_until(20, || {
        read_verified_fields(&depot_b.agent, &a_id, 120_000).get("status") == Some(&json!("accepting-donations"))
    }).await;
    assert!(still_verifies, "the pre-rotation field still verifies via A's retained key set");
    println!("[phase 2] depot-b STILL verifies the old-key-signed field after the rotation ✓ (retained-key set)");

    // ── Phase 3 — A signs a fresh field with the NEW key; B verifies that too ───
    assert!(publish_field(&depot_a.agent, "status", json!("draining-for-handover")),
        "depot-a publishes an updated field with the new key");
    let seen_new = wait_until(20, || {
        read_verified_fields(&depot_b.agent, &a_id, 120_000).get("status") == Some(&json!("draining-for-handover"))
    }).await;
    assert!(seen_new, "depot-b verifies A's new-key-signed update");
    println!("[phase 3] depot-b verified A's fresh field signed by the NEW key ✓");

    println!("\nAll assertions passed — identity rotated live; peer verifies across the rotation, no disruption.");

    depot_b.shutdown().await;
    depot_a.shutdown().await;
    let _ = std::fs::remove_dir_all(&cert_dir);
    Ok(())
}
