//! `GossipAgent`-driven tests for [`MeshHandle`](mycelium_core::MeshHandle).
//!
//! The handle itself lives in `mycelium-core` (v2 M3); the pure `SignalHandlers`
//! unit test stays beside it there. These exercise the handle through a live
//! `GossipAgent`, so they belong in the full crate.

use crate::signal::kv_ns;
use crate::{GossipAgent, GossipConfig, NodeId, SignalScope};
use bytes::Bytes;
use std::{sync::Arc, time::Duration};

fn make_agent() -> GossipAgent {
    GossipAgent::new(NodeId::new("127.0.0.1", 0).unwrap(), GossipConfig::default())
}

// ── quorum ────────────────────────────────────────────────────────────

#[test]
fn quorum_false_initially() {
    let agent = make_agent();
    assert!(!agent.mesh().quorum("contract.available", 1, Duration::from_secs(10)));
}

#[test]
fn quorum_true_after_delivery() {
    let agent = make_agent();
    let _ = agent.mesh().emit("contract.available", SignalScope::System, Bytes::new());
    assert!(
        agent.mesh().quorum("contract.available", 1, Duration::from_secs(10)),
        "quorum(k, 1, 10s) must be true after one delivery",
    );
}

// ── pheromone trail ───────────────────────────────────────────────────

#[test]
fn pheromone_trail_write_read_and_evaporate() {
    let agent = make_agent();
    let load_key = format!("{}worker-1", kv_ns::LOAD);
    let _ = agent.kv().set(load_key.clone(), b"queue=0".to_vec());
    let trails = agent.kv().scan_prefix(kv_ns::LOAD);
    assert_eq!(trails.len(), 1);
    assert_eq!(trails[0].1, Bytes::from_static(b"queue=0"));
    let _ = agent.kv().set(load_key.clone(), b"queue=3".to_vec());
    assert_eq!(agent.kv().scan_prefix(kv_ns::LOAD).len(), 1,
               "update overwrites in place — store has one entry per worker");
    let _ = agent.kv().delete(load_key);
    assert_eq!(agent.kv().scan_prefix(kv_ns::LOAD).len(), 0,
               "tombstone evaporates pheromone trail");
}

// ── group join/leave ──────────────────────────────────────────────────

#[test]
fn join_group_idempotent() {
    let agent = make_agent();
    agent.mesh().join_group("nlp");
    agent.mesh().join_group("nlp");
    let _rx = agent.mesh().signal_rx("t");
    let _ = agent.mesh().emit("t", SignalScope::Group(Arc::from("nlp")), b"ok".to_vec());
    let key = format!("grp/nlp/{}", agent.node_id());
    assert_eq!(agent.kv().get(&key), Some(Bytes::from_static(b"1")), "join is still reflected in store");
}

#[test]
fn leave_group_idempotent() {
    let agent = make_agent();
    agent.mesh().join_group("compute");
    agent.mesh().leave_group("compute");
    agent.mesh().leave_group("compute");
    let key = format!("grp/compute/{}", agent.node_id());
    assert_eq!(agent.kv().get(&key), None, "tombstone stands after double leave");
}

#[tokio::test]
async fn join_group_published_to_store() {
    let agent = make_agent();
    agent.mesh().join_group("compute");
    let key = format!("grp/compute/{}", agent.node_id());
    assert_eq!(agent.kv().get(&key), Some(Bytes::from_static(b"1")));
}

#[tokio::test]
async fn leave_group_tombstones_store_entry() {
    let agent = make_agent();
    agent.mesh().join_group("compute");
    agent.mesh().leave_group("compute");
    let key = format!("grp/compute/{}", agent.node_id());
    assert_eq!(agent.kv().get(&key), None, "leave_group should tombstone the membership key");
}
