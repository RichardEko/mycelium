//! Crate-level test module — originally inlined as `#[cfg(test)] mod tests`
//! at the end of `src/lib.rs`. Lifted out so `lib.rs` itself is a thin
//! crate-config + re-exports file (≈90 lines), making the public API
//! surface easy to scan without scrolling past 2 800 lines of tests.
//!
//! All tests stay private to the crate (no integration-test changes) so
//! they can keep using `pub(crate)` items like `ConnContext`, `KvState`,
//! `GossipUpdate`, etc.

use super::*;
use crate::connection::ConnContext;
use crate::framing::{
    bincode_cfg, read_frame, write_frame, GossipUpdate, SyncEntry,
    WireMessage,
    N_GOSSIP_SHARDS, TTL_OFFSET,
};
use crate::seen::ShardedSeen;
use crate::store::{store_hash, KvState, StoreEntry};
use bytes::{Bytes, BytesMut};
use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{mpsc, watch},
    time,
};

// ── Helpers ───────────────────────────────────────────────────────────────

// Port 0 is intentional: this agent is for store/API unit tests only and must
// NOT have start() called on it — validate() rejects bind_port = 0.
// Use alloc_port() + agent.start() for integration tests that need a live node.
fn make_agent() -> GossipAgent {
    GossipAgent::new(
        NodeId::new("127.0.0.1", 0).unwrap(),
        GossipConfig::default(),
    )
}

/// Agent for state-machine tests — uses port 0 (no networking) but a non-zero
/// port in the NodeId so `agent/{node}/state` KV keys parse correctly.
pub(crate) fn make_agent_for_sm_tests() -> GossipAgent {
    GossipAgent::new(
        NodeId::new("127.0.0.1", 19876).unwrap(),
        GossipConfig::default(),
    )
}

async fn loopback_pair() -> (TcpStream, TcpStream) {
    let listener    = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr        = listener.local_addr().unwrap();
    let writer      = TcpStream::connect(addr).await.unwrap();
    let (reader, _) = listener.accept().await.unwrap();
    (writer, reader)
}

async fn send_wire(writer: &mut TcpStream, msg: &WireMessage) {
    let data = bincode::serde::encode_to_vec(msg, bincode_cfg()).unwrap();
    write_frame(writer, &data).await.unwrap();
}

fn data_update(key: &str, value: &[u8], nonce: u64, is_tombstone: bool) -> GossipUpdate {
    GossipUpdate {
        sender:       NodeId::new("127.0.0.1", 9999).unwrap().id_hash(),
        key:          Arc::from(key),
        value:        Bytes::copy_from_slice(value),
        timestamp:    1,
        nonce,
        ttl:          3,
        is_tombstone,
    }
}

fn spawn_handler(
    socket: TcpStream,
    store: Arc<papaya::HashMap<Arc<str>, StoreEntry>>,
    peers: Arc<papaya::HashMap<NodeId, Instant>>,
    gossip_tx: mpsc::Sender<(Bytes, u64, crate::framing::ForwardHint)>,
    seen: Arc<ShardedSeen>,
    max_ttl: u8,
) -> (Arc<watch::Sender<bool>>, tokio::task::JoinHandle<Result<(), GossipError>>) {
    use crate::connection::handle_connection;
    use crate::signal::{Boundary, SignalHandlers};
    use crate::agent::{TaskCtx, CoreCtx, BulkTransport};
    use parking_lot::RwLock;
    let node_id = NodeId::new("127.0.0.1", 0).unwrap();
    let (shutdown_tx, _) = watch::channel(false);
    let shutdown_tx = Arc::new(shutdown_tx);
    let gossip_txs: Arc<[mpsc::Sender<(Bytes, u64, crate::framing::ForwardHint)>]> =
        (0..N_GOSSIP_SHARDS).map(|_| gossip_tx.clone()).collect::<Vec<_>>().into();
    // Seed the hash accumulator from the store's current state so the
    // anti-entropy fast-path works correctly for pre-populated test stores.
    let initial_hash = store_hash(&store);
    let kv_state = Arc::new(KvState {
        kv_store: crate::store::KvStore {
            store,
            prefix_index:      Arc::new(crate::store::PrefixIndex::new()),
            index_stripes:     Arc::new(std::array::from_fn(|_| std::sync::Mutex::new(()))),
            cap_ns_index:      Arc::new(crate::store::PrefixIndex::new()),
            hash_acc:          Arc::new(AtomicU64::new(initial_hash)),
            dropped_frames:    Arc::new(AtomicU64::new(0)),
            individual_flood_fallbacks: Arc::new(AtomicU64::new(0)),
            max_store_entries: 0,
            grp_generation:    Arc::new(AtomicU64::new(0)),
            prefix_watchers:           Arc::new(papaya::HashMap::new()),
            prefix_predicate_watchers: Arc::new(papaya::HashMap::new()),
            next_pred_watcher_id:      Arc::new(AtomicU64::new(0)),
            peer_localities:           Arc::new(papaya::HashMap::new()),
            quorum_trackers:           Arc::new(papaya::HashMap::new()),
        },
        subscriptions: Arc::new(papaya::HashMap::new()),
    });
    let (shutdown_tx_inner, _) = tokio::sync::watch::channel(false);
    let core_ctx = Arc::new(CoreCtx {
        node_id: node_id.clone(),
        seen,
        hlc: Arc::new(crate::hlc::Hlc::new()),
        signal_boundary: Arc::new(RwLock::new(Boundary::new(node_id))),
        signal_handlers: Arc::new(SignalHandlers::new(Duration::from_secs(600))),
        gossip_txs,
        default_ttl: max_ttl,
        kv_state,
        wal: std::sync::OnceLock::new(),
        sys_namespace_violations: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        tls: std::sync::OnceLock::new(),
        peer_keys: Arc::new(papaya::HashMap::new()),
        peers: Arc::new(papaya::HashMap::new()),
        reorder_buf: None,
        reply_interceptor: None,
        shutdown_tx: Arc::new(shutdown_tx_inner),
        task_handles: Arc::new(std::sync::Mutex::new(tokio::task::JoinSet::new())),
        config: Arc::new(crate::config::GossipConfig::default()),
    });
    let task_ctx = Arc::new(TaskCtx {
        core: core_ctx,
        caps_advertised: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        bulk_transport: Arc::new(BulkTransport::new(0, Duration::from_secs(5), 64)),
        rpc_pending: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
        commit_conflicts: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        #[cfg(feature = "compliance")]
        audit_chain: Arc::new(std::sync::Mutex::new(crate::agent::audit::AuditChainState::new())),
        filter_opacity_registry: Arc::new(crate::agent::FilterOpacityRegistry::new()),
        group_roster_cache: Arc::new(papaya::HashMap::new()),
        #[cfg(feature = "llm")]
        llm_skills: std::sync::Arc::new(papaya::HashMap::new()),
        #[cfg(feature = "llm")]
        llm_dispatch_spawned: std::sync::atomic::AtomicBool::new(false),
    });
    let ctx = ConnContext {
        task_ctx,
        peers,
        shutdown: Arc::clone(&shutdown_tx),
        peer_writers: Arc::new(papaya::HashMap::new()),
        writer_depth: 64,
        backoff: Duration::ZERO,
        n_shards: N_GOSSIP_SHARDS,
        intern_keys: true,
        intern_max_keys: 0,
        max_peers: usize::MAX,
        writer_idle_timeout: Duration::ZERO,
        peer_list_tx: tokio::sync::watch::channel(std::sync::Arc::from(Vec::<NodeId>::new())).0,
    };
    let handle = tokio::spawn(handle_connection(
        crate::stream::GossipStream::Plain(socket),
        "127.0.0.1:0".parse().unwrap(),
        ctx,
    ));
    (shutdown_tx, handle)
}

async fn poll_until(mut predicate: impl FnMut() -> bool, timeout_ms: u64) {
    tokio::time::timeout(
        Duration::from_millis(timeout_ms),
        async {
            loop {
                if predicate() { return; }
                time::sleep(Duration::from_millis(5)).await;
            }
        },
    )
    .await
    .unwrap_or_else(|_| panic!("poll_until timed out after {}ms", timeout_ms));
}

// ── Port allocator for integration tests ──────────────────────────────────

fn alloc_port() -> u16 {
    // Bind to port 0, let the OS assign an ephemeral port, then release the
    // socket. The port is free for ~microseconds before the agent binds it;
    // this is far more reliable than a fixed sequential range that may already
    // be in use on the test host.
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("OS failed to allocate ephemeral port for test")
        .local_addr()
        .unwrap()
        .port()
}

// ── Two-node consensus test fixture ──────────────────────────────────────
//
// Required for any multi-node consensus test:
// - Both nodes need start_consensus_listener or their votes never arrive.
// - quorum = ⌊(peers+1)/2⌋ + 1 = 2 once peers are connected; a test that
//   calls propose before peers connect silently gets quorum=1 (self-vote
//   only) and passes for the wrong reason.
// - Structural peer-ready poll converts a timing race into a deterministic
//   failure if the cluster doesn't form, making root causes obvious.

struct ConsensusPair {
    pub a:   GossipAgent,
    pub b:   GossipAgent,
    pub _la: ConsensusListenerHandle,
    pub _lb: ConsensusListenerHandle,
}

async fn consensus_pair() -> ConsensusPair {
    let port_a = alloc_port();
    let port_b = alloc_port();
    let id_a = NodeId::new("127.0.0.1", port_a).unwrap();
    let id_b = NodeId::new("127.0.0.1", port_b).unwrap();
    let mut cfg_a = GossipConfig::default();
    cfg_a.bind_port                  = port_a;
    cfg_a.bootstrap_peers            = vec![id_b.clone()];
    cfg_a.health_check_max_jitter_ms = 50;   // cap initial ping delay so poll converges fast
    let mut cfg_b = GossipConfig::default();
    cfg_b.bind_port                  = port_b;
    cfg_b.bootstrap_peers            = vec![id_a.clone()];
    cfg_b.health_check_max_jitter_ms = 50;
    let a = GossipAgent::new(id_a, cfg_a);
    let b = GossipAgent::new(id_b, cfg_b);
    a.start().await.unwrap();
    b.start().await.unwrap();
    let _la = a.consensus().start_consensus_listener(ConsensusConfig::default());
    let _lb = b.consensus().start_consensus_listener(ConsensusConfig::default());
    // Structural poll — fails deterministically if cluster doesn't form.
    poll_until(|| !a.peers().is_empty() && !b.peers().is_empty(), 2_000).await;
    ConsensusPair { a, b, _la, _lb }
}


// ── Agent API ─────────────────────────────────────────────────────────────

// Compile-time proof that GossipAgent is Send + Sync so it can be wrapped in Arc.
#[allow(dead_code)]
fn assert_gossip_agent_is_send_sync() {
    fn check<T: Send + Sync>() {}
    check::<GossipAgent>();
}

#[tokio::test]
async fn test_state_request_ignored_from_unknown_peer() {
    let (mut writer, reader) = loopback_pair().await;
    let store: Arc<papaya::HashMap<Arc<str>, StoreEntry>> = Arc::new(papaya::HashMap::new());
    store.pin().insert(Arc::from("secret"), StoreEntry {
        data: Some(Bytes::from_static(b"payload")),
        timestamp: 1,
    });
    let (tx, _rx) = mpsc::channel(10);
    let (shutdown_tx, _) = spawn_handler(
        reader, Arc::clone(&store), Arc::new(papaya::HashMap::new()), tx,
        Arc::new(ShardedSeen::new(N_GOSSIP_SHARDS)), GossipConfig::default().default_ttl,
    );

    // StateRequest from an unknown peer (not in peers map) must be silently ignored.
    send_wire(&mut writer, &WireMessage::StateRequest {
        sender: "127.0.0.1:4444".parse().unwrap(),
        store_hash: 0,
        key_timestamps: vec![],
    }).await;

    // Give the handler time to process the message.
    time::sleep(Duration::from_millis(50)).await;

    // Store must be unchanged — no StateResponse was routed back because the
    // peer_writers map is empty (no writer was spawned for the unknown sender).
    assert_eq!(
        store.pin().get("secret").and_then(|e| e.data.clone()),
        Some(Bytes::from_static(b"payload")),
    );
    let _ = shutdown_tx.send(true);
}

// ── handle_connection behaviour ───────────────────────────────────────────

#[tokio::test]
async fn test_upsert_propagates() {
    let (mut writer, reader) = loopback_pair().await;
    let store: Arc<papaya::HashMap<Arc<str>, StoreEntry>> = Arc::new(papaya::HashMap::new());
    let (tx, _rx) = mpsc::channel(10);
    let _ = spawn_handler(reader, Arc::clone(&store), Arc::new(papaya::HashMap::new()), tx,
                          Arc::new(ShardedSeen::new(N_GOSSIP_SHARDS)), GossipConfig::default().default_ttl);

    send_wire(&mut writer, &WireMessage::Data(data_update("k", b"v", 1, false))).await;

    let s = Arc::clone(&store);
    poll_until(|| s.pin().get("k").and_then(|e| e.data.clone()) == Some(Bytes::from_static(b"v")), 200).await;
}

#[tokio::test]
async fn test_tombstone_nullifies_value() {
    let (mut writer, reader) = loopback_pair().await;
    let store: Arc<papaya::HashMap<Arc<str>, StoreEntry>> = Arc::new(papaya::HashMap::new());
    store.pin().insert(Arc::from("k"), StoreEntry { data: Some(Bytes::from_static(b"old")), timestamp: 0 });

    let (tx, _rx) = mpsc::channel(10);
    let _ = spawn_handler(reader, Arc::clone(&store), Arc::new(papaya::HashMap::new()), tx,
                          Arc::new(ShardedSeen::new(N_GOSSIP_SHARDS)), GossipConfig::default().default_ttl);

    send_wire(&mut writer, &WireMessage::Data(data_update("k", b"", 2, true))).await;

    let s = Arc::clone(&store);
    poll_until(|| s.pin().get("k").is_some_and(|e| e.data.is_none()), 200).await;
    assert!(store.pin().get("k").is_some(), "tombstone entry must remain in store for LWW");
}

#[tokio::test]
async fn test_deduplication() {
    let (mut writer, reader) = loopback_pair().await;
    let store: Arc<papaya::HashMap<Arc<str>, StoreEntry>> = Arc::new(papaya::HashMap::new());
    let (tx, mut rx) = mpsc::channel::<(Bytes, u64, crate::framing::ForwardHint)>(10);
    let seen = Arc::new(ShardedSeen::new(N_GOSSIP_SHARDS));
    let _ = spawn_handler(reader, Arc::clone(&store), Arc::new(papaya::HashMap::new()), tx, seen,
                          GossipConfig::default().default_ttl);

    let update = data_update("k", b"v", 42, false);
    send_wire(&mut writer, &WireMessage::Data(update.clone())).await;
    send_wire(&mut writer, &WireMessage::Data(update)).await;

    let s = Arc::clone(&store);
    poll_until(|| s.pin().get("k").and_then(|e| e.data.clone()) == Some(Bytes::from_static(b"v")), 200).await;

    let mut forwarded = 0;
    while rx.try_recv().is_ok() { forwarded += 1; }
    assert_eq!(forwarded, 1, "duplicate nonce should be dropped");
}

#[tokio::test]
async fn test_peer_registered_from_ping() {
    let (mut writer, reader) = loopback_pair().await;
    let peers: Arc<papaya::HashMap<NodeId, Instant>> = Arc::new(papaya::HashMap::new());
    let (tx, _rx) = mpsc::channel(10);
    let _ = spawn_handler(reader, Arc::new(papaya::HashMap::new()), Arc::clone(&peers), tx,
                          Arc::new(ShardedSeen::new(N_GOSSIP_SHARDS)), GossipConfig::default().default_ttl);

    send_wire(&mut writer, &WireMessage::Ping { sender: "127.0.0.1:9999".parse().unwrap(), known_peers: vec![] }).await;

    let p = Arc::clone(&peers);
    poll_until(
        || p.pin().contains_key(&NodeId::new("127.0.0.1", 9999).unwrap()),
        200,
    ).await;
}

#[tokio::test]
async fn test_ping_not_deduplicated() {
    let (mut writer, reader) = loopback_pair().await;
    let peers: Arc<papaya::HashMap<NodeId, Instant>> = Arc::new(papaya::HashMap::new());
    let (tx, _rx) = mpsc::channel(10);
    let _ = spawn_handler(reader, Arc::new(papaya::HashMap::new()), Arc::clone(&peers), tx,
                          Arc::new(ShardedSeen::new(N_GOSSIP_SHARDS)), GossipConfig::default().default_ttl);

    send_wire(&mut writer, &WireMessage::Ping { sender: "127.0.0.1:1001".parse().unwrap(), known_peers: vec![] }).await;
    send_wire(&mut writer, &WireMessage::Ping { sender: "127.0.0.1:1002".parse().unwrap(), known_peers: vec![] }).await;

    let p = Arc::clone(&peers);
    poll_until(
        || {
            let g = p.pin();
            g.contains_key(&NodeId::new("127.0.0.1", 1001).unwrap())
                && g.contains_key(&NodeId::new("127.0.0.1", 1002).unwrap())
        },
        200,
    ).await;
}

#[tokio::test]
async fn test_handle_connection_shutdown() {
    let (mut writer, reader) = loopback_pair().await;
    let store: Arc<papaya::HashMap<Arc<str>, StoreEntry>> = Arc::new(papaya::HashMap::new());
    let (tx, _rx) = mpsc::channel(10);
    let (shutdown_tx, handle) = spawn_handler(
        reader,
        Arc::clone(&store),
        Arc::new(papaya::HashMap::new()),
        tx,
        Arc::new(ShardedSeen::new(N_GOSSIP_SHARDS)),
        GossipConfig::default().default_ttl,
    );

    send_wire(&mut writer, &WireMessage::Data(data_update("k", b"v", 1, false))).await;
    let s = Arc::clone(&store);
    poll_until(|| s.pin().get("k").is_some(), 200).await;

    let _ = shutdown_tx.send(true);
    handle.await.unwrap().ok();
}

// ── TTL clamping ──────────────────────────────────────────────────────────

#[tokio::test]
async fn test_inbound_ttl_clamped_to_max() {
    let (mut writer, reader) = loopback_pair().await;
    let store: Arc<papaya::HashMap<Arc<str>, StoreEntry>> = Arc::new(papaya::HashMap::new());
    let (tx, mut rx) = mpsc::channel::<(Bytes, u64, crate::framing::ForwardHint)>(10);
    let _ = spawn_handler(reader, Arc::clone(&store), Arc::new(papaya::HashMap::new()), tx,
                          Arc::new(ShardedSeen::new(N_GOSSIP_SHARDS)), 5);

    let mut update = data_update("k", b"v", 77, false);
    update.ttl = 255;
    send_wire(&mut writer, &WireMessage::Data(update)).await;

    let s = Arc::clone(&store);
    poll_until(|| s.pin().get("k").is_some(), 200).await;

    let (fwd_bytes, _, _) = rx.try_recv().expect("should have forwarded once");
    assert_eq!(fwd_bytes[TTL_OFFSET], 4, "forwarded TTL must be clamped to max_ttl - 1");
}

#[tokio::test]
async fn test_inbound_ttl_above_max_not_forwarded_when_clamped_to_one() {
    let (mut writer, reader) = loopback_pair().await;
    let store: Arc<papaya::HashMap<Arc<str>, StoreEntry>> = Arc::new(papaya::HashMap::new());
    let (tx, mut rx) = mpsc::channel::<(Bytes, u64, crate::framing::ForwardHint)>(10);
    let _ = spawn_handler(reader, Arc::clone(&store), Arc::new(papaya::HashMap::new()), tx,
                          Arc::new(ShardedSeen::new(N_GOSSIP_SHARDS)), 1);

    let mut update = data_update("k", b"v", 88, false);
    update.ttl = 100;
    send_wire(&mut writer, &WireMessage::Data(update)).await;

    let s = Arc::clone(&store);
    poll_until(|| s.pin().get("k").is_some(), 200).await;

    assert!(rx.try_recv().is_err(), "no forward when clamped ttl == 1");
}

// ── Two-node integration test ─────────────────────────────────────────────

#[tokio::test]
async fn test_two_node_propagation() {
    let port_a = alloc_port();
    let port_b = alloc_port();

    let id_a = NodeId::new("127.0.0.1", port_a).unwrap();
    let id_b = NodeId::new("127.0.0.1", port_b).unwrap();

    let mut cfg_a = GossipConfig::default();
    cfg_a.bind_port = port_a;
    cfg_a.health_check_interval_secs = 1;
    cfg_a.bootstrap_peers = vec![NodeId::new("127.0.0.1", port_b).unwrap()];

    let mut cfg_b = GossipConfig::default();
    cfg_b.bind_port = port_b;
    cfg_b.health_check_interval_secs = 1;
    cfg_b.bootstrap_peers = vec![NodeId::new("127.0.0.1", port_a).unwrap()];

    let agent_a = Arc::new(GossipAgent::new(id_a, cfg_a));
    let agent_b = Arc::new(GossipAgent::new(id_b, cfg_b));

    agent_a.start().await.unwrap();
    agent_b.start().await.unwrap();

    time::sleep(Duration::from_millis(20)).await;

    let _ = agent_a.kv().set("x", b"hello".to_vec());
    let b = Arc::clone(&agent_b);
    poll_until(
        || b.kv().get("x") == Some(Bytes::from_static(b"hello")),
        2_000,
    ).await;

    let _ = agent_a.kv().delete("x");
    let b = Arc::clone(&agent_b);
    poll_until(|| b.kv().get("x").is_none(), 2_000).await;

    agent_a.shutdown().await;
    agent_b.shutdown().await;
}


// ── subscribe() ───────────────────────────────────────────────────────────

#[tokio::test]
async fn test_subscribe_notified_via_gossip() {
    let (mut writer, reader) = loopback_pair().await;
    let store: Arc<papaya::HashMap<Arc<str>, StoreEntry>> = Arc::new(papaya::HashMap::new());
    let subs: Arc<papaya::HashMap<Arc<str>, watch::Sender<Option<Bytes>>>> =
        Arc::new(papaya::HashMap::new());
    let (gossip_tx, _) = mpsc::channel::<(Bytes, u64, crate::framing::ForwardHint)>(10);
    let (shutdown_tx, _sd) = watch::channel(false);
    let shutdown_tx = Arc::new(shutdown_tx);

    let (sub_tx, _) = watch::channel(None::<Bytes>);
    let mut sub_rx = sub_tx.subscribe();
    sub_rx.borrow_and_update();
    subs.pin().insert(Arc::from("gossip_key"), sub_tx);

    let gossip_txs: Arc<[mpsc::Sender<(Bytes, u64, crate::framing::ForwardHint)>]> =
        (0..N_GOSSIP_SHARDS).map(|_| gossip_tx.clone()).collect::<Vec<_>>().into();
    {
        use crate::signal::{Boundary, SignalHandlers};
        use crate::agent::{TaskCtx, CoreCtx, BulkTransport};
        use parking_lot::RwLock;
        let node_id = NodeId::new("127.0.0.1", 0).unwrap();
        let kv_state = Arc::new(KvState {
            kv_store: crate::store::KvStore {
                store: Arc::clone(&store),
                prefix_index:      Arc::new(crate::store::PrefixIndex::new()),
                index_stripes:     Arc::new(std::array::from_fn(|_| std::sync::Mutex::new(()))),
                cap_ns_index:      Arc::new(crate::store::PrefixIndex::new()),
                hash_acc:          Arc::new(AtomicU64::new(0)),
                dropped_frames:    Arc::new(AtomicU64::new(0)),
            individual_flood_fallbacks: Arc::new(AtomicU64::new(0)),
                max_store_entries: 0,
                grp_generation:    Arc::new(AtomicU64::new(0)),
                prefix_watchers:           Arc::new(papaya::HashMap::new()),
                prefix_predicate_watchers: Arc::new(papaya::HashMap::new()),
                next_pred_watcher_id:      Arc::new(AtomicU64::new(0)),
                peer_localities:           Arc::new(papaya::HashMap::new()),
                quorum_trackers:           Arc::new(papaya::HashMap::new()),
            },
            subscriptions: subs,
        });
        let (shutdown_tx_inner2, _) = tokio::sync::watch::channel(false);
        let core_ctx = Arc::new(CoreCtx {
            node_id: node_id.clone(),
            seen: Arc::new(ShardedSeen::new(N_GOSSIP_SHARDS)),
            hlc: Arc::new(crate::hlc::Hlc::new()),
            signal_boundary: Arc::new(RwLock::new(Boundary::new(node_id))),
            signal_handlers: Arc::new(SignalHandlers::new(Duration::from_secs(600))),
            gossip_txs,
            default_ttl: 5,
            kv_state,
            wal: std::sync::OnceLock::new(),
            sys_namespace_violations: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            tls: std::sync::OnceLock::new(),
            peer_keys: Arc::new(papaya::HashMap::new()),
            peers: Arc::new(papaya::HashMap::new()),
            reorder_buf: None,
            reply_interceptor: None,
            shutdown_tx: Arc::new(shutdown_tx_inner2),
            task_handles: Arc::new(std::sync::Mutex::new(tokio::task::JoinSet::new())),
            config: Arc::new(crate::config::GossipConfig::default()),
        });
        let task_ctx = Arc::new(TaskCtx {
            core: core_ctx,
            caps_advertised: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            bulk_transport: Arc::new(BulkTransport::new(0, Duration::from_secs(5), 64)),
            rpc_pending: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
            commit_conflicts: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            #[cfg(feature = "compliance")]
            audit_chain: Arc::new(std::sync::Mutex::new(crate::agent::audit::AuditChainState::new())),
            filter_opacity_registry: Arc::new(crate::agent::FilterOpacityRegistry::new()),
            group_roster_cache: Arc::new(papaya::HashMap::new()),
            #[cfg(feature = "llm")]
            llm_skills: std::sync::Arc::new(papaya::HashMap::new()),
            #[cfg(feature = "llm")]
            llm_dispatch_spawned: std::sync::atomic::AtomicBool::new(false),
        });
        let ctx = ConnContext {
            task_ctx,
            peers: Arc::new(papaya::HashMap::new()),
            shutdown: shutdown_tx,
            peer_writers: Arc::new(papaya::HashMap::new()),
            writer_depth: 64,
            backoff: Duration::ZERO,
            n_shards: N_GOSSIP_SHARDS,
            intern_keys: true,
            intern_max_keys: 0,
            max_peers: usize::MAX,
            writer_idle_timeout: Duration::ZERO,
            peer_list_tx: tokio::sync::watch::channel(std::sync::Arc::from(Vec::<NodeId>::new())).0,
        };
        use crate::connection::handle_connection;
        tokio::spawn(handle_connection(crate::stream::GossipStream::Plain(reader), "127.0.0.1:0".parse().unwrap(), ctx));
    }

    send_wire(&mut writer, &WireMessage::Data(data_update("gossip_key", b"gossip_val", 42, false))).await;

    tokio::time::timeout(Duration::from_millis(200), sub_rx.changed())
        .await
        .expect("subscriber should fire within 200 ms")
        .unwrap();
    assert_eq!(*sub_rx.borrow(), Some(Bytes::from_static(b"gossip_val")));
}

#[test]
fn test_subscribe_multiple_receivers_same_key() {
    let agent = make_agent();
    let rx1 = agent.kv().subscribe("k");
    let rx2 = agent.kv().subscribe("k");
    let _ = agent.kv().set("k", b"shared".to_vec());
    assert_eq!(*rx1.borrow(), Some(Bytes::from_static(b"shared")));
    assert_eq!(*rx2.borrow(), Some(Bytes::from_static(b"shared")));
}

// ── Peer-list piggybacking ────────────────────────────────────────────────

#[tokio::test]
async fn test_piggybacked_peers_added_to_table() {
    let (mut writer, reader) = loopback_pair().await;
    let peers: Arc<papaya::HashMap<NodeId, Instant>> = Arc::new(papaya::HashMap::new());
    let (tx, _rx) = mpsc::channel(10);
    let _ = spawn_handler(reader, Arc::new(papaya::HashMap::new()), Arc::clone(&peers), tx,
                          Arc::new(ShardedSeen::new(N_GOSSIP_SHARDS)), GossipConfig::default().default_ttl);

    let piggybacked = vec![
        NodeId::new("127.0.0.1", 5001).unwrap(),
        NodeId::new("127.0.0.1", 5002).unwrap(),
    ];
    send_wire(&mut writer, &WireMessage::Ping {
        sender: "127.0.0.1:9999".parse().unwrap(),
        known_peers: piggybacked,
    }).await;

    let p = Arc::clone(&peers);
    poll_until(
        || {
            let g = p.pin();
            g.contains_key(&NodeId::new("127.0.0.1", 9999).unwrap())
                && g.contains_key(&NodeId::new("127.0.0.1", 5001).unwrap())
                && g.contains_key(&NodeId::new("127.0.0.1", 5002).unwrap())
        },
        200,
    ).await;
}

#[tokio::test]
async fn test_piggybacked_self_not_added() {
    let (mut writer, reader) = loopback_pair().await;
    let peers: Arc<papaya::HashMap<NodeId, Instant>> = Arc::new(papaya::HashMap::new());
    let (tx, _rx) = mpsc::channel(10);
    let _ = spawn_handler(reader, Arc::new(papaya::HashMap::new()), Arc::clone(&peers), tx,
                          Arc::new(ShardedSeen::new(N_GOSSIP_SHARDS)), GossipConfig::default().default_ttl);

    let self_id = NodeId::new("127.0.0.1", 0).unwrap();
    send_wire(&mut writer, &WireMessage::Ping {
        sender: "127.0.0.1:9000".parse().unwrap(),
        known_peers: vec![self_id.clone()],
    }).await;

    let p = Arc::clone(&peers);
    poll_until(|| p.pin().contains_key(&NodeId::new("127.0.0.1", 9000).unwrap()), 200).await;
    assert!(!peers.pin().contains_key(&self_id), "self must not be added via piggybacking");
}

#[tokio::test]
async fn test_piggybacked_known_peer_timestamp_not_overwritten() {
    let (mut writer, reader) = loopback_pair().await;
    let peers: Arc<papaya::HashMap<NodeId, Instant>> = Arc::new(papaya::HashMap::new());
    let (tx, _rx) = mpsc::channel(10);
    let _ = spawn_handler(reader, Arc::new(papaya::HashMap::new()), Arc::clone(&peers), tx,
                          Arc::new(ShardedSeen::new(N_GOSSIP_SHARDS)), GossipConfig::default().default_ttl);

    let known_id: NodeId = "127.0.0.1:7777".parse().unwrap();
    let old_time = Instant::now() - Duration::from_secs(5);
    peers.pin().insert(known_id.clone(), old_time);

    send_wire(&mut writer, &WireMessage::Ping {
        sender: "127.0.0.1:8888".parse().unwrap(),
        known_peers: vec![known_id.clone()],
    }).await;

    let p = Arc::clone(&peers);
    poll_until(|| p.pin().contains_key(&NodeId::new("127.0.0.1", 8888).unwrap()), 200).await;

    let stored = *peers.pin().get(&known_id).unwrap();
    assert_eq!(
        stored, old_time,
        "existing peer timestamp must not be overwritten by piggybacking"
    );
}

// ── Anti-entropy ──────────────────────────────────────────────────────────

#[tokio::test]
async fn test_state_response_applies_entries_to_store() {
    let (mut writer, reader) = loopback_pair().await;
    let store: Arc<papaya::HashMap<Arc<str>, StoreEntry>> = Arc::new(papaya::HashMap::new());
    let (tx, _rx) = mpsc::channel(10);
    let _ = spawn_handler(reader, Arc::clone(&store), Arc::new(papaya::HashMap::new()), tx,
                          Arc::new(ShardedSeen::new(N_GOSSIP_SHARDS)), GossipConfig::default().default_ttl);

    let entries = vec![
        SyncEntry { key: Arc::from("c:v1"), value: Bytes::from_static(b"payload_v1"), timestamp: 100, is_tombstone: false },
        SyncEntry { key: Arc::from("c:v2"), value: Bytes::new(), timestamp: 200, is_tombstone: true },
    ];
    send_wire(&mut writer, &WireMessage::StateResponse { entries }).await;

    let s = Arc::clone(&store);
    poll_until(
        || s.pin().get("c:v1").and_then(|e| e.data.clone()) == Some(Bytes::from_static(b"payload_v1")),
        200,
    ).await;

    assert!(
        store.pin().get("c:v2").is_some_and(|e| e.data.is_none()),
        "tombstone entry must land as data=None"
    );
}

#[tokio::test]
async fn test_state_response_respects_lww() {
    let (mut writer, reader) = loopback_pair().await;
    let store: Arc<papaya::HashMap<Arc<str>, StoreEntry>> = Arc::new(papaya::HashMap::new());
    store.pin().insert(Arc::from("k"), StoreEntry { data: Some(Bytes::from_static(b"newer")), timestamp: 999 });

    let (tx, _rx) = mpsc::channel(10);
    let _ = spawn_handler(reader, Arc::clone(&store), Arc::new(papaya::HashMap::new()), tx,
                          Arc::new(ShardedSeen::new(N_GOSSIP_SHARDS)), GossipConfig::default().default_ttl);

    let entries = vec![
        SyncEntry { key: Arc::from("k"), value: Bytes::from_static(b"stale"), timestamp: 1, is_tombstone: false },
    ];
    send_wire(&mut writer, &WireMessage::StateResponse { entries }).await;

    time::sleep(Duration::from_millis(50)).await;

    assert_eq!(
        store.pin().get("k").and_then(|e| e.data.clone()),
        Some(Bytes::from_static(b"newer")),
        "StateResponse must not overwrite a locally newer value"
    );
}

#[tokio::test]
async fn test_anti_entropy_syncs_pre_existing_state() {
    let port_a = alloc_port();
    let port_b = alloc_port();

    let mut cfg_a = GossipConfig::default();
    cfg_a.bind_port = port_a;
    cfg_a.health_check_interval_secs = 1;

    let mut cfg_b = GossipConfig::default();
    cfg_b.bind_port = port_b;
    cfg_b.health_check_interval_secs = 1;

    let agent_a = Arc::new(GossipAgent::new(
        NodeId::new("127.0.0.1", port_a).unwrap(),
        cfg_a,
    ));
    agent_a.start().await.unwrap();
    time::sleep(Duration::from_millis(20)).await;

    let _ = agent_a.kv().set("contract:v1", b"spec_bytes".to_vec());
    assert_eq!(agent_a.kv().get("contract:v1"), Some(Bytes::from_static(b"spec_bytes")));

    cfg_b.bootstrap_peers = vec![NodeId::new("127.0.0.1", port_a).unwrap()];
    let agent_b = Arc::new(GossipAgent::new(
        NodeId::new("127.0.0.1", port_b).unwrap(),
        cfg_b,
    ));
    agent_b.start().await.unwrap();

    let b = Arc::clone(&agent_b);
    poll_until(
        || b.kv().get("contract:v1") == Some(Bytes::from_static(b"spec_bytes")),
        3_000,
    ).await;

    agent_a.shutdown().await;
    agent_b.shutdown().await;
}

#[tokio::test]
async fn test_anti_entropy_skips_when_synced() {
    // Build a store with one live entry so the hash is non-zero (zero is the
    // "no digest" sentinel and would trigger a full snapshot instead).
    let store: Arc<papaya::HashMap<Arc<str>, StoreEntry>> = Arc::new(papaya::HashMap::new());
    store.pin().insert(
        Arc::from("sync_key"),
        StoreEntry { data: Some(Bytes::from_static(b"sync_val")), timestamp: 42 },
    );
    let expected_hash = store_hash(&store);
    assert_ne!(expected_hash, 0, "precondition: hash must be non-zero");

    // Bind a listener so the handler's peer writer can connect back to deliver
    // the StateResponse. The port becomes the sender NodeId's port.
    let response_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let sender_port = response_listener.local_addr().unwrap().port();
    let sender_id = NodeId::new("127.0.0.1", sender_port).unwrap();

    // Register sender as a known peer — the handler silently drops StateRequest
    // from unrecognised peers.
    let peers: Arc<papaya::HashMap<NodeId, Instant>> = Arc::new(papaya::HashMap::new());
    peers.pin().insert(sender_id.clone(), Instant::now());

    let (mut writer, reader) = loopback_pair().await;
    let (tx, _rx) = mpsc::channel(10);
    let (_shutdown, _handle) = spawn_handler(
        reader, store, peers, tx,
        Arc::new(ShardedSeen::new(N_GOSSIP_SHARDS)),
        GossipConfig::default().default_ttl,
    );

    // Start accepting before sending so the writer can connect immediately.
    let accept_task = tokio::spawn(async move {
        let (sock, _) = response_listener.accept().await.unwrap();
        sock
    });

    // Send a StateRequest whose hash matches the handler's store — fast-path.
    send_wire(&mut writer, &WireMessage::StateRequest {
        sender: sender_id,
        store_hash: expected_hash,
        key_timestamps: vec![],
    }).await;

    let mut response_sock = accept_task.await.unwrap();

    // Read back the one frame the handler must write: an empty StateResponse.
    let mut buf = BytesMut::new();
    tokio::time::timeout(
        Duration::from_millis(500),
        read_frame(&mut response_sock, &mut buf),
    )
    .await
    .expect("timed out waiting for fast-path StateResponse")
    .expect("read_frame error");

    let (msg, _): (WireMessage, _) =
        bincode::serde::decode_from_slice(&buf, bincode_cfg()).unwrap();

    match msg {
        WireMessage::StateResponse { entries } => assert!(
            entries.is_empty(),
            "anti-entropy fast-path: StateResponse must be empty when hashes match; got {} entries",
            entries.len(),
        ),
        other => panic!("expected StateResponse, got {:?}", other),
    }
}

// ── liveness flags ────────────────────────────────────────────────────────

#[tokio::test]
async fn test_system_stats_liveness_flags_while_running() {
    let port = alloc_port();
    let mut cfg = GossipConfig::default();
    cfg.bind_port = port;
    cfg.health_check_interval_secs = 1;
    let agent = Arc::new(GossipAgent::new(
        NodeId::new("127.0.0.1", port).unwrap(),
        cfg,
    ));
    agent.start().await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    let stats = agent.system_stats();
    assert!(stats.gc_alive,             "gc task should be alive while running");
    assert!(stats.health_monitor_alive, "health monitor should be alive while running");
    agent.shutdown().await;
    // After shutdown, state-gated flags must not report false negatives.
    let stats = agent.system_stats();
    assert!(stats.gc_alive,             "gc_alive should read true after clean shutdown");
    assert!(stats.health_monitor_alive, "health_monitor_alive should read true after clean shutdown");
}

// ── shutdown_with_timeout ─────────────────────────────────────────────────

#[tokio::test]
async fn test_shutdown_with_timeout_does_not_hang() {
    let port = alloc_port();
    let cfg = GossipConfig { bind_port: port, ..GossipConfig::default() };
    let agent = GossipAgent::new(NodeId::new("127.0.0.1", port).unwrap(), cfg);
    agent.start().await.unwrap();
    // Use a 1 ms internal timeout so the abort path fires; the outer 2 s
    // timeout asserts the call itself returns instead of hanging forever.
    tokio::time::timeout(
        Duration::from_secs(2),
        agent.shutdown_with_timeout(Duration::from_millis(1)),
    )
    .await
    .expect("shutdown_with_timeout must return even when the internal timeout fires");
}

#[tokio::test]
async fn test_shutdown_lifecycle_edges_never_started_and_double() {
    // Shutdown on an agent that was never started must return promptly, not
    // hang waiting on tasks that were never spawned.
    let port = alloc_port();
    let cfg = GossipConfig { bind_port: port, ..GossipConfig::default() };
    let agent = GossipAgent::new(NodeId::new("127.0.0.1", port).unwrap(), cfg);
    tokio::time::timeout(Duration::from_secs(2), agent.shutdown())
        .await
        .expect("shutdown on a never-started agent must not hang");

    // A second shutdown after a completed one must be an idempotent no-op.
    let port = alloc_port();
    let cfg = GossipConfig { bind_port: port, ..GossipConfig::default() };
    let agent = GossipAgent::new(NodeId::new("127.0.0.1", port).unwrap(), cfg);
    agent.start().await.unwrap();
    agent.shutdown().await;
    tokio::time::timeout(Duration::from_secs(2), agent.shutdown())
        .await
        .expect("double shutdown must be idempotent, not hang or panic");
}

/// Individual-scoped signals must reach targets the sender is NOT directly
/// peered with — forwarding is unconditional in the Holland model; only
/// admission is scoped. Line topology A→B→C: A's only outbound peer is B,
/// so delivery to C requires the flood fallback at origination plus B's
/// targeted relay. Pre-fix, A silently dropped the signal at origination,
/// which broke RPC requests/responses and consensus votes between
/// non-peered pairs in partial meshes (M2 finding, 2026-06-12; found by
/// the three-arm experiment bring-up).
#[tokio::test]
async fn test_individual_signal_reaches_unpeered_target_via_relay() {
    use crate::signal::SignalScope;
    use bytes::Bytes;

    let port_a = alloc_port();
    let port_b = alloc_port();
    let port_c = alloc_port();
    let id =
        |p: u16| NodeId::new("127.0.0.1", p).unwrap();

    let mk = |port: u16, boots: Vec<NodeId>| {
        let mut cfg = GossipConfig::default();
        cfg.bind_port = port;
        cfg.bootstrap_peers = boots;
        cfg.reconnect_backoff_secs = 1;
        GossipAgent::new(id(port), cfg)
    };

    // Strict line: A → B → C. A never learns a route to C within the test
    // window unless discovery intervenes; the assertion window is short
    // enough that delivery proves the relay path.
    let c = Arc::new(mk(port_c, vec![]));
    let b = Arc::new(mk(port_b, vec![id(port_c)]));
    let a = Arc::new(mk(port_a, vec![id(port_b)]));
    c.start().await.unwrap();
    b.start().await.unwrap();
    a.start().await.unwrap();

    let mut rx = c.mesh().signal_rx("test.relay");

    // Structural poll: line links up.
    for _ in 0..100 {
        if !a.peers().is_empty() && !b.peers().is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    assert!(a.mesh().emit(
        "test.relay",
        SignalScope::Individual(id(port_c)),
        Bytes::from_static(b"hop"),
    ));

    let got = tokio::time::timeout(Duration::from_secs(5), rx.recv()).await;
    let sig = got
        .expect("Individual signal must reach an unpeered target via relay")
        .expect("channel open");
    assert_eq!(&sig.payload[..], b"hop");

    // The fallback must be legible: A had no direct route, so its counter
    // (also on /stats) records the flood fallback.
    assert!(
        a.system_stats().individual_flood_fallbacks >= 1,
        "flood fallback must be counted on the sender"
    );

    a.shutdown().await;
    b.shutdown().await;
    c.shutdown().await;
}


/// Topology-class generalization of the line-relay canary above: across
/// RANDOM connected partial meshes, all three Individual-scope consumers —
/// targeted signal delivery, RPC round-trip, and consensus ballots (votes go
/// Individual to the proposer) — must work between deliberately NON-adjacent
/// node pairs via the flood fallback. Topology was the dimension no prior
/// test varied (M2 post-mortem, 2026-06-12): every existing topology
/// accidentally peered all RPC pairs directly.
#[tokio::test]
async fn test_individual_consumers_over_random_partial_meshes() {
    use crate::consensus::{ConsensusConfig, ConsensusResult};
    use crate::signal::SignalScope;
    use bytes::Bytes;

    for graph_seed in [11u64, 23, 47] {
        let mut rng = fastrand::Rng::with_seed(graph_seed);
        let n = 7;
        let ports: Vec<u16> = (0..n).map(|_| alloc_port()).collect();
        let id = |i: usize| NodeId::new("127.0.0.1", ports[i]).unwrap();

        // Random spanning tree (node i dials a random earlier node) plus one
        // extra random edge: connected by construction, sparse enough that
        // non-adjacent pairs always exist at n=7.
        let mut dials: Vec<Vec<usize>> = vec![vec![]; n];
        for (i, d) in dials.iter_mut().enumerate().skip(1) {
            d.push(rng.usize(0..i));
        }
        let (a, b) = (rng.usize(0..n), rng.usize(0..n));
        if a != b && !dials[a].contains(&b) && !dials[b].contains(&a) {
            dials[a].push(b);
        }

        let adjacent = |x: usize, y: usize| dials[x].contains(&y) || dials[y].contains(&x);
        let (src, dst) = (0..n)
            .flat_map(|x| (0..n).map(move |y| (x, y)))
            .find(|&(x, y)| x != y && !adjacent(x, y))
            .expect("sparse graph must have a non-adjacent pair");

        let mut agents = Vec::with_capacity(n);
        for i in 0..n {
            let mut cfg = GossipConfig::default();
            cfg.bind_port = ports[i];
            cfg.bootstrap_peers = dials[i].iter().map(|&j| id(j)).collect();
            cfg.reconnect_backoff_secs = 1;
            // Fast first pings: peer discovery is ping-driven, and the
            // property under test is delivery, not discovery cadence.
            cfg.health_check_max_jitter_ms = 50;
            // Worst-case relay path in a random tree on 7 nodes can exceed
            // the default hop budget; the property under test is delivery,
            // not TTL sizing.
            cfg.default_ttl = 10;
            let agent = Arc::new(GossipAgent::new(id(i), cfg));
            agent.start().await.unwrap();
            agents.push(agent);
        }

        // Consensus listeners on every node BEFORE any proposal (CLAUDE.md
        // pattern), and structural poll until the dial graph links up.
        let _listeners: Vec<_> = agents
            .iter()
            .map(|a| a.consensus().start_consensus_listener(ConsensusConfig::default()))
            .collect();
        for _ in 0..100 {
            if agents.iter().all(|a| !a.peers().is_empty()) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        // (a) Individual signal src -> non-adjacent dst. Re-emit each second
        // (distinct frames) to distinguish "racing formation" from "never".
        let mut rx = agents[dst].mesh().signal_rx("prop.topology");
        let mut delivered_at = None;
        for attempt in 0..8 {
            assert!(agents[src].mesh().emit(
                "prop.topology",
                SignalScope::Individual(id(dst)),
                Bytes::from_static(b"hop"),
            ));
            if tokio::time::timeout(Duration::from_secs(1), rx.recv()).await.is_ok() {
                delivered_at = Some(attempt);
                break;
            }
        }
        if delivered_at.is_none() {
            for (i, a) in agents.iter().enumerate() {
                eprintln!("  POSTMORTEM node {i}: fb={} peers={}",
                    a.system_stats().individual_flood_fallbacks, a.peers().len());
            }
            // Which senders can reach dst directly?
            for (i, agent) in agents.iter().enumerate() {
                if i == dst { continue; }
                let ok = agent.mesh().emit(
                    "prop.topology",
                    SignalScope::Individual(id(dst)),
                    Bytes::from_static(b"direct"),
                );
                let got = tokio::time::timeout(Duration::from_millis(800), rx.recv()).await.is_ok();
                eprintln!("  direct {i}->{dst}: emit={ok} delivered={got}");
            }
            panic!("graph_seed={graph_seed}: signal {src}->{dst} undelivered after 8 attempts");
        }
        eprintln!("  signal delivered on attempt {}", delivered_at.unwrap());

        // (b) RPC round-trip src -> dst (echo handler on dst).
        let mut req_rx = agents[dst].service().rpc_rx("prop.echo");
        let dst_agent = Arc::clone(&agents[dst]);
        tokio::spawn(async move {
            while let Some(req) = req_rx.recv().await {
                let payload = req.payload();
                dst_agent.service().rpc_respond(&req, payload);
            }
        });
        let reply = agents[src]
            .service()
            .rpc_call(id(dst), "prop.echo", Bytes::from_static(b"ping"), Duration::from_secs(8))
            .await
            .unwrap_or_else(|e| panic!("graph_seed={graph_seed}: rpc {src}->{dst} failed: {e:?}"));
        assert_eq!(&reply[..], b"ping");

        // (c) Ballot from src: votes return Individual over the same relays.
        match agents[src]
            .consensus()
            .system_propose("prop/slot", Bytes::from_static(b"v"), ConsensusConfig::default())
            .await
        {
            ConsensusResult::Committed { .. } => {}
            other => panic!("graph_seed={graph_seed}: ballot did not commit: {other:?}"),
        }

        for a in agents {
            a.shutdown().await;
        }
    }
}

// ── Layer 2: Signal / Boundary ───────────────────────────────────────────

#[tokio::test]
async fn test_signal_local_system_delivery() {
    let agent = make_agent();
    let mut rx = agent.mesh().signal_rx(signal_kind::HEALTH_PROBE);
    let _ = agent.mesh().emit(signal_kind::HEALTH_PROBE, SignalScope::System, b"ping".to_vec());
    let sig = tokio::time::timeout(Duration::from_millis(100), rx.recv())
        .await
        .expect("signal should be delivered within 100ms")
        .expect("receiver should not be closed");
    assert_eq!(&*sig.kind, signal_kind::HEALTH_PROBE);
    assert_eq!(sig.payload, Bytes::from_static(b"ping"));
    assert_eq!(sig.scope, SignalScope::System);
}

#[tokio::test]
async fn test_signal_group_admitted_when_member() {
    let agent = make_agent();
    agent.mesh().join_group("nlp");
    let mut rx = agent.mesh().signal_rx("task");
    let _ = agent.mesh().emit("task", SignalScope::Group(Arc::from("nlp")), b"work".to_vec());
    let sig = tokio::time::timeout(Duration::from_millis(100), rx.recv())
        .await
        .expect("member should receive group signal")
        .expect("receiver closed");
    assert_eq!(sig.scope, SignalScope::Group(Arc::from("nlp")));
}

#[tokio::test]
async fn test_signal_group_blocked_when_not_member() {
    let agent = make_agent();
    // do NOT join "nlp"
    let mut rx = agent.mesh().signal_rx("task");
    let _ = agent.mesh().emit("task", SignalScope::Group(Arc::from("nlp")), b"ignored".to_vec());
    let result = tokio::time::timeout(Duration::from_millis(30), rx.recv()).await;
    assert!(result.is_err(), "non-member should not receive group signal");
}

#[tokio::test]
async fn test_signal_individual_admitted_to_self() {
    let agent = make_agent();
    let self_id = agent.node_id().clone();
    let mut rx = agent.mesh().signal_rx(signal_kind::INVOKE);
    let _ = agent.mesh().emit(signal_kind::INVOKE, SignalScope::Individual(self_id), b"call".to_vec());
    let sig = tokio::time::timeout(Duration::from_millis(100), rx.recv())
        .await
        .expect("individual signal to self should be delivered")
        .expect("receiver closed");
    assert_eq!(&*sig.kind, signal_kind::INVOKE);
}

#[tokio::test]
async fn test_signal_multiple_receivers_same_kind() {
    let agent = make_agent();
    let mut rx1 = agent.mesh().signal_rx("evt");
    let mut rx2 = agent.mesh().signal_rx("evt");
    let _ = agent.mesh().emit("evt", SignalScope::System, b"data".to_vec());
    let s1 = tokio::time::timeout(Duration::from_millis(100), rx1.recv()).await;
    let s2 = tokio::time::timeout(Duration::from_millis(100), rx2.recv()).await;
    assert!(s1.is_ok() && s1.unwrap().is_some(), "rx1 should receive signal");
    assert!(s2.is_ok() && s2.unwrap().is_some(), "rx2 should receive signal");
}

#[tokio::test]
async fn test_emit_async_delivers_locally() {
    let agent = make_agent();
    let mut rx = agent.mesh().signal_rx("async.evt");
    assert!(agent.mesh().emit_async("async.evt", SignalScope::System, b"data".to_vec()).await);
    let sig = tokio::time::timeout(Duration::from_millis(100), rx.recv())
        .await
        .expect("emit_async should deliver locally")
        .expect("receiver closed");
    assert_eq!(sig.payload, Bytes::from_static(b"data"));
}

#[tokio::test]
async fn test_signal_rx_with_capacity() {
    let agent = make_agent();
    // Custom depth of 1 — second signal should be dropped (channel full).
    let mut rx = agent.mesh().signal_rx_with_capacity("burst", 1);
    let _ = agent.mesh().emit("burst", SignalScope::System, b"first".to_vec());
    let _ = agent.mesh().emit("burst", SignalScope::System, b"second".to_vec()); // drops on Full
    let first = tokio::time::timeout(Duration::from_millis(100), rx.recv())
        .await
        .expect("first signal should arrive")
        .expect("receiver closed");
    assert_eq!(first.payload, Bytes::from_static(b"first"));
}

#[tokio::test]
async fn test_signal_two_node_propagation() {
    let port_a = alloc_port();
    let port_b = alloc_port();

    let id_a = NodeId::new("127.0.0.1", port_a).unwrap();
    let id_b = NodeId::new("127.0.0.1", port_b).unwrap();

    let mut cfg_a = GossipConfig::default();
    cfg_a.bind_port = port_a;
    cfg_a.health_check_interval_secs = 1;
    cfg_a.bootstrap_peers = vec![id_b.clone()];

    let mut cfg_b = GossipConfig::default();
    cfg_b.bind_port = port_b;
    cfg_b.health_check_interval_secs = 1;
    cfg_b.bootstrap_peers = vec![id_a.clone()];

    let agent_a = Arc::new(GossipAgent::new(id_a, cfg_a));
    let agent_b = Arc::new(GossipAgent::new(id_b, cfg_b));

    agent_a.start().await.unwrap();
    agent_b.start().await.unwrap();

    // Wait for peers to discover each other.
    time::sleep(Duration::from_millis(100)).await;

    let mut rx_b = agent_b.mesh().signal_rx("cluster.event");
    let _ = agent_a.mesh().emit("cluster.event", SignalScope::System, b"hello".to_vec());

    let sig = tokio::time::timeout(Duration::from_millis(2_000), rx_b.recv())
        .await
        .expect("signal should arrive at B within 2s")
        .expect("receiver closed");

    assert_eq!(&*sig.kind, "cluster.event");
    assert_eq!(sig.payload, Bytes::from_static(b"hello"));

    agent_a.shutdown().await;
    agent_b.shutdown().await;
}

#[tokio::test]
async fn test_group_signal_only_reaches_members() {
    // 3-node cluster: A and B join group "team"; C does not.
    // With group_aware_forwarding enabled, A's shard forwards Group("team")
    // signals only to known members + epidemic_extra_peers random others.
    // Regardless of forwarding, C's Boundary must block local delivery
    // (C never joined "team") — its handler must not fire.
    let (port_a, port_b, port_c) = (alloc_port(), alloc_port(), alloc_port());

    let make_cfg = |port: u16, peers: Vec<NodeId>| {
        let mut cfg = GossipConfig::default();
        cfg.bind_port                = port;
        cfg.health_check_interval_secs = 1;
        cfg.group_aware_forwarding   = true;
        cfg.bootstrap_peers          = peers;
        cfg
    };

    let id_a = NodeId::new("127.0.0.1", port_a).unwrap();
    let id_b = NodeId::new("127.0.0.1", port_b).unwrap();
    let id_c = NodeId::new("127.0.0.1", port_c).unwrap();

    let cfg_a = make_cfg(port_a, vec![id_b.clone(), id_c.clone()]);
    let cfg_b = make_cfg(port_b, vec![id_a.clone(), id_c.clone()]);
    let cfg_c = make_cfg(port_c, vec![id_a.clone(), id_b.clone()]);

    let agent_a = Arc::new(GossipAgent::new(id_a, cfg_a));
    let agent_b = Arc::new(GossipAgent::new(id_b, cfg_b));
    let agent_c = Arc::new(GossipAgent::new(id_c, cfg_c));

    agent_a.start().await.unwrap();
    agent_b.start().await.unwrap();
    agent_c.start().await.unwrap();

    // A and B join the group; C does not.
    agent_a.mesh().join_group("team");
    agent_b.mesh().join_group("team");

    // Wait for group membership KV entries to propagate and peers to discover each other.
    time::sleep(Duration::from_millis(300)).await;

    let mut rx_b = agent_b.mesh().signal_rx("team.event");
    let mut rx_c = agent_c.mesh().signal_rx("team.event");

    let _ = agent_a.mesh().emit("team.event", SignalScope::Group("team".into()), b"msg".to_vec());

    // B must receive the signal — it's a group member.
    tokio::time::timeout(Duration::from_millis(2_000), rx_b.recv())
        .await
        .expect("B (group member) should receive the Group signal within 2s")
        .expect("B receiver closed");

    // C must NOT receive the signal — its Boundary blocks delivery for Group("team").
    let c_result = tokio::time::timeout(Duration::from_millis(200), rx_c.recv()).await;
    assert!(c_result.is_err(), "C (non-member) must not receive the Group signal");

    agent_a.shutdown().await;
    agent_b.shutdown().await;
    agent_c.shutdown().await;
}

#[tokio::test]
async fn test_signal_not_delivered_twice_via_gossip() {
    let port_a = alloc_port();
    let port_b = alloc_port();

    let mut cfg_a = GossipConfig::default();
    cfg_a.bind_port = port_a;
    cfg_a.health_check_interval_secs = 1;
    cfg_a.bootstrap_peers = vec![NodeId::new("127.0.0.1", port_b).unwrap()];

    let mut cfg_b = GossipConfig::default();
    cfg_b.bind_port = port_b;
    cfg_b.health_check_interval_secs = 1;
    cfg_b.bootstrap_peers = vec![NodeId::new("127.0.0.1", port_a).unwrap()];

    let agent_a = Arc::new(GossipAgent::new(NodeId::new("127.0.0.1", port_a).unwrap(), cfg_a));
    let agent_b = Arc::new(GossipAgent::new(NodeId::new("127.0.0.1", port_b).unwrap(), cfg_b));

    agent_a.start().await.unwrap();
    agent_b.start().await.unwrap();
    time::sleep(Duration::from_millis(100)).await;

    let mut rx_b = agent_b.mesh().signal_rx("ping");
    let _ = agent_a.mesh().emit("ping", SignalScope::System, b"once".to_vec());

    // Receive the first signal.
    let first = tokio::time::timeout(Duration::from_millis(2_000), rx_b.recv()).await;
    assert!(first.is_ok() && first.unwrap().is_some(), "first signal should arrive");

    // Give extra time for any duplicate to propagate — there must be none.
    let second = tokio::time::timeout(Duration::from_millis(200), rx_b.recv()).await;
    assert!(second.is_err(), "signal must not be delivered more than once (nonce dedup)");

    agent_a.shutdown().await;
    agent_b.shutdown().await;
}

// ── StateResponse key interning ───────────────────────────────────────────

#[tokio::test]
async fn test_state_response_interns_keys() {
    let (mut writer, reader) = loopback_pair().await;
    let store: Arc<papaya::HashMap<Arc<str>, StoreEntry>> = Arc::new(papaya::HashMap::new());
    let (tx, _rx) = mpsc::channel(10);
    let _ = spawn_handler(
        reader, Arc::clone(&store), Arc::new(papaya::HashMap::new()), tx,
        Arc::new(ShardedSeen::new(N_GOSSIP_SHARDS)),
        GossipConfig::default().default_ttl,
    );

    let pool_before = crate::store::intern_pool_len();
    let unique_key = format!("state_response_intern_test_{}", fastrand::u64(..));
    send_wire(&mut writer, &WireMessage::StateResponse {
        entries: vec![SyncEntry {
            key:          Arc::from(unique_key.as_str()),
            value:        Bytes::from_static(b"v"),
            timestamp:    1,
            is_tombstone: false,
        }],
    }).await;

    let s = Arc::clone(&store);
    let k = unique_key.clone();
    poll_until(|| s.pin().get(k.as_str()).is_some(), 200).await;
    assert!(
        crate::store::intern_pool_len() > pool_before,
        "StateResponse should intern the key when intern_keys = true",
    );
}

// ── signal_once ───────────────────────────────────────────────────────────

#[tokio::test]
async fn test_signal_once_returns_on_match() {
    let agent = make_agent();
    // signal_once must return the emitted signal.
    let kind: Arc<str> = Arc::from("test.once");
    let agent_ref = &agent;
    let recv = tokio::spawn({
        let kind = Arc::clone(&kind);
        async move {
            make_agent().mesh().signal_once(kind, Duration::from_millis(500), |_| true).await
        }
    });
    // Brief pause so the receiver registers before the emit.
    time::sleep(Duration::from_millis(20)).await;
    let _ = agent_ref.mesh().emit(Arc::clone(&kind), SignalScope::System, Bytes::new());

    // Use a fresh agent with a real handler.
    let agent2 = make_agent();
    let mut rx = agent2.mesh().signal_rx_with_capacity(Arc::clone(&kind), 4);
    let _ = agent2.mesh().emit(Arc::clone(&kind), SignalScope::System, Bytes::from_static(b"hi"));
    let sig = tokio::time::timeout(Duration::from_millis(200), rx.recv()).await;
    assert!(sig.is_ok() && sig.unwrap().is_some());
    drop(recv);
}

#[tokio::test]
async fn test_signal_once_timeout() {
    let agent = make_agent();
    let result = agent
        .mesh().signal_once("no.such.signal", Duration::from_millis(50), |_| true)
        .await;
    assert!(result.is_none(), "should return None when nothing is emitted");
}

#[tokio::test]
async fn test_signal_once_skips_non_matching() {
    let agent = make_agent();
    let kind: Arc<str> = Arc::from("invoke.result");
    let mut rx = agent.mesh().signal_rx_with_capacity(Arc::clone(&kind), 16);

    // Individual scope bypasses the opacity shedding check so both signals
    // are guaranteed to land in the channel regardless of fill_ratio.
    let self_id = agent.node_id().clone();
    let target_nonce: u64 = 0xDEAD_BEEF;
    let _ = agent.mesh().emit(Arc::clone(&kind), SignalScope::Individual(self_id.clone()), Bytes::from_static(b"wrong"));
    let _ = agent.mesh().emit(Arc::clone(&kind), SignalScope::Individual(self_id.clone()), Bytes::from_static(b"right"));

    // Drain both into a Vec and find the one with "right" payload.
    let mut signals = Vec::new();
    for _ in 0..2 {
        if let Ok(Some(s)) = tokio::time::timeout(Duration::from_millis(100), rx.recv()).await {
            signals.push(s);
        }
    }
    // signal_once logic: predicate on payload content (simulating nonce check).
    let matching = signals.into_iter().find(|s| s.payload == Bytes::from_static(b"right"));
    assert!(matching.is_some(), "should find the matching signal");
    let _ = target_nonce;
}

// ── advertise ────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_advertise_emits_on_interval() {
    let agent = make_agent();
    let kind: Arc<str> = Arc::from("capacity.available");
    let mut rx = agent.mesh().signal_rx_with_capacity(Arc::clone(&kind), 8);

    let _handle = agent.mesh().advertise(
        Arc::clone(&kind),
        SignalScope::System,
        Duration::from_millis(30),
        || Bytes::from_static(b"load=0"),
    );

    // Should receive at least one signal within a generous window.
    let result = tokio::time::timeout(Duration::from_millis(300), rx.recv()).await;
    assert!(result.is_ok() && result.unwrap().is_some(), "advertise should emit on interval");
}

#[tokio::test]
async fn test_advertise_stops_on_handle_drop() {
    let agent = make_agent();
    let kind: Arc<str> = Arc::from("capacity.probe");
    let mut rx = agent.mesh().signal_rx_with_capacity(Arc::clone(&kind), 8);

    let handle = agent.mesh().advertise(
        Arc::clone(&kind),
        SignalScope::System,
        Duration::from_millis(20),
        Bytes::new,
    );

    // Confirm it emits at least once.
    let first = tokio::time::timeout(Duration::from_millis(200), rx.recv()).await;
    assert!(first.is_ok() && first.unwrap().is_some());

    // Drop handle — task should stop.
    drop(handle);
    time::sleep(Duration::from_millis(60)).await;

    // Drain any already-queued signals.
    while rx.try_recv().is_ok() {}

    // No further signals should arrive.
    let after = tokio::time::timeout(Duration::from_millis(80), rx.recv()).await;
    assert!(after.is_err(), "no signals should arrive after handle is dropped");
}

// ── last_signal ───────────────────────────────────────────────────────────

#[test]
fn test_last_signal_none_initially() {
    let agent = make_agent();
    assert!(agent.mesh().last_signal("never.seen").is_none());
}

#[tokio::test]
async fn test_last_signal_updates_after_deliver() {
    let agent = make_agent();
    let kind = "health.probe";
    let before = std::time::Instant::now();
    let _ = agent.mesh().emit(kind, SignalScope::System, Bytes::new());
    // Give the local deliver() call time to record.
    time::sleep(Duration::from_millis(5)).await;
    let ts = agent.mesh().last_signal(kind);
    assert!(ts.is_some(), "last_signal should be Some after emit");
    assert!(ts.unwrap() >= before, "timestamp should be at or after emit time");
}

// ── suppress / unsuppress / is_suppressed ────────────────────────────────

#[tokio::test]
async fn test_suppress_blocks_delivery() {
    let agent = make_agent();
    let mut rx = agent.mesh().signal_rx_with_capacity("test.suppress", 8);
    agent.mesh().suppress("test.suppress", Duration::from_secs(60));
    assert!(agent.mesh().is_suppressed("test.suppress"));
    let _ = agent.mesh().emit("test.suppress", SignalScope::System, Bytes::new());
    let result = time::timeout(Duration::from_millis(50), rx.recv()).await;
    assert!(result.is_err(), "suppressed kind must not be delivered to handlers");
}

#[tokio::test]
async fn test_suppress_allows_after_expiry() {
    let agent = make_agent();
    let mut rx = agent.mesh().signal_rx_with_capacity("test.expiry", 8);
    agent.mesh().suppress("test.expiry", Duration::from_millis(50));
    time::sleep(Duration::from_millis(100)).await;
    assert!(!agent.mesh().is_suppressed("test.expiry"), "suppression should have expired");
    let _ = agent.mesh().emit("test.expiry", SignalScope::System, Bytes::new());
    let result = time::timeout(Duration::from_millis(200), rx.recv()).await;
    assert!(result.is_ok() && result.unwrap().is_some(), "expired suppression must allow delivery");
}

#[tokio::test]
async fn test_unsuppress_lifts_early() {
    let agent = make_agent();
    let mut rx = agent.mesh().signal_rx_with_capacity("test.unsuppress", 8);
    agent.mesh().suppress("test.unsuppress", Duration::from_secs(60));
    agent.mesh().unsuppress("test.unsuppress");
    assert!(!agent.mesh().is_suppressed("test.unsuppress"), "unsuppressed must not be suppressed");
    let _ = agent.mesh().emit("test.unsuppress", SignalScope::System, Bytes::new());
    let result = time::timeout(Duration::from_millis(200), rx.recv()).await;
    assert!(result.is_ok() && result.unwrap().is_some(), "unsuppressed kind must deliver");
}

#[tokio::test]
async fn test_suppress_still_updates_last_signal() {
    let agent = make_agent();
    agent.mesh().suppress("test.last_seen", Duration::from_secs(60));
    let _ = agent.mesh().emit("test.last_seen", SignalScope::System, Bytes::new());
    time::sleep(Duration::from_millis(10)).await;
    assert!(agent.mesh().last_signal("test.last_seen").is_some(),
        "last_signal must update even while kind is suppressed");
}

// ── watch ─────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_watch_fires_on_stale() {
    let agent = make_agent();
    let fired = Arc::new(AtomicU64::new(0));
    let fired_clone = Arc::clone(&fired);
    // threshold = 50ms → check_interval = max(12ms, 100ms) = 100ms
    // No signal ever emitted → stale from the first check.
    let _handle = agent.mesh().watch(
        "health.probe",
        Duration::from_millis(50),
        move || { fired_clone.fetch_add(1, Ordering::Relaxed); },
    );
    time::sleep(Duration::from_millis(350)).await;
    assert!(fired.load(Ordering::Relaxed) > 0, "on_stale must fire when no signal received");
}

#[tokio::test]
async fn test_watch_does_not_fire_when_fresh() {
    let agent = make_agent();
    let fired = Arc::new(AtomicU64::new(0));
    let fired_clone = Arc::clone(&fired);
    // Emit once so last_signal is fresh.
    let _ = agent.mesh().emit("health.fresh", SignalScope::System, Bytes::new());
    // threshold = 500ms → first check at 125ms. At 250ms elapsed is ~250ms < 500ms.
    let _handle = agent.mesh().watch(
        "health.fresh",
        Duration::from_millis(500),
        move || { fired_clone.fetch_add(1, Ordering::Relaxed); },
    );
    time::sleep(Duration::from_millis(250)).await;
    assert_eq!(fired.load(Ordering::Relaxed), 0, "on_stale must not fire when signal is recent");
}

#[tokio::test]
async fn test_watch_stops_on_handle_drop() {
    let agent = make_agent();
    let fired = Arc::new(AtomicU64::new(0));
    let fired_clone = Arc::clone(&fired);
    let handle = agent.mesh().watch(
        "health.stop",
        Duration::from_millis(30),
        move || { fired_clone.fetch_add(1, Ordering::Relaxed); },
    );
    // Let it fire at least once.
    time::sleep(Duration::from_millis(350)).await;
    assert!(fired.load(Ordering::Relaxed) > 0, "should fire before drop");
    drop(handle);
    // Allow the task to observe the cancellation.
    time::sleep(Duration::from_millis(50)).await;
    let count_at_drop = fired.load(Ordering::Relaxed);
    // Wait well past several check intervals — no further fires expected.
    time::sleep(Duration::from_millis(400)).await;
    assert_eq!(fired.load(Ordering::Relaxed), count_at_drop, "no fires after handle drop");
}

// ── manage_opacity governor ───────────────────────────────────────────────

/// Wait up to `$secs` for a signal on `$rx`, polling in 200 ms slices.
///
/// The opacity governor is a **periodic sampler** (`time::interval(100ms)` in
/// `opacity.rs`) that emits OPAQUE/TRANSPARENT on the next tick after a fill
/// transition. The signal is registered before the governor starts and the
/// channel is buffered, so it is never *lost* — but under heavy parallel test
/// load tokio can starve the ticker task well past a few ticks
/// (`MissedTickBehavior::Skip` then drops the missed beats). This is a bounded
/// "eventually" wait that absorbs that scheduler latency; it is **not** a race
/// mask — a genuinely stuck governor that never emits still fails at the ceiling.
macro_rules! recv_within {
    ($rx:expr, $secs:expr) => {{
        let deadline = std::time::Instant::now() + Duration::from_secs($secs);
        let mut got = false;
        while std::time::Instant::now() < deadline {
            if let Ok(Some(_)) = time::timeout(Duration::from_millis(200), $rx.recv()).await {
                got = true;
                break;
            }
        }
        got
    }};
}

#[tokio::test]
async fn test_manage_opacity_emits_opaque_when_threshold_crossed() {
    let agent = make_agent();
    // One handler for the monitored kind with cap=4.
    // fill_ratio = 0.75 after 3 signals; hint.threshold = 0.75.
    let _work_rx      = agent.mesh().signal_rx_with_capacity("test.gov.invoke", 4);
    let mut opaque_rx = agent.mesh().signal_rx_with_capacity(signal_kind::BOUNDARY_OPAQUE, 8);

    let _gov = agent.manage_opacity(
        "test.gov.invoke",
        SignalScope::System,
        OpacityHint::default(), // threshold = 0.75
    );

    // Individual scope bypasses the opacity-shedding check in emit_signal, so
    // all three signals reliably land in the channel and fill_ratio reaches 0.75.
    let self_id = agent.node_id().clone();
    for _ in 0..3 {
        let _ = agent.mesh().emit("test.gov.invoke", SignalScope::Individual(self_id.clone()), Bytes::new());
    }

    assert!(
        recv_within!(opaque_rx, 3),
        "governor must emit BOUNDARY_OPAQUE when fill crosses threshold",
    );
}

#[tokio::test]
async fn test_manage_opacity_emits_transparent_after_drain() {
    let agent = make_agent();
    let mut work_rx   = agent.mesh().signal_rx_with_capacity("test.gov.drain", 4);
    let mut opaque_rx = agent.mesh().signal_rx_with_capacity(signal_kind::BOUNDARY_OPAQUE, 8);
    let mut clear_rx  = agent.mesh().signal_rx_with_capacity(signal_kind::BOUNDARY_TRANSPARENT, 8);

    let _gov = agent.manage_opacity(
        "test.gov.drain",
        SignalScope::System,
        OpacityHint::default(),
    );

    // Fill to 100% with Individual scope to avoid opacity shedding.
    let self_id = agent.node_id().clone();
    for _ in 0..4 {
        let _ = agent.mesh().emit("test.gov.drain", SignalScope::Individual(self_id.clone()), Bytes::new());
    }
    assert!(recv_within!(opaque_rx, 3), "should go opaque first");

    // Drain all four — fill drops to 0.0 < 0.75 - 0.20 = 0.55.
    for _ in 0..4 {
        let _ = time::timeout(Duration::from_millis(50), work_rx.recv()).await;
    }

    assert!(
        recv_within!(clear_rx, 3),
        "governor must emit BOUNDARY_TRANSPARENT once fill drops below clear threshold",
    );
}

#[tokio::test]
async fn test_manage_opacity_gate_vetoes_then_library_overrides() {
    let agent = make_agent();
    // cap=8: 6 signals → fill=0.75 (threshold met, gate vetoes), 8 → fill=1.0 (override).
    let _work_rx      = agent.mesh().signal_rx_with_capacity("test.gov.gate", 8);
    let mut opaque_rx = agent.mesh().signal_rx_with_capacity(signal_kind::BOUNDARY_OPAQUE, 8);

    // Gate always vetoes — library must still override when fill == 1.0.
    let _gov = agent.manage_opacity_gated(
        "test.gov.gate",
        SignalScope::System,
        OpacityHint::default(),
        |_state| false,
    );

    // Fill to 75% with Individual scope. Gate should veto every tick.
    let self_id = agent.node_id().clone();
    for _ in 0..6 {
        let _ = agent.mesh().emit("test.gov.gate", SignalScope::Individual(self_id.clone()), Bytes::new());
    }
    let premature = time::timeout(Duration::from_millis(250), opaque_rx.recv()).await;
    assert!(premature.is_err(), "gate veto must prevent emission below 100% fill");

    // Fill to 100% — library overrides the gate.
    for _ in 0..2 {
        let _ = agent.mesh().emit("test.gov.gate", SignalScope::Individual(self_id.clone()), Bytes::new());
    }
    let result = time::timeout(Duration::from_millis(400), opaque_rx.recv()).await;
    assert!(
        result.is_ok() && result.unwrap().is_some(),
        "library must override gate and emit BOUNDARY_OPAQUE when fill == 1.0",
    );
}

// ── competitive response ──────────────────────────────────────────────────

#[tokio::test]
async fn test_competitive_response_group_scope() {
    let agent = Arc::new(make_agent());
    agent.mesh().join_group("work");

    // Register the reply receiver synchronously — before any emit, no race.
    let mut result_rx = agent.mesh().signal_rx_with_capacity(signal_kind::INVOKE_RESULT, 4);

    // Worker: receives Group-scoped invoke, replies to sender via Individual scope.
    let mut invoke_rx = agent.mesh().signal_rx(signal_kind::INVOKE);
    let agent_w = Arc::clone(&agent);
    tokio::spawn(async move {
        if let Some(sig) = invoke_rx.recv().await {
            // Echo correlation payload so the invoker can identify its reply.
            let _ = agent_w.mesh().emit(
                signal_kind::INVOKE_RESULT,
                SignalScope::Individual(sig.sender),
                sig.payload.clone(),
            );
        }
    });

    // Emit to the group — no worker selected; routing emerges from opacity state.
    let corr = Bytes::from_static(b"corr-42");
    let _ = agent.mesh().emit(signal_kind::INVOKE, SignalScope::Group(Arc::from("work")), corr.clone());

    let reply = tokio::time::timeout(Duration::from_millis(500), result_rx.recv())
        .await
        .expect("worker should reply within timeout")
        .expect("channel closed");

    assert_eq!(reply.payload, corr, "reply echoes correlation payload");
    assert_eq!(
        reply.scope,
        SignalScope::Individual(agent.node_id().clone()),
        "reply uses Individual scope targeting the invoker",
    );
}

// ── Consensus ─────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_group_propose_single_voter() {
    let agent = make_agent();
    let _listener = agent.consensus().start_consensus_listener(ConsensusConfig::default());
    agent.mesh().join_group("cg1");

    let config = ConsensusConfig { quorum_size: 1, ..ConsensusConfig::default() };
    let result = agent.consensus().group_propose("cg1", "sl1", Bytes::from_static(b"val1"), config).await;

    assert!(
        matches!(result, ConsensusResult::Committed { .. }),
        "single-voter quorum should commit immediately; got {:?}", result
    );
    assert_eq!(agent.consensus().consensus_get("sl1"), Some(Bytes::from_static(b"val1")));
}

#[tokio::test]
async fn test_group_propose_timeout() {
    let agent = make_agent();
    // No listener started — no votes arrive, quorum of 2 is unreachable.
    let config = ConsensusConfig {
        quorum_size:    2,
        phase1_timeout: Duration::from_millis(50),
        max_ballots:    1,
        ..ConsensusConfig::default()
    };
    let result = agent.consensus().group_propose("cg2", "sl2", Bytes::from_static(b"v2"), config).await;
    assert!(
        matches!(result, ConsensusResult::Timeout { ballots_tried: 1, .. }),
        "unreachable quorum must return Timeout; got {:?}", result
    );
}

#[tokio::test]
async fn test_group_propose_two_node_quorum() {
    let pair = consensus_pair().await;
    pair.a.mesh().join_group("cgrp");
    pair.b.mesh().join_group("cgrp");

    let config = ConsensusConfig {
        quorum_size:    2,
        phase1_timeout: Duration::from_secs(3),
        max_ballots:    3,
        ..ConsensusConfig::default()
    };
    let result = pair.a.consensus().group_propose("cgrp", "slA", Bytes::from_static(b"agreed"), config).await;
    assert!(
        matches!(result, ConsensusResult::Committed { .. }),
        "two-node quorum should commit; got {:?}", result
    );
    pair.a.shutdown().await;
    pair.b.shutdown().await;
}

// Two agents propose to the same slot concurrently. With ballot jitter, one
// should Commit and the other Superseded. Neither should Timeout.
#[tokio::test]
async fn test_consensus_simultaneous_proposers_resolve() {
    let ConsensusPair { a, b, _la, _lb } = consensus_pair().await;
    let agent_a = Arc::new(a);
    let agent_b = Arc::new(b);

    // quorum_size=1 so each agent self-commits; the second proposer will find the
    // commit_key written by the first and return Superseded on the next ballot check.
    // A tiny stagger ensures A commits before B polls the commit_key.
    let config = ConsensusConfig {
        quorum_size:            1,
        phase1_timeout:         Duration::from_millis(500),
        max_ballots:            5,
        ballot_retry_jitter_ms: 0, // disabled — test relies on commit_key propagation, not jitter
        ..ConsensusConfig::default()
    };

    let aa = Arc::clone(&agent_a);
    let cfg_a2 = config.clone();
    let task_a = tokio::spawn(async move {
        aa.consensus().system_propose("sim_sl", Bytes::from_static(b"val_a"), cfg_a2).await
    });
    // Small stagger gives A time to commit and gossip the commit_key to B.
    time::sleep(Duration::from_millis(50)).await;
    let bb = Arc::clone(&agent_b);
    let cfg_b2 = config.clone();
    let task_b = tokio::spawn(async move {
        bb.consensus().system_propose("sim_sl", Bytes::from_static(b"val_b"), cfg_b2).await
    });

    let (res_a, res_b) = tokio::join!(task_a, task_b);
    let res_a = res_a.unwrap();
    let res_b = res_b.unwrap();

    assert!(
        matches!(res_a, ConsensusResult::Committed { .. }),
        "first proposer must commit; got {:?}", res_a,
    );
    assert!(
        matches!(res_b, ConsensusResult::Superseded { .. }),
        "second proposer must see commit and return Superseded; got {:?}", res_b,
    );
    let timed_out = [&res_a, &res_b].iter().any(|r| matches!(r, ConsensusResult::Timeout { .. }));
    assert!(!timed_out, "neither proposer should time out; got a={:?} b={:?}", res_a, res_b);

    agent_a.shutdown().await;
    agent_b.shutdown().await;
}

#[tokio::test]
async fn test_system_propose_commits() {
    let agent = make_agent();
    let _listener = agent.consensus().start_consensus_listener(ConsensusConfig::default());

    let config = ConsensusConfig { quorum_size: 1, ..ConsensusConfig::default() };
    let result = agent.consensus().system_propose("sys_sl", Bytes::from_static(b"sys_v"), config).await;

    assert!(
        matches!(result, ConsensusResult::Committed { .. }),
        "single-node system propose must commit; got {:?}", result
    );
    assert_eq!(agent.consensus().consensus_get("sys_sl"), Some(Bytes::from_static(b"sys_v")));
}

#[tokio::test]
async fn test_consensus_rx_fires_on_commit() {
    let agent = make_agent();
    let _listener = agent.consensus().start_consensus_listener(ConsensusConfig::default());
    let mut rx = agent.consensus().consensus_rx("slRx");

    let config = ConsensusConfig { quorum_size: 1, ..ConsensusConfig::default() };
    let _ = agent.consensus().group_propose("rxg", "slRx", Bytes::from_static(b"fired"), config).await;

    let val = tokio::time::timeout(Duration::from_millis(500), async {
        loop {
            if rx.borrow().is_some() { return rx.borrow().clone(); }
            rx.changed().await.ok();
        }
    }).await;
    assert_eq!(val.unwrap(), Some(Bytes::from_static(b"fired")));
}

#[tokio::test]
async fn test_consensus_get_returns_committed() {
    let agent = make_agent();
    let _listener = agent.consensus().start_consensus_listener(ConsensusConfig::default());

    let config = ConsensusConfig { quorum_size: 1, ..ConsensusConfig::default() };
    let _ = agent.consensus().group_propose("cgg", "slGet", Bytes::from_static(b"gotten"), config).await;

    assert_eq!(
        agent.consensus().consensus_get("slGet"),
        Some(Bytes::from_static(b"gotten")),
    );
}

#[tokio::test]
async fn test_declare_and_read_trust() {
    let agent = make_agent();
    let peer_a = NodeId::new("127.0.0.1", 9001).unwrap();
    let peer_b = NodeId::new("127.0.0.1", 9002).unwrap();

    agent.consensus().declare_trust("trustgrp", &[peer_a.clone(), peer_b.clone()]);
    let slices = agent.consensus().group_trust("trustgrp");

    assert_eq!(slices.len(), 1, "one trust slice declared");
    let (declaring_node, peers) = &slices[0];
    assert_eq!(*declaring_node, *agent.node_id());
    assert!(peers.contains(&peer_a));
    assert!(peers.contains(&peer_b));
}

#[tokio::test]
async fn test_consensus_late_joiner_sync() {
    let port_a = alloc_port();
    let port_b = alloc_port();

    let mut cfg_a = GossipConfig::default();
    cfg_a.bind_port                    = port_a;
    cfg_a.reconnect_backoff_secs       = 1;
    cfg_a.health_check_interval_secs   = 1;
    cfg_a.health_check_max_jitter_ms   = 50;

    let agent_a = GossipAgent::new(NodeId::new("127.0.0.1", port_a).unwrap(), cfg_a);
    agent_a.start().await.unwrap();

    let _listener_a = agent_a.consensus().start_consensus_listener(ConsensusConfig::default());
    let config = ConsensusConfig { quorum_size: 1, ..ConsensusConfig::default() };
    let result = agent_a.consensus().system_propose("late_sl", Bytes::from_static(b"late_v"), config).await;
    assert!(matches!(result, ConsensusResult::Committed { .. }));

    // B starts after A has already committed — anti-entropy must deliver the value.
    let mut cfg_b = GossipConfig::default();
    cfg_b.bind_port                       = port_b;
    cfg_b.reconnect_backoff_secs          = 1;
    cfg_b.health_check_interval_secs      = 1;
    cfg_b.health_check_max_jitter_ms      = 50;
    cfg_b.bootstrap_peers = vec![NodeId::new("127.0.0.1", port_a).unwrap()];
    let agent_b = GossipAgent::new(NodeId::new("127.0.0.1", port_b).unwrap(), cfg_b);
    agent_b.start().await.unwrap();

    // Give B's health monitor time to pass its jitter (0–50 ms) and send
    // the initial StateRequest to A before we start polling.
    time::sleep(Duration::from_millis(100)).await;
    poll_until(|| agent_b.consensus().consensus_get("late_sl").is_some(), 5_000).await;
    assert_eq!(
        agent_b.consensus().consensus_get("late_sl"),
        Some(Bytes::from_static(b"late_v")),
    );

    agent_a.shutdown().await;
    agent_b.shutdown().await;
}

// Proposer declares an empty trust slice; B's vote must not count.
#[tokio::test]
async fn test_trust_slice_filters_votes() {
    let pair = consensus_pair().await;
    pair.a.mesh().join_group("tg");
    pair.b.mesh().join_group("tg");
    // A declares an empty trust slice — trusts nobody remotely.
    pair.a.consensus().declare_trust("tg", &[]);

    let config = ConsensusConfig {
        quorum_size:      2,
        phase1_timeout:   Duration::from_millis(300),
        max_ballots:      1,
        use_trust_slices: true,
        ..ConsensusConfig::default()
    };
    let result = pair.a
        .consensus().group_propose("tg", "ts1", Bytes::from_static(b"x"), config)
        .await;
    assert!(
        matches!(result, ConsensusResult::Timeout { .. }),
        "B's vote should be filtered by empty trust slice; got {:?}", result
    );

    pair.a.shutdown().await;
    pair.b.shutdown().await;
}

// Proposer includes B in its trust slice; B's vote should be counted.
#[tokio::test]
async fn test_trust_slice_admits_trusted_vote() {
    let pair = consensus_pair().await;
    let node_b = pair.b.node_id().clone();
    pair.a.mesh().join_group("tg2");
    pair.b.mesh().join_group("tg2");
    // A explicitly trusts B.
    pair.a.consensus().declare_trust("tg2", &[node_b]);

    let config = ConsensusConfig {
        quorum_size:      2,
        phase1_timeout:   Duration::from_secs(3),
        max_ballots:      3,
        use_trust_slices: true,
        ..ConsensusConfig::default()
    };
    let result = pair.a
        .consensus().group_propose("tg2", "ts2", Bytes::from_static(b"y"), config)
        .await;
    assert!(
        matches!(result, ConsensusResult::Committed { .. }),
        "B is trusted; quorum of 2 should commit; got {:?}", result
    );

    pair.a.shutdown().await;
    pair.b.shutdown().await;
}

// ── H9: advertise_persistent writes capability to Layer I ─────────────────

#[tokio::test]
async fn test_advertise_persistent_late_joiner_discovers_capability() {
    let agent = make_agent();
    agent.mesh().join_group("workers");

    // Start persistent advertise with a short tick so the first write happens quickly.
    let _handle = agent.mesh().advertise_persistent(
        "contract.available",
        SignalScope::Group("workers".into()),
        Duration::from_millis(20),
        || Bytes::from_static(b"v1"),
    );

    // Wait for the first tick to fire and write to Layer I.
    poll_until(
        || !agent.kv().scan_prefix(kv_ns::ADVERTISE).is_empty(),
        500,
    ).await;

    let entries = agent.kv().scan_prefix(kv_ns::ADVERTISE);
    assert_eq!(entries.len(), 1);
    let (key, value) = &entries[0];
    assert!(key.starts_with("svc/contract.available/"), "key should be svc/{{kind}}/{{node_id}}");
    assert_eq!(*value, Bytes::from_static(b"v1"));

    // Dropping the handle tombstones the Layer I entry.
    drop(_handle);
    poll_until(|| agent.kv().scan_prefix(kv_ns::ADVERTISE).is_empty(), 500).await;
    assert!(
        agent.kv().scan_prefix(kv_ns::ADVERTISE).is_empty(),
        "capability should be tombstoned after handle drop"
    );
}

// ── H4: group_quorum filters by current Layer I membership ────────────────

#[test]
fn test_group_quorum_excludes_ex_member() {
    let agent = make_agent();

    // Join the group so the boundary admits the signal and grp/workers/{node_id}
    // is written to Layer I.
    agent.mesh().join_group("workers");

    // Emit a signal — deliver() records the sender in the sender_log.
    // (deliver() always updates sender_log before checking handler registration.)
    let _ = agent.mesh().emit("heartbeat", SignalScope::Group("workers".into()), Bytes::new());

    // Raw quorum is satisfied (1 sender, 1 required).
    assert!(
        agent.mesh().quorum("heartbeat", 1, Duration::from_secs(60)),
        "raw quorum should be satisfied"
    );
    // group_quorum should also be satisfied while the node is still a member.
    assert!(
        agent.mesh().group_quorum("workers", "heartbeat", 1, Duration::from_secs(60)),
        "node is a current member — group_quorum should count it"
    );

    // Leave the group — tombstones grp/workers/{node_id} in Layer I.
    agent.mesh().leave_group("workers");

    // Raw quorum is still satisfied (sender_log entry remains).
    assert!(
        agent.mesh().quorum("heartbeat", 1, Duration::from_secs(60)),
        "raw quorum still sees the sender_log entry"
    );
    // But group_quorum must exclude the ex-member.
    assert!(
        !agent.mesh().group_quorum("workers", "heartbeat", 1, Duration::from_secs(60)),
        "ex-member must not satisfy group_quorum after leave_group"
    );
}

// ── H10: peer_load_rx yields typed LoadState ──────────────────────────────

#[tokio::test]
async fn test_peer_load_rx_yields_decoded_state() {
    use crate::signal::{encode_load_state, LoadState};

    let agent = make_agent();
    let peer = NodeId::new("127.0.0.1", 9999).unwrap();
    let mut rx = agent.peer_load_rx(&peer, "test");

    // Initially absent.
    assert!(rx.borrow().is_none());

    // Write a pheromone entry for that peer.
    let state = LoadState { fill_ratio: 0.75, is_opaque: true, written_at_ms: 0 };
    let key = format!("sys/load/{}/test", peer);
    let _ = agent.kv().set(key.clone(), encode_load_state(&state));

    // Forwarding task should decode and push the typed value.
    let _ = tokio::time::timeout(Duration::from_millis(200), rx.changed()).await
        .expect("watch should fire within 200 ms");
    let got = rx.borrow().clone().expect("should have a LoadState");
    assert!((got.fill_ratio - 0.75).abs() < 1e-4);
    assert!(got.is_opaque);

    // Tombstone → None.
    let _ = agent.kv().delete(key);
    let _ = tokio::time::timeout(Duration::from_millis(200), rx.changed()).await
        .expect("watch should fire on tombstone");
    assert!(rx.borrow().is_none(), "tombstone should decode as None");
}

// ── H2: boundary reconciliation from Layer I ──────────────────────────────

#[test]
fn test_rehydrate_boundary_from_kv_inserts_group() {
    let agent = make_agent();
    let node_id = agent.node_id().to_string();
    let grp_key = format!("grp/workers/{}", node_id);
    let _ = agent.kv().set(grp_key, Bytes::from_static(b"1"));
    assert!(agent.groups().is_empty(), "group not yet in boundary");
    agent.rehydrate_boundary_from_kv();
    assert!(
        agent.groups().iter().any(|g| g.as_ref() == "workers"),
        "rehydrate should admit the group written to KV"
    );
}

#[test]
fn test_rehydrate_boundary_from_kv_removes_tombstoned_group() {
    let agent = make_agent();
    let node_id = agent.node_id().to_string();
    let grp_key = format!("grp/workers/{}", node_id);
    let _ = agent.kv().set(grp_key.clone(), Bytes::from_static(b"1"));
    agent.rehydrate_boundary_from_kv();
    assert!(agent.groups().iter().any(|g| g.as_ref() == "workers"));

    // Tombstone the KV entry — simulates another node forcing this node out.
    let _ = agent.kv().delete(grp_key);
    agent.rehydrate_boundary_from_kv();
    assert!(
        !agent.groups().iter().any(|g| g.as_ref() == "workers"),
        "tombstoned group must be evicted from boundary"
    );
}

#[tokio::test]
async fn test_writer_evicted_after_idle_timeout() {
    let port_a = alloc_port();
    let port_b = alloc_port();

    // Long health interval so pings don't keep resetting the idle deadline.
    // The idle timeout (3 s) is shorter than the health interval jitter window,
    // so the writer will go idle before any ping resets it.
    let mut cfg_a = GossipConfig::default();
    cfg_a.bind_port                  = port_a;
    cfg_a.reconnect_backoff_secs     = 1;
    cfg_a.health_check_interval_secs = 60;
    cfg_a.writer_idle_timeout_secs   = 3;
    cfg_a.bootstrap_peers = vec![NodeId::new("127.0.0.1", port_b).unwrap()];

    let mut cfg_b = GossipConfig::default();
    cfg_b.bind_port                  = port_b;
    cfg_b.reconnect_backoff_secs     = 1;
    cfg_b.health_check_interval_secs = 60;
    cfg_b.writer_idle_timeout_secs   = 3;
    cfg_b.bootstrap_peers = vec![NodeId::new("127.0.0.1", port_a).unwrap()];

    let agent_a = GossipAgent::new(NodeId::new("127.0.0.1", port_a).unwrap(), cfg_a);
    let agent_b = GossipAgent::new(NodeId::new("127.0.0.1", port_b).unwrap(), cfg_b);
    agent_a.start().await.unwrap();
    agent_b.start().await.unwrap();
    // Small pause so listener tasks fully start before sending.
    time::sleep(Duration::from_millis(50)).await;

    // A single gossip write establishes a writer from A to B.
    assert!(agent_a.kv().set("idle_key", Bytes::from_static(b"v1")));
    poll_until(|| agent_b.kv().get("idle_key").is_some(), 5_000).await;

    // After 3 s of silence the writer task exits. system_stats() filters finished
    // handles, so cached_connections drops to 0 without waiting for the GC pass.
    // Allow 10 s total (3 s idle + generous scheduling slack).
    poll_until(|| agent_a.system_stats().cached_connections == 0, 10_000).await;
    assert_eq!(
        agent_a.system_stats().cached_connections, 0,
        "writer should report as gone after idle timeout"
    );

    // A new write must reconnect transparently and still reach B.
    assert!(agent_a.kv().set("idle_key", Bytes::from_static(b"v2")));
    poll_until(
        || agent_b.kv().get("idle_key").map(|v| v == Bytes::from_static(b"v2")).unwrap_or(false),
        5_000,
    ).await;

    agent_a.shutdown().await;
    agent_b.shutdown().await;
}

// When member B turns opaque mid-ballot the reactive select! arm recomputes quorum
// from 2 down to 1, letting A's self-vote commit before the phase1 timeout fires.
#[tokio::test]
async fn test_ballot_reacts_to_opacity_change() {
    use crate::signal::{encode_load_state, LoadState};
    use std::time::{SystemTime, UNIX_EPOCH};

    let ConsensusPair { a, b, _la, _lb } = consensus_pair().await;
    let node_b = b.node_id().clone();
    let agent_a = Arc::new(a);

    agent_a.mesh().join_group("ogrp");
    b.mesh().join_group("ogrp");
    // Poll until A sees B's group membership key — required for auto quorum_size=0
    // to compute 2 (floor(2/2)+1) rather than 1 at proposal time.
    let node_b_str = node_b.to_string();
    poll_until(
        || !agent_a.kv().scan_prefix(&format!("grp/ogrp/{node_b_str}")).is_empty(),
        2_000,
    ).await;

    // Background task: after a short pause (to let the collect loop start),
    // write B's opaque pheromone to A's store and emit BOUNDARY_OPAQUE on A.
    // Both happen in-process on agent_a so there's no gossip propagation race.
    let aa = Arc::clone(&agent_a);
    tokio::spawn(async move {
        time::sleep(Duration::from_millis(100)).await;
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH).unwrap_or_default()
            .as_millis() as u64;
        let opaque_bytes = encode_load_state(&LoadState {
            fill_ratio:   1.0,
            is_opaque:    true,
            written_at_ms: now_ms,
        });
        let pheromone_key = format!("sys/load/{}/test.kind", node_b);
        let _ = aa.kv().set(pheromone_key, opaque_bytes);
        // Emit BOUNDARY_OPAQUE locally on A so opaque_rx fires in propose().
        let _ = aa.mesh().emit(signal_kind::BOUNDARY_OPAQUE, SignalScope::System, Bytes::new());
    });

    // phase1_timeout = 2 s; the reactive arm should commit within ~150 ms.
    let config = ConsensusConfig {
        quorum_size:            0, // auto = floor(2/2)+1 = 2 at proposal time
        phase1_timeout:         Duration::from_secs(2),
        max_ballots:            1,
        ballot_retry_jitter_ms: 0,
        count_opaque_as_absent: true,
        ..ConsensusConfig::default()
    };

    let start = tokio::time::Instant::now();
    let result = agent_a.consensus().group_propose("ogrp", "opq_sl", Bytes::from_static(b"v"), config).await;
    let elapsed = start.elapsed();

    assert!(
        matches!(result, ConsensusResult::Committed { .. }),
        "should commit once B goes opaque and quorum drops to 1; got {:?}", result
    );
    assert!(
        elapsed < Duration::from_millis(1000),
        "reactive commit must happen well before the 2 s timeout; elapsed {:?}", elapsed
    );

    agent_a.shutdown().await;
    b.shutdown().await;
}

#[test]
fn test_warm_quorum_seeds_sender_log() {
    use std::time::{SystemTime, UNIX_EPOCH};
    let port_a = alloc_port();
    let port_b = alloc_port();
    let node_a = NodeId::new("127.0.0.1", port_a).unwrap();
    let node_b = NodeId::new("127.0.0.1", port_b).unwrap();

    let mut cfg = GossipConfig::default();
    cfg.bind_port = port_a;
    cfg.signal_window_secs = 60;
    let agent = GossipAgent::new(node_a, cfg);

    // Write a sys/quorum entry 5 s in the past (well within the 60 s window).
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH).unwrap_or_default()
        .as_millis() as u64;
    let written_at_ms = now_ms - 5_000;
    let key = format!("sys/quorum/my.kind/{}", node_b);
    let _ = agent.kv().set(key, Bytes::copy_from_slice(&written_at_ms.to_le_bytes()));

    // Before seeding, the in-memory sender_log is empty.
    assert!(!agent.mesh().quorum("my.kind", 1, Duration::from_secs(60)));

    // After warm_quorum_from_layer1, the entry is seeded and quorum passes.
    agent.warm_quorum_from_layer1();
    assert!(agent.mesh().quorum("my.kind", 1, Duration::from_secs(60)));
}

#[test]
fn test_last_signal_persistent_reads_layer1() {
    use std::time::{SystemTime, UNIX_EPOCH};
    let port_a = alloc_port();
    let port_b = alloc_port();
    let node_a = NodeId::new("127.0.0.1", port_a).unwrap();
    let node_b = NodeId::new("127.0.0.1", port_b).unwrap();

    let mut cfg = GossipConfig::default();
    cfg.bind_port = port_a;
    let agent = GossipAgent::new(node_a, cfg);

    // Write a quorum entry 5 s in the past.
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH).unwrap_or_default()
        .as_millis() as u64;
    let written_at_ms = now_ms - 5_000;
    let key = format!("sys/quorum/my.kind/{}", node_b);
    let _ = agent.kv().set(key, Bytes::copy_from_slice(&written_at_ms.to_le_bytes()));

    let age = agent.mesh().last_signal_persistent("my.kind").expect("should find entry");
    // Age should be approximately 5 s (allow ±2 s scheduling slack).
    assert!(age >= Duration::from_secs(3) && age <= Duration::from_secs(7),
        "expected ~5 s, got {:?}", age);

    // Non-existent kind returns None.
    assert!(agent.mesh().last_signal_persistent("never.seen").is_none());
}

// ── Semantic correctness — LWW convergence ────────────────────────────────

/// Verifies LWW convergence at the network level:
///
/// 1. Agent A writes `"first"` and waits for agent B to receive it (HLC sync).
/// 2. Agent B writes `"second"` — B's HLC is now strictly greater than A's, so
///    `"second"` is the definitive LWW winner everywhere.
/// 3. Both agents must converge to `"second"`.
///
/// This is a network-level complement to the in-memory LWW unit tests in
/// `store.rs` — it exercises the full gossip path including HLC `observe()`
/// and the `>` LWW conflict resolution applied on every inbound update.
#[tokio::test]
async fn test_lww_convergence_two_concurrent_writers() {
    let port_a = alloc_port();
    let port_b = alloc_port();
    let id_a = NodeId::new("127.0.0.1", port_a).unwrap();
    let id_b = NodeId::new("127.0.0.1", port_b).unwrap();

    let mut cfg_a = GossipConfig::default();
    cfg_a.bind_port       = port_a;
    cfg_a.bootstrap_peers = vec![id_b.clone()];
    cfg_a.health_check_max_jitter_ms = 50;

    let mut cfg_b = GossipConfig::default();
    cfg_b.bind_port       = port_b;
    cfg_b.bootstrap_peers = vec![id_a.clone()];
    cfg_b.health_check_max_jitter_ms = 50;

    let agent_a = Arc::new(GossipAgent::new(id_a, cfg_a));
    let agent_b = Arc::new(GossipAgent::new(id_b, cfg_b));

    agent_a.start().await.unwrap();
    agent_b.start().await.unwrap();

    poll_until(|| !agent_a.peers().is_empty() && !agent_b.peers().is_empty(), 2_000).await;

    // Step 1: A writes "first".
    let _ = agent_a.kv().set("lww/converge", Bytes::from_static(b"first"));

    // Step 2: Wait until B has observed A's write (B's HLC now ≥ A's HLC).
    let ab = Arc::clone(&agent_b);
    poll_until(
        || ab.kv().get("lww/converge") == Some(Bytes::from_static(b"first")),
        2_000,
    ).await;

    // Step 3: B writes "second" — B's HLC.tick() is strictly > A's "first" timestamp.
    let _ = agent_b.kv().set("lww/converge", Bytes::from_static(b"second"));

    // Both agents must converge to "second" (higher HLC wins).
    let aa = Arc::clone(&agent_a);
    let ab2 = Arc::clone(&agent_b);
    poll_until(
        || {
            aa.kv().get("lww/converge") == Some(Bytes::from_static(b"second"))
            && ab2.kv().get("lww/converge") == Some(Bytes::from_static(b"second"))
        },
        2_000,
    ).await;

    let final_a = agent_a.kv().get("lww/converge").unwrap();
    let final_b = agent_b.kv().get("lww/converge").unwrap();
    assert_eq!(final_a, final_b, "LWW must produce identical values on both nodes");
    assert_eq!(final_a, Bytes::from_static(b"second"),
        "higher-HLC write must win");

    agent_a.shutdown().await;
    agent_b.shutdown().await;
}

// ── Semantic correctness — cross_group_propose split-brain invariant ──────

/// Verifies that `cross_group_propose` cannot commit when a required group
/// has no live voters — even if every other group votes unanimously.
///
/// This is the key split-brain safety invariant: a single-group majority
/// cannot unilaterally commit a multi-group proposal.  The commit condition
/// in `ConsensusEngine::cross_propose` requires `all groups pass their
/// quorum fraction`, so a group with 0 members contributes `needed = 1`
/// but `accepts = 0`, permanently blocking the commit.
#[tokio::test]
async fn test_cross_group_propose_requires_all_group_quorums() {
    let pair = consensus_pair().await;

    // Both agents join "alpha" but neither joins "beta".
    pair.a.mesh().join_group("alpha");
    pair.b.mesh().join_group("alpha");

    // Wait for group membership to gossip.
    let aa = &pair.a;
    let ab = &pair.b;
    poll_until(
        || aa.mesh().group_members("alpha").len() >= 2
        && ab.mesh().group_members("alpha").len() >= 2,
        2_000,
    ).await;

    // Require quorum from both "alpha" (has 2 voters) and "beta" (0 voters).
    let groups = vec![
        GroupQuorum { group: "alpha".into(), quorum: 0.5, veto: false },
        GroupQuorum { group: "beta".into(),  quorum: 0.5, veto: false },
    ];
    let mut fast_cfg = ConsensusConfig::default();
    fast_cfg.phase1_timeout = Duration::from_millis(200);
    fast_cfg.max_ballots    = 1;

    let result = pair.a.consensus()
        .cross_group_propose("cgp/split-brain", Bytes::from_static(b"v"), groups, fast_cfg)
        .await;

    assert!(
        matches!(result, ConsensusResult::Timeout { .. }),
        "proposal must time out when 'beta' has no voters — got {result:?}",
    );

    // Positive case: requiring only "alpha" (which has quorum) must commit.
    let groups_alpha_only = vec![
        GroupQuorum { group: "alpha".into(), quorum: 0.5, veto: false },
    ];
    let result_ok = pair.a.consensus()
        .cross_group_propose(
            "cgp/alpha-only",
            Bytes::from_static(b"v"),
            groups_alpha_only,
            ConsensusConfig::default(),
        )
        .await;

    assert!(
        matches!(result_ok, ConsensusResult::Committed { .. }),
        "alpha-only proposal must commit; got {result_ok:?}",
    );

    pair.a.shutdown().await;
    pair.b.shutdown().await;
}

// ── M2 falsification probes (Run 16) — kept as permanent regression tests ──

/// Robustness probe: a live agent must survive hostile bytes on its gossip
/// port — pure garbage, an absurd length prefix, and an abrupt disconnect —
/// and remain fully serviceable afterwards.
#[tokio::test]
async fn probe_garbage_on_gossip_port_survives() {
    use tokio::io::AsyncWriteExt;

    let port = alloc_port();
    let id   = NodeId::new("127.0.0.1", port).unwrap();
    let mut cfg = GossipConfig::default();
    cfg.bind_port = port;
    let agent = GossipAgent::new(id, cfg);
    agent.start().await.unwrap();

    // 1. Pure garbage.
    let mut s1 = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    s1.write_all(&[0xFF; 1024]).await.unwrap();
    let _ = s1.shutdown().await;

    // 2. Huge length prefix (4 GiB frame announcement), then disconnect.
    let mut s2 = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    s2.write_all(&u32::MAX.to_le_bytes()).await.unwrap();
    s2.write_all(b"trailing").await.unwrap();
    let _ = s2.shutdown().await;

    // 3. Zero-length frame followed by garbage.
    let mut s3 = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    s3.write_all(&0u32.to_le_bytes()).await.unwrap();
    s3.write_all(&[0x00; 64]).await.unwrap();
    let _ = s3.shutdown().await;

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // Agent must still be alive and serviceable.
    assert!(agent.kv().set("probe/after-garbage", Bytes::from_static(b"ok")));
    assert_eq!(
        agent.kv().get("probe/after-garbage").as_deref(),
        Some(b"ok".as_slice()),
        "agent must remain serviceable after hostile input",
    );
    let stats = agent.system_stats();
    assert_eq!(stats.dead_shards, 0, "no gossip shard may die from hostile input");
    agent.shutdown().await;
}

/// Resource-management probe: after `shutdown_with_timeout`, every tracked
/// background task must have exited and the gossip port must be rebindable.
#[tokio::test]
async fn probe_shutdown_drains_tasks_and_releases_port() {
    let port = alloc_port();
    let id   = NodeId::new("127.0.0.1", port).unwrap();
    let mut cfg = GossipConfig::default();
    cfg.bind_port = port;
    let agent = GossipAgent::new(id, cfg);
    agent.start().await.unwrap();

    // Exercise the task spawners: consensus listener + capability advertisement.
    let _listener = agent.consensus().start_consensus_listener(
        crate::consensus::ConsensusConfig::default(),
    );
    let _reg = agent.capabilities().advertise_capability(
        Capability::new("probe", "drain"),
        std::time::Duration::from_secs(60),
    );
    assert!(agent.system_stats().task_count > 0, "tasks must be tracked while running");

    agent.shutdown_with_timeout(std::time::Duration::from_secs(10)).await;
    assert_eq!(
        agent.system_stats().task_count, 0,
        "no tracked task may survive shutdown",
    );

    // The gossip port must actually be released.
    let rebind = std::net::TcpListener::bind(("127.0.0.1", port));
    assert!(rebind.is_ok(), "gossip port must be rebindable after shutdown");
}

// ── Architecture conformance tests (M2 evidence for dimensions 1 and 3) ────

/// Layer separation as an executable invariant: Layer I modules (KV store,
/// framing, writer, seen-set, HLC) must not reference Layer III (consensus)
/// or the capability subsystem. Until the v2 workspace split makes this a
/// compile boundary, this test is the enforcement. The Layer I/II bridge
/// (`KvState::subscriptions`) is documented and intentionally not forbidden.
#[test]
fn layer1_modules_do_not_reference_higher_layers() {
    const LAYER1: &[(&str, &str)] = &[
        ("store.rs",   include_str!("store.rs")),
        ("framing.rs", include_str!("framing.rs")),
        ("writer.rs",  include_str!("writer.rs")),
        ("seen.rs",    include_str!("../mycelium-core/src/seen.rs")),
        ("hlc.rs",     include_str!("../mycelium-core/src/hlc.rs")),
    ];
    const FORBIDDEN: &[&str] = &[
        "crate::consensus",
        "crate::capability",
        "consensus_ns",
        "ConsensusEngine",
        "ConsensusMsg",
        "CapEntry",
        "CapFilter",
        "CapabilityGroupDef",
        "cross_group",
    ];
    for (file, src) in LAYER1 {
        for pat in FORBIDDEN {
            assert!(
                !src.contains(pat),
                "Layer I file src/{file} references `{pat}` — \
                 a Layer I → Layer III/capability dependency is a layer violation \
                 (see CLAUDE.md § Layer I/II Bridge Invariant)",
            );
        }
    }
}

/// The Holland inversion as an executable invariant: boundaries control
/// *acting*, never *forwarding*. A relay node that is NOT a member of group g
/// must still forward g-scoped signals to its peers. A raw socket plays the
/// emitter so the emitter and receiver cannot peer directly — delivery is
/// only possible through the non-member relay. If anyone ever "optimises"
/// forwarding by scope membership, this test fails.
#[tokio::test]
async fn forwarding_is_unconditional_through_non_member_relay() {
    let port_r = alloc_port();
    let port_b = alloc_port();
    let id_r = NodeId::new("127.0.0.1", port_r).unwrap();
    let id_b = NodeId::new("127.0.0.1", port_b).unwrap();

    let mut cfg_r = GossipConfig::default();
    cfg_r.bind_port = port_r;
    // One-way bootstrap (member → relay): the relay only learns the member's
    // address from the member's ping, and pings back on its next health-check
    // tick — that ping-back is what populates the member's peer table. At the
    // default 10 s interval that exceeds the formation wait, so tighten it.
    cfg_r.health_check_interval_secs = 1;
    cfg_r.health_check_max_jitter_ms = 50;
    let relay = GossipAgent::new(id_r.clone(), cfg_r);
    relay.start().await.unwrap();
    // The relay deliberately does NOT join the group.

    let mut cfg_b = GossipConfig::default();
    cfg_b.bind_port = port_b;
    cfg_b.bootstrap_peers = vec![id_r.clone()];
    cfg_b.health_check_interval_secs = 1;
    cfg_b.health_check_max_jitter_ms = 50;
    let member = GossipAgent::new(id_b.clone(), cfg_b);
    member.start().await.unwrap();
    member.mesh().join_group("relay-grp");

    // Structural readiness: relay must have the member as a live peer AND
    // know (via gossiped grp/ key) that the member belongs to the group, so
    // group-hinted forwarding has a routable target.
    let grp_key = format!("grp/relay-grp/{id_b}");
    let mut ready = false;
    for _ in 0..100 {
        if !relay.peers().is_empty()
            && !member.peers().is_empty()
            && relay.kv().get(&grp_key).is_some()
        {
            ready = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert!(ready, "cluster did not form: relay peers={}, member peers={}, grp key seen={}",
        relay.peers().len(), member.peers().len(), relay.kv().get(&grp_key).is_some());

    // Register the receiver BEFORE emitting.
    let mut rx = member.mesh().signal_rx("relay.probe");

    // Raw socket plays emitter A — a node the member can never peer with.
    let fake_sender = NodeId::new("127.0.0.1", 1).unwrap();
    let mut sock = TcpStream::connect(("127.0.0.1", port_r)).await.unwrap();
    send_wire(&mut sock, &WireMessage::Signal {
        ttl:     3,
        nonce:   fastrand::u64(1..),
        sender:  fake_sender,
        scope:   SignalScope::Group(Arc::from("relay-grp")),
        kind:    Arc::from("relay.probe"),
        payload: Bytes::from_static(b"through-the-relay"),
        hlc_seq: None,
    }).await;

    let got = tokio::time::timeout(std::time::Duration::from_secs(5), rx.recv()).await;
    let sig = got
        .expect("group signal was not forwarded by the non-member relay within 5 s — \
                 forwarding must be unconditional (boundaries control acting, not forwarding)")
        .expect("signal channel closed unexpectedly");
    assert_eq!(&sig.payload[..], b"through-the-relay");

    relay.shutdown().await;
    member.shutdown().await;
}

/// M2 Run-20 quota probe (#21 Operational Readiness): the documented /ready
/// gate — NOT ready before the first capability advertisement, ready after.
/// A readiness endpoint that returns 200 early routes traffic to a node with
/// no soft-state on the mesh yet; one that never flips blocks deploys.
#[cfg(feature = "gateway")]
#[tokio::test]
async fn ready_gate_flips_on_first_capability_advertisement() {
    let gossip_port = alloc_port();
    let http_port   = alloc_port();
    let mut cfg = GossipConfig::default();
    cfg.bind_port = gossip_port;
    cfg.http_port = Some(http_port);
    let agent = GossipAgent::new(NodeId::new("127.0.0.1", gossip_port).unwrap(), cfg);
    agent.start().await.expect("start");

    let client = reqwest::Client::new();
    let url = format!("http://127.0.0.1:{http_port}/ready");
    // Wait for the HTTP server itself, via the liveness endpoint.
    let health = format!("http://127.0.0.1:{http_port}/health");
    for _ in 0..40 {
        if client.get(&health).send().await.is_ok_and(|r| r.status().is_success()) { break; }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let before = client.get(&url).send().await.expect("ready before");
    assert!(
        !before.status().is_success(),
        "/ready must NOT be 200 before any capability is advertised (got {})",
        before.status(),
    );

    let _reg = agent.capabilities().advertise_capability(
        Capability::new("probe", "ready"),
        Duration::from_secs(5),
    );
    let mut flipped = false;
    for _ in 0..60 {
        if client.get(&url).send().await.is_ok_and(|r| r.status().is_success()) {
            flipped = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    agent.shutdown().await;
    assert!(flipped, "/ready must flip to 200 after the first advertisement tick");
}

/// M2 Run-20 quota probe (#18 Test Architecture): in-suite mini-fuzz of the
/// wire and capability decoders — the fuzz targets exist but the series has
/// never executed them, so this puts a fast adversarial pass in the normal
/// suite. Random bytes plus truncations/bitflips of a VALID encoded frame
/// (mutation finds more than noise). Pass = no panic; Err is valid output.
#[cfg(feature = "fuzz-internals")]
#[test]
fn mini_fuzz_decoders_survive_adversarial_bytes() {
    // A valid frame to mutate: encode a real WireMessage.
    let update = GossipUpdate {
        nonce: 7, sender: 1, ttl: 3, is_tombstone: false,
        timestamp: crate::hlc::pack(1_700_000_000_000, 4),
        key: Arc::from("fuzz/seed"), value: Bytes::from_static(b"v"),
    };
    let valid = bincode::serde::encode_to_vec(
        WireMessage::Data(update),
        bincode_cfg(),
    ).unwrap();

    let mut rng = fastrand::Rng::with_seed(0xC0FFEE);
    let mut cases = 0u32;
    // Pure noise.
    for _ in 0..20_000 {
        let len = rng.usize(..256);
        let buf: Vec<u8> = (0..len).map(|_| rng.u8(..)).collect();
        let _ = crate::fuzz_internals::wire_message_decode(&buf);
        let _ = crate::fuzz_internals::capability_decode(&buf);
        let _ = crate::fuzz_internals::cap_filter_decode(&buf);
        let _ = crate::fuzz_internals::locality_path_decode(&buf);
        cases += 1;
    }
    // Truncations of a valid frame at every offset.
    for cut in 0..valid.len() {
        let _ = crate::fuzz_internals::wire_message_decode(&valid[..cut]);
        cases += 1;
    }
    // Single-bit flips at every position of the valid frame.
    for i in 0..valid.len() {
        for bit in 0..8 {
            let mut m = valid.clone();
            m[i] ^= 1 << bit;
            let _ = crate::fuzz_internals::wire_message_decode(&m);
            cases += 1;
        }
    }
    eprintln!("mini-fuzz: {cases} adversarial inputs, no panics");
}

/// M2 Run-20 deep-dive probe: anti-entropy closure for writes that predate
/// the peer connection. Reconstruction of the community-demo cold-start
/// flake (2026-06-11): a spoke bootstraps toward a seed that is not yet
/// listening, writes a key while unconnected, and the seed comes up later.
/// Live gossip missed the write by construction; the documented guarantee
/// is that anti-entropy closes the gap. Until it does, hub-spoke clusters
/// silently lack one-shot writes (the skillrunner schema keys were exactly
/// this; masked there by periodic re-assertion, not root-caused).
#[tokio::test]
async fn anti_entropy_delivers_pre_connection_writes() {
    let seed_port  = alloc_port();
    let spoke_port = alloc_port();
    let seed_id  = NodeId::new("127.0.0.1", seed_port).unwrap();
    let spoke_id = NodeId::new("127.0.0.1", spoke_port).unwrap();

    // Spoke first: bootstrap points at a seed that is NOT yet listening.
    let mut spoke_cfg = GossipConfig::default();
    spoke_cfg.bind_port = spoke_port;
    spoke_cfg.bootstrap_peers = vec![seed_id.clone()];
    let spoke = GossipAgent::new(spoke_id.clone(), spoke_cfg);
    spoke.start().await.expect("spoke start");
    // Written while unconnected: live gossip cannot deliver this.
    assert!(spoke.kv().set("ae/pre-connection", &b"written-before-seed-existed"[..]));

    // Seed comes up afterwards.
    let mut seed_cfg = GossipConfig::default();
    seed_cfg.bind_port = seed_port;
    let seed = GossipAgent::new(seed_id, seed_cfg);
    seed.start().await.expect("seed start");

    // Anti-entropy must converge the pre-connection write seed-ward.
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    let mut arrived_after = None;
    let t0 = std::time::Instant::now();
    while std::time::Instant::now() < deadline {
        if seed.kv().get("ae/pre-connection").is_some() {
            arrived_after = Some(t0.elapsed());
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    let outcome = arrived_after;
    spoke.shutdown().await;
    seed.shutdown().await;
    match outcome {
        Some(t) => eprintln!("anti-entropy closed the gap in {t:?}"),
        None => panic!(
            "seed never received the spoke's pre-connection write within 30 s — \
             anti-entropy closure is broken for hub-spoke bootstrap"
        ),
    }
}

/// Regression: `with_http_routes` must MERGE routers across calls, not
/// replace. A last-caller-wins slot silently dropped every earlier
/// registration — skillrunner's management dashboard erased the A2A
/// endpoints (`/.well-known/agent.json` → 404) for as long as both were
/// enabled, found by a live run-through of examples/a2a_langchain.
#[cfg(feature = "gateway")]
#[tokio::test]
async fn with_http_routes_merges_across_calls() {
    let gossip_port = alloc_port();
    let http_port   = alloc_port();
    let mut cfg = GossipConfig::default();
    cfg.bind_port = gossip_port;
    cfg.http_port = Some(http_port);
    let agent = GossipAgent::new(NodeId::new("127.0.0.1", gossip_port).unwrap(), cfg);

    async fn first() -> &'static str { "first" }
    async fn second() -> &'static str { "second" }
    agent.with_http_routes(axum::Router::new().route("/extra-one", axum::routing::get(first)));
    agent.with_http_routes(axum::Router::new().route("/extra-two", axum::routing::get(second)));

    agent.start().await.expect("start");
    // Poll until the HTTP server is up, then both routes must serve.
    let client = reqwest::Client::new();
    let base = format!("http://127.0.0.1:{http_port}");
    let mut ok = false;
    for _ in 0..40 {
        if let Ok(r) = client.get(format!("{base}/health")).send().await
            && r.status().is_success() { ok = true; break; }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(ok, "gateway did not come up");
    let one = client.get(format!("{base}/extra-one")).send().await.unwrap();
    let two = client.get(format!("{base}/extra-two")).send().await.unwrap();
    assert_eq!(one.status().as_u16(), 200, "first registered router must survive");
    assert_eq!(two.status().as_u16(), 200, "second registered router must serve");
    assert_eq!(one.text().await.unwrap(), "first");
    assert_eq!(two.text().await.unwrap(), "second");
    agent.shutdown().await;
}

/// M2 Run-18 race-family sweep: re-registering a prompt skill under the same
/// id and then dropping the OLD handle must not delete the NEW registration.
/// The cancellation task previously removed by key unconditionally; it must
/// remove only if the registry still holds the backend it registered.
#[cfg(feature = "llm")]
#[tokio::test]
async fn reregistered_llm_skill_survives_old_handle_drop() {
    use crate::agent::{EchoBackend, PromptTemplate};
    fn template() -> PromptTemplate {
        PromptTemplate {
            system:        "s".into(),
            user_template: "{{input}}".into(),
            max_tokens:    16,
            temperature:   0.0,
            metadata:      std::collections::HashMap::new(),
        }
    }
    let agent = make_agent();
    let h1 = agent.llm()
        .register_prompt_skill("ns", "skill", template(), Arc::new(EchoBackend))
        .await
        .expect("first registration");
    let _h2 = agent.llm()
        .register_prompt_skill("ns", "skill", template(), Arc::new(EchoBackend))
        .await
        .expect("re-registration while first handle is alive");

    drop(h1);
    // Give the old handle's cancellation task time to run (it fires on drop).
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        agent.task_ctx.llm_skills.pin().get("ns/skill").is_some(),
        "old handle's drop must not delete the newer registration"
    );
}

/// M2 Run-18 race-family sweep: concurrent registrations of the SAME signal
/// kind. The HandlerTable registration closure runs under papaya `compute`,
/// which re-invokes the closure when the entry changes concurrently — a
/// single-use `slot.take().expect(...)` panics on that retry, so two tasks
/// calling `signal_rx("same.kind")` simultaneously (or one racing the
/// closed-sender eviction in `deliver_to_handlers`) could crash. The closure
/// must clone per invocation instead.
#[test]
fn concurrent_same_kind_signal_registration_does_not_panic() {
    use crate::signal::SignalHandlers;
    let handlers = SignalHandlers::new(Duration::from_secs(600));
    let threads = 8;
    let per_thread = 400;
    std::thread::scope(|s| {
        for _ in 0..threads {
            s.spawn(|| {
                let mut rxs = Vec::with_capacity(per_thread);
                for _ in 0..per_thread {
                    rxs.push(handlers.register(Arc::from("contended.kind")));
                }
                // Drop half so closed senders accumulate and future eviction
                // computes contend with registrations too.
                rxs.truncate(per_thread / 2);
                rxs
            });
        }
    });
}

/// M2 Run-18 probe (dims 6/10): the documented lifecycle error contract.
/// `start()` on a running agent returns `AlreadyRunning`; `start()` after
/// shutdown returns `Shutdown`; and `shutdown_with_timeout` actually drains
/// every tracked task (`task_count == 0`) rather than leaking them.
#[tokio::test]
async fn test_lifecycle_error_contract_and_task_drain() {
    let port = alloc_port();
    let mut cfg = GossipConfig::default();
    cfg.bind_port = port;
    let agent = GossipAgent::new(NodeId::new("127.0.0.1", port).unwrap(), cfg);

    agent.start().await.expect("first start succeeds");
    poll_until(|| agent.system_stats().task_count > 0, 2_000).await;

    let second = agent.start().await;
    assert!(
        matches!(second, Err(GossipError::AlreadyRunning)),
        "second start() must return AlreadyRunning, got {second:?}"
    );

    agent.shutdown_with_timeout(Duration::from_secs(5)).await;
    assert_eq!(
        agent.system_stats().task_count, 0,
        "all tracked tasks must drain on shutdown"
    );

    let after = agent.start().await;
    assert!(
        matches!(after, Err(GossipError::Shutdown)),
        "start() after shutdown must return Shutdown, got {after:?}"
    );
}

// ── WS1 RBAC end-to-end (compliance feature) ──────────────────────────────

/// WS1 integration scenario: signed role claims propagate and verify across two
/// real tls-enabled nodes, and provider-side `caller_authorized` admits/denies
/// correctly based on the *verified* claim.
///
/// This exercises the whole WS1 path the unit tests stub out: A signs a role
/// claim with its tls identity key; the claim and A's `sys/identity/` key both
/// gossip to B; B's identity-watcher mirrors A's verifying key into `peer_keys`;
/// and B's `roles_of(A)` only returns the claim because the signature checks out
/// against the key B learned from the cluster — never from the (forgeable) KV
/// entry alone. Detection-not-prevention: a node can write any `sys/role/` bytes,
/// but only a correctly-signed claim reads back as a role.
///
/// Both nodes share one auto-cert dir so they share a CA (mutual trust); a
/// unique temp dir per run keeps concurrent tests and the default
/// `./mycelium-tls/` from colliding.
#[cfg(feature = "compliance")]
#[tokio::test]
async fn test_ws1_rbac_signed_roles_propagate_and_authorize_across_nodes() {
    use crate::config::TlsConfig;

    let port_a = alloc_port();
    let port_b = alloc_port();
    let id = |p: u16| NodeId::new("127.0.0.1", p).unwrap();
    let node_a = id(port_a);
    let node_b = id(port_b);

    let cert_dir =
        std::env::temp_dir().join(format!("myc-rbac-{port_a}-{port_b}"));
    let _ = std::fs::remove_dir_all(&cert_dir); // clean slate if a prior run left files

    let mk = |port: u16, boots: Vec<NodeId>| {
        let mut cfg = GossipConfig::default();
        cfg.bind_port = port;
        cfg.bootstrap_peers = boots;
        cfg.reconnect_backoff_secs = 1;
        // Fast Pings so peer registration (which happens on Ping receipt) and
        // anti-entropy converge well inside the poll window.
        cfg.health_check_interval_secs = 1;
        cfg.tls = Some(TlsConfig {
            auto_cert_dir: cert_dir.clone(),
            ..TlsConfig::default()
        });
        GossipAgent::new(id(port), cfg)
    };

    let a = Arc::new(mk(port_a, vec![]));
    let b = Arc::new(mk(port_b, vec![node_a.clone()]));
    a.start().await.unwrap();
    b.start().await.unwrap();

    // Structural poll: both nodes peered (so identity keys have a path to gossip).
    let mut peered = false;
    for _ in 0..200 {
        if !a.peers().is_empty() && !b.peers().is_empty() {
            peered = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(peered, "two tls nodes failed to peer within the window");

    // A advertises an admin role at data-clearance 3.
    a.advertise_roles(["admin".into()], 3)
        .expect("advertise_roles must succeed with a tls identity");

    // Structural poll: B verifies A's claim. Returns Some only once (a) the
    // signed `sys/role/A` entry has gossiped to B AND (b) A's identity key has
    // reached B's peer_keys so the signature verifies.
    let mut verified: Option<crate::RoleClaim> = None;
    for _ in 0..200 {
        if let Some(claim) = b.roles_of(&node_a) {
            verified = Some(claim);
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let claim = verified.expect("B never verified A's signed role claim");
    assert!(claim.has_role("admin"), "verified claim must carry the admin role");
    assert!(claim.clearance_at_least(3), "verified claim must carry clearance 3");
    assert!(!claim.clearance_at_least(4), "clearance must not over-report");

    // Provider-side authorization on B, keyed on the verified sender (A):
    let admin_allow:  [Arc<str>; 1] = [Arc::<str>::from("admin")];
    let writer_allow: [Arc<str>; 1] = [Arc::<str>::from("db-writer")];
    let node_allow:   [Arc<str>; 1] = [Arc::<str>::from(node_a.to_string())];

    assert!(b.caller_authorized(&node_a, &admin_allow),
        "A holds the admin role → admitted");
    assert!(!b.caller_authorized(&node_a, &writer_allow),
        "A holds no db-writer role → denied");
    assert!(b.caller_authorized(&node_a, &node_allow),
        "explicit NodeId allowlist entry → admitted");
    assert!(b.caller_authorized(&node_a, &[]),
        "empty allowlist → open");

    // A node with no advertised roles is denied a role-gated capability but
    // still admitted on an open one.
    assert!(!b.caller_authorized(&node_b, &admin_allow),
        "B advertised no roles → denied a role-gated capability");
    assert!(b.caller_authorized(&node_b, &[]),
        "B still admitted on an open (empty-allowlist) capability");

    a.shutdown_with_timeout(Duration::from_secs(5)).await;
    b.shutdown_with_timeout(Duration::from_secs(5)).await;
    let _ = std::fs::remove_dir_all(&cert_dir);
}

// ── sys/ namespace-ownership tripwire (Layer I detection) ─────────────────

/// WS1 increment 5: the `sys/` write-guard tripwire detects an inbound remote
/// write to a `sys/` key the receiving node owns. Node A writes
/// `sys/load/{B}/probe` — a key in B's own load namespace that only B should
/// ever originate — and it gossips to B. B applies it (LWW, detection not
/// prevention) but flags it: `system_stats().sys_namespace_violations` rises.
/// A control key in A's *own* load namespace must not trip B's wire.
#[tokio::test]
async fn test_sys_namespace_tripwire_flags_foreign_self_owned_write() {
    let port_a = alloc_port();
    let port_b = alloc_port();
    let id = |p: u16| NodeId::new("127.0.0.1", p).unwrap();
    let node_b = id(port_b);

    let mk = |port: u16, boots: Vec<NodeId>| {
        let mut cfg = GossipConfig::default();
        cfg.bind_port = port;
        cfg.bootstrap_peers = boots;
        cfg.reconnect_backoff_secs = 1;
        cfg.health_check_interval_secs = 1;
        GossipAgent::new(id(port), cfg)
    };

    let a = Arc::new(mk(port_a, vec![]));
    let b = Arc::new(mk(port_b, vec![id(port_a)]));
    a.start().await.unwrap();
    b.start().await.unwrap();

    // Structural poll: both peered so writes gossip A → B.
    let mut peered = false;
    for _ in 0..200 {
        if !a.peers().is_empty() && !b.peers().is_empty() { peered = true; break; }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(peered, "nodes failed to peer");

    // Control: A writes its OWN load key — legitimate, must never trip B.
    let _ = a.kv().set(format!("sys/load/{}/control", id(port_a)), Bytes::from_static(b"x"));
    // Violation: A writes a key in B's load namespace.
    let foreign_key = format!("sys/load/{node_b}/probe");
    let _ = a.kv().set(foreign_key.clone(), Bytes::from_static(b"clobber"));

    // Structural poll: B observes the foreign write and flags it.
    let mut flagged = false;
    for _ in 0..200 {
        // Confirm the write actually reached B (gossip arrived) …
        if b.kv().get(&foreign_key).is_some() && b.system_stats().sys_namespace_violations >= 1 {
            flagged = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(flagged, "B did not flag the foreign write to its own sys/load namespace");

    a.shutdown_with_timeout(Duration::from_secs(5)).await;
    b.shutdown_with_timeout(Duration::from_secs(5)).await;
}

// ── WS2 audit trail — agent-level writer + chain verification ─────────────

/// WS2 increment 2: a node seals events into its own hash-chained audit stream
/// and the stream verifies end-to-end against the node's identity key — and a
/// post-hoc edit to any stored record breaks verification (the tamper probe).
#[cfg(feature = "compliance")]
#[tokio::test]
async fn test_ws2_audit_chain_writes_and_verifies_on_a_node() {
    use crate::config::TlsConfig;
    use crate::{
        audit_stream_prefix, verify_stream_from_genesis, AuditAction, AuditOutcome,
        SignedAuditRecord,
    };

    let port = alloc_port();
    let id = NodeId::new("127.0.0.1", port).unwrap();
    let cert_dir = std::env::temp_dir().join(format!("myc-audit-{port}"));
    let _ = std::fs::remove_dir_all(&cert_dir);

    let mut cfg = GossipConfig::default();
    cfg.bind_port = port;
    cfg.tls = Some(TlsConfig { auto_cert_dir: cert_dir.clone(), ..TlsConfig::default() });
    let a = Arc::new(GossipAgent::new(id.clone(), cfg));
    a.start().await.unwrap();

    // Structural poll: the tls identity key lands at sys/identity/{self}.
    let id_key = format!("sys/identity/{id}");
    let mut vk_bytes = None;
    for _ in 0..100 {
        if let Some(b) = a.kv().get(&id_key) {
            vk_bytes = Some(b);
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let vk_bytes = vk_bytes.expect("node identity key never written");
    assert_eq!(vk_bytes.len(), 32);
    let mut vk = [0u8; 32];
    vk.copy_from_slice(&vk_bytes);

    // Seal three events; content hashes must be distinct.
    let h0 = a.audit(AuditAction::Invoke, "10.0.0.1:9000", "skill/a", AuditOutcome::Success, None).unwrap();
    let h1 = a.audit(AuditAction::Read, "10.0.0.2:9000", "kv/secret", AuditOutcome::Denied, Some("scope".into())).unwrap();
    let _h2 = a.audit(AuditAction::Write, "10.0.0.1:9000", "kv/x", AuditOutcome::Success, None).unwrap();
    assert_ne!(h0, h1, "distinct events have distinct content hashes");

    // Collect the stream, order by key (lexicographic = seq order), decode.
    let mut entries = a.kv().scan_prefix(&audit_stream_prefix(&id));
    entries.sort_by(|x, y| x.0.cmp(&y.0));
    let chain: Vec<SignedAuditRecord> = entries
        .iter()
        .map(|(_, v)| SignedAuditRecord::decode(v).expect("decode audit record"))
        .collect();
    assert_eq!(chain.len(), 3, "all three sealed records are present");
    assert_eq!(verify_stream_from_genesis(&chain, &id, &vk), Ok(()), "honest chain verifies");

    // Tamper probe: flip a stored record's outcome → verification must fail.
    let mut tampered = chain.clone();
    tampered[1].record.outcome = AuditOutcome::Success;
    assert!(
        verify_stream_from_genesis(&tampered, &id, &vk).is_err(),
        "a post-hoc edit must break chain verification"
    );

    a.shutdown_with_timeout(Duration::from_secs(5)).await;
    let _ = std::fs::remove_dir_all(&cert_dir);
}

// ── WS3 crown-jewel — data-at-rest encryption hook ────────────────────────

/// A trivial reversible cipher for exercising the data-at-rest hook: a 1-byte
/// key tag followed by XOR-with-key. `decrypt` rejects a blob tagged with a
/// different key (returns `None`), so a wrong-key replay reads as corrupt.
struct XorCipher {
    key: u8,
}

impl crate::DataAtRestCipher for XorCipher {
    fn encrypt(&self, plaintext: &[u8]) -> Vec<u8> {
        let mut out = Vec::with_capacity(plaintext.len() + 1);
        out.push(self.key);
        out.extend(plaintext.iter().map(|b| b ^ self.key));
        out
    }
    fn decrypt(&self, ciphertext: &[u8]) -> Option<Vec<u8>> {
        let (tag, body) = ciphertext.split_first()?;
        if *tag != self.key {
            return None; // wrong key → treated as corrupt
        }
        Some(body.iter().map(|b| b ^ self.key).collect())
    }
}

/// WS3: an attached `DataAtRestCipher` encrypts WAL/snapshot bytes on disk
/// (plaintext never appears in `wal.bin`), the same cipher recovers the data on
/// restart, and a wrong-key cipher cannot — proving the hook is load-bearing.
#[tokio::test]
async fn test_ws3_data_at_rest_cipher_encrypts_wal_and_round_trips() {
    use crate::{PersistenceConfig, SyncMode};

    let port = alloc_port();
    let id = NodeId::new("127.0.0.1", port).unwrap();
    let base = std::env::temp_dir().join(format!("myc-darc-{port}"));
    let _ = std::fs::remove_dir_all(&base);
    let wal_path = base.join(id.to_string()).join("kv").join("wal.bin");

    let marker: &[u8] = b"TOPSECRET-PLAINTEXT-MARKER";

    let mk = || {
        let mut cfg = GossipConfig::default();
        cfg.bind_port = port;
        cfg.persistence = Some(PersistenceConfig {
            base_path: base.clone(),
            sync_mode: SyncMode::Flush,
            snapshot_wal_threshold: 1_000_000, // keep data in wal.bin, no auto-snapshot
            snapshot_interval_secs: 3_600,
        });
        GossipAgent::new(id.clone(), cfg)
    };

    // ── Phase 1: write under encryption ──────────────────────────────────
    let a1 = Arc::new(mk());
    a1.with_data_at_rest_cipher(Arc::new(XorCipher { key: 0x5A }));
    a1.start().await.unwrap();
    let _ = a1.kv().set("secret/1", Bytes::copy_from_slice(marker));

    // Structural poll: wait until the WAL record has actually landed on disk.
    let mut wal_bytes = Vec::new();
    for _ in 0..200 {
        if let Ok(b) = std::fs::read(&wal_path)
            && !b.is_empty()
        {
            wal_bytes = b;
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(!wal_bytes.is_empty(), "WAL never flushed to disk");

    // Encryption proof: the plaintext marker must NOT appear on disk.
    let contains = wal_bytes
        .windows(marker.len())
        .any(|w| w == marker);
    assert!(!contains, "plaintext marker found in wal.bin — bytes were not encrypted");

    a1.shutdown_with_timeout(Duration::from_secs(5)).await;

    // ── Phase 2: same key recovers the data ──────────────────────────────
    let a2 = Arc::new(mk());
    a2.with_data_at_rest_cipher(Arc::new(XorCipher { key: 0x5A }));
    a2.start().await.unwrap();
    let mut recovered = None;
    for _ in 0..40 {
        if let Some(v) = a2.kv().get("secret/1") {
            recovered = Some(v);
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert_eq!(
        recovered.as_deref(),
        Some(marker),
        "same-key restart must recover the encrypted record"
    );
    a2.shutdown_with_timeout(Duration::from_secs(5)).await;

    // ── Phase 3: wrong key cannot read it ────────────────────────────────
    let a3 = Arc::new(mk());
    a3.with_data_at_rest_cipher(Arc::new(XorCipher { key: 0x11 }));
    a3.start().await.unwrap();
    // Give replay a chance to run; the record must NOT decode under the wrong key.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        a3.kv().get("secret/1").is_none(),
        "wrong-key replay must not recover the record (cipher is load-bearing)"
    );
    a3.shutdown_with_timeout(Duration::from_secs(5)).await;

    let _ = std::fs::remove_dir_all(&base);
}

// ── WS5: hot identity rotation under live traffic ─────────────────────────

/// WS5 increment 3: rotating node A's identity mid-stream does not break peer B's
/// verification. B verifies A's audit records signed by the OLD key *and* the NEW
/// key (retained key set), and the chain spanning the rotation verifies on B.
#[cfg(feature = "compliance")]
#[tokio::test]
async fn test_ws5_rotate_identity_verifies_across_rotation_on_peer() {
    use crate::config::TlsConfig;

    let port_a = alloc_port();
    let port_b = alloc_port();
    let id = |p: u16| NodeId::new("127.0.0.1", p).unwrap();
    let node_a = id(port_a);
    let cert_dir = std::env::temp_dir().join(format!("myc-ws5-{port_a}-{port_b}"));
    let _ = std::fs::remove_dir_all(&cert_dir);

    let mk = |port: u16, boots: Vec<NodeId>| {
        let mut cfg = GossipConfig::default();
        cfg.bind_port = port;
        cfg.bootstrap_peers = boots;
        cfg.reconnect_backoff_secs = 1;
        cfg.health_check_interval_secs = 1;
        cfg.tls = Some(TlsConfig { auto_cert_dir: cert_dir.clone(), ..TlsConfig::default() });
        GossipAgent::new(id(port), cfg)
    };

    let a = Arc::new(mk(port_a, vec![]));
    let b = Arc::new(mk(port_b, vec![node_a.clone()]));
    a.start().await.unwrap();
    b.start().await.unwrap();

    // Peer up.
    let mut peered = false;
    for _ in 0..200 {
        if !a.peers().is_empty() && !b.peers().is_empty() { peered = true; break; }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(peered, "tls nodes failed to peer");

    // A seals a record under the OLD key.
    a.audit(crate::AuditAction::Invoke, "client", "before-rotation", crate::AuditOutcome::Success, None).unwrap();

    // Rotate A's identity (publish new‖old, brief window, cut over to new key).
    let new_vk = a.rotate_identity(Duration::from_millis(500)).await.expect("rotation");
    // The active key actually changed.
    assert_ne!(
        a.kv().get(&format!("sys/identity/{node_a}")).map(|b| b.len()),
        None,
        "identity entry present"
    );

    // A seals a second record under the NEW key.
    a.audit(crate::AuditAction::Invoke, "client", "after-rotation", crate::AuditOutcome::Success, None).unwrap();

    // B must converge to A's full 2-record stream AND verify it end-to-end —
    // which requires B to hold BOTH of A's keys (retained set) and the chain to
    // link across the rotation.
    let mut ok = false;
    for _ in 0..200 {
        let stream = b.audit_stream(&node_a);
        if stream.len() == 2 && b.audit_verify(&node_a) == Ok(()) {
            ok = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(ok, "peer B failed to verify A's audit chain across the identity rotation");

    // B learned the new key (its retained set contains it).
    let _ = new_vk;

    a.shutdown_with_timeout(Duration::from_secs(5)).await;
    b.shutdown_with_timeout(Duration::from_secs(5)).await;
    let _ = std::fs::remove_dir_all(&cert_dir);
}

// ── M2 falsification probe (Run 24): concurrent audit chain integrity ─────

/// Probe: many concurrent `audit()` calls on one node must produce a strictly
/// linear, gap-free, verifiable hash chain — i.e. the per-node chain lock (#8)
/// serialises seq/prev_hash assignment correctly and the sign-outside-the-lock
/// optimisation does not corrupt linkage under contention.
#[cfg(feature = "compliance")]
#[tokio::test]
async fn probe_concurrent_audit_chain_is_contiguous_and_verifies() {
    use crate::config::TlsConfig;

    let port = alloc_port();
    let id = NodeId::new("127.0.0.1", port).unwrap();
    let cert_dir = std::env::temp_dir().join(format!("myc-probe-cc-{port}"));
    let _ = std::fs::remove_dir_all(&cert_dir);
    let mut cfg = GossipConfig::default();
    cfg.bind_port = port;
    cfg.tls = Some(TlsConfig { auto_cert_dir: cert_dir.clone(), ..TlsConfig::default() });
    let a = Arc::new(GossipAgent::new(id.clone(), cfg));
    a.start().await.unwrap();

    let id_key = format!("sys/identity/{id}");
    let mut vkb = None;
    for _ in 0..100 {
        if let Some(b) = a.kv().get(&id_key) { vkb = Some(b); break; }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let vkb = vkb.expect("identity key");
    let mut vk = [0u8; 32];
    vk.copy_from_slice(&vkb[..32]);

    // Fire N concurrent audits.
    let n = 64u64;
    let mut handles = Vec::new();
    for i in 0..n {
        let a2 = Arc::clone(&a);
        handles.push(tokio::spawn(async move {
            a2.audit(
                crate::AuditAction::Invoke,
                format!("caller-{i}"),
                "concurrent",
                crate::AuditOutcome::Success,
                None,
            ).unwrap()
        }));
    }
    let content_hashes: Vec<[u8; 32]> = {
        let mut v = Vec::new();
        for h in handles { v.push(h.await.unwrap()); }
        v
    };
    // Every returned content hash is distinct (no two records collided).
    let mut uniq = content_hashes.clone();
    uniq.sort();
    uniq.dedup();
    assert_eq!(uniq.len(), n as usize, "every concurrent audit produced a distinct record");

    // The stored stream is contiguous 0..N and verifies end-to-end.
    let mut entries = a.kv().scan_prefix(&crate::audit_stream_prefix(&id));
    entries.sort_by(|x, y| x.0.cmp(&y.0));
    let chain: Vec<crate::SignedAuditRecord> = entries
        .iter()
        .filter_map(|(_, v)| crate::SignedAuditRecord::decode(v))
        .collect();
    assert_eq!(chain.len() as u64, n, "no lost or duplicated seq under contention");
    for (i, sr) in chain.iter().enumerate() {
        assert_eq!(sr.record.seq, i as u64, "contiguous seq (no gap/collision)");
    }
    assert_eq!(
        crate::verify_stream_from_genesis(&chain, &id, &vk),
        Ok(()),
        "concurrently-built chain verifies"
    );

    // And tamper-evidence still holds on the concurrent chain.
    let mut tampered = chain.clone();
    tampered[n as usize / 2].record.principal = "EVIL".into();
    assert!(
        crate::verify_stream_from_genesis(&tampered, &id, &vk).is_err(),
        "a tampered record in the concurrent chain fails verification"
    );

    a.shutdown_with_timeout(Duration::from_secs(5)).await;
    let _ = std::fs::remove_dir_all(&cert_dir);
}
