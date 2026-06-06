use std::sync::Arc;

use bytes::Bytes;

use super::{helpers::make_gossip_update, TaskCtx};
use crate::store::apply_and_notify;

// ── Public types ─────────────────────────────────────────────────────────────

/// Error returned when a consensus round does not commit.
#[derive(Debug, Clone)]
pub enum ConsistencyError {
    /// All ballot attempts timed out without reaching quorum.
    Timeout { ballots_tried: u32 },
    /// Another node committed a value to the same slot first.
    Superseded,
    /// Quorum met in headcount but the Hard topology gate was not satisfied.
    TopologyUnsatisfied,
}

impl std::fmt::Display for ConsistencyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Timeout { ballots_tried } =>
                write!(f, "consensus timed out after {ballots_tried} ballot(s)"),
            Self::Superseded =>
                write!(f, "another node committed to this slot first"),
            Self::TopologyUnsatisfied =>
                write!(f, "quorum met but Hard topology gate not satisfied"),
        }
    }
}

impl std::error::Error for ConsistencyError {}

/// RAII guard for a distributed lock acquired via [`ConsensusHandle::distributed_lock`].
///
/// Tombstones `lock/{name}` in the gossip KV when dropped.
/// `token` is the consensus ballot number — a monotonically increasing fencing
/// token across successive acquisitions of the same lock name.
pub struct LockGuard {
    pub(super) ctx:      Arc<TaskCtx>,
    pub(super) name:     Arc<str>,
    /// Fencing token (consensus ballot at commit time).
    pub token: u64,
    pub(super) released: bool,
}

impl LockGuard {
    /// Explicitly release the lock. Equivalent to dropping the guard.
    pub fn release(mut self) { self.do_release(); }

    fn do_release(&mut self) {
        if !self.released {
            self.released = true;
            let key: Arc<str> = Arc::from(format!("lock/{}", self.name).as_str());
            let update = make_gossip_update(
                &self.ctx.node_id,
                self.ctx.default_ttl,
                key,
                Bytes::new(),
                true,   // tombstone
                &self.ctx.hlc,
            );
            apply_and_notify(&self.ctx.kv_state, &update);
            crate::framing::dispatch_gossip_try_send(
                &self.ctx.gossip_txs,
                crate::framing::WireMessage::Data(update),
                self.ctx.node_id.id_hash(),
                crate::framing::ForwardHint::All,
                &self.ctx.kv_state.dropped_frames,
            );
        }
    }
}

impl Drop for LockGuard { fn drop(&mut self) { self.do_release(); } }

// ── Unit tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use crate::{GossipAgent, GossipConfig, NodeId};
    use super::ConsistencyError;

    fn alloc_port() -> u16 {
        use std::net::TcpListener;
        TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
    }

    async fn make_agent(port: u16, peers: &[u16]) -> GossipAgent {
        let id = NodeId::new("127.0.0.1", port).unwrap();
        let cfg = GossipConfig {
            bind_address:    "127.0.0.1".parse().unwrap(),
            bind_port:       port,
            bootstrap_peers: peers.iter().map(|p| NodeId::new("127.0.0.1", *p).unwrap()).collect(),
            ..GossipConfig::default()
        };
        let a = GossipAgent::new(id, cfg);
        a.start().await.unwrap();
        a
    }

    #[tokio::test]
    async fn test_consistent_set_and_get_two_nodes() {
        let p1 = alloc_port();
        let p2 = alloc_port();
        let a1 = make_agent(p1, &[p2]).await;
        let a2 = make_agent(p2, &[p1]).await;
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;

        a1.consensus().consistent_set("cfg/x", Bytes::from_static(b"hello")).await.unwrap();

        assert_eq!(a1.consensus().consistent_get("cfg/x").as_deref(), Some(b"hello".as_slice()));

        a1.shutdown().await;
        a2.shutdown().await;
    }

    #[tokio::test]
    async fn test_consistent_set_single_node_succeeds() {
        // Single node: quorum auto-computes to 1 (majority of 1), so it commits.
        let a = make_agent(alloc_port(), &[]).await;
        let r = a.consensus().consistent_set("cfg/solo", Bytes::from_static(b"ok")).await;
        assert!(r.is_ok(), "single-node consistent_set should succeed: {r:?}");
        assert_eq!(a.consensus().consistent_get("cfg/solo").as_deref(), Some(b"ok".as_slice()));
        a.shutdown().await;
    }

    #[tokio::test]
    async fn test_consistent_set_timeout_unreachable_quorum() {
        // Require quorum > 1 by proposing with an explicit ConsensusConfig that
        // sets max_ballots = 1 and quorum_size = 2. Use system_propose directly.
        use crate::consensus::ConsensusConfig;
        let p   = alloc_port();
        let id  = NodeId::new("127.0.0.1", p).unwrap();
        let cfg = GossipConfig {
            bind_address: "127.0.0.1".parse().unwrap(),
            bind_port:    p,
            ..GossipConfig::default()
        };
        let a = GossipAgent::new(id, cfg);
        a.start().await.unwrap();

        let custom = ConsensusConfig { quorum_size: 2, max_ballots: 1, ..ConsensusConfig::default() };
        match a.consensus().system_propose("test/slot", Bytes::from_static(b"x"), custom).await {
            crate::consensus::ConsensusResult::Timeout { .. } => {}
            other => panic!("expected Timeout, got {other:?}"),
        }
        a.shutdown().await;
    }

    // Suppress unused variant warning — ConsistencyError::Timeout is tested above.
    #[allow(dead_code)]
    fn _assert_consistency_error_variants() {
        let _ = ConsistencyError::Superseded;
        let _ = ConsistencyError::TopologyUnsatisfied;
    }
}
