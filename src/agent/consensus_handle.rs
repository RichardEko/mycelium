//! Consensus operations — [`ConsensusHandle`].
//!
//! Wraps the epidemic two-phase agreement primitives (Layer III):
//! group proposals, system-wide proposals, cross-group proposals,
//! distributed locks, leader election, trust-slice declarations,
//! and the consistent KV overlay.
//!
//! Obtain a handle via [`GossipAgent::consensus`](crate::GossipAgent::consensus).

use crate::consensus::{
    consensus_kind, consensus_ns, ConsensusConfig, ConsensusListenerHandle,
    ConsensusResult, OpaqueRecompute,
};
use crate::node_id::NodeId;
use crate::signal::SignalScope;
use ahash::AHashSet;
use bytes::{BufMut, Bytes, BytesMut};
use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tracing::warn;

use super::TaskCtx;
use super::helpers::{
    cached_group_members_ctx, compute_quorum_size,
    kv_get, kv_scan_prefix, kv_set, kv_subscribe, make_consensus_engine_ctx,
    suggest_leader_ctx,
};
use super::opacity::{
    count_opaque_members_ctx, count_opaque_system_ctx, effective_opacity_ctx,
    peer_load_ctx, count_opaque_members_in_kv, count_opaque_all_in_kv,
};
use crate::framing::bincode_cfg;

// Re-export public types used by callers.
pub use super::overlay_consistent::{ConsistencyError, LockGuard};

/// Domain handle for consensus operations. Obtained via [`GossipAgent::consensus()`].
///
/// Provides group proposals, system-wide proposals, cross-group proposals,
/// distributed locks, leader election, trust-slice declarations,
/// and the consistent KV overlay.
///
/// The handle is `Clone + Send + Sync` and can be stored, moved across tasks,
/// or captured in closures.
#[derive(Clone)]
pub struct ConsensusHandle {
    pub(crate) ctx: Arc<TaskCtx>,
}

impl ConsensusHandle {
    // ── Signal window helper ─────────────────────────────────────────────────

    fn signal_window(&self) -> Duration {
        Duration::from_secs(self.ctx.config.signal_window_secs)
    }

    // ── Consensus ops ────────────────────────────────────────────────────────

    /// Subscribes to committed values for a consensus slot.
    ///
    /// Returns a `watch::Receiver` that fires whenever the slot is committed or
    /// overwritten. Initial value is the current committed state (or `None`).
    #[must_use]
    pub fn consensus_rx(&self, slot: &str) -> tokio::sync::watch::Receiver<Option<Bytes>> {
        kv_subscribe(&self.ctx, format!("{}{}", consensus_ns::COMMITTED, slot))
    }

    /// Returns the last committed value for a consensus slot, or `None`.
    pub fn consensus_get(&self, slot: &str) -> Option<Bytes> {
        kv_get(&self.ctx, &format!("{}{}", consensus_ns::COMMITTED, slot))
    }

    /// Declares this node's quorum trust slice for `group` (SCP §3.1).
    ///
    /// Stored at `consensus/trust/{group}/{node_id}` and gossip-synced to all
    /// peers. The current protocol uses simple majority regardless of slices;
    /// this stores intent for future slice-aware quorum extensions.
    pub fn declare_trust(&self, group: &str, trusted_peers: &[NodeId]) {
        let key = format!("{}{}/{}", consensus_ns::TRUST, group, self.ctx.node_id);
        let mut buf = BytesMut::new();
        if bincode::serde::encode_into_std_write(
            trusted_peers, &mut (&mut buf).writer(), bincode_cfg(),
        ).is_ok() {
            let _ = kv_set(&self.ctx, Arc::from(key.as_str()), buf.freeze());
        }
        let member_prefix = crate::signal::grp_prefix(group);
        let members: AHashSet<String> = kv_scan_prefix(&self.ctx, &member_prefix)
            .into_iter()
            .filter_map(|(k, _)| k.strip_prefix(&member_prefix).map(str::to_string))
            .collect();
        for peer in trusted_peers {
            if !members.contains(&peer.to_string()) {
                warn!(
                    group, peer = %peer,
                    "declare_trust: peer is not a current group member; \
                     use_trust_slices=true will time out waiting for their vote"
                );
            }
        }
    }

    /// Returns all declared trust slices for `group`, keyed by declaring node.
    pub fn group_trust(&self, group: &str) -> Vec<(NodeId, Vec<NodeId>)> {
        let prefix = format!("{}{}/", consensus_ns::TRUST, group);
        kv_scan_prefix(&self.ctx, &prefix)
            .into_iter()
            .filter_map(|(key, bytes)| {
                let node_str = key.strip_prefix(&prefix)?;
                let node_id: NodeId = node_str.parse().ok()?;
                let (peers, _) = bincode::serde::decode_from_slice::<Vec<NodeId>, _>(
                    &bytes, bincode_cfg(),
                ).ok()?;
                Some((node_id, peers))
            })
            .collect()
    }

    /// Returns the group member with the lowest observed load for `kind`.
    ///
    /// Iterates `grp/{group}/` for member NodeIds, then reads `load/{member}/{kind}`
    /// from Layer I for each. Members with no load entry are ranked lowest
    /// (transparent). Returns the lowest-load member, or `self.node_id` when the
    /// group is empty or no members have load data within `max_age`.
    ///
    /// `max_age` is used for pheromone evaporation — entries older than this are
    /// treated as transparent. Ties are broken deterministically by `id_hash()`.
    pub fn suggest_leader(&self, group: &str, kind: &str, max_age: Duration) -> NodeId {
        suggest_leader_ctx(&self.ctx, group, kind, max_age)
    }

    /// Proposes `value` for a named `slot` within a group.
    ///
    /// Blocks until quorum commits, another node commits first, or all ballot
    /// attempts are exhausted.
    ///
    /// Quorum defaults to `floor(N/2)+1` where N is the current group member
    /// count. Set `config.quorum_size > 0` to override.
    #[tracing::instrument(level = "debug", skip(self, value), fields(node = %self.ctx.node_id))]
    pub async fn group_propose(
        &self,
        group:  &str,
        slot:   &str,
        value:  Bytes,
        config: ConsensusConfig,
    ) -> ConsensusResult {
        let local_opacity = effective_opacity_ctx(&self.ctx, consensus_kind::PROPOSE);
        if local_opacity > 0.0 && config.ballot_retry_jitter_ms > 0 {
            let defer_ms = (local_opacity * config.ballot_retry_jitter_ms as f32 * 2.0) as u64;
            tokio::time::sleep(Duration::from_millis(defer_ms)).await;
        }
        if config.use_suggest_leader && config.ballot_retry_jitter_ms > 0 {
            let suggested = suggest_leader_ctx(&self.ctx, group, consensus_kind::PROPOSE, self.signal_window());
            if suggested != self.ctx.node_id {
                tokio::time::sleep(Duration::from_millis(config.ballot_retry_jitter_ms)).await;
            }
        }
        let roster_ttl = Duration::from_secs(self.ctx.config.health_check_interval_secs);
        let cached = cached_group_members_ctx(&self.ctx, group, roster_ttl);
        let member_ids: AHashSet<String> = cached.members
            .iter()
            .map(NodeId::to_string)
            .collect();
        let freshness = Duration::from_millis(
            self.ctx.config.health_check_interval_secs * 2 * 1000,
        );
        let raw_members = member_ids.len().max(1);
        let active_members = if config.count_opaque_as_absent {
            let opaque_count = count_opaque_members_ctx(&self.ctx, &member_ids, freshness);
            raw_members.saturating_sub(opaque_count).max(1)
        } else {
            raw_members
        };
        let quorum = compute_quorum_size(config.quorum_size, active_members);
        let opaque_recompute = if config.count_opaque_as_absent {
            let kv_cb  = Arc::clone(&self.ctx.kv_state);
            let ids_cb = member_ids.clone();
            let freshness_ms = freshness.as_millis() as u64;
            let count_opaque: Arc<dyn Fn() -> usize + Send + Sync> = Arc::new(move || {
                let now_ms = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64;
                count_opaque_members_in_kv(&kv_cb, &ids_cb, freshness_ms, now_ms)
            });
            Some(OpaqueRecompute { total_members: raw_members, config_quorum: config.quorum_size, count_opaque })
        } else {
            None
        };
        let topology_policy = self.ctx.config.topology_policies.get(group).cloned();
        make_consensus_engine_ctx(
            &self.ctx,
            config.abstain_when_opaque, config.use_trust_slices, config.max_abstain_ballots,
            topology_policy,
        )
            .propose(SignalScope::Group(Arc::from(group)), Arc::from(slot), value, quorum, config, opaque_recompute)
            .await
    }

    /// Proposes `value` for system-wide consensus (all known peers vote).
    ///
    /// Quorum defaults to `floor(N/2)+1` where N is `peers + 1` (including self).
    /// Set `config.quorum_size > 0` to override.
    #[tracing::instrument(level = "debug", skip(self, value), fields(node = %self.ctx.node_id))]
    pub async fn system_propose(
        &self,
        slot:   &str,
        value:  Bytes,
        config: ConsensusConfig,
    ) -> ConsensusResult {
        let local_opacity = effective_opacity_ctx(&self.ctx, consensus_kind::PROPOSE);
        if local_opacity > 0.0 && config.ballot_retry_jitter_ms > 0 {
            let defer_ms = (local_opacity * config.ballot_retry_jitter_ms as f32 * 2.0) as u64;
            tokio::time::sleep(Duration::from_millis(defer_ms)).await;
        }
        if config.use_suggest_leader && config.ballot_retry_jitter_ms > 0 {
            let my_fill = local_opacity;
            let is_lightest = peer_load_ctx(&self.ctx, self.signal_window())
                .iter()
                .filter(|(_, k, _)| k.as_ref() == consensus_kind::PROPOSE)
                .all(|(_, _, s)| s.fill_ratio >= my_fill);
            if !is_lightest {
                tokio::time::sleep(Duration::from_millis(config.ballot_retry_jitter_ms)).await;
            }
        }
        let n_nodes = (self.ctx.peers.len() + 1).max(1);
        let freshness_ms = self.ctx.config.health_check_interval_secs * 2 * 1000;
        let active_n = if config.count_opaque_as_absent {
            let opaque_count = count_opaque_system_ctx(
                &self.ctx,
                Duration::from_millis(freshness_ms),
            );
            n_nodes.saturating_sub(opaque_count).max(1)
        } else {
            n_nodes
        };
        let quorum = compute_quorum_size(config.quorum_size, active_n);
        let opaque_recompute = if config.count_opaque_as_absent {
            let kv_cb = Arc::clone(&self.ctx.kv_state);
            let count_opaque: Arc<dyn Fn() -> usize + Send + Sync> = Arc::new(move || {
                let now_ms = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64;
                count_opaque_all_in_kv(&kv_cb, freshness_ms, now_ms)
            });
            Some(OpaqueRecompute { total_members: n_nodes, config_quorum: config.quorum_size, count_opaque })
        } else {
            None
        };
        make_consensus_engine_ctx(
            &self.ctx,
            config.abstain_when_opaque, config.use_trust_slices, config.max_abstain_ballots,
            None,
        )
            .propose(SignalScope::System, Arc::from(slot), value, quorum, config, opaque_recompute)
            .await
    }

    /// Proposes `value` for `slot` requiring independent quorum from each group in `groups`.
    ///
    /// Commits only when **all** specified groups individually reach their configured quorum
    /// fraction.
    pub async fn cross_group_propose(
        &self,
        slot:   &str,
        value:  Bytes,
        groups: Vec<crate::GroupQuorum>,
        config: ConsensusConfig,
    ) -> ConsensusResult {
        make_consensus_engine_ctx(&self.ctx, false, false, 0, None)
            .cross_propose(Arc::from(slot), value, &groups, config)
            .await
    }

    /// Starts the consensus voter/listener task.
    ///
    /// Nodes that call this participate as voters in all consensus rounds.
    /// Nodes that do not call this still receive committed values via anti-entropy
    /// KV sync but their votes will not be counted.
    ///
    /// Returns a [`ConsensusListenerHandle`] whose drop stops the task. The task
    /// also exits on agent shutdown.
    pub fn start_consensus_listener(&self, config: ConsensusConfig) -> ConsensusListenerHandle {
        let (cancel_tx, cancel_rx) = tokio::sync::oneshot::channel::<()>();
        let shutdown_rx = self.ctx.shutdown_tx.subscribe();
        let engine = make_consensus_engine_ctx(
            &self.ctx,
            config.abstain_when_opaque,
            config.use_trust_slices,
            config.max_abstain_ballots,
            None,
        );
        self.ctx.spawn_task(crate::consensus::run_consensus_listener(engine, cancel_rx, shutdown_rx));
        ConsensusListenerHandle { _cancel: cancel_tx }
    }

    // ── Consistent overlay ───────────────────────────────────────────────────

    /// Consensus-durable write: runs a ballot-voting round before committing.
    ///
    /// Broadcasts a `Propose` message, waits for `floor(N/2)+1` peer votes, then
    /// writes the value to `consensus/committed/consistent/{key}` (durable, anti-entropy-
    /// synced to all nodes) and to the raw gossip KV key.
    ///
    /// **Guarantee: ballot serialization, not linearizability.** Concurrent writes to
    /// the same key are totally ordered by ballot number — the highest-ballot committed
    /// value is the authoritative entry in `consensus/committed/`. Two concurrent
    /// proposers can each return `Ok(())` at different ballots; the higher-ballot value
    /// wins via LWW. `consistent_get` is a local read and may lag the cluster-wide
    /// committed value by up to one anti-entropy round.
    ///
    /// **Suitable for:** leader election, distributed locks, single-writer coordinator
    /// patterns where "only one writer should commit first" is sufficient. Use ballot-based
    /// fencing tokens (see `distributed_lock`) to protect downstream consumers from
    /// lower-ballot writers.
    ///
    /// **Not suitable for:** read-after-write guarantees without polling. After calling
    /// `consistent_set`, callers on other nodes should poll `consistent_get` until the
    /// expected value appears (usually within one gossip round).
    ///
    /// Use [`KvHandle::set`](crate::KvHandle::set) for ordinary eventually-consistent writes.
    #[tracing::instrument(level = "debug", skip(self, key, value), fields(node = %self.ctx.node_id))]
    pub async fn consistent_set(
        &self,
        key:   impl Into<Arc<str>>,
        value: impl Into<Bytes>,
    ) -> Result<(), ConsistencyError> {
        let key: Arc<str> = key.into();
        let value: Bytes   = value.into();
        let slot = format!("consistent/{key}");

        match self.system_propose(&slot, value.clone(), ConsensusConfig::default()).await {
            ConsensusResult::Committed { .. } => {
                kv_set(&self.ctx, key, value);
                Ok(())
            }
            ConsensusResult::Timeout { ballots_tried, .. } =>
                Err(ConsistencyError::Timeout { ballots_tried }),
            ConsensusResult::Superseded { .. } =>
                Err(ConsistencyError::Superseded),
            ConsensusResult::TopologyUnsatisfied { .. } =>
                Err(ConsistencyError::TopologyUnsatisfied),
        }
    }

    /// Read the latest ballot-committed value for `key` visible to this node.
    ///
    /// Checks `consensus/committed/consistent/{key}` first (written on quorum commit
    /// and anti-entropy-synced to all nodes); falls back to the raw gossip KV key.
    ///
    /// **Not a read quorum.** Returns whatever has anti-entropy-propagated to this node,
    /// which may lag the cluster-wide committed value by up to one gossip round. For
    /// read-after-write guarantees, poll until the expected value appears.
    pub fn consistent_get(&self, key: &str) -> Option<Bytes> {
        kv_get(&self.ctx, &format!("consensus/committed/consistent/{key}"))
            .or_else(|| kv_get(&self.ctx, key))
    }

    /// Acquire a named distributed lock via cluster consensus.
    ///
    /// Returns a [`LockGuard`] that releases the lock on drop.
    /// `ttl` is advisory — stored in the lock record for fencing-token expiry checks.
    #[tracing::instrument(level = "debug", skip(self), fields(node = %self.ctx.node_id))]
    pub async fn distributed_lock(
        &self,
        name: &str,
        ttl:  Duration,
    ) -> Result<LockGuard, ConsistencyError> {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);

        let lock_json = serde_json::json!({
            "holder":     self.ctx.node_id.to_string(),
            "expires_ms": now_ms + ttl.as_millis() as u64,
        });
        let value = Bytes::from(serde_json::to_vec(&lock_json).unwrap_or_default());
        let slot  = format!("lock/{name}");

        match self.system_propose(&slot, value, ConsensusConfig::default()).await {
            ConsensusResult::Committed { ballot, .. } => Ok(LockGuard {
                ctx:      Arc::clone(&self.ctx),
                name:     Arc::from(name),
                token:    ballot,
                released: false,
            }),
            ConsensusResult::Timeout { ballots_tried, .. } =>
                Err(ConsistencyError::Timeout { ballots_tried }),
            ConsensusResult::Superseded { .. } =>
                Err(ConsistencyError::Superseded),
            ConsensusResult::TopologyUnsatisfied { .. } =>
                Err(ConsistencyError::TopologyUnsatisfied),
        }
    }

    /// Elect a leader for `group` via consensus.
    ///
    /// If this node wins, returns its own `NodeId`. If another node committed first,
    /// reads the winner from the committed KV slot and returns it.
    pub async fn elect_leader(&self, group: &str) -> Result<NodeId, ConsistencyError> {
        let slot  = format!("leader/{group}");
        let value = Bytes::from(self.ctx.node_id.to_string().into_bytes());

        match self.group_propose(group, &slot, value, ConsensusConfig::default()).await {
            ConsensusResult::Committed { .. } => Ok(self.ctx.node_id.clone()),
            ConsensusResult::Superseded { .. } => {
                if let Some(raw) = kv_get(&self.ctx, &format!("consensus/committed/{slot}"))
                    && let Ok(s) = std::str::from_utf8(&raw)
                        && let Ok(id) = s.parse::<NodeId>() {
                            return Ok(id);
                        }
                Err(ConsistencyError::Superseded)
            }
            ConsensusResult::Timeout { ballots_tried, .. } =>
                Err(ConsistencyError::Timeout { ballots_tried }),
            ConsensusResult::TopologyUnsatisfied { .. } =>
                Err(ConsistencyError::TopologyUnsatisfied),
        }
    }
}

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
    async fn test_consistent_set_single_node_succeeds() {
        let a = make_agent(alloc_port(), &[]).await;
        let r = a.consensus().consistent_set("cfg/solo", Bytes::from_static(b"ok")).await;
        assert!(r.is_ok(), "single-node consistent_set should succeed: {r:?}");
        assert_eq!(a.consensus().consistent_get("cfg/solo").as_deref(), Some(b"ok".as_slice()));
        a.shutdown().await;
    }

    #[tokio::test]
    async fn test_consistent_set_timeout_unreachable_quorum() {
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

    #[allow(dead_code)]
    fn _assert_consistency_error_variants() {
        let _ = ConsistencyError::Superseded;
        let _ = ConsistencyError::TopologyUnsatisfied;
    }
}
