use crate::consensus::{ConsensusConfig, ConsensusListenerHandle, ConsensusResult};
use crate::node_id::NodeId;
use bytes::Bytes;
use std::time::Duration;

use super::GossipAgent;
use super::helpers::suggest_leader_ctx;

impl GossipAgent {
    /// Subscribes to committed values for a consensus slot.
    ///
    /// Use [`ConsensusHandle::consensus_rx`] via [`GossipAgent::consensus`] instead.
    #[must_use]
    pub fn consensus_rx(&self, slot: &str) -> tokio::sync::watch::Receiver<Option<Bytes>> {
        self.consensus().consensus_rx(slot)
    }

    /// Returns the last committed value for a consensus slot, or `None`.
    ///
    /// Use [`ConsensusHandle::consensus_get`] via [`GossipAgent::consensus`] instead.
    pub fn consensus_get(&self, slot: &str) -> Option<Bytes> {
        self.consensus().consensus_get(slot)
    }

    /// Declares this node's quorum trust slice for `group`.
    ///
    /// Use [`ConsensusHandle::declare_trust`] via [`GossipAgent::consensus`] instead.
    pub fn declare_trust(&self, group: &str, trusted_peers: &[NodeId]) {
        self.consensus().declare_trust(group, trusted_peers)
    }

    /// Returns all declared trust slices for `group`, keyed by declaring node.
    ///
    /// Use [`ConsensusHandle::group_trust`] via [`GossipAgent::consensus`] instead.
    pub fn group_trust(&self, group: &str) -> Vec<(NodeId, Vec<NodeId>)> {
        self.consensus().group_trust(group)
    }

    /// Returns the group member with the lowest observed load for `kind`.
    ///
    /// Thin stub — delegates to [`suggest_leader_ctx`](super::helpers::suggest_leader_ctx).
    /// Available as [`ConsensusHandle::suggest_leader`] or [`CapabilityHandle::suggest_leader`].
    pub fn suggest_leader(&self, group: &str, kind: &str, max_age: Duration) -> NodeId {
        suggest_leader_ctx(&self.task_ctx, group, kind, max_age)
    }

    /// Proposes `value` for a named `slot` within a group.
    ///
    /// Use [`ConsensusHandle::group_propose`] via [`GossipAgent::consensus`] instead.
    pub async fn group_propose(
        &self,
        group:  &str,
        slot:   &str,
        value:  Bytes,
        config: ConsensusConfig,
    ) -> ConsensusResult {
        self.consensus().group_propose(group, slot, value, config).await
    }

    /// Proposes `value` for system-wide consensus (all known peers vote).
    ///
    /// Use [`ConsensusHandle::system_propose`] via [`GossipAgent::consensus`] instead.
    pub async fn system_propose(
        &self,
        slot:   &str,
        value:  Bytes,
        config: ConsensusConfig,
    ) -> ConsensusResult {
        self.consensus().system_propose(slot, value, config).await
    }

    /// Proposes `value` for `slot` requiring independent quorum from each group in `groups`.
    ///
    /// Use [`ConsensusHandle::cross_group_propose`] via [`GossipAgent::consensus`] instead.
    pub async fn cross_group_propose(
        &self,
        slot:   &str,
        value:  Bytes,
        groups: Vec<crate::GroupQuorum>,
        config: ConsensusConfig,
    ) -> ConsensusResult {
        self.consensus().cross_group_propose(slot, value, groups, config).await
    }

    /// Starts the consensus voter/listener task.
    ///
    /// Use [`ConsensusHandle::start_consensus_listener`] via [`GossipAgent::consensus`] instead.
    pub fn start_consensus_listener(&self, config: ConsensusConfig) -> ConsensusListenerHandle {
        self.consensus().start_consensus_listener(config)
    }
}
