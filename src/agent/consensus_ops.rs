use crate::consensus::{
    consensus_kind, consensus_ns, ConsensusConfig, ConsensusHandle, ConsensusResult,
};
use crate::framing::bincode_cfg;
use crate::node_id::NodeId;
use crate::signal::{decode_load_state, kv_ns, SignalScope};
use ahash::{AHashMap, AHashSet};
use bytes::{BufMut, Bytes, BytesMut};
use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use super::GossipAgent;
use super::helpers::compute_quorum_size;

impl GossipAgent {
    /// Subscribes to committed values for a consensus slot.
    ///
    /// Returns a `watch::Receiver` that fires whenever the slot is committed or
    /// overwritten. Initial value is the current committed state (or `None`).
    #[must_use]
    pub fn consensus_rx(&self, slot: &str) -> tokio::sync::watch::Receiver<Option<Bytes>> {
        self.subscribe(format!("{}{}", consensus_ns::COMMITTED, slot))
    }

    /// Returns the last committed value for a consensus slot, or `None`.
    pub fn consensus_get(&self, slot: &str) -> Option<Bytes> {
        self.get(&format!("{}{}", consensus_ns::COMMITTED, slot))
    }

    /// Declares this node's quorum trust slice for `group` (SCP §3.1).
    ///
    /// Stored at `consensus/trust/{group}/{node_id}` and gossip-synced to all
    /// peers. The current protocol uses simple majority regardless of slices;
    /// this stores intent for future slice-aware quorum extensions.
    pub fn declare_trust(&self, group: &str, trusted_peers: &[NodeId]) {
        let key = format!("{}{}/{}", consensus_ns::TRUST, group, self.node_id);
        let mut buf = BytesMut::new();
        if bincode::serde::encode_into_std_write(
            trusted_peers, &mut (&mut buf).writer(), bincode_cfg(),
        ).is_ok() {
            let _ = self.set(key, buf.freeze());
        }
        // Warn about trusted peers that are not current group members — with
        // use_trust_slices=true this would cause ballots to time out indefinitely.
        let member_prefix = crate::signal::grp_prefix(group);
        let members: AHashSet<String> = self.scan_prefix(&member_prefix)
            .into_iter()
            .filter_map(|(k, _)| k.strip_prefix(&member_prefix).map(str::to_string))
            .collect();
        for peer in trusted_peers {
            if !members.contains(&peer.to_string()) {
                tracing::warn!(
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
        self.scan_prefix(&prefix)
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
    /// from Layer I (written by [`manage_opacity`](Self::manage_opacity)) for each.
    /// Members with no load entry are ranked lowest (transparent). Returns the
    /// lowest-load member, or `self.node_id().clone()` when the group is empty or
    /// no members have load data within `max_age`.
    ///
    /// `max_age` is used for pheromone evaporation — entries older than this are
    /// treated as transparent. Ties are broken deterministically by `id_hash()`.
    pub fn suggest_leader(&self, group: &str, kind: &str, max_age: Duration) -> NodeId {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH).unwrap_or_default()
            .as_millis() as u64;
        let max_age_ms = max_age.as_millis() as u64;

        let members: Vec<NodeId> = self.group_members(group);

        if members.is_empty() {
            return self.node_id.clone();
        }

        // Build trust_count map: candidate id_hash → number of current group members
        // that have declared a trust slice including this candidate.
        let trust_prefix = format!("{}{}/", consensus_ns::TRUST, group);
        let mut trust_counts: AHashMap<u64, usize> = AHashMap::new();
        for (_, bytes) in self.scan_prefix(&trust_prefix) {
            let Ok((peers, _)) = bincode::serde::decode_from_slice::<Vec<NodeId>, _>(
                &bytes, crate::framing::bincode_cfg()
            ) else { continue };
            for p in peers {
                *trust_counts.entry(p.id_hash()).or_insert(0) += 1;
            }
        }

        let best = members.iter().min_by(|a, b| {
            let score = |n: &NodeId| -> f32 {
                let fill = self.get(&format!("{}{}/{}", kv_ns::LOAD, n, kind))
                    .and_then(|b| decode_load_state(&b))
                    .filter(|s| now_ms.saturating_sub(s.written_at_ms) <= max_age_ms)
                    .map(|s| s.fill_ratio)
                    .unwrap_or(0.0);
                let trust = *trust_counts.get(&n.id_hash()).unwrap_or(&0) as f32;
                fill / (1.0 + trust)
            };
            score(a).partial_cmp(&score(b))
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.id_hash().cmp(&b.id_hash()))
        });

        best.cloned().unwrap_or_else(|| self.node_id.clone())
    }

    /// Proposes `value` for a named `slot` within a group.
    ///
    /// Blocks until quorum commits, another node commits first, or all ballot
    /// attempts are exhausted. All group members that called
    /// [`start_consensus_listener`](Self::start_consensus_listener) participate
    /// as voters.
    ///
    /// Quorum defaults to `floor(N/2)+1` where N is the current group member
    /// count. Set `config.quorum_size > 0` to override.
    ///
    /// **Partition safety**: committed values are stored in the LWW KV store at
    /// `consensus/committed/{slot}`. During a network partition, both halves may
    /// commit different values to the same slot. After partition healing,
    /// anti-entropy's LWW merge silently retains the higher-timestamp value and
    /// discards the other. For safety-critical slots, include a fencing token in
    /// the committed value or use `consensus_rx` to detect competing commits.
    pub async fn group_propose(
        &self,
        group:  &str,
        slot:   &str,
        value:  Bytes,
        config: ConsensusConfig,
    ) -> ConsensusResult {
        let local_opacity = self.effective_opacity(consensus_kind::PROPOSE);
        if local_opacity > 0.0 && config.ballot_retry_jitter_ms > 0 {
            let defer_ms = (local_opacity * config.ballot_retry_jitter_ms as f32 * 2.0) as u64;
            tokio::time::sleep(Duration::from_millis(defer_ms)).await;
        }
        if config.use_suggest_leader && config.ballot_retry_jitter_ms > 0 {
            let suggested = self.suggest_leader(group, consensus_kind::PROPOSE, self.signal_window());
            if suggested != self.node_id {
                tokio::time::sleep(Duration::from_millis(config.ballot_retry_jitter_ms)).await;
            }
        }
        // Collect member ids once; used for both the count and the opaque filter.
        let member_ids: AHashSet<String> = self.group_members(group)
            .iter()
            .map(NodeId::to_string)
            .collect();
        let raw_members = member_ids.len().max(1);
        let active_members = if config.count_opaque_as_absent {
            let freshness = Duration::from_millis(
                self.config.health_check_interval_secs * 2 * 1000,
            );
            let opaque_count = self.count_opaque_members(&member_ids, freshness);
            raw_members.saturating_sub(opaque_count).max(1)
        } else {
            raw_members
        };
        let quorum = compute_quorum_size(config.quorum_size, active_members);
        self.make_consensus_engine(config.abstain_when_opaque, config.use_trust_slices, config.max_abstain_ballots)
            .propose(SignalScope::Group(Arc::from(group)), Arc::from(slot), value, quorum, config)
            .await
    }

    /// Proposes `value` for system-wide consensus (all known peers vote).
    ///
    /// Quorum defaults to `floor(N/2)+1` where N is `peers + 1` (including self).
    /// Set `config.quorum_size > 0` to override.
    ///
    /// **Partition safety**: committed values are stored in the LWW KV store at
    /// `consensus/committed/{slot}`. During a network partition, both halves may
    /// commit different values to the same slot. After partition healing,
    /// anti-entropy's LWW merge silently retains the higher-timestamp value and
    /// discards the other. For safety-critical slots, include a fencing token in
    /// the committed value or use `consensus_rx` to detect competing commits.
    pub async fn system_propose(
        &self,
        slot:   &str,
        value:  Bytes,
        config: ConsensusConfig,
    ) -> ConsensusResult {
        let local_opacity = self.effective_opacity(consensus_kind::PROPOSE);
        if local_opacity > 0.0 && config.ballot_retry_jitter_ms > 0 {
            let defer_ms = (local_opacity * config.ballot_retry_jitter_ms as f32 * 2.0) as u64;
            tokio::time::sleep(Duration::from_millis(defer_ms)).await;
        }
        // Defer if another peer has lower propose-trail load than this node.
        if config.use_suggest_leader && config.ballot_retry_jitter_ms > 0 {
            let my_fill = local_opacity;
            let is_lightest = self.peer_load(self.signal_window())
                .iter()
                .filter(|(_, k, _)| k.as_ref() == consensus_kind::PROPOSE)
                .all(|(_, _, s)| s.fill_ratio >= my_fill);
            if !is_lightest {
                tokio::time::sleep(Duration::from_millis(config.ballot_retry_jitter_ms)).await;
            }
        }
        let n_nodes = (self.system_stats().peers + 1).max(1);
        let active_n = if config.count_opaque_as_absent {
            let freshness = Duration::from_millis(
                self.config.health_check_interval_secs * 2 * 1000,
            );
            let opaque_count = self.count_opaque_system(freshness);
            n_nodes.saturating_sub(opaque_count).max(1)
        } else {
            n_nodes
        };
        let quorum = compute_quorum_size(config.quorum_size, active_n);
        self.make_consensus_engine(config.abstain_when_opaque, config.use_trust_slices, config.max_abstain_ballots)
            .propose(SignalScope::System, Arc::from(slot), value, quorum, config)
            .await
    }

    /// Starts the consensus voter/listener task.
    ///
    /// Nodes that call this participate as voters in all consensus rounds.
    /// Nodes that do not call this still receive committed values via anti-entropy
    /// KV sync but their votes will not be counted.
    ///
    /// `config.abstain_when_opaque` controls whether this voter silently drops
    /// PROPOSE messages while its pheromone trail shows `is_opaque: true`.
    ///
    /// Returns a [`ConsensusHandle`] whose drop stops the task. The task also
    /// exits on [`shutdown`](Self::shutdown).
    pub fn start_consensus_listener(&self, config: ConsensusConfig) -> ConsensusHandle {
        let (cancel_tx, cancel_rx) = tokio::sync::oneshot::channel::<()>();
        let shutdown_rx = self.shutdown_tx.subscribe();

        let handle = self.make_consensus_engine(
            config.abstain_when_opaque,
            config.use_trust_slices,
            config.max_abstain_ballots,
        ).spawn_listener(cancel_rx, shutdown_rx);
        {
            let mut handles = self.task_handles.lock().unwrap_or_else(|e| e.into_inner());
            handles.retain(|h| !h.is_finished());
            handles.push(handle);
        }
        ConsensusHandle { _cancel: cancel_tx }
    }
}
