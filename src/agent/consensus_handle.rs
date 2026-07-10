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
use bytes::Bytes;
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
    ///
    /// **Raw KV view**: the receiver reflects the stored bytes and does not
    /// apply the epoch-lease convention — an expired leased slot still shows
    /// its last value here. Use [`consensus_get`](Self::consensus_get) for
    /// lease-aware reads.
    #[must_use]
    pub fn consensus_rx(&self, slot: &str) -> tokio::sync::watch::Receiver<Option<Bytes>> {
        kv_subscribe(&self.ctx, format!("{}{}", consensus_ns::COMMITTED, slot))
    }

    /// Returns the **live** committed value for a consensus slot, or `None`.
    ///
    /// Lease-aware: when the slot was committed with
    /// [`ConsensusConfig::committed_lease_secs`] set, a value whose lease
    /// window has elapsed reads as `None` — the slot has reopened for
    /// re-proposal. Permanent commitments (the default) never expire.
    pub fn consensus_get(&self, slot: &str) -> Option<Bytes> {
        crate::consensus::live_committed_value(
            &self.ctx.kv_state, slot, crate::consensus::wall_now_ms(),
        )
    }

    /// Declares this node's quorum trust slice for `group` (SCP §3.1).
    ///
    /// Stored at `consensus/trust/{group}/{node_id}` and gossip-synced to all
    /// peers. The current protocol uses simple majority regardless of slices;
    /// this stores intent for future slice-aware quorum extensions.
    pub fn declare_trust(&self, group: &str, trusted_peers: &[NodeId]) {
        let key = format!("{}{}/{}", consensus_ns::TRUST, group, self.ctx.node_id);
        if let Ok(encoded) = mycelium_core::serde_fixint::to_vec(trusted_peers) {
            let _ = kv_set(&self.ctx, Arc::from(key.as_str()), Bytes::from(encoded));
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
                let peers = mycelium_core::serde_fixint::from_slice::<Vec<NodeId>>(&bytes).ok()?;
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
        // Register the voter's signal handlers *before* spawning so that a
        // PROPOSE or COMMIT arriving before the task's first poll is queued
        // rather than dropped — otherwise this node silently fails to vote on
        // proposals raced against listener startup.
        let rx_propose = self.ctx.signal_handlers.register_with_capacity(
            std::sync::Arc::from(consensus_kind::PROPOSE), 512,
        );
        let rx_commit = self.ctx.signal_handlers.register_with_capacity(
            std::sync::Arc::from(consensus_kind::COMMIT), 256,
        );
        self.ctx.spawn_task(crate::consensus::run_consensus_listener(
            engine, cancel_rx, shutdown_rx, rx_propose, rx_commit,
        ));
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
    /// which may lag the cluster-wide committed value by up to one gossip round.
    /// The staleness bound is `GossipConfig::anti_entropy_interval_secs` (default 30 s);
    /// on a healthy cluster, propagation typically completes in well under one second.
    /// For read-after-write guarantees, poll until the expected value appears.
    pub fn consistent_get(&self, key: &str) -> Option<Bytes> {
        crate::consensus::live_committed_value(
            &self.ctx.kv_state, &format!("consistent/{key}"), crate::consensus::wall_now_ms(),
        )
            .or_else(|| kv_get(&self.ctx, key))
    }

    /// The [distributed lock **service**](super::LockService) — blocking acquire, scoped critical
    /// sections, and the when-to-use guidance — over this handle's [`distributed_lock`](Self::distributed_lock).
    ///
    /// `distributed_lock` is the raw try-lock; `locks()` is the ergonomic layer most callers want.
    pub fn locks(&self) -> super::LockService {
        super::LockService { ctx: std::sync::Arc::clone(&self.ctx) }
    }

    /// Acquire a named distributed lock via cluster consensus.
    ///
    /// A **leased, mutually-exclusive** lock: exactly one holder cluster-wide until it releases
    /// (drop / [`release`](LockGuard::release)) or `ttl` elapses (the lock auto-expires — the
    /// commit carries a consensus lease). The returned [`LockGuard::token`] is a monotonic fencing
    /// token for resource-side checks.
    ///
    /// Coarse-grained by design (a consensus round per acquire, ~1 s to let the commit converge) —
    /// suited to leader election, shard/config ownership, not high-rate fine-grained locking.
    ///
    /// Returns [`ConsistencyError::Superseded`] if another holder won the lock (or a live lease is
    /// held elsewhere). #164: the pre-2026-07-10 implementation returned on the *local* optimistic
    /// commit (no mutual exclusion under a race) and its release tombstoned the wrong key (locks
    /// were permanently unreleasable) — both fixed here via the converged-holder discipline (#151).
    #[tracing::instrument(level = "debug", skip(self), fields(node = %self.ctx.node_id))]
    pub async fn distributed_lock(
        &self,
        name: &str,
        ttl:  Duration,
    ) -> Result<LockGuard, ConsistencyError> {
        let slot = format!("lock/{name}");
        // Value = `{holder}:{nonce}` (#164). Expiry is the consensus commit-*lease* below, not a
        // JSON `expires_ms` field: the old field was never enforced (the lock never expired). The
        // per-acquire nonce makes each guard's value unique, so release can tell this acquisition
        // apart from any later one (even the same node re-acquiring after its lease lapsed).
        let value = Bytes::from(
            format!("{}:{:016x}", self.ctx.node_id, fastrand::u64(..)).into_bytes(),
        );
        let cfg = ConsensusConfig {
            committed_lease_secs: Some(ttl.as_secs().max(1)),
            ..ConsensusConfig::default()
        };

        match self.system_propose(&slot, value.clone(), cfg).await {
            ConsensusResult::Committed { .. } => {
                // #164 bug A: two proposers can both *optimistically* commit against their own
                // local view — the propose return is NOT mutually exclusive. Commit-keys are
                // LWW-resolved by HLC, so let the winning commit converge, then read the
                // authoritative converged value; only the node whose value survived holds the
                // lock. Losers get `Superseded` and never receive a guard.
                tokio::time::sleep(Duration::from_millis(1000)).await;
                match crate::consensus::live_committed_with_hlc(
                        &self.ctx.kv_state, &slot, crate::consensus::wall_now_ms()) {
                    // Fencing token is the commit's HLC, not the ballot: the HLC is monotonic
                    // across successive holders (each observes the prior release), so a resource
                    // that rejects a lower token is actually fenced. The ballot regresses under
                    // gossip lag and is unsafe for fencing (#164 example finding).
                    Some((converged, hlc)) if converged.as_ref() == value.as_ref() =>
                        Ok(LockGuard {
                            ctx:      Arc::clone(&self.ctx),
                            name:     Arc::from(name),
                            value,
                            token:    hlc,
                            released: false,
                        }),
                    _ => Err(ConsistencyError::Superseded),
                }
            }
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
                if let Some(raw) = self.consensus_get(&slot)
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

    fn alloc_port() -> u16 { crate::test_util::alloc_port() }

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

    #[tokio::test]
    async fn test_leased_commit_expires_and_reopens() {
        use crate::consensus::{ConsensusConfig, ConsensusResult};
        let a = make_agent(alloc_port(), &[]).await;
        let c = a.consensus();

        // Commit with a 0-second lease: expires as soon as the wall clock moves.
        let leased = ConsensusConfig { committed_lease_secs: Some(0), ..ConsensusConfig::default() };
        match c.system_propose("lease/slot", Bytes::from_static(b"v1"), leased).await {
            ConsensusResult::Committed { .. } => {}
            other => panic!("expected Committed, got {other:?}"),
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert_eq!(c.consensus_get("lease/slot"), None, "expired lease must read as absent");

        // The slot has reopened: a different value commits (no Superseded), and
        // committing without a lease clears the stale lease entry → permanent.
        match c.system_propose("lease/slot", Bytes::from_static(b"v2"), ConsensusConfig::default()).await {
            ConsensusResult::Committed { .. } => {}
            other => panic!("expected Committed on reopened slot, got {other:?}"),
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert_eq!(
            c.consensus_get("lease/slot").as_deref(), Some(b"v2".as_slice()),
            "permanent re-commit must not be expired by the stale lease entry",
        );
        a.shutdown().await;
    }

    #[tokio::test]
    async fn test_leased_commit_renewal_and_supersession() {
        use crate::consensus::{ConsensusConfig, ConsensusResult};
        let a = make_agent(alloc_port(), &[]).await;
        let c = a.consensus();

        let leased = ConsensusConfig { committed_lease_secs: Some(3600), ..ConsensusConfig::default() };
        match c.system_propose("lease/renew", Bytes::from_static(b"leader-a"), leased.clone()).await {
            ConsensusResult::Committed { .. } => {}
            other => panic!("expected Committed, got {other:?}"),
        }
        // Same value while the lease is live: renewal — allowed.
        match c.system_propose("lease/renew", Bytes::from_static(b"leader-a"), leased.clone()).await {
            ConsensusResult::Committed { .. } => {}
            other => panic!("expected Committed (renewal), got {other:?}"),
        }
        // Different value while the lease is live: superseded, value unchanged.
        match c.system_propose("lease/renew", Bytes::from_static(b"leader-b"), leased).await {
            ConsensusResult::Superseded { .. } => {}
            other => panic!("expected Superseded while lease live, got {other:?}"),
        }
        assert_eq!(c.consensus_get("lease/renew").as_deref(), Some(b"leader-a".as_slice()));

        // Without a lease there is no renewal: even the same value is Superseded.
        match c.system_propose("perm/slot", Bytes::from_static(b"x"), crate::consensus::ConsensusConfig::default()).await {
            ConsensusResult::Committed { .. } => {}
            other => panic!("expected Committed, got {other:?}"),
        }
        match c.system_propose("perm/slot", Bytes::from_static(b"x"), crate::consensus::ConsensusConfig::default()).await {
            ConsensusResult::Superseded { .. } => {}
            other => panic!("permanent slots must stay commit-once, got {other:?}"),
        }
        a.shutdown().await;
    }

    #[tokio::test]
    async fn test_commit_conflict_tripwire() {
        use crate::consensus::{
            consensus_kind, encode_consensus_msg, ConsensusConfig, ConsensusMsg, ConsensusResult,
        };
        use crate::signal::SignalScope;
        use std::sync::Arc;

        let a = make_agent(alloc_port(), &[]).await;
        let _listener = a.consensus().start_consensus_listener(ConsensusConfig::default());

        match a.consensus().system_propose("trip/slot", Bytes::from_static(b"genuine"), ConsensusConfig::default()).await {
            ConsensusResult::Committed { .. } => {}
            other => panic!("expected Committed, got {other:?}"),
        }
        assert_eq!(a.system_stats().commit_conflicts, 0);

        // Forge a COMMIT carrying a different value for the live slot. Local
        // emits self-deliver, so the listener on this node receives it.
        let forged = ConsensusMsg::Commit {
            slot:   Arc::from("trip/slot"),
            ballot: 42,
            value:  Bytes::from_static(b"clobber"),
        };
        assert!(a.mesh().emit(consensus_kind::COMMIT, SignalScope::System, encode_consensus_msg(&forged)));

        // Structural poll: the tripwire must fire and refuse to endorse.
        // 15 s budget: 4 s expired once on a loaded 4-vCPU CI runner
        // (2026-06-12) with the suite's ~16 threads competing; the poll is
        // structural, so a broken tripwire still fails — just later.
        let mut fired = false;
        for _ in 0..300 {
            if a.system_stats().commit_conflicts >= 1 { fired = true; break; }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        assert!(fired, "tripwire did not fire on conflicting COMMIT");
        assert_eq!(
            a.consensus().consensus_get("trip/slot").as_deref(), Some(b"genuine".as_slice()),
            "conflicting COMMIT must not be endorsed",
        );

        // An idempotent re-COMMIT of the same value is legal and must not trip.
        let idempotent = ConsensusMsg::Commit {
            slot:   Arc::from("trip/slot"),
            ballot: 43,
            value:  Bytes::from_static(b"genuine"),
        };
        assert!(a.mesh().emit(consensus_kind::COMMIT, SignalScope::System, encode_consensus_msg(&idempotent)));
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        assert_eq!(a.system_stats().commit_conflicts, 1, "same-value COMMIT must not count as a conflict");
        a.shutdown().await;
    }
}
