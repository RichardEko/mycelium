//! Consensus — epidemic two-phase agreement built on the signal mesh.
//!
//! Lightweight Group-level and System-level agreement built on top of the
//! epidemic signal layer. Uses a two-phase gossip voting protocol:
//!
//! ```text
//! Propose → (votes from group members) → Commit → KV committed/{slot}
//! ```
//!
//! Committed values are written to the Layer 1 KV store at
//! `consensus/committed/{slot}` and anti-entropy synced to late joiners.
//!
//! See [`GossipAgent::group_propose`] and [`GossipAgent::system_propose`] for
//! the entry points. [`GossipAgent::start_consensus_listener`] must be called
//! on every node that should participate as a voter.
//!
//! # Design notes
//!
//! - **Ballot numbering** (from SCP §6.2): monotonic counter stored at
//!   `consensus/ballot/{slot}`; higher ballot supersedes lower.
//! - **Group-scoped votes**: all group members see all votes; any member that
//!   reaches quorum may commit — proposer crash does not stall the slot.
//! - **No signing**: trusted-domain only; Byzantine fault tolerance is
//!   out of scope.
//! - **Quorum slices** (optional, SCP §3.1): nodes may declare trust sets via
//!   [`GossipAgent::declare_trust`]. The basic protocol uses simple majority;
//!   trust-slice-based quorum intersection is a future extension.

use crate::agent::{emit_signal, emit_signal_async, make_gossip_update, TaskCtx};
use crate::config::{GroupTopologyPolicy, TopologyEnforcement};
use crate::framing::{
    dispatch_gossip_send, dispatch_gossip_try_send, make_kv_wire_msg,
    sync_entry_from, ForwardHint, GossipUpdate,
};
use crate::locality::LocalityPath;
use crate::node_id::NodeId;
use crate::signal::{grp_prefix, signal_kind, Signal, SignalScope};
use crate::store::{apply_and_notify, scan_kv_prefix};
use ahash::{AHashMap, AHashSet};
use bytes::Bytes;
use std::{
    sync::Arc,
    time::Duration,
};
use tokio::{
    sync::{mpsc, oneshot, watch},
    time,
};

/// Configuration for a single consensus round.
///
/// Use [`ConsensusConfig::default`] and override only what you need. When
/// `quorum_size` is 0, the library computes majority from the live group
/// membership or peer count at proposal time.
#[derive(Clone, Debug)]
pub struct ConsensusConfig {
    /// Minimum number of distinct voters needed to commit.
    ///
    /// `0` = auto: `floor(N / 2) + 1` where N is the group member count
    /// (for [`group_propose`](crate::GossipAgent::group_propose)) or the
    /// known peer count + 1 (for
    /// [`system_propose`](crate::GossipAgent::system_propose)).
    pub quorum_size:    usize,
    /// How long to wait for votes before declaring a ballot attempt failed.
    pub phase1_timeout: Duration,
    /// Maximum number of ballot attempts before returning [`ConsensusResult::Timeout`].
    pub max_ballots:    u32,
    /// Maximum random sleep (ms) before each ballot retry. Breaks lock-step livelock
    /// when two proposers increment their ballots in unison and repeatedly Nack each
    /// other. Two proposers sleeping for independent durations in `[0, N)` ms will
    /// rarely collide on the next retry; the first to wake succeeds.
    ///
    /// `0` disables jitter (not recommended outside tests). Default: `50`.
    pub ballot_retry_jitter_ms: u64,

    /// When `true`, group members that have a fresh `is_opaque: true` pheromone
    /// entry in Layer I (`load/{node_id}/{any kind}`) are excluded from the
    /// member count used to compute quorum. Prevents ballots from timing out
    /// waiting for overloaded voters.
    ///
    /// Requires `manage_opacity` writing pheromone trails (Fix B) to be effective.
    /// Default: `false`.
    ///
    /// **Availability trade-off**: the effective quorum is floored at 1 (a single
    /// transparent node can commit). When all members are simultaneously opaque, a
    /// lone transparent node satisfies quorum. Set `quorum_size` explicitly to
    /// prevent this if your correctness model requires a minimum voter count
    /// regardless of opacity.
    pub count_opaque_as_absent: bool,

    /// When `true`, this node will not vote in consensus rounds while any of its
    /// managed `load/{node_id}/*` entries show `is_opaque: true`. The node neither
    /// votes nor nacks — it silently drops `PROPOSE` messages while overloaded.
    /// Default: `false`.
    ///
    /// **Liveness risk**: if all nodes are simultaneously opaque, every ballot
    /// times out indefinitely. Set `max_abstain_ballots > 0` to automatically
    /// relax the abstain rule after that many consecutive abstentions, guaranteeing
    /// liveness at the cost of accepting votes from temporarily overloaded nodes.
    pub abstain_when_opaque: bool,

    /// When `true`, the proposer counts only votes from nodes in its own trust
    /// slice declared via [`GossipAgent::declare_trust`]. If no slice is declared
    /// for the group, all votes are counted (same as `false`). Default: `false`.
    pub use_trust_slices: bool,

    /// When `true`, `group_propose` calls [`suggest_leader`](crate::GossipAgent::suggest_leader)
    /// before entering the ballot loop. If the suggested leader is not this node, an additional
    /// deferral of `ballot_retry_jitter_ms` is applied, giving the healthier peer a window to
    /// win the first ballot unopposed.
    ///
    /// Uses [`SENDER_LOG_WINDOW`](crate::signal::SENDER_LOG_WINDOW) as the `max_age` for
    /// pheromone freshness. Default: `false`.
    ///
    /// **Note**: in `group_propose`, suggestion is based on pheromone load + trust counts
    /// within the group. In `system_propose`, suggestion defers this node if it is not the
    /// lowest-load proposer among all peers that have written a `consensus.propose` trail.
    pub use_suggest_leader: bool,

    /// Maximum consecutive ballot attempts during which this node may abstain due to
    /// `abstain_when_opaque`. After this many consecutive abstentions, the node votes
    /// regardless of its opacity state, guaranteeing liveness even when all nodes are
    /// simultaneously overloaded.
    ///
    /// `0` = no limit (always abstain when opaque). Default: `0`.
    pub max_abstain_ballots: u32,

    /// When `Some(secs)`, the commitment is **epoch-leased** rather than permanent:
    /// readers ([`consensus_get`](crate::ConsensusHandle::consensus_get),
    /// `GET /consensus/{slot}`) treat the committed value as absent once
    /// `now − commit_time > secs`, and the slot reopens for re-proposal.
    ///
    /// The lease window is written to `consensus/lease/{slot}` and gossips like any
    /// other key; expiry is evaluated read-side against the committed entry's HLC
    /// timestamp — the same evaporation convention capability entries use
    /// ([`CapEntry::is_fresh`](crate::CapEntry::is_fresh)). No background task, no
    /// renewal RPC: an expired lease is simply no longer acted upon.
    ///
    /// **Renewal** is a fresh quorum round: re-propose the *same* value while the
    /// lease is live (allowed; refreshes the commit timestamp), or any value after
    /// expiry (the slot has reopened). Proposing a *different* value while the lease
    /// is live returns [`ConsensusResult::Superseded`], same as a permanent commit.
    ///
    /// `None` (default) = permanent commitment; behaviour is unchanged.
    pub committed_lease_secs: Option<u64>,
}

impl Default for ConsensusConfig {
    fn default() -> Self {
        Self {
            quorum_size:             0,
            phase1_timeout:          Duration::from_secs(5),
            max_ballots:             3,
            ballot_retry_jitter_ms:  50,
            count_opaque_as_absent:  false,
            abstain_when_opaque:     false,
            use_trust_slices:        false,
            use_suggest_leader:      false,
            max_abstain_ballots:     0,
            committed_lease_secs:    None,
        }
    }
}

/// Per-group quorum requirement for [`GossipAgent::cross_group_propose`].
///
/// Each entry describes one named capability group and the fraction of its
/// members that must accept before the proposal can commit across all groups.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct GroupQuorum {
    /// Name of the capability group (matches the name used in [`GossipAgent::join_group`]).
    pub group: String,
    /// Fraction of group members required to accept. `0.5` means strict majority.
    /// Clamped to `(0.0, 1.0]` at runtime.
    pub quorum: f32,
    /// When `true`, this group acts as a ratification / compliance gate: it must
    /// reach its quorum fraction independently of all other groups. No additional
    /// wire semantics — the effect is simply that this group cannot be outweighed
    /// by others (the commit condition already requires all groups to pass).
    pub veto: bool,
}

/// Outcome of a [`group_propose`](crate::GossipAgent::group_propose) or
/// [`system_propose`](crate::GossipAgent::system_propose) call.
#[derive(Clone, Debug)]
pub enum ConsensusResult {
    /// Quorum reached and the value was committed to the KV store.
    Committed {
        slot:   Arc<str>,
        value:  Bytes,
        ballot: u64,
    },
    /// All ballot attempts timed out without reaching quorum.
    Timeout {
        slot:          Arc<str>,
        ballots_tried: u32,
        /// Votes received during the final ballot attempt.
        ///
        /// Distinguishes "no voters heard at all" (likely partition) from
        /// "some voters heard but quorum was not met" (likely overloaded members
        /// or quorum set too high). `0` if no vote arrived in the last ballot.
        votes_last_ballot: usize,
        /// Quorum size that was required (as computed at proposal time).
        ///
        /// Compare to `votes_last_ballot` to understand how far off quorum was.
        quorum_required: usize,
    },
    /// Another node committed a value for this slot before quorum was reached
    /// by this proposer. The committed value is readable via
    /// [`consensus_get`](crate::GossipAgent::consensus_get).
    Superseded {
        slot:   Arc<str>,
        ballot: u64,
    },
    /// Quorum size was met but the Hard topology gate was not satisfied — too
    /// few distinct domains at `spread_depth`. The proposal is **not** committed.
    /// The caller decides whether to retry, wait for more diverse voters to
    /// come online, or surface the failure.
    ///
    /// Hard enforcement never silently degrades: it is better to refuse a
    /// fault-isolated write than to commit one that doesn't satisfy the
    /// operator-stated redundancy contract.
    TopologyUnsatisfied {
        slot:             Arc<str>,
        ballot:           u64,
        voters_seen:      usize,
        quorum_required:  usize,
        distinct_domains: usize,
        domains_required: usize,
        spread_depth:     usize,
    },
}

/// Wire payload carried inside `Signal.payload` for all consensus messages.
///
/// Encoded with `bincode_cfg()` (fixed-int, same as the rest of the wire format).
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub(crate) enum ConsensusMsg {
    Propose {
        slot:     Arc<str>,
        ballot:   u64,
        value:    Bytes,
        proposer: NodeId,
    },
    Vote {
        slot:   Arc<str>,
        ballot: u64,
        voter:  NodeId,
    },
    Commit {
        slot:   Arc<str>,
        ballot: u64,
        value:  Bytes,
    },
    Nack {
        slot:        Arc<str>,
        seen_ballot: u64,
    },
    /// Voter response carrying the voter's `LocalityPath` so the proposer can
    /// evaluate topology gates (Hard enforcement). Voters that have no
    /// configured locality send `locality: None`; their vote still counts
    /// toward quorum but contributes zero topology diversity.
    ///
    /// Added after `Nack` so old proposers (which decode unknown variants as
    /// `None` and drop the message) silently lose these votes rather than
    /// misinterpreting them. Mixed-version clusters cannot run Hard policies.
    VoteWithLocality {
        slot:     Arc<str>,
        ballot:   u64,
        voter:    NodeId,
        locality: Option<LocalityPath>,
    },
}

/// Cancels the consensus listener task on drop.
///
/// Obtain from [`ConsensusHandle::start_consensus_listener`].
/// The task also exits when the agent shuts down even if this handle is live.
pub struct ConsensusListenerHandle {
    pub(crate) _cancel: oneshot::Sender<()>,
}

/// Well-known signal kind strings for consensus messages.
pub mod consensus_kind {
    /// Phase 1: proposer broadcasts a candidate value.
    pub const PROPOSE: &str = "consensus.propose";
    /// Phase 1: voter confirms it will support the ballot.
    pub const VOTE:    &str = "consensus.vote";
    /// Phase 2: any node broadcasts that quorum has been reached.
    pub const COMMIT:  &str = "consensus.commit";
    /// Phase 1: voter rejects a stale ballot (higher already seen).
    pub const NACK:    &str = "consensus.nack";
}

/// KV key namespace prefixes used by the consensus layer.
pub mod consensus_ns {
    /// Durable committed values. Key: `consensus/committed/{slot}`.
    /// Written on commit; anti-entropy syncs to late joiners automatically.
    pub const COMMITTED: &str = "consensus/committed/";
    /// Highest ballot seen for a slot. Key: `consensus/ballot/{slot}`.
    /// Prevents stale commits from overwriting fresh ones.
    pub const BALLOT:    &str = "consensus/ballot/";
    /// Quorum trust slice declarations (optional, SCP §3.1 inspired).
    /// Key: `consensus/trust/{group}/{node_id}`. Value: bincode-encoded
    /// `Vec<NodeId>` of trusted peers.
    pub const TRUST:     &str = "consensus/trust/";
    /// Epoch-lease window for a committed slot. Key: `consensus/lease/{slot}`.
    /// Value: u64 LE milliseconds. Written at commit time when
    /// [`ConsensusConfig::committed_lease_secs`] is set; absent for permanent
    /// commitments. Expiry is evaluated read-side against the committed
    /// entry's HLC timestamp — see [`ConsensusConfig::committed_lease_secs`].
    pub const LEASE:     &str = "consensus/lease/";
}

// ── Lease helpers ─────────────────────────────────────────────────────────────

pub(crate) fn encode_lease_ms(ms: u64) -> Bytes {
    Bytes::copy_from_slice(&ms.to_le_bytes())
}

/// `None` on malformed bytes — readers treat a malformed lease as *permanent*
/// (never silently expire a commitment because a lease entry was corrupted).
pub(crate) fn decode_lease_ms(bytes: &Bytes) -> Option<u64> {
    (bytes.len() >= 8).then(|| u64::from_le_bytes(bytes[..8].try_into().unwrap_or([0u8; 8])))
}

/// Wall-clock now in milliseconds (same basis as HLC physical time).
pub(crate) fn wall_now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Returns the **live** committed value for `slot`, applying the epoch-lease
/// convention: a committed entry whose `consensus/lease/{slot}` window has
/// elapsed (measured against the committed entry's HLC timestamp) reads as
/// absent — the slot has reopened. Slots without a lease entry are permanent.
///
/// This is the Layer III read-side analogue of [`CapEntry::is_fresh`]
/// (crate::CapEntry::is_fresh): expiry is a property readers apply, not a
/// store mechanism — the substrate never deletes or special-cases the key.
pub(crate) fn live_committed_value(
    kv:     &crate::store::KvState,
    slot:   &str,
    now_ms: u64,
) -> Option<Bytes> {
    live_committed_with_hlc(kv, slot, now_ms).map(|(v, _)| v)
}

/// Like [`live_committed_value`] but also returns the committed entry's **HLC timestamp** — the
/// value used as a lock's fencing token. The HLC is monotonic-respecting-causality, so successive
/// holders of a slot see strictly increasing tokens (unlike the ballot, which is per-node-local
/// and gossip-lagged — it can regress across acquisitions and must NOT be used for fencing).
pub(crate) fn live_committed_with_hlc(
    kv:     &crate::store::KvState,
    slot:   &str,
    now_ms: u64,
) -> Option<(Bytes, u64)> {
    let commit_key = format!("{}{}", consensus_ns::COMMITTED, slot);
    let guard = kv.store.pin();
    let entry = guard.get(commit_key.as_str())?;
    let data  = entry.data.clone()?;
    let hlc   = entry.timestamp;
    let lease_key = format!("{}{}", consensus_ns::LEASE, slot);
    let Some(lease_bytes) = guard.get(lease_key.as_str()).and_then(|e| e.data.clone()) else {
        return Some((data, hlc)); // no lease (or tombstoned lease) → permanent
    };
    let Some(lease_ms) = decode_lease_ms(&lease_bytes) else {
        return Some((data, hlc)); // malformed lease → treat as permanent
    };
    let written_ms = crate::hlc::physical_ms(entry.timestamp);
    // BOUNDED-CLOCK-SKEW ASSUMPTION (audit 2026-07-15). This compares the caller's clock (`now_ms`,
    // currently a wall-clock reading) against the writer's HLC physical component — two clock
    // *domains*. Under clock skew two nodes can disagree on whether the SAME lease is still live, so
    // both can briefly believe they hold a `distributed_lock`. Like every lease-based lock (Chubby,
    // etcd), correctness for a *lease* holds only while skew < lease. The **skew-proof** guard for
    // correctness-critical writes is the FENCING TOKEN returned alongside — the commit's HLC, which is
    // monotonic across successive holders (each observes the prior release); a resource that rejects a
    // lower token is fenced even if two nodes momentarily both think they hold the lock. FOLLOW-UP: put
    // both sides of this comparison on the causal HLC clock (pass `hlc.current()` physical, not raw
    // wall time) at all call sites to shrink the window to true unsynchronised skew.
    (now_ms.saturating_sub(written_ms) <= lease_ms).then_some((data, hlc))
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Counts how many distinct segment values appear at `depth` across `voters`.
/// Voters whose locality is `None`, or whose path is shorter than `depth + 1`,
/// contribute zero. Order-independent and unbiased.
pub(crate) fn distinct_domains_at_depth(
    voters: &AHashMap<NodeId, Option<LocalityPath>>,
    depth: usize,
) -> usize {
    let mut seen: AHashSet<&str> = AHashSet::new();
    for loc in voters.values().flatten() {
        if let Some(seg) = loc.value_at(depth) {
            seen.insert(seg.as_ref());
        }
    }
    seen.len()
}

/// Returns whether a Hard topology policy is satisfied by the current voter
/// set. Soft and policy-less calls always pass. The returned `usize` is the
/// current `distinct_domains` count — useful for populating
/// `ConsensusResult::TopologyUnsatisfied`.
pub(crate) fn evaluate_topology_gate(
    voters: &AHashMap<NodeId, Option<LocalityPath>>,
    policy: &GroupTopologyPolicy,
) -> (bool, usize) {
    if policy.enforcement != TopologyEnforcement::Hard {
        return (true, 0);
    }
    let Some(depth) = policy.spread_depth else {
        // Hard with no spread_depth is invalid config; validate() rejects this
        // at startup. Treat as pass to avoid stalling production after a
        // hot-reload bug.
        return (true, 0);
    };
    let distinct = distinct_domains_at_depth(voters, depth);
    (distinct >= policy.spread_min_distinct, distinct)
}

/// Context for mid-ballot opaque-member recomputation.
///
/// Injected by `group_propose` / `system_propose` at the signal-mesh call site so
/// `propose` does not read `KvState` directly — the opacity query strategy is an
/// injected dependency, not a consensus-engine concern.
pub(crate) struct OpaqueRecompute {
    /// Total group/system member count at proposal time (before opacity exclusions).
    pub(crate) total_members: usize,
    /// Operator-configured quorum size (0 = auto-majority from active count).
    pub(crate) config_quorum: usize,
    /// Callback that returns the current number of opaque members on each call.
    pub(crate) count_opaque:  Arc<dyn Fn() -> usize + Send + Sync>,
}

// ── ConsensusEngine ──────────────────────────────────────────────────────────
//
// Shared context for both the voter/listener task and the proposer.
// Constructed by GossipAgent::start_consensus_listener and
// GossipAgent::group_propose / system_propose, then either spawned
// (spawn_listener) or driven directly (propose).

/// Bundles the Arc fields needed for consensus tasks.
///
/// Replaces the former `ConsensusListenerCtx` that was private to `agent.rs`.
/// Infrastructure fields are shared via `Arc<TaskCtx>` to avoid cloning them
/// individually at each spawn site.
pub(crate) struct ConsensusEngine {
    pub(crate) task_ctx:            Arc<TaskCtx>,
    /// When `true`, this node silently abstains from voting while any pheromone
    /// trail under `sys/load/{node_id}/` shows `is_opaque: true`.
    pub(crate) abstain_when_opaque: bool,
    /// When `true`, the proposer filters incoming votes against its declared
    /// trust slice for the group (`consensus/trust/{group}/{node_id}`).
    pub(crate) use_trust_slices:    bool,
    pub(crate) max_abstain_ballots: u32,
    /// This node's locality, captured at engine-construction time from
    /// `GossipConfig::locality_path`. Used by the voter task to populate
    /// `VoteWithLocality`. `None` when the node has no configured locality.
    pub(crate) self_locality:       Option<LocalityPath>,
    /// Per-group topology policy for the proposer side. `None` means no
    /// policy: ballots commit on quorum without a diversity check.
    /// `Some(Soft)` policies still skip the gate — they affect fan-out
    /// scoring only. `Some(Hard)` enables the topology gate in `propose`.
    pub(crate) topology_policy:     Option<GroupTopologyPolicy>,
}

impl ConsensusEngine {
    // ── KV helpers ───────────────────────────────────────────────────────────
    //
    // ConsensusEngine writes directly to the KV substrate (`apply_and_notify`,
    // `dispatch_gossip_*`, `make_gossip_update`) for its own `consensus/ballot/*`
    // and `consensus/committed/*` namespace. This is intentional, not a layer violation:
    //
    // 1. `GossipAgent::set` cannot be used here — it would create a reference cycle
    //    (`ConsensusEngine` → `GossipAgent` → task handles → `ConsensusEngine`),
    //    so `ConsensusEngine` receives only `Arc<TaskCtx>` with no agent back-reference.
    //
    // 2. Ownership of `consensus/*` is analogous to the opacity governor owning
    //    `sys/load/*`: the module writes directly to its documented KV prefix because
    //    the data is architectural state, not user data.
    //
    // 3. Consensus uses exactly the same KV primitives (`make_gossip_update` +
    //    `apply_and_notify`) that every other subsystem uses — no additional coupling
    //    to the transport layer is introduced.

    fn get(&self, key: &str) -> Option<Bytes> {
        self.task_ctx.kv_state.store.pin().get(key).and_then(|e| e.data.clone())
    }

    fn read_ballot(&self, ballot_key: &str) -> u64 {
        self.get(ballot_key).map(|b| decode_ballot(&b)).unwrap_or(0)
    }

    /// Lease-aware committed read — see [`live_committed_value`]. An expired
    /// lease reads as `None`: the slot has reopened for re-proposal.
    fn live_committed(&self, slot: &str) -> Option<Bytes> {
        live_committed_value(&self.task_ctx.kv_state, slot, wall_now_ms())
    }

    /// Applies a KV update from within a consensus task.
    /// Uses `try_send` for gossip dispatch — dropped frames recovered via anti-entropy.
    fn kv_set(&self, key: String, value: Bytes) {
        let tc  = &self.task_ctx;
        let upd = make_gossip_update(&tc.node_id, tc.default_ttl, Arc::from(key.as_str()), value, false, &tc.hlc);
        apply_and_notify(&tc.kv_state, &upd);
        let tls = tc.tls.get().map(std::sync::Arc::as_ref);
        let msg = make_kv_wire_msg(upd, tc.node_id.id_hash(), tls);
        dispatch_gossip_try_send(
            &tc.gossip_txs, msg,
            tc.node_id.id_hash(), ForwardHint::All, &tc.kv_state.dropped_frames,
        );
    }

    /// Tombstones `key` in the KV store and gossips the deletion.
    /// Used to clean up ballot entries once a slot has committed.
    fn kv_delete(&self, key: &str) {
        let tc  = &self.task_ctx;
        let upd = make_gossip_update(&tc.node_id, tc.default_ttl, Arc::from(key), Bytes::new(), true, &tc.hlc);
        apply_and_notify(&tc.kv_state, &upd);
        let tls = tc.tls.get().map(std::sync::Arc::as_ref);
        let msg = make_kv_wire_msg(upd, tc.node_id.id_hash(), tls);
        dispatch_gossip_try_send(
            &tc.gossip_txs, msg,
            tc.node_id.id_hash(), ForwardHint::All, &tc.kv_state.dropped_frames,
        );
    }

    /// Like `kv_set` but awaits channel capacity (used by the proposer).
    /// Returns the applied update so the caller can WAL-append the exact entry.
    async fn set_async(&self, key: &str, value: Bytes) -> GossipUpdate {
        let tc  = &self.task_ctx;
        let upd = make_gossip_update(&tc.node_id, tc.default_ttl, Arc::from(key), value, false, &tc.hlc);
        apply_and_notify(&tc.kv_state, &upd);
        let tls = tc.tls.get().map(std::sync::Arc::as_ref);
        let msg = make_kv_wire_msg(upd.clone(), tc.node_id.id_hash(), tls);
        dispatch_gossip_send(
            &tc.gossip_txs, msg,
            tc.node_id.id_hash(), ForwardHint::All,
        ).await;
        upd
    }

    // ── Signal-mesh bridge helpers ───────────────────────────────────────────

    /// True if any `sys/load/{node_id}/*` pheromone entry is `is_opaque`.
    ///
    /// Delegates to the opacity helper in `agent::opacity` so this type
    /// does not scan `KvState` directly.
    fn is_overloaded(&self) -> bool {
        crate::agent::is_self_opaque(&self.task_ctx.kv_state, &self.task_ctx.node_id)
    }

    fn emit(&self, kind: Arc<str>, scope: SignalScope, payload: Bytes) {
        emit_signal(&self.task_ctx, kind, scope, payload);
    }

    /// Evaluates the Hard topology gate against the current voter set, honouring
    /// any `sys/topology-override/{group}` operator override. Returns:
    /// - `passes`: whether the proposer may commit on this voter set
    /// - `distinct_domains`: count at `spread_depth` (0 when no Hard policy)
    /// - `policy_meta`: `Some((spread_depth, spread_min_distinct))` only when a
    ///   Hard policy is actually being enforced — used to populate
    ///   `ConsensusResult::TopologyUnsatisfied`.
    ///
    /// **`sys/topology-override/{group}` value format**: the override is active
    /// when the KV value is exactly the ASCII bytes `b"true"`. Any other value
    /// (including absent) leaves Hard enforcement in effect. Operators writing
    /// `b"false"` or empty bytes will *not* disable enforcement — this guards
    /// against fat-finger overrides where the presence of the key alone would
    /// otherwise be load-bearing.
    fn topology_check(
        &self,
        voters:     &AHashMap<NodeId, Option<LocalityPath>>,
        group_name: Option<&str>,
    ) -> (bool, usize, Option<(usize, usize)>) {
        let Some(policy) = self.topology_policy.as_ref() else { return (true, 0, None); };
        if policy.enforcement != TopologyEnforcement::Hard { return (true, 0, None); }
        if let Some(name) = group_name {
            let override_key = format!("sys/topology-override/{}", name);
            if let Some(value) = self.get(&override_key)
                && value.as_ref() == b"true" {
                    // Operator override — degrade to no-gate behaviour.
                    return (true, 0, None);
                }
        }
        let (passes, distinct) = evaluate_topology_gate(voters, policy);
        let meta = (policy.spread_depth.unwrap_or(0), policy.spread_min_distinct);
        (passes, distinct, Some(meta))
    }

    async fn emit_async(&self, kind: Arc<str>, scope: SignalScope, payload: Bytes) -> bool {
        emit_signal_async(&self.task_ctx, kind, scope, payload).await
    }

    // ── Payload signing / verification ───────────────────────────────────────

    /// Wraps `bytes` in a `SignedConsensusMsg` when TLS is active; returns
    /// `bytes` unchanged when TLS is disabled (zero overhead on the non-TLS path).
    fn sign_payload(&self, bytes: Bytes) -> Bytes {
        #[cfg(feature = "tls")]
        if let Some(tls) = self.task_ctx.tls.get() {
            let sig = crate::tls::sign_bytes(&tls.signing_key(), &bytes);
            let signed = SignedConsensusMsg {
                msg_bytes:  bytes.clone(),
                signer:     self.task_ctx.node_id.clone(),
                signature:  sig.to_vec(),
            };
            if let Ok(encoded) = mycelium_core::serde_fixint::to_vec(&signed) {
                return Bytes::from(encoded);
            }
        }
        bytes
    }

    /// Decodes `payload` as a `ConsensusMsg`, verifying its Ed25519 signature
    /// first when TLS is enabled. Returns `None` on bad signature or decode failure.
    fn decode_verify(&self, payload: &Bytes) -> Option<ConsensusMsg> {
        #[cfg(feature = "tls")]
        if self.task_ctx.tls.get().is_some() {
            let signed: SignedConsensusMsg =
                mycelium_core::serde_fixint::from_slice(payload).ok()?;
            // Look up the sender's verifying key SET (WS5 retained keys): the
            // in-memory cache first, else parse the `sys/identity/` KV entry
            // (32 = one key, 64 = current‖previous). Verify against any so a
            // rotated key still validates in-flight/historical consensus msgs.
            let mut key_set: Vec<[u8; 32]> =
                self.task_ctx.peer_keys.pin().get(&signed.signer).cloned().unwrap_or_default();
            if key_set.is_empty() {
                let kv_key = format!("{}{}", crate::signal::kv_ns::IDENTITY, signed.signer);
                if let Some(b) = self.task_ctx.kv_state.store.pin()
                    .get(kv_key.as_str()).and_then(|e| e.data.clone())
                {
                    key_set = crate::agent::helpers::parse_identity_keys(&b);
                }
            }
            if key_set.is_empty()
                || !key_set.iter().any(|k| crate::tls::verify_bytes(k, &signed.msg_bytes, &signed.signature))
            {
                tracing::warn!("dropping consensus msg: bad/unknown signature from {}", signed.signer);
                return None;
            }
            return decode_consensus_msg(&signed.msg_bytes);
        }
        decode_consensus_msg(payload)
    }

    // ── Proposer ─────────────────────────────────────────────────────────────

    /// Runs one full proposal attempt sequence for `slot`.
    ///
    /// Called by `GossipAgent::group_propose` and `GossipAgent::system_propose`.
    ///
    /// `opaque_recompute` — when `Some`, `propose` registers for `BOUNDARY_OPAQUE` signals
    /// and re-evaluates the effective quorum size mid-ballot when any member transitions.
    /// The callback is built by the call site so `propose` does not read `KvState`
    /// directly — the opacity query strategy is an injected dependency.
    pub(crate) async fn propose(
        &self,
        scope:             SignalScope,
        slot:              Arc<str>,
        value:             Bytes,
        quorum_size:       usize,
        config:            ConsensusConfig,
        opaque_recompute:  Option<OpaqueRecompute>,
    ) -> ConsensusResult {
        let ballot_key = format!("{}{}", consensus_ns::BALLOT, &*slot);
        let commit_key = format!("{}{}", consensus_ns::COMMITTED, &*slot);

        let mut quorum_size = quorum_size;
        // Register for BOUNDARY_TRANSPARENT signals so we can re-evaluate quorum
        // mid-ballot when previously-opaque members become available.
        // Watch for BOUNDARY_OPAQUE: a member going opaque shrinks active_members,
        // which may lower the quorum threshold below votes already collected.
        let mut opaque_rx = if opaque_recompute.is_some() {
            Some(self.task_ctx.signal_handlers.register_with_capacity(
                Arc::from(signal_kind::BOUNDARY_OPAQUE), 8,
            ))
        } else {
            None
        };

        // Build trust set once before the ballot loop — it doesn't change mid-round.
        let trust_set: Option<AHashSet<u64>> = if self.use_trust_slices {
            if let SignalScope::Group(ref group_name) = scope {
                let key = format!("{}{}/{}", consensus_ns::TRUST, group_name, self.task_ctx.node_id);
                self.get(&key).and_then(|b| {
                    mycelium_core::serde_fixint::from_slice::<Vec<NodeId>>(&b).ok()
                }).map(|peers| peers.iter().map(|p| p.id_hash()).collect())
            } else {
                None
            }
        } else {
            None
        };

        let mut ballot = self.read_ballot(&ballot_key) + 1;
        let mut votes_last_ballot: usize = 0;
        // Captured topology-gate failure from the most recent ballot that
        // reached quorum-by-count but failed the Hard gate. Used to surface
        // `TopologyUnsatisfied` after all ballots are exhausted.
        let mut topology_failed: Option<(usize, usize, usize)> = None;

        // Epoch lease window (ms) for this proposal, if configured.
        let lease_ms = config.committed_lease_secs.map(|s| s.saturating_mul(1000));
        // A slot already committed with the *same* value under a lease may be
        // re-proposed while live — a successful re-commit refreshes the commit
        // timestamp (lease renewal). Any other live commitment supersedes us.
        let superseded_by_live = |existing: &Bytes| -> bool {
            !(lease_ms.is_some() && *existing == value)
        };

        // Extract the group name once for `sys/topology-override/{group}` lookups
        // and for completing the TopologyUnsatisfied return.
        let group_name: Option<Arc<str>> = match &scope {
            SignalScope::Group(g) => Some(Arc::clone(g)),
            _                     => None,
        };

        for _attempt in 0..config.max_ballots {
            if let Some(existing) = self.live_committed(&slot)
                && superseded_by_live(&existing) {
                    return ConsensusResult::Superseded {
                        slot,
                        ballot: self.read_ballot(&ballot_key),
                    };
                }

            // Early exit when all members are opaque — waiting for votes is futile.
            if let Some(ref or) = opaque_recompute {
                let opaque = (or.count_opaque)();
                if opaque >= or.total_members {
                    #[cfg(feature = "metrics")]
                    metrics::counter!("mycelium_consensus_timeouts_total", "reason" => "all_opaque")
                        .increment(1);
                    return ConsensusResult::Timeout {
                        slot,
                        ballots_tried: 0,
                        votes_last_ballot: 0,
                        quorum_required: quorum_size,
                    };
                }
            }

            self.set_async(ballot_key.as_str(), encode_ballot(ballot)).await;

            // Register before emitting so no vote/nack can arrive before we listen.
            let mut vote_rx = self.task_ctx.signal_handlers.register_with_capacity(
                Arc::from(consensus_kind::VOTE), 512,
            );
            let mut nack_rx = self.task_ctx.signal_handlers.register_with_capacity(
                Arc::from(consensus_kind::NACK), 64,
            );

            let propose_msg = ConsensusMsg::Propose {
                slot: Arc::clone(&slot), ballot, value: value.clone(),
                proposer: self.task_ctx.node_id.clone(),
            };
            self.emit_async(
                Arc::from(consensus_kind::PROPOSE), scope.clone(), self.sign_payload(encode_consensus_msg(&propose_msg)),
            ).await;

            // Voter dedup map per (slot, ballot). NodeId-keyed so each voter contributes
            // exactly once even if they re-emit; the Option<LocalityPath> value is required
            // for Hard topology gate evaluation. Cannot use signal_handlers.quorum_for_group
            // here: the sender_log records (sender, received_at) without slot/ballot
            // correlation, so it would conflate votes from different rounds.
            //
            // Membership changes mid-ballot are intentionally ignored. The group member
            // set is captured once before the ballot loop (in group_propose) and the quorum
            // threshold is fixed for that ballot's lifetime. A joining member's votes do not
            // count toward this ballot; a leaving member's existing vote remains counted.
            let mut voters: AHashMap<NodeId, Option<LocalityPath>> = AHashMap::new();
            voters.insert(self.task_ctx.node_id.clone(), self.self_locality.clone());

            // Single-node quorum check before entering the collect loop.
            if let Some(res) = self.try_commit_if_ready(
                &voters, quorum_size, group_name.as_deref(),
                &scope, &slot, ballot, &value, &ballot_key, &commit_key, lease_ms,
            ).await {
                return res;
            }

            // Drive one ballot attempt.
            let deadline = time::Instant::now() + config.phase1_timeout;
            let outcome = self.collect_one_ballot(
                &mut voters, &mut quorum_size,
                &mut vote_rx, &mut nack_rx, &mut opaque_rx,
                deadline,
                &slot, ballot, &value, &scope,
                &ballot_key, &commit_key,
                group_name.as_deref(),
                trust_set.as_ref(),
                opaque_recompute.as_ref(),
                lease_ms,
            ).await;

            let mut nack_ballot = 0u64;
            match outcome {
                BallotOutcome::Committed(res) => return res,
                BallotOutcome::NackHigher(b)  => nack_ballot = b,
                BallotOutcome::Timeout        => {}
            }

            if let Some(existing) = self.live_committed(&slot)
                && superseded_by_live(&existing) {
                    return ConsensusResult::Superseded {
                        slot,
                        ballot: self.read_ballot(&ballot_key),
                    };
                }

            votes_last_ballot = voters.len();
            // If this ballot reached quorum-by-count but failed the Hard gate,
            // remember it for the final TopologyUnsatisfied return.
            if voters.len() >= quorum_size {
                let (passes, distinct, meta) = self.topology_check(&voters, group_name.as_deref());
                if !passes
                    && let Some((depth, required)) = meta {
                        topology_failed = Some((distinct, required, depth));
                    }
            }

            if config.ballot_retry_jitter_ms > 0 {
                let jitter = fastrand::u64(0..config.ballot_retry_jitter_ms);
                tokio::time::sleep(Duration::from_millis(jitter)).await;
            }
            ballot = nack_ballot.max(self.read_ballot(&ballot_key)).max(ballot) + 1;
        }

        // After exhausting max_ballots: prefer TopologyUnsatisfied over Timeout
        // when the last attempt reached quorum but failed the gate — the caller
        // needs to distinguish "not enough voters" from "voters not diverse enough."
        if let Some((distinct, required, depth)) = topology_failed {
            return ConsensusResult::TopologyUnsatisfied {
                slot,
                ballot,
                voters_seen:      votes_last_ballot,
                quorum_required:  quorum_size,
                distinct_domains: distinct,
                domains_required: required,
                spread_depth:     depth,
            };
        }

        #[cfg(feature = "metrics")]
        metrics::counter!("mycelium_consensus_timeouts_total",
            "reason" => if votes_last_ballot == 0 { "no_voters" } else { "quorum_short" })
            .increment(1);
        ConsensusResult::Timeout {
            slot,
            ballots_tried: config.max_ballots,
            votes_last_ballot,
            quorum_required: quorum_size,
        }
    }

    /// Proposes `value` for `slot` requiring independent quorum from each group in `groups`.
    ///
    /// Commits only when every group reaches its configured [`GroupQuorum::quorum`] fraction.
    /// Uses a single ballot round so the commit is atomic — no group can commit without
    /// all others also committing.
    ///
    /// Called by [`GossipAgent::cross_group_propose`].
    pub(crate) async fn cross_propose(
        &self,
        slot:   Arc<str>,
        value:  Bytes,
        groups: &[GroupQuorum],
        config: ConsensusConfig,
    ) -> ConsensusResult {
        if groups.is_empty() {
            #[cfg(feature = "metrics")]
            metrics::counter!("mycelium_consensus_timeouts_total", "reason" => "empty_groups")
                .increment(1);
            return ConsensusResult::Timeout {
                slot,
                ballots_tried: 0, votes_last_ballot: 0, quorum_required: 0,
            };
        }

        let ballot_key = format!("{}{}", consensus_ns::BALLOT,    &*slot);
        let commit_key = format!("{}{}", consensus_ns::COMMITTED, &*slot);

        // Epoch lease window (ms), with the same renewal exception as `propose`:
        // a live same-value leased commitment may be re-proposed to refresh it.
        let lease_ms = config.committed_lease_secs.map(|s| s.saturating_mul(1000));
        let superseded_by_live = |existing: &Bytes| -> bool {
            !(lease_ms.is_some() && *existing == value)
        };

        if let Some(existing) = self.live_committed(&slot)
            && superseded_by_live(&existing) {
                return ConsensusResult::Superseded { slot, ballot: self.read_ballot(&ballot_key) };
            }

        // Per-group state (rebuilt from KV once before the ballot loop).
        struct CrossState {
            members:     ahash::AHashSet<NodeId>,
            quorum_frac: f32,
            accepts:     usize,
            seen:        ahash::AHashSet<NodeId>, // distinct voters — each counts once
        }

        let mut group_states: AHashMap<Arc<str>, CrossState> = AHashMap::new();
        for gq in groups {
            let prefix  = grp_prefix(&gq.group);
            let entries = scan_kv_prefix(&self.task_ctx.kv_state, &prefix);
            let members: ahash::AHashSet<NodeId> = entries.iter()
                .filter_map(|(key, _)| key.strip_prefix(&prefix).and_then(|s| s.parse().ok()))
                .collect();
            group_states.insert(Arc::from(gq.group.as_str()), CrossState {
                members,
                quorum_frac: gq.quorum.clamp(0.001, 1.0),
                accepts: 0,
                seen: ahash::AHashSet::new(),
            });
        }

        // Pre-compute voter → group membership for O(1) lookup during vote collection.
        let mut node_groups: AHashMap<NodeId, Vec<Arc<str>>> = AHashMap::new();
        for (gname, gs) in &group_states {
            for nid in &gs.members {
                node_groups.entry(nid.clone()).or_default().push(Arc::clone(gname));
            }
        }

        let scope = SignalScope::Groups(
            groups.iter().map(|g| Arc::from(g.group.as_str())).collect(),
        );

        let mut vote_rx = self.task_ctx.signal_handlers.register_with_capacity(
            Arc::from(consensus_kind::VOTE), 512,
        );
        let mut nack_rx = self.task_ctx.signal_handlers.register_with_capacity(
            Arc::from(consensus_kind::NACK), 64,
        );

        let mut ballot = self.read_ballot(&ballot_key) + 1;

        for _attempt in 0..config.max_ballots {
            for gs in group_states.values_mut() { gs.accepts = 0; }

            self.set_async(&ballot_key, encode_ballot(ballot)).await;

            let propose_msg = ConsensusMsg::Propose {
                slot: Arc::clone(&slot), ballot, value: value.clone(),
                proposer: self.task_ctx.node_id.clone(),
            };
            self.emit_async(
                Arc::from(consensus_kind::PROPOSE),
                scope.clone(),
                self.sign_payload(encode_consensus_msg(&propose_msg)),
            ).await;

            let deadline  = time::Instant::now() + config.phase1_timeout;
            let sleep_fut = time::sleep_until(deadline);
            tokio::pin!(sleep_fut);
            let mut nack_ballot = 0u64;

            'collect: loop {
                tokio::select! { biased;
                    _ = &mut sleep_fut => break 'collect,
                    Some(sig) = vote_rx.recv() => {
                        let (s, b, voter) = match self.decode_verify(&sig.payload) {
                            Some(ConsensusMsg::Vote { slot: s, ballot: b, voter }) =>
                                (s, b, voter),
                            Some(ConsensusMsg::VoteWithLocality { slot: s, ballot: b, voter, .. }) =>
                                (s, b, voter),
                            _ => continue 'collect,
                        };
                        if s != slot || b != ballot { continue 'collect; }

                        if let Some(gnames) = node_groups.get(&voter) {
                            for gname in gnames {
                                if let Some(gs) = group_states.get_mut(gname) {
                                    // Count each DISTINCT voter once — a re-delivered vote
                                    // (gossip re-flood / duplicate PROPOSE) must not inflate
                                    // the tally (the sibling `propose` path is NodeId-keyed too).
                                    if gs.seen.insert(voter.clone()) {
                                        gs.accepts += 1;
                                    }
                                }
                            }
                        }

                        // Commit when every group has independently reached its fraction.
                        let all_ready = group_states.values().all(|gs| {
                            gs.accepts >= cross_group_quorum(gs.members.len(), gs.quorum_frac)
                        });
                        if all_ready {
                            // Lost race with another proposer mid-ballot: refuse
                            // to clobber a different live commitment.
                            if let Some(existing) = self.live_committed(&slot)
                                && existing != value {
                                    return ConsensusResult::Superseded {
                                        slot, ballot: self.read_ballot(&ballot_key),
                                    };
                                }
                            let commit_msg = ConsensusMsg::Commit {
                                slot: Arc::clone(&slot), ballot, value: value.clone(),
                            };
                            self.emit_async(
                                Arc::from(consensus_kind::COMMIT),
                                scope.clone(),
                                self.sign_payload(encode_consensus_msg(&commit_msg)),
                            ).await;
                            let committed_upd = self.set_async(&commit_key, value.clone()).await;
                            if let Some(wal) = self.task_ctx.wal.get() {
                                let _ = wal.append_sync(
                                    crate::framing::sync_entry_from(&committed_upd)
                                ).await;
                            }
                            self.write_lease(&slot, lease_ms).await;
                            self.kv_delete(&ballot_key);
                            return ConsensusResult::Committed { slot, value: value.clone(), ballot };
                        }
                    }
                    Some(sig) = nack_rx.recv() => {
                        if let Some(ConsensusMsg::Nack { slot: s, seen_ballot }) =
                            self.decode_verify(&sig.payload)
                            && s == slot && seen_ballot > ballot {
                                nack_ballot = seen_ballot;
                                break 'collect;
                            }
                    }
                }
            }

            if let Some(existing) = self.live_committed(&slot)
                && superseded_by_live(&existing) {
                    return ConsensusResult::Superseded { slot, ballot: self.read_ballot(&ballot_key) };
                }

            if config.ballot_retry_jitter_ms > 0 {
                let jitter = fastrand::u64(0..config.ballot_retry_jitter_ms);
                tokio::time::sleep(Duration::from_millis(jitter)).await;
            }
            ballot = nack_ballot.max(self.read_ballot(&ballot_key)).max(ballot) + 1;
        }

        let votes_last_ballot: usize = group_states.values().map(|gs| gs.accepts).sum();
        #[cfg(feature = "metrics")]
        metrics::counter!("mycelium_consensus_timeouts_total",
            "reason" => if votes_last_ballot == 0 { "no_voters" } else { "quorum_short" })
            .increment(1);
        ConsensusResult::Timeout {
            slot,
            ballots_tried:     config.max_ballots,
            votes_last_ballot,
            quorum_required:   groups.len(),
        }
    }

    /// Evaluates quorum-by-count + topology gate. When both pass, dispatches
    /// the commit (emit `COMMIT`, write `consensus/committed/{slot}` and the
    /// `consensus/lease/{slot}` window when leased, tombstone
    /// `consensus/ballot/{slot}`) and returns `Some(ConsensusResult::Committed)`.
    /// Returns `None` when either gate fails — caller keeps collecting.
    ///
    /// Refuses to clobber a *different* live commitment that landed between the
    /// caller's supersession check and quorum being reached (a lost race with
    /// another proposer) — returns `Superseded` instead of overwriting.
    #[allow(clippy::too_many_arguments)]
    async fn try_commit_if_ready(
        &self,
        voters:      &AHashMap<NodeId, Option<LocalityPath>>,
        quorum_size: usize,
        group_name:  Option<&str>,
        scope:       &SignalScope,
        slot:        &Arc<str>,
        ballot:      u64,
        value:       &Bytes,
        ballot_key:  &str,
        commit_key:  &str,
        lease_ms:    Option<u64>,
    ) -> Option<ConsensusResult> {
        if voters.len() < quorum_size { return None; }
        let (passes, _, _) = self.topology_check(voters, group_name);
        if !passes { return None; }

        if let Some(existing) = self.live_committed(slot)
            && existing != *value {
                return Some(ConsensusResult::Superseded {
                    slot:   Arc::clone(slot),
                    ballot: self.read_ballot(ballot_key),
                });
            }

        let commit = ConsensusMsg::Commit {
            slot: Arc::clone(slot), ballot, value: value.clone(),
        };
        self.emit_async(
            Arc::from(consensus_kind::COMMIT), scope.clone(), self.sign_payload(encode_consensus_msg(&commit)),
        ).await;
        let committed_upd = self.set_async(commit_key, value.clone()).await;
        if let Some(wal) = self.task_ctx.wal.get() {
            let _ = wal.append_sync(sync_entry_from(&committed_upd)).await;
        }
        self.write_lease(slot, lease_ms).await;
        self.kv_delete(ballot_key);
        Some(ConsensusResult::Committed {
            slot:   Arc::clone(slot),
            value:  value.clone(),
            ballot,
        })
    }

    /// Writes (or clears) the epoch-lease window for `slot` at commit time.
    ///
    /// `Some(ms)` → write `consensus/lease/{slot}` (WAL-appended alongside the
    /// committed value so a restart cannot resurrect an expired slot as
    /// permanent). `None` → tombstone any stale lease left by a previous
    /// leased commitment, so the new permanent commit cannot be expired by it.
    async fn write_lease(&self, slot: &Arc<str>, lease_ms: Option<u64>) {
        let lease_key = format!("{}{}", consensus_ns::LEASE, &**slot);
        match lease_ms {
            Some(ms) => {
                let upd = self.set_async(&lease_key, encode_lease_ms(ms)).await;
                if let Some(wal) = self.task_ctx.wal.get() {
                    let _ = wal.append_sync(sync_entry_from(&upd)).await;
                }
            }
            None => {
                if self.get(&lease_key).is_some() {
                    self.kv_delete(&lease_key);
                }
            }
        }
    }

    /// Drives one ballot's `'collect` loop: accept votes (Vote or
    /// VoteWithLocality, with optional trust-slice filtering), accept
    /// nacks, re-evaluate quorum on opacity recompute, all until
    /// `deadline` expires. Returns the outcome.
    ///
    /// `voters` and `quorum_size` are passed `&mut` so callers can read
    /// final state after a `Timeout` outcome (for the
    /// `TopologyUnsatisfied`-vs-`Timeout` decision at the end of
    /// `propose`).
    #[allow(clippy::too_many_arguments)]
    async fn collect_one_ballot(
        &self,
        voters:           &mut AHashMap<NodeId, Option<LocalityPath>>,
        quorum_size:      &mut usize,
        vote_rx:          &mut mpsc::Receiver<Signal>,
        nack_rx:          &mut mpsc::Receiver<Signal>,
        opaque_rx:        &mut Option<mpsc::Receiver<Signal>>,
        deadline:         time::Instant,
        slot:             &Arc<str>,
        ballot:           u64,
        value:            &Bytes,
        scope:            &SignalScope,
        ballot_key:       &str,
        commit_key:       &str,
        group_name:       Option<&str>,
        trust_set:        Option<&AHashSet<u64>>,
        opaque_recompute: Option<&OpaqueRecompute>,
        lease_ms:         Option<u64>,
    ) -> BallotOutcome {
        let sleep = time::sleep_until(deadline);
        tokio::pin!(sleep);

        loop {
            tokio::select! { biased;
                _ = &mut sleep => return BallotOutcome::Timeout,
                Some(sig) = vote_rx.recv() => {
                    // Accept both Vote (legacy, no locality) and VoteWithLocality.
                    // Legacy votes contribute to quorum but to zero topology diversity.
                    let (s, b, voter, locality) = match self.decode_verify(&sig.payload) {
                        Some(ConsensusMsg::Vote { slot: s, ballot: b, voter }) =>
                            (s, b, voter, None),
                        Some(ConsensusMsg::VoteWithLocality { slot: s, ballot: b, voter, locality }) =>
                            (s, b, voter, locality),
                        _ => continue,
                    };
                    if s == *slot && b == ballot {
                        // Trust-slice filtering: only count votes from declared peers.
                        if let Some(ts) = trust_set
                            && !ts.contains(&voter.id_hash()) { continue; }
                        voters.insert(voter, locality);
                        if let Some(res) = self.try_commit_if_ready(
                            voters, *quorum_size, group_name,
                            scope, slot, ballot, value, ballot_key, commit_key, lease_ms,
                        ).await {
                            return BallotOutcome::Committed(res);
                        }
                        // Quorum count met but topology gate failed — keep
                        // collecting until timeout in case a more diverse voter
                        // arrives. (try_commit_if_ready returned None.)
                    }
                }
                Some(sig) = nack_rx.recv() => {
                    if let Some(ConsensusMsg::Nack { slot: s, seen_ballot }) =
                        self.decode_verify(&sig.payload)
                        && s == *slot && seen_ballot > ballot {
                            return BallotOutcome::NackHigher(seen_ballot);
                        }
                }
                // Re-evaluate quorum when a member turns opaque mid-ballot.
                // BOUNDARY_OPAQUE means the sender is now excluded from active_members,
                // shrinking the quorum threshold. If we already have enough votes, commit.
                Some(_) = async {
                    match opaque_rx.as_mut() {
                        Some(rx) => rx.recv().await,
                        None => std::future::pending().await,
                    }
                } => {
                    if let Some(or) = opaque_recompute {
                        let opaque = (or.count_opaque)();
                        let active = or.total_members.saturating_sub(opaque).max(1);
                        *quorum_size = if or.config_quorum > 0 { or.config_quorum } else { active / 2 + 1 };
                        if let Some(res) = self.try_commit_if_ready(
                            voters, *quorum_size, group_name,
                            scope, slot, ballot, value, ballot_key, commit_key, lease_ms,
                        ).await {
                            return BallotOutcome::Committed(res);
                        }
                    }
                }
            }
        }
    }
}

/// Outcome of `ConsensusEngine::collect_one_ballot`.
enum BallotOutcome {
    /// Quorum + topology gate satisfied; commit dispatched.
    Committed(ConsensusResult),
    /// A NACK arrived with a strictly higher seen_ballot. Caller must
    /// re-issue at `>= seen_ballot + 1` to win on the next attempt.
    NackHigher(u64),
    /// `phase1_timeout` elapsed. Caller may retry or surface TopologyUnsatisfied
    /// based on whether voters reached quorum-by-count.
    Timeout,
}

// ── Wire encoding ─────────────────────────────────────────────────────────────

pub(crate) fn encode_consensus_msg(msg: &ConsensusMsg) -> Bytes {
    Bytes::from(mycelium_core::serde_fixint::to_vec(msg).unwrap_or_default())
}

pub(crate) fn decode_consensus_msg(bytes: &Bytes) -> Option<ConsensusMsg> {
    mycelium_core::serde_fixint::from_slice(bytes).ok()
}

pub(crate) fn encode_ballot(ballot: u64) -> Bytes {
    Bytes::copy_from_slice(&ballot.to_le_bytes())
}

pub(crate) fn decode_ballot(bytes: &Bytes) -> u64 {
    if bytes.len() >= 8 {
        u64::from_le_bytes(bytes[..8].try_into().unwrap_or([0u8; 8]))
    } else {
        0
    }
}

/// Wrapper used when `tls` is enabled: the raw `ConsensusMsg` bytes plus an
/// Ed25519 signature over them, so forged ballots can be detected and dropped.
/// Encoded/decoded with the same `bincode_cfg()` as `ConsensusMsg` itself.
///
/// The TLS transport already prevents unauthenticated TCP connections; this
/// adds a second layer so a compromised insider node cannot inject false
/// consensus messages into an established connection.
#[cfg(feature = "tls")]
#[derive(serde::Serialize, serde::Deserialize)]
pub(crate) struct SignedConsensusMsg {
    pub msg_bytes: Bytes,
    pub signer:    NodeId,
    pub signature: Vec<u8>,
}

// ── Voter task ───────────────────────────────────────────────────────────────

/// Background voter task — processes incoming consensus signals and emits
/// votes, nacks, and KV commit writes on behalf of this node.
///
/// Spawned by [`GossipAgent::start_consensus_listener`] via [`ConsensusEngine::spawn_listener`].
///
/// `rx_propose` / `rx_commit` are registered **synchronously by the caller**
/// (`start_consensus_listener`) before this task is spawned, so a proposal or
/// commit that arrives in the window between `start_consensus_listener`
/// returning and this task's first poll is queued rather than silently
/// dropped. Registering here instead would reintroduce that race: this node
/// would not vote on (or endorse) signals that arrive before the scheduler
/// first polls the task.
pub(crate) async fn run_consensus_listener(
    ctx:             ConsensusEngine,
    mut cancel:      oneshot::Receiver<()>,
    mut shutdown_rx: watch::Receiver<bool>,
    mut rx_propose:  mpsc::Receiver<Signal>,
    mut rx_commit:   mpsc::Receiver<Signal>,
) {
    let mut seen_ballot: AHashMap<Arc<str>, u64> = AHashMap::new();
    let mut consecutive_abstains: u32 = 0;

    loop {
        tokio::select! { biased;
            _ = &mut cancel                  => break,
            _ = shutdown_rx.wait_for(|v| *v) => break,
            Some(sig) = rx_propose.recv() => {
                // Silent abstain: overloaded node neither votes nor nacks.
                // max_abstain_ballots > 0 caps how many ballots can be skipped in a row.
                if ctx.abstain_when_opaque {
                    let at_cap = ctx.max_abstain_ballots > 0
                        && consecutive_abstains >= ctx.max_abstain_ballots;
                    if !at_cap && ctx.is_overloaded() {
                        consecutive_abstains += 1;
                        continue;
                    }
                }
                consecutive_abstains = 0;

                let Some(ConsensusMsg::Propose { slot, ballot, value: _, proposer }) =
                    ctx.decode_verify(&sig.payload)
                else { continue };

                let local = *seen_ballot.get(&slot).unwrap_or(&0);
                if ballot < local {
                    let nack = ConsensusMsg::Nack { slot, seen_ballot: local };
                    ctx.emit(
                        Arc::from(consensus_kind::NACK),
                        SignalScope::Individual(proposer),
                        ctx.sign_payload(encode_consensus_msg(&nack)),
                    );
                } else {
                    seen_ballot.insert(Arc::clone(&slot), ballot);
                    ctx.kv_set(
                        format!("{}{}", consensus_ns::BALLOT, &*slot),
                        encode_ballot(ballot),
                    );
                    // Always emit VoteWithLocality (carrying None when locality is
                    // unspecified). Topology gates only count voters that arrived
                    // via this variant — Soft policies and gate-less proposals
                    // count every voter regardless.
                    let vote = ConsensusMsg::VoteWithLocality {
                        slot:     Arc::clone(&slot),
                        ballot,
                        voter:    ctx.task_ctx.node_id.clone(),
                        locality: ctx.self_locality.clone(),
                    };
                    ctx.emit(
                        Arc::from(consensus_kind::VOTE),
                        sig.scope,
                        ctx.sign_payload(encode_consensus_msg(&vote)),
                    );
                }
            }
            Some(sig) = rx_commit.recv() => {
                let Some(ConsensusMsg::Commit { slot, ballot, value }) =
                    ctx.decode_verify(&sig.payload)
                else { continue };

                // ── Commit-conflict tripwire ─────────────────────────────────
                // Slots are commit-once (or renewed with the same value while an
                // epoch lease is live; any value once the lease has expired —
                // `live_committed` returns None for an expired lease, so a legal
                // reopen never trips this). A COMMIT carrying a *different* value
                // while the existing commitment is still live is a protocol
                // violation: a raced double-commit, a buggy proposer, or a forged
                // message. Refuse to endorse it — re-writing it here would
                // propagate the clobber with a fresh HLC from this node, actively
                // helping it win LWW everywhere. Detection only: the substrate's
                // own forwarding of the foreign frame is untouched (Layer I/II
                // stay ignorant of Layer III's laws).
                if let Some(existing) = ctx.live_committed(&slot)
                    && existing != value {
                        ctx.task_ctx.commit_conflicts.fetch_add(
                            1, std::sync::atomic::Ordering::Relaxed,
                        );
                        // Legible-Emergence Phase 2: record the "hot slot". Retry-safe compute —
                        // recompute the count from `existing` on papaya CAS retry.
                        ctx.task_ctx.commit_conflict_slots.pin().compute(
                            std::sync::Arc::clone(&slot),
                            |existing| {
                                let n = existing.map(|(_, v)| *v).unwrap_or(0) + 1;
                                papaya::Operation::<u64, ()>::Insert(n)
                            },
                        );
                        // Phase 3 explain: record the event (gated — the ring only records when
                        // the diagnostics feature is on; RT4 zero-overhead-off).
                        if ctx.task_ctx.config.emergent_detectors_enabled {
                            crate::agent::emergent::record_event(
                                &ctx.task_ctx, "commit_conflict",
                                format!("conflicting COMMIT for live slot {slot} at ballot {ballot}"),
                            );
                        }
                        tracing::warn!(
                            slot = %slot, ballot,
                            "commit conflict: COMMIT carries a different value for a \
                             live committed slot; not endorsing \
                             (see SystemStats::commit_conflicts)"
                        );
                        continue;
                    }

                let current = *seen_ballot.get(&slot).unwrap_or(&0);
                if ballot >= current {
                    // Remove instead of insert: once a slot is committed it cannot
                    // receive a valid higher ballot, so we don't need to track it.
                    seen_ballot.remove(&slot);
                }
                ctx.kv_set(
                    format!("{}{}", consensus_ns::COMMITTED, &*slot),
                    value,
                );
                ctx.kv_delete(&format!("{}{}", consensus_ns::BALLOT, &*slot));
            }
        }
    }
}

#[cfg(test)]
mod topology_tests {
    use super::*;
    use crate::locality::LocalityPath;

    fn nid(port: u16) -> NodeId {
        NodeId::new("127.0.0.1", port).expect("valid loopback NodeId")
    }

    fn loc(segs: &[&str]) -> LocalityPath {
        LocalityPath::new(segs.iter().copied())
    }

    #[test]
    fn distinct_domains_counts_unique_segments() {
        let mut voters: AHashMap<NodeId, Option<LocalityPath>> = AHashMap::new();
        voters.insert(nid(1), Some(loc(&["eu", "az1"])));
        voters.insert(nid(2), Some(loc(&["eu", "az2"])));
        voters.insert(nid(3), Some(loc(&["eu", "az1"])));
        // depth 0 (region): all "eu" → 1 domain
        assert_eq!(distinct_domains_at_depth(&voters, 0), 1);
        // depth 1 (az): "az1" + "az2" → 2 domains
        assert_eq!(distinct_domains_at_depth(&voters, 1), 2);
        // depth 2 (out of bounds for all): 0 domains
        assert_eq!(distinct_domains_at_depth(&voters, 2), 0);
    }

    #[test]
    fn distinct_domains_ignores_unknown_locality() {
        let mut voters: AHashMap<NodeId, Option<LocalityPath>> = AHashMap::new();
        voters.insert(nid(1), Some(loc(&["eu"])));
        voters.insert(nid(2), None); // legacy Vote, no locality
        voters.insert(nid(3), Some(loc(&["us"])));
        assert_eq!(distinct_domains_at_depth(&voters, 0), 2);
    }

    #[test]
    fn evaluate_gate_soft_always_passes() {
        let voters: AHashMap<NodeId, Option<LocalityPath>> = AHashMap::new();
        let policy = GroupTopologyPolicy {
            prefer_shared_depth: 0,
            spread_depth:        Some(1),
            spread_min_distinct: 3,
            enforcement:         TopologyEnforcement::Soft,
        };
        let (passes, _) = evaluate_topology_gate(&voters, &policy);
        assert!(passes, "Soft enforcement must never block");
    }

    #[test]
    fn evaluate_gate_hard_requires_distinct_domains() {
        let mut voters: AHashMap<NodeId, Option<LocalityPath>> = AHashMap::new();
        voters.insert(nid(1), Some(loc(&["eu", "az1"])));
        voters.insert(nid(2), Some(loc(&["eu", "az1"])));
        let policy = GroupTopologyPolicy {
            prefer_shared_depth: 0,
            spread_depth:        Some(1),
            spread_min_distinct: 2,
            enforcement:         TopologyEnforcement::Hard,
        };
        let (passes, distinct) = evaluate_topology_gate(&voters, &policy);
        assert!(!passes, "two voters in the same AZ must fail spread_min_distinct=2");
        assert_eq!(distinct, 1);

        voters.insert(nid(3), Some(loc(&["eu", "az2"])));
        let (passes, distinct) = evaluate_topology_gate(&voters, &policy);
        assert!(passes, "adding a voter in a different AZ should satisfy the gate");
        assert_eq!(distinct, 2);
    }

    #[test]
    fn evaluate_gate_hard_missing_spread_depth_passes() {
        // Hard with spread_depth = None should never be constructable via
        // GossipConfig::validate, but if it somehow appears at runtime (hot-reload
        // bug), the gate falls through to pass rather than stall production.
        let voters: AHashMap<NodeId, Option<LocalityPath>> = AHashMap::new();
        let policy = GroupTopologyPolicy {
            prefer_shared_depth: 0,
            spread_depth:        None,
            spread_min_distinct: 2,
            enforcement:         TopologyEnforcement::Hard,
        };
        let (passes, _) = evaluate_topology_gate(&voters, &policy);
        assert!(passes);
    }

    #[test]
    fn evaluate_gate_hard_legacy_vote_doesnt_contribute_diversity() {
        // A voter with locality: None (sent via legacy ConsensusMsg::Vote on a
        // mixed-version cluster) cannot contribute to the diversity count.
        let mut voters: AHashMap<NodeId, Option<LocalityPath>> = AHashMap::new();
        voters.insert(nid(1), Some(loc(&["eu"])));
        voters.insert(nid(2), None);
        voters.insert(nid(3), None);
        let policy = GroupTopologyPolicy {
            prefer_shared_depth: 0,
            spread_depth:        Some(0),
            spread_min_distinct: 2,
            enforcement:         TopologyEnforcement::Hard,
        };
        let (passes, distinct) = evaluate_topology_gate(&voters, &policy);
        assert!(!passes);
        assert_eq!(distinct, 1, "only the EU voter contributes diversity; the two legacy votes do not");
    }
}

#[cfg(test)]
mod lease_tests {
    use super::*;
    use crate::store::{KvState, StoreEntry};

    fn put(kv: &KvState, key: &str, value: &[u8], ts_ms: u64) {
        kv.store.pin().insert(
            Arc::from(key),
            StoreEntry {
                data:      Some(Bytes::copy_from_slice(value)),
                timestamp: crate::hlc::pack(ts_ms, 0),
            },
        );
    }

    #[test]
    fn no_lease_entry_means_permanent() {
        let kv = KvState::new(1024);
        put(&kv, "consensus/committed/cfg", b"v1", 1_000);
        // Arbitrarily far in the future — still live without a lease entry.
        assert_eq!(
            live_committed_value(&kv, "cfg", u64::MAX / 2).as_deref(),
            Some(b"v1".as_slice()),
        );
    }

    #[test]
    fn lease_fresh_then_expired() {
        let kv = KvState::new(1024);
        put(&kv, "consensus/committed/cfg", b"v1", 1_000_000);
        put(&kv, "consensus/lease/cfg", &5_000u64.to_le_bytes(), 1_000_000);
        // Inside the 5 s window.
        assert!(live_committed_value(&kv, "cfg", 1_004_999).is_some());
        // Exactly at the boundary — still fresh (<=).
        assert!(live_committed_value(&kv, "cfg", 1_005_000).is_some());
        // One ms past the window — the slot has reopened.
        assert!(live_committed_value(&kv, "cfg", 1_005_001).is_none());
    }

    #[test]
    fn malformed_lease_is_treated_as_permanent() {
        // Never silently expire a commitment because a lease entry was corrupted.
        let kv = KvState::new(1024);
        put(&kv, "consensus/committed/cfg", b"v1", 1_000);
        put(&kv, "consensus/lease/cfg", b"xyz", 1_000); // < 8 bytes
        assert!(live_committed_value(&kv, "cfg", u64::MAX / 2).is_some());
    }

    #[test]
    fn tombstoned_lease_is_treated_as_permanent() {
        let kv = KvState::new(1024);
        put(&kv, "consensus/committed/cfg", b"v1", 1_000);
        kv.store.pin().insert(
            Arc::from("consensus/lease/cfg"),
            StoreEntry { data: None, timestamp: crate::hlc::pack(2_000, 0) },
        );
        assert!(live_committed_value(&kv, "cfg", u64::MAX / 2).is_some());
    }

    #[test]
    fn tombstoned_commit_reads_as_absent() {
        // LockGuard release tombstones the committed slot — must read as reopened.
        let kv = KvState::new(1024);
        kv.store.pin().insert(
            Arc::from("consensus/committed/lock/x"),
            StoreEntry { data: None, timestamp: crate::hlc::pack(1_000, 0) },
        );
        assert!(live_committed_value(&kv, "lock/x", 2_000).is_none());
    }

    #[test]
    fn decode_lease_roundtrip_and_malformed() {
        assert_eq!(decode_lease_ms(&encode_lease_ms(86_400_000)), Some(86_400_000));
        assert_eq!(decode_lease_ms(&Bytes::from_static(b"short")), None);
        assert_eq!(decode_lease_ms(&Bytes::new()), None);
    }
}

/// Distinct accepts a group of `n` members needs at fraction `frac` for a **safe
/// (intersecting)** quorum. `floor(n·frac)+1`, floored at strict majority so a small
/// fraction can never yield two disjoint quorums (which would split-brain the slot).
/// At `frac == 0.5` this is exactly `n/2 + 1` — matching the main path's
/// `compute_quorum_size`; the old `ceil(n·frac)` under-counted on even n (`ceil(4·0.5)=2`
/// allowed disjoint `{A,B}`/`{C,D}` — audit 2026-07-15).
pub(crate) fn cross_group_quorum(n: usize, frac: f32) -> usize {
    (((n as f32) * frac).floor() as usize + 1)
        .max(n / 2 + 1)
        .min(n.max(1))
}

#[cfg(test)]
mod cross_group_tests {
    use super::GroupQuorum;

    // Helper: mirrors the quorum check in ConsensusEngine::cross_propose (shared code).
    fn quorum_met(accepts: usize, member_count: usize, frac: f32) -> bool {
        accepts >= super::cross_group_quorum(member_count, frac)
    }

    /// Regression (audit 2026-07-15): a fraction-0.5 quorum on EVEN n must exceed n/2 so any
    /// two quorums intersect — the old `ceil(n·0.5)` allowed disjoint majorities to both commit.
    #[test]
    fn regression_even_n_quorum_intersects() {
        assert!(!quorum_met(2, 4, 0.5), "2 of 4 is a non-intersecting quorum (split-brain)");
        assert!( quorum_met(3, 4, 0.5), "3 of 4 is strict majority");
        assert!(!quorum_met(1, 2, 0.5), "1 of 2 is a minority");
        assert!( quorum_met(2, 2, 0.5));
        // exactly matches the main path (compute_quorum_size) at 0.5, every n:
        for n in 1..=12 { assert_eq!(super::cross_group_quorum(n, 0.5), n / 2 + 1, "n={n}"); }
        // supermajority fractions stay at or above strict majority (still safe):
        assert!(super::cross_group_quorum(6, 0.67) > 6 / 2);
    }

    #[test]
    fn strict_majority_requires_ceil_half_plus_one() {
        // 5-member group at quorum=0.5 → ceil(2.5) = 3 required
        assert!(!quorum_met(2, 5, 0.5), "2/5 must not satisfy majority");
        assert!( quorum_met(3, 5, 0.5), "3/5 must satisfy majority");
        assert!( quorum_met(5, 5, 0.5), "5/5 trivially satisfies");
    }

    #[test]
    fn unanimous_requires_all_members() {
        assert!(!quorum_met(4, 5, 1.0), "4/5 must not satisfy unanimous");
        assert!( quorum_met(5, 5, 1.0), "5/5 must satisfy unanimous");
    }

    #[test]
    fn single_member_group_always_needs_one_accept() {
        // ceil(1 * 0.5) = 1, not 0
        assert!(!quorum_met(0, 1, 0.5), "0 accepts in a 1-member group must fail");
        assert!( quorum_met(1, 1, 0.5));
    }

    #[test]
    fn empty_group_clamps_to_one_required() {
        // member_count=0 → .max(1) ensures needed=1, not 0 (no free commit)
        assert!(!quorum_met(0, 0, 0.5));
    }

    #[test]
    fn all_groups_must_individually_reach_quorum() {
        let group_a_ok = quorum_met(3, 5, 0.5); // ceil(2.5)=3 → passes
        let group_b_ok = quorum_met(2, 5, 0.5); // ceil(2.5)=3 → fails
        assert!( group_a_ok);
        assert!(!group_b_ok);
        assert!(!(group_a_ok && group_b_ok), "overall must fail when any group misses quorum");
    }

    #[test]
    fn group_quorum_struct_fields() {
        let gq = GroupQuorum { group: "compliance".into(), quorum: 0.75, veto: true };
        assert_eq!(gq.group, "compliance");
        assert!((gq.quorum - 0.75).abs() < f32::EPSILON);
        assert!(gq.veto);
    }
}
