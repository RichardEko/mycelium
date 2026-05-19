//! Consensus — Layer 2 extension.
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
use crate::framing::{
    bincode_cfg, dispatch_gossip_send, dispatch_gossip_try_send, ForwardHint, WireMessage,
};
use crate::node_id::NodeId;
use crate::signal::{signal_kind, SignalScope};
use crate::store::apply_and_notify;
use ahash::{AHashMap, AHashSet};
use bytes::{BufMut, Bytes, BytesMut};
use std::{
    sync::Arc,
    time::Duration,
};
use tokio::{
    sync::{oneshot, watch},
    task::JoinHandle,
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
        }
    }
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
}

/// Cancels the consensus listener task on drop.
///
/// Obtain from [`GossipAgent::start_consensus_listener`].
/// The task also exits when the agent shuts down even if this handle is live.
pub struct ConsensusHandle {
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
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Closure type injected by `group_propose` or `system_propose` for mid-ballot
/// opaque-member recomputation.
///
/// `(total_member_count, config_quorum_size, count_opaque_fn)`.
/// `count_opaque_fn()` returns the current number of opaque members. Built at
/// the Layer II call site so `propose` does not read `KvState` directly.
type OpaqueRecompute = Option<(usize, usize, Arc<dyn Fn() -> usize + Send + Sync>)>;

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
}

impl ConsensusEngine {
    // ── KV helpers ───────────────────────────────────────────────────────────

    fn get(&self, key: &str) -> Option<Bytes> {
        self.task_ctx.kv_state.store.pin().get(key).and_then(|e| e.data.clone())
    }

    fn read_ballot(&self, ballot_key: &str) -> u64 {
        self.get(ballot_key).map(|b| decode_ballot(&b)).unwrap_or(0)
    }

    /// Applies a KV update from within a consensus task.
    /// Uses `try_send` for gossip dispatch — dropped frames recovered via anti-entropy.
    fn kv_set(&self, key: String, value: Bytes) {
        let tc = &self.task_ctx;
        let upd = make_gossip_update(&tc.node_id, tc.default_ttl, Arc::from(key.as_str()), value, false);
        apply_and_notify(&tc.kv_state, &upd);
        dispatch_gossip_try_send(
            &tc.gossip_txs, WireMessage::Data(upd),
            tc.node_id.id_hash(), ForwardHint::All, &tc.kv_state.dropped_frames,
        );
    }

    /// Tombstones `key` in the KV store and gossips the deletion.
    /// Used to clean up ballot entries once a slot has committed.
    fn kv_delete(&self, key: &str) {
        let tc = &self.task_ctx;
        let upd = make_gossip_update(&tc.node_id, tc.default_ttl, Arc::from(key), Bytes::new(), true);
        apply_and_notify(&tc.kv_state, &upd);
        dispatch_gossip_try_send(
            &tc.gossip_txs, WireMessage::Data(upd),
            tc.node_id.id_hash(), ForwardHint::All, &tc.kv_state.dropped_frames,
        );
    }

    /// Like `kv_set` but awaits channel capacity (used by the proposer).
    async fn set_async(&self, key: &str, value: Bytes) {
        let tc = &self.task_ctx;
        let upd = make_gossip_update(&tc.node_id, tc.default_ttl, Arc::from(key), value, false);
        apply_and_notify(&tc.kv_state, &upd);
        dispatch_gossip_send(
            &tc.gossip_txs, WireMessage::Data(upd),
            tc.node_id.id_hash(), ForwardHint::All,
        ).await;
    }

    // ── Layer II bridge helpers ──────────────────────────────────────────────

    /// True if any `sys/load/{node_id}/*` pheromone entry is `is_opaque`.
    ///
    /// Delegates to the Layer II helper in `agent::opacity` so this Layer III type
    /// does not scan `KvState` directly.
    fn is_overloaded(&self) -> bool {
        crate::agent::is_self_opaque(&self.task_ctx.kv_state, &self.task_ctx.node_id)
    }

    fn emit(&self, kind: Arc<str>, scope: SignalScope, payload: Bytes) {
        emit_signal(&self.task_ctx, kind, scope, payload);
    }

    async fn emit_async(&self, kind: Arc<str>, scope: SignalScope, payload: Bytes) -> bool {
        emit_signal_async(&self.task_ctx, kind, scope, payload).await
    }

    // ── Proposer ─────────────────────────────────────────────────────────────

    /// Runs one full proposal attempt sequence for `slot`.
    ///
    /// Called by `GossipAgent::group_propose` and `GossipAgent::system_propose`.
    ///
    /// `opaque_recompute` — when `Some((member_ids, freshness, config_quorum_size, count_opaque))`,
    /// `propose` registers for `BOUNDARY_OPAQUE` signals and re-evaluates the effective quorum
    /// size mid-ballot when any member transitions. `count_opaque` is a callback built by the
    /// Layer II call site (`group_propose`) so `propose` does not read `KvState` directly —
    /// the opacity query strategy is an injected dependency, not a Layer III concern.
    pub(crate) async fn propose(
        &self,
        scope:             SignalScope,
        slot:              Arc<str>,
        value:             Bytes,
        quorum_size:       usize,
        config:            ConsensusConfig,
        opaque_recompute:  OpaqueRecompute,
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
                    bincode::serde::decode_from_slice::<Vec<NodeId>, _>(&b, bincode_cfg()).ok()
                }).map(|(peers, _)| peers.iter().map(|p| p.id_hash()).collect())
            } else {
                None
            }
        } else {
            None
        };

        let mut ballot = self.read_ballot(&ballot_key) + 1;
        let mut votes_last_ballot: usize = 0;

        for _attempt in 0..config.max_ballots {
            if self.get(&commit_key).is_some() {
                return ConsensusResult::Superseded {
                    slot,
                    ballot: self.read_ballot(&ballot_key),
                };
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
                slot: slot.clone(), ballot, value: value.clone(),
                proposer: self.task_ctx.node_id.clone(),
            };
            self.emit_async(
                Arc::from(consensus_kind::PROPOSE), scope.clone(), encode_consensus_msg(&propose_msg),
            ).await;

            // Local dedup per (slot, ballot). Cannot use signal_handlers.quorum_for_group
            // here: the sender_log records (sender, received_at) without slot/ballot
            // correlation, so it would conflate votes from different rounds.
            //
            // Membership changes mid-ballot are intentionally ignored. The group member
            // set is captured once before the ballot loop (in group_propose) and the quorum
            // threshold is fixed for that ballot's lifetime. A joining member's votes do not
            // count toward this ballot; a leaving member's existing vote remains counted.
            // This is a known non-goal: supporting live membership changes mid-ballot would
            // require distributed coordination that exceeds the protocol's scope.
            let mut voters: AHashSet<u64> = AHashSet::new();
            voters.insert(self.task_ctx.node_id.id_hash());
            if voters.len() >= quorum_size {
                let commit = ConsensusMsg::Commit {
                    slot: slot.clone(), ballot, value: value.clone(),
                };
                self.emit_async(
                    Arc::from(consensus_kind::COMMIT), scope.clone(), encode_consensus_msg(&commit),
                ).await;
                self.set_async(commit_key.as_str(), value.clone()).await;
                self.kv_delete(&ballot_key);
                return ConsensusResult::Committed { slot, value, ballot };
            }

            let sleep = time::sleep_until(time::Instant::now() + config.phase1_timeout);
            tokio::pin!(sleep);
            let mut nack_ballot = 0u64;

            'collect: loop {
                tokio::select! { biased;
                    _ = &mut sleep => break 'collect,
                    Some(sig) = vote_rx.recv() => {
                        if let Some(ConsensusMsg::Vote { slot: s, ballot: b, voter }) =
                            decode_consensus_msg(&sig.payload)
                        {
                            if s == slot && b == ballot {
                                // Trust-slice filtering: only count votes from declared peers.
                                if let Some(ref ts) = trust_set {
                                    if !ts.contains(&voter.id_hash()) { continue; }
                                }
                                voters.insert(voter.id_hash());
                                if voters.len() >= quorum_size {
                                    let commit = ConsensusMsg::Commit {
                                        slot: slot.clone(), ballot, value: value.clone(),
                                    };
                                    self.emit_async(
                                        Arc::from(consensus_kind::COMMIT), scope.clone(), encode_consensus_msg(&commit),
                                    ).await;
                                    self.set_async(commit_key.as_str(), value.clone()).await;
                                    self.kv_delete(&ballot_key);
                                    return ConsensusResult::Committed { slot, value, ballot };
                                }
                            }
                        }
                    }
                    Some(sig) = nack_rx.recv() => {
                        if let Some(ConsensusMsg::Nack { slot: s, seen_ballot }) =
                            decode_consensus_msg(&sig.payload)
                        {
                            if s == slot && seen_ballot > ballot {
                                nack_ballot = seen_ballot;
                                break 'collect;
                            }
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
                        if let Some((total_members, config_qs, ref count_opaque)) = opaque_recompute {
                            let opaque = count_opaque();
                            let active = total_members.saturating_sub(opaque).max(1);
                            quorum_size = if config_qs > 0 { config_qs } else { active / 2 + 1 };
                            // Check immediately — we may already have enough votes.
                            if voters.len() >= quorum_size {
                                let commit = ConsensusMsg::Commit {
                                    slot: slot.clone(), ballot, value: value.clone(),
                                };
                                self.emit_async(
                                    Arc::from(consensus_kind::COMMIT), scope.clone(),
                                    encode_consensus_msg(&commit),
                                ).await;
                                self.set_async(commit_key.as_str(), value.clone()).await;
                                self.kv_delete(&ballot_key);
                                return ConsensusResult::Committed { slot, value, ballot };
                            }
                        }
                    }
                }
            }

            if self.get(&commit_key).is_some() {
                return ConsensusResult::Superseded {
                    slot,
                    ballot: self.read_ballot(&ballot_key),
                };
            }

            votes_last_ballot = voters.len();

            if config.ballot_retry_jitter_ms > 0 {
                let jitter = fastrand::u64(0..config.ballot_retry_jitter_ms);
                tokio::time::sleep(Duration::from_millis(jitter)).await;
            }
            ballot = nack_ballot.max(self.read_ballot(&ballot_key)).max(ballot) + 1;
        }

        ConsensusResult::Timeout {
            slot,
            ballots_tried: config.max_ballots,
            votes_last_ballot,
            quorum_required: quorum_size,
        }
    }

    // ── Listener ─────────────────────────────────────────────────────────────

    /// Spawns the voter/listener task.
    ///
    /// Called by `GossipAgent::start_consensus_listener`. Consumes `self`.
    pub(crate) fn spawn_listener(
        self,
        cancel_rx:   oneshot::Receiver<()>,
        shutdown_rx: watch::Receiver<bool>,
    ) -> JoinHandle<()> {
        tokio::spawn(run_consensus_listener(self, cancel_rx, shutdown_rx))
    }
}

// ── Wire encoding ─────────────────────────────────────────────────────────────

pub(crate) fn encode_consensus_msg(msg: &ConsensusMsg) -> Bytes {
    let mut buf = BytesMut::new();
    let _ = bincode::serde::encode_into_std_write(msg, &mut (&mut buf).writer(), bincode_cfg());
    buf.freeze()
}

pub(crate) fn decode_consensus_msg(bytes: &Bytes) -> Option<ConsensusMsg> {
    bincode::serde::decode_from_slice(bytes, bincode_cfg())
        .ok()
        .map(|(v, _)| v)
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

// ── Voter task ───────────────────────────────────────────────────────────────

/// Background voter task — processes incoming consensus signals and emits
/// votes, nacks, and KV commit writes on behalf of this node.
///
/// Spawned by [`GossipAgent::start_consensus_listener`] via [`ConsensusEngine::spawn_listener`].
async fn run_consensus_listener(
    ctx:             ConsensusEngine,
    mut cancel:      oneshot::Receiver<()>,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    let mut rx_propose = ctx.task_ctx.signal_handlers.register_with_capacity(
        Arc::from(consensus_kind::PROPOSE), 512,
    );
    let mut rx_commit = ctx.task_ctx.signal_handlers.register_with_capacity(
        Arc::from(consensus_kind::COMMIT), 256,
    );
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
                    decode_consensus_msg(&sig.payload)
                else { continue };

                let local = *seen_ballot.get(&slot).unwrap_or(&0);
                if ballot < local {
                    let nack = ConsensusMsg::Nack { slot, seen_ballot: local };
                    ctx.emit(
                        Arc::from(consensus_kind::NACK),
                        SignalScope::Individual(proposer),
                        encode_consensus_msg(&nack),
                    );
                } else {
                    seen_ballot.insert(slot.clone(), ballot);
                    ctx.kv_set(
                        format!("{}{}", consensus_ns::BALLOT, &*slot),
                        encode_ballot(ballot),
                    );
                    let vote = ConsensusMsg::Vote {
                        slot: slot.clone(), ballot, voter: ctx.task_ctx.node_id.clone(),
                    };
                    ctx.emit(
                        Arc::from(consensus_kind::VOTE),
                        sig.scope,
                        encode_consensus_msg(&vote),
                    );
                }
            }
            Some(sig) = rx_commit.recv() => {
                let Some(ConsensusMsg::Commit { slot, ballot, value }) =
                    decode_consensus_msg(&sig.payload)
                else { continue };

                let current = *seen_ballot.get(&slot).unwrap_or(&0);
                if ballot >= current {
                    seen_ballot.insert(slot.clone(), ballot);
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
