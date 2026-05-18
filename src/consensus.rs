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

use crate::framing::{
    bincode_cfg, dispatch_gossip_send, dispatch_gossip_try_send, ForwardHint, GossipUpdate,
    WireMessage,
};
use crate::node_id::NodeId;
use crate::seen::ShardedSeen;
use crate::signal::{decode_load_state, kv_ns, Signal, SignalHandlers, SignalScope, Boundary};
use crate::store::{apply_and_notify, StoreEntry};
use ahash::{AHashMap, AHashSet};
use bytes::{BufMut, Bytes, BytesMut};
use parking_lot::RwLock;
use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    sync::{mpsc, oneshot, watch},
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

// ── ConsensusEngine ──────────────────────────────────────────────────────────
//
// Shared context for both the voter/listener task and the proposer.
// Constructed by GossipAgent::start_consensus_listener and
// GossipAgent::group_propose / system_propose, then either spawned
// (spawn_listener) or driven directly (propose).

/// Bundles the Arc fields needed for consensus tasks.
///
/// Replaces the former `ConsensusListenerCtx` that was private to `agent.rs`.
pub(crate) struct ConsensusEngine {
    pub(crate) node_id:             NodeId,
    pub(crate) seen:                Arc<ShardedSeen>,
    pub(crate) current_ts:          Arc<AtomicU64>,
    pub(crate) signal_boundary:     Arc<RwLock<Boundary>>,
    pub(crate) signal_handlers:     Arc<SignalHandlers>,
    pub(crate) gossip_txs:          Arc<[mpsc::Sender<(Bytes, u64, ForwardHint)>]>,
    pub(crate) default_ttl:         u8,
    pub(crate) dropped_frames:      Arc<AtomicU64>,
    pub(crate) store:               Arc<papaya::HashMap<Arc<str>, StoreEntry>>,
    pub(crate) subscriptions:       Arc<papaya::HashMap<Arc<str>, watch::Sender<Option<Bytes>>>>,
    /// When `true`, this node silently abstains from voting while any pheromone
    /// trail under `load/{node_id}/` shows `is_opaque: true`.
    pub(crate) abstain_when_opaque: bool,
    /// When `true`, the proposer filters incoming votes against its declared
    /// trust slice for the group (`consensus/trust/{group}/{node_id}`).
    pub(crate) use_trust_slices: bool,
    pub(crate) max_store_entries:   usize,
    pub(crate) max_abstain_ballots: u32,
}

impl ConsensusEngine {
    // ── KV helpers ───────────────────────────────────────────────────────────

    fn get(&self, key: &str) -> Option<Bytes> {
        self.store.pin().get(key).and_then(|e| e.data.clone())
    }

    fn read_ballot(&self, ballot_key: &str) -> u64 {
        self.get(ballot_key).map(|b| decode_ballot(&b)).unwrap_or(0)
    }

    fn make_kv_update(&self, key: Arc<str>, value: Bytes) -> GossipUpdate {
        GossipUpdate {
            nonce:        fastrand::u64(1..),
            sender:       self.node_id.id_hash(),
            ttl:          self.default_ttl,
            is_tombstone: false,
            timestamp:    SystemTime::now()
                .duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64,
            key,
            value,
        }
    }

    /// Applies a KV update from within a consensus task.
    /// Uses `try_send` for gossip dispatch — dropped frames recovered via anti-entropy.
    fn kv_set(&self, key: String, value: Bytes) {
        let upd = self.make_kv_update(Arc::from(key.as_str()), value);
        apply_and_notify(&self.store, &self.subscriptions, &upd, self.max_store_entries);
        dispatch_gossip_try_send(
            &self.gossip_txs, WireMessage::Data(upd),
            self.node_id.id_hash(), ForwardHint::All, &self.dropped_frames,
        );
    }

    /// Like `kv_set` but awaits channel capacity (used by the proposer).
    async fn set_async(&self, key: &str, value: Bytes) {
        let upd = self.make_kv_update(Arc::from(key), value);
        apply_and_notify(&self.store, &self.subscriptions, &upd, self.max_store_entries);
        dispatch_gossip_send(
            &self.gossip_txs, WireMessage::Data(upd),
            self.node_id.id_hash(), ForwardHint::All,
        ).await;
    }

    // ── Signal helpers ───────────────────────────────────────────────────────

    /// Emits a signal; uses `try_send` (non-blocking, for voter tasks).
    fn emit_sync(&self, kind: Arc<str>, scope: SignalScope, payload: Bytes) {
        let nonce = fastrand::u64(1..);
        let ts = self.current_ts.load(Ordering::Relaxed);
        let _ = self.seen.is_duplicate(nonce, ts);
        if self.signal_boundary.read().admits(&scope) {
            let admit = match &scope {
                SignalScope::Individual(_) => true,
                _ => {
                    let opacity = self.signal_handlers.fill_ratio(&kind);
                    opacity == 0.0 || fastrand::f32() >= opacity
                }
            };
            if admit {
                self.signal_handlers.deliver(&Signal {
                    kind: kind.clone(), scope: scope.clone(),
                    payload: payload.clone(), sender: self.node_id.clone(), nonce,
                });
            }
        }
        let hint = match &scope {
            SignalScope::System           => ForwardHint::All,
            SignalScope::Group(name)      => ForwardHint::Group(name.clone()),
            SignalScope::Individual(peer) => ForwardHint::Individual(peer.clone()),
        };
        dispatch_gossip_try_send(
            &self.gossip_txs,
            WireMessage::Signal { ttl: self.default_ttl, nonce, sender: self.node_id.clone(), scope, kind, payload },
            self.node_id.id_hash(), hint, &self.dropped_frames,
        );
    }

    /// Emits a signal; awaits channel capacity (used by the proposer).
    async fn emit_async(&self, kind: &str, scope: SignalScope, payload: Bytes) {
        let kind: Arc<str> = Arc::from(kind);
        let nonce = fastrand::u64(1..);
        let ts = self.current_ts.load(Ordering::Relaxed);
        let _ = self.seen.is_duplicate(nonce, ts);
        if self.signal_boundary.read().admits(&scope) {
            let admit = match &scope {
                SignalScope::Individual(_) => true,
                _ => {
                    let opacity = self.signal_handlers.fill_ratio(&kind);
                    opacity == 0.0 || fastrand::f32() >= opacity
                }
            };
            if admit {
                self.signal_handlers.deliver(&Signal {
                    kind: kind.clone(), scope: scope.clone(),
                    payload: payload.clone(), sender: self.node_id.clone(), nonce,
                });
            }
        }
        let hint = match &scope {
            SignalScope::System           => ForwardHint::All,
            SignalScope::Group(name)      => ForwardHint::Group(name.clone()),
            SignalScope::Individual(peer) => ForwardHint::Individual(peer.clone()),
        };
        dispatch_gossip_send(
            &self.gossip_txs,
            WireMessage::Signal { ttl: self.default_ttl, nonce, sender: self.node_id.clone(), scope, kind, payload },
            self.node_id.id_hash(), hint,
        ).await;
    }

    // ── Proposer ─────────────────────────────────────────────────────────────

    /// Runs one full proposal attempt sequence for `slot`.
    ///
    /// Called by `GossipAgent::group_propose` and `GossipAgent::system_propose`.
    pub(crate) async fn propose(
        &self,
        scope:       SignalScope,
        slot:        Arc<str>,
        value:       Bytes,
        quorum_size: usize,
        config:      ConsensusConfig,
    ) -> ConsensusResult {
        let ballot_key = format!("{}{}", consensus_ns::BALLOT, &*slot);
        let commit_key = format!("{}{}", consensus_ns::COMMITTED, &*slot);

        // Build trust set once before the ballot loop — it doesn't change mid-round.
        let trust_set: Option<AHashSet<u64>> = if self.use_trust_slices {
            if let SignalScope::Group(ref group_name) = scope {
                let key = format!("{}{}/{}", consensus_ns::TRUST, group_name, self.node_id);
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
            let mut vote_rx = self.signal_handlers.register_with_capacity(
                Arc::from(consensus_kind::VOTE), 512,
            );
            let mut nack_rx = self.signal_handlers.register_with_capacity(
                Arc::from(consensus_kind::NACK), 64,
            );

            let propose_msg = ConsensusMsg::Propose {
                slot: slot.clone(), ballot, value: value.clone(),
                proposer: self.node_id.clone(),
            };
            self.emit_async(
                consensus_kind::PROPOSE, scope.clone(), encode_consensus_msg(&propose_msg),
            ).await;

            // Proposer counts its own vote.
            let mut voters: AHashSet<u64> = AHashSet::new();
            voters.insert(self.node_id.id_hash());
            if voters.len() >= quorum_size {
                let commit = ConsensusMsg::Commit {
                    slot: slot.clone(), ballot, value: value.clone(),
                };
                self.emit_async(
                    consensus_kind::COMMIT, scope.clone(), encode_consensus_msg(&commit),
                ).await;
                self.set_async(commit_key.as_str(), value.clone()).await;
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
                                        consensus_kind::COMMIT, scope.clone(),
                                        encode_consensus_msg(&commit),
                                    ).await;
                                    self.set_async(commit_key.as_str(), value.clone()).await;
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
    let mut rx_propose = ctx.signal_handlers.register_with_capacity(
        Arc::from(consensus_kind::PROPOSE), 512,
    );
    let mut rx_commit = ctx.signal_handlers.register_with_capacity(
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
                    if !at_cap {
                        let load_prefix = format!("{}{}/", kv_ns::LOAD, ctx.node_id);
                        let is_overloaded = ctx.store.pin()
                            .iter()
                            .filter(|(k, v)| k.starts_with(&*load_prefix) && v.data.is_some())
                            .any(|(_, v)| {
                                v.data.as_ref()
                                    .and_then(decode_load_state)
                                    .map(|s| s.is_opaque)
                                    .unwrap_or(false)
                            });
                        if is_overloaded {
                            consecutive_abstains += 1;
                            continue;
                        }
                    }
                }
                consecutive_abstains = 0;

                let Some(ConsensusMsg::Propose { slot, ballot, value: _, proposer }) =
                    decode_consensus_msg(&sig.payload)
                else { continue };

                let local = *seen_ballot.get(&slot).unwrap_or(&0);
                if ballot < local {
                    let nack = ConsensusMsg::Nack { slot, seen_ballot: local };
                    ctx.emit_sync(
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
                        slot: slot.clone(), ballot, voter: ctx.node_id.clone(),
                    };
                    ctx.emit_sync(
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
            }
        }
    }
}
