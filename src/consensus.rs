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

use crate::node_id::NodeId;
use bytes::Bytes;
use std::{sync::Arc, time::Duration};
use tokio::sync::oneshot;

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
}

impl Default for ConsensusConfig {
    fn default() -> Self {
        Self {
            quorum_size:             0,
            phase1_timeout:          Duration::from_secs(5),
            max_ballots:             3,
            ballot_retry_jitter_ms:  50,
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
