//! The **access broker** (Phase 2 remainder) — the curator's membership gate on store access.
//!
//! A group agent learns *where* to read and *whether it may* by a **one-time** RPC handshake with the
//! curator ([`Wiki::request_store_access`](crate::Wiki::request_store_access)); after that it opens the
//! store and reads **directly**, forever. So the broker is **not on the read path** — the data plane's
//! node-independence (parallel direct reads, no curator) is preserved. The broker is the initial
//! *"where is the store, and am I allowed?"* step.
//!
//! Why RPC (not KV): a grant — and, for a real object store, the scoped **credential** it would carry —
//! must go point-to-point to the requester, not flood the whole cluster as gossiped KV would. The
//! requester's identity is [`RpcRequest::sender`](mycelium::RpcRequest::sender), authenticated by the
//! transport (mTLS), so the curator gates on a trustworthy node id.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

/// The curator's membership gate. `Open` grants any requester (the default — the pre-broker behaviour);
/// `Allowlist` grants only the listed node-ids (the hard gate). Set on the [`CuratorBrain`].
///
/// [`CuratorBrain`]: crate::CuratorBrain
#[derive(Debug, Clone, Default)]
pub enum Membership {
    /// Any requester is granted store access.
    #[default]
    Open,
    /// Only requesters whose node-id (`NodeId::to_string`) is in the set are granted.
    Allowlist(BTreeSet<String>),
}

impl Membership {
    /// An allowlist from an iterator of node-id strings.
    pub fn allow(ids: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self::Allowlist(ids.into_iter().map(Into::into).collect())
    }
    /// Does this policy grant `node_id`?
    pub(crate) fn permits(&self, node_id: &str) -> bool {
        match self {
            Membership::Open => true,
            Membership::Allowlist(set) => set.contains(node_id),
        }
    }
}

/// A granted store-access token from the curator: **where** to read. A real object-store adapter would
/// also carry a scoped credential + expiry here; for a shared-filesystem [`FsStore`](crate::FsStore)
/// the location *is* the access, so the grant is the location plus the authorization decision.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoreGrant {
    pub group:    String,
    pub location: String,
}

/// The curator's RPC reply to an access request (wire form).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct AccessReply {
    pub granted:  bool,
    pub location: Option<String>,
}

/// Why an access request did not yield a grant.
#[derive(Debug)]
pub enum AccessError {
    /// No curator is currently elected — retry once failover settles.
    NoCurator,
    /// The curator's membership gate denied this node.
    Denied,
    /// The RPC to the curator failed (timeout / transport).
    Rpc(String),
    /// The curator's reply could not be decoded.
    Decode(String),
}

impl std::fmt::Display for AccessError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AccessError::NoCurator   => write!(f, "wiki access: no curator elected"),
            AccessError::Denied      => write!(f, "wiki access: denied by the curator's membership gate"),
            AccessError::Rpc(e)      => write!(f, "wiki access: rpc to curator failed: {e}"),
            AccessError::Decode(e)   => write!(f, "wiki access: undecodable grant: {e}"),
        }
    }
}
impl std::error::Error for AccessError {}
