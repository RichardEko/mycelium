//! The agent's **peer-topology surface**: who this node is connected to, group membership,
//! and the direct-route pinning API for RPC-heavy pairs (`connect_peer`/`disconnect_peer`).
//!
//! Split out of the former `kv.rs` grab-bag (2026-07-10) — that file's name was a fossil
//! from before the KV code moved to `mycelium-core` (v2 M3); none of its methods were KV.

use std::sync::Arc;

use super::GossipAgent;

impl GossipAgent {
    /// Returns a snapshot of all currently live peer `NodeId`s.
    ///
    /// Useful at Layer 3 when a direct connection (e.g. HTTP) must be opened to
    /// a specific peer. The list reflects the peers table at the moment of the call;
    /// it may be stale by the time it is acted on — treat it as advisory.
    pub fn peers(&self) -> Vec<crate::node_id::NodeId> {
        self.peers.pin().iter().map(|(k, _)| k.clone()).collect()
    }

    /// Pin a **direct forwarding route** to `peer`.
    ///
    /// The forwarding target set de-pins non-active peers (the seed-scalability design), so an
    /// Individual-scoped RPC to a peer that isn't currently a forwarding target degrades to
    /// flood-relay — fine for one-shot signals, but too slow for a request-response RPC, which
    /// then times out. A relationship that is *RPC-heavy* toward a specific peer (e.g. a
    /// tuple-space secondary calling its primary) should pin that peer so its RPCs keep a direct
    /// route. Idempotent; the pin is honoured on every forwarding-target rebuild until
    /// [`disconnect_peer`](Self::disconnect_peer). Beyond pinning, this *actively warms* the link
    /// — writers connect lazily on first frame, so it spawns the writer and sends a Ping now,
    /// establishing the connection ahead of the first RPC rather than on its deadline. Call it a
    /// little ahead of the RPC (e.g. from a background keeper), not inline on the hot path, for
    /// the warm to win the race. See #150.
    pub fn connect_peer(&self, peer: crate::node_id::NodeId) {
        self.pinned_peers.pin().insert(peer.clone(), ());
        // Actively warm the direct link. Writers connect *lazily* on their first frame
        // (`mycelium_core::writer::run_peer_writer`), so an RPC to a freshly-pinned peer would
        // otherwise pay TCP(+TLS) setup on its own critical path and can miss its deadline — the
        // S13 cold-start miss (#150). Spawn the writer now and push a Ping so the connection is
        // established ahead of the first real RPC; on an already-live link this is just a cheap
        // keepalive that resets the writer's idle deadline. Best-effort — a full channel or
        // shutdown simply skips the warm (the pin still stands for the next rebuild).
        if let Some(tx) = mycelium_core::writer::get_or_spawn_writer(
            &peer,
            &self.peer_writers,
            self.task_ctx.hot.writer_depth(),
            std::time::Duration::from_secs(self.task_ctx.hot.reconnect_backoff_secs(self.config.reconnect_backoff_secs)),
            std::time::Duration::from_secs(self.config.writer_idle_timeout_secs),
            &self.shutdown_tx,
            &self.kv_state.dropped_frames,
            self.task_ctx.tls.get().cloned(),
        ) {
            let ping = mycelium_core::codec::wire_to_bytes(&crate::framing::WireMessage::Ping {
                sender: self.node_id.clone(),
                known_peers: Vec::new(),
            });
            let _ = tx.try_send(ping);
        }
    }

    /// Drop a direct-route pin previously set by [`connect_peer`](Self::connect_peer). The peer
    /// reverts to normal forwarding-target eligibility (flood-relay when not active).
    pub fn disconnect_peer(&self, peer: &crate::node_id::NodeId) {
        self.pinned_peers.pin().remove(peer);
    }

    /// Returns the groups this node has currently joined.
    ///
    /// Reflects the local [`Boundary`] state at the moment of the call. Useful for
    /// diagnostics and Layer 3 routing decisions that depend on group membership.
    pub fn groups(&self) -> Vec<Arc<str>> {
        self.task_ctx.signal_boundary.read().groups.iter().cloned().collect()
    }

    /// Returns per-peer cumulative drop counts (only peers with at least one drop).
    ///
    /// Each entry is the total number of gossip frames dropped to that peer due to
    /// reconnect backoff since the peer writer was last spawned. Useful for identifying
    /// slow or unreachable peers that inflate the global `dropped_frames` counter.
    pub fn peer_drop_counts(&self) -> Vec<(crate::node_id::NodeId, u64)> {
        use std::sync::atomic::Ordering;
        self.peer_writers.pin()
            .iter()
            .map(|(k, v)| (k.clone(), v.dropped.load(Ordering::Relaxed)))
            .filter(|(_, n)| *n > 0)
            .collect()
    }
}
