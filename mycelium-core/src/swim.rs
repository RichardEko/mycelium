//! SWIM-style UDP failure-detector transport (WS-B M5).
//!
//! **Stages 1–2 (this module so far):** the datagram type + a compact versioned codec
//! (Stage 1), and the failure detector (Stage 2) — direct probe → `Ack`, indirect probe
//! (`PingReq` relayed through `k` random peers) on timeout, driven by [`SwimState`] and
//! the [`run_swim_prober`] loop. It is wired up only when
//! [`crate::config::GossipConfig::swim_failure_detector`] is set, so it is inert for
//! existing deployments. The symmetric membership / peer-sampling layer that flattens
//! connection fan-out (and the `Alive`/`Suspect`/`Dead` incarnation gossip) arrives in
//! Stage 3 — see `docs/plans/v2-wsb-scale-transport.md` §"M5 execution staging".
//!
//! Heartbeats move to UDP because they are loss-tolerable and connection-free: a UDP
//! datagram leaves no entry in the Docker-bridge iptables FORWARD chain / conntrack
//! table, which is the O(N²) ceiling WS-B exists to break. TCP is retained for
//! anti-entropy and Data/Signal delivery, opened on demand.
//!
//! **Liveness integration (Stage 2):** a successful probe (direct *or* indirect) refreshes
//! the peer's last-seen timestamp in the shared `peers` map — the same signal the TCP ping
//! used to provide — so the existing staleness-based eviction in the health monitor works
//! unchanged. The TCP heartbeat ping is removed at the Stage 4 cutover.

use crate::node_id::NodeId;
use crate::swim_membership::{ApplyEffect, MemberUpdate, SwimMembership};
use crate::writer::{evict_peer_writer, WriterEntry};
use ahash::AHashMap;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc, Mutex,
};
use std::time::{Duration, Instant};
use tokio::{net::UdpSocket, sync::oneshot, sync::watch};
use tracing::{debug, warn};

/// Version byte prefixed to every SWIM datagram. Lets the wire evolve independently
/// of the TCP `WIRE_VERSION`; a receiver drops datagrams it does not understand
/// (loss-tolerable — the failure detector simply retries / falls back to indirect).
/// v2 (Stage 3) adds the piggybacked membership `gossip` to `Ping`/`Ack`.
pub const SWIM_DATAGRAM_VERSION: u8 = 2;

/// Soft cap on the size of a SWIM datagram we emit. Kept well under a typical
/// 1500-byte path MTU so probes never fragment; the Stage 3 membership piggyback
/// budgets its gossip against this.
pub const SWIM_MAX_DATAGRAM: usize = 512;

/// A SWIM control datagram carried over UDP.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SwimDatagram {
    /// Direct liveness probe. The receiver merges `gossip`, applies it, and replies with
    /// [`SwimDatagram::Ack`] echoing `seq` and carrying its own gossip sample.
    Ping { seq: u64, from: NodeId, gossip: Vec<MemberUpdate> },
    /// Reply to a direct `Ping` (or relayed). Carries the responder's gossip sample so
    /// membership spreads on every probe round — discovery independent of forwarding.
    Ack { seq: u64, from: NodeId, gossip: Vec<MemberUpdate> },
    /// Indirect-probe request (SWIM): `from` could not reach `target` directly and
    /// asks the receiver to `Ping` `target` on its behalf and relay the `Ack` back.
    PingReq { seq: u64, from: NodeId, target: NodeId },
    /// Relayed acknowledgement: the receiver of a `PingReq` confirms that `target`
    /// answered, so the original prober can clear its suspicion.
    PingReqAck { seq: u64, target: NodeId },
}

impl SwimDatagram {
    /// Encode as `[version byte][bincode body]`.
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(48);
        buf.push(SWIM_DATAGRAM_VERSION);
        match crate::serde_fixint::to_vec(self) {
            Ok(body) => buf.extend_from_slice(&body),
            Err(e) => warn!("SWIM datagram encode failed: {e}"),
        }
        buf
    }

    /// Decode a datagram, returning `None` for an empty buffer, an unknown version,
    /// or a malformed body (all of which are dropped — UDP loss is tolerable).
    pub fn decode(bytes: &[u8]) -> Option<Self> {
        let (&ver, body) = bytes.split_first()?;
        if ver != SWIM_DATAGRAM_VERSION {
            return None;
        }
        crate::serde_fixint::from_slice(body).ok()
    }
}

/// Shared SWIM transport state: the UDP socket, this node's identity, a monotonic
/// probe-sequence counter, and the table of in-flight probes awaiting an acknowledgement.
/// Held in an `Arc` and shared between the listener loop (which resolves acks) and the
/// prober loop (which registers and awaits them).
pub struct SwimState {
    socket: Arc<UdpSocket>,
    self_id: NodeId,
    seq: AtomicU64,
    /// seq → completion sender. A probe registers an entry before sending; the listener
    /// removes + fires it when the matching `Ack` / `PingReqAck` arrives.
    pending: Mutex<AHashMap<u64, oneshot::Sender<()>>>,
    /// The SWIM membership table (Stage 3) — the source of truth gossiped over probes.
    membership: Mutex<SwimMembership>,
    /// Shared liveness/discovery map: gossip-learned `Alive` members are inserted here
    /// (so the bounded-fan-out reconcile + prober see the full cluster), and confirmed
    /// `Dead` members are removed + their TCP writer evicted.
    peers: Arc<papaya::HashMap<NodeId, Instant>>,
    peer_writers: Arc<papaya::HashMap<NodeId, WriterEntry>>,
    /// How many membership updates to piggyback on each `Ping`/`Ack` (bounded for MTU).
    gossip_updates: usize,
    /// The forwarding fan-out watch (same channel the health monitor publishes). A member
    /// learned Alive must become *sendable* immediately — mirroring the TCP Ping arm's
    /// event-driven activation. Without this, a SWIM-learned member was sendable only
    /// after the health monitor's next tick reconcile (up to startup-jitter + interval),
    /// and an early Individual-scoped RPC response hit "no peers at all" (the mailbox_llm
    /// cold-start drop, 2026-07-21).
    peer_list_tx: tokio::sync::watch::Sender<Arc<[NodeId]>>,
    /// Fan-out resolution inputs for the bounded activation (same `resolved_fanout` as
    /// the Ping arm — activation never un-bounds the active set).
    gossip_fanout: usize,
    max_active_connections: usize,
}

impl SwimState {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        socket: Arc<UdpSocket>,
        self_id: NodeId,
        peers: Arc<papaya::HashMap<NodeId, Instant>>,
        peer_writers: Arc<papaya::HashMap<NodeId, WriterEntry>>,
        gossip_updates: usize,
        peer_list_tx: tokio::sync::watch::Sender<Arc<[NodeId]>>,
        gossip_fanout: usize,
        max_active_connections: usize,
    ) -> Arc<Self> {
        let membership = Mutex::new(SwimMembership::new(self_id.clone()));
        Arc::new(Self {
            socket,
            self_id,
            seq: AtomicU64::new(1),
            pending: Mutex::new(AHashMap::new()),
            membership,
            peers,
            peer_writers,
            gossip_updates,
            peer_list_tx,
            gossip_fanout,
            max_active_connections,
        })
    }

    fn lock_membership(&self) -> std::sync::MutexGuard<'_, SwimMembership> {
        self.membership.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// A gossip sample to piggyback on an outgoing datagram.
    fn gossip(&self) -> Vec<MemberUpdate> {
        self.lock_membership().gossip_sample(self.gossip_updates)
    }

    /// Merge inbound gossip and apply each effect to the shared `peers` map. Returns the
    /// updates we must re-gossip to refute a rumour about ourselves (if any).
    fn merge_gossip(&self, updates: &[MemberUpdate]) {
        let now = Instant::now();
        let mut effects = Vec::new();
        {
            let mut m = self.lock_membership();
            for u in updates {
                effects.push(m.apply(u, now));
            }
        }
        for eff in effects {
            self.apply_effect(eff, now);
        }
    }

    /// Apply a membership effect to the shared `peers` / writer maps. `BecameAlive` is the
    /// discovery hook (a node learned only via gossip becomes probeable + forwardable);
    /// `BecameDead` removes it and closes its connection. Refutation needs no side effect
    /// here — our bumped incarnation rides out on the next gossip sample.
    fn apply_effect(&self, eff: ApplyEffect, now: Instant) {
        match eff {
            ApplyEffect::BecameAlive(node) => {
                if node != self.self_id {
                    self.peers.pin().get_or_insert(node.clone(), now);
                    // Event-driven fan-out activation — the SWIM twin of the TCP Ping
                    // arm's append. Bounded by the same resolved fan-out `k`; the health
                    // monitor remains the steady-state reconciler/evictor. Without this,
                    // an Alive member was unsendable until the next tick reconcile.
                    let known_len = self.peers.pin().len();
                    self.peer_list_tx.send_if_modified(|current| {
                        // Size the cap by everything we're acquainted with: the live map
                        // PLUS what already sits in the published set — the watch is
                        // bootstrap-seeded before those peers surface in the map. Sizing
                        // by the map alone let an early BecameAlive find k=1 already
                        // "filled" by the bootstrap seed and refuse the only LIVE member
                        // (failover client stuck on a dead primary until the first tick
                        // reconcile — CI failover_preserves_items_and_ids, 2026-07-21).
                        let sizing = known_len.max(current.len() + 1);
                        let k = crate::config::resolved_fanout(
                            self.gossip_fanout, self.max_active_connections, sizing);
                        if current.contains(&node) || current.len() >= k.max(1) {
                            return false;
                        }
                        let mut next: Vec<NodeId> = current.to_vec();
                        next.push(node.clone());
                        *current = next.into();
                        true
                    });
                }
            }
            ApplyEffect::BecameDead(node) => {
                self.peers.pin().remove(&node);
                evict_peer_writer(&self.peer_writers, &node);
            }
            ApplyEffect::None | ApplyEffect::RefutedSelf(_) => {}
        }
    }

    /// Record that `node` is alive (a probe/`Ack` confirmed it) and refresh its liveness
    /// timestamp for the health-monitor staleness eviction.
    fn observe_alive(&self, node: &NodeId) {
        if *node == self.self_id {
            return;
        }
        let now = Instant::now();
        let eff = self.lock_membership().observe_alive(node, now);
        self.apply_effect(eff, now);
        refresh(&self.peers, node);
    }

    /// Locally suspect `node` (direct + indirect probes both failed). The suspicion is
    /// recorded in the table and rides out on subsequent gossip samples; it is promoted
    /// to `Dead` by [`SwimState::tick_suspicion`] if no one refutes it in time.
    fn suspect(&self, node: &NodeId) {
        let _ = self.lock_membership().suspect(node, Instant::now());
    }

    /// Promote suspects that have outlived `timeout` to `Dead`, removing them from `peers`.
    fn tick_suspicion(&self, timeout: Duration) {
        let now = Instant::now();
        let dead = self.lock_membership().promote_expired_suspects(now, timeout);
        for u in dead {
            self.apply_effect(ApplyEffect::BecameDead(u.node), now);
        }
    }

    /// Members currently believed alive (probe candidates).
    fn alive_members(&self) -> Vec<NodeId> {
        self.lock_membership().alive_members()
    }

    fn next_seq(&self) -> u64 {
        self.seq.fetch_add(1, Ordering::Relaxed)
    }

    /// Register interest in `seq`, returning a receiver that fires when the matching
    /// acknowledgement arrives. The entry is auto-removed on resolve or on drop-cleanup.
    fn register(&self, seq: u64) -> oneshot::Receiver<()> {
        let (tx, rx) = oneshot::channel();
        self.pending.lock().unwrap_or_else(|e| e.into_inner()).insert(seq, tx);
        rx
    }

    /// Resolve a pending probe (an `Ack`/`PingReqAck` arrived). No-op if unknown/expired.
    fn resolve(&self, seq: u64) {
        if let Some(tx) = self.pending.lock().unwrap_or_else(|e| e.into_inner()).remove(&seq) {
            let _ = tx.send(());
        }
    }

    fn forget(&self, seq: u64) {
        self.pending.lock().unwrap_or_else(|e| e.into_inner()).remove(&seq);
    }

    /// Direct probe: `Ping` the target and wait up to `timeout` for its `Ack`.
    /// Returns `true` iff the target acknowledged.
    pub async fn probe_direct(&self, target: SocketAddr, timeout: Duration) -> bool {
        let seq = self.next_seq();
        let rx = self.register(seq);
        let ping = SwimDatagram::Ping {
            seq,
            from: self.self_id.clone(),
            gossip: self.gossip(),
        }
        .encode();
        if self.socket.send_to(&ping, target).await.is_err() {
            self.forget(seq);
            return false;
        }
        let ok = tokio::time::timeout(timeout, rx).await.map(|r| r.is_ok()).unwrap_or(false);
        self.forget(seq);
        ok
    }

    /// Indirect probe: ask each `relay` to probe `target` on our behalf (`PingReq`) and
    /// wait up to `timeout` for the first relayed `PingReqAck`. Returns `true` iff any
    /// relay confirmed the target is alive.
    pub async fn probe_indirect(
        &self,
        target: &NodeId,
        relays: &[SocketAddr],
        timeout: Duration,
    ) -> bool {
        if relays.is_empty() {
            return false;
        }
        let seq = self.next_seq();
        let rx = self.register(seq);
        let req = SwimDatagram::PingReq {
            seq,
            from: self.self_id.clone(),
            target: target.clone(),
        }
        .encode();
        let mut sent = false;
        for relay in relays {
            if self.socket.send_to(&req, *relay).await.is_ok() {
                sent = true;
            }
        }
        if !sent {
            self.forget(seq);
            return false;
        }
        let ok = tokio::time::timeout(timeout, rx).await.map(|r| r.is_ok()).unwrap_or(false);
        self.forget(seq);
        ok
    }
}

/// UDP listener: decodes inbound datagrams and drives the SWIM control plane.
///
/// - `Ping` → reply `Ack` (reflector).
/// - `PingReq` → relay: probe `target` directly on the requester's behalf, and on its
///   `Ack` send `PingReqAck` back to the requester (spawned so the recv loop never
///   blocks on the relayed probe).
/// - `Ack` → resolve the matching direct probe.
/// - `PingReqAck` → resolve the matching indirect probe.
///
/// Runs until shutdown. `probe_timeout` bounds the relayed direct probe.
pub async fn run_swim_listener(
    state: Arc<SwimState>,
    shutdown_tx: Arc<watch::Sender<bool>>,
    alive: Arc<AtomicBool>,
    probe_timeout: Duration,
) {
    alive.store(true, Ordering::Relaxed);
    let mut shutdown_rx = shutdown_tx.subscribe();
    let mut buf = vec![0u8; 2048];
    loop {
        tokio::select! {
            r = state.socket.recv_from(&mut buf) => {
                let (n, src) = match r {
                    Ok(v) => v,
                    Err(e) => { warn!("SWIM recv_from error: {e}"); continue; }
                };
                let Some(dg) = SwimDatagram::decode(&buf[..n]) else {
                    debug!(%src, "dropping undecodable SWIM datagram ({n} bytes)");
                    continue;
                };
                match dg {
                    SwimDatagram::Ping { seq, from, gossip } => {
                        state.merge_gossip(&gossip);
                        // The pinging peer is alive (we just heard from it).
                        state.observe_alive(&from);
                        let ack = SwimDatagram::Ack {
                            seq,
                            from: state.self_id.clone(),
                            gossip: state.gossip(),
                        }
                        .encode();
                        if let Err(e) = state.socket.send_to(&ack, src).await {
                            debug!(%src, "SWIM Ack send failed: {e}");
                        }
                    }
                    SwimDatagram::Ack { seq, from, gossip } => {
                        state.merge_gossip(&gossip);
                        state.observe_alive(&from);
                        state.resolve(seq);
                    }
                    SwimDatagram::PingReqAck { seq, .. } => state.resolve(seq),
                    SwimDatagram::PingReq { seq, from: _, target } => {
                        // Relay: probe the target ourselves; if it answers, tell the
                        // requester (reply to `src`, the datagram's origin). Spawned so a
                        // slow/dead target cannot stall the recv loop.
                        let state = Arc::clone(&state);
                        tokio::spawn(async move {
                            if state.probe_direct(probe_addr(&target), probe_timeout).await {
                                let ack = SwimDatagram::PingReqAck { seq, target }.encode();
                                let _ = state.socket.send_to(&ack, src).await;
                            }
                        });
                    }
                }
            }
            // `changed()` (not `wait_for`) avoids holding a !Send watch guard across the
            // await; shutdown is monotonic false→true, so any change means stop.
            _ = shutdown_rx.changed() => break,
        }
    }
    alive.store(false, Ordering::Relaxed);
}

/// Compute the UDP probe address for a peer. Peers advertise their gossip TCP address
/// (`NodeId`); under the same-port SWIM convention the UDP port equals that TCP port.
fn probe_addr(peer: &NodeId) -> SocketAddr {
    peer.to_socket_addr()
}

/// The SWIM prober loop. Every protocol period it (1) promotes expired suspects to
/// `Dead`, then (2) picks a random member and probes it (direct → indirect on timeout).
/// A successful probe records the member `Alive` (and refreshes its liveness timestamp);
/// a member that answers neither probe is marked `Suspect`, which gossips out and, if not
/// refuted within `suspicion_timeout`, is promoted to `Dead` and evicted. Membership
/// learned via the gossip piggyback (not just direct contact) makes discovery — and the
/// resulting de-pinning of well-known seeds — independent of the bounded forwarding set.
/// Runs until shutdown.
#[allow(clippy::too_many_arguments)]
pub async fn run_swim_prober(
    state: Arc<SwimState>,
    bootstrap: Arc<[NodeId]>,
    shutdown_tx: Arc<watch::Sender<bool>>,
    alive: Arc<AtomicBool>,
    interval: Duration,
    probe_timeout: Duration,
    indirect_k: usize,
    suspicion_timeout: Duration,
) {
    alive.store(true, Ordering::Relaxed);
    let mut shutdown_rx = shutdown_tx.subscribe();
    // `.max(1ms)`: `tokio::time::interval(Duration::ZERO)` PANICS ("period must be non-zero"), and
    // this task is spawned fire-and-forget, so a zero `swim_probe_interval_ms` (env-settable, and NOT
    // caught by `validate()` before the pass-4 fix) aborts the whole node under the release
    // `panic="abort"` profile. Defense-in-depth mirroring the GC/health tickers (audit 2026-07-15 pass 4).
    let mut ticker = tokio::time::interval(interval.max(Duration::from_millis(1)));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = ticker.tick() => {
                state.tick_suspicion(suspicion_timeout);

                // Probe candidates: alive members, falling back to bootstrap peers while
                // the table is still empty (join — we have no gossip yet).
                let mut members = state.alive_members();
                if members.is_empty() {
                    members = bootstrap.iter().cloned().collect();
                }
                if members.is_empty() { continue; }
                let target = members[fastrand::usize(..members.len())].clone();

                if state.probe_direct(probe_addr(&target), probe_timeout).await {
                    state.observe_alive(&target);
                    continue;
                }
                // Direct probe failed — ask k random *other* members to probe indirectly.
                let relays: Vec<SocketAddr> = {
                    let mut others: Vec<&NodeId> = members.iter().filter(|p| **p != target).collect();
                    fastrand::shuffle(&mut others);
                    others.into_iter().take(indirect_k).map(probe_addr).collect()
                };
                if state.probe_indirect(&target, &relays, probe_timeout).await {
                    state.observe_alive(&target);
                } else {
                    state.suspect(&target);
                }
            }
            _ = shutdown_rx.changed() => break,
        }
    }
    alive.store(false, Ordering::Relaxed);
}

/// Refresh a peer's last-seen timestamp to now, only if it is still present (do not
/// resurrect a peer the eviction path just removed — retry-safe `compute`).
fn refresh(peers: &papaya::HashMap<NodeId, Instant>, peer: &NodeId) {
    let now = Instant::now();
    peers.pin().compute(peer.clone(), |existing| match existing {
        Some(_) => papaya::Operation::Insert(now),
        None => papaya::Operation::Abort(()),
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::swim_membership::MemberStatus;

    fn id(p: u16) -> NodeId { NodeId::new("127.0.0.1", p).unwrap() }
    fn upd(p: u16) -> MemberUpdate { MemberUpdate { node: id(p), incarnation: 0, status: MemberStatus::Alive } }

    /// The bootstrap-seeded-watch trap (CI `failover_preserves_items_and_ids`,
    /// 2026-07-21): the forwarding watch already holds a bootstrap entry that has not
    /// yet surfaced in the peers map. An early `BecameAlive` must still activate the
    /// live member — sizing the fan-out cap by the map alone found k=1 already
    /// "filled" by the seed and refused the only live peer, leaving the node sending
    /// to a dead primary until the first tick reconcile.
    #[tokio::test]
    async fn became_alive_activates_despite_bootstrap_seeded_watch() {
        let socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let port = socket.local_addr().unwrap().port();
        let me = NodeId::new("127.0.0.1", port).unwrap();
        let peers = Arc::new(papaya::HashMap::new());
        let writers = Arc::new(papaya::HashMap::new());
        let bootstrap = id(9101);
        let live = id(9102);
        let (plt, rx) = watch::channel::<Arc<[NodeId]>>(vec![bootstrap.clone()].into());
        let state = SwimState::new(socket, me, Arc::clone(&peers), writers, 6, plt, 0, 0);
        state.apply_effect(ApplyEffect::BecameAlive(live.clone()), Instant::now());
        let published = rx.borrow().clone();
        assert!(published.contains(&live),
            "BecameAlive must activate the live member even with a bootstrap-seeded watch");
        assert!(published.contains(&bootstrap), "activation appends, never replaces");
    }

    #[test]
    fn datagram_round_trips_all_variants() {
        let cases = [
            SwimDatagram::Ping { seq: 7, from: id(8000), gossip: vec![upd(8003), upd(8004)] },
            SwimDatagram::Ack { seq: 7, from: id(8001), gossip: vec![] },
            SwimDatagram::PingReq { seq: 42, from: id(8000), target: id(8002) },
            SwimDatagram::PingReqAck { seq: 42, target: id(8002) },
        ];
        for dg in cases {
            let encoded = dg.encode();
            assert_eq!(encoded[0], SWIM_DATAGRAM_VERSION, "version byte prefixed");
            assert!(encoded.len() <= SWIM_MAX_DATAGRAM, "stays under the MTU budget");
            assert_eq!(SwimDatagram::decode(&encoded), Some(dg));
        }
    }

    #[test]
    fn decode_rejects_empty_unknown_version_and_garbage() {
        assert_eq!(SwimDatagram::decode(&[]), None);
        // Unknown version byte.
        let mut bad = SwimDatagram::Ping { seq: 1, from: id(8000), gossip: vec![] }.encode();
        bad[0] = SWIM_DATAGRAM_VERSION.wrapping_add(1);
        assert_eq!(SwimDatagram::decode(&bad), None);
        // Right version, garbage body.
        assert_eq!(SwimDatagram::decode(&[SWIM_DATAGRAM_VERSION, 0xFF, 0xFF, 0xFF]), None);
    }

    const PT: Duration = Duration::from_millis(500);

    /// Spin up a live SWIM node (socket + state + running listener) on an ephemeral
    /// port, returning its state, identity, shutdown handle, and its `peers` map.
    async fn spawn_node() -> (Arc<SwimState>, NodeId, Arc<watch::Sender<bool>>, Arc<papaya::HashMap<NodeId, Instant>>) {
        let socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let port = socket.local_addr().unwrap().port();
        let node = NodeId::new("127.0.0.1", port).unwrap();
        let peers = Arc::new(papaya::HashMap::new());
        let writers = Arc::new(papaya::HashMap::new());
        let (peer_list_tx, _) = watch::channel::<Arc<[NodeId]>>(Vec::new().into());
        let state = SwimState::new(Arc::clone(&socket), node.clone(), Arc::clone(&peers), writers, 6,
                                   peer_list_tx, 0, 0);
        let (sh, _) = watch::channel(false);
        let sh = Arc::new(sh);
        let alive = Arc::new(AtomicBool::new(false));
        tokio::spawn(run_swim_listener(Arc::clone(&state), Arc::clone(&sh), alive, PT));
        (state, node, sh, peers)
    }

    /// A `NodeId` whose port is (almost certainly) not bound — bind then drop to free it.
    async fn dead_node() -> NodeId {
        let s = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let port = s.local_addr().unwrap().port();
        drop(s);
        NodeId::new("127.0.0.1", port).unwrap()
    }

    #[tokio::test]
    async fn direct_probe_succeeds_against_live_peer_and_times_out_against_dead() {
        let (a, _aid, _ash, _ap) = spawn_node().await;
        let (_b, bid, _bsh, _bp) = spawn_node().await;
        assert!(a.probe_direct(probe_addr(&bid), PT).await, "live peer acks");

        let dead = dead_node().await;
        assert!(!a.probe_direct(probe_addr(&dead), Duration::from_millis(300)).await,
            "dead target times out");
        // No pending state should leak after either probe.
        assert!(a.pending.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn indirect_probe_succeeds_via_relay() {
        let (a, _aid, _ash, _ap) = spawn_node().await;
        let (_r, rid, _rsh, _rp) = spawn_node().await; // relay
        let (_c, cid, _csh, _cp) = spawn_node().await; // live target
        // A reaches C only by asking R to probe on its behalf.
        assert!(
            a.probe_indirect(&cid, &[probe_addr(&rid)], PT).await,
            "relay confirms the live target"
        );
        assert!(a.pending.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn indirect_probe_fails_when_target_is_dead() {
        let (a, _aid, _ash, _ap) = spawn_node().await;
        let (_r, rid, _rsh, _rp) = spawn_node().await; // relay is alive…
        let dead = dead_node().await; // …but the target is not
        assert!(
            !a.probe_indirect(&dead, &[probe_addr(&rid)], Duration::from_millis(400)).await,
            "no relay can confirm a dead target"
        );
    }

    #[tokio::test]
    async fn indirect_probe_with_no_relays_is_false() {
        let (a, _aid, _ash, _ap) = spawn_node().await;
        let dead = dead_node().await;
        assert!(!a.probe_indirect(&dead, &[], PT).await);
    }

    #[tokio::test]
    async fn gossip_spreads_membership_for_discovery() {
        // The Stage-3 property: B learns about C purely from A's gossip, without ever
        // probing C — discovery decoupled from direct contact / the forwarding set.
        let (a, _aid, _ash, _ap) = spawn_node().await;
        let (_b, bid, _bsh, b_peers) = spawn_node().await;
        let cid = id(60123); // a node A "knows" but B has never contacted

        // Seed A's membership with C as alive, then have A probe B (carrying gossip).
        a.lock_membership().apply(
            &MemberUpdate { node: cid.clone(), incarnation: 0, status: MemberStatus::Alive },
            Instant::now(),
        );
        assert!(a.probe_direct(probe_addr(&bid), PT).await, "A reaches B");

        // B should have learned C from A's piggybacked gossip and inserted it into peers.
        // (Allow a brief moment for the listener to process the Ping.)
        let mut found = false;
        for _ in 0..20 {
            if b_peers.pin().contains_key(&cid) { found = true; break; }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        assert!(found, "B discovered C via gossip without probing it");
    }

    #[tokio::test]
    async fn refresh_only_touches_present_peers() {
        let peers: papaya::HashMap<NodeId, Instant> = papaya::HashMap::new();
        let present = id(9001);
        let absent = id(9002);
        let old = Instant::now() - Duration::from_secs(60);
        peers.pin().insert(present.clone(), old);

        refresh(&peers, &present);
        refresh(&peers, &absent);

        let g = peers.pin();
        assert!(*g.get(&present).unwrap() > old, "present peer refreshed");
        assert!(g.get(&absent).is_none(), "absent peer not resurrected");
    }
}
