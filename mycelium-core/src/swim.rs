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

use crate::framing::bincode_cfg;
use crate::node_id::NodeId;
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
pub const SWIM_DATAGRAM_VERSION: u8 = 1;

/// Soft cap on the size of a SWIM datagram we emit. Kept well under a typical
/// 1500-byte path MTU so probes never fragment; the Stage 3 membership piggyback
/// budgets its gossip against this.
pub const SWIM_MAX_DATAGRAM: usize = 512;

/// A SWIM control datagram carried over UDP.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SwimDatagram {
    /// Direct liveness probe. The receiver replies with [`SwimDatagram::Ack`] echoing `seq`.
    Ping { seq: u64, from: NodeId },
    /// Reply to a direct `Ping`, or to a relayed `PingReq` (indirect probe).
    Ack { seq: u64, from: NodeId },
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
        match bincode::serde::encode_to_vec(self, bincode_cfg()) {
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
        bincode::serde::decode_from_slice(body, bincode_cfg())
            .ok()
            .map(|(d, _)| d)
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
}

impl SwimState {
    pub fn new(socket: Arc<UdpSocket>, self_id: NodeId) -> Arc<Self> {
        Arc::new(Self { socket, self_id, seq: AtomicU64::new(1), pending: Mutex::new(AHashMap::new()) })
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
        let ping = SwimDatagram::Ping { seq, from: self.self_id.clone() }.encode();
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
                    SwimDatagram::Ping { seq, .. } => {
                        let ack = SwimDatagram::Ack { seq, from: state.self_id.clone() }.encode();
                        if let Err(e) = state.socket.send_to(&ack, src).await {
                            debug!(%src, "SWIM Ack send failed: {e}");
                        }
                    }
                    SwimDatagram::Ack { seq, .. } => state.resolve(seq),
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

/// The SWIM prober loop: every protocol period, pick a random live peer, probe it
/// (direct → indirect on timeout), and on success refresh its last-seen timestamp in
/// `peers` so the health monitor's staleness eviction treats it as alive. A peer that
/// answers neither probe simply stops being refreshed and ages out via the existing
/// eviction window. Runs until shutdown.
#[allow(clippy::too_many_arguments)]
pub async fn run_swim_prober(
    state: Arc<SwimState>,
    peers: Arc<papaya::HashMap<NodeId, Instant>>,
    shutdown_tx: Arc<watch::Sender<bool>>,
    alive: Arc<AtomicBool>,
    interval: Duration,
    probe_timeout: Duration,
    indirect_k: usize,
) {
    alive.store(true, Ordering::Relaxed);
    let mut shutdown_rx = shutdown_tx.subscribe();
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = ticker.tick() => {
                // Snapshot the current peer set.
                let members: Vec<NodeId> = {
                    let g = peers.pin();
                    g.iter().map(|(id, _)| id.clone()).collect()
                };
                if members.is_empty() { continue; }
                let target = &members[fastrand::usize(..members.len())];

                if state.probe_direct(probe_addr(target), probe_timeout).await {
                    refresh(&peers, target);
                    continue;
                }
                // Direct probe failed — ask k random *other* peers to probe indirectly.
                let relays: Vec<SocketAddr> = {
                    let mut others: Vec<&NodeId> = members.iter().filter(|p| *p != target).collect();
                    fastrand::shuffle(&mut others);
                    others.into_iter().take(indirect_k).map(probe_addr).collect()
                };
                if state.probe_indirect(target, &relays, probe_timeout).await {
                    refresh(&peers, target);
                }
                // Else: no refresh → the health monitor evicts the peer once it goes stale.
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

    fn id(p: u16) -> NodeId { NodeId::new("127.0.0.1", p).unwrap() }

    #[test]
    fn datagram_round_trips_all_variants() {
        let cases = [
            SwimDatagram::Ping { seq: 7, from: id(8000) },
            SwimDatagram::Ack { seq: 7, from: id(8001) },
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
        let mut bad = SwimDatagram::Ping { seq: 1, from: id(8000) }.encode();
        bad[0] = SWIM_DATAGRAM_VERSION.wrapping_add(1);
        assert_eq!(SwimDatagram::decode(&bad), None);
        // Right version, garbage body.
        assert_eq!(SwimDatagram::decode(&[SWIM_DATAGRAM_VERSION, 0xFF, 0xFF, 0xFF]), None);
    }

    const PT: Duration = Duration::from_millis(500);

    /// Spin up a live SWIM node (socket + state + running listener) on an ephemeral
    /// port, returning its state, identity, and a shutdown handle.
    async fn spawn_node() -> (Arc<SwimState>, NodeId, Arc<watch::Sender<bool>>) {
        let socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let port = socket.local_addr().unwrap().port();
        let node = NodeId::new("127.0.0.1", port).unwrap();
        let state = SwimState::new(Arc::clone(&socket), node.clone());
        let (sh, _) = watch::channel(false);
        let sh = Arc::new(sh);
        let alive = Arc::new(AtomicBool::new(false));
        tokio::spawn(run_swim_listener(Arc::clone(&state), Arc::clone(&sh), alive, PT));
        (state, node, sh)
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
        let (a, _aid, _ash) = spawn_node().await;
        let (_b, bid, _bsh) = spawn_node().await;
        assert!(a.probe_direct(probe_addr(&bid), PT).await, "live peer acks");

        let dead = dead_node().await;
        assert!(!a.probe_direct(probe_addr(&dead), Duration::from_millis(300)).await,
            "dead target times out");
        // No pending state should leak after either probe.
        assert!(a.pending.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn indirect_probe_succeeds_via_relay() {
        let (a, _aid, _ash) = spawn_node().await;
        let (_r, rid, _rsh) = spawn_node().await; // relay
        let (_c, cid, _csh) = spawn_node().await; // live target
        // A reaches C only by asking R to probe on its behalf.
        assert!(
            a.probe_indirect(&cid, &[probe_addr(&rid)], PT).await,
            "relay confirms the live target"
        );
        assert!(a.pending.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn indirect_probe_fails_when_target_is_dead() {
        let (a, _aid, _ash) = spawn_node().await;
        let (_r, rid, _rsh) = spawn_node().await; // relay is alive…
        let dead = dead_node().await; // …but the target is not
        assert!(
            !a.probe_indirect(&dead, &[probe_addr(&rid)], Duration::from_millis(400)).await,
            "no relay can confirm a dead target"
        );
    }

    #[tokio::test]
    async fn indirect_probe_with_no_relays_is_false() {
        let (a, _aid, _ash) = spawn_node().await;
        let dead = dead_node().await;
        assert!(!a.probe_indirect(&dead, &[], PT).await);
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
