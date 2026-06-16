//! SWIM-style UDP failure-detector transport (WS-B M5).
//!
//! **Stage 1 (this module so far):** the datagram type + a compact versioned codec,
//! and a gated UDP listener that reflects `Ping → Ack`. It is wired up only when
//! [`crate::config::GossipConfig::swim_failure_detector`] is set, so it is inert for
//! existing deployments. The full SWIM failure detector (direct/indirect probing,
//! `Alive`/`Suspect`/`Dead` with incarnation numbers) and the symmetric
//! membership/peer-sampling layer that flattens connection fan-out arrive in later
//! stages — see `docs/plans/v2-wsb-scale-transport.md` §"M5 execution staging".
//!
//! Heartbeats move to UDP because they are loss-tolerable and connection-free: a UDP
//! datagram leaves no entry in the Docker-bridge iptables FORWARD chain / conntrack
//! table, which is the O(N²) ceiling WS-B exists to break. TCP is retained for
//! anti-entropy and Data/Signal delivery, opened on demand.

use crate::framing::bincode_cfg;
use crate::node_id::NodeId;
use serde::{Deserialize, Serialize};
use std::sync::{atomic::{AtomicBool, Ordering}, Arc};
use tokio::{net::UdpSocket, sync::watch};
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

/// Stage-1 UDP listener: decodes inbound datagrams and reflects `Ping → Ack`.
///
/// Later stages graft the failure-detector state machine onto this loop; for now it
/// only proves the transport round-trips end-to-end. Runs until shutdown.
pub async fn run_swim_listener(
    socket: Arc<UdpSocket>,
    node_id: NodeId,
    shutdown_tx: Arc<watch::Sender<bool>>,
    alive: Arc<AtomicBool>,
) {
    alive.store(true, Ordering::Relaxed);
    let mut shutdown_rx = shutdown_tx.subscribe();
    let mut buf = vec![0u8; 2048];
    loop {
        tokio::select! {
            r = socket.recv_from(&mut buf) => {
                let (n, src) = match r {
                    Ok(v) => v,
                    Err(e) => { warn!("SWIM recv_from error: {e}"); continue; }
                };
                let Some(dg) = SwimDatagram::decode(&buf[..n]) else {
                    debug!(%src, "dropping undecodable SWIM datagram ({n} bytes)");
                    continue;
                };
                if let SwimDatagram::Ping { seq, .. } = dg {
                    let ack = SwimDatagram::Ack { seq, from: node_id.clone() }.encode();
                    if let Err(e) = socket.send_to(&ack, src).await {
                        debug!(%src, "SWIM Ack send failed: {e}");
                    }
                }
                // Ack / PingReq / PingReqAck handling lands with the Stage 2 detector.
            }
            // `changed()` (not `wait_for`) avoids holding a !Send watch guard across the
            // await; shutdown is monotonic false→true, so any change means stop.
            _ = shutdown_rx.changed() => break,
        }
    }
    alive.store(false, Ordering::Relaxed);
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

    #[tokio::test]
    async fn listener_reflects_ping_with_ack() {
        // Bind the listener socket and a client socket; the client Pings, expects an Ack.
        let server = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let server_addr = server.local_addr().unwrap();
        let (sh_tx, _) = watch::channel(false);
        let sh_tx = Arc::new(sh_tx);
        let alive = Arc::new(AtomicBool::new(false));
        let node = NodeId::new("127.0.0.1", server_addr.port()).unwrap();
        let task = tokio::spawn(run_swim_listener(
            Arc::clone(&server), node, Arc::clone(&sh_tx), Arc::clone(&alive)));

        let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let ping = SwimDatagram::Ping { seq: 99, from: id(client.local_addr().unwrap().port()) };
        client.send_to(&ping.encode(), server_addr).await.unwrap();

        let mut buf = vec![0u8; 512];
        let (n, _) = tokio::time::timeout(
            std::time::Duration::from_secs(2), client.recv_from(&mut buf))
            .await.expect("ack within timeout").unwrap();
        match SwimDatagram::decode(&buf[..n]) {
            Some(SwimDatagram::Ack { seq, .. }) => assert_eq!(seq, 99, "ack echoes seq"),
            other => panic!("expected Ack, got {other:?}"),
        }
        let _ = sh_tx.send(true);
        let _ = task.await;
    }
}
