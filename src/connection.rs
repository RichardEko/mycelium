use crate::error::GossipError;
use crate::framing::{
    bincode_cfg, is_connection_closed, read_frame, shard_for_key, GossipUpdate,
    SyncEntry, WireMessage, ANTI_ENTROPY_NONCE, DATA_TAG, NONCE_OFFSET, TTL_OFFSET,
};
use crate::signal::{Boundary, Signal, SignalHandlers, SignalScope};
use crate::store::intern_key;
use crate::node_id::NodeId;
use crate::seen::ShardedSeen;
use crate::store::{apply_and_notify, StoreEntry};
use crate::writer::{get_or_spawn_writer, request_state, WriterEntry};
use bytes::{BufMut, Bytes, BytesMut};
use dashmap::DashMap;
use papaya::HashMap as PapayaMap;
use parking_lot::RwLock;
use std::time::Instant;
use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{io::BufReader, net::TcpStream, sync::{mpsc, mpsc::error::TrySendError, watch}};
use tracing::{error, warn};

/// Shared state threaded into every inbound connection handler.
pub(crate) struct ConnContext {
    pub(crate) node_id:          NodeId,
    pub(crate) store:            Arc<PapayaMap<Arc<str>, StoreEntry>>,
    pub(crate) peers:            Arc<PapayaMap<NodeId, Instant>>,
    /// One sender per gossip shard; carries pre-encoded frame bytes + sender id_hash.
    /// The shard fans out bytes directly — no re-encoding per hop (zero-copy forwarding).
    pub(crate) gossip_txs:       Arc<[mpsc::Sender<(Bytes, u64)>]>,
    pub(crate) seen:             Arc<ShardedSeen>,
    pub(crate) shutdown:         Arc<watch::Sender<bool>>,
    pub(crate) max_ttl:          u8,
    pub(crate) subscriptions:    Arc<PapayaMap<Arc<str>, watch::Sender<Option<Bytes>>>>,
    /// Unix-millisecond clock shared with the health monitor.
    pub(crate) current_ts:       Arc<AtomicU64>,
    pub(crate) peer_writers:     Arc<DashMap<NodeId, WriterEntry>>,
    pub(crate) writer_depth:     usize,
    pub(crate) backoff:          Duration,
    pub(crate) n_shards:         usize,
    pub(crate) intern_keys:      bool,
    pub(crate) signal_boundary:  Arc<RwLock<Boundary>>,
    pub(crate) signal_handlers:  Arc<SignalHandlers>,
    /// Cap on the peer table. Piggybacked peers are silently ignored once this
    /// is reached; bootstrap peers and direct senders are always admitted.
    pub(crate) max_peers:        usize,
}

pub(crate) async fn handle_connection(
    socket: TcpStream,
    peer_addr: SocketAddr,
    ctx: ConnContext,
) -> Result<(), GossipError> {
    let ConnContext {
        node_id, store, peers, gossip_txs, seen, shutdown, max_ttl,
        subscriptions, current_ts, peer_writers, writer_depth, backoff, n_shards,
        intern_keys, signal_boundary, signal_handlers, max_peers,
    } = ctx;
    let mut socket = BufReader::with_capacity(8_192, socket);
    let mut shutdown_rx = shutdown.subscribe();
    // BytesMut: recv_buf.split().freeze() at TTL_OFFSET is O(1) for zero-copy forwarding.
    let mut recv_buf: BytesMut = BytesMut::with_capacity(2_048);

    loop {
        let live = tokio::select! { biased;
            result = read_frame(&mut socket, &mut recv_buf) => {
                match result {
                    Ok(()) => true,
                    Err(e) if is_connection_closed(&e) => false,
                    Err(e) => { warn!("Read error from {}: {}", peer_addr, e); false }
                }
            }
            _ = shutdown_rx.wait_for(|v| *v) => false,
        };
        if !live { break; }

        // Fast-path dedup: read nonce directly from the wire buffer before the
        // full bincode decode. Data is variant 0 (tag DATA_TAG = [0,0,0,0] LE);
        // NONCE_OFFSET=4 points at the u64 nonce in fixed-int encoding.
        // Under TTL=5, ~80% of inbound Data frames are duplicates; this saves
        // a full decode + two heap allocations (Arc<str> key, Bytes value) on
        // every duplicate. A malformed frame whose first 12 bytes look like a
        // valid Data header may poison that nonce in the seen-set; since nonces
        // are random u64s the collision probability is negligible (< 1 in 2^64).
        if recv_buf.len() >= NONCE_OFFSET + 8 && recv_buf[..4] == DATA_TAG {
            let nonce = u64::from_le_bytes(
                recv_buf[NONCE_OFFSET..NONCE_OFFSET + 8].try_into().unwrap(),
            );
            if seen.is_duplicate(nonce, current_ts.load(Ordering::Relaxed)) {
                continue;
            }
        }

        let msg: WireMessage = match bincode::serde::decode_from_slice(
            &recv_buf, bincode_cfg(),
        ).map(|(v, _)| v) {
            Ok(m) => m,
            Err(e) => {
                warn!("Malformed message from {}: {}", peer_addr, e);
                continue;
            }
        };

        match msg {
            WireMessage::Ping { sender, known_peers } => {
                let now = Instant::now();
                let sender_is_new = {
                    let guard = peers.pin();
                    let is_new = guard.insert(sender.clone(), now).is_none();
                    // Only add piggybacked peers while the table has room.
                    // The direct sender is always admitted (inserted above); only
                    // the forwarded list is capped.
                    for peer in known_peers {
                        if peer != node_id && guard.len() < max_peers {
                            guard.get_or_insert(peer, now);
                        }
                    }
                    is_new
                };
                if sender_is_new {
                    request_state(&sender, &peer_writers, writer_depth, backoff, &shutdown, &node_id);
                }
            }

            WireMessage::StateRequest { sender } => {
                // Trusted-domain check: sender must be a known peer. We do not verify that
                // peer_addr matches sender.to_socket_addr() — that would reject NAT'd topologies.
                // In the trusted domain a connected node could spoof the sender field; the
                // consequence is a StateResponse routed to another peer, which is harmless.
                if !peers.pin().contains_key(&sender) {
                    warn!("Ignoring StateRequest from unknown peer {} (reported as {})", peer_addr, sender);
                    continue;
                }
                let entries: Vec<SyncEntry> = {
                    let guard = store.pin();
                    guard.iter()
                        .map(|(k, v)| SyncEntry {
                            key:          k.clone(),
                            value:        v.data.clone().unwrap_or_default(),
                            timestamp:    v.timestamp,
                            is_tombstone: v.data.is_none(),
                        })
                        .collect()
                };
                let mut buf = BytesMut::new();
                match bincode::serde::encode_into_std_write(
                    WireMessage::StateResponse { entries },
                    &mut (&mut buf).writer(),
                    bincode_cfg(),
                ) {
                    Err(e) => warn!("StateResponse serialize failed for {}: {}", sender, e),
                    Ok(_) => {
                        let data: Bytes = buf.freeze();
                        // Guard against oversized frames before they reach the writer,
                        // where a failed write would silently abort the anti-entropy sync.
                        if 1 + data.len() > crate::framing::MAX_FRAME_BYTES {
                            warn!(
                                "StateResponse for {} is {} B (limit {} B); \
                                 skipping anti-entropy — store has too many entries \
                                 for a single frame",
                                sender, data.len(), crate::framing::MAX_FRAME_BYTES,
                            );
                            continue;
                        }
                        let tx = get_or_spawn_writer(&sender, &peer_writers, writer_depth, backoff, &shutdown);
                        match tx.try_send(data) {
                            Ok(()) => {}
                            Err(TrySendError::Full(_)) => {
                                warn!("StateResponse channel full for {}", sender);
                            }
                            Err(TrySendError::Closed(_)) => {
                                error!("StateResponse writer for {} has exited", sender);
                            }
                        }
                    }
                }
            }

            WireMessage::StateResponse { entries } => {
                for entry in entries {
                    // Intern keys from anti-entropy the same way as Data messages so
                    // both paths share the same Arc<str> allocation for the same key.
                    let key = if intern_keys { intern_key(entry.key) } else { entry.key };
                    let update = GossipUpdate {
                        // StateResponse entries bypass the seen-set; TTL=1 prevents re-gossip.
                        nonce:        ANTI_ENTROPY_NONCE,
                        sender:       node_id.id_hash(),
                        ttl:          1,
                        is_tombstone: entry.is_tombstone,
                        timestamp:    entry.timestamp,
                        key,
                        value:        entry.value,
                    };
                    apply_and_notify(&store, &subscriptions, &update);
                }
            }

            WireMessage::Signal { ttl, nonce, sender, scope, kind, payload } => {
                let ts = current_ts.load(Ordering::Relaxed);
                if seen.is_duplicate(nonce, ts) {
                    continue;
                }
                // Boundary check: act if admitted (forwarding is unconditional below).
                // Individual signals bypass opacity — no routing alternative exists.
                // System/Group signals are subject to load-adaptive opacity: when handler
                // channels fill up the boundary probabilistically blocks admission,
                // shedding load to less-busy peers without coordination.
                if signal_boundary.read().admits(&scope) {
                    let admit = match &scope {
                        SignalScope::Individual(_) => true,
                        _ => {
                            let opacity = signal_handlers.fill_ratio(&kind);
                            opacity == 0.0 || fastrand::f32() >= opacity
                        }
                    };
                    if admit {
                        signal_handlers.deliver(&Signal {
                            kind: kind.clone(),
                            scope: scope.clone(),
                            payload: payload.clone(),
                            sender: sender.clone(),
                            nonce,
                        });
                    }
                }
                // Always forward — epidemic propagation regardless of scope.
                // Signal frames have variable-length scope so TTL cannot be decremented
                // in-place at a fixed offset (unlike Data frames). Full re-encode required.
                if ttl > 1 {
                    let shard = shard_for_key(&kind, n_shards);
                    let mut fwd_buf = BytesMut::with_capacity(recv_buf.len());
                    match bincode::serde::encode_into_std_write(
                        WireMessage::Signal {
                            ttl: ttl - 1, nonce,
                            sender: sender.clone(), scope, kind, payload,
                        },
                        &mut (&mut fwd_buf).writer(),
                        bincode_cfg(),
                    ) {
                        Ok(_) => {
                            let fwd_data = fwd_buf.freeze();
                            match gossip_txs[shard].try_send((fwd_data, sender.id_hash())) {
                                Ok(()) => {}
                                Err(TrySendError::Full(_)) => {
                                    warn!("Gossip shard {} full, dropping signal forward from {}", shard, peer_addr);
                                }
                                Err(TrySendError::Closed(_)) => {
                                    error!("Gossip shard {} dead, signal will not propagate", shard);
                                }
                            }
                        }
                        Err(e) => warn!("Signal re-encode failed from {}: {}", peer_addr, e),
                    }
                }
            }

            WireMessage::Data(mut update) => {
                // Nonce was already checked and inserted by the early-dedup path above.
                if intern_keys { update.key = intern_key(update.key); }
                apply_and_notify(&store, &subscriptions, &update);

                // Clamp inbound TTL to config.default_ttl before forwarding.
                let fwd_ttl = update.ttl.min(max_ttl);
                if fwd_ttl > 1 {
                    let shard = shard_for_key(&update.key, n_shards);
                    // Decrement TTL in-place at TTL_OFFSET (fixed layout, v6 wire format).
                    // split().freeze() is O(1) when the backing store is unshared.
                    recv_buf[TTL_OFFSET] = fwd_ttl - 1;
                    let data = recv_buf.split().freeze();
                    match gossip_txs[shard].try_send((data, update.sender)) {
                        Ok(()) => {}
                        Err(TrySendError::Full(_)) => {
                            warn!("Gossip shard {} channel full, dropping forward from {}", shard, peer_addr);
                        }
                        Err(TrySendError::Closed(_)) => {
                            error!("Gossip shard {} is dead, dropping forward from {}", shard, peer_addr);
                        }
                    }
                }
            }
        }
    }
    Ok(())
}
