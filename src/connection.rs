use crate::error::GossipError;
use crate::framing::{
    bincode_cfg, bincode_cfg_prev, is_connection_closed,
    read_frame, shard_for_key, ForwardHint, FrameVersion, GossipUpdate,
    SyncEntry, WireMessage, WireMessageV7, ANTI_ENTROPY_NONCE, DATA_TAG, NONCE_OFFSET, TTL_OFFSET,
};
use crate::signal::{parse_own_grp_key, Boundary, Signal, SignalHandlers, SignalScope};
use crate::store::{apply_and_notify, intern_key, store_hash_acc, KvState};
use crate::node_id::NodeId;
use crate::seen::ShardedSeen;
use crate::writer::{get_or_spawn_writer, request_state, WriterEntry};
use bytes::{BufMut, Bytes, BytesMut};
use parking_lot::RwLock;
use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use tokio::{io::BufReader, net::TcpStream, sync::{mpsc, mpsc::error::TrySendError, watch}};
use tracing::{error, warn};

/// Shared state threaded into every inbound connection handler.
#[derive(Clone)]
pub(crate) struct ConnContext {
    pub(crate) node_id:          NodeId,
    pub(crate) peers:            Arc<papaya::HashMap<NodeId, Instant>>,
    /// One sender per gossip shard; carries pre-encoded frame bytes + sender id_hash + forward hint.
    /// The shard fans out bytes directly — no re-encoding per hop (zero-copy forwarding).
    pub(crate) gossip_txs:       Arc<[mpsc::Sender<(Bytes, u64, ForwardHint)>]>,
    pub(crate) seen:             Arc<ShardedSeen>,
    pub(crate) shutdown:         Arc<watch::Sender<bool>>,
    pub(crate) max_ttl:          u8,
    /// Unix-millisecond clock shared with the health monitor.
    pub(crate) current_ts:       Arc<AtomicU64>,
    pub(crate) peer_writers:     Arc<papaya::HashMap<NodeId, WriterEntry>>,
    pub(crate) writer_depth:     usize,
    pub(crate) backoff:          Duration,
    pub(crate) n_shards:         usize,
    pub(crate) intern_keys:      bool,
    pub(crate) intern_max_keys:  usize,
    pub(crate) signal_boundary:  Arc<RwLock<Boundary>>,
    pub(crate) signal_handlers:  Arc<SignalHandlers>,
    /// Cap on the peer table. Piggybacked peers are silently ignored once this
    /// is reached; bootstrap peers and direct senders are always admitted.
    pub(crate) max_peers:        usize,
    /// Idle timeout forwarded to `get_or_spawn_writer` / `request_state`.
    /// Zero means no timeout (default).
    pub(crate) writer_idle_timeout: Duration,
    /// Bundled KV-path state (store, subscriptions, prefix_index, hash_acc,
    /// dropped_frames, max_store_entries). Replaces five individual Arc fields.
    pub(crate) kv_state:         Arc<KvState>,
}

pub(crate) async fn handle_connection(
    socket: TcpStream,
    peer_addr: SocketAddr,
    ctx: ConnContext,
) -> Result<(), GossipError> {
    let ConnContext {
        node_id, peers, gossip_txs, seen, shutdown, max_ttl,
        current_ts, peer_writers, writer_depth, backoff, n_shards,
        intern_keys, intern_max_keys, signal_boundary, signal_handlers, max_peers,
        writer_idle_timeout, kv_state,
    } = ctx;
    let mut socket = BufReader::with_capacity(8_192, socket);
    let mut shutdown_rx = shutdown.subscribe();
    // BytesMut: recv_buf.split().freeze() at TTL_OFFSET is O(1) for zero-copy forwarding.
    let mut recv_buf: BytesMut = BytesMut::with_capacity(2_048);
    let node_id_str = node_id.to_string();

    loop {
        // read_frame returns FrameVersion so we can select the right decoder.
        // The never-type of break expressions coerces to FrameVersion, allowing
        // break directly inside the select! arms.
        let frame_version: FrameVersion = tokio::select! { biased;
            result = read_frame(&mut socket, &mut recv_buf) => match result {
                Ok(v)                              => v,
                Err(e) if is_connection_closed(&e) => break,
                Err(e) => { warn!("Read error from {}: {}", peer_addr, e); break; }
            },
            _ = shutdown_rx.wait_for(|v| *v) => break,
        };

        // Fast-path dedup: only valid for FrameVersion::Current where fixed field
        // offsets are known. NONCE_OFFSET=4 points at the u64 nonce in fixed-int
        // encoding for Data frames (tag DATA_TAG = [0,0,0,0] LE).
        // Under TTL=5, ~80% of inbound Data frames are duplicates; this saves
        // a full decode + two heap allocations (Arc<str> key, Bytes value) on
        // every duplicate. A malformed frame whose first 12 bytes look like a
        // valid Data header may poison that nonce in the seen-set; since nonces
        // are random u64s the collision probability is negligible (< 1 in 2^64).
        if frame_version == FrameVersion::Current
            && recv_buf.len() >= NONCE_OFFSET + 8
            && recv_buf[..4] == DATA_TAG
        {
            let nonce = u64::from_le_bytes(
                recv_buf[NONCE_OFFSET..NONCE_OFFSET + 8].try_into().unwrap(),
            );
            if seen.is_duplicate(nonce, current_ts.load(Ordering::Relaxed)) {
                continue;
            }
        }

        // Decode with the layout matching the sender's wire version.
        // Previous-version frames use WireMessageV7 to correctly handle the
        // missing `key_timestamps` field in StateRequest (bincode fixed-int cannot
        // decode a struct with missing trailing fields; WireMessageV7 has the
        // correct v7 layout and converts to WireMessage via From, filling
        // key_timestamps with vec![] — the "full snapshot" sentinel).
        let msg: WireMessage = if frame_version == FrameVersion::Current {
            match bincode::serde::decode_from_slice::<WireMessage, _>(&recv_buf, bincode_cfg()) {
                Ok((m, _)) => m,
                Err(e) => {
                    warn!("Malformed v8 message from {}: {}", peer_addr, e);
                    continue;
                }
            }
        } else {
            match bincode::serde::decode_from_slice::<WireMessageV7, _>(&recv_buf, bincode_cfg_prev()) {
                Ok((m, _)) => WireMessage::from(m),
                Err(e) => {
                    warn!("Malformed v7 message from {}: {}", peer_addr, e);
                    continue;
                }
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
                    let mut dropped_peers = 0usize;
                    for peer in known_peers {
                        if peer == node_id { continue; }
                        if guard.len() < max_peers {
                            guard.get_or_insert(peer, now);
                        } else {
                            dropped_peers += 1;
                        }
                    }
                    if dropped_peers > 0 {
                        warn!(
                            from = %sender, dropped = dropped_peers, max_peers,
                            "Ping: max_peers reached; dropped piggybacked peers"
                        );
                    }
                    is_new
                };
                if sender_is_new {
                    let key_timestamps: Vec<(std::sync::Arc<str>, u64)> = {
                        let guard = kv_state.store.pin();
                        guard.iter().map(|(k, v)| (k.clone(), v.timestamp)).collect()
                    };
                    request_state(&sender, &peer_writers, writer_depth, backoff, writer_idle_timeout, &shutdown, &node_id, &kv_state.hash_acc, &kv_state.dropped_frames, key_timestamps);
                }
            }

            WireMessage::StateRequest { sender, store_hash: their_hash, key_timestamps } => {
                // Trusted-domain check: sender must be a known peer. We do not verify that
                // peer_addr matches sender.to_socket_addr() — that would reject NAT'd topologies.
                // In the trusted domain a connected node could spoof the sender field; the
                // consequence is a StateResponse routed to another peer, which is harmless.
                if !peers.pin().contains_key(&sender) {
                    warn!("Ignoring StateRequest from unknown peer {} (reported as {})", peer_addr, sender);
                    continue;
                }
                // Anti-entropy fast-path: if the sender's store hash matches ours and is
                // non-zero (zero = "no digest" sentinel from v7 peers), send an empty
                // StateResponse to acknowledge we're alive without transferring entries.
                let my_hash = store_hash_acc(&kv_state.hash_acc);
                if their_hash != 0 && their_hash == my_hash {
                    let empty = WireMessage::StateResponse { entries: vec![] };
                    let mut fast_buf = BytesMut::new();
                    match bincode::serde::encode_into_std_write(
                        empty,
                        &mut (&mut fast_buf).writer(),
                        bincode_cfg(),
                    ) {
                        Ok(_) => {
                            let data: Bytes = fast_buf.freeze();
                            let tx = get_or_spawn_writer(&sender, &peer_writers, writer_depth, backoff, writer_idle_timeout, &shutdown, &kv_state.dropped_frames);
                            tokio::spawn(async move {
                                if tx.send(data).await.is_err() {
                                    tracing::error!("Fast-path StateResponse writer for {} has exited", sender);
                                }
                            });
                        }
                        Err(e) => warn!("Fast-path StateResponse serialize failed for {}: {}", sender, e),
                    }
                    continue;
                }
                // Delta sync (v8+): build a map of the sender's key→timestamp index.
                // If key_timestamps is empty (v7 peer or first contact), we do a full dump.
                // Otherwise, only send entries that the sender is missing or has stale.
                let entries: Vec<SyncEntry> = {
                    let guard = kv_state.store.pin();
                    if key_timestamps.is_empty() {
                        // Full dump: v7 peer or sender sent no digest.
                        guard.iter()
                            .map(|(k, v)| SyncEntry {
                                key:          k.clone(),
                                value:        v.data.clone().unwrap_or_default(),
                                timestamp:    v.timestamp,
                                is_tombstone: v.data.is_none(),
                            })
                            .collect()
                    } else {
                        // Delta: build their index, then emit only entries they're missing/stale.
                        let their_index: ahash::AHashMap<&str, u64> = key_timestamps.iter()
                            .map(|(k, ts)| (k.as_ref(), *ts))
                            .collect();
                        guard.iter()
                            .filter(|(k, v)| {
                                match their_index.get(k.as_ref()) {
                                    None => true,               // sender lacks this key entirely
                                    Some(&their_ts) => v.timestamp > their_ts, // ours is newer
                                }
                            })
                            .map(|(k, v)| SyncEntry {
                                key:          k.clone(),
                                value:        v.data.clone().unwrap_or_default(),
                                timestamp:    v.timestamp,
                                is_tombstone: v.data.is_none(),
                            })
                            .collect()
                    }
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
                        let tx = get_or_spawn_writer(&sender, &peer_writers, writer_depth, backoff, writer_idle_timeout, &shutdown, &kv_state.dropped_frames);
                        // Use send().await (not try_send) — StateResponse is a rare,
                        // join-time message. Dropping it causes permanent divergence
                        // because StateRequest is only sent on first contact; there is
                        // no automatic retry. Wrap in spawn so the connection handler
                        // is not blocked waiting for the writer to drain.
                        tokio::spawn(async move {
                            if tx.send(data).await.is_err() {
                                error!("StateResponse writer for {} has exited", sender);
                            }
                        });
                    }
                }
            }

            WireMessage::StateResponse { entries } => {
                for entry in entries {
                    // Intern keys from anti-entropy the same way as Data messages so
                    // both paths share the same Arc<str> allocation for the same key.
                    let key = if intern_keys { intern_key(entry.key, intern_max_keys) } else { entry.key };
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
                    apply_and_notify(&kv_state, &update);
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
                            let handler_fill = signal_handlers.fill_ratio(&kind);
                            let shard_fill: f32 = gossip_txs.iter()
                                .map(|tx| { let max = tx.max_capacity(); if max == 0 { 0.0_f32 } else { 1.0 - tx.capacity() as f32 / max as f32 } })
                                .fold(0.0_f32, f32::max);
                            let combined = handler_fill.max(shard_fill);
                            combined == 0.0 || fastrand::f32() >= combined
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
                        signal_handlers.record_quorum_evidence(
                            &kind, &sender, &kv_state, &node_id, max_ttl, &gossip_txs,
                        );
                    }
                }
                // Always forward — epidemic propagation regardless of scope.
                // Signal frames have variable-length scope so TTL cannot be decremented
                // in-place at a fixed offset (unlike Data frames). Full re-encode required.
                if ttl > 1 {
                    let hint = match &scope {
                        SignalScope::System           => ForwardHint::All,
                        SignalScope::Group(name)      => ForwardHint::Group(name.clone()),
                        SignalScope::Individual(peer) => ForwardHint::Individual(peer.clone()),
                    };
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
                            match gossip_txs[shard].try_send((fwd_data, sender.id_hash(), hint)) {
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
                // Nonce was already checked and inserted by the early-dedup path above
                // for FrameVersion::Current. For FrameVersion::Previous, check now.
                if frame_version == FrameVersion::Previous {
                    let ts = current_ts.load(Ordering::Relaxed);
                    if seen.is_duplicate(update.nonce, ts) {
                        continue;
                    }
                }
                if intern_keys { update.key = intern_key(update.key, intern_max_keys); }
                apply_and_notify(&kv_state, &update);

                // Push-based boundary sync: if the received key is grp/{group}/{this_node},
                // update the boundary immediately rather than waiting for the GC-tick reconcile.
                if let Some(group_str) = parse_own_grp_key(&update.key, &node_id_str) {
                    let mut bnd = signal_boundary.write();
                    if update.is_tombstone {
                        bnd.groups.remove(group_str);
                    } else {
                        bnd.groups.insert(Arc::from(group_str));
                    }
                }

                // Clamp inbound TTL to config.default_ttl before forwarding.
                let fwd_ttl = update.ttl.min(max_ttl);
                if fwd_ttl > 1 {
                    let shard = shard_for_key(&update.key, n_shards);
                    if frame_version == FrameVersion::Current {
                        // Zero-copy forward: TTL decremented in-place at TTL_OFFSET
                        // (fixed layout, v6 wire format). split().freeze() is O(1).
                        recv_buf[TTL_OFFSET] = fwd_ttl - 1;
                        let data = recv_buf.split().freeze();
                        match gossip_txs[shard].try_send((data, update.sender, ForwardHint::All)) {
                            Ok(()) => {}
                            Err(TrySendError::Full(_)) => {
                                warn!("Gossip shard {} channel full, dropping forward from {}", shard, peer_addr);
                            }
                            Err(TrySendError::Closed(_)) => {
                                error!("Gossip shard {} is dead, dropping forward from {}", shard, peer_addr);
                            }
                        }
                    } else {
                        // Previous-version frame: field layout differs from v6, so
                        // zero-copy is unsafe. Re-encode at WIRE_VERSION for forwarding.
                        let fwd_update = GossipUpdate { ttl: fwd_ttl - 1, ..update.clone() };
                        let mut fwd_buf = BytesMut::with_capacity(256);
                        match bincode::serde::encode_into_std_write(
                            WireMessage::Data(fwd_update),
                            &mut (&mut fwd_buf).writer(),
                            bincode_cfg(),
                        ) {
                            Ok(_) => {
                                match gossip_txs[shard].try_send((fwd_buf.freeze(), update.sender, ForwardHint::All)) {
                                    Ok(()) => {}
                                    Err(TrySendError::Full(_)) => {
                                        warn!("Gossip shard {} channel full, dropping v{} forward from {}",
                                            shard, crate::framing::PREV_WIRE_VERSION, peer_addr);
                                    }
                                    Err(TrySendError::Closed(_)) => {
                                        error!("Gossip shard {} dead, dropping v{} forward from {}",
                                            shard, crate::framing::PREV_WIRE_VERSION, peer_addr);
                                    }
                                }
                            }
                            Err(e) => warn!("Re-encode of v{} Data frame failed: {}",
                                crate::framing::PREV_WIRE_VERSION, e),
                        }
                    }
                }
            }
        }
    }
    Ok(())
}
