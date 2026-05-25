use crate::agent::TaskCtx;
use crate::error::GossipError;
use crate::framing::{
    bincode_cfg, dispatch_gossip_try_send, is_connection_closed,
    make_gossip_update, read_frame, shard_for_key, sync_entry_from, ForwardHint, FrameVersion,
    GossipUpdate, SyncEntry, WireMessage, WireMessageV7, ANTI_ENTROPY_NONCE, DATA_TAG,
    NONCE_OFFSET, TTL_OFFSET,
};
#[cfg(feature = "tls")]
use crate::framing::canonical_sign_bytes;
use crate::signal::{parse_own_grp_key, Signal, SignalScope, signal_kind};
use crate::store::{apply_and_notify, intern_key, store_hash_acc};
use crate::node_id::NodeId;
use crate::writer::{get_or_spawn_writer, request_state, WriterEntry};
use bytes::{BufMut, Bytes, BytesMut};
use std::{
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};
use crate::stream::GossipStream;
use tokio::{io::BufReader, sync::{mpsc::error::TrySendError, watch}};
use tracing::{error, warn};

/// Shared state threaded into every inbound connection handler.
#[derive(Clone)]
pub(crate) struct ConnContext {
    /// Shared infrastructure bundle (node_id, gossip_txs, seen, hlc,
    /// signal_boundary, signal_handlers, kv_state, wal, default_ttl).
    pub(crate) task_ctx:            Arc<TaskCtx>,
    pub(crate) peers:               Arc<papaya::HashMap<NodeId, Instant>>,
    pub(crate) shutdown:            Arc<watch::Sender<bool>>,
    pub(crate) peer_writers:        Arc<papaya::HashMap<NodeId, WriterEntry>>,
    pub(crate) writer_depth:        usize,
    pub(crate) backoff:             Duration,
    pub(crate) n_shards:            usize,
    pub(crate) intern_keys:         bool,
    pub(crate) intern_max_keys:     usize,
    /// Cap on the peer table. Piggybacked peers are silently ignored once this
    /// is reached; bootstrap peers and direct senders are always admitted.
    pub(crate) max_peers:           usize,
    /// Idle timeout forwarded to `get_or_spawn_writer` / `request_state`.
    /// Zero means no timeout (default).
    pub(crate) writer_idle_timeout: Duration,
}

pub(crate) async fn handle_connection(
    socket: GossipStream,
    peer_addr: SocketAddr,
    ctx: ConnContext,
) -> Result<(), GossipError> {
    let ConnContext {
        task_ctx, peers, shutdown, peer_writers, writer_depth, backoff, n_shards,
        intern_keys, intern_max_keys, max_peers, writer_idle_timeout,
    } = ctx;
    let node_id         = task_ctx.node_id.clone();
    let gossip_txs      = task_ctx.gossip_txs.clone();
    let seen            = task_ctx.seen.clone();
    let max_ttl         = task_ctx.default_ttl;
    let hlc             = task_ctx.hlc.clone();
    let signal_boundary = task_ctx.signal_boundary.clone();
    let signal_handlers = task_ctx.signal_handlers.clone();
    let kv_state        = task_ctx.kv_state.clone();
    let wal             = task_ctx.wal.get().cloned();
    let tls             = task_ctx.tls.get().cloned();
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
        //
        // Non-Data variants fall through with no logging — that's intentional.
        // Signal, Ping, StateRequest, and StateResponse have variable-length
        // payloads ahead of any nonce, so we let the full decoder below handle
        // their dedup and dispatch. Logging here would be noisy on every Ping.
        if frame_version == FrameVersion::Current
            && recv_buf.len() >= NONCE_OFFSET + 8
            && recv_buf[..4] == DATA_TAG
        {
            let nonce = u64::from_le_bytes(
                recv_buf[NONCE_OFFSET..NONCE_OFFSET + 8].try_into().unwrap(),
            );
            // Seen-set TTL eviction is keyed by physical milliseconds — extract
            // the high 48 bits of the packed HLC so the "age" math the seen-set
            // does internally still maps to real time.
            if seen.mark_and_check(nonce, crate::hlc::physical_ms(hlc.current())) {
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
            // v7 encoding config is identical to v8; only the struct layout differs
            // (handled by WireMessageV7, which converts to WireMessage via From).
            match bincode::serde::decode_from_slice::<WireMessageV7, _>(&recv_buf, bincode_cfg()) {
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
                    request_state(&sender, &peer_writers, writer_depth, backoff, writer_idle_timeout, &shutdown, &node_id, &kv_state.hash_acc, &kv_state.dropped_frames, key_timestamps, tls.clone());
                }
            }

            WireMessage::StateRequest { sender, store_hash: their_hash, mut key_timestamps } => {
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
                            if let Some(tx) = get_or_spawn_writer(&sender, &peer_writers, writer_depth, backoff, writer_idle_timeout, &shutdown, &kv_state.dropped_frames, tls.clone()) {
                                tokio::spawn(async move {
                                    if tx.send(data).await.is_err() {
                                        tracing::error!("Fast-path StateResponse writer for {} has exited", sender);
                                    }
                                });
                            }
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
                        // Delta: sort their index once, then binary-search per local key.
                        // O(N log N) sort + O(M log N) lookups vs O(N) map build + O(M) lookups;
                        // avoids an O(N) heap allocation for the map.
                        key_timestamps.sort_unstable_by(|a, b| a.0.as_ref().cmp(b.0.as_ref()));
                        guard.iter()
                            .filter(|(k, v)| {
                                match key_timestamps.binary_search_by(|(kk, _)| kk.as_ref().cmp(k.as_ref())) {
                                    Err(_) => true,                              // sender lacks this key
                                    Ok(i) => v.timestamp > key_timestamps[i].1, // ours is newer
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
                        // Use send().await (not try_send) — StateResponse is a rare,
                        // join-time message. Dropping it causes permanent divergence
                        // because StateRequest is only sent on first contact; there is
                        // no automatic retry. Wrap in spawn so the connection handler
                        // is not blocked waiting for the writer to drain.
                        if let Some(tx) = get_or_spawn_writer(&sender, &peer_writers, writer_depth, backoff, writer_idle_timeout, &shutdown, &kv_state.dropped_frames, tls.clone()) {
                            tokio::spawn(async move {
                                if tx.send(data).await.is_err() {
                                    error!("StateResponse writer for {} has exited", sender);
                                }
                            });
                        }
                    }
                }
            }

            WireMessage::StateResponse { entries } => {
                for entry in entries {
                    // Absorb the remote HLC stamp so our clock dominates anything
                    // anti-entropy hands us, even on a fresh restart where the
                    // local clock is otherwise far behind any prior cluster state.
                    hlc.observe(entry.timestamp);
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
                    if let Some(ref wal) = wal {
                        let _ = wal.append(sync_entry_from(&update)).await;
                    }
                    apply_and_notify(&kv_state, &update);
                }
                #[cfg(feature = "metrics")]
                metrics::counter!("gossip_anti_entropy_rounds_total").increment(1);
            }

            WireMessage::Signal { ttl, nonce, sender, scope, kind, payload } => {
                let ts = crate::hlc::physical_ms(hlc.current());
                if seen.mark_and_check(nonce, ts) {
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
                            let combined = handler_fill.max(crate::framing::gossip_shard_fill(&gossip_txs));
                            combined == 0.0 || fastrand::f32() >= combined
                        }
                    };
                    if admit {
                        // O(1) fast-path for correlated rpc.result / bulk.result:
                        // if the correlation nonce is registered in rpc_pending,
                        // fire the oneshot and skip the signal_handlers fan-out.
                        let nonce_claimed = if payload.len() >= 8
                            && (kind.as_ref() == signal_kind::RPC_RESULT
                                || kind.as_ref() == signal_kind::BULK_RESULT)
                        {
                            let call_nonce = u64::from_le_bytes(
                                payload[..8].try_into().unwrap(),
                            );
                            if let Some(tx) = task_ctx.rpc_pending.lock().unwrap().remove(&call_nonce) {
                                let _ = tx.send(Signal {
                                    kind: kind.clone(),
                                    scope: scope.clone(),
                                    payload: payload.clone(),
                                    sender: sender.clone(),
                                    nonce,
                                });
                                true
                            } else {
                                false
                            }
                        } else {
                            false
                        };
                        if !nonce_claimed {
                            signal_handlers.deliver(&Signal {
                                kind: kind.clone(),
                                scope: scope.clone(),
                                payload: payload.clone(),
                                sender: sender.clone(),
                                nonce,
                            });
                        }
                        // Quorum evidence: write sys/quorum/{kind}/{sender} to Layer I.
                        // Rate-limited by quorum_evidence_payload — skips write if entry
                        // is less than 1 s old. The write and gossip dispatch are done
                        // here rather than inside SignalHandlers to keep transport and
                        // KV-write concerns out of the Layer II type.
                        if let Some((q_key, q_val)) = signal_handlers.quorum_evidence_payload(
                            &kind, &sender,
                        ) {
                            let upd = make_gossip_update(&node_id, max_ttl, q_key, q_val, false, &hlc);
                            if let Some(ref wal) = wal {
                                let _ = wal.append(sync_entry_from(&upd)).await;
                            }
                            apply_and_notify(&kv_state, &upd);
                            dispatch_gossip_try_send(
                                &gossip_txs, WireMessage::Data(upd),
                                node_id.id_hash(), ForwardHint::All, &kv_state.dropped_frames,
                            );
                        }
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
                    let ts = crate::hlc::physical_ms(hlc.current());
                    if seen.mark_and_check(update.nonce, ts) {
                        continue;
                    }
                }
                // Absorb the remote HLC stamp so any locally-originated update we
                // emit afterwards strictly dominates this one — preserves causal
                // happens-before across the cluster even under wall-clock skew.
                hlc.observe(update.timestamp);
                if intern_keys { update.key = intern_key(update.key, intern_max_keys); }
                if let Some(ref wal) = wal {
                    let _ = wal.append(sync_entry_from(&update)).await;
                }
                apply_and_notify(&kv_state, &update);
                #[cfg(feature = "metrics")]
                metrics::counter!("gossip_messages_received_total").increment(1);

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

            WireMessage::SignedData { mut update, signer, signature } => {
                // Dedup by nonce (no early fast-path — SignedData has a non-zero variant tag).
                let ts = crate::hlc::physical_ms(hlc.current());
                if seen.mark_and_check(update.nonce, ts) {
                    continue;
                }

                // Signature verification — fail-open: accept frames whose signer's public
                // key is not yet in peer_keys (key hasn't gossiped yet).
                #[cfg(feature = "tls")]
                {
                    let guard = task_ctx.peer_keys.pin();
                    if let Some((_, pub_bytes)) = guard.iter().find(|(k, _)| k.id_hash() == signer) {
                        let canonical = canonical_sign_bytes(&update);
                        let (lo, hi)  = &signature;
                        let mut sig_bytes = [0u8; 64];
                        sig_bytes[..32].copy_from_slice(lo);
                        sig_bytes[32..].copy_from_slice(hi);
                        if !crate::tls::verify_bytes(pub_bytes, &canonical, &sig_bytes) {
                            warn!(
                                "SignedData from {} (signer={:#x}) failed Ed25519 verification, dropping",
                                peer_addr, signer
                            );
                            continue;
                        }
                    }
                    // Unknown signer → fail-open, proceed to apply.
                }

                // Absorb HLC and apply to local store.
                hlc.observe(update.timestamp);
                if intern_keys { update.key = intern_key(update.key, intern_max_keys); }
                if let Some(ref wal) = wal {
                    let _ = wal.append(sync_entry_from(&update)).await;
                }
                apply_and_notify(&kv_state, &update);
                #[cfg(feature = "metrics")]
                metrics::counter!("gossip_messages_received_total").increment(1);

                // Forward with TTL-1, preserving the originator's signature.
                // TTL is excluded from the signed bytes so the signature is still valid
                // after decrement. Re-encode (no zero-copy — no fixed TTL_OFFSET for SignedData).
                let fwd_ttl = update.ttl.min(max_ttl);
                if fwd_ttl > 1 {
                    let shard = shard_for_key(&update.key, n_shards);
                    let mut fwd_buf = BytesMut::with_capacity(256);
                    let fwd_msg = WireMessage::SignedData {
                        update: GossipUpdate { ttl: fwd_ttl - 1, ..update.clone() },
                        signer,
                        signature,
                    };
                    match bincode::serde::encode_into_std_write(
                        fwd_msg, &mut (&mut fwd_buf).writer(), bincode_cfg(),
                    ) {
                        Ok(_) => {
                            match gossip_txs[shard].try_send((fwd_buf.freeze(), update.sender, ForwardHint::All)) {
                                Ok(()) => {}
                                Err(TrySendError::Full(_)) => {
                                    warn!("Gossip shard {} full, dropping SignedData forward from {}", shard, peer_addr);
                                }
                                Err(TrySendError::Closed(_)) => {
                                    error!("Gossip shard {} dead, dropping SignedData forward from {}", shard, peer_addr);
                                }
                            }
                        }
                        Err(e) => warn!("SignedData re-encode failed from {}: {}", peer_addr, e),
                    }
                }
            }
        }
    }
    Ok(())
}
