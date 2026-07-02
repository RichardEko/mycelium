use crate::context::CoreCtx;
use crate::error::GossipError;
use crate::framing::{
    dispatch_gossip_try_send, is_connection_closed,
    make_gossip_update, read_frame, shard_for_key, sync_entry_from, ForwardHint, FrameVersion,
    GossipUpdate, SyncEntry, WireMessage, ANTI_ENTROPY_NONCE, DATA_TAG,
    NONCE_OFFSET, TTL_OFFSET,
};
#[cfg(feature = "tls")]
use crate::framing::canonical_sign_bytes;
use crate::signal::{parse_own_grp_key, Signal, SignalScope};
use crate::store::{apply_and_notify, intern_key, store_hash_acc};
use crate::node_id::NodeId;
use crate::writer::{get_or_spawn_writer, request_state, WriterEntry};
use bytes::{Bytes, BytesMut};
use std::{
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};
use crate::stream::GossipStream;
use tokio::{io::BufReader, sync::{mpsc::error::TrySendError, watch}};
use tracing::{error, warn};

/// `sys/` sub-prefixes whose node-id segment immediately follows the prefix.
/// Only the named node should ever originate writes to these keys; a remote
/// write naming *this* node is a namespace-ownership violation. `sys/quorum/`
/// is deliberately excluded — peers legitimately write quorum evidence naming
/// the node they observed.
const SELF_OWNED_SYS_PREFIXES: [&str; 4] =
    ["sys/identity/", "sys/load/", "sys/role/", "sys/tuple/"];

/// `sys/` namespace-ownership tripwire — **detection, not prevention**.
///
/// Called on each *inbound* (remote) update. If the key targets a `sys/`
/// namespace owned by this node (the node-id segment equals `self_node`), a
/// peer is clobbering a key only we should ever write: bump the diagnostic
/// counter and `warn!`. The write itself is left to LWW — Layer I never learns
/// the `sys/` ownership convention (that would invert the layer dependency, as
/// the consensus commit-conflict tripwire comment explains). Signed keys
/// (`identity`, `role`) additionally fail signature verification at read;
/// unsigned keys (`load`, `tuple`) rely on this signal alone.
fn flag_foreign_sys_write(
    key: &str,
    self_node: &str,
    counter: &std::sync::atomic::AtomicU64,
) {
    for prefix in SELF_OWNED_SYS_PREFIXES {
        if let Some(rest) = key.strip_prefix(prefix) {
            if rest.split('/').next() == Some(self_node) {
                counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                warn!(
                    key = %key,
                    "sys/ namespace violation: inbound remote write to a self-owned \
                     sys/ key; applying per LWW but flagging \
                     (see SystemStats::sys_namespace_violations)"
                );
            }
            return;
        }
    }
}

/// Shared state threaded into every inbound connection handler.
#[derive(Clone)]
pub struct ConnContext {
    /// Shared Layers I+II context (node_id, gossip_txs, seen, hlc,
    /// signal_boundary, signal_handlers, kv_state, wal, default_ttl, …).
    /// The upper crate passes `Arc::clone(&task_ctx.core)`.
    pub task_ctx:            Arc<CoreCtx>,
    pub peers:               Arc<papaya::HashMap<NodeId, Instant>>,
    pub shutdown:            Arc<watch::Sender<bool>>,
    pub peer_writers:        Arc<papaya::HashMap<NodeId, WriterEntry>>,
    pub backoff:             Duration,
    pub n_shards:            usize,
    pub intern_keys:         bool,
    pub intern_max_keys:     usize,
    /// Cap on the peer table. Piggybacked peers are silently ignored once this
    /// is reached; bootstrap peers and direct senders are always admitted.
    pub max_peers:           usize,
    /// Idle timeout forwarded to `get_or_spawn_writer` / `request_state`.
    /// Zero means no timeout (default).
    pub writer_idle_timeout: Duration,
    /// Fan-out list publisher (same channel the health monitor feeds). A peer
    /// learned here must become sendable IMMEDIATELY: waiting for the health
    /// monitor's next tick left inbound-only nodes mute for live sends —
    /// including Individual-scoped RPC responses — for up to two
    /// health-check intervals.
    pub peer_list_tx: tokio::sync::watch::Sender<Arc<[NodeId]>>,
}

pub async fn handle_connection(
    socket: GossipStream,
    peer_addr: SocketAddr,
    ctx: ConnContext,
) -> Result<(), GossipError> {
    let ConnContext {
        task_ctx, peers, shutdown, peer_writers, backoff, n_shards,
        intern_keys, intern_max_keys, max_peers, writer_idle_timeout, peer_list_tx,
    } = ctx;
    let node_id         = task_ctx.node_id.clone();
    let gossip_txs      = Arc::clone(&task_ctx.gossip_txs);
    let seen            = Arc::clone(&task_ctx.seen);
    let max_ttl         = task_ctx.default_ttl;
    let hlc             = Arc::clone(&task_ctx.hlc);
    let signal_boundary = Arc::clone(&task_ctx.signal_boundary);
    let signal_handlers = Arc::clone(&task_ctx.signal_handlers);
    let kv_state        = Arc::clone(&task_ctx.kv_state);
    let sys_violations  = Arc::clone(&task_ctx.sys_namespace_violations);
    let wal             = task_ctx.wal.get().cloned();
    let tls             = task_ctx.tls.get().cloned();
    let mut socket = BufReader::with_capacity(8_192, socket);
    let mut shutdown_rx = shutdown.subscribe();
    // BytesMut: recv_buf.split().freeze() at TTL_OFFSET is O(1) for zero-copy forwarding.
    let mut recv_buf: BytesMut = BytesMut::with_capacity(2_048);
    let node_id_str = node_id.to_string();
    // Per-connection anti-entropy rate limit. StateRequest triggers an O(store_size) scan;
    // processing repeated requests from the same peer on the same connection faster than
    // the health-check interval provides no convergence benefit and adds CPU/alloc pressure.
    //
    // Set to interval - 1 so the health monitor's first tick (at t = startup_jitter +
    // interval_secs) arrives at least 1 s after this cooldown expires. Without the gap, the
    // startup StateRequest and the first-tick retry land at the same wall-clock instant,
    // creating a timing race where the retry is spuriously blocked by the cooldown.
    let anti_entropy_cooldown = Duration::from_secs(
        task_ctx.config.health_check_interval_secs.saturating_sub(1).max(1)
    );
    let mut last_state_sent: Option<std::time::Instant> = None;
    // Per-connection inbound rate limiter. Resets every second; 0 = unlimited.
    // The limit is read from the hot cell each frame (WS-C M9) so it can be retuned live.
    let mut rate_window_start = std::time::Instant::now();
    let mut rate_frame_count: u64 = 0;
    // M7 (WS-C) distributed rate-limiting: the immediate peer's identity (the sender key) + its
    // locally-decided throttle budget, refreshed once per window. Inert unless `rate_observation`.
    let m7_enabled = task_ctx.config.rate_observation_enabled;
    let peer_key: std::sync::Arc<str> = std::sync::Arc::from(peer_addr.to_string());
    let mut sender_throttle: u64 = 0;

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

        // Inbound rate limiting: drop frames from a flooding peer. The effective limit is the
        // tightest of the global per-peer limit (`max_inbound_frames_per_sec`, M9-hot) and the M7
        // distributed throttle (a fair-share budget decided from cluster-wide aggregate evidence).
        let global_limit = task_ctx.hot.inbound_fps();
        if global_limit > 0 || m7_enabled {
            let elapsed = rate_window_start.elapsed();
            if elapsed >= Duration::from_secs(1) {
                // Window rollover: publish this peer's observed rate as shared M7 evidence, then
                // refresh its locally-decided throttle budget for the new window.
                if m7_enabled {
                    if rate_frame_count > 0 {
                        crate::rate::publish_observation(&task_ctx, &peer_key, rate_frame_count);
                    }
                    sender_throttle = crate::rate::throttle_for(&task_ctx, &peer_key);
                }
                rate_frame_count = 0;
                rate_window_start = std::time::Instant::now();
            }
            rate_frame_count += 1;
            let effective = match (global_limit, sender_throttle) {
                (0, t) => t,
                (g, 0) => g,
                (g, t) => g.min(t),
            };
            if effective > 0 && rate_frame_count > effective {
                warn!(
                    from = %peer_addr,
                    fps  = rate_frame_count,
                    limit = effective,
                    "inbound rate limit exceeded; dropping frame"
                );
                continue;
            }
        }

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
                recv_buf[NONCE_OFFSET..NONCE_OFFSET + 8].try_into()
                    .expect("NONCE_OFFSET..NONCE_OFFSET+8 is always 8 bytes; length checked above"),
            );
            // Seen-set TTL eviction is keyed by physical milliseconds — extract
            // the high 48 bits of the packed HLC so the "age" math the seen-set
            // does internally still maps to real time.
            if seen.mark_and_check(nonce, crate::hlc::physical_ms(hlc.current())) {
                continue;
            }
        }

        // Decode with the layout matching the sender's wire version.
        // Previous-version (v10) frames use WireMessageV10 to handle the missing
        // `hlc_seq` field in Signal. The struct layout is otherwise identical;
        // the From conversion fills hlc_seq = None (unordered delivery).
        let msg: WireMessage = if frame_version == FrameVersion::Current {
            match crate::codec::decode_wire(&recv_buf) {
                Ok(m) => m,
                Err(e) => {
                    warn!("Malformed v11 message from {}: {}", peer_addr, e);
                    continue;
                }
            }
        } else {
            match crate::codec::decode_wire_v11(&recv_buf) {
                Ok(m) => m,
                Err(e) => {
                    warn!("Malformed v11 message from {}: {}", peer_addr, e);
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
                    // Event-driven fan-out activation (WS-B M4: bounded). Add the new peer
                    // to the *current* active forwarding set so it is immediately sendable
                    // (signals, RPC responses, votes) — but only while the set is below the
                    // resolved fan-out `k`, so this never un-bounds it. Appending to the
                    // current published set (rather than republishing the full peer map)
                    // keeps it consistent with the health monitor's sticky set; once the set
                    // is at `k`, new peers are reached via multi-hop flooding instead. The
                    // health monitor remains the steady-state reconciler/evictor.
                    let known_len = peers.pin().len();
                    let k = crate::config::resolved_fanout(
                        task_ctx.config.gossip_fanout, task_ctx.config.max_active_connections, known_len);
                    let current = peer_list_tx.borrow().clone();
                    if !current.contains(&sender) && current.len() < k.max(1) {
                        let mut next: Vec<NodeId> = current.to_vec();
                        next.push(sender.clone());
                        let _ = peer_list_tx.send(next.into());
                    }
                    let bucket_hashes = crate::store::store_bucket_hashes(&kv_state);
                    request_state(&sender, &peer_writers, task_ctx.hot.writer_depth(), backoff, writer_idle_timeout, &shutdown, &node_id, &kv_state.hash_acc, &kv_state.dropped_frames, bucket_hashes, tls.clone());
                }
            }

            WireMessage::StateRequest { sender, store_hash: their_hash, bucket_hashes: their_buckets } => {
                // Trusted-domain check: sender must be a known peer. We do not verify that
                // peer_addr matches sender.to_socket_addr() — that would reject NAT'd topologies.
                // In the trusted domain a connected node could spoof the sender field; the
                // consequence is a StateResponse routed to another peer, which is harmless.
                if !peers.pin().contains_key(&sender) {
                    warn!("Ignoring StateRequest from unknown peer {} (reported as {})", peer_addr, sender);
                    continue;
                }
                // Rate-limit: one full anti-entropy scan per health-check interval per connection.
                // A reconnecting peer gets a fresh connection and a fresh cooldown window.
                if last_state_sent.is_some_and(|t| t.elapsed() < anti_entropy_cooldown) {
                    tracing::debug!(
                        "Anti-entropy cooldown active for {}; ignoring repeated StateRequest",
                        sender
                    );
                    continue;
                }
                // Anti-entropy fast-path: if the sender's store hash matches ours and is
                // non-zero (zero = "no digest" sentinel from v7 peers), send an empty
                // StateResponse to acknowledge we're alive without transferring entries.
                let my_hash = store_hash_acc(&kv_state.hash_acc);
                if their_hash != 0 && their_hash == my_hash {
                    let data: Bytes = crate::codec::wire_to_bytes(
                        &WireMessage::StateResponse { entries: vec![] });
                    if let Some(tx) = get_or_spawn_writer(&sender, &peer_writers, task_ctx.hot.writer_depth(), backoff, writer_idle_timeout, &shutdown, &kv_state.dropped_frames, tls.clone()) {
                        tokio::spawn(async move {
                            if tx.send(data).await.is_err() {
                                tracing::error!("Fast-path StateResponse writer for {} has exited", sender);
                            }
                        });
                    }
                    last_state_sent = Some(std::time::Instant::now());
                    continue;
                }
                // Merkle delta sync (v12): the sender's `bucket_hashes` is its per-bucket
                // digest of its live store. We send every entry (incl. tombstones, so
                // deletes propagate) that falls in a bucket whose hash differs from ours,
                // plus a full dump when the digest is absent (no-digest sentinel: empty
                // store, first contact, or a v11 peer downgraded via the shim) or malformed.
                // The sender applies LWW, so sending a whole divergent bucket is safe —
                // entries it already has at an equal/newer timestamp are no-ops.
                let full_dump = their_buckets.len() != crate::store::ANTI_ENTROPY_BUCKETS;
                let entries: Vec<SyncEntry> = {
                    let guard = kv_state.store.pin();
                    if full_dump {
                        guard.iter()
                            .map(|(k, v)| SyncEntry {
                                key:          Arc::clone(k),
                                value:        v.data.clone().unwrap_or_default(),
                                timestamp:    v.timestamp,
                                is_tombstone: v.data.is_none(),
                            })
                            .collect()
                    } else {
                        let my_buckets = crate::store::store_bucket_hashes(&kv_state);
                        guard.iter()
                            .filter(|(k, _)| {
                                let b = crate::store::bucket_for_key(k);
                                my_buckets[b] != their_buckets[b] // bucket diverges
                            })
                            .map(|(k, v)| SyncEntry {
                                key:          Arc::clone(k),
                                value:        v.data.clone().unwrap_or_default(),
                                timestamp:    v.timestamp,
                                is_tombstone: v.data.is_none(),
                            })
                            .collect()
                    }
                };
                // Chunk the response so the total divergent set is never bounded by one
                // frame: each chunk stays under a conservative byte budget and the sender
                // applies chunks independently (entries bypass the seen-set, TTL = 1), so
                // N frames are semantically identical to one. Pre-2026-07-02 this was a
                // single frame and a >MAX_FRAME_BYTES store *silently could not bootstrap
                // a late joiner* (skipped with a warn, never retried). An individual entry
                // that alone exceeds the budget is skipped with a warn naming the key —
                // `kv_set` now rejects such writes, so only a legacy store can hold one —
                // and the rest of the sync still proceeds.
                let chunk_budget = crate::framing::MAX_KV_WRITE_BYTES;
                let mut chunks: Vec<Vec<SyncEntry>> = Vec::new();
                let mut chunk: Vec<SyncEntry> = Vec::new();
                let mut chunk_bytes = 0usize;
                for e in entries {
                    // Conservative per-entry envelope: key/value length prefixes,
                    // timestamp, tombstone flag (~25 B actual).
                    let e_bytes = e.key.len() + e.value.len() + 64;
                    if e_bytes > chunk_budget {
                        warn!(
                            "skipping anti-entropy for oversized entry '{}' ({} B > {} B budget) \
                             — entry cannot fit a gossip frame; peers will never receive it",
                            e.key, e.value.len(), chunk_budget,
                        );
                        continue;
                    }
                    if chunk_bytes + e_bytes > chunk_budget {
                        chunks.push(std::mem::take(&mut chunk));
                        chunk_bytes = 0;
                    }
                    chunk.push(e);
                    chunk_bytes += e_bytes;
                }
                // Always send the final chunk, even when empty: an empty StateResponse
                // doubles as the liveness ack (same as the fast-path above).
                chunks.push(chunk);
                let frames: Vec<Bytes> = chunks.into_iter()
                    .map(|entries| crate::codec::wire_to_bytes(&WireMessage::StateResponse { entries }))
                    .collect();
                // Use send().await (not try_send) — StateResponse is a rare,
                // join-time message. Dropping it causes permanent divergence
                // because StateRequest is only sent on first contact; there is
                // no automatic retry. Wrap in spawn so the connection handler
                // is not blocked waiting for the writer to drain.
                if let Some(tx) = get_or_spawn_writer(&sender, &peer_writers, task_ctx.hot.writer_depth(), backoff, writer_idle_timeout, &shutdown, &kv_state.dropped_frames, tls.clone()) {
                    tokio::spawn(async move {
                        for data in frames {
                            if tx.send(data).await.is_err() {
                                error!("StateResponse writer for {} has exited", sender);
                                break;
                            }
                        }
                    });
                }
                last_state_sent = Some(std::time::Instant::now());
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
                    flag_foreign_sys_write(&update.key, &node_id_str, &sys_violations);
                    apply_and_notify(&kv_state, &update);
                }
                #[cfg(feature = "metrics")]
                metrics::counter!("gossip_anti_entropy_rounds_total").increment(1);
            }

            WireMessage::Signal { ttl, nonce, sender, scope, kind, payload, hlc_seq } => {
                let ts = crate::hlc::physical_ms(hlc.current());
                if seen.mark_and_check(nonce, ts) {
                    continue;
                }
                // Advance HLC on ordered signals so local writes after this
                // observation carry a strictly greater timestamp.
                if let Some(seq) = hlc_seq {
                    hlc.observe(seq);
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
                        let raw_signal = Signal {
                            kind: Arc::clone(&kind), scope: scope.clone(),
                            payload: payload.clone(), sender: sender.clone(), nonce,
                        };
                        // When ordered delivery is enabled and the signal carries an
                        // hlc_seq, route through the reorder buffer; otherwise deliver
                        // directly. Unordered signals (hlc_seq = None) always bypass.
                        let signals_to_deliver: Vec<Signal> =
                            if let (Some(seq), Some(rbuf)) = (hlc_seq, &task_ctx.reorder_buf) {
                                // Drain any stale entries before ingesting the new one.
                                let mut buf = rbuf.lock().unwrap_or_else(|e| e.into_inner());
                                let mut out = buf.flush_expired();
                                out.extend(buf.ingest(seq, raw_signal));
                                out
                            } else {
                                vec![raw_signal]
                            };

                        for sig in signals_to_deliver {
                        // Opt-in O(1) fast-path: the upper service layer may register a
                        // reply interceptor (`CoreCtx::reply_interceptor`) that claims
                        // correlated rpc.result / bulk.result signals — firing the waiting
                        // oneshot — and returns true to skip the signal_handlers fan-out.
                        // Core stays RPC-agnostic: it only asks "did anything claim this?".
                        let nonce_claimed = match task_ctx.reply_interceptor.as_ref() {
                            Some(claim) => claim(&sig),
                            None => false,
                        };
                        if !nonce_claimed {
                            signal_handlers.deliver(&sig);
                        }
                        // Quorum evidence: write sys/quorum/{kind}/{sender} to Layer I.
                        // Rate-limited by quorum_evidence_payload — skips write if entry
                        // is less than 1 s old. The write and gossip dispatch are done
                        // here rather than inside SignalHandlers to keep transport and
                        // KV-write concerns out of the Layer II type.
                        if let Some((q_key, q_val)) = signal_handlers.quorum_evidence_payload(
                            &sig.kind, &sig.sender,
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
                        } // end for sig in signals_to_deliver
                    }
                }
                // Always forward — epidemic propagation regardless of scope.
                // Signal frames have variable-length scope so TTL cannot be decremented
                // in-place at a fixed offset (unlike Data frames). Full re-encode required.
                if ttl > 1 {
                    let hint = match &scope {
                        SignalScope::System             => ForwardHint::All,
                        SignalScope::Group(name)        => ForwardHint::Group(Arc::clone(name)),
                        SignalScope::Individual(peer)   => ForwardHint::Individual(peer.clone()),
                        SignalScope::Groups(_)          => ForwardHint::All,
                    };
                    let shard = shard_for_key(&kind, n_shards);
                    let mut fwd_buf = BytesMut::with_capacity(recv_buf.len());
                    crate::codec::encode_wire(&mut fwd_buf, &WireMessage::Signal {
                        ttl: ttl - 1, nonce,
                        sender: sender.clone(), scope, kind, payload, hlc_seq,
                    });
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
                flag_foreign_sys_write(&update.key, &node_id_str, &sys_violations);
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
                        crate::codec::encode_wire(&mut fwd_buf, &WireMessage::Data(fwd_update));
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
                    // Retained key set per node (WS5): try every published key so a
                    // rotated-in signature verifies even before the old key ages out.
                    if let Some((_, pub_key_set)) = guard.iter().find(|(k, _)| k.id_hash() == signer) {
                        let canonical = canonical_sign_bytes(&update);
                        let (lo, hi)  = &signature;
                        let mut sig_bytes = [0u8; 64];
                        sig_bytes[..32].copy_from_slice(lo);
                        sig_bytes[32..].copy_from_slice(hi);
                        if !pub_key_set.iter().any(|k| crate::tls::verify_bytes(k, &canonical, &sig_bytes)) {
                            warn!(
                                "SignedData from {} (signer={:#x}) failed Ed25519 verification, dropping",
                                peer_addr, signer
                            );
                            continue;
                        }
                    }
                    // Unknown signer → fail-closed. The value will arrive via anti-entropy
                    // (StateRequest fires immediately on reconnect, carrying the signer's
                    // sys/identity/ key). Accepting unsigned frames during the bootstrap
                    // window would defeat the purpose of signed writes.
                    else {
                        warn!(
                            "SignedData from unknown signer {:#x} via {}, dropping (identity not yet received)",
                            signer, peer_addr
                        );
                        continue;
                    }
                }

                // Absorb HLC and apply to local store.
                hlc.observe(update.timestamp);
                if intern_keys { update.key = intern_key(update.key, intern_max_keys); }
                if let Some(ref wal) = wal {
                    let _ = wal.append(sync_entry_from(&update)).await;
                }
                flag_foreign_sys_write(&update.key, &node_id_str, &sys_violations);
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
                    crate::codec::encode_wire(&mut fwd_buf, &fwd_msg);
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
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::flag_foreign_sys_write;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn flagged(key: &str, self_node: &str) -> u64 {
        let c = AtomicU64::new(0);
        flag_foreign_sys_write(key, self_node, &c);
        c.load(Ordering::Relaxed)
    }

    #[test]
    fn flags_remote_write_to_each_self_owned_prefix() {
        let me = "127.0.0.1:8080";
        assert_eq!(flagged("sys/identity/127.0.0.1:8080", me), 1);
        assert_eq!(flagged("sys/load/127.0.0.1:8080/cpu", me), 1);
        assert_eq!(flagged("sys/role/127.0.0.1:8080", me), 1);
        assert_eq!(flagged("sys/tuple/127.0.0.1:8080/orders/depth", me), 1);
    }

    #[test]
    fn ignores_other_nodes_and_unowned_namespaces() {
        let me = "127.0.0.1:8080";
        // Same prefix, a *different* node id → legitimate, not flagged.
        assert_eq!(flagged("sys/load/10.0.0.5:9000/cpu", me), 0);
        // sys/quorum is excluded — peers legitimately attest about us.
        assert_eq!(flagged("sys/quorum/work.done/127.0.0.1:8080", me), 0);
        // Non-sys keys are never flagged.
        assert_eq!(flagged("cap/orders/127.0.0.1:8080", me), 0);
        assert_eq!(flagged("grp/team/127.0.0.1:8080", me), 0);
        // A node id that only *prefixes* ours must not match (segment-exact).
        assert_eq!(flagged("sys/load/127.0.0.1:80800/cpu", me), 0);
    }

    #[test]
    fn counter_accumulates_across_calls() {
        let me = "127.0.0.1:8080";
        let c = AtomicU64::new(0);
        flag_foreign_sys_write("sys/load/127.0.0.1:8080/a", me, &c);
        flag_foreign_sys_write("sys/role/127.0.0.1:8080",   me, &c);
        flag_foreign_sys_write("sys/load/10.0.0.5:9000/a",  me, &c); // not ours
        assert_eq!(c.load(Ordering::Relaxed), 2);
    }
}
