use crate::error::GossipError;
use crate::node_id::NodeId;
use crate::signal::SignalScope;
use ahash::RandomState;
use bytes::{BufMut, Bytes, BytesMut};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, OnceLock,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    sync::{mpsc, mpsc::error::TrySendError},
};
use tracing::warn;

pub(crate) const MAX_FRAME_BYTES: usize = 10 * 1024 * 1024;
/// Framing-level protocol version. Written before every serialized payload.
/// v2: switched serialization from bincode 1.x to bincode 2.x (incompatible wire format).
/// v3: timestamps changed from second to millisecond granularity (incompatible LWW semantics).
/// v4: GossipUpdate.sender changed from NodeId (string) to u64 id_hash (compact wire identity).
/// v5: integer encoding changed from varint to fixed-width (faster encode/decode).
/// v6: GossipUpdate field order changed (nonce, sender, ttl, is_tombstone, timestamp, key, value)
///     to place all fixed-width fields before variable-length fields, enabling in-place TTL
///     decrement and zero-copy forwarding without re-encoding on each hop.
/// v7: WireMessage::StateRequest gained `store_hash: u64` for anti-entropy fast-path skipping.
///     v6 peers send `StateRequest { sender }` (no `store_hash` bytes). Bincode fixed-int
///     cannot decode a struct with missing trailing fields; v6 frames are decoded into
///     `WireMessageV6` and converted via `From`, producing `store_hash = 0` — the "no
///     digest" sentinel that always triggers a full snapshot (correct graceful downgrade).
/// v8: WireMessage::StateRequest gained `key_timestamps: Vec<(Arc<str>, u64)>` for two-round
///     delta anti-entropy. The sender includes its full key→timestamp index so the responder
///     only sends entries that are newer or absent on the sender's side. v7 peers send
///     StateRequest without this field; they are decoded via `WireMessageV7` and converted
///     with `key_timestamps = vec![]` — the "no digest" sentinel that triggers a full snapshot.
/// v9: `GossipUpdate.timestamp` is now a Hybrid Logical Clock value packed as
///     `(physical_ms_48 << 16) | logical_16` rather than a raw wall-clock millisecond.
///     LWW comparisons are unchanged — `>` on the packed `u64` is still equivalent to
///     `(phys, logical)` lex order. Receivers feed every incoming timestamp through
///     `Hlc::observe` so locally-originated updates after an observed remote always
///     have a strictly greater HLC, preserving causal happens-before under clock skew.
///     v8 timestamps cannot be safely reinterpreted as HLC (they'd parse as a far-past
///     physical with logical=0 and lose every LWW comparison), so v8 acceptance was
///     closed when v9 shipped — `PREV_WIRE_VERSION = WIRE_VERSION = 9`. Mixed-version
///     clusters must perform a stop-the-world upgrade.
/// v10: Ed25519 signatures on locally-originated KV writes under the `tls` feature.
///     A new `WireMessage::SignedData { update: GossipUpdate, signer: u64, signature: [u8; 64] }`
///     variant carries the originator's Ed25519 signature over the hop-invariant fields
///     (nonce, sender, is_tombstone, timestamp, key, value — excludes TTL). Receivers
///     that know the signer's public key drop frames whose signature does not verify;
///     receivers that have not yet received the signer's identity key accept the frame
///     (fail-open). `WireMessage::Data` continues to be accepted unconditionally for
///     non-TLS clusters and during rolling upgrades. v9 peers send only `Data` frames
///     (never `SignedData`); since `Data` is variant 0 and `SignedData` is variant 5,
///     no WireMessageV9 shim is needed — v9 frames decode cleanly with the v10 decoder.
///
/// Rolling-upgrade policy: `read_frame` accepts frames at both `WIRE_VERSION` and
/// `PREV_WIRE_VERSION`. When bumping WIRE_VERSION to N+1:
///   1. Add a `WireMessageVN` / `GossipUpdateVN` struct with the *old* field layout if
///      the binary layout of existing variants changed.
///   2. Set `PREV_WIRE_VERSION = old WIRE_VERSION` (N).
///   3. In the `FrameVersion::Previous` decode path, deserialize into `WireMessageVN`
///      and convert to `WireMessage` via `From`.
///   4. Forwarding always re-encodes at `WIRE_VERSION` so the cluster converges quickly.
///   5. After all nodes are upgraded, set `PREV_WIRE_VERSION = WIRE_VERSION` to close
///      the acceptance window.
///
/// **Current state**: `PREV_WIRE_VERSION = 9`, `WIRE_VERSION = 10`. v9 peers send only
/// `Data` frames; the v10 decoder handles these without a shim because `SignedData` (the
/// only new v10 variant) can never appear in a v9 frame.
pub(crate) const WIRE_VERSION: u8 = 10;
/// Previous wire version accepted during rolling upgrades.
/// v9 peers send only `WireMessage::Data` frames (never `SignedData`), so both versions
/// decode cleanly with the same `WireMessage` type — no `WireMessageV9` shim needed.
pub(crate) const PREV_WIRE_VERSION: u8 = 9;

/// Which wire version a received frame was encoded with.
/// Used by `handle_connection` to select the appropriate decoder and to decide
/// whether the zero-copy Data forward path is safe (current version only).
#[derive(Copy, Clone, Debug, PartialEq)]
pub(crate) enum FrameVersion {
    /// Encoded at `WIRE_VERSION` — use `bincode_cfg()` and zero-copy Data forwarding.
    Current,
    /// Encoded at `PREV_WIRE_VERSION` — use `bincode_cfg_prev()` and full re-encode on forward.
    Previous,
}
/// Fallback shard count used in unit tests that build `ConnContext` directly.
#[cfg(test)]
pub(crate) const N_GOSSIP_SHARDS: usize = 4;

/// Layout of a bincode-encoded `WireMessage::Data` payload (fixed-int encoding):
///
///   [0..4]  WireMessage variant tag (u32 LE) = 0 for Data  ← DATA_TAG
///   [4..12] GossipUpdate.nonce   (u64 LE)                  ← NONCE_OFFSET
///  [12..20] GossipUpdate.sender  (u64 LE)
///     [20]  GossipUpdate.ttl     (u8)                      ← TTL_OFFSET
///     [21]  GossipUpdate.is_tombstone (u8)
///  [22..30] GossipUpdate.timestamp (u64 LE)
///  [30..]   GossipUpdate.key, GossipUpdate.value (variable-length)
///
/// IMPORTANT: if `GossipUpdate`'s field order or `bincode_cfg()` changes these
/// constants must be updated. `test_ttl_offset_matches_wire_layout` and
/// `test_nonce_offset_matches_wire_layout` encode live messages and assert the
/// byte offsets — update them alongside these constants.
///
/// Used by the early-dedup path to read the nonce directly from the wire buffer
/// without a full bincode decode.
pub(crate) const NONCE_OFFSET: usize = 4;
/// Byte offset of the `ttl` field. Used for in-place TTL decrement during zero-copy forwarding.
pub(crate) const TTL_OFFSET: usize = 20;
/// Little-endian u32(0): the `WireMessage::Data` variant tag. Only Data frames
/// carry a nonce at `NONCE_OFFSET`; all other variants have a non-zero tag byte.
pub(crate) const DATA_TAG: [u8; 4] = [0, 0, 0, 0];

/// Sentinel nonce used for entries injected via anti-entropy (`StateResponse`).
/// The `Data` arm is the only code path that calls `seen.is_duplicate`, so this
/// value is never inserted into the seen set; it exists solely as a placeholder
/// to satisfy the `GossipUpdate` struct's nonce field.
pub(crate) const ANTI_ENTROPY_NONCE: u64 = 0;

/// A gossip data update propagated between nodes.
///
/// Field order is load-bearing for the wire format (v6): fixed-width fields
/// (nonce, sender, ttl, is_tombstone, timestamp) come first so the TTL can be
/// decremented in-place at a known byte offset without a full decode/re-encode.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub(crate) struct GossipUpdate {
    /// Random identifier for network-wide deduplication.
    pub(crate) nonce: u64,
    /// Originating node's `id_hash` — compact u64 used for echo-suppression.
    pub(crate) sender: u64,
    /// Remaining hops; decremented on each forward.
    pub(crate) ttl: u8,
    /// When true the key is deleted rather than upserted.
    pub(crate) is_tombstone: bool,
    /// Unix-millisecond timestamp for last-write-wins conflict resolution.
    pub(crate) timestamp: u64,
    /// `Arc<str>` so clone is O(1) on every fan-out hop.
    pub(crate) key: Arc<str>,
    pub(crate) value: Bytes,
}

/// Wire envelope separating control-plane pings from data-plane gossip updates.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub(crate) enum WireMessage {
    Data(GossipUpdate),
    /// `known_peers` carries the sender's current peer table for passive peer discovery.
    Ping { sender: NodeId, known_peers: Vec<NodeId> },
    /// Anti-entropy probe: ask the receiver to reply with a delta of its store.
    ///
    /// `store_hash` is the sender's current XOR-hash of {key, timestamp} pairs (see
    /// `store::store_hash`). If the receiver's own hash matches, it replies with an
    /// empty `StateResponse` (fast-path skip). Hash `0` means "no digest" — the
    /// receiver always sends a full snapshot.
    ///
    /// `key_timestamps` is the sender's full (key, timestamp) index. The receiver
    /// computes a delta: entries whose timestamp is newer than the sender's, plus any
    /// entries absent from the sender's index. An empty vec means "no delta info" —
    /// the receiver sends a full snapshot (backward-compat sentinel for v7 peers).
    StateRequest { sender: NodeId, store_hash: u64, key_timestamps: Vec<(Arc<str>, u64)> },
    /// Response to `StateRequest`; contains the responder's current store.
    StateResponse { entries: Vec<SyncEntry> },
    /// An ephemeral signal propagated epidemically (Layer 2).
    ///
    /// Unlike `Data`, Signal has variable-length scope so TTL cannot be decremented
    /// in-place — it is re-encoded on every forward. All nodes forward regardless of
    /// scope; the receiver's `Boundary` decides whether to act.
    Signal {
        ttl:     u8,
        nonce:   u64,
        sender:  NodeId,
        scope:   SignalScope,
        kind:    Arc<str>,
        payload: bytes::Bytes,
    },
    /// An Ed25519-signed KV write (`tls` feature). Carries the originator's signature
    /// over the hop-invariant fields of `update` (see `canonical_sign_bytes`).
    ///
    /// `signer` is the originator's `NodeId::id_hash()`. Receivers that know the
    /// corresponding public key verify and drop on failure; unknown signers are
    /// accepted (fail-open). TTL is excluded from the signed bytes so the signature
    /// remains valid through all forwarding hops.
    ///
    /// The 64-byte Ed25519 signature is split into two `[u8; 32]` halves because
    /// serde only supports fixed arrays up to size 32.
    SignedData { update: GossipUpdate, signer: u64, signature: ([u8; 32], [u8; 32]) },
}

/// A single key-value record carried inside `WireMessage::StateResponse`.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub(crate) struct SyncEntry {
    pub(crate) key:          Arc<str>,
    pub(crate) value:        Bytes,
    pub(crate) timestamp:    u64,
    pub(crate) is_tombstone: bool,
}

fn shard_hasher() -> &'static RandomState {
    static STATE: OnceLock<RandomState> = OnceLock::new();
    STATE.get_or_init(RandomState::new)
}

/// Maps a key to one of `n_shards` gossip worker channels.
/// `n_shards` must be a power of two; callers normalise it in `GossipAgent::new`.
pub(crate) fn shard_for_key(key: &str, n_shards: usize) -> usize {
    debug_assert!(n_shards.is_power_of_two(), "n_shards must be a power of two");
    shard_hasher().hash_one(key) as usize & (n_shards - 1)
}

/// Returns the bincode configuration used for all wire encoding/decoding.
/// Fixed-width integer encoding is faster than varint for u64/u8 fields and
/// produces a more predictable wire size — no branching on the encode/decode hot path.
#[inline(always)]
pub(crate) fn bincode_cfg() -> impl bincode::config::Config {
    bincode::config::standard().with_fixed_int_encoding()
}


/// Extracts the gossip shard routing key from a wire message.
fn wire_msg_key(msg: &WireMessage) -> &str {
    match msg {
        WireMessage::Data(u)                    => &u.key,
        WireMessage::SignedData { update, .. }  => &update.key,
        WireMessage::Signal { kind, .. }        => kind,
        _                                       => "",
    }
}

/// Encodes `msg`, routes to the correct shard, and dispatches via `try_send`.
/// Returns `false` if the channel is full (increments `dropped`) or closed.
pub(crate) fn dispatch_gossip_try_send(
    gossip_txs:  &[mpsc::Sender<(Bytes, u64, ForwardHint)>],
    msg:         WireMessage,
    sender_hash: u64,
    hint:        ForwardHint,
    dropped:     &AtomicU64,
) -> bool {
    let shard = shard_for_key(wire_msg_key(&msg), gossip_txs.len());
    let mut buf = BytesMut::with_capacity(256);
    if bincode::serde::encode_into_std_write(msg, &mut (&mut buf).writer(), bincode_cfg()).is_err() {
        return false;
    }
    match gossip_txs[shard].try_send((buf.freeze(), sender_hash, hint)) {
        Ok(())                     => true,
        Err(TrySendError::Full(_)) => {
            let n = dropped.fetch_add(1, Ordering::Relaxed) + 1;
            if n % 1_000 == 0 {
                warn!("Gossip channel saturation: {} cumulative frames dropped", n);
            }
            false
        }
        Err(TrySendError::Closed(_)) => false,
    }
}

/// Like [`dispatch_gossip_try_send`] but awaits channel capacity instead of dropping.
pub(crate) async fn dispatch_gossip_send(
    gossip_txs:  &[mpsc::Sender<(Bytes, u64, ForwardHint)>],
    msg:         WireMessage,
    sender_hash: u64,
    hint:        ForwardHint,
) -> bool {
    let shard = shard_for_key(wire_msg_key(&msg), gossip_txs.len());
    let mut buf = BytesMut::with_capacity(256);
    if bincode::serde::encode_into_std_write(msg, &mut (&mut buf).writer(), bincode_cfg()).is_err() {
        return false;
    }
    gossip_txs[shard].send((buf.freeze(), sender_hash, hint)).await.is_ok()
}

/// Wire envelope for `PREV_WIRE_VERSION` (v7) frames.
///
/// Identical to [`WireMessage`] except `StateRequest` has no `key_timestamps` field.
/// Bincode fixed-int encoding has no optional/missing-field concept; decoding a v7
/// `StateRequest` as v8 `WireMessage::StateRequest` would attempt to read a Vec field
/// that is not present and error. This struct gives the decoder the correct v7 layout,
/// after which [`From<WireMessageV7>`] upgrades it to a `WireMessage` with
/// `key_timestamps = vec![]` (the "no delta info" sentinel — full snapshot).
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub(crate) enum WireMessageV7 {
    Data(GossipUpdate),
    Ping { sender: NodeId, known_peers: Vec<NodeId> },
    /// v7 variant — has `store_hash` but no `key_timestamps`.
    StateRequest { sender: NodeId, store_hash: u64 },
    StateResponse { entries: Vec<SyncEntry> },
    Signal {
        ttl:     u8,
        nonce:   u64,
        sender:  NodeId,
        scope:   SignalScope,
        kind:    Arc<str>,
        payload: Bytes,
    },
}

impl From<WireMessageV7> for WireMessage {
    fn from(m: WireMessageV7) -> Self {
        match m {
            WireMessageV7::Data(u) => WireMessage::Data(u),
            WireMessageV7::Ping { sender, known_peers } =>
                WireMessage::Ping { sender, known_peers },
            WireMessageV7::StateRequest { sender, store_hash } =>
                WireMessage::StateRequest { sender, store_hash, key_timestamps: vec![] },
            WireMessageV7::StateResponse { entries } =>
                WireMessage::StateResponse { entries },
            WireMessageV7::Signal { ttl, nonce, sender, scope, kind, payload } =>
                WireMessage::Signal { ttl, nonce, sender, scope, kind, payload },
        }
    }
}

/// Writes a length-prefixed frame: `[4-byte len][WIRE_VERSION][payload]`.
/// The 5-byte header and payload are written as two consecutive `write_all` calls;
/// through the caller's `BufWriter` both land in the same kernel write on flush.
pub(crate) async fn write_frame<W>(stream: &mut W, data: &[u8]) -> Result<(), GossipError>
where
    W: AsyncWrite + Unpin,
{
    let payload_len = 1 + data.len();
    if payload_len > MAX_FRAME_BYTES {
        return Err(GossipError::Network(format!(
            "Frame too large to send: {} bytes (max {})",
            data.len(),
            MAX_FRAME_BYTES
        )));
    }
    let len = u32::try_from(payload_len).map_err(|_| {
        GossipError::Network(format!("Frame payload too large: {} bytes", data.len()))
    })?;
    let mut header = [0u8; 5];
    header[..4].copy_from_slice(&len.to_be_bytes());
    header[4] = WIRE_VERSION;
    stream.write_all(&header).await.map_err(GossipError::Io)?;
    stream.write_all(data).await.map_err(GossipError::Io)?;
    Ok(())
}

/// Reads one length-prefixed frame into `buf`, reusing its allocation.
/// Returns [`FrameVersion`] so the caller can select the appropriate decoder.
/// Accepts frames at both `WIRE_VERSION` and `PREV_WIRE_VERSION` to support
/// rolling upgrades — see the `WIRE_VERSION` doc for the upgrade policy.
///
/// Uses `read_buf` with a `BufMut::limit` guard to avoid zero-initialising the
/// destination region before `read_exact` fills it (safe, no unsafe code needed).
pub(crate) async fn read_frame<R>(stream: &mut R, buf: &mut BytesMut) -> Result<FrameVersion, GossipError>
where
    R: AsyncRead + Unpin,
{
    let mut header = [0u8; 5];
    stream.read_exact(&mut header).await?;
    let total = u32::from_be_bytes([header[0], header[1], header[2], header[3]]) as usize;
    if total == 0 || total > MAX_FRAME_BYTES {
        return Err(GossipError::Network(format!(
            "Frame length out of range: {} bytes",
            total
        )));
    }
    let frame_version = if header[4] == WIRE_VERSION {
        FrameVersion::Current
    } else if header[4] == PREV_WIRE_VERSION {
        FrameVersion::Previous
    } else {
        let hint = if header[4] < PREV_WIRE_VERSION {
            "peer is running a significantly older version"
        } else {
            "peer is running a newer version"
        };
        return Err(GossipError::Network(format!(
            "Unsupported wire version {} (accepted {} and {}; {})",
            header[4], WIRE_VERSION, PREV_WIRE_VERSION, hint
        )));
    };
    let payload_len = total - 1;
    buf.clear();
    buf.reserve(payload_len);
    // Fill exactly payload_len bytes. `limit()` constrains read_buf to the budget
    // so it cannot overshoot, and the spare_capacity_mut path avoids zero-init.
    {
        let mut limited = (&mut *buf).limit(payload_len);
        loop {
            if limited.remaining_mut() == 0 { break; }
            let n = stream.read_buf(&mut limited).await.map_err(GossipError::Io)?;
            if n == 0 {
                return Err(GossipError::Io(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "connection closed mid-frame",
                )));
            }
        }
    }
    Ok(frame_version)
}

/// Forwarding hint passed alongside pre-encoded signal bytes into the gossip shard.
/// Data frames always use `ForwardHint::All`.
#[derive(Clone, Debug)]
pub(crate) enum ForwardHint {
    /// Forward to all targets — System signals and Data frames.
    All,
    /// Forward to known group members plus up to `epidemic_extra_peers` random non-members.
    Group(Arc<str>),
    /// Forward only to the named target peer.
    Individual(crate::node_id::NodeId),
}

/// Constructs a [`GossipUpdate`] for a locally-originated KV write or tombstone.
/// Calls `hlc.tick()` so every locally-originated write gets a strictly-greater
/// HLC timestamp than every previous local write and every observed remote
/// stamp — see `crate::hlc` for the causal-ordering rationale.
///
/// ## Placement note
///
/// This function lives in `framing.rs` (the lowest layer) but is the canonical
/// write-side factory for every higher layer: capability and emergent-group
/// machinery in `agent::capability_ops` / `agent::wiring` /
/// `agent::emergent_groups`, opacity governors in `agent::opacity`, consensus
/// in `consensus.rs`, and the connection handler's persistent-quorum writes.
/// The home is correct — the function owns the wire encoding shape (nonce,
/// sender hash, ttl, tombstone flag, timestamp, key, value) and the byte
/// offsets that the zero-copy forwarding path in `connection.rs` depends on
/// (TTL_OFFSET, NONCE_OFFSET). Placing it higher would force `framing.rs` to
/// depend on the agent layer or on `Hlc`, breaking the dependency direction.
///
/// Callers obtain an `&Hlc` from whichever context holds it
/// (`TaskCtx::hlc`, `ConnContext::hlc`, or `GossipAgent::task_ctx.hlc`).
pub(crate) fn make_gossip_update(
    node_id:      &crate::node_id::NodeId,
    ttl:          u8,
    key:          Arc<str>,
    value:        bytes::Bytes,
    is_tombstone: bool,
    hlc:          &crate::hlc::Hlc,
) -> GossipUpdate {
    GossipUpdate {
        nonce:        fastrand::u64(1..),
        sender:       node_id.id_hash(),
        ttl,
        is_tombstone,
        timestamp:    hlc.tick(),
        key,
        value,
    }
}

/// Returns the canonical byte representation of `update` for Ed25519 signing and
/// verification. Covers all hop-invariant fields: `nonce`, `sender`, `is_tombstone`,
/// `timestamp`, `key`, `value`. TTL is intentionally excluded because it is
/// decremented on each forwarding hop — the originator's signature must remain valid
/// through the entire propagation path.
#[cfg_attr(not(feature = "tls"), allow(dead_code))]
pub(crate) fn canonical_sign_bytes(u: &GossipUpdate) -> Vec<u8> {
    bincode::serde::encode_to_vec(
        (u.nonce, u.sender, u.is_tombstone, u.timestamp, u.key.as_ref(), u.value.as_ref()),
        bincode_cfg(),
    ).unwrap_or_default()
}

/// Wraps `update` in the appropriate `WireMessage` variant for local-origination dispatch.
///
/// When `tls` is `Some` (i.e. the `tls` feature is enabled and TLS is configured),
/// signs the hop-invariant fields with the node's Ed25519 key and returns
/// `WireMessage::SignedData`. Otherwise returns `WireMessage::Data`.
///
/// Used by `dispatch_update`, `dispatch_update_async`, and the consensus engine's
/// KV write helpers so signing is applied consistently at every locally-originated
/// write site.
pub(crate) fn make_kv_wire_msg(
    update:      GossipUpdate,
    sender_hash: u64,
    tls:         Option<&crate::tls::NodeTls>,
) -> WireMessage {
    #[cfg(feature = "tls")]
    if let Some(t) = tls {
        let canonical  = canonical_sign_bytes(&update);
        let sig_bytes  = crate::tls::sign_bytes(&t.signing_key, &canonical);
        let sig_lo: [u8; 32] = sig_bytes[..32].try_into().expect("signature is 64 bytes");
        let sig_hi: [u8; 32] = sig_bytes[32..].try_into().expect("signature is 64 bytes");
        return WireMessage::SignedData { update, signer: sender_hash, signature: (sig_lo, sig_hi) };
    }
    let _ = (sender_hash, tls);
    WireMessage::Data(update)
}

/// Projects the persistence-relevant fields from a [`GossipUpdate`] into a
/// [`SyncEntry`], stripping wire-only fields (`nonce`, `sender`, `ttl`).
/// Used by WAL call sites to record exactly what the store contains.
pub(crate) fn sync_entry_from(u: &GossipUpdate) -> SyncEntry {
    SyncEntry {
        key:          u.key.clone(),
        value:        u.value.clone(),
        timestamp:    u.timestamp,
        is_tombstone: u.is_tombstone,
    }
}

/// Returns the worst-case fill ratio across all gossip shard channels.
///
/// `0.0` = all channels empty. `1.0` = at least one shard fully saturated.
/// Used by admission-control paths that need to account for gossip backpressure
/// independently of handler channel fill ratios.
pub(crate) fn gossip_shard_fill(txs: &[mpsc::Sender<(Bytes, u64, ForwardHint)>]) -> f32 {
    txs.iter()
        .map(|tx| {
            let max = tx.max_capacity();
            if max == 0 { 0.0_f32 } else { 1.0 - tx.capacity() as f32 / max as f32 }
        })
        .fold(0.0_f32, f32::max)
}

pub(crate) fn is_connection_closed(e: &GossipError) -> bool {
    match e {
        GossipError::Io(io_err) => matches!(
            io_err.kind(),
            std::io::ErrorKind::UnexpectedEof
                | std::io::ErrorKind::ConnectionReset
                | std::io::ErrorKind::ConnectionAborted
                | std::io::ErrorKind::BrokenPipe
        ),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::{Bytes, BytesMut};
    use std::sync::Arc;
    use tokio::{io::AsyncWriteExt, net::{TcpListener, TcpStream}};

    #[test]
    fn ttl_offset_matches_wire_layout() {
        let update = GossipUpdate {
            nonce:        0xABCD_EF01_2345_6789,
            sender:       0x1111_2222_3333_4444,
            ttl:          0xAA,
            is_tombstone: false,
            timestamp:    0,
            key:          Arc::from("k"),
            value:        Bytes::new(),
        };
        let encoded = bincode::serde::encode_to_vec(
            WireMessage::Data(update), bincode_cfg(),
        ).unwrap();
        assert_eq!(
            encoded[TTL_OFFSET], 0xAA,
            "TTL_OFFSET={} does not point at ttl byte; wire layout may have changed",
            TTL_OFFSET,
        );
    }

    #[test]
    fn nonce_offset_matches_wire_layout() {
        let update = GossipUpdate {
            nonce:        0xABCD_EF01_2345_6789,
            sender:       0x1111_2222_3333_4444,
            ttl:          5,
            is_tombstone: false,
            timestamp:    0,
            key:          Arc::from("k"),
            value:        Bytes::new(),
        };
        let encoded = bincode::serde::encode_to_vec(
            WireMessage::Data(update), bincode_cfg(),
        ).unwrap();
        let nonce = u64::from_le_bytes(
            encoded[NONCE_OFFSET..NONCE_OFFSET + 8].try_into().unwrap(),
        );
        assert_eq!(
            nonce, 0xABCD_EF01_2345_6789,
            "NONCE_OFFSET={} does not point at the nonce field; wire layout may have changed",
            NONCE_OFFSET,
        );
    }

    #[tokio::test]
    async fn read_frame_rejects_wrong_version() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let mut writer = TcpStream::connect(addr).await.unwrap();
        let (mut reader, _) = listener.accept().await.unwrap();
        let payload = b"test";
        let total = (1u32 + payload.len() as u32).to_be_bytes();
        writer.write_all(&total).await.unwrap();
        writer.write_all(&[0u8]).await.unwrap();
        writer.write_all(payload).await.unwrap();
        let mut buf = BytesMut::new();
        let result = read_frame(&mut reader, &mut buf).await;
        assert!(result.is_err(), "wrong version should be rejected");
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("wire version"), "error should mention wire version: {}", msg);
    }

    #[tokio::test]
    async fn read_frame_accepts_correct_version() {
        let (mut writer, mut reader) = {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let w = TcpStream::connect(addr).await.unwrap();
            let (r, _) = listener.accept().await.unwrap();
            (w, r)
        };
        let payload = b"hello";
        write_frame(&mut writer, payload).await.unwrap();
        let mut buf = BytesMut::new();
        read_frame(&mut reader, &mut buf).await.unwrap();
        assert_eq!(&buf[..], payload);
    }

    #[test]
    fn canonical_sign_bytes_excludes_ttl() {
        let u1 = GossipUpdate {
            nonce: 1, sender: 2, ttl: 5, is_tombstone: false,
            timestamp: 100, key: Arc::from("k"), value: Bytes::from_static(b"v"),
        };
        let u2 = GossipUpdate { ttl: 3, ..u1.clone() };
        assert_eq!(
            canonical_sign_bytes(&u1), canonical_sign_bytes(&u2),
            "TTL must not affect canonical signed bytes",
        );
    }

    #[test]
    fn canonical_sign_bytes_differs_on_value_change() {
        let u = GossipUpdate {
            nonce: 1, sender: 2, ttl: 5, is_tombstone: false,
            timestamp: 100, key: Arc::from("k"), value: Bytes::from_static(b"original"),
        };
        let tampered = GossipUpdate { value: Bytes::from_static(b"injected"), ..u.clone() };
        assert_ne!(
            canonical_sign_bytes(&u), canonical_sign_bytes(&tampered),
            "Different values must produce different canonical bytes",
        );
    }

    #[cfg(feature = "tls")]
    #[test]
    fn sign_and_verify_gossip_update() {
        use ed25519_dalek::SigningKey;
        use rand_core::OsRng;
        let sk  = SigningKey::generate(&mut OsRng);
        let vk  = sk.verifying_key().to_bytes();
        let u   = GossipUpdate {
            nonce: 42, sender: 99, ttl: 5, is_tombstone: false,
            timestamp: 1000, key: Arc::from("test-key"), value: Bytes::from_static(b"hello"),
        };
        let canonical = canonical_sign_bytes(&u);
        let sig_bytes = crate::tls::sign_bytes(&sk, &canonical);
        assert!(crate::tls::verify_bytes(&vk, &canonical, &sig_bytes), "valid signature must verify");
    }

    #[cfg(feature = "tls")]
    #[test]
    fn tampered_value_fails_verification() {
        use ed25519_dalek::SigningKey;
        use rand_core::OsRng;
        let sk        = SigningKey::generate(&mut OsRng);
        let vk        = sk.verifying_key().to_bytes();
        let u         = GossipUpdate {
            nonce: 1, sender: 2, ttl: 1, is_tombstone: false,
            timestamp: 50, key: Arc::from("k"), value: Bytes::from_static(b"legit"),
        };
        let canonical = canonical_sign_bytes(&u);
        let sig_bytes = crate::tls::sign_bytes(&sk, &canonical);
        let tampered  = GossipUpdate { value: Bytes::from_static(b"injected"), ..u.clone() };
        assert!(
            !crate::tls::verify_bytes(&vk, &canonical_sign_bytes(&tampered), &sig_bytes),
            "tampered value must not verify",
        );
    }


    #[test]
    fn make_kv_wire_msg_no_tls_returns_data() {
        let u = GossipUpdate {
            nonce: 1, sender: 2, ttl: 5, is_tombstone: false,
            timestamp: 0, key: Arc::from("k"), value: Bytes::new(),
        };
        let msg = make_kv_wire_msg(u, 2, None);
        assert!(matches!(msg, WireMessage::Data(_)), "no-tls path must return Data variant");
    }
}
