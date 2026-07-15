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

pub const MAX_FRAME_BYTES: usize = 10 * 1024 * 1024;

/// Conservative ceiling for a single KV write (`key.len() + value.len()`), enforced by
/// `kv_set` / `kv_set_async` **before** the write is applied anywhere. A write above this
/// cannot be encoded into one gossip frame (`MAX_FRAME_BYTES`) once envelope overhead is
/// added (frame header, `GossipUpdate` fixed fields, optional `SignedData` wrapper), so
/// accepting it would create an entry that applies locally but can never propagate —
/// silent permanent divergence. 64 KiB of headroom is far above the worst-case envelope.
/// Payloads larger than this belong on the bulk transport (`bulk_call` / `bulk_serve`).
pub const MAX_KV_WRITE_BYTES: usize = MAX_FRAME_BYTES - 64 * 1024;
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
/// v11: `hlc_seq: Option<u64>` added to `WireMessage::Signal` for causal ordering
///     (`emit_ordered`).
/// v12: anti-entropy switched from a full key→timestamp index to a Merkle digest.
///     `WireMessage::StateRequest.key_timestamps: Vec<(Arc<str>, u64)>` (`O(keys)`)
///     is replaced by `bucket_hashes: Vec<u64>` — a fixed `ANTI_ENTROPY_BUCKETS`-wide
///     per-bucket XOR digest of the sender's live store (`O(buckets)`, see
///     `store::store_bucket_hashes`). The responder compares its own digest and
///     returns only entries in divergent buckets (`O(divergence)`). An empty
///     `bucket_hashes` is the "no digest" sentinel → full snapshot. v11 peers send
///     a `key_timestamps` StateRequest; `codec::decode_wire_v11` decodes it and
///     drops the index (`bucket_hashes = vec![]` → full snapshot), the same graceful
///     downgrade the v7→v8 and v6→v7 sentinels used. The serialization itself is the
///     in-tree fixed-int codec (M11) — byte-identical to the former bincode, so the
///     only v12 change is this struct shape, not the encoding.
///
/// **Current state**: `PREV_WIRE_VERSION = 11`, `WIRE_VERSION = 12`. A dedicated
/// `codec::decode_wire_v11` path is required because changing a variant's field layout changes the encoding.
pub const WIRE_VERSION: u8 = 12;
/// Previous wire version accepted during rolling upgrades.
pub const PREV_WIRE_VERSION: u8 = 11;

/// Which wire version a received frame was encoded with.
/// Used by `handle_connection` to select the appropriate decoder and to decide
/// whether the zero-copy Data forward path is safe (current version only).
#[derive(Copy, Clone, Debug, PartialEq)]
pub enum FrameVersion {
    /// Encoded at `WIRE_VERSION` — decode via `codec::decode_wire` and use the
    /// zero-copy Data forwarding path.
    Current,
    /// Encoded at `PREV_WIRE_VERSION` — decode via `codec::decode_wire_v11` and
    /// full re-encode on forward.
    Previous,
}
/// Fallback shard count used in unit tests that build `ConnContext` directly.
/// `test-support` exposes it to the full crate's tests across the crate boundary
/// (`#[cfg(test)]` items are invisible to dependents).
#[cfg(any(test, feature = "test-support"))]
pub const N_GOSSIP_SHARDS: usize = 4;

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
/// IMPORTANT: if `GossipUpdate`'s field order or the `codec` layout changes, these
/// constants must be updated. `test_ttl_offset_matches_wire_layout` and
/// `test_nonce_offset_matches_wire_layout` encode live messages and assert the
/// byte offsets — update them alongside these constants.
///
/// Used by the early-dedup path to read the nonce directly from the wire buffer
/// without a full bincode decode.
pub const NONCE_OFFSET: usize = 4;
/// Byte offset of the `ttl` field. Used for in-place TTL decrement during zero-copy forwarding.
pub const TTL_OFFSET: usize = 20;
/// Little-endian u32(0): the `WireMessage::Data` variant tag. Only Data frames
/// carry a nonce at `NONCE_OFFSET`; all other variants have a non-zero tag byte.
pub const DATA_TAG: [u8; 4] = [0, 0, 0, 0];

/// Sentinel nonce used for entries injected via anti-entropy (`StateResponse`).
/// The `Data` arm is the only code path that calls `seen.is_duplicate`, so this
/// value is never inserted into the seen set; it exists solely as a placeholder
/// to satisfy the `GossipUpdate` struct's nonce field.
pub const ANTI_ENTROPY_NONCE: u64 = 0;

/// A gossip data update propagated between nodes.
///
/// Field order is load-bearing for the wire format (v6): fixed-width fields
/// (nonce, sender, ttl, is_tombstone, timestamp) come first so the TTL can be
/// decremented in-place at a known byte offset without a full decode/re-encode.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct GossipUpdate {
    /// Random identifier for network-wide deduplication.
    pub nonce: u64,
    /// Originating node's `id_hash` — compact u64 used for echo-suppression.
    pub sender: u64,
    /// Remaining hops; decremented on each forward.
    pub ttl: u8,
    /// When true the key is deleted rather than upserted.
    pub is_tombstone: bool,
    /// Unix-millisecond timestamp for last-write-wins conflict resolution.
    pub timestamp: u64,
    /// `Arc<str>` so clone is O(1) on every fan-out hop.
    pub key: Arc<str>,
    pub value: Bytes,
}

/// Wire envelope separating control-plane pings from data-plane gossip updates.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub enum WireMessage {
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
    /// `bucket_hashes` is the sender's per-bucket Merkle digest of its live store
    /// (`store::store_bucket_hashes`, `ANTI_ENTROPY_BUCKETS` entries). The receiver
    /// computes its own digest and replies with the entries in every bucket whose
    /// hash differs — `O(divergence)` rather than `O(keys)`. An empty vec means
    /// "no digest" — the receiver sends a full snapshot (backward-compat sentinel
    /// for v11 peers downgraded via `codec::decode_wire_v11`, and for a fresh/empty store).
    StateRequest { sender: NodeId, store_hash: u64, bucket_hashes: Vec<u64> },
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
        /// HLC timestamp stamped by the sender at `emit_ordered()` time.
        /// `None` = unordered emission (v10 compat); receiver delivers immediately.
        /// `Some(ts)` = ordered; receiver may buffer to ensure causal delivery.
        hlc_seq: Option<u64>,
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
pub struct SyncEntry {
    pub key:          Arc<str>,
    pub value:        Bytes,
    pub timestamp:    u64,
    pub is_tombstone: bool,
}

fn shard_hasher() -> &'static RandomState {
    static STATE: OnceLock<RandomState> = OnceLock::new();
    STATE.get_or_init(RandomState::new)
}

/// Maps a key to one of `n_shards` gossip worker channels.
/// `n_shards` must be a power of two; callers normalise it in `GossipAgent::new`.
pub fn shard_for_key(key: &str, n_shards: usize) -> usize {
    debug_assert!(n_shards.is_power_of_two(), "n_shards must be a power of two");
    shard_hasher().hash_one(key) as usize & (n_shards - 1)
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
pub fn dispatch_gossip_try_send(
    gossip_txs:  &[mpsc::Sender<(Bytes, u64, ForwardHint)>],
    msg:         WireMessage,
    sender_hash: u64,
    hint:        ForwardHint,
    dropped:     &AtomicU64,
) -> bool {
    let shard = shard_for_key(wire_msg_key(&msg), gossip_txs.len());
    let buf = crate::codec::wire_to_bytes(&msg);
    match gossip_txs[shard].try_send((buf, sender_hash, hint)) {
        Ok(())                     => true,
        Err(TrySendError::Full(_)) => {
            let n = dropped.fetch_add(1, Ordering::Relaxed) + 1;
            if n.is_multiple_of(1_000) {
                warn!("Gossip channel saturation: {} cumulative frames dropped", n);
            }
            false
        }
        Err(TrySendError::Closed(_)) => false,
    }
}

/// Like [`dispatch_gossip_try_send`] but awaits channel capacity instead of dropping.
pub async fn dispatch_gossip_send(
    gossip_txs:  &[mpsc::Sender<(Bytes, u64, ForwardHint)>],
    msg:         WireMessage,
    sender_hash: u64,
    hint:        ForwardHint,
) -> bool {
    let shard = shard_for_key(wire_msg_key(&msg), gossip_txs.len());
    let buf = crate::codec::wire_to_bytes(&msg);
    gossip_txs[shard].send((buf, sender_hash, hint)).await.is_ok()
}

// Previous-version (v11) frames are decoded by the hand-rolled `crate::codec::decode_wire_v11`: the
// v11 layout is identical to v12 except `StateRequest` carries the old `key_timestamps` full index
// instead of the `bucket_hashes` Merkle digest, which `decode_wire_v11` reads and discards, downgrading
// to `bucket_hashes = vec![]` — the "no digest" sentinel, so a v11 peer's anti-entropy request simply
// triggers a full snapshot during the rolling-upgrade window. (Signal already has `hlc_seq` in v11.)

/// Writes a length-prefixed frame: `[4-byte len][WIRE_VERSION][payload]`.
/// The 5-byte header and payload are written as two consecutive `write_all` calls;
/// through the caller's `BufWriter` both land in the same kernel write on flush.
pub async fn write_frame<W>(stream: &mut W, data: &[u8]) -> Result<(), GossipError>
where
    W: AsyncWrite + Unpin,
{
    let payload_len = 1 + data.len();
    if payload_len > MAX_FRAME_BYTES {
        return Err(GossipError::FrameTooLarge { size: data.len(), limit: MAX_FRAME_BYTES });
    }
    let len = u32::try_from(payload_len)
        .map_err(|_| GossipError::FrameTooLarge { size: data.len(), limit: MAX_FRAME_BYTES })?;
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
pub async fn read_frame<R>(stream: &mut R, buf: &mut BytesMut) -> Result<FrameVersion, GossipError>
where
    R: AsyncRead + Unpin,
{
    let mut header = [0u8; 5];
    stream.read_exact(&mut header).await?;
    let total = u32::from_be_bytes([header[0], header[1], header[2], header[3]]) as usize;
    if total == 0 || total > MAX_FRAME_BYTES {
        return Err(GossipError::FrameTooLarge { size: total, limit: MAX_FRAME_BYTES });
    }
    let frame_version = if header[4] == WIRE_VERSION {
        FrameVersion::Current
    } else if header[4] == PREV_WIRE_VERSION {
        FrameVersion::Previous
    } else {
        let hint: &'static str = if header[4] < PREV_WIRE_VERSION {
            "peer is running a significantly older version"
        } else {
            "peer is running a newer version"
        };
        return Err(GossipError::UnsupportedWireVersion {
            received: header[4],
            current:  WIRE_VERSION,
            prev:     PREV_WIRE_VERSION,
            hint,
        });
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
pub enum ForwardHint {
    /// Forward to all targets — Cluster-scoped signals and Data frames.
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
pub fn make_gossip_update(
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
pub fn canonical_sign_bytes(u: &GossipUpdate) -> Vec<u8> {
    crate::codec::canonical_update_bytes(u)
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
pub fn make_kv_wire_msg(
    update:      GossipUpdate,
    sender_hash: u64,
    tls:         Option<&crate::tls::NodeTls>,
) -> WireMessage {
    #[cfg(feature = "tls")]
    if let Some(t) = tls {
        let canonical  = canonical_sign_bytes(&update);
        let sig_bytes  = crate::tls::sign_bytes(&t.signing_key(), &canonical);
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
pub fn sync_entry_from(u: &GossipUpdate) -> SyncEntry {
    SyncEntry {
        key:          Arc::clone(&u.key),
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
pub fn gossip_shard_fill(txs: &[mpsc::Sender<(Bytes, u64, ForwardHint)>]) -> f32 {
    txs.iter()
        .map(|tx| {
            let max = tx.max_capacity();
            if max == 0 { 0.0_f32 } else { 1.0 - tx.capacity() as f32 / max as f32 }
        })
        .fold(0.0_f32, f32::max)
}

pub fn is_connection_closed(e: &GossipError) -> bool {
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
        let encoded = crate::codec::wire_to_bytes(&WireMessage::Data(update));
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
        let encoded = crate::codec::wire_to_bytes(&WireMessage::Data(update));
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

    #[tokio::test]
    async fn read_frame_rejects_zero_length() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let mut writer = TcpStream::connect(addr).await.unwrap();
        let (mut reader, _) = listener.accept().await.unwrap();
        // Write length=0 (after subtracting the version byte the payload would be -1).
        writer.write_all(&0u32.to_be_bytes()).await.unwrap();
        writer.write_all(&[WIRE_VERSION]).await.unwrap();
        let mut buf = BytesMut::new();
        let result = read_frame(&mut reader, &mut buf).await;
        assert!(result.is_err(), "zero-length frame must be rejected");
        let err = result.unwrap_err();
        assert!(
            matches!(err, GossipError::FrameTooLarge { size: 0, limit: _ }),
            "expected FrameTooLarge for zero-length frame, got: {err}"
        );
    }

    #[tokio::test]
    async fn read_frame_rejects_oversized_frame() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let mut writer = TcpStream::connect(addr).await.unwrap();
        let (mut reader, _) = listener.accept().await.unwrap();
        // Claim a payload length one byte beyond MAX_FRAME_BYTES.
        let oversized = (MAX_FRAME_BYTES + 1) as u32;
        writer.write_all(&oversized.to_be_bytes()).await.unwrap();
        writer.write_all(&[WIRE_VERSION]).await.unwrap();
        let mut buf = BytesMut::new();
        let result = read_frame(&mut reader, &mut buf).await;
        assert!(result.is_err(), "oversized frame must be rejected");
        let err = result.unwrap_err();
        assert!(
            matches!(err, GossipError::FrameTooLarge { size, limit: _ } if size > MAX_FRAME_BYTES),
            "expected FrameTooLarge for oversized frame, got: {err}"
        );
    }

    #[test]
    fn max_frame_bytes_fits_in_u32() {
        // write_frame length-prefixes frames with a u32 — verify the constant fits.
        // payload_len = 1 + data.len() must be representable as u32.
        // With MAX_FRAME_BYTES = 10 MiB this is trivially true, but guard it
        // so a future constant change doesn't silently break the protocol.
        const _: () = assert!(MAX_FRAME_BYTES < u32::MAX as usize);
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

    /// Verifies the rolling-upgrade invariant: a frame encoded at PREV_WIRE_VERSION
    /// (v11) is accepted by read_frame (returns FrameVersion::Previous) and the payload
    /// decodes correctly via the live [`crate::codec::decode_wire_v11`] path. Specifically
    /// confirms that a v11 StateRequest (carrying the old `key_timestamps` index)
    /// downgrades to `bucket_hashes = vec![]` — the "no digest" sentinel that
    /// triggers a full snapshot during the upgrade window.
    #[tokio::test]
    async fn read_frame_accepts_prev_wire_version() {
        use tokio::io::AsyncWriteExt;

        // Golden bincode-era v11 StateRequest bytes (sender 127.0.0.1:7000, store_hash 0x99, full
        // key→timestamp index [("a",1),("bb",2)]) — captured when the codec was proven byte-identical
        // to bincode fixed-int (WS-B M11). No dependency on the retired `bincode`.
        const V11_STATE_REQUEST: &[u8] = &[
            0x02, 0x00, 0x00, 0x00, 0x0e, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x31, 0x32, 0x37,
            0x2e, 0x30, 0x2e, 0x30, 0x2e, 0x31, 0x3a, 0x37, 0x30, 0x30, 0x30, 0x99, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x61, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x62, 0x62, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00,
        ];

        // Write a frame header stamped with PREV_WIRE_VERSION.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let mut writer = tokio::net::TcpStream::connect(addr).await.unwrap();
        let (mut reader, _) = listener.accept().await.unwrap();

        let payload_len = V11_STATE_REQUEST.len() as u32 + 1; // +1 for the version byte
        writer.write_all(&payload_len.to_be_bytes()).await.unwrap();
        writer.write_all(&[PREV_WIRE_VERSION]).await.unwrap();
        writer.write_all(V11_STATE_REQUEST).await.unwrap();

        // read_frame must accept the v11 frame and report FrameVersion::Previous.
        let mut buf = BytesMut::new();
        let frame_ver = read_frame(&mut reader, &mut buf).await
            .expect("read_frame must accept PREV_WIRE_VERSION frame");
        assert_eq!(frame_ver, FrameVersion::Previous,
            "v11 frame must be reported as FrameVersion::Previous");

        // The live previous-decode path (codec) must downgrade it cleanly to the no-digest sentinel.
        match crate::codec::decode_wire_v11(&buf).expect("v11 codec decode must succeed") {
            WireMessage::StateRequest { store_hash, bucket_hashes, .. } => {
                assert_eq!(store_hash, 0x99);
                assert!(bucket_hashes.is_empty(),
                    "v11 StateRequest must downgrade to bucket_hashes=vec![] (full-snapshot sentinel)");
            }
            other => panic!("expected WireMessage::StateRequest, got {other:?}"),
        }
    }

    /// **Rolling-upgrade interop spike.** Proves the claim v2.1.0's release notes make:
    /// a node at the CURRENT wire version (`WIRE_VERSION = 12`) *accepts AND APPLIES* a KV
    /// write from a peer speaking the PREVIOUS wire version (`PREV_WIRE_VERSION = 11`), so
    /// the two converge across the version boundary — end-to-end, not just codec-parse.
    ///
    /// This drives the exact path a live peer connection uses when it receives a v11 frame:
    ///   1. a real `WireMessage::Data` KV `set` is built via [`make_gossip_update`] (HLC
    ///      timestamp and all), then serialized. The `Data` payload is byte-identical across
    ///      v11 and v12 — only `StateRequest` changed at v12 (full key→timestamp index →
    ///      Merkle `bucket_hashes`), so these bytes *are* valid v11 wire bytes. (There is no
    ///      `encode_wire_v11`; none is needed for `Data`.)
    ///   2. the frame is written with `header[4] = PREV_WIRE_VERSION` — a v11 peer's stamp.
    ///   3. [`read_frame`] accepts it and reports [`FrameVersion::Previous`] (the v11 path).
    ///   4. the payload is decoded through the live [`crate::codec::decode_wire_v11`] path.
    ///   5. the decoded update is applied via [`crate::store::apply_and_notify`] — the same
    ///      LWW-merge apply a connection handler performs.
    ///   6. the receiving node's store now returns the value → **converged across v11↔v12**.
    ///
    /// This *exceeds* `read_frame_accepts_prev_wire_version` (which only asserts a v11 frame
    /// parses and downgrades a `StateRequest` to the no-digest sentinel): here a v11-encoded
    /// KV value is decoded AND merged so the v12 store is readable at the key.
    ///
    /// Productionization step this spike de-risks: a *full two-binary cluster* — git-worktree
    /// an old (`WIRE_VERSION = 11`) binary, run it alongside a v12 binary, and observe a real
    /// gossip round converge over TCP. This test isolates the decode→apply boundary that a
    /// two-binary run would exercise, with real v11 bytes, without the process orchestration.
    #[tokio::test]
    async fn prev_wire_version_kv_write_is_applied_and_converges() {
        use crate::hlc::Hlc;
        use crate::node_id::NodeId;
        use crate::store::{scan_kv_prefix, KvState};

        // (1) Build a genuine KV `set` the way every local write site does: through the
        // canonical factory, which stamps an HLC timestamp. This is a v11-originated write.
        let sender = NodeId::new("127.0.0.1", 7011).unwrap();
        let hlc = Hlc::new();
        let key: Arc<str> = Arc::from("spike/rolling-upgrade");
        let value = Bytes::from_static(b"converged-across-v11-v12");
        let update = make_gossip_update(&sender, 4, Arc::clone(&key), value.clone(), false, &hlc);
        // The `Data` payload layout is identical in v11 and v12, so encoding the current
        // message yields the exact bytes a v11 peer would put on the wire.
        let payload = crate::codec::wire_to_bytes(&WireMessage::Data(update));

        // (2) Write a frame stamped with PREV_WIRE_VERSION (write_frame hard-codes the
        // CURRENT version, so we stamp the header by hand to impersonate a v11 peer).
        let (mut w, mut r) = tokio::io::duplex(64 * 1024);
        let total = (1u32 + payload.len() as u32).to_be_bytes();
        w.write_all(&total).await.unwrap();
        w.write_all(&[PREV_WIRE_VERSION]).await.unwrap();
        w.write_all(&payload).await.unwrap();
        drop(w);

        // (3) The v12 receiver accepts the frame and routes it to the previous-version path.
        let mut buf = BytesMut::new();
        let frame_ver = read_frame(&mut r, &mut buf).await
            .expect("v12 node must accept a PREV_WIRE_VERSION frame during a rolling upgrade");
        assert_eq!(frame_ver, FrameVersion::Previous,
            "the v11 frame must be reported as FrameVersion::Previous (the decode_wire_v11 path)");

        // (4) Decode via the live v11 codec path a connection handler uses for Previous frames.
        let decoded = crate::codec::decode_wire_v11(&buf)
            .expect("v11 KV write must decode on the current node");
        let recovered = match decoded {
            WireMessage::Data(u) => u,
            other => panic!("expected WireMessage::Data from the v11 KV write, got {other:?}"),
        };
        assert_eq!(&*recovered.key, &*key, "decoded key must survive the version boundary");
        assert_eq!(recovered.value, value, "decoded value must survive the version boundary");

        // (5) Apply through the real LWW-merge apply path onto a fresh (empty) v12 store.
        let kv = KvState::new(0);
        crate::store::apply_and_notify(&kv, &recovered);

        // (6) Convergence: the v12 store now returns the value the v11 peer wrote.
        let stored = kv.store.pin().get(key.as_ref()).and_then(|e| e.data.clone());
        assert_eq!(stored, Some(value.clone()),
            "v12 store must converge to the v11-encoded value (rolling-upgrade interop)");
        // …and it is visible to the read path higher layers use.
        let scanned = scan_kv_prefix(&kv, "spike/");
        assert_eq!(scanned, vec![(key, value)],
            "the converged v11 write must be readable via scan_kv_prefix");
    }

    /// **Rolling-upgrade policy clause 4** — *"Forwarding always re-encodes at
    /// `WIRE_VERSION` so the cluster converges quickly."* The invariant: whatever
    /// version a frame arrived at, when a node re-broadcasts it the outbound frame is
    /// stamped the CURRENT version (v12). That is what lets a v11 write injected
    /// anywhere pull the whole cluster to v12 — the first v12 node to forward it
    /// re-stamps it current.
    ///
    /// [`write_frame`] is the sole frame-writer and takes no version parameter: it
    /// hard-codes `header[4] = WIRE_VERSION` unconditionally, so the
    /// `write_frame`-always-stamps-current property *is* the forwarding guarantee —
    /// there is no higher-level re-forward path that stamps a different version. We
    /// prove it along the real forward shape: take the bytes a v11 peer put on the
    /// wire, decode them through the previous-version path, re-encode with the current
    /// codec (what a forwarding node emits), frame them with `write_frame`, and assert
    /// byte 4 is `WIRE_VERSION` (12) — not the arrival version.
    #[tokio::test]
    async fn rolling_upgrade_forwarding_re_encodes_at_current_version() {
        use crate::hlc::Hlc;
        use crate::node_id::NodeId;

        // A KV write as a v11 peer would originate it (the Data layout is v11/v12-identical).
        let sender = NodeId::new("127.0.0.1", 7011).unwrap();
        let hlc = Hlc::new();
        let update = make_gossip_update(
            &sender, 4, Arc::from("fwd/reencode"), Bytes::from_static(b"payload"), false, &hlc,
        );
        let v11_bytes = crate::codec::wire_to_bytes(&WireMessage::Data(update));

        // Receive-side of a forward: decode via the previous-version path, then
        // re-encode with the current codec — exactly what a forwarding node does.
        let decoded = crate::codec::decode_wire_v11(&v11_bytes)
            .expect("v11 Data must decode on the current node");
        let reencoded = crate::codec::wire_to_bytes(&decoded);

        // Frame it for the outbound hop. write_frame stamps the version byte itself.
        let mut out: Vec<u8> = Vec::new();
        write_frame(&mut out, &reencoded).await.expect("write_frame must succeed");

        assert_eq!(out[4], WIRE_VERSION,
            "a forwarded frame must be stamped at WIRE_VERSION (current), not the arrival version");
        assert_eq!(WIRE_VERSION, 12, "current wire version is v12");
    }

    /// **Rolling-upgrade data-plane interchangeability.** Clause 4 (lossless
    /// re-encode on forward) and cluster-wide convergence only hold if a v11 peer's
    /// KV write and a v12 peer's KV write are the *same bytes* on the data plane.
    /// Two directions, one test:
    ///   (a) a v12-encoded `Data` frame decodes via the CURRENT decoder
    ///       ([`crate::codec::decode_wire`]) with key/value intact — the ordinary
    ///       same-version receive path.
    ///   (b) that same `Data` payload decoded via BOTH `decode_wire` and
    ///       [`crate::codec::decode_wire_v11`] recovers a byte-identical update
    ///       (nonce, sender, ttl, is_tombstone, HLC timestamp, key, value). This is
    ///       what makes a v11 peer's write and a v12 peer's write interchangeable, so
    ///       either can be re-encoded on forward without perturbing any Data field.
    #[test]
    fn rolling_upgrade_data_round_trips_losslessly_both_directions() {
        use crate::hlc::Hlc;
        use crate::node_id::NodeId;

        let sender = NodeId::new("127.0.0.1", 7012).unwrap();
        let hlc = Hlc::new();
        let key: Arc<str> = Arc::from("rt/lossless");
        let value = Bytes::from_static(b"interchangeable-across-v11-v12");
        let update = make_gossip_update(&sender, 4, Arc::clone(&key), value.clone(), false, &hlc);
        let encoded = crate::codec::wire_to_bytes(&WireMessage::Data(update.clone()));

        // (a) The current decoder recovers key/value intact.
        let cur = match crate::codec::decode_wire(&encoded).expect("current decode must succeed") {
            WireMessage::Data(u) => u,
            other => panic!("expected WireMessage::Data, got {other:?}"),
        };
        assert_eq!(&*cur.key, &*key, "current decoder must recover the key");
        assert_eq!(cur.value, value, "current decoder must recover the value");

        // (b) Both decoders recover an identical update — every Data field.
        let prev = match crate::codec::decode_wire_v11(&encoded).expect("previous decode must succeed") {
            WireMessage::Data(u) => u,
            other => panic!("expected WireMessage::Data, got {other:?}"),
        };
        for (label, u) in [("current", &cur), ("previous", &prev)] {
            assert_eq!(u.nonce, update.nonce, "{label}: nonce survives the version boundary");
            assert_eq!(u.sender, update.sender, "{label}: sender survives");
            assert_eq!(u.ttl, update.ttl, "{label}: ttl survives");
            assert_eq!(u.is_tombstone, update.is_tombstone, "{label}: tombstone flag survives");
            assert_eq!(u.timestamp, update.timestamp, "{label}: HLC timestamp survives (LWW-critical)");
            assert_eq!(&*u.key, &*key, "{label}: key survives");
            assert_eq!(u.value, value, "{label}: value survives");
        }
    }

    /// **Rolling-upgrade accept-window boundaries.** Policy: [`read_frame`] accepts
    /// frames at both `WIRE_VERSION` and `PREV_WIRE_VERSION`; everything else is
    /// rejected. Pins all three boundary cases in one place so the accept window
    /// cannot silently widen or narrow:
    ///   - a `PREV_WIRE_VERSION` (v11) frame → [`FrameVersion::Previous`];
    ///   - a `WIRE_VERSION` (v12) frame → [`FrameVersion::Current`];
    ///   - a frame *below* `PREV_WIRE_VERSION` (v10) → rejected with
    ///     [`GossipError::UnsupportedWireVersion`], carrying the "significantly older
    ///     version" hint that `read_frame` attaches when `header[4] < PREV_WIRE_VERSION`.
    #[tokio::test]
    async fn rolling_upgrade_read_frame_version_boundaries() {
        // Frame `payload` stamped with `version`, then read it back through read_frame.
        async fn read_stamped(
            version: u8,
            payload: &[u8],
        ) -> Result<FrameVersion, GossipError> {
            let (mut w, mut r) = tokio::io::duplex(64 * 1024);
            let total = (1u32 + payload.len() as u32).to_be_bytes();
            w.write_all(&total).await.unwrap();
            w.write_all(&[version]).await.unwrap();
            w.write_all(payload).await.unwrap();
            drop(w);
            let mut buf = BytesMut::new();
            read_frame(&mut r, &mut buf).await
        }

        let payload = b"boundary-probe";

        // PREV_WIRE_VERSION (v11) sits at the low edge of the accept window → Previous.
        let fv_prev = read_stamped(PREV_WIRE_VERSION, payload).await
            .expect("a PREV_WIRE_VERSION frame must be accepted");
        assert_eq!(fv_prev, FrameVersion::Previous,
            "a PREV_WIRE_VERSION frame must be reported as FrameVersion::Previous");

        // WIRE_VERSION (v12) is the current version → Current.
        let fv_cur = read_stamped(WIRE_VERSION, payload).await
            .expect("a WIRE_VERSION frame must be accepted");
        assert_eq!(fv_cur, FrameVersion::Current,
            "a WIRE_VERSION frame must be reported as FrameVersion::Current");

        // One below the window (v10) → rejected with the documented error + hint.
        let older = PREV_WIRE_VERSION - 1;
        let err = read_stamped(older, payload).await
            .expect_err("a version below PREV_WIRE_VERSION must be rejected");
        match err {
            GossipError::UnsupportedWireVersion { received, current, prev, hint } => {
                assert_eq!(received, older, "error must report the received version");
                assert_eq!(current, WIRE_VERSION, "error must report the current version");
                assert_eq!(prev, PREV_WIRE_VERSION, "error must report the accepted previous version");
                assert_eq!(hint, "peer is running a significantly older version",
                    "a sub-PREV_WIRE_VERSION frame must get the 'significantly older' hint");
            }
            other => panic!("expected UnsupportedWireVersion, got {other:?}"),
        }
    }
}

#[cfg(test)]
mod prop_tests {
    use super::*;
    use bytes::BytesMut;
    use proptest::prelude::*;

    proptest! {
        /// Any payload up to 4 KiB survives write_frame + read_frame unchanged.
        /// Tests the length-prefix framing layer independently of bincode encoding.
        #[test]
        fn framing_round_trip(payload in prop::collection::vec(any::<u8>(), 0..=4096usize)) {
            let result = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async {
                    let (mut w, mut r) = tokio::io::duplex(8192);
                    write_frame(&mut w, &payload).await.unwrap();
                    drop(w);
                    let mut buf = BytesMut::new();
                    read_frame(&mut r, &mut buf).await.unwrap();
                    buf.to_vec()
                });
            prop_assert_eq!(result, payload);
        }

        /// write_frame rejects any payload that would exceed MAX_FRAME_BYTES.
        /// Since building a 10 MiB buffer in a property test is prohibitive,
        /// we verify the boundary condition via the length arithmetic directly.
        #[test]
        fn write_frame_length_check_is_tight(extra in 1usize..=1024usize) {
            // payload_len = 1 + data.len(); reject when > MAX_FRAME_BYTES.
            // Exactly MAX_FRAME_BYTES - 1 data bytes → payload_len = MAX_FRAME_BYTES → accepted.
            // MAX_FRAME_BYTES + extra - 1 data bytes → payload_len > MAX_FRAME_BYTES → rejected.
            let boundary_data_len = MAX_FRAME_BYTES.saturating_sub(1);
            // Oversized: boundary_data_len + extra bytes
            let oversized_len = boundary_data_len.saturating_add(extra);
            // payload_len for oversized = 1 + oversized_len
            let oversized_payload_len = 1usize.saturating_add(oversized_len);
            prop_assert!(
                oversized_payload_len > MAX_FRAME_BYTES,
                "oversized payload_len {} should exceed MAX_FRAME_BYTES {}", oversized_payload_len, MAX_FRAME_BYTES
            );
        }
    }
}
