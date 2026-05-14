use crate::error::GossipError;
use crate::node_id::NodeId;
use crate::signal::SignalScope;
use ahash::RandomState;
use bytes::{BufMut, Bytes, BytesMut};
use std::sync::{Arc, OnceLock};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub(crate) const MAX_FRAME_BYTES: usize = 10 * 1024 * 1024;
/// Framing-level protocol version. Written before every serialized payload;
/// frames with a different version byte are rejected without deserialization.
/// v2: switched serialization from bincode 1.x to bincode 2.x (incompatible wire format).
/// v3: timestamps changed from second to millisecond granularity (incompatible LWW semantics).
/// v4: GossipUpdate.sender changed from NodeId (string) to u64 id_hash (compact wire identity).
/// v5: integer encoding changed from varint to fixed-width (faster encode/decode).
/// v6: GossipUpdate field order changed (nonce, sender, ttl, is_tombstone, timestamp, key, value)
///     to place all fixed-width fields before variable-length fields, enabling in-place TTL
///     decrement and zero-copy forwarding without re-encoding on each hop.
///
/// TODO(rolling-upgrade): when bumping WIRE_VERSION, keep the previous version accepted for one
/// full rolling-upgrade window (all nodes at new version) before dropping old-version frames.
pub(crate) const WIRE_VERSION: u8 = 6;
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
    /// Anti-entropy probe: ask the receiver to reply with its full store snapshot.
    StateRequest { sender: NodeId },
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
/// Uses `read_buf` with a `BufMut::limit` guard to avoid zero-initialising the
/// destination region before `read_exact` fills it (safe, no unsafe code needed).
pub(crate) async fn read_frame<R>(stream: &mut R, buf: &mut BytesMut) -> Result<(), GossipError>
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
    if header[4] != WIRE_VERSION {
        let hint = if header[4] < WIRE_VERSION {
            "peer is running an older version"
        } else {
            "peer is running a newer version"
        };
        return Err(GossipError::Network(format!(
            "Unsupported wire version {} (expected {}; {})",
            header[4], WIRE_VERSION, hint
        )));
    }
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
    Ok(())
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
