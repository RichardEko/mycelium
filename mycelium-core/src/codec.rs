//! Hand-rolled wire codec for the closed `WireMessage` enum and its on-disk / control
//! cousins (WS-B M11). Replaces the unmaintained `bincode` crate (RUSTSEC-2025-0141)
//! with an explicit, fixed-layout encoder/decoder.
//!
//! **Byte-exact with the old format.** The layout reproduces `bincode 2.x`
//! `standard().with_fixed_int_encoding()` byte-for-byte, so this is a *drop-in* swap:
//! no wire-version bump, no on-disk migration, and frames/snapshots written by either
//! codec interop. The `codec::tests` module proves equivalence against `bincode` for
//! every type (bincode is retained as a `dev-dependency` test oracle only).
//!
//! ## Fixed-int layout rules (the bincode subset we reproduce)
//! - integers (`u8`/`u16`/`u32`/`u64`): little-endian, fixed width;
//! - `bool`: one byte, `0` / `1`;
//! - enum variant tag: `u32` LE discriminant (declaration order);
//! - `Option<T>`: one byte tag (`0` = None, `1` = Some) then `T`;
//! - collection / `str` / byte-slice length: `u64` LE, then the elements/bytes;
//! - fixed array `[u8; N]`: `N` raw bytes, no length prefix;
//! - tuple / struct: fields concatenated in declaration order.
//!
//! The `Data` variant keeps `nonce` at byte 4 and `ttl` at byte 20 (see
//! `framing::{NONCE_OFFSET, TTL_OFFSET, DATA_TAG}`) so the zero-copy forwarding path
//! is unchanged.

use crate::framing::{GossipUpdate, SyncEntry, WireMessage};
use crate::node_id::NodeId;
use crate::signal::SignalScope;
use bytes::{BufMut, Bytes, BytesMut};
use std::sync::Arc;

/// A decode failure. Carries a static reason for diagnostics; callers drop the frame
/// (UDP/gossip loss is tolerable) exactly as they did on a bincode decode error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CodecError(pub &'static str);

impl std::fmt::Display for CodecError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "wire codec decode error: {}", self.0)
    }
}
impl std::error::Error for CodecError {}

// ── Encode primitives ───────────────────────────────────────────────────────────

#[inline]
fn put_u32(b: &mut BytesMut, v: u32) { b.put_u32_le(v); }
#[inline]
fn put_u64(b: &mut BytesMut, v: u64) { b.put_u64_le(v); }
#[inline]
fn put_bool(b: &mut BytesMut, v: bool) { b.put_u8(v as u8); }
/// Length-prefixed byte slice: `u64` LE length then the raw bytes (matches bincode's
/// `&[u8]` / `String` / `Vec<u8>` encoding under fixed-int).
#[inline]
fn put_bytes(b: &mut BytesMut, bytes: &[u8]) {
    b.put_u64_le(bytes.len() as u64);
    b.put_slice(bytes);
}
#[inline]
fn put_str(b: &mut BytesMut, s: &str) { put_bytes(b, s.as_bytes()); }
#[inline]
fn put_len(b: &mut BytesMut, n: usize) { b.put_u64_le(n as u64); }

// ── Decode cursor ─────────────────────────────────────────────────────────────────

/// Bounds-checked reader over a frame buffer. Every read returns `Err` rather than
/// panicking on truncation, and length-prefixed reads cap allocation to the remaining
/// buffer (an element is ≥ 1 byte, so a claimed length can never exceed bytes left) —
/// a malformed length cannot drive an unbounded `Vec::with_capacity`.
pub struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Reader { buf, pos: 0 }
    }

    #[inline]
    fn remaining(&self) -> usize { self.buf.len() - self.pos }

    #[inline]
    fn take(&mut self, n: usize) -> Result<&'a [u8], CodecError> {
        if self.remaining() < n {
            return Err(CodecError("unexpected end of frame"));
        }
        let s = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(s)
    }

    #[inline]
    fn u8(&mut self) -> Result<u8, CodecError> { Ok(self.take(1)?[0]) }
    #[inline]
    fn bool(&mut self) -> Result<bool, CodecError> {
        match self.u8()? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(CodecError("invalid bool tag")),
        }
    }
    #[inline]
    fn u32(&mut self) -> Result<u32, CodecError> {
        let s = self.take(4)?;
        Ok(u32::from_le_bytes([s[0], s[1], s[2], s[3]]))
    }
    #[inline]
    fn u64(&mut self) -> Result<u64, CodecError> {
        let s = self.take(8)?;
        Ok(u64::from_le_bytes(s.try_into().unwrap()))
    }
    /// A `u64` length prefix, validated to fit within the remaining buffer.
    #[inline]
    fn len(&mut self) -> Result<usize, CodecError> {
        let n = self.u64()? as usize;
        if n > self.remaining() {
            return Err(CodecError("length prefix exceeds frame"));
        }
        Ok(n)
    }
    fn bytes(&mut self) -> Result<Bytes, CodecError> {
        let n = self.len()?;
        Ok(Bytes::copy_from_slice(self.take(n)?))
    }
    fn arc_str(&mut self) -> Result<Arc<str>, CodecError> {
        let n = self.len()?;
        let s = std::str::from_utf8(self.take(n)?).map_err(|_| CodecError("invalid utf-8"))?;
        Ok(Arc::from(s))
    }
    /// Capacity hint for a collection of `min_elem`-byte elements: bounded by the
    /// remaining buffer so a hostile length cannot pre-allocate gigabytes.
    #[inline]
    fn cap_hint(&self, len: usize, min_elem: usize) -> usize {
        len.min(self.remaining() / min_elem.max(1))
    }

    /// True once the whole buffer is consumed — callers reject trailing garbage.
    #[inline]
    pub fn is_empty(&self) -> bool { self.remaining() == 0 }
}

// ── NodeId (serializes as its address string) ─────────────────────────────────────

fn put_node_id(b: &mut BytesMut, n: &NodeId) { put_str(b, &n.to_string()); }
fn get_node_id(r: &mut Reader) -> Result<NodeId, CodecError> {
    let n = r.len()?;
    let s = std::str::from_utf8(r.take(n)?).map_err(|_| CodecError("invalid utf-8 in node id"))?;
    s.parse::<NodeId>().map_err(|_| CodecError("invalid node id"))
}

// ── SignalScope ─────────────────────────────────────────────────────────────────

fn put_scope(b: &mut BytesMut, s: &SignalScope) {
    match s {
        SignalScope::System          => put_u32(b, 0),
        SignalScope::Group(g)        => { put_u32(b, 1); put_str(b, g); }
        SignalScope::Individual(n)   => { put_u32(b, 2); put_node_id(b, n); }
        SignalScope::Groups(gs)      => {
            put_u32(b, 3);
            put_len(b, gs.len());
            for g in gs { put_str(b, g); }
        }
    }
}
fn get_scope(r: &mut Reader) -> Result<SignalScope, CodecError> {
    Ok(match r.u32()? {
        0 => SignalScope::System,
        1 => SignalScope::Group(r.arc_str()?),
        2 => SignalScope::Individual(get_node_id(r)?),
        3 => {
            let n = r.len()?;
            let mut gs = Vec::with_capacity(r.cap_hint(n, 8));
            for _ in 0..n { gs.push(r.arc_str()?); }
            SignalScope::Groups(gs)
        }
        _ => return Err(CodecError("invalid SignalScope tag")),
    })
}

// ── GossipUpdate / SyncEntry ──────────────────────────────────────────────────────

fn put_update(b: &mut BytesMut, u: &GossipUpdate) {
    put_u64(b, u.nonce);
    put_u64(b, u.sender);
    b.put_u8(u.ttl);
    put_bool(b, u.is_tombstone);
    put_u64(b, u.timestamp);
    put_str(b, &u.key);
    put_bytes(b, &u.value);
}
fn get_update(r: &mut Reader) -> Result<GossipUpdate, CodecError> {
    Ok(GossipUpdate {
        nonce:        r.u64()?,
        sender:       r.u64()?,
        ttl:          r.u8()?,
        is_tombstone: r.bool()?,
        timestamp:    r.u64()?,
        key:          r.arc_str()?,
        value:        r.bytes()?,
    })
}

/// Canonical signed-bytes encoding of a `GossipUpdate`'s hop-invariant fields, byte-exact
/// with the old `bincode` tuple encoding `(nonce, sender, is_tombstone, timestamp, key, value)`
/// under fixed-int. TTL is excluded (it is decremented on each forward), so the originator's
/// Ed25519 signature stays valid through every hop. See `framing::canonical_sign_bytes`.
pub fn canonical_update_bytes(u: &GossipUpdate) -> Vec<u8> {
    let mut b = BytesMut::with_capacity(48 + u.key.len() + u.value.len());
    put_u64(&mut b, u.nonce);
    put_u64(&mut b, u.sender);
    put_bool(&mut b, u.is_tombstone);
    put_u64(&mut b, u.timestamp);
    put_str(&mut b, &u.key);
    put_bytes(&mut b, &u.value);
    b.to_vec()
}

fn put_sync_entry(b: &mut BytesMut, e: &SyncEntry) {
    put_str(b, &e.key);
    put_bytes(b, &e.value);
    put_u64(b, e.timestamp);
    put_bool(b, e.is_tombstone);
}
fn get_sync_entry(r: &mut Reader) -> Result<SyncEntry, CodecError> {
    Ok(SyncEntry {
        key:          r.arc_str()?,
        value:        r.bytes()?,
        timestamp:    r.u64()?,
        is_tombstone: r.bool()?,
    })
}

// ── WireMessage ─────────────────────────────────────────────────────────────────

/// Encode a `WireMessage` into `b`, byte-compatible with the old bincode fixed-int format.
pub fn encode_wire(b: &mut BytesMut, msg: &WireMessage) {
    match msg {
        WireMessage::Data(u) => { put_u32(b, 0); put_update(b, u); }
        WireMessage::Ping { sender, known_peers } => {
            put_u32(b, 1);
            put_node_id(b, sender);
            put_len(b, known_peers.len());
            for p in known_peers { put_node_id(b, p); }
        }
        WireMessage::StateRequest { sender, store_hash, key_timestamps } => {
            put_u32(b, 2);
            put_node_id(b, sender);
            put_u64(b, *store_hash);
            put_len(b, key_timestamps.len());
            for (k, ts) in key_timestamps { put_str(b, k); put_u64(b, *ts); }
        }
        WireMessage::StateResponse { entries } => {
            put_u32(b, 3);
            put_len(b, entries.len());
            for e in entries { put_sync_entry(b, e); }
        }
        WireMessage::Signal { ttl, nonce, sender, scope, kind, payload, hlc_seq } => {
            put_u32(b, 4);
            b.put_u8(*ttl);
            put_u64(b, *nonce);
            put_node_id(b, sender);
            put_scope(b, scope);
            put_str(b, kind);
            put_bytes(b, payload);
            match hlc_seq {
                None      => b.put_u8(0),
                Some(seq) => { b.put_u8(1); put_u64(b, *seq); }
            }
        }
        WireMessage::SignedData { update, signer, signature } => {
            put_u32(b, 5);
            put_update(b, update);
            put_u64(b, *signer);
            b.put_slice(&signature.0);
            b.put_slice(&signature.1);
        }
    }
}

/// Encode a `WireMessage` to an owned `Bytes` — convenience for call sites that
/// previously did `bincode::encode_into_std_write(msg, buf, cfg)?; buf.freeze()`.
/// Infallible (the codec cannot fail to serialize an in-memory message).
pub fn wire_to_bytes(msg: &WireMessage) -> Bytes {
    let mut b = BytesMut::with_capacity(256);
    encode_wire(&mut b, msg);
    b.freeze()
}

/// Decode one `WireMessage` from `buf`. Rejects trailing bytes (a well-formed frame
/// is consumed exactly).
pub fn decode_wire(buf: &[u8]) -> Result<WireMessage, CodecError> {
    let mut r = Reader::new(buf);
    let msg = match r.u32()? {
        0 => WireMessage::Data(get_update(&mut r)?),
        1 => {
            let sender = get_node_id(&mut r)?;
            let n = r.len()?;
            let mut known_peers = Vec::with_capacity(r.cap_hint(n, 8));
            for _ in 0..n { known_peers.push(get_node_id(&mut r)?); }
            WireMessage::Ping { sender, known_peers }
        }
        2 => {
            let sender = get_node_id(&mut r)?;
            let store_hash = r.u64()?;
            let n = r.len()?;
            let mut key_timestamps = Vec::with_capacity(r.cap_hint(n, 16));
            for _ in 0..n {
                let k = r.arc_str()?;
                let ts = r.u64()?;
                key_timestamps.push((k, ts));
            }
            WireMessage::StateRequest { sender, store_hash, key_timestamps }
        }
        3 => {
            let n = r.len()?;
            let mut entries = Vec::with_capacity(r.cap_hint(n, 18));
            for _ in 0..n { entries.push(get_sync_entry(&mut r)?); }
            WireMessage::StateResponse { entries }
        }
        4 => {
            let ttl = r.u8()?;
            let nonce = r.u64()?;
            let sender = get_node_id(&mut r)?;
            let scope = get_scope(&mut r)?;
            let kind = r.arc_str()?;
            let payload = r.bytes()?;
            let hlc_seq = match r.u8()? {
                0 => None,
                1 => Some(r.u64()?),
                _ => return Err(CodecError("invalid Option tag")),
            };
            WireMessage::Signal { ttl, nonce, sender, scope, kind, payload, hlc_seq }
        }
        5 => {
            let update = get_update(&mut r)?;
            let signer = r.u64()?;
            let lo: [u8; 32] = r.take(32)?.try_into().unwrap();
            let hi: [u8; 32] = r.take(32)?.try_into().unwrap();
            WireMessage::SignedData { update, signer, signature: (lo, hi) }
        }
        _ => return Err(CodecError("invalid WireMessage tag")),
    };
    if !r.is_empty() {
        return Err(CodecError("trailing bytes after WireMessage"));
    }
    Ok(msg)
}

/// Decode one `PREV_WIRE_VERSION` (v10) `WireMessage`. The v10 layout is identical to
/// the current one except `Signal` has no trailing `hlc_seq` field; it is filled with
/// `None` (the "unordered delivery" sentinel) on upgrade — byte-exact with the old
/// `WireMessageV10` → `WireMessage` bincode path. All other variants are unchanged.
pub fn decode_wire_v10(buf: &[u8]) -> Result<WireMessage, CodecError> {
    let mut r = Reader::new(buf);
    let msg = match r.u32()? {
        0 => WireMessage::Data(get_update(&mut r)?),
        1 => {
            let sender = get_node_id(&mut r)?;
            let n = r.len()?;
            let mut known_peers = Vec::with_capacity(r.cap_hint(n, 8));
            for _ in 0..n { known_peers.push(get_node_id(&mut r)?); }
            WireMessage::Ping { sender, known_peers }
        }
        2 => {
            let sender = get_node_id(&mut r)?;
            let store_hash = r.u64()?;
            let n = r.len()?;
            let mut key_timestamps = Vec::with_capacity(r.cap_hint(n, 16));
            for _ in 0..n {
                let k = r.arc_str()?;
                let ts = r.u64()?;
                key_timestamps.push((k, ts));
            }
            WireMessage::StateRequest { sender, store_hash, key_timestamps }
        }
        3 => {
            let n = r.len()?;
            let mut entries = Vec::with_capacity(r.cap_hint(n, 18));
            for _ in 0..n { entries.push(get_sync_entry(&mut r)?); }
            WireMessage::StateResponse { entries }
        }
        4 => {
            // v10 Signal: no trailing hlc_seq byte.
            let ttl = r.u8()?;
            let nonce = r.u64()?;
            let sender = get_node_id(&mut r)?;
            let scope = get_scope(&mut r)?;
            let kind = r.arc_str()?;
            let payload = r.bytes()?;
            WireMessage::Signal { ttl, nonce, sender, scope, kind, payload, hlc_seq: None }
        }
        5 => {
            let update = get_update(&mut r)?;
            let signer = r.u64()?;
            let lo: [u8; 32] = r.take(32)?.try_into().unwrap();
            let hi: [u8; 32] = r.take(32)?.try_into().unwrap();
            WireMessage::SignedData { update, signer, signature: (lo, hi) }
        }
        _ => return Err(CodecError("invalid WireMessage tag")),
    };
    if !r.is_empty() {
        return Err(CodecError("trailing bytes after WireMessage"));
    }
    Ok(msg)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::framing::bincode_cfg;
    use crate::framing::WireMessageV10;

    fn nid(p: u16) -> NodeId { NodeId::new("127.0.0.1", p).unwrap() }

    fn bincode_bytes(m: &WireMessage) -> Vec<u8> {
        bincode::serde::encode_to_vec(m, bincode_cfg()).unwrap()
    }

    fn sample_messages() -> Vec<WireMessage> {
        let upd = GossipUpdate {
            nonce: 0xDEAD_BEEF_0102_0304, sender: 42, ttl: 7, is_tombstone: true,
            timestamp: 0x0001_0002_0003_0004, key: Arc::from("cap/n/k"),
            value: Bytes::from_static(b"hello world payload"),
        };
        vec![
            WireMessage::Data(GossipUpdate { is_tombstone: false, value: Bytes::new(), ..upd.clone() }),
            WireMessage::Data(upd.clone()),
            WireMessage::Ping { sender: nid(7000), known_peers: vec![] },
            WireMessage::Ping { sender: nid(7000), known_peers: vec![nid(7001), nid(7002)] },
            WireMessage::StateRequest { sender: nid(7000), store_hash: 0, key_timestamps: vec![] },
            WireMessage::StateRequest { sender: nid(7000), store_hash: 99,
                key_timestamps: vec![(Arc::from("a"), 1), (Arc::from("bb"), 2)] },
            WireMessage::StateResponse { entries: vec![] },
            WireMessage::StateResponse { entries: vec![
                SyncEntry { key: Arc::from("k1"), value: Bytes::from_static(b"v1"), timestamp: 5, is_tombstone: false },
                SyncEntry { key: Arc::from("k2"), value: Bytes::new(), timestamp: 6, is_tombstone: true },
            ] },
            WireMessage::Signal { ttl: 3, nonce: 1, sender: nid(7000), scope: SignalScope::System,
                kind: Arc::from("sys.x"), payload: Bytes::from_static(b"p"), hlc_seq: None },
            WireMessage::Signal { ttl: 4, nonce: 2, sender: nid(7000), scope: SignalScope::Group(Arc::from("g")),
                kind: Arc::from("g.x"), payload: Bytes::new(), hlc_seq: Some(123) },
            WireMessage::Signal { ttl: 5, nonce: 3, sender: nid(7000), scope: SignalScope::Individual(nid(7009)),
                kind: Arc::from("i.x"), payload: Bytes::from_static(b"zz"), hlc_seq: None },
            WireMessage::Signal { ttl: 6, nonce: 4, sender: nid(7000),
                scope: SignalScope::Groups(vec![Arc::from("a"), Arc::from("b")]),
                kind: Arc::from("gs.x"), payload: Bytes::new(), hlc_seq: Some(0) },
            WireMessage::SignedData { update: upd, signer: 7, signature: ([0xAB; 32], [0xCD; 32]) },
        ]
    }

    #[test]
    fn codec_matches_bincode_byte_for_byte() {
        for m in sample_messages() {
            let mut mine = BytesMut::new();
            encode_wire(&mut mine, &m);
            assert_eq!(&mine[..], &bincode_bytes(&m)[..], "encode mismatch for {m:?}");
        }
    }

    #[test]
    fn codec_round_trips_and_decodes_bincode_bytes() {
        for m in sample_messages() {
            let mut mine = BytesMut::new();
            encode_wire(&mut mine, &m);
            // our bytes decode back
            let back = decode_wire(&mine).expect("round trip");
            let mut reenc = BytesMut::new();
            encode_wire(&mut reenc, &back);
            assert_eq!(mine, reenc, "round-trip re-encode mismatch for {m:?}");
            // bincode's bytes decode through our decoder too (interop)
            let from_bincode = decode_wire(&bincode_bytes(&m)).expect("decode bincode bytes");
            let mut reenc2 = BytesMut::new();
            encode_wire(&mut reenc2, &from_bincode);
            assert_eq!(mine, reenc2, "bincode-interop mismatch for {m:?}");
        }
    }

    #[test]
    fn decode_wire_v10_matches_bincode_shim() {
        // A v10 Signal frame (no hlc_seq) must decode to hlc_seq=None, byte-exact with
        // the old WireMessageV10 → WireMessage bincode path.
        let v10 = WireMessageV10::Signal {
            ttl: 5, nonce: 0xDEAD_BEEF, sender: nid(7000),
            scope: SignalScope::Group(Arc::from("g")),
            kind: Arc::from("k"), payload: Bytes::from_static(b"body"),
        };
        let bytes = bincode::serde::encode_to_vec(&v10, bincode_cfg()).unwrap();
        let via_codec = decode_wire_v10(&bytes).expect("v10 codec decode");
        let (via_bincode, _) =
            bincode::serde::decode_from_slice::<WireMessageV10, _>(&bytes, bincode_cfg()).unwrap();
        let via_bincode: WireMessage = via_bincode.into();
        let mut a = BytesMut::new();
        let mut b = BytesMut::new();
        encode_wire(&mut a, &via_codec);
        encode_wire(&mut b, &via_bincode);
        assert_eq!(a, b, "v10 codec decode must match bincode shim");
        match via_codec {
            WireMessage::Signal { hlc_seq, .. } => assert_eq!(hlc_seq, None),
            other => panic!("expected Signal, got {other:?}"),
        }
        // A non-Signal v10 variant (Data) decodes identically through both paths.
        let v10d = WireMessageV10::Data(GossipUpdate {
            nonce: 1, sender: 2, ttl: 3, is_tombstone: false,
            timestamp: 4, key: Arc::from("k"), value: Bytes::from_static(b"v"),
        });
        let dbytes = bincode::serde::encode_to_vec(&v10d, bincode_cfg()).unwrap();
        let mut x = BytesMut::new();
        let mut y = BytesMut::new();
        encode_wire(&mut x, &decode_wire(&dbytes).unwrap());
        encode_wire(&mut y, &decode_wire_v10(&dbytes).unwrap());
        assert_eq!(x, y, "Data layout is identical across v10/v11");
    }

    #[test]
    fn decode_rejects_truncation_and_trailing_garbage() {
        let m = WireMessage::Ping { sender: nid(7000), known_peers: vec![nid(7001)] };
        let mut full = BytesMut::new();
        encode_wire(&mut full, &m);
        // truncations never panic, always Err
        for cut in 0..full.len() {
            assert!(decode_wire(&full[..cut]).is_err(), "truncation at {cut} must error");
        }
        // trailing garbage rejected
        let mut extra = full.clone();
        extra.put_u8(0xFF);
        assert!(decode_wire(&extra).is_err(), "trailing byte must error");
    }

    #[test]
    fn canonical_update_bytes_matches_bincode_tuple() {
        let u = GossipUpdate {
            nonce: 0xAABB_CCDD_1122_3344, sender: 0x99, ttl: 7, is_tombstone: true,
            timestamp: 0x0102_0304_0506_0708, key: Arc::from("sys/identity/x"),
            value: Bytes::from_static(b"signed payload bytes"),
        };
        let mine = canonical_update_bytes(&u);
        let theirs = bincode::serde::encode_to_vec(
            (u.nonce, u.sender, u.is_tombstone, u.timestamp, u.key.as_ref(), u.value.as_ref()),
            bincode_cfg(),
        ).unwrap();
        assert_eq!(mine, theirs, "canonical signed bytes must match bincode tuple");
    }

    #[test]
    fn data_fast_path_offsets_preserved() {
        // nonce@4, ttl@20, tag = DATA_TAG — the zero-copy forward path depends on these.
        let m = WireMessage::Data(GossipUpdate {
            nonce: 0x1122_3344_5566_7788, sender: 0, ttl: 9, is_tombstone: false,
            timestamp: 0, key: Arc::from("k"), value: Bytes::new(),
        });
        let mut b = BytesMut::new();
        encode_wire(&mut b, &m);
        assert_eq!(&b[0..4], &crate::framing::DATA_TAG, "data tag");
        assert_eq!(&b[crate::framing::NONCE_OFFSET..crate::framing::NONCE_OFFSET + 8],
                   &0x1122_3344_5566_7788u64.to_le_bytes(), "nonce offset");
        assert_eq!(b[crate::framing::TTL_OFFSET], 9, "ttl offset");
    }

    #[test]
    fn adversarial_bytes_never_panic() {
        // Decoder must return Err (never panic/OOM) on arbitrary input.
        let mut rng = fastrand::Rng::with_seed(0xC0DEC);
        for _ in 0..20_000 {
            let len = rng.usize(0..64);
            let mut v = vec![0u8; len];
            for byte in &mut v { *byte = rng.u8(..); }
            let _ = decode_wire(&v); // must not panic
        }
    }
}
