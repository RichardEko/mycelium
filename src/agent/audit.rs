//! WS2 — durable, tamper-evident audit trail.
//!
//! Each node maintains its **own** hash-chained stream of audit records under
//! `sys/audit/{node}/{seq:016x}`. A single global chain would need a sequencer —
//! a coordinator — which the substrate's first principle forbids, so the chain is
//! **per-node**: every record hash-links to its predecessor in the same node's
//! stream, and the cluster-wide trail is the union of independently verifiable
//! streams. Records are Ed25519-signed by the writing node's identity key, so a
//! record cannot be forged or re-attributed; the hash-chain additionally proves
//! that within a stream no record was removed, reordered, or back-dated.
//!
//! **Detection, not prevention** (house style): the trail records and proves; it
//! never blocks a write. The records live in plain KV — a tamperer can edit the
//! bytes, but [`verify_chain`] then fails, which is the whole point.
//!
//! **Forward-compat (M16 / NANDA):** [`AuditRecord::content_hash`] is the stable,
//! citable per-record identifier the self-attestation consumer references. It is
//! named for what it is (a content hash), never after any AgentFacts field.
//!
//! Gated behind the `compliance` feature (implies `tls`).

use crate::framing::bincode_cfg;
use crate::node_id::NodeId;
use bytes::{BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// KV prefix for the cluster-wide audit trail. One sub-stream per node.
pub const AUDIT_PREFIX: &str = "sys/audit/";

/// KV key for a single record: `sys/audit/{node}/{seq:016x}`. Zero-padded hex
/// seq gives lexicographic = chronological order within a node's stream.
pub fn audit_key(node: &NodeId, seq: u64) -> String {
    format!("{AUDIT_PREFIX}{node}/{seq:016x}")
}

/// Prefix scan key for one node's full stream: `sys/audit/{node}/`.
pub fn audit_stream_prefix(node: &NodeId) -> String {
    format!("{AUDIT_PREFIX}{node}/")
}

/// The kind of event recorded.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum AuditAction {
    /// A write to a resource (KV set, capability advertise…).
    Write,
    /// A read of a resource — the read-side principal-binding facet.
    Read,
    /// A capability / skill invocation.
    Invoke,
    /// An administrative action (role grant, config change…).
    Admin,
}

/// How the event resolved.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum AuditOutcome {
    Success,
    /// Authorization denied (e.g. failed `caller_authorized` / gateway scope).
    Denied,
    Error,
}

/// One event in a node's hash-chained audit stream.
///
/// `prev_hash` is the SHA-256 [`content_hash`](Self::content_hash) of the
/// predecessor record in the same stream (all-zero for the genesis record). The
/// signature and the content-hash both cover the *entire* record including
/// `prev_hash`, so a record is bound to its position in the chain.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuditRecord {
    /// The recording node — owner of this stream/chain.
    pub node_id: NodeId,
    /// Per-node monotonic sequence; the genesis record is `0`.
    pub seq: u64,
    /// HLC timestamp at seal time (packed `u64`).
    pub hlc: u64,
    /// The principal that caused the event: a verified NodeId string under the
    /// `tls` identity, or another caller identity. Bound into the signature.
    pub principal: String,
    pub action: AuditAction,
    /// The resource acted upon — a KV key, `ns/name` capability, endpoint, etc.
    pub target: String,
    pub outcome: AuditOutcome,
    /// Optional free-form detail (small JSON or text).
    pub detail: Option<String>,
    /// SHA-256 content hash of the predecessor in this stream; all-zero at genesis.
    pub prev_hash: [u8; 32],
}

impl AuditRecord {
    /// Canonical bytes the content-hash and signature both cover — the whole
    /// record, so the hash binds every field including `prev_hash` (the chain
    /// link) and `seq` (the position).
    fn canonical(&self) -> Vec<u8> {
        let mut buf = BytesMut::new();
        let _ = bincode::serde::encode_into_std_write(self, &mut (&mut buf).writer(), bincode_cfg());
        buf.to_vec()
    }

    /// Stable SHA-256 content hash of this record — the citable per-record
    /// identifier (M16 self-attestation references it). Deterministic for a given
    /// logical record; changes if any field is edited.
    pub fn content_hash(&self) -> [u8; 32] {
        let mut h = Sha256::new();
        h.update(self.canonical());
        h.finalize().into()
    }
}

/// An [`AuditRecord`] plus the writer's Ed25519 signature over its canonical
/// bytes. This is the value stored at `sys/audit/{node}/{seq}`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignedAuditRecord {
    pub record: AuditRecord,
    /// 64-byte Ed25519 signature over `record.canonical()`.
    pub sig: Vec<u8>,
}

impl SignedAuditRecord {
    /// Seal `record` with the node's identity signing key. `compliance` implies
    /// `tls`, so the signing API is always available in this build.
    pub fn sign(record: AuditRecord, signing_key: &ed25519_dalek::SigningKey) -> Self {
        let sig = crate::tls::sign_bytes(signing_key, &record.canonical()).to_vec();
        Self { record, sig }
    }

    /// Verify the signature against the writer's 32-byte verifying key (from
    /// `sys/identity/{node}` → `peer_keys`).
    pub fn verify(&self, verifying_key: &[u8; 32]) -> bool {
        crate::tls::verify_bytes(verifying_key, &self.record.canonical(), &self.sig)
    }

    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::new();
        let _ = bincode::serde::encode_into_std_write(self, &mut (&mut buf).writer(), bincode_cfg());
        buf.freeze()
    }

    pub fn decode(bytes: &[u8]) -> Option<Self> {
        bincode::serde::decode_from_slice(bytes, bincode_cfg()).ok().map(|(v, _)| v)
    }
}

/// Why a chain failed to verify. Each variant names the offending `seq` so an
/// inspector can point at the exact record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuditVerifyError {
    /// A record's signature did not verify against the stream owner's key —
    /// the record was edited or signed by the wrong key.
    BadSignature { seq: u64 },
    /// A record's `prev_hash` did not equal its predecessor's content hash —
    /// a record was edited, removed, or reordered.
    BrokenLink { seq: u64 },
    /// The stream's sequence numbers are not contiguous — a record is missing
    /// or the order is wrong.
    SequenceGap { expected: u64, found: u64 },
    /// A record claims a different owner than the stream it appears in.
    WrongOwner { seq: u64 },
}

/// Verify a contiguous slice of one node's stream.
///
/// Checks, for each record in order: it is owned by `owner`, its `seq` is
/// contiguous from `expected_first_seq`, its `prev_hash` matches the running
/// chain hash (starting from `expected_first_prev`), and its signature verifies
/// against `verifying_key`. To verify a whole stream from genesis pass
/// `(expected_first_seq = 0, expected_first_prev = [0; 32])` — or use
/// [`verify_stream_from_genesis`]; to verify a mid-stream range pass the known
/// boundary `(seq, prev_hash)` of the first returned record.
pub fn verify_chain(
    records: &[SignedAuditRecord],
    owner: &NodeId,
    verifying_key: &[u8; 32],
    expected_first_seq: u64,
    expected_first_prev: [u8; 32],
) -> Result<(), AuditVerifyError> {
    let mut prev = expected_first_prev;
    for (i, sr) in records.iter().enumerate() {
        let r = &sr.record;
        let expect_seq = expected_first_seq + i as u64;
        if &r.node_id != owner {
            return Err(AuditVerifyError::WrongOwner { seq: r.seq });
        }
        if r.seq != expect_seq {
            return Err(AuditVerifyError::SequenceGap { expected: expect_seq, found: r.seq });
        }
        if r.prev_hash != prev {
            return Err(AuditVerifyError::BrokenLink { seq: r.seq });
        }
        if !sr.verify(verifying_key) {
            return Err(AuditVerifyError::BadSignature { seq: r.seq });
        }
        prev = r.content_hash();
    }
    Ok(())
}

/// Verify a full stream starting at the genesis record (seq 0, zero prev_hash).
pub fn verify_stream_from_genesis(
    records: &[SignedAuditRecord],
    owner: &NodeId,
    verifying_key: &[u8; 32],
) -> Result<(), AuditVerifyError> {
    verify_chain(records, owner, verifying_key, 0, [0u8; 32])
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::SigningKey;

    fn key(seed: u8) -> SigningKey { SigningKey::from_bytes(&[seed; 32]) }
    fn node() -> NodeId { NodeId::new("127.0.0.1", 7100).unwrap() }

    /// Build a signed chain of `n` records for `owner`, sealed with `sk`.
    fn build_chain(owner: &NodeId, sk: &SigningKey, n: u64) -> Vec<SignedAuditRecord> {
        let mut out = Vec::new();
        let mut prev = [0u8; 32];
        for seq in 0..n {
            let rec = AuditRecord {
                node_id: owner.clone(),
                seq,
                hlc: seq + 1,
                principal: format!("10.0.0.{seq}:9000"),
                action: AuditAction::Invoke,
                target: format!("skill/job-{seq}"),
                outcome: AuditOutcome::Success,
                detail: None,
                prev_hash: prev,
            };
            prev = rec.content_hash();
            out.push(SignedAuditRecord::sign(rec, sk));
        }
        out
    }

    #[test]
    fn genesis_chain_verifies() {
        let sk = key(11);
        let vk = sk.verifying_key().to_bytes();
        let chain = build_chain(&node(), &sk, 4);
        assert_eq!(verify_stream_from_genesis(&chain, &node(), &vk), Ok(()));
    }

    #[test]
    fn empty_chain_verifies() {
        let sk = key(11);
        let vk = sk.verifying_key().to_bytes();
        assert_eq!(verify_stream_from_genesis(&[], &node(), &vk), Ok(()));
    }

    #[test]
    fn editing_a_field_breaks_the_signature() {
        let sk = key(12);
        let vk = sk.verifying_key().to_bytes();
        let mut chain = build_chain(&node(), &sk, 3);
        // Tamper with record 1's target without re-signing.
        chain[1].record.target = "skill/EVIL".into();
        assert_eq!(
            verify_stream_from_genesis(&chain, &node(), &vk),
            Err(AuditVerifyError::BadSignature { seq: 1 })
        );
    }

    #[test]
    fn removing_a_record_breaks_the_chain() {
        let sk = key(13);
        let vk = sk.verifying_key().to_bytes();
        let mut chain = build_chain(&node(), &sk, 4);
        // Drop the middle record (seq 2). Remaining records are individually
        // valid, but seq 3's prev_hash no longer matches seq 1's content hash,
        // and the sequence is no longer contiguous.
        chain.remove(2);
        assert_eq!(
            verify_stream_from_genesis(&chain, &node(), &vk),
            Err(AuditVerifyError::SequenceGap { expected: 2, found: 3 })
        );
    }

    #[test]
    fn reordering_records_is_detected() {
        let sk = key(14);
        let vk = sk.verifying_key().to_bytes();
        let mut chain = build_chain(&node(), &sk, 3);
        chain.swap(0, 1);
        // First record now has seq 1, but genesis verification expects seq 0.
        assert_eq!(
            verify_stream_from_genesis(&chain, &node(), &vk),
            Err(AuditVerifyError::SequenceGap { expected: 0, found: 1 })
        );
    }

    #[test]
    fn wrong_key_fails_verification() {
        let sk = key(15);
        let other = key(99).verifying_key().to_bytes();
        let chain = build_chain(&node(), &sk, 2);
        assert_eq!(
            verify_stream_from_genesis(&chain, &node(), &other),
            Err(AuditVerifyError::BadSignature { seq: 0 })
        );
    }

    #[test]
    fn mid_stream_range_verifies_with_known_boundary() {
        let sk = key(16);
        let vk = sk.verifying_key().to_bytes();
        let chain = build_chain(&node(), &sk, 5);
        // Verify the tail [2..] given the known boundary of record 2.
        let boundary_seq = chain[2].record.seq;
        let boundary_prev = chain[2].record.prev_hash;
        assert_eq!(
            verify_chain(&chain[2..], &node(), &vk, boundary_seq, boundary_prev),
            Ok(())
        );
        // A wrong boundary prev is caught.
        assert_eq!(
            verify_chain(&chain[2..], &node(), &vk, boundary_seq, [7u8; 32]),
            Err(AuditVerifyError::BrokenLink { seq: 2 })
        );
    }

    #[test]
    fn content_hash_is_stable_and_sensitive() {
        let rec = AuditRecord {
            node_id: node(), seq: 0, hlc: 1, principal: "p".into(),
            action: AuditAction::Read, target: "kv/secret".into(),
            outcome: AuditOutcome::Success, detail: None, prev_hash: [0u8; 32],
        };
        let h1 = rec.content_hash();
        assert_eq!(h1, rec.content_hash(), "stable across calls");
        let mut edited = rec.clone();
        edited.target = "kv/other".into();
        assert_ne!(h1, edited.content_hash(), "sensitive to field edits");
    }

    #[test]
    fn encode_decode_roundtrip() {
        let sk = key(17);
        let chain = build_chain(&node(), &sk, 1);
        let bytes = chain[0].encode();
        let back = SignedAuditRecord::decode(&bytes).expect("decode");
        assert_eq!(back, chain[0]);
    }

    #[test]
    fn audit_key_is_lexicographically_ordered() {
        let n = node();
        assert!(audit_key(&n, 1) < audit_key(&n, 2));
        assert!(audit_key(&n, 9) < audit_key(&n, 10), "zero-padded hex sorts numerically");
        assert!(audit_key(&n, 1).starts_with(&audit_stream_prefix(&n)));
    }
}
