//! Key revocation (WS-D / Quilt-DD#2, track 1 · D1) — closing the WS5 compromise caveat.
//!
//! WS5's retained-key-set rotation keeps a *retired* key trusted for verification forever (so
//! historical signatures stay valid across a rotation). That is right for *hygiene* rotation but
//! wrong for *compromise*: once a key is known-compromised, nothing it ever signed should be
//! trusted. This module adds explicit revocation — gossip-replicated, per-node, **coordinator-free**
//! (a new owned KV namespace, no central CT operator).
//!
//! ## Model
//!
//! A node revokes one of its own verifying keys by writing a **signed** [`RevocationEvent`] to
//! `sys/revocation/{node}/{revoked-key-hex}` (idempotent — keyed by the revoked key). It gossips
//! like any KV value. Every `compliance` retained-key verify path — via
//! [`super::helpers::known_verifying_keys`] — then **excludes** revoked keys, so a key signed by a
//! revoked key fails verification cluster-wide once the revocation has propagated ("sub-second
//! revocation", bounded by gossip latency).
//!
//! ## Validation rule (who may revoke what)
//!
//! A revocation of key `R` for node `N` counts **iff** it is signed by `N`'s **current** identity
//! key (`sys/identity/{N}`, current-first) *and* `R` is in `N`'s identity history. So:
//!
//! - Only the holder of the *current* key can revoke — a compromised *old* key cannot revoke
//!   anything (it is not the current key), which is exactly the case this closes: the legitimate
//!   owner rotates to a fresh key, then revokes the old (now-compromised) one *with the new key*.
//! - A node can only revoke *its own* keys (ownership check), never another node's.
//!
//! Detection-not-prevention still applies to forging the KV bytes: a forged `sys/revocation/` write
//! is LWW-accepted by the substrate but fails this validation at read, so it has no effect (it
//! cannot revoke a key it does not legitimately own).
//!
//! D2 adds Merkle inclusion proofs over these events (`/gateway/transparency`); D1 is the
//! exclusion semantics that make revocation *work*.

use ed25519_dalek::{Signature, SigningKey, Verifier, VerifyingKey};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

use crate::node_id::NodeId;
use super::TaskCtx;

/// KV namespace owned by the revocation log: `sys/revocation/{node}/{revoked-key-hex}`.
pub const REVOCATION_PREFIX: &str = "sys/revocation/";

fn hex32(k: &[u8; 32]) -> String {
    let mut s = String::with_capacity(64);
    for b in k {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

/// KV key for a node's revocation of one verifying key.
pub fn revocation_key(node: &NodeId, revoked_key: &[u8; 32]) -> String {
    format!("{REVOCATION_PREFIX}{node}/{}", hex32(revoked_key))
}

/// One revocation event: node `node_id` retires `revoked_key` as compromised.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RevocationEvent {
    pub node_id:     NodeId,
    /// HLC timestamp at seal time (packed `u64`).
    pub hlc:         u64,
    /// The verifying key being revoked (one of `node_id`'s own keys).
    pub revoked_key: [u8; 32],
    pub reason:      Option<String>,
}

impl RevocationEvent {
    fn canonical(&self) -> Vec<u8> {
        mycelium_core::serde_fixint::to_vec(self).unwrap_or_default()
    }
}

/// A [`RevocationEvent`] plus the writer's Ed25519 signature over its canonical bytes. Stored at
/// `sys/revocation/{node}/{revoked-key-hex}`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignedRevocation {
    pub event: RevocationEvent,
    pub sig:   Vec<u8>,
}

impl SignedRevocation {
    fn sign(event: RevocationEvent, signing_key: &SigningKey) -> Self {
        let sig = crate::tls::sign_bytes(signing_key, &event.canonical()).to_vec();
        Self { event, sig }
    }

    /// Verify the signature against `verifying_key`.
    fn verify(&self, verifying_key: &[u8; 32]) -> bool {
        let Ok(sig) = <[u8; 64]>::try_from(self.sig.as_slice()) else { return false };
        let Ok(vk) = VerifyingKey::from_bytes(verifying_key) else { return false };
        vk.verify(&self.event.canonical(), &Signature::from_bytes(&sig)).is_ok()
    }

    pub(crate) fn encode(&self) -> bytes::Bytes {
        bytes::Bytes::from(mycelium_core::serde_fixint::to_vec(self).unwrap_or_default())
    }

    pub(crate) fn decode(bytes: &[u8]) -> Option<Self> {
        mycelium_core::serde_fixint::from_slice(bytes).ok()
    }
}

/// Append a signed revocation of `revoked_key` for *this* node. Requires the `tls` identity (the
/// revocation is signed by the current key). Returns `Err` without a tls identity. The write
/// gossips; once propagated, every node's verify paths exclude `revoked_key`.
pub(crate) fn revoke_key(
    ctx: &TaskCtx,
    revoked_key: [u8; 32],
    reason: Option<String>,
) -> Result<(), crate::error::GossipError> {
    let tls = ctx.tls.get().ok_or(crate::error::GossipError::InvalidField {
        field:  "tls",
        reason: "revocation requires the tls identity (set GossipConfig::tls)".into(),
    })?;
    let event = RevocationEvent {
        node_id:     ctx.node_id.clone(),
        hlc:         ctx.hlc.tick(),
        revoked_key,
        reason,
    };
    let signed = SignedRevocation::sign(event, &tls.signing_key());
    let key = revocation_key(&ctx.node_id, &revoked_key);
    let _ = mycelium_core::kv_handle::KvHandle::from_core(std::sync::Arc::clone(&ctx.core))
        .set(key, signed.encode());
    Ok(())
}

/// The set of verifying keys that have been **validly** revoked, read from the local gossip view.
/// A revocation counts only if signed by the revoking node's *current* identity key and the revoked
/// key is in that node's identity history (see the module-level validation rule). A forged or
/// foreign-signed revocation is ignored.
pub(crate) fn revoked_key_set(ctx: &TaskCtx) -> HashSet<[u8; 32]> {
    let mut revoked = HashSet::new();
    for (key, bytes) in crate::store::scan_kv_prefix(&ctx.kv_state, REVOCATION_PREFIX) {
        let Some(rest) = key.strip_prefix(REVOCATION_PREFIX) else { continue };
        let Some((node_seg, _)) = rest.split_once('/') else { continue };
        let Ok(node) = node_seg.parse::<NodeId>() else { continue };
        let Some(signed) = SignedRevocation::decode(&bytes) else { continue };
        if signed.event.node_id != node {
            continue; // key path must match the event's claimed owner
        }
        // The revoking node's identity history (current first).
        let identity = mycelium_core::kv_handle::KvHandle::from_core(std::sync::Arc::clone(&ctx.core))
            .get(&format!("sys/identity/{node}"))
            .map(|b| super::helpers::parse_identity_keys(&b))
            .unwrap_or_default();
        let Some(current) = identity.first() else { continue };
        // Valid iff signed by the current key AND the revoked key is one of the node's own keys.
        if signed.verify(current) && identity.contains(&signed.event.revoked_key) {
            revoked.insert(signed.event.revoked_key);
        }
    }
    revoked
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::SigningKey;

    fn key(seed: u8) -> ([u8; 32], SigningKey) {
        let sk = SigningKey::from_bytes(&[seed; 32]);
        (sk.verifying_key().to_bytes(), sk)
    }

    #[test]
    fn signed_revocation_round_trips_and_verifies() {
        let (pub_a, sk_a) = key(1);
        let event = RevocationEvent {
            node_id: "127.0.0.1:9000".parse().unwrap(),
            hlc: 42,
            revoked_key: [9u8; 32],
            reason: Some("compromise".into()),
        };
        let signed = SignedRevocation::sign(event, &sk_a);
        assert!(signed.verify(&pub_a), "verifies against the signer");
        let (pub_b, _) = key(2);
        assert!(!signed.verify(&pub_b), "rejects a different key");
        let round = SignedRevocation::decode(&signed.encode()).unwrap();
        assert_eq!(round, signed, "encode/decode round-trips");
    }

    #[test]
    fn tampering_the_event_breaks_the_signature() {
        let (pub_a, sk_a) = key(1);
        let event = RevocationEvent {
            node_id: "127.0.0.1:9000".parse().unwrap(),
            hlc: 1, revoked_key: [9u8; 32], reason: None,
        };
        let mut signed = SignedRevocation::sign(event, &sk_a);
        signed.event.revoked_key = [7u8; 32]; // tamper
        assert!(!signed.verify(&pub_a), "a tampered event fails verification");
    }
}
