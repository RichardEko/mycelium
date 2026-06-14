//! WS1 — RBAC: signed, verifiable node role claims.
//!
//! A node declares its roles (and an optional data-classification *clearance*
//! level — the L1/L2/L3 layer-clearance facet) as a [`SignedRoleClaim`], signed
//! by its Ed25519 identity key (the same key published at `sys/identity/{node}`)
//! and gossiped under `sys/role/{node}`. Readers verify the signature against the
//! node's verifying key (from `peer_keys`) before trusting any role, so a peer
//! cannot forge another node's roles. This is **detection, not prevention** in
//! the house style: a forged role write is rejected on read, never blocked at the
//! store (`apply_and_notify` learns no RBAC law).
//!
//! This is the WS1 foundation that capability-assertion authorization, gateway
//! endpoint ACLs, and layer-clearance build on. Gated behind the `compliance`
//! feature, which implies `gateway` (the auth surface) + `tls` (signed identity).
//!
//! **Forward-compat (M16 / NANDA, per `docs/plans/v1x-completion.md`):** roles are
//! self-signed claims bound to the node identity — the *stable substrate shape*
//! that self-certified AgentFacts credential assertions consume later. We do not
//! couple to any AgentFacts field name (that surface is a moving v0.3 target).
//!
//! Namespace note: role claims live under their own `sys/role/` prefix rather than
//! nested under `sys/identity/{node}` so they do not disturb the existing
//! identity-key mirror that scans `sys/identity/` for 32-byte verifying keys.

use crate::framing::bincode_cfg;
use crate::node_id::NodeId;
use bytes::{BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// KV prefix for role claims. One entry per node: `sys/role/{node_id}`.
pub const ROLE_PREFIX: &str = "sys/role/";

/// KV key holding `node`'s signed role claim.
pub fn role_key(node: &NodeId) -> String {
    format!("{ROLE_PREFIX}{node}")
}

/// A node's declared roles plus a data-classification clearance, ready to sign.
///
/// `roles` is canonicalised (sorted + deduplicated) by [`RoleClaim::new`] so the
/// signature is stable for a given logical claim. `node_id` and `issued_at_ms`
/// are bound into the signature, so a claim cannot be replayed under a different
/// node id or back-dated without detection.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoleClaim {
    pub node_id:      NodeId,
    pub roles:        Vec<Arc<str>>,
    /// Data-classification clearance: `0` = none, `1`/`2`/`3` = L1/L2/L3. The
    /// layer-clearance facet — an L1 board read is not L3 SPOF topology.
    pub clearance:    u8,
    /// Issue time (unix ms). Lets readers prefer fresher claims and supports
    /// future rotation/evaporation.
    pub issued_at_ms: u64,
}

impl RoleClaim {
    pub fn new(
        node_id: NodeId,
        roles: impl IntoIterator<Item = Arc<str>>,
        clearance: u8,
        issued_at_ms: u64,
    ) -> Self {
        let mut roles: Vec<Arc<str>> = roles.into_iter().collect();
        roles.sort();
        roles.dedup();
        Self { node_id, roles, clearance, issued_at_ms }
    }

    /// True if this claim grants `role`.
    pub fn has_role(&self, role: &str) -> bool {
        self.roles.iter().any(|r| r.as_ref() == role)
    }

    /// True if the clearance is at least `level` (L1/L2/L3 → 1/2/3).
    pub fn clearance_at_least(&self, level: u8) -> bool {
        self.clearance >= level
    }

    /// Deterministic bytes the signature covers.
    fn signing_bytes(&self) -> Vec<u8> {
        let mut buf = BytesMut::new();
        let _ = bincode::serde::encode_into_std_write(self, &mut (&mut buf).writer(), bincode_cfg());
        buf.to_vec()
    }
}

/// A [`RoleClaim`] plus an Ed25519 signature over its canonical bytes. This is the
/// value written to `sys/role/{node}` and verified on read.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignedRoleClaim {
    pub claim: RoleClaim,
    /// 64-byte Ed25519 signature over `claim.signing_bytes()`.
    pub sig: Vec<u8>,
}

impl SignedRoleClaim {
    /// Sign `claim` with the node's identity signing key. `compliance` implies
    /// `tls`, so the signing API is always available in this build.
    pub fn sign(claim: RoleClaim, signing_key: &ed25519_dalek::SigningKey) -> Self {
        let sig = crate::tls::sign_bytes(signing_key, &claim.signing_bytes()).to_vec();
        Self { claim, sig }
    }

    /// Verify the signature against `verifying_key` (the 32-byte Ed25519 public key
    /// published at `sys/identity/{node}`). The caller must additionally confirm
    /// `verifying_key` is the key bound to `self.claim.node_id` (via `peer_keys`).
    pub fn verify(&self, verifying_key: &[u8; 32]) -> bool {
        crate::tls::verify_bytes(verifying_key, &self.claim.signing_bytes(), &self.sig)
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

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::SigningKey;

    fn key(seed: u8) -> SigningKey { SigningKey::from_bytes(&[seed; 32]) }
    fn node() -> NodeId { NodeId::new("127.0.0.1", 7000).unwrap() }

    #[test]
    fn new_canonicalises_roles_and_queries() {
        let c = RoleClaim::new(node(), ["b".into(), "a".into(), "b".into()], 2, 1);
        assert_eq!(
            c.roles.iter().map(|r| r.as_ref()).collect::<Vec<_>>(),
            ["a", "b"],
            "roles must be sorted + deduped"
        );
        assert!(c.has_role("a") && c.has_role("b") && !c.has_role("c"));
        assert!(c.clearance_at_least(2) && !c.clearance_at_least(3));
    }

    #[test]
    fn sign_verify_roundtrip() {
        let sk = key(7);
        let vk = sk.verifying_key().to_bytes();
        let signed = SignedRoleClaim::sign(RoleClaim::new(node(), ["admin".into()], 3, 42), &sk);
        assert!(signed.verify(&vk), "valid signature must verify");
    }

    #[test]
    fn tampered_claim_fails_verification() {
        let sk = key(7);
        let vk = sk.verifying_key().to_bytes();
        let mut signed =
            SignedRoleClaim::sign(RoleClaim::new(node(), ["reader".into()], 1, 42), &sk);
        // Privilege-escalate the role set after signing — must be detected.
        signed.claim.roles.push("admin".into());
        signed.claim.roles.sort();
        assert!(!signed.verify(&vk), "tampered claim must NOT verify");
    }

    #[test]
    fn wrong_key_fails_verification() {
        let signed = SignedRoleClaim::sign(RoleClaim::new(node(), ["admin".into()], 3, 1), &key(7));
        let other_vk = key(9).verifying_key().to_bytes();
        assert!(!signed.verify(&other_vk), "a different node's key must NOT verify");
    }

    #[test]
    fn encode_decode_roundtrip() {
        let sk = key(7);
        let signed =
            SignedRoleClaim::sign(RoleClaim::new(node(), ["a".into(), "b".into()], 2, 99), &sk);
        let back = SignedRoleClaim::decode(&signed.encode()).expect("decode");
        assert_eq!(signed, back, "wire round-trip must preserve the signed claim");
        assert!(back.verify(&sk.verifying_key().to_bytes()), "decoded claim still verifies");
    }

    #[test]
    fn role_key_uses_sys_role_prefix() {
        assert_eq!(role_key(&node()), format!("sys/role/{}", node()));
        assert!(role_key(&node()).starts_with(ROLE_PREFIX));
    }
}
