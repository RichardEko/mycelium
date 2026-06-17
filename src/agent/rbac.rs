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

use crate::node_id::NodeId;
use bytes::Bytes;
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
        mycelium_core::serde_fixint::to_vec(self).unwrap_or_default()
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
        Bytes::from(mycelium_core::serde_fixint::to_vec(self).unwrap_or_default())
    }

    pub fn decode(bytes: &[u8]) -> Option<Self> {
        mycelium_core::serde_fixint::from_slice(bytes).ok()
    }
}

/// Read-path verification for a `sys/role/{node}` value: decode it, confirm the
/// claim is bound to `expected_node` (the slot owner), and verify the signature
/// against that node's `verifying_key`. Returns the claim only if all three hold;
/// a forged, mis-attributed, or garbage value yields `None` — detection, not
/// prevention. This is the single security-critical read primitive; the agent's
/// `roles_of` is thin glue over it.
pub(crate) fn verified_roles(
    signed_bytes: &[u8],
    expected_node: &NodeId,
    verifying_key: &[u8; 32],
) -> Option<RoleClaim> {
    let signed = SignedRoleClaim::decode(signed_bytes)?;
    if &signed.claim.node_id != expected_node {
        return None; // a claim found at sys/role/{X} must be a claim *for* X
    }
    if !signed.verify(verifying_key) {
        return None;
    }
    Some(signed.claim)
}

/// Provider-side authorization decision: may a caller with verified id
/// `caller_node` and verified roles `caller_roles` invoke a capability whose
/// `authorized_callers` allowlist is `allow`?
///
/// - Empty `allow` ⇒ unrestricted (admit).
/// - Otherwise admit iff the caller's NodeId string is listed, **or** the caller
///   holds a role that is listed. Allowlist entries may name either node ids or
///   role names, so a capability can be opened to "any node holding role X" or to
///   "exactly node Y".
pub(crate) fn caller_admitted(
    allow: &[Arc<str>],
    caller_node: &NodeId,
    caller_roles: &[Arc<str>],
) -> bool {
    if allow.is_empty() {
        return true;
    }
    let node_str = caller_node.to_string();
    allow.iter().any(|entry| {
        entry.as_ref() == node_str || caller_roles.iter().any(|r| r.as_ref() == entry.as_ref())
    })
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

    #[test]
    fn verified_roles_accepts_valid_claim() {
        let sk = key(7);
        let vk = sk.verifying_key().to_bytes();
        let bytes =
            SignedRoleClaim::sign(RoleClaim::new(node(), ["admin".into()], 3, 1), &sk).encode();
        let got = verified_roles(&bytes, &node(), &vk).expect("valid claim must verify");
        assert!(got.has_role("admin") && got.clearance_at_least(3));
    }

    #[test]
    fn verified_roles_rejects_node_mismatch() {
        // A validly-signed claim for node() planted under a *different* node's slot.
        let sk = key(7);
        let vk = sk.verifying_key().to_bytes();
        let bytes =
            SignedRoleClaim::sign(RoleClaim::new(node(), ["admin".into()], 3, 1), &sk).encode();
        let other = NodeId::new("127.0.0.1", 7001).unwrap();
        assert!(
            verified_roles(&bytes, &other, &vk).is_none(),
            "a claim must be bound to its slot's node id"
        );
    }

    #[test]
    fn verified_roles_rejects_bad_signature_and_garbage() {
        let vk = key(7).verifying_key().to_bytes();
        // Forged by a different key.
        let forged =
            SignedRoleClaim::sign(RoleClaim::new(node(), ["admin".into()], 3, 1), &key(9)).encode();
        assert!(verified_roles(&forged, &node(), &vk).is_none(), "wrong-key signature rejected");
        // Garbage decodes to None, never panics.
        assert!(verified_roles(b"not a signed claim", &node(), &vk).is_none());
    }

    #[test]
    fn caller_admitted_by_node_role_and_open() {
        let caller = node();
        let node_str: Arc<str> = caller.to_string().into();
        // Empty allowlist = unrestricted.
        assert!(caller_admitted(&[], &caller, &[]));
        // Listed neither by node id nor by a held role → denied.
        assert!(!caller_admitted(&["orchestrator".into()], &caller, &[]));
        // Admitted by a held role.
        assert!(caller_admitted(&["orchestrator".into()], &caller, &["orchestrator".into()]));
        // Admitted by explicit node id.
        assert!(caller_admitted(&[node_str], &caller, &[]));
        // Role match within a mixed allowlist.
        assert!(caller_admitted(&["planner".into(), "writer".into()], &caller, &["writer".into()]));
        // Holding a different role is not enough.
        assert!(!caller_admitted(&["planner".into()], &caller, &["reader".into()]));
    }
}
