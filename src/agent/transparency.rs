//! Revocation transparency — Merkle inclusion proofs over the [revocation](super::revocation) log
//! (WS-D / Quilt-DD#2, track 1 · D2).
//!
//! D1 made revocation *work* (revoked keys excluded from verification). D2 makes the revocation log
//! **client-checkable**: a node commits to its validated revocation set with a Merkle root, and a
//! fetcher can verify that a specific revocation is included under that root with a compact audit
//! path — without trusting the server or replaying the whole log, and without a central CT operator
//! (the log stays per-node, gossip-replicated).
//!
//! Hashing follows RFC 6962 (Certificate Transparency): a domain-separation prefix distinguishes
//! leaves (`0x00`) from interior nodes (`0x01`), preventing second-preimage attacks. Leaves are
//! sorted by revoked-key hex so the root is deterministic from any node's local gossip view.

use sha2::{Digest, Sha256};

use crate::node_id::NodeId;
use super::TaskCtx;

// ── Pure Merkle primitives (RFC 6962 style) ────────────────────────────────────

/// Leaf hash: `SHA256(0x00 || data)`.
pub fn leaf_hash(data: &[u8]) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update([0x00]);
    h.update(data);
    h.finalize().into()
}

/// Interior node hash: `SHA256(0x01 || left || right)`.
fn node_hash(left: &[u8; 32], right: &[u8; 32]) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update([0x01]);
    h.update(left);
    h.update(right);
    h.finalize().into()
}

/// The Merkle root over `leaves` (already leaf-hashed). Empty ⇒ all-zero. An odd level duplicates
/// its last node (the common convention); the prefix domain-separation makes that unambiguous.
pub fn merkle_root(leaves: &[[u8; 32]]) -> [u8; 32] {
    if leaves.is_empty() {
        return [0u8; 32];
    }
    let mut level: Vec<[u8; 32]> = leaves.to_vec();
    while level.len() > 1 {
        let mut next = Vec::with_capacity(level.len().div_ceil(2));
        let mut i = 0;
        while i < level.len() {
            let l = level[i];
            let r = if i + 1 < level.len() { level[i + 1] } else { level[i] };
            next.push(node_hash(&l, &r));
            i += 2;
        }
        level = next;
    }
    level[0]
}

/// One step of an inclusion proof: a sibling hash and whether it sits on the **right** of the
/// running hash (so the verifier folds in the correct order).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProofStep {
    pub sibling:  [u8; 32],
    pub on_right: bool,
}

/// The audit path proving the leaf at `index` is in the tree over `leaves`. `None` if out of range.
pub fn merkle_proof(leaves: &[[u8; 32]], index: usize) -> Option<Vec<ProofStep>> {
    if index >= leaves.len() {
        return None;
    }
    let mut proof = Vec::new();
    let mut level: Vec<[u8; 32]> = leaves.to_vec();
    let mut idx = index;
    while level.len() > 1 {
        let sib = if idx.is_multiple_of(2) {
            // running hash is the left child; sibling is the right (or self if odd tail).
            let s = if idx + 1 < level.len() { level[idx + 1] } else { level[idx] };
            ProofStep { sibling: s, on_right: true }
        } else {
            ProofStep { sibling: level[idx - 1], on_right: false }
        };
        proof.push(sib);
        // Build the next level.
        let mut next = Vec::with_capacity(level.len().div_ceil(2));
        let mut i = 0;
        while i < level.len() {
            let l = level[i];
            let r = if i + 1 < level.len() { level[i + 1] } else { level[i] };
            next.push(node_hash(&l, &r));
            i += 2;
        }
        level = next;
        idx /= 2;
    }
    Some(proof)
}

/// Verify that `leaf` is included under `root` via `proof`. Pure — a fetcher runs this with no
/// trust in the server. Tampering with the leaf, a sibling, or the order yields a different root.
pub fn verify_inclusion(leaf: &[u8; 32], proof: &[ProofStep], root: &[u8; 32]) -> bool {
    let mut acc = *leaf;
    for step in proof {
        acc = if step.on_right {
            node_hash(&acc, &step.sibling)
        } else {
            node_hash(&step.sibling, &acc)
        };
    }
    &acc == root
}

// ── Revocation-log views ───────────────────────────────────────────────────────

/// A node's **validated** revocation leaves: `(revoked_key, leaf_hash)` for every revocation that
/// passes [`revocation`](super::revocation)'s validation rule, sorted by revoked-key hex (so the
/// root is deterministic). The leaf hash commits to the full signed revocation.
#[cfg(feature = "compliance")]
fn node_leaves(ctx: &TaskCtx, node: &NodeId) -> Vec<([u8; 32], [u8; 32])> {
    use super::revocation::{validated_signed_revocations, SignedRevocation};
    let mut entries: Vec<SignedRevocation> = validated_signed_revocations(ctx, node);
    entries.sort_by_key(|s| s.event.revoked_key);
    entries
        .iter()
        .map(|s| (s.event.revoked_key, leaf_hash(&s.encode())))
        .collect()
}

/// `(root, count)` — a node's revocation-log head: the Merkle root over its validated revocations
/// and how many there are. Recomputable on any node from its local gossip view.
#[cfg(feature = "compliance")]
pub(crate) fn revocation_head(ctx: &TaskCtx, node: &NodeId) -> ([u8; 32], usize) {
    let leaves = node_leaves(ctx, node);
    let hashes: Vec<[u8; 32]> = leaves.iter().map(|(_, h)| *h).collect();
    (merkle_root(&hashes), hashes.len())
}

/// An inclusion proof that a revoked key is in a node's revocation log: the leaf hash, its index,
/// the Merkle audit path, and the root to verify against.
pub(crate) type Inclusion = ([u8; 32], usize, Vec<ProofStep>, [u8; 32]);

/// An inclusion proof that `revoked_key` is in `node`'s revocation log. `None` if the node has no
/// validated revocation for that key.
#[cfg(feature = "compliance")]
pub(crate) fn inclusion_proof(
    ctx: &TaskCtx,
    node: &NodeId,
    revoked_key: &[u8; 32],
) -> Option<Inclusion> {
    let leaves = node_leaves(ctx, node);
    let index = leaves.iter().position(|(k, _)| k == revoked_key)?;
    let hashes: Vec<[u8; 32]> = leaves.iter().map(|(_, h)| *h).collect();
    let proof = merkle_proof(&hashes, index)?;
    Some((hashes[index], index, proof, merkle_root(&hashes)))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lh(b: u8) -> [u8; 32] {
        leaf_hash(&[b])
    }

    #[test]
    fn single_leaf_root_is_the_leaf() {
        let l = lh(1);
        assert_eq!(merkle_root(&[l]), l);
        let proof = merkle_proof(&[l], 0).unwrap();
        assert!(proof.is_empty());
        assert!(verify_inclusion(&l, &proof, &l));
    }

    #[test]
    fn inclusion_proof_verifies_for_every_leaf() {
        for n in [2usize, 3, 4, 5, 8, 9] {
            let leaves: Vec<[u8; 32]> = (0..n as u8).map(lh).collect();
            let root = merkle_root(&leaves);
            for i in 0..n {
                let proof = merkle_proof(&leaves, i).expect("proof");
                assert!(verify_inclusion(&leaves[i], &proof, &root), "n={n} i={i} should verify");
            }
        }
    }

    #[test]
    fn tampering_fails_inclusion() {
        let leaves: Vec<[u8; 32]> = (0..5u8).map(lh).collect();
        let root = merkle_root(&leaves);
        let i = 2;
        let proof = merkle_proof(&leaves, i).unwrap();
        // A wrong leaf fails.
        assert!(!verify_inclusion(&lh(99), &proof, &root), "wrong leaf rejected");
        // A tampered sibling fails.
        let mut bad = proof.clone();
        bad[0].sibling = lh(123);
        assert!(!verify_inclusion(&leaves[i], &bad, &root), "tampered sibling rejected");
        // A flipped order fails.
        let mut flipped = proof.clone();
        flipped[0].on_right = !flipped[0].on_right;
        assert!(!verify_inclusion(&leaves[i], &flipped, &root), "flipped order rejected");
        // Dropping a leaf (different set) yields a different root → the old proof fails.
        let dropped: Vec<[u8; 32]> = (0..4u8).map(lh).collect();
        let new_root = merkle_root(&dropped);
        assert_ne!(new_root, root, "dropping a revocation changes the root");
    }
}
