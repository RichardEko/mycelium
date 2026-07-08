//! The policy-audit **verification tool** — reconstruct a provider's tamper-evident chain and
//! prove the guardrail fired.
//!
//! Under the `compliance` feature, the Tier-C gate ([`check_caller`](crate::check_caller) /
//! [`guarded_rpc_serve`](crate::guarded_rpc_serve)) **seals** every unauthorized invocation as an
//! `Invoke`/`Denied` record — verified principal, signed and hash-chained — into the provider's
//! own audit stream. This module reads that stream back, re-verifies the whole chain (integrity +
//! contiguity + signature), and narrates the sealed denials as a proof.
//!
//! ## Honest framing — what IS and what is NOT proven (binding #3)
//!
//! The tool proves the provider **tamper-evidently sealed denying X** — *provable-stopping*: these
//! specific denials cannot have been forged, reordered, or removed without the chain failing to
//! verify. It does **NOT** give a global negative proof ("X could not have done Y *anywhere*"):
//!
//! - The audit chain is **per-node**, so absence of a denial in *one* provider's chain is not
//!   proof X did nothing elsewhere — only that *this* provider stopped and sealed *these* calls.
//! - Only **guarded** capabilities that route through the gate seal denials; an action that never
//!   reaches a gating provider is neither stopped nor recorded here.
//!
//! Do not overclaim. The claim this tool underwrites is precise: *the provider tamper-evidently
//! sealed stopping X*, not *X could not have done Y*.

use mycelium::{AuditAction, AuditOutcome, GossipAgent, NodeId};

/// One tamper-evidently-sealed denial: an `Invoke`/`Denied` record from the provider's chain.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SealedDenial {
    /// `record.principal` — the signature-verified caller that was stopped.
    pub caller: String,
    /// `record.target` — the capability/kind whose invocation was denied.
    pub target: String,
    /// The record's per-node sequence number (position in the chain).
    pub seq: u64,
    /// The HLC timestamp packed at seal time.
    pub hlc: u64,
    /// The record's stable SHA-256 content hash — the citable per-denial identifier.
    pub content_hash: [u8; 32],
    /// Optional free-form detail sealed with the denial (small JSON: nonce + reason).
    pub detail: Option<String>,
}

/// The proof that a provider stopped (and sealed) a set of unauthorized invocations.
///
/// See the [module docs](self) for the honest framing: this is *provable-stopping* of the listed
/// denials in *this* provider's chain — **not** a global "X could not have done Y" claim.
#[derive(Clone, Debug)]
pub struct DenialProof {
    /// The provider whose chain was reconstructed.
    pub provider: NodeId,
    /// The provider's whole audit chain verified (integrity + contiguity + signature). When true,
    /// the denials below cannot have been forged, reordered, or removed without detection.
    pub chain_verified: bool,
    /// The verify error if the chain did NOT verify (then the proof is void — say so).
    pub verify_error: Option<String>,
    /// Every `Invoke`/`Denied` record from the provider's chain (filtered by caller when asked),
    /// sorted by `seq`.
    pub denials: Vec<SealedDenial>,
}

/// Reconstruct `provider`'s tamper-evident chain from `agent`'s KV view and prove which
/// unauthorized invocations it sealed as denied.
///
/// Pulls `agent.audit_stream(provider)`, runs `agent.audit_verify(provider)` (recording
/// `chain_verified` / `verify_error`), and collects every `Invoke`/`Denied` record — filtered to
/// `caller` when `Some` — sorted by `seq`.
///
/// **Honest framing (binding #3):** the returned [`DenialProof`] attests that the provider
/// *tamper-evidently sealed stopping* these callers — provable-stopping. It is **not** a global
/// negative proof: the chain is per-node, and only guarded capabilities that reach the gate seal
/// denials, so an empty or caller-absent result is *not* proof the caller did nothing elsewhere.
/// Any node may run this — the audit chain gossips fleet-wide, so a third-party observer proves
/// the denial exactly as the provider itself would.
pub fn prove_denials(agent: &GossipAgent, provider: &NodeId, caller: Option<&str>) -> DenialProof {
    let (chain_verified, verify_error) = match agent.audit_verify(provider) {
        Ok(()) => (true, None),
        Err(e) => (false, Some(format!("{e:?}"))),
    };

    let mut denials: Vec<SealedDenial> = agent
        .audit_stream(provider)
        .into_iter()
        .filter(|sr| {
            sr.record.action == AuditAction::Invoke && sr.record.outcome == AuditOutcome::Denied
        })
        .filter(|sr| caller.is_none_or(|c| sr.record.principal == c))
        .map(|sr| SealedDenial {
            caller: sr.record.principal.clone(),
            target: sr.record.target.clone(),
            seq: sr.record.seq,
            hlc: sr.record.hlc,
            content_hash: sr.record.content_hash(),
            detail: sr.record.detail.clone(),
        })
        .collect();
    denials.sort_by_key(|d| d.seq);

    DenialProof { provider: provider.clone(), chain_verified, verify_error, denials }
}

/// Human-readable proof lines for a demo or operator report.
///
/// Structure: a **header** stating exactly what IS and ISN'T proven (the honest framing —
/// chain-verified tamper-evidence of these specific denials; **not** a global "could not have done
/// Y" claim), one line **per denial**, then a **footer** attesting the chain (or voiding the proof
/// if it did not verify).
///
/// The caveat is emitted verbatim so the proof never reads as more than it is (binding #3).
pub fn narrate_proof(proof: &DenialProof) -> Vec<String> {
    let mut lines = Vec::new();

    // ── Header: the honest framing, stated up front. ──
    lines.push(format!(
        "policy-audit proof — provider {} sealed {} denial(s)",
        proof.provider,
        proof.denials.len()
    ));
    lines.push(
        "  PROVES: this provider tamper-evidently sealed STOPPING these callers — the records \
         below cannot have been forged, reordered, or removed without the chain failing to verify."
            .to_string(),
    );
    lines.push(
        "  DOES NOT PROVE: that a caller \"could not have done Y anywhere\". The audit chain is \
         per-node, and only guarded capabilities that reach the gate seal denials — absence here \
         is not proof of absence elsewhere."
            .to_string(),
    );

    // ── One line per sealed denial. ──
    for d in &proof.denials {
        lines.push(format!(
            "  [seq {}] caller {} was DENIED {} (sealed, hash {}…)",
            d.seq,
            d.caller,
            d.target,
            hex8(&d.content_hash),
        ));
    }

    // ── Footer: attest the chain, or void the proof. ──
    if proof.chain_verified {
        lines.push(
            "  ✓ the provider's chain verifies — these denials are tamper-evident".to_string(),
        );
    } else {
        lines.push(format!(
            "  ✗ chain did NOT verify — proof void: {}",
            proof.verify_error.as_deref().unwrap_or("unknown error")
        ));
    }

    lines
}

/// First 8 hex chars of a content hash — a short, citable denial fingerprint.
fn hex8(hash: &[u8; 32]) -> String {
    hash.iter().take(4).map(|b| format!("{b:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hex8_is_eight_chars() {
        let mut h = [0u8; 32];
        h[0] = 0xde;
        h[1] = 0xad;
        h[2] = 0xbe;
        h[3] = 0xef;
        assert_eq!(hex8(&h), "deadbeef");
    }

    #[test]
    fn narrate_header_states_both_sides_of_the_claim() {
        let proof = DenialProof {
            provider: NodeId::new("127.0.0.1", 7000).unwrap(),
            chain_verified: true,
            verify_error: None,
            denials: vec![SealedDenial {
                caller: "127.0.0.1:9001".into(),
                target: "agent.tool.invoke".into(),
                seq: 3,
                hlc: 42,
                content_hash: [0xab; 32],
                detail: None,
            }],
        };
        let lines = narrate_proof(&proof);
        let joined = lines.join("\n");
        // The honest framing appears verbatim: what it PROVES and what it DOES NOT.
        assert!(joined.contains("PROVES: this provider tamper-evidently sealed STOPPING"));
        assert!(joined.contains("DOES NOT PROVE"));
        assert!(joined.contains("per-node"));
        // The per-denial line and the tamper-evident footer.
        assert!(joined.contains("[seq 3] caller 127.0.0.1:9001 was DENIED agent.tool.invoke"));
        assert!(joined.contains("these denials are tamper-evident"));
    }

    #[test]
    fn narrate_voids_the_proof_when_the_chain_fails() {
        let proof = DenialProof {
            provider: NodeId::new("127.0.0.1", 7000).unwrap(),
            chain_verified: false,
            verify_error: Some("BadSignature { seq: 2 }".into()),
            denials: vec![],
        };
        let joined = narrate_proof(&proof).join("\n");
        assert!(joined.contains("proof void"));
        assert!(joined.contains("BadSignature"));
    }
}
