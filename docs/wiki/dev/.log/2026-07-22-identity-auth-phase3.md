# 2026-07-22 — identity-auth Phase 3 shipped (reject unsigned, config-gated)

SOC 2 WS-E, final step. Closes the last poisoning residual: an *unsigned* `sys/identity` entry
(mimicking a pre-Phase-2 node) can no longer modify a peer's key set.

**Mechanism decision — config flag, NOT a wire bump (the durable lesson).** The plan's decision #2
committed to a "v13 wire bump." Implementation revealed that was the wrong instrument: **Phase 3
changes no frame format** — proofs and identity gossip as ordinary `Data` frames, and "reject
unsigned" is a read-side KV *policy*. A literal `WIRE_VERSION` bump would gate nothing frame-wise
and would spuriously open a rolling-upgrade window. So the gate is a config flag
`GossipConfig::require_identity_proofs` (default `false`; `GOSSIP_REQUIRE_IDENTITY_PROOFS`). The
design's "gated like a wire-version bump / document the min-version like `PREV_WIRE_VERSION`" is the
*rollout discipline* (two releases, documented min-version), not a literal version change — realized
by the operator flipping the flag after full rollout. Surfaced to the user rather than doing a
cosmetic bump.

**When set:** `validate_and_merge_identity`'s no-proof branch rejects (+ counts
`identity_anchor_conflicts`) instead of accepting. **Two-release rollout** (cert-rotation runbook):
R1 = deploy Phase 2 fleet-wide (all nodes write proofs, flag off); R2 = set the flag. Flipping early
would reject legitimate pre-upgrade nodes.

**Ordering fix (needed for Phase 3 correctness):** the identity watcher subscription broadened from
`sys/identity/` to `sys/identity` (no trailing slash), so a `sys/identity-proof/` change ALSO
re-triggers the re-scan. Without it, an identity that gossiped in *before* its proof would be
rejected under the flag and stay rejected (the proof-only change wouldn't re-fire the watcher). The
scan still filters to `sys/identity/` entries. (Phase 2, flag off, didn't need this — no-proof
accepts.)

**Gate:** `test_require_identity_proofs_rejects_unsigned` (flag off → unsigned accepted; flag on →
rejected + counted). Full lib suite stays green with the flag defaulting off. No wire change.

**WS-E complete:** 1a (primitive) · 1b (anchor + detection) · 2 (signed proofs, prevention) · 3
(reject unsigned, config-gated). The forged-consensus-quorum vector is closed; enabling
`require_identity_proofs` post-rollout closes it against unsigned mimics too.
