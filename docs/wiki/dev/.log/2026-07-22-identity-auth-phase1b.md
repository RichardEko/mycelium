# 2026-07-22 — identity-auth Phase 1b shipped (CA-anchor harvest + tripwire)

SOC 2 WS-E, first implementation step of `docs/design/identity-authentication.md`. Closes the
*detection* half of the `sys/identity` key-poisoning vector (Phase 2 adds prevention).

**What shipped:** the outbound writer harvests each directly-connected peer's **CA-validated**
Ed25519 key (from its mTLS cert, client side only — the SAN is IP so correlation to a `NodeId`
is clean only when we dialed) into `peer_anchor_keys` (on `CoreCtx`) and merges it into `peer_keys`.
A `sys/identity/{V}` KV key differing from V's anchor trips `identity_anchor_conflicts` (`/stats` +
`SystemStats`) at both KV-merge sites. Gate: `test_identity_anchor_recorded_and_conflict_flagged`.

**Implementation deviation from the design (simpler + lower-risk) — the durable lesson:** the design
prescribed threading an `anchor_sink` callback through `get_or_spawn_writer` and its ~10 hot-path
call sites, on the belief that the harvest (in `mycelium-core`) couldn't reach the `mycelium`-side
`peer_keys`. **But `peer_keys` is on `CoreCtx`** (`context.rs`), not the upper crate, and
`tls: Option<Arc<NodeTls>>` is *already* threaded through all 10 writer-spawn sites. So instead:
- the anchor maps live on `CoreCtx` (both crates reach them);
- the recorder hangs off `NodeTls` (`set_anchor_sink` at start / `record_anchor` on the hot path),
  reusing the existing `tls` thread — **zero new parameters** through the 10 call sites;
- `GossipStream::peer_ed25519_key()` pulls the key from the client `TlsStream`'s
  `peer_certificates()` post-handshake.

Lesson: before threading a callback across a crate boundary, check whether the state is actually
already shared (CoreCtx) and whether an existing already-threaded handle (here `NodeTls`) can carry
the new capability. Saved ~10 signature changes on hot-path code.

**Known limitation (documented):** the tripwire may briefly fire on a legitimate rotation until the
new key is re-anchored on reconnect — acceptable for a detection-only, non-fatal signal; Phase 2's
signed proofs make it precise. **Anchors outbound peers only** (inbound accept can't correlate the
IP-SAN cert to a NodeId); forwarded-consensus-only peers stay on the Phase-2 signed-proof path.

Next: Phase 2 (signed `sys/identity-proof/`, rotation chained to a trusted key — prevention), then
Phase 3 (reject unsigned, wire-v13-gated).
