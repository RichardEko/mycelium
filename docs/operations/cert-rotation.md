# Mycelium — Hot Certificate / Identity Rotation Runbook

Operator guide to rotating a node's Ed25519 TLS/identity key **without cluster
disruption** (WS5; `tls` feature). Concept: the same key is the node's mTLS cert
key *and* its signing/identity key (`sys/identity/{node}`), so a rotation swaps
both at once. See [`../guide/09-security.md`](../guide/09-security.md).

---

## 1. What rotation does

`GossipAgent::rotate_identity(propagation)`:

1. Generates a new key + a fresh node cert **signed by the existing cluster CA**
   (the CA is *not* rotated), persisted to disk.
2. Publishes `sys/identity/{self}` = `new ‖ old` (the raw `32×N`-byte key history),
   so peers' retained key sets accept both. **NOTE (2026-07-15):** this entry is
   written **unsigned** — a plain Layer-I KV value, *not* signed by the old key (that
   was the original design intent; it was never implemented). Peers accept the new key
   because the KV is unauthenticated and accumulated — the identity-poisoning gap tracked
   in [`docs/design/identity-authentication.md`](../design/identity-authentication.md)
   (CFT-not-BFT: a compromised admitted node can inject a key; out of the base threat model).
3. Waits `propagation` for that to gossip cluster-wide.
4. Atomically cuts over the active key + rustls configs. New gossip signatures
   and **new** TLS handshakes use the new key immediately; **existing**
   connections keep their CA-trusted session (no listener restart, no dropped
   frames).

```rust
// node started with cfg.tls = Some(..)
let new_vk = agent.rotate_identity(std::time::Duration::from_secs(10)).await?;
```

Pick `propagation` ≥ a few gossip rounds (your `health_check_interval_secs` ×
2–3) so peers learn the new key before the cutover. Over-shooting is harmless;
under-shooting just means a brief window where peers catch up via anti-entropy
(verification still converges — see §3).

---

## 2. Retained-key verification (why historical records still verify)

Identity keys rotate, but records signed earlier must stay verifiable. So
`peer_keys` holds a **set** of keys per node, **accumulated** across rotations
(option B): every verify path — inbound `SignedData`, consensus votes, role
claims, and the audit chain — tries all keys for the signer. A node's audit
stream therefore verifies end-to-end even though records before and after a
rotation are signed by different keys.

- The `sys/identity/{node}` value stores the **full** key history (`32 × N` bytes:
  current first, then every prior key), so verification survives **any** number of
  rotations and restarts — historical records always have their signing key
  available. The entry grows 32 bytes per rotation (rotations are rare).
- **Compromise caveat:** a retired key remains *accepted for verification* — good
  for history, but it means rotating away from a **compromised** key does not by
  itself stop the attacker's old signatures from verifying. Rotation is **hygiene**;
  compromise response needs explicit **revocation**.

### Compromise remediation — rotate *and* revoke (SOC 2 WS-B)

A signed revocation of the old key is validated cluster-wide and excluded on **every**
verify path — role claims, the audit chain, **and consensus** — so the old key stops
verifying everywhere. Two ways:

- **One call:** `agent.rotate_identity_on_compromise(propagation)` — rotates to a fresh key,
  then revokes the outgoing one *with the new key*. Use this when the current key may be
  compromised.
- **Operator trigger (no code):** `POST /gateway/identity/revoke` (scope `identity:write`,
  `compliance`), body `{"revoked_key":"<64 hex>","reason":"..."}` — after a plain
  `rotate_identity`, revoke the old key over HTTP.

```bash
curl -X POST https://gateway:9443/gateway/identity/revoke \
  -H "authorization: Bearer $TOKEN" \
  -d '{"revoked_key":"<old-verifying-key-hex>","reason":"suspected compromise"}'
```

**Ownership limit (by design):** only the node itself, holding its *current* key, can revoke
its own keys — the coordinator-free trade-off. A **fully-compromised or offline** node cannot
be force-revoked by a fleet operator without a separate operator-authority mechanism (not yet
provided). Re-issuing the cluster CA remains the heavier fallback for that case.

---

## 3. Verify a rotation went cleanly

```bash
# The identity entry grows by one 32-byte key per rotation (current ‖ priors).
# Peers' audit verification of this node must stay green across the rotation:
curl -s -H 'Authorization: Bearer <audit-token>' \
     "http://PEER:PORT/gateway/audit?node=<rotated-node>" | jq '.streams[0].verified'
# → true   (the chain spans the rotation; the peer holds both keys)
```

Expectations during/after rotation:
- **No dropped frames**, no connection loss (existing sessions persist; new ones
  use the new cert).
- Peers' `audit_verify(rotated_node)` stays `true` across the rotation.
- A peer that was offline during the window picks up the new key via
  anti-entropy on reconnect (the retained set means the old key still verifies
  any backlog signed before the cutover).

---

## 4. Failure modes

| Symptom | Cause | Fix |
|---|---|---|
| `rotate_identity` → `InvalidField { field: "tls" }` | node has no `GossipConfig::tls`, or no cluster CA on disk | enable tls; rotation requires an established CA (`ca-cert.pem` + `ca-key.pem` in `auto_cert_dir`) |
| peers briefly reject the node's new-key frames | cutover happened before the new key gossiped | increase `propagation`; verification self-heals via anti-entropy once the key arrives |
| `sys/identity/{node}` entry growing over time | by design — the full key history is retained (32 B/rotation) so old signatures verify | none needed; rotations are rare. If ever a concern, prune keys older than your audit-retention horizon |
| rotating away from a compromised key, old signatures still verify | retained-key design (option B) | perform explicit revocation (re-issue CA / rebuild trust), not just rotation |
