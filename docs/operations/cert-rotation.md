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
2. Publishes `sys/identity/{self}` = `new ‖ old` (64 bytes), signed by the
   **old** key — which peers still trust — so their retained key sets accept both.
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

- The `current ‖ previous` identity format preserves the immediately-prior key
  across **one** restart. Rotating more than once between restarts retains only
  the most recent prior key on disk (the in-memory set keeps all keys seen since
  start).
- **Compromise caveat:** a retired key remains *accepted for verification* — good
  for history, but it means rotating away from a **compromised** key does not by
  itself stop the attacker's old signatures from verifying. Compromise response
  needs explicit revocation (e.g. re-issue the cluster CA / rebuild the trust
  set), which is a heavier operation than hygiene rotation.

---

## 3. Verify a rotation went cleanly

```bash
# The identity entry should briefly be 64 bytes (current‖previous) during the
# window, then settle. Peers' audit verification of this node must stay green:
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
| historical `audit_verify` fails after several rotations + a restart | only the most-recent prior key is persisted (64-byte format) | export/verify audit streams before repeated rotations; a multi-key archive is future work |
| rotating away from a compromised key, old signatures still verify | retained-key design (option B) | perform explicit revocation (re-issue CA / rebuild trust), not just rotation |
