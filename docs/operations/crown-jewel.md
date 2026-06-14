# Mycelium — Crown-Jewel Operations Runbook (data-at-rest + egress)

Operator guide to the WS3 crown-jewel controls: opt-in data-at-rest encryption
and the outbound egress allowlist. Blast-radius context:
[`../threat-model.md`](../threat-model.md). Both controls are **feature-free**
(no cargo feature required) and **opt-in** — absent, behaviour is unchanged.

---

## 1. Data-at-rest encryption

The substrate encrypts the **on-disk** persistence surface (WAL records +
snapshots) through an operator-supplied cipher. It does **not** encrypt the store
in memory or the gossip wire (the wire is the `tls` feature's job).

### Attach a cipher

Implement `DataAtRestCipher` over your KMS/keyring and attach it **before**
`start()`:

```rust
use mycelium::{DataAtRestCipher, GossipAgent};
use std::sync::Arc;

struct KmsCipher { /* handle to your KMS/keyring */ }
impl DataAtRestCipher for KmsCipher {
    fn encrypt(&self, plaintext: &[u8]) -> Vec<u8> { /* AEAD seal */ }
    fn decrypt(&self, ciphertext: &[u8]) -> Option<Vec<u8>> { /* AEAD open; None on auth fail */ }
}

let agent = GossipAgent::new(id, cfg);
agent.with_data_at_rest_cipher(Arc::new(KmsCipher { /* … */ }));
agent.start().await?;
```

### Operating rules

- **Key stability.** The key must be available and identical across restarts, or
  the node cannot replay its own WAL/snapshot (records fail to decrypt and are
  skipped — silent data loss on restart). Source it from a KMS/HSM, not a local file.
- **Use a real AEAD.** The hook hands you opaque bytes; use an authenticated
  cipher (e.g. AES-GCM / ChaCha20-Poly1305) so `decrypt` can return `None` on
  tamper. (The substrate's own test cipher is XOR — illustrative only, never
  ship it.)
- **Rotation** is the operator's concern: re-encrypting an existing on-disk store
  under a new key is an offline migration (decrypt-old → encrypt-new). There is no
  in-place rotation hook yet.
- **The node identity key is also a crown jewel** — protect `tls` key material
  with the same rigor (see [`../threat-model.md`](../threat-model.md) Boundary A).

### Verify it is working

A quick check that bytes are not plaintext on disk:

```bash
# With a cipher attached, a known plaintext value must NOT appear in wal.bin:
grep -a 'MY-KNOWN-VALUE' "$BASE/$NODE_ID/kv/wal.bin" && echo "NOT ENCRYPTED" || echo "encrypted"
```

---

## 2. Outbound egress allowlist

`EgressPolicy.allow_hosts` constrains which external hosts the substrate may
reach. It is a **node-local posture, not a coordinator** — set it per node.

```rust
cfg.egress = mycelium::EgressPolicy {
    allow_hosts: vec![
        "tools.internal".into(),   // exact host
        ".corp.example".into(),    // ".suffix" → host or any subdomain
    ],
};
```

- **Empty `allow_hosts` = allow all** (the default). A non-empty list is
  **fail-closed**: a host not matched — including a URL whose host can't be parsed
  — is denied.
- Matching is case-insensitive; `.suffix` matches the bare suffix and any
  subdomain (`.corp.example` matches `corp.example` and `api.corp.example`).

### Coverage — read this carefully

The gate is currently enforced **only at the MCP client bridge**
(`connect_mcp_server`). It is **not yet enforced** in code for:

| Outbound path | Gated in code? | How to restrict today |
|---|:-:|---|
| MCP client bridge (`connect_mcp_server`) | ✓ `EgressPolicy` | allowlist above |
| LLM backend calls (SkillRunner / prompt skills) | ✗ | network layer (firewall / egress proxy) |
| Capability HTTP probes | ✗ | network layer |
| A2A client | ✗ | network layer |

For the ungated paths, enforce egress at the **network layer** (firewall rules,
security groups, an egress proxy with its own allowlist). Extending the in-code
gate to these call sites is tracked work; until then, treat `EgressPolicy` as
defence-in-depth for the MCP path, not the sole egress control.

---

## 3. Failure modes

| Symptom | Cause | Fix |
|---|---|---|
| node loses data after restart | cipher key changed/unavailable → records fail to decrypt, skipped | restore the exact key; key must be stable across restarts |
| plaintext visible in `wal.bin` | no cipher attached, or attached after `start()` | attach `with_data_at_rest_cipher` **before** `start()` |
| `connect_mcp_server` → "egress denied by policy" | target host not in `allow_hosts` | add the host (exact or `.suffix`) |
| data exfiltrated via LLM/probe/A2A | those paths are not code-gated | add network-layer egress control (see §2) |
