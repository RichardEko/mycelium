# Mycelium — Audit & Evidence Operations Runbook

Operator-facing guide to the WS2 tamper-evident audit trail (`compliance`
feature). Concept and API: [`docs/guide/09-security.md`](../guide/09-security.md)
§The audit trail in practice. Architecture invariants: `CLAUDE.md` §Audit trail.

Like RBAC, audit signing requires the TLS identity — `compliance = ["gateway",
"tls"]`. A node with no `GossipConfig::tls` cannot seal signed records; under
`compliance` `agent.audit()` returns `InvalidField` and the event is logged, not
written. Configure TLS on every node that must produce evidence.

---

## 1. What gets recorded, and where

| Build | Trail key | Signed | Hash-chained |
|---|---|:-:|:-:|
| default (no `compliance`) | `audit/{ts_unix_nanos}/{node}` | ✗ | ✗ |
| `compliance` | `sys/audit/{node}/{seq:016x}` | ✓ | ✓ |

Each `compliance` record carries: the recording `node_id`, a per-node monotonic
`seq` (genesis = 0), an `hlc` timestamp, the `principal` (who caused the event),
an `action` (`Write`/`Read`/`Invoke`/`Admin`), the `target` resource, an
`outcome` (`Success`/`Denied`/`Error`), optional `detail`, and `prev_hash` (the
SHA-256 content hash of the previous record in this node's stream).

**The chain is per-node.** There is no global chain (that would need a
coordinator). Verify each node's stream independently; the cluster trail is the
union of those streams.

---

## 2. Querying the trail

```bash
# All streams, verified, most-recent 100 records each:
curl -H 'Authorization: Bearer <audit-token>' \
     'http://NODE:PORT/gateway/audit?limit=100'

# One node's full stream:
curl -H 'Authorization: Bearer <audit-token>' \
     'http://NODE:PORT/gateway/audit?node=10.0.0.7:8080'
```

The endpoint requires the `audit:read` scope (configure a `gateway_scoped_tokens`
entry granting it — see [`rbac.md`](rbac.md) §2). Response shape:

```json
{ "streams": [
  { "node": "10.0.0.7:8080",
    "count": 1422,
    "verified": true,
    "verify_error": null,
    "head_hash": "9f86d081…",
    "records": [ { "seq": 1421, "hlc": 173…, "principal": "10.0.0.3:8080",
                   "action": "Invoke", "target": "orders/place",
                   "outcome": "Success", "detail": "…", "content_hash": "…" } ] } ] }
```

Programmatically: `agent.audit_stream(&node)` (decoded, seq-ordered),
`agent.audit_verify(&node)` (full-stream verification), `agent.audit_stream_nodes()`.

---

## 3. Verifying evidence

`verified: true` means, for that stream: every record's Ed25519 signature checks
against the node's identity key, the sequence is contiguous from genesis, and
every `prev_hash` matches its predecessor's content hash. A `verify_error` names
the **first** offending `seq`:

| `verify_error` | Meaning | Likely cause |
|---|---|---|
| `BadSignature { seq }` | a record's signature failed | the record was edited, or signed by the wrong key |
| `BrokenLink { seq }` | `prev_hash` ≠ predecessor's content hash | a record was edited or removed |
| `SequenceGap { expected, found }` | seq not contiguous | a record is missing or reordered |
| `WrongOwner { seq }` | record claims a different node | a forged or misfiled entry |
| `UnknownSigner` | this node lacks the stream owner's key | owner's `sys/identity/` not yet learned, or unshared CA |

**Any `verified: false` on a stream you did not intentionally prune is a
trust-boundary incident.** Capture the stream, the `verify_error`, and the
offending `seq`; the content hashes let you cite exactly which record changed.

To cite a specific event in an external report, record its `content_hash` — it is
stable for the logical record and changes if any field is altered.

---

## 4. Export to an external SIEM / WORM (SOC 2 WS-C)

Attach an `AuditSink` and every sealed record is streamed to your external store on a
background drain task, **off the write path**. The in-cluster hash-chain stays
authoritative; the sink is the long-term / tamper-evident mirror.

```rust
use mycelium::{AuditSink, SignedAuditRecord};

struct SiemSink { /* your client / buffer */ }
impl AuditSink for SiemSink {
    fn export(&self, rec: &SignedAuditRecord) {
        // push rec.encode() to your SIEM/WORM. Runs on the drain task — keep it
        // non-blocking (buffer, or spawn_blocking your own IO).
    }
}

agent.with_audit_sink(std::sync::Arc::new(SiemSink { /* … */ })); // before start()
```

Semantics: the drain channel is bounded; if your sink can't keep up, the **mirror** copy is
dropped with a `warn!` — never the chain record, which you can always re-export from
`sys/audit/`. So a slow sink degrades to "re-export from the chain", never to lost audit data.

## 5. Retention (checkpointing — WS-D, in progress)

Audit records are normal replicated KV entries; the trail grows unbounded. Pruning a hash
chain is **not** as simple as deleting old keys: verification runs from genesis, so removing
record 0 makes the whole stream read as `SequenceGap`. **Checkpointing (WS-D)** lands a signed
mid-chain boundary so exported records can be pruned and verification resumes from the
checkpoint; until it ships:

- Leave the trail in-cluster and size `GOSSIP_MAX_STORE_ENTRIES` accordingly.
- Use the `AuditSink` above to mirror to WORM/SIEM and treat the in-cluster trail as a hot
  window (for 7-yr-class HIPAA retention the WORM archive is the system of record).

Do **not** hand-delete `sys/audit/` keys on a live node expecting verification to
still pass — it won't, and that is by design (the chain is meant to notice).

---

## 5. Failure modes

| Symptom | Cause | Fix |
|---|---|---|
| no `sys/audit/` entries under `compliance` | `GossipConfig::tls` unset → `audit()` errors | enable TLS; check logs for "set GossipConfig::tls" |
| `/gateway/audit` → 403 | token lacks `audit:read` | grant the scope (or `*`) on the token |
| `verified: false`, `UnknownSigner` | owner's identity key not learned | confirm peering + shared CA; the key arrives via `sys/identity/` gossip |
| `verified: false`, `BrokenLink`/`BadSignature` | records tampered or store hand-edited | treat as an incident; cite the offending `seq` + `content_hash` |
| trail not shrinking | by design — no time-eviction of live keys | export + size the store; see §4 |

---

## 6. `/gateway/transparency` — revocation proofs

Key **revocations** get their own tamper-evident surface: a Merkle transparency log over
each node's validated revocation list, served at `GET /gateway/transparency` (scope
`transparency:read`; `compliance` feature). It answers a different question from §2's audit
trail — not "what happened" but "**can I prove this key was revoked fleet-wide, without
trusting the server that told me?**"

Two modes:

```bash
# Head mode (no query) — every node's revocation-log Merkle root + count:
curl -H 'Authorization: Bearer <transparency-token>' \
     'http://NODE:PORT/gateway/transparency'
# → { "nodes": [ { "node": "10.0.0.7:8080", "root": "<64-hex>", "count": 3 }, … ] }

# Inclusion-proof mode (?node=&key=<64-hex of the revoked verifying key>):
curl -H 'Authorization: Bearer <transparency-token>' \
     'http://NODE:PORT/gateway/transparency?node=10.0.0.7:8080&key=<64-hex>'
# → { "node": "…", "revoked_key": "…", "included": true,
#     "root": "<64-hex>", "leaf": "<64-hex>", "index": 1,
#     "proof": [ { "sibling": "<64-hex>", "on_right": true }, … ] }
```

**What it proves.** The inclusion `proof` is the Merkle audit path from the `leaf` (the
revocation record's hash) up to the `root`. The operator verifies it **client-side** —
recompute the root from `leaf` + `proof` and check it equals `root` (the
`transparency::verify_inclusion` function, exposed for exactly this). Because the root is
recomputable on any node from its own gossiped view, a matching proof shows the key was
revoked *and* that the node's revocation log can't have been forged or silently altered —
no trust in the responding server is required. `included: false` means that node has no
validated revocation for the key.

The head `count` is also the cheap fleet-wide sanity check: if nodes disagree on a node's
`root`/`count`, revocations haven't fully propagated yet (or one view is partitioned).

*Code: `src/agent/http.rs::gw_transparency` (~708), `src/agent/transparency.rs`
(`inclusion_proof`, `revocation_head`, `verify_inclusion`), `src/agent/revocation.rs`.*

---

## 7. Proving a guardrail stopped an agent

When a node runs the `mycelium-guardrails` **Tier-C** invoke gate (feature `compliance`),
every *unauthorized* invocation it blocks is sealed as an `Invoke`/`Denied` record into
**this same** per-node, signed, hash-chained audit trail (§1) — verified caller as
`principal`, the RPC kind as `target`. So "prove agent X was stopped from doing Y" reduces
to reconstructing and re-verifying that chain and citing the sealed denials.

The [`prove_denials` / `narrate_proof`](../guide/16-guardrails.md#proving-a-guardrail-fired)
tool (guide 16) does exactly that — for a compliance officer, the flow is:

```rust
use mycelium_guardrails::{prove_denials, narrate_proof};
// provider = the node that ran the gate; caller = the agent you want to prove was stopped.
let proof = prove_denials(&any_node, provider.node_id(), Some("10.0.0.3:8080"));
for line in narrate_proof(&proof) { println!("{line}"); }
```

It pulls `provider`'s stream, runs the full §3 chain verification (`chain_verified` +
`verify_error`), and lists every matching sealed denial with its citable `content_hash`.
**Any node can run it** — the audit chain gossips fleet-wide, so a neutral third party
proves the denial exactly as the provider would.

**Read the claim honestly** (the tool prints this framing itself): it proves the provider
*tamper-evidently sealed stopping* those specific calls — **provable-stopping**, not a
global "X could not have done Y *anywhere*." The [honest limits](../guide/16-guardrails.md#proving-a-guardrail-fired)
carry straight over from §3: the chain is **per-node**, so absence in one provider's chain
is not proof of absence elsewhere; only *guarded* capabilities that reach the gate seal
denials; and if the chain doesn't verify, the proof is **voided**, not asserted. Verify the
underlying chain with §3 before citing any denial.

*Code: `mycelium-guardrails/src/verify.rs` (`prove_denials`, `narrate_proof`),
`mycelium-guardrails/src/guard.rs` (the sealing gate).*
