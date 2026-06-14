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

## 4. Retention (known limitation)

Audit records are normal replicated KV entries; the trail grows unbounded, as the
pre-WS2 trail did. Pruning a hash chain is **not** as simple as deleting old keys:
verification runs from genesis, so removing record 0 makes the whole stream read
as `SequenceGap`. Until checkpointing lands (a later WS), options are:

- Leave the trail in-cluster and size `GOSSIP_MAX_STORE_ENTRIES` accordingly.
- Periodically **export** streams to an external WORM store / SIEM (scan
  `sys/audit/`, verify, archive) and treat the in-cluster trail as a hot window.

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
