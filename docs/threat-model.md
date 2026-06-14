# Mycelium — Blast-Radius Threat Model

*Crown-jewel posture (Production Readiness Gap sub-gate #3, WS3).* This document
states what an attacker gains at each trust boundary, the substrate mitigations
in force, and the residual risk an operator must own. It is deliberately blunt:
the value here is a specific blast-radius conversation, not a reassurance.

Cross-links: operator runbook [`operations/crown-jewel.md`](operations/crown-jewel.md);
RBAC [`operations/rbac.md`](operations/rbac.md); audit [`operations/audit.md`](operations/audit.md);
security guide [`guide/09-security.md`](guide/09-security.md). Mechanisms referenced
here ship under the `tls` and `compliance` features unless noted.

---

## 1. Assets (the crown jewels)

| Asset | Where it lives | Why it matters |
|---|---|---|
| **Digital-twin state** (L1/L2/L3) | KV store + WAL/snapshot on disk | The concentrated map of every SPOF, critical path, and escalation route in the deployment. The highest-value target. |
| **Node identity keys** | Ed25519 signing key (`tls`), on disk | Compromise lets an attacker impersonate a node: forge signed KV writes, role claims, audit records, consensus votes. |
| **Audit trail** | `sys/audit/{node}/{seq}` | The tamper-evidence record. An attacker wants to edit history without detection. |
| **Role/clearance claims** | `sys/role/{node}` | Authorization input; forging one would grant access. |
| **Outbound reach** | network egress | A compromised node with open egress is the data-exfiltration vector. |

The L1/L2/L3 **clearance** model (WS1) reflects that these are not uniform: an
L1 board-level read is not the full L3 SPOF topology, and authorization is
classification-aware.

---

## 2. Trust boundaries & blast radius

### Boundary A — a single compromised node

An attacker who controls one node process (RCE, stolen host) gains:

- **Its own identity key** → can sign anything *as that node*: KV writes, its own
  role claim, its own audit records, consensus votes. Cannot forge *other* nodes'
  signatures (those keys never leave their hosts).
- **Read of all replicated KV state** that reached this node via gossip — i.e. the
  twin state. This is the worst single-node outcome: the map is replicated, so one
  node sees (much of) it.
- **Ability to clobber un-owned `sys/` keys** via LWW — but this is **detected**:
  the `sys/` namespace tripwire (`SystemStats::sys_namespace_violations`) flags a
  remote write to a key another node owns, and signed keys (`identity`, `role`,
  `audit`) fail signature verification at read, so a forged value reads as absent.

*Mitigations:* per-node key isolation; signature verification on all signed
entries (detection-not-prevention); the namespace tripwire; data-at-rest
encryption (WS3) limits an attacker with *disk* access but not process control.

*Residual:* a fully compromised node sees the twin state it has gossiped. Mycelium
does not encrypt in memory. Containment is the clearance model (limit what each
node holds) + operational isolation, not a substrate guarantee.

### Boundary B — the trusted gossip domain

Without `tls`, every node on the gossip port is assumed cooperative: a connected
peer can inject KV entries (bounded by LWW timestamps), claim any NodeId in a
`StateRequest` (harmless — misdirected response), or poison a dedup nonce
(P < 1/2⁶⁴). **Do not expose the gossip port to untrusted networks without TLS.**

*Mitigations:* `tls` makes the domain cryptographically closed — mTLS on every
connection (cluster-CA-signed certs), so a node without the shared CA cannot
join; signed consensus payloads and signed KV writes (`SignedData`, wire v10)
make undetected mutation require a node's private key.

*Residual:* a valid cluster member is trusted to gossip; mTLS authenticates
*membership*, not *intent*. RBAC (WS1) constrains what a member may *assert/serve*;
the audit trail (WS2) records what it did.

### Boundary C — external egress

A compromised node with unrestricted outbound reach can exfiltrate twin state to
an attacker-controlled endpoint, or pull malicious tool definitions.

*Mitigations:* `EgressPolicy.allow_hosts` (WS3) gates outbound at the MCP client
bridge — the canonical "twin reaches an external tool server" path. Empty = allow
all (default); set it to fail-closed against unlisted hosts.

*Residual & coverage (be honest):* the egress gate currently covers the **MCP
client bridge** only. **Not yet gated** in code: LLM backend calls (SkillRunner /
prompt skills), capability HTTP probes, and the A2A client. For those, restrict
egress at the network layer (firewall / security group / proxy allowlist) — see
the crown-jewel runbook. Extending the gate to those call sites is tracked work.

---

## 3. Data at rest

The substrate provides wire-level mTLS but does **not** encrypt the store in
memory. On disk, the **opt-in** `DataAtRestCipher` hook (WS3) envelope-encrypts
WAL records and snapshots; the operator supplies the KMS/keyring adapter (the
substrate is neutral on key custody). Without an attached cipher, persistence
bytes are plaintext on disk — a stolen disk or backup is a disclosure.

*Operator responsibility:* attach a cipher whose key is in a KMS/HSM and stable
across restarts; protect the node identity key (`tls`) with the same rigor — it
is itself a crown jewel (Boundary A).

---

## 4. What the substrate does NOT defend against

- **In-memory compromise** of a running node (no memory encryption).
- **A trusted member acting maliciously within its authorization** — RBAC limits
  scope and audit records intent, but a member authorized for X can do X.
- **Egress on the not-yet-gated paths** (LLM/probe/A2A) — network-layer control.
- **Availability attacks** beyond the documented gossip backpressure/opacity
  mechanisms.
- **Key custody** — Mycelium provides hooks; the KMS/HSM and rotation are the
  operator's.

This list is the point of the document: it converts "is it secure?" into a
specific, reviewable set of boundaries and residual risks.
