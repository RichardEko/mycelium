# Mycelium — RBAC / Identity Operations Runbook

Operator-facing configuration and verification for the WS1 RBAC subset
(`compliance` feature). Concept and API: [`docs/guide/09-security.md`](../guide/09-security.md)
§Role-based access control. Architecture invariants: `CLAUDE.md` §RBAC / identity.

The `compliance` feature is `["gateway", "tls"]`. **TLS is a hard prerequisite** —
roles are Ed25519-signed by the node's TLS identity key, so a node with no
`GossipConfig::tls` cannot advertise roles (`advertise_roles` returns
`InvalidField { field: "tls" }`).

---

## 1. Build & enable

```bash
cargo build --features compliance        # library / embedding
cargo build --bin skillrunner --features compliance
```

```rust
let mut cfg = GossipConfig::default();
cfg.tls = Some(TlsConfig::default());    // required: signs roles, mTLS transport
// gateway ACLs (optional):
cfg.gateway_scoped_tokens = vec![
    GatewayToken { token: "orchestrator".into(),
                   scopes: vec!["kv:read".into(), "kv:write".into(), "mesh:write".into()] },
    GatewayToken { token: "readonly".into(),
                   scopes: vec!["kv:read".into()] },
];
```

Distribute the auto-generated `./mycelium-tls/ca-cert.pem` to every node (shared
cluster CA) — see the TLS runbook in `09-security.md`.

---

## 2. Scope vocabulary (gateway ACLs)

Coarse `resource:verb` families. A route admits a token holding the required
scope **or** `"*"`. Unmapped routes require `admin` (deny-by-default).

| Scope | Grants |
|---|---|
| `kv:read` / `kv:write` | `GET /gateway/kv*` / `POST`,`DELETE /gateway/kv*`, `/kv/quorum` |
| `cap:read` / `cap:write` | capability resolve, shard owner / advertise, drop |
| `mesh:read` / `mesh:write` | signal SSE, mailbox/rpc-serve, demand / signal emit, rpc call, scatter |
| `consensus:read` / `consensus:write` | overlay log scan, consistent get / consistent set, lock, elect, log append, cross-group propose |
| `llm:read` / `llm:write` / `llm:invoke` | prompt get/list / prompt put,delete / llm call,stream |
| `*` | everything (the legacy `gateway_auth_token` is equivalent) |
| `admin` | the deny-by-default fallback for any route not in the table |

**Public, never scope-gated** (M16 edge criterion): `/health`, `/ready`,
`/stats`, `/metrics`, and the A2A descriptor (`/.well-known/agent.json`).

---

## 3. Advertise & verify roles

```rust
agent.advertise_roles(["admin".into(), "orchestrator".into()], /* clearance L3 */ 3)?;
```

- `clearance` is the L1/L2/L3 data-classification level (0–255; 1/2/3 by convention).
- The claim persists at `sys/role/{node}` and anti-entropy-syncs like any KV entry;
  re-call to update.
- Other nodes read it **verified**: `agent.roles_of(&node)` returns `Some` only if
  the signature checks against the cluster-learned identity key. A forged write
  reads back as `None`.

Capability providers gate invocations with `authorized_callers` (empty = open):

```toml
# in a .skill.toml — SkillRunner enforces this automatically under compliance
[policy]
authorized_callers = ["orchestrator", "127.0.0.1:8080"]   # role names or NodeIds
```

---

## 4. Verification checklist

```bash
# Gateway ACL — expect 200 / 403 / 401
curl -s -o /dev/null -w '%{http_code}\n' -H 'Authorization: Bearer readonly' \
     http://NODE:PORT/gateway/kv/keys            # 200 (kv:read)
curl -s -w '%{http_code}\n' -H 'Authorization: Bearer readonly' \
     -X POST http://NODE:PORT/gateway/kv -d '{"key":"k","value":"v"}'   # 403 + {"required_scope":"kv:write"}
curl -s -o /dev/null -w '%{http_code}\n' http://NODE:PORT/gateway/kv/keys   # 401 (no token)
curl -s -o /dev/null -w '%{http_code}\n' http://NODE:PORT/health             # 200 (public)
```

---

## 5. The `sys/` namespace tripwire

Core diagnostic (present even without `compliance`). A **remote** write naming
this node in a self-owned `sys/` prefix (`identity`, `load`, `role`, `tuple`)
is flagged — detection, not prevention (the write still applies per LWW).

```bash
curl -s http://NODE:PORT/stats | jq '.sys_namespace_violations'
```

- **Steady-state value: `0`.** Any non-zero value warrants investigation: a peer
  is writing keys only this node should own (misconfiguration, a buggy client,
  or a hostile node in the mesh).
- Each detection also emits a `warn!` naming the offending key.
- `sys/quorum/` is intentionally **not** flagged — peers legitimately write
  quorum evidence naming the node they observed.

Pair with `commit_conflicts` (the consensus tripwire) on the same `/stats`
endpoint as the two "promise-strength namespace violated" signals.

---

## 6. Failure modes

| Symptom | Cause | Fix |
|---|---|---|
| `advertise_roles` → `InvalidField { field: "tls" }` | no `GossipConfig::tls` | enable TLS; roles require the identity key |
| `roles_of(peer)` always `None` | peer's `sys/identity/` not yet learned, or unshared CA | confirm peering + that the CA cert is distributed |
| every gateway request → 401 | token not in `gateway_scoped_tokens` / no `Bearer` header | check the token list and header |
| legitimate route → 403 | scope not granted; or route is unmapped (needs `admin`) | grant the scope shown in `required_scope`, or `"*"` |
| `sys_namespace_violations` climbing | a peer clobbering this node's owned keys | identify the source from the `warn!` log; treat as a trust-boundary incident |
