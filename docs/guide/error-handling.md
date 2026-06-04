# Error Handling in Mycelium

Mycelium exposes distinct error types per domain. Each handle returns exactly
the type that matches its failure modes; callers never need to downcast a
catch-all error to discover what went wrong.

---

## Error type taxonomy

| Type | Returned by | Recoverable? |
|------|-------------|:------------:|
| [`GossipError`](#gossipError) | `GossipAgent::new` / `start` / config load | Depends on variant |
| [`ConsistencyError`](#consistencyerror) | `ConsensusHandle::consistent_set`, `distributed_lock`, `elect_leader` | Yes — retry |
| [`RpcError`](#rpcerror) | `ServiceHandle::rpc_call` | Yes — retry / call another peer |
| [`QuorumError`](#quorumerror) | `KvHandle::set_with_min_acks` | Yes — retransmit when peers rejoin |
| [`ScatterError`](#scattererror) | `ServiceHandle::scatter_gather` | Yes — retry or reduce `min_ok` |
| [`SchemaError`](#schemaerror) | `SchemaHandle::publish_schema`, `seed_schemas_from_dir` | Depends on variant |
| [`BulkError`](#bulkerror) | `ServiceHandle::bulk_call` | Yes — retry |
| [`ShardError`](#sharderror) | `ServiceHandle::emit_sharded` | Yes — wait for providers |

Feature-gated extras: `PromptSkillError` (`llm` feature), `LlmError` (`llm`),
`McpError` (internal to the MCP bridge, not public).

---

## `GossipError`

```rust
pub enum GossipError {
    Network(String),    // TCP dial/accept failure
    Config(String),     // invalid field in GossipConfig
    State(String),      // runtime state corruption (rare)
    Io(std::io::Error), // file I/O (persistence, TLS certs)
    Toml(toml::de::Error),          // config file parse failure
    Parse(std::num::ParseIntError), // env-var parse failure
}
```

**When you see it:** startup (`new` + `apply_env_overrides` + `start`), TOML
config loading (`GossipConfig::load_from_file`).

**Recoverability:**
- `Config` / `Toml` / `Parse` — fix the configuration; these are always fatal
  at startup.
- `Network` / `Io` — typically fatal at startup (port in use, cert not found);
  occasionally surfaces in long-running lifecycle paths.
- `State` — internal invariant violation; treat as a bug, not a runtime error.

---

## `ConsistencyError`

```rust
pub enum ConsistencyError {
    Timeout { ballots_tried: u32 }, // no quorum reached within deadline
    Superseded,                     // another node committed first
    TopologyUnsatisfied,            // quorum met but Hard topology gate failed
}
```

**When you see it:** `consistent_set`, `consistent_get`, `append`,
`distributed_lock`, `elect_leader`.

**Recoverability:**
- `Timeout` — retry; the cluster may be partitioned or underloaded. Check
  `ballots_tried` to distinguish a slow cluster from a hard split.
- `Superseded` — a concurrent writer won the slot. Re-read the current value
  and decide whether to retry with a new key or accept the other writer's value.
- `TopologyUnsatisfied` — quorum has the right headcount but the Hard topology
  policy (e.g. "must span two racks") was not satisfied. Retry is unlikely to
  help unless nodes rejoin from the missing segments.

---

## `RpcError`

```rust
pub enum RpcError {
    Timeout,  // no reply before the deadline
}
```

**When you see it:** `ServiceHandle::rpc_call`.

**Recoverability:** yes. The target node may be slow or temporarily
unreachable. Retry against the same node, or resolve a different provider via
`capabilities().resolve(...)` and retry there.

---

## `QuorumError`

```rust
pub enum QuorumError {
    Timeout { acks_received: usize }, // fewer peers ACKed than requested
}
```

**When you see it:** `KvHandle::set_with_min_acks`.

**Recoverability:** yes. The write succeeded locally and will propagate
eventually. The `acks_received` field tells you how many peers did confirm;
you can relax the durability requirement or retry when more peers rejoin.

---

## `ScatterError`

```rust
pub enum ScatterError {
    InsufficientReplies { got: usize, needed: usize },
}
```

**When you see it:** `ServiceHandle::scatter_gather`.

**Recoverability:** yes. Fewer than `min_ok` targets replied before timeout.
Check `got` / `needed` and either retry, reduce `min_ok`, or wait for more
capable peers to join.

---

## `SchemaError`

```rust
pub enum SchemaError {
    InvalidJson(String),                           // bytes are not valid JSON
    NotAnObject { kind: &'static str },            // JSON root is not an object
    InvalidSchemaId { id: String, reason: &'static str }, // malformed schema ID
    Io { path: PathBuf, source: std::io::Error },  // file read error
}
```

**When you see it:** `SchemaHandle::publish_schema`, `force_publish_schema`,
`seed_schemas_from_dir`.

**Recoverability:**
- `InvalidJson` / `NotAnObject` / `InvalidSchemaId` — fix the schema bytes or
  ID; these are always caller errors.
- `Io` — file system error during directory seeding; the `path` field
  identifies the offending file.

Note: `publish_schema` returns `SchemaPublishResult` (not an error type) to
distinguish `Published`, `Unchanged`, and `Conflict` outcomes. `SchemaError`
is only returned when the input itself is malformed.

---

## `BulkError`

```rust
pub enum BulkError {
    Timeout,      // target did not fetch staged payload before deadline
    NoHttpPort,   // caller has no http_port configured in GossipConfig
}
```

**When you see it:** `ServiceHandle::bulk_call`.

**Recoverability:**
- `Timeout` — retry; the target node may have been slow to connect.
- `NoHttpPort` — configuration error; set `GossipConfig::http_port` before
  starting the agent. This will always fail until the config is fixed.

---

## `ShardError`

```rust
pub enum ShardError {
    NoProviders,  // no peers match the capability filter at call time
}
```

**When you see it:** `ServiceHandle::emit_sharded`.

**Recoverability:** yes. Wait for providers to advertise the required
capability and retry, or widen the `CapFilter`.

---

## Propagation strategy

All six handles propagate errors via `?` throughout their implementations.
`GossipError` is the only error type that surfaces from startup and lifecycle
paths; domain errors (`ConsistencyError`, `RpcError`, etc.) only surface from
the specific calls that can fail at runtime.

There is no global error wrapper — callers match exactly the variants they
care about:

```rust
match agent.consensus().consistent_set("seq/head", b"v2").await {
    Ok(())                                   => { /* committed */ }
    Err(ConsistencyError::Superseded)        => { /* read current, re-evaluate */ }
    Err(ConsistencyError::Timeout { .. })    => { /* retry */ }
    Err(ConsistencyError::TopologyUnsatisfied) => { /* alert ops */ }
}
```

`unwrap()` inside the library is limited to slice-indexing operations where
the invariant is enforced by the type system, plus a small number of `Mutex`
lock calls where poisoning is recovered rather than propagated.

---

## Relationship diagram

```
GossipAgent lifecycle ──── GossipError
KvHandle ────────────────── (infallible set/get; QuorumError for set_with_min_acks)
MeshHandle ─────────────── (infallible emit / signal_rx)
ConsensusHandle ─────────── ConsistencyError
ServiceHandle ──────────── RpcError, ScatterError, BulkError, ShardError
SchemaHandle ───────────── SchemaError (malformed input)
                           SchemaPublishResult (conflict detection — not an error)
CapabilitiesHandle ──────── (infallible resolve; no runtime errors)
```
