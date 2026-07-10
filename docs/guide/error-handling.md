# Error Handling in Mycelium

Mycelium exposes distinct error types per domain. Each handle returns exactly
the type that matches its failure modes; callers never need to downcast a
catch-all error to discover what went wrong.

---

## Error type taxonomy

| Type | Returned by | Recoverable? |
|------|-------------|:------------:|
| [`GossipError`](#gossiperror) | `GossipAgent::new` / `start` / config load | Depends on variant |
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

Re-exported from `mycelium-core` (`mycelium::GossipError`). All ten variants:

```rust
pub enum GossipError {
    InvalidField { field: &'static str, reason: String },   // a GossipConfig field is out of range
    FieldConflict { field_a: &'static str, field_b: &'static str, reason: String }, // e.g. http_port == bind_port
    NodeIdMismatch { node_id: String, bind_addr: String },  // node_id doesn't encode the bind address
    FrameTooLarge { size: usize, limit: usize },            // a frame exceeds MAX_FRAME_BYTES
    UnsupportedWireVersion { received: u8, current: u8, prev: u8, hint: &'static str }, // peer wire skew
    AlreadyRunning,                                          // start() called twice
    Shutdown,                                                // start() after shutdown (create a new agent)
    Io(std::io::Error),                                      // listener bind, WAL replay, TLS cert setup
    Toml(toml::de::Error),                                  // config file parse failure
    Parse(std::num::ParseIntError),                         // env-var parse failure
}
```

**When you see it:** mostly startup (`new` + `apply_env_overrides` + `start`) and
config loading (`GossipConfig::load_from_file`). Two are not startup-only:
`FrameTooLarge` guards the wire path and `UnsupportedWireVersion` is raised when a peer
speaks an out-of-range wire version.

**Recoverability:**
- `InvalidField` / `FieldConflict` / `NodeIdMismatch` / `Toml` / `Parse` — fix the
  configuration; always fatal at startup.
- `Io` — typically fatal at startup (port in use, cert not found). Runtime TCP errors
  (peer unreachable, write timeout) are **not** raised here — they are absorbed and
  surfaced via `system_stats().dropped_frames` and `peer_drop_counts()`.
- `FrameTooLarge { size, limit }` — the frame exceeds `framing::MAX_FRAME_BYTES`. KV writes
  are size-gated at `framing::MAX_KV_WRITE_BYTES` (= `MAX_FRAME_BYTES − 64 KiB`), so **chunk
  large state yourself**. An oversized *inbound* frame is dropped (counted in `dropped_frames`)
  without tearing down the connection, and anti-entropy *skips* an oversized entry rather than
  stalling — so this rarely reaches a caller, but it is the error to expect if you hand a single
  value larger than the budget to a write.
- `UnsupportedWireVersion` — the peer is on a different wire version (see the wire-version policy
  atop `framing.rs`); upgrade the lagging node.
- `AlreadyRunning` — call `start()` at most once per agent instance.
- `Shutdown` — create a new `GossipAgent` instead of restarting a shut-down one.

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
