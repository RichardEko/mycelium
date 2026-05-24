# SkillRunner Developer Reference

SkillRunner is a standalone binary that joins the Mycelium mesh as a
capability node driven by a `.skill.toml` manifest. Each node advertises
one named capability, handles invocations via nonce RPC, drives an
OpenAI-compatible LLM, and writes an audit trail.

## Contents

- [Quick start](#quick-start)
- [Manifest reference](#manifest-reference)
- [Invoking skills](#invoking-skills)
- [Skill composition — skills calling skills](#skill-composition)
- [Concurrency and load management](#concurrency-and-load-management)
- [Audit trail](#audit-trail)
- [OTEL span export](#otel-span-export)
- [TLS and persistence](#tls-and-persistence)
- [Running a community](#running-a-community)
- [Troubleshooting](#troubleshooting)

---

## Quick start

```sh
# Build
cargo build --bin skillrunner

# Run (requires Ollama or any OpenAI-compatible server)
./target/debug/skillrunner --skill examples/skills/hello.skill.toml

# Smoke test from a second terminal
cargo run --example invoke_skill
```

---

## Manifest reference

A `.skill.toml` has three required sections: `[node]`, `[capability]`, and `[skill]`.

### `[node]`

Controls how this node joins the mesh.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `bind_address` | string | `"127.0.0.1"` | IP address to listen on |
| `bind_port` | integer | — | **Required.** TCP port for gossip |
| `bootstrap_peers` | `["ip:port"]` | `[]` | Peers to contact on startup. Empty = standalone |
| `http_port` | integer | — | Enable embedded HTTP gateway (MCP bridge, SSE) |
| `[node.persistence]` | section | — | Enable KV durability (WAL + snapshots) |
| `[node.tls]` | section | — | Enable mTLS (auto-generates certs if paths omitted) |

```toml
[node]
bind_address    = "0.0.0.0"
bind_port       = 7947
bootstrap_peers = ["10.0.0.1:7946", "10.0.0.2:7946"]
http_port       = 9000

[node.persistence]
base_path  = "/var/lib/mycelium/my-skill"
sync_flush = false   # true = fdatasync every WAL write (safer, slower)

[node.tls]
auto_cert_dir = "./certs/"   # auto-generates CA + node cert on first run
```

### `[capability]`

Declares what this skill advertises on the mesh.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `ns` | string | — | **Required.** Namespace (e.g. `"llm"`, `"dev"`) |
| `name` | string | — | **Required.** Skill name (e.g. `"chat"`, `"code-review"`) |
| `description` | string | — | Human-readable description; gossiped as an attribute |
| `ttl_secs` | integer | `60` | Capability refresh interval. Entries age out after `3×` this |
| `[capability.input]` | JSON Schema | — | Input schema; pushed to `skills/{ns}/{name}/{node}/input` in KV |
| `[capability.output]` | JSON Schema | — | Output schema; pushed to `skills/{ns}/{name}/{node}/output` in KV |
| `[capability.policy]` | section | — | Concurrency and access control |
| `[capability.platform]` | section | — | Platform constraints (advertised as capability attributes) |

```toml
[capability]
ns          = "llm"
name        = "code-review"
description = "Reviews PR diffs for security and correctness"
ttl_secs    = 120

[capability.input]
type     = "object"
required = ["pr_number"]
[capability.input.properties]
pr_number = { type = "integer" }
focus     = { type = "string", enum = ["security", "performance", "all"] }

[capability.output]
type = "object"
[capability.output.properties]
summary = { type = "string" }
verdict = { type = "string", enum = ["approve", "request-changes"] }

[capability.policy]
max_concurrent     = 2          # reject with "skill saturated" beyond this
authorized_callers = ["orchestrator"]  # empty = unrestricted

[capability.platform]
requires = ["gpu"]   # advertised as capability attribute "requires.gpu = true"
```

### `[skill]`

Controls LLM execution and tool wiring.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `prompt` | string | — | **Required.** System prompt injected into every LLM call |
| `tools` | `["ns/name"]` | `[]` | Mesh capabilities the LLM may invoke (resolved at call time) |
| `[skill.llm]` | section | — | **Required.** LLM backend configuration |
| `[skill.otel]` | section | — | Optional OTEL span export (requires `--features otel` at build time) |

```toml
[skill]
prompt = """
You are a senior Rust engineer reviewing pull requests.
Given the PR number, fetch the diff and return structured JSON:
{"summary": "...", "verdict": "approve" or "request-changes"}
"""
tools = ["dev/fetch-diff", "dev/run-tests"]

[skill.llm]
endpoint    = "http://localhost:11434/v1"   # Ollama
model       = "llama3.2"
api_key     = ""           # empty for local Ollama; set for OpenAI / Anthropic
max_tokens  = 4096
temperature = 0.2
```

### `[skill.llm]` fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `endpoint` | string | — | **Required.** Base URL of OpenAI-compatible server (no trailing `/`) |
| `model` | string | — | **Required.** Model name (as understood by the server) |
| `api_key` | string | `""` | Bearer token; omit or leave empty for local servers |
| `max_tokens` | integer | — | Maximum tokens in the completion |
| `temperature` | float | `0.7` | Sampling temperature |

**Compatible servers:** Ollama, llama.cpp (`llama-server`), OpenAI, Anthropic (via adapter), vLLM, LM Studio, Jan.

---

## Invoking skills

Skills communicate over the Mycelium signal mesh. A caller node:
1. Discovers the skill via `resolve()` (returns the advertising `NodeId`)
2. Calls `rpc_call(node_id, "skill.invoke", json_payload, timeout)`
3. Receives the LLM output as a JSON-serialised response

### From Rust

```rust
use std::time::Duration;
use bytes::Bytes;
use mycelium::{CapFilter, GossipAgent, GossipConfig, NodeId};

let node_id = NodeId::new("127.0.0.1", 7955)?;
let mut cfg = GossipConfig::default();
cfg.bind_port = 7955;
cfg.bootstrap_peers = vec![NodeId::new("127.0.0.1", 7947)?];

let agent = std::sync::Arc::new(GossipAgent::new(node_id, cfg));
agent.start().await?;

// Discover
let filter = CapFilter::new("llm", "chat");
let (skill_node, _) = agent.resolve(&filter)
    .into_iter().next()
    .expect("no skill on mesh");

// Invoke
let input = serde_json::to_vec(&serde_json::json!({"message": "Hello!"}))?;
let result = agent.rpc_call(
    skill_node, "skill.invoke", Bytes::from(input), Duration::from_secs(30)
).await?;

let reply: serde_json::Value = serde_json::from_slice(&result)?;
println!("{}", reply["reply"]);
```

### From Python (`mycelium-py`)

```python
import json
from mycelium import MyceliumAgent

# Connect to any mesh node's HTTP gateway
agent = MyceliumAgent("127.0.0.1", 9000)   # http_port of any node

# Discover
providers = agent.resolve_capability("llm", "chat")
node_id   = providers[0].node_id

# Invoke
payload = json.dumps({"message": "What is a gossip protocol?"}).encode()
result  = agent.rpc_call(node_id, "skill.invoke", payload, timeout_secs=30)
print(json.loads(result)["reply"])
```

### Response format

SkillRunner passes the raw LLM output to the caller. If the LLM follows the
output schema (e.g. returns `{"reply": "..."}`), the response is that JSON.
On error the response is `{"error": "<description>"}`.

---

## Skill composition

A skill can call other skills by listing them in `tools`. At inference time
SkillRunner resolves the tool names from the mesh KV store, hands the schemas
to the LLM as OpenAI function-call tools, and dispatches any `tool_calls` the
LLM returns back to the mesh via `rpc_call("skill.invoke", ...)`.

Tool names in `tools` must be `ns/name`:

```toml
[skill]
prompt = "Research the topic and then write a summary report."
tools  = ["llm/researcher", "llm/writer"]
```

The call graph:

```
Caller
  └─ rpc_call → orchestrator
       ├─ tool_call → llm/researcher  → LLM inference
       │    └─ rpc_call result ──────────┐
       └─ tool_call → llm/writer ←───────┘
            └─ rpc_call result → orchestrator → Caller
```

**Load balancing:** if multiple nodes advertise `llm/researcher`, SkillRunner
picks the first result from `resolve()`. Add a `CapFilter` ranking on
`"model"` or `"locality"` attributes to control selection.

**Depth limit:** the tool-call loop runs up to 5 rounds per invocation to
prevent runaway recursion. Deep chains complete if the LLM converges quickly;
otherwise the runner returns an error.

---

## Concurrency and load management

`max_concurrent` in `[capability.policy]` limits simultaneous in-flight LLM
calls per node. Requests beyond the limit receive `{"error": "skill saturated"}`
immediately — the caller can retry or fail over to another provider via
`resolve()`.

```toml
[capability.policy]
max_concurrent = 2   # GPU-bound: only 2 parallel inferences
```

When a node is saturated its load is automatically visible to the mesh through
the opacity subsystem (`sys/load/{node}/...`). Upstream callers using
`is_node_opaque()` or the demand subsystem route around it.

---

## Audit trail

Every invocation writes a record to the gossip KV store:

```
audit/{unix_nanos}/{node_id}  →  JSON
```

The record contains:

| Field | Description |
|-------|-------------|
| `skill_ns` / `skill_name` | The invoked capability |
| `caller` | NodeId of the requesting node |
| `nonce` | RPC correlation nonce — use as trace ID |
| `success` | Whether the LLM call completed without error |
| `duration_ms` | Wall-clock time of the full invocation |
| `tool_calls` | Names of any sub-skills invoked |
| `ts_unix_nanos` | Invocation timestamp |

Because the record is gossiped, any node in the cluster can read it:

```rust
let records = agent.scan_prefix("audit/");
for (key, val) in records {
    let rec: serde_json::Value = serde_json::from_slice(&val)?;
    println!("{key}: {}", rec["skill_name"]);
}
```

Records age out via the normal KV gossip TTL (controlled by `default_ttl` in
GossipConfig, default 5 hops). For longer retention, configure persistence
and a larger TTL.

---

## OTEL span export

Build with `--features otel` and add `[skill.otel]` to the manifest:

```toml
[skill.otel]
endpoint     = "http://localhost:4317"   # OTLP gRPC collector
service_name = "my-skill"
```

Each invocation emits one OTEL span:

| Attribute | Value |
|-----------|-------|
| `skill.ns` / `skill.name` | Capability identity |
| `caller` | Requesting NodeId |
| `nonce` | RPC correlation nonce (same as audit trail) |
| `success` | Boolean |
| `duration_ms` | Wall-clock duration |
| `tool_calls` | Count of sub-skill invocations |

The span's trace ID is derived from the RPC nonce, so audit KV records and
OTEL spans cross-correlate on the `nonce` field.

```sh
cargo build --bin skillrunner --features otel
./target/debug/skillrunner --skill my.skill.toml
# → spans appear in Jaeger / Grafana / Honeycomb at the configured endpoint
```

---

## TLS and persistence

### mTLS

Set `[node.tls]` in the manifest. On first run SkillRunner auto-generates a
cluster CA and a per-node certificate in `auto_cert_dir`. All nodes in the
cluster must share the same CA.

```toml
[node.tls]
auto_cert_dir = "./mycelium-tls/"   # shared CA; unique node cert per instance
```

To use externally-issued certificates:

```toml
[node.tls]
cert_pem    = "/etc/mycelium/node.crt"
key_pem     = "/etc/mycelium/node.key"
ca_cert_pem = "/etc/mycelium/ca.crt"
```

### KV persistence

Survives unclean shutdown. Data is stored under `{base_path}/{node_id}/kv/`.

```toml
[node.persistence]
base_path  = "/var/lib/mycelium/my-skill"
sync_flush = true   # fdatasync on every WAL write; safest option
```

---

## Running a community

A community is just several SkillRunner processes with overlapping
`bootstrap_peers`. Each process:

1. Joins the mesh and gossips its capability advertisement
2. Pushes its input/output schema to `skills/{ns}/{name}/{node}/input`
3. Waits for `skill.invoke` RPC calls

### 3-skill example

See [`examples/community/`](../examples/community/) for ready-to-run manifests
and a startup script:

```sh
cd examples/community
./start.sh        # starts orchestrator + researcher + writer
./stop.sh         # graceful shutdown
```

The orchestrator's `tools = ["llm/researcher", "llm/writer"]` resolves both
specialists from the mesh at call time — adding more researcher nodes
automatically provides load balancing.

### Choosing ports

Each skill needs its own `bind_port`. Recommended convention:

| Role | Port range |
|------|-----------|
| Primary mesh nodes / services | 7940–7949 |
| SkillRunner skills | 7950–7999 |
| HTTP gateways | 9000–9099 |

### Environment variables

All `GossipConfig` fields can be overridden via `GOSSIP_*` env vars:

```sh
GOSSIP_BIND_PORT=7951 GOSSIP_BOOTSTRAP_PEERS="10.0.0.1:7946" \
    ./skillrunner --skill my.skill.toml
```

---

## Troubleshooting

**`timed out waiting for capability on mesh`**

The skill isn't visible to the caller. Check:
- Both nodes are running and ports are reachable (`nc -zv 127.0.0.1 7950`)
- `bootstrap_peers` in the caller's config points at the skill's `bind_port`
- The capability TTL hasn't expired (check `ttl_secs`; lower = more frequent refresh)
- Run with `RUST_LOG=mycelium=debug` to see gossip activity

**`{"error": "HTTP error: ..."}`**

The LLM endpoint returned an error. Check:
- Ollama is running: `curl http://localhost:11434/api/tags`
- The model is pulled: `ollama pull llama3.2`
- The `endpoint` in `[skill.llm]` matches the server's URL (no trailing `/`)

**`{"error": "skill saturated"}`**

All `max_concurrent` slots are occupied. Options:
- Increase `max_concurrent` in `[capability.policy]`
- Run a second instance of the skill (mesh load-balances automatically)
- Implement retry with exponential backoff in the caller

**`{"error": "exceeded maximum tool-call rounds"}`**

The LLM issued more than 5 tool calls without producing a final response.
Reduce the scope of the prompt or add an explicit instruction like
"Make at most 2 tool calls then return your final answer."

**Port already in use**

Each SkillRunner instance needs a unique `bind_port`. If running multiple
skills on one machine, assign sequential ports (7950, 7951, 7952, …).
