# Prompt Skills — Design Reference

> **Status: PLANNED** — not yet implemented.
> Target: next development phase after empirical benchmarks for AAMAS 2027 paper.

---

## What this is

A first-class mechanism for storing, distributing, and invoking **LLM-backed skills** across
a Mycelium cluster. A Prompt Skill binds a named prompt template (stored in the gossip KV
store) to an LLM backend (local to the node), and exposes the result as a callable capability
on the capability ring — indistinguishable to callers from any other Mycelium skill.

The prompt is a cluster-wide resource; the LLM credentials and rate limits are node-local.
Update the prompt once; every node serving that skill picks it up on the next invocation
without redeployment.

---

## Problem statement

Mycelium today provides the plumbing (KV store, MCP bridge, capability ring, RPC) but no
first-class concept of an LLM-backed skill with a managed prompt. A developer must:

1. Register an MCP tool manually
2. Hardcode the prompt inline in the handler
3. Repeat this independently on every node that should serve the skill
4. Redeploy all nodes to change the prompt

This means prompts are not cluster resources — they are node-local implementation details.
In an AI agent fleet, the prompt *is* the unit of behaviour. It belongs in the substrate.

---

## Core concepts

### PromptTemplate

The prompt definition stored in KV. Contains the prompt content and output-shaping
parameters — but **not the model identifier**:

```rust
pub struct PromptTemplate {
    /// System prompt. May contain {{variable}} placeholders.
    pub system: String,
    /// User message template. Must contain at least {{input}}.
    pub user_template: String,
    /// Maximum tokens in the response.
    pub max_tokens: u32,
    /// Sampling temperature. 0.0 = deterministic.
    pub temperature: f32,
    /// Arbitrary metadata (tags, version notes, author).
    /// Use `metadata["model_hint"]` to document the intended model as a
    /// non-binding annotation; use capability attributes for hard routing.
    pub metadata: HashMap<String, serde_json::Value>,
}
```

`model` is intentionally absent from `PromptTemplate`. Model availability is **node-local
knowledge** — each node knows what LLM it can reach; the template author does not. Baking a
model name into the cluster-wide KV entry would centralise a decision that belongs to each
node (Hayek: local knowledge cannot be correctly specified from a central point).

Each `LlmBackend` instance is constructed with its model baked in (e.g.
`OpenAiBackend::new("https://api.anthropic.com/v1", api_key, "claude-sonnet-4-6")`). If
callers need to route to a node with a specific model, they advertise the model name as a
capability attribute and filter with `CapFilter::with_attribute("model", "claude-sonnet-4-6")`.
That is the existing, correct, decentralised routing mechanism.

### LlmBackend

The pluggable abstraction over LLM providers. Node-local; not gossip-replicated.
Each backend instance is constructed with its model baked in — `complete` and `stream`
take only prompt content, not a model selector:

```rust
/// The result of a single LLM completion.
pub struct LlmResult {
    pub output:     String,
    pub model_used: String,   // which model actually responded (for observability)
    pub tokens_used: u32,
}

#[async_trait::async_trait]
pub trait LlmBackend: Send + Sync {
    async fn complete(
        &self,
        system:      &str,
        user:        &str,
        max_tokens:  u32,
        temperature: f32,
    ) -> Result<LlmResult, LlmError>;

    /// Optional: token-by-token streaming. Default impl calls complete() and
    /// yields the full response as a single chunk.
    async fn stream(
        &self,
        system:      &str,
        user:        &str,
        max_tokens:  u32,
        temperature: f32,
        tx:          tokio::sync::mpsc::Sender<String>,
    ) -> Result<LlmResult, LlmError> {
        let r = self.complete(system, user, max_tokens, temperature).await?;
        let _ = tx.send(r.output.clone()).await;
        Ok(r)
    }
}
```

`model_used` in `LlmResult` surfaces which model actually responded — useful for
observability and debugging without baking a model constraint into the template.

Built-in implementations:

| Backend | Constructor | Description |
|---|---|---|
| `OpenAiBackend` | `::new(base_url, api_key, model)` | OpenAI-compatible REST API (Claude API, OpenAI, Mistral, Ollama) |
| `EchoBackend` | `::new()` | Test double — returns `"echo: {input}"`, `model_used = "echo"` |

`McpBackend` is **not included**. The MCP bridge flows LLM → Mycelium; a backend that reversed
that direction would be architecturally inconsistent with the existing mesh. If a future use case
requires calling an external agent as an LLM backend, that belongs in an `A2aBackend` using the
already-built A2A protocol — not through MCP.

### Template rendering

Simple `{{variable}}` substitution. No new dependencies — 15-line inline implementation.
Reserved variables:

| Variable | Value |
|---|---|
| `{{input}}` | The caller-supplied input string |
| `{{node_id}}` | The serving node's address |
| `{{skill_name}}` | `{ns}/{name}` |
| `{{timestamp}}` | ISO-8601 UTC at invocation time |

Additional variables supplied by the caller via a `context` map at call time.

---

## KV namespace

```
prompts/{ns}/{name}          →  PromptTemplate (JSON-encoded, TTL = 604800s / 1 week)
```

This follows the existing `cap/` and `tools/` namespace conventions.

The TTL is deliberately long because prompt templates are **configuration**, not a presence
signal. Capability heartbeats (TTL 30s, continuously refreshed while the node lives) control
discoverability. The template entry should survive node restarts independently — it only
evaporates when no node has updated it for a week, indicating the skill has been abandoned.
There is **no refresh task** for the template entry; `update_prompt` is the only write path.
To explicitly decommission a skill, call `delete_prompt` (tombstone) or let TTL expire.

Capability advertisement (existing `cap/` namespace, no change to existing machinery):

```
cap/{node_id}/{ns}/{name}    →  CapabilityEntry   (TTL = 30s, refreshed while node is alive)
```

No special `provides` tags are needed on the capability entry. Discovery via
`resolve(&CapFilter::new(ns, name))` is sufficient. `list_prompts()` scans the `prompts/`
prefix directly.

RPC signal kind (new constant added to `signal_kind` in `src/signal.rs`):

```
signal_kind::LLM_INVOKE = "llm.invoke"
```

Request payload:  `{ "prompt": "{ns}/{name}", "input": "...", "context": { ... } }`
Response payload: `{ "output": "...", "model_used": "...", "tokens_used": N }`

---

## Rust API

### New file: `src/agent/prompt.rs`

```rust
/// A stored prompt template. Retrieve from KV or construct directly.
pub struct PromptTemplate { ... }   // as above

/// Errors from prompt skill operations.
pub enum PromptSkillError {
    NoProvider { ns: String, name: String },
    TemplateNotFound(String),
    RenderError { template: String, var: String },
    LlmError(String),
    Rpc(crate::agent::rpc::RpcError),
    Timeout,
}

/// Returned by register_prompt_skill. Dropping retracts the skill.
///
/// Internally holds:
/// - `_cap`: the `CapabilityHandle` from `advertise_capability` — dropping it tombstones
///   the `cap/` entry and stops the 30s capability heartbeat
/// - `_handler_cancel`: a `oneshot::Sender<()>` — dropping it signals the shared
///   `"llm.invoke"` dispatch loop to remove this skill's entry from the node's
///   `LlmSkillRegistry` DashMap; when the map becomes empty the loop exits
///
/// This mirrors the `CapabilityHandle { _retract: oneshot::Sender<()> }` pattern exactly.
/// `JoinHandle::drop` only detaches a task; it does NOT cancel it.
#[must_use = "dropping PromptSkillHandle retracts the skill immediately"]
pub struct PromptSkillHandle {
    _cap:            CapabilityHandle,
    _handler_cancel: tokio::sync::oneshot::Sender<()>,
}
```

### New file: `src/agent/llm.rs`

```rust
pub struct LlmResult { ... }       // as above
pub trait LlmBackend: Send + Sync { ... }   // as above

pub struct OpenAiBackend {
    pub base_url:  String,   // "https://api.anthropic.com/v1" or local Ollama etc.
    pub api_key:   String,
    pub model:     String,   // baked in at construction; not read from PromptTemplate
    pub client:    reqwest::Client,
}

pub struct EchoBackend;
```

### Methods on `GossipAgent`

```rust
impl GossipAgent {

    /// Publish a prompt template to the cluster KV and register an RPC handler
    /// that serves `llm.invoke` calls for this skill.
    ///
    /// The skill is discoverable immediately. Dropping the returned handle
    /// tombstones the KV entry and stops the handler.
    pub async fn register_prompt_skill(
        &self,
        ns:       &str,
        name:     &str,
        template: PromptTemplate,
        backend:  Arc<dyn LlmBackend>,
    ) -> Result<PromptSkillHandle, PromptSkillError>;

    /// Call a prompt skill by namespace/name. Resolves a provider via the
    /// capability ring, sends an RPC call, returns the LLM's response.
    ///
    /// The caller does not need to know whether the skill is LLM-backed or
    /// any other kind of Mycelium skill.
    pub async fn call_prompt_skill(
        &self,
        ns:      &str,
        name:    &str,
        input:   &str,
        context: HashMap<String, String>,
        timeout: Duration,
    ) -> Result<String, PromptSkillError>;

    /// Update a prompt template in-place across the cluster.
    /// Nodes currently serving this skill pick up the change on next invocation.
    /// Does not require the caller to hold the original PromptSkillHandle.
    pub async fn update_prompt(
        &self,
        ns:       &str,
        name:     &str,
        template: PromptTemplate,
    ) -> Result<(), PromptSkillError>;

    /// Retrieve the current prompt template for a skill from the local KV snapshot.
    /// Synchronous — reads from the in-memory KV state, same as `resolve()`.
    pub fn get_prompt(&self, ns: &str, name: &str) -> Option<PromptTemplate>;

    /// List all prompt skills currently visible in the cluster KV.
    pub fn list_prompts(&self) -> Vec<(String, String)>;  // Vec<(ns, name)>

    /// Tombstone a prompt template in the cluster KV.
    /// Does not affect any node currently serving this skill (their capability entries
    /// will evaporate naturally when they stop). Use when permanently retiring a skill.
    pub async fn delete_prompt(&self, ns: &str, name: &str) -> Result<(), PromptSkillError>;
}
```

### Node-level `LlmSkillRegistry`

Each node holds one shared registry as a field on `GossipAgent` — **not** in `TaskCtx`.
`TaskCtx` is the substrate's internal wiring; application-layer state must not contaminate it.
The registry stores **only the backend** — the template is always read fresh from KV:

```rust
// In GossipAgent (behind #[cfg(feature = "llm")]):
pub(crate) type LlmSkillRegistry = Arc<DashMap<String, Arc<dyn LlmBackend>>>;
//                                                  key = "{ns}/{name}"

pub struct GossipAgent {
    // ... existing fields ...
    #[cfg(feature = "llm")]
    pub(crate) llm_skills: LlmSkillRegistry,
}
```

The `PromptTemplate` is intentionally **not** cached in the registry. `update_prompt` writes
to KV; if the registry held a local copy there would be no mechanism to update it when gossip
delivers a remote write — the serving node would silently use a stale template until restart.
`ctx.kv_state.store.pin().get(key)` is a synchronous in-memory lookup: fast, zero I/O, and
always reflects the latest gossip-replicated state. The KV substrate is the source of truth.

The dispatch task receives `Arc::clone(&self.llm_skills)` and `Arc::clone(&self.task_ctx)`
via closure capture when spawned — it does not reach through `TaskCtx` for the registry.
This mirrors how MCP's tool table is owned by the MCP subsystem, not embedded in the substrate.

A single `"llm.invoke"` handler task is started lazily on the first call to
`register_prompt_skill` and runs until the node shuts down (or all skills are retracted).
Subsequent registrations only insert into the registry — no additional handler tasks.

Multiple concurrent `register_with_capacity("llm.invoke")` receivers would fan-out every
signal to every handler — each attempting to respond. One handler, one registry. Correct.

### Internal flow of `register_prompt_skill`

1. Write `prompts/{ns}/{name}` to KV with TTL = 604800s (one week, one-shot — no refresh
   task). Configuration, not a heartbeat; the `cap/` entry is the presence signal.
2. Call `advertise_capability(Capability::new(ns, name), Duration::from_secs(30))` — returns
   a `CapabilityHandle` stored in `PromptSkillHandle`. This is the presence heartbeat.
3. Insert `(ns/name → backend)` into `self.llm_skills`. If the registry was empty, also spawn
   the shared dispatch task. The dispatch loop is a **pure dispatcher** — it `tokio::spawn`s
   each invocation immediately and returns to listening. LLM calls (seconds) must not block
   new requests:
   ```
   let registry = Arc::clone(&self.llm_skills);
   let ctx      = Arc::clone(&self.task_ctx);
   loop {
     let req = rpc_rx("llm.invoke").recv().await       // fast — receive only
     let registry = Arc::clone(&registry);
     let ctx      = Arc::clone(&ctx);
     tokio::spawn(async move {                         // concurrent — one task per call
       let skill_id = parse ns/name from req.payload;
       let Some(backend) = registry.get(&skill_id) else {
         rpc_respond_ctx(&ctx, &req, error_bytes("skill_not_found")); return;
       };
       // Read template fresh from KV — always reflects latest update_prompt write
       let key = format!("prompts/{}", skill_id);
       let Some(bytes) = ctx.kv_state.store.pin().get(&*key).and_then(|e| e.data.clone()) else {
         rpc_respond_ctx(&ctx, &req, error_bytes("template_not_found")); return;
       };
       let template: PromptTemplate = serde_json::from_slice(&bytes)?;
       let rendered = render(&template, &req.payload.input, &context);
       let result   = backend.complete(rendered.system, rendered.user,
                                       template.max_tokens, template.temperature).await;
       rpc_respond_ctx(&ctx, &req, encode(result));
     });
   }
   ```
   A slow LLM call on behalf of one caller cannot block a different caller's invocation.
4. Create `oneshot::channel::<()>()`. Store `cancel_tx` in `PromptSkillHandle._handler_cancel`.
   When fired (on drop), the dispatch task removes the entry from the registry. When the
   registry is empty, the dispatch task exits via the shutdown watch.

### Internal flow of `call_prompt_skill`

1. Resolve via `resolve(&CapFilter::new(ns, name))` — standard capability ring lookup
2. If no providers: return `PromptSkillError::NoProvider`
3. Call `rpc_call_ctx(target, "llm.invoke", payload, timeout)`
4. Deserialise response, return `output` string

---

## HTTP gateway endpoints

All under the existing HTTP gateway in `src/agent/http.rs`:

### Prompt management

```
GET  /gateway/prompts
     → [{ "ns": "...", "name": "...", "max_tokens": N, "temperature": 0.7, "metadata": {...} }, ...]

GET  /gateway/prompts/{ns}/{name}
     → PromptTemplate JSON, or 404

PUT  /gateway/prompts/{ns}/{name}
     body: PromptTemplate JSON
     → { "ok": true }
     (calls update_prompt — gossips new template to cluster)

DELETE /gateway/prompts/{ns}/{name}
     → { "ok": true }
     (tombstones KV entry — skill becomes undiscoverable)
```

### Invocation

```
POST /gateway/llm/call
     body: { "ns": "...", "name": "...", "input": "...", "context": { ... }, "timeout_ms": 30000 }
     → { "output": "...", "model_used": "...", "tokens_used": N, "provider": "ip:port" }
     or { "error": "no_provider" | "timeout" | "llm_error", "detail": "..." }

POST /gateway/llm/stream
     body: { "ns": "...", "name": "...", "input": "...", "context": { ... } }
     → SSE stream (v1 — single buffered event):
         data: {"type":"done","output":"Hello world","tokens_used":12,"model_used":"claude-sonnet-4-6"}
```

**v1 behaviour**: the gateway calls the serving node via `rpc_call_ctx` (request/response),
waits for the full `LlmResult`, then emits a single `{"type":"done"}` event. No intermediate
`{"type":"token"}` events are produced — the RPC layer has no streaming primitive.

Per-token SSE events require a chunked RPC extension (v2 follow-up). The `LlmBackend::stream`
method and the `mpsc::Sender<String>` channel are defined now so backends can implement true
streaming; the gateway will use them once the RPC transport supports it.

---

## Python SDK

### New file: `mycelium-py/src/mycelium/prompt_skill.py`

```python
from dataclasses import dataclass, field
from typing import Iterator
import httpx

@dataclass
class PromptTemplate:
    system:        str
    user_template: str
    max_tokens:    int          = 4096
    temperature:   float        = 0.7
    metadata:      dict         = field(default_factory=dict)
    # Model routing: advertise capability attribute {"model": "..."} on the serving
    # node and filter via CapFilter — do not put the model name here.

class PromptSkillClient:
    """
    HTTP client for Mycelium prompt skill management and invocation.
    Wraps the /gateway/prompts and /gateway/llm endpoints.
    """

    def __init__(self, gateway_url: str, timeout: float = 30.0):
        self._base = gateway_url.rstrip("/")
        self._client = httpx.Client(timeout=timeout)

    # ── Management ──────────────────────────────────────────────────────────

    def register(self, ns: str, name: str, template: PromptTemplate) -> None:
        """Publish a prompt template to the cluster."""
        r = self._client.put(
            f"{self._base}/gateway/prompts/{ns}/{name}",
            json=vars(template)
        )
        r.raise_for_status()

    def update(self, ns: str, name: str, template: PromptTemplate) -> None:
        """Update an existing prompt template in-place."""
        self.register(ns, name, template)   # PUT is idempotent

    def get(self, ns: str, name: str) -> PromptTemplate:
        r = self._client.get(f"{self._base}/gateway/prompts/{ns}/{name}")
        r.raise_for_status()
        return PromptTemplate(**r.json())

    def list(self) -> list[dict]:
        r = self._client.get(f"{self._base}/gateway/prompts")
        r.raise_for_status()
        return r.json()

    def delete(self, ns: str, name: str) -> None:
        r = self._client.delete(f"{self._base}/gateway/prompts/{ns}/{name}")
        r.raise_for_status()

    # ── Invocation ───────────────────────────────────────────────────────────

    def call(
        self,
        ns:      str,
        name:    str,
        input:   str,
        context: dict = None,
        timeout: float = 30.0,
    ) -> str:
        """Call a prompt skill and return the LLM's response."""
        r = self._client.post(
            f"{self._base}/gateway/llm/call",
            json={
                "ns": ns, "name": name,
                "input": input,
                "context": context or {},
                "timeout_ms": int(timeout * 1000),
            },
            timeout=timeout + 2,
        )
        r.raise_for_status()
        body = r.json()
        if "error" in body:
            raise RuntimeError(f"PromptSkillError: {body['error']}: {body.get('detail','')}")
        return body["output"]

    def stream(
        self,
        ns:      str,
        name:    str,
        input:   str,
        context: dict = None,
    ) -> Iterator[str]:
        """Stream tokens from a prompt skill. Yields token strings."""
        import json
        with self._client.stream(
            "POST",
            f"{self._base}/gateway/llm/stream",
            json={"ns": ns, "name": name, "input": input, "context": context or {}},
        ) as r:
            r.raise_for_status()
            for line in r.iter_lines():
                if line.startswith("data: "):
                    event = json.loads(line[6:])
                    if event["type"] == "token":
                        yield event["token"]
                    elif event["type"] == "done":
                        return
```

Re-export in `__init__.py`:

```python
from .prompt_skill import PromptTemplate, PromptSkillClient
```

---

## TypeScript SDK

### New file: `mycelium-ts/src/prompt_skill.ts`

```typescript
export interface PromptTemplate {
    system:       string;
    userTemplate: string;
    maxTokens?:   number;     // default: 4096
    temperature?: number;     // default: 0.7
    metadata?:    Record<string, unknown>;
    // Model routing: advertise capability attribute {model: "..."} on the serving
    // node and filter via CapFilter — do not put the model name here.
}

export interface CallResult {
    output:     string;
    modelUsed:  string;   // which model the serving node actually used
    tokensUsed: number;
    provider:   string;
}

export class PromptSkillClient {
    constructor(private readonly gatewayUrl: string) {}

    // ── Management ─────────────────────────────────────────────────────────

    async register(ns: string, name: string, template: PromptTemplate): Promise<void> {
        const r = await fetch(`${this.gatewayUrl}/gateway/prompts/${ns}/${name}`, {
            method: "PUT",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(template),
        });
        if (!r.ok) throw new Error(`register failed: ${r.status}`);
    }

    async update(ns: string, name: string, template: PromptTemplate): Promise<void> {
        return this.register(ns, name, template);
    }

    async get(ns: string, name: string): Promise<PromptTemplate> {
        const r = await fetch(`${this.gatewayUrl}/gateway/prompts/${ns}/${name}`);
        if (!r.ok) throw new Error(`not found: ${ns}/${name}`);
        return r.json();
    }

    async list(): Promise<Array<{ ns: string; name: string; maxTokens: number; temperature: number; metadata?: Record<string, unknown> }>> {
        const r = await fetch(`${this.gatewayUrl}/gateway/prompts`);
        if (!r.ok) throw new Error("list failed");
        return r.json();
    }

    async delete(ns: string, name: string): Promise<void> {
        const r = await fetch(`${this.gatewayUrl}/gateway/prompts/${ns}/${name}`, {
            method: "DELETE",
        });
        if (!r.ok) throw new Error(`delete failed: ${r.status}`);
    }

    // ── Invocation ──────────────────────────────────────────────────────────

    async call(
        ns:      string,
        name:    string,
        input:   string,
        context: Record<string, string> = {},
        timeoutMs = 30_000,
    ): Promise<string> {
        const r = await fetch(`${this.gatewayUrl}/gateway/llm/call`, {
            method:  "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ ns, name, input, context, timeout_ms: timeoutMs }),
        });
        if (!r.ok) throw new Error(`call failed: ${r.status}`);
        const body = await r.json();
        if (body.error) throw new Error(`PromptSkillError: ${body.error}: ${body.detail ?? ""}`);
        return body.output as string;
    }

    async *stream(
        ns:      string,
        name:    string,
        input:   string,
        context: Record<string, string> = {},
    ): AsyncGenerator<string> {
        const r = await fetch(`${this.gatewayUrl}/gateway/llm/stream`, {
            method:  "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ ns, name, input, context }),
        });
        if (!r.ok) throw new Error(`stream failed: ${r.status}`);
        const reader = r.body!.getReader();
        const decoder = new TextDecoder();
        let buf = "";
        while (true) {
            const { value, done } = await reader.read();
            if (done) break;
            buf += decoder.decode(value, { stream: true });
            const lines = buf.split("\n");
            buf = lines.pop()!;
            for (const line of lines) {
                if (line.startsWith("data: ")) {
                    const event = JSON.parse(line.slice(6));
                    if (event.type === "token") yield event.token as string;
                    if (event.type === "done") return;
                }
            }
        }
    }
}
```

Re-export in `src/index.ts`:

```typescript
export { PromptSkillClient, PromptTemplate, CallResult } from "./prompt_skill";
```

---

## Unit tests

### `src/agent/prompt.rs`

| Test | Verifies |
|---|---|
| `render_basic` | `{{input}}` substitution in both system and user fields |
| `render_context_vars` | Caller-supplied `context` map variables substituted |
| `render_unknown_var` | Unknown `{{var}}` returns `RenderError` |
| `render_reserved_node_id` | `{{node_id}}` and `{{timestamp}}` are populated automatically |
| `template_roundtrip` | `PromptTemplate` serialises and deserialises cleanly |

### `src/agent/llm.rs`

| Test | Verifies |
|---|---|
| `echo_backend_complete` | `EchoBackend::complete()` returns `LlmResult { output: "echo: …", model_used: "echo", … }` |
| `echo_backend_no_model_param` | `LlmBackend::complete` takes no `model` argument; model is baked into `EchoBackend` |
| `register_and_call_roundtrip` | Register with `EchoBackend`; call from same node; `output` and `model_used` correct |
| `call_picks_live_template` | `update_prompt` writes new template; next call uses it (handler reads KV, not cached copy) |
| `no_provider_error` | `call_prompt_skill` on unregistered skill returns `NoProvider` |
| `handle_drop_retracts` | Drop `PromptSkillHandle`; subsequent call returns `NoProvider`; registry entry removed |
| `multi_skill_single_handler` | Register two skills on same node; each call routed to correct template; no cross-talk |
| `concurrent_invocations` | Two concurrent `call_prompt_skill` calls to same node both complete; dispatch loop does not block one on the other |
| `multi_node_routing` | Register on node A; call from node B; response correct |
| `context_vars_forwarded` | Context map at call site reaches template renderer |
| `registry_not_in_task_ctx` | Compile-time: `TaskCtx` has no `llm_skill` field; registry accessed only through `GossipAgent` |

---

## Integration scenario (Scenario 12)

Adds to the existing 11-scenario integration suite:

**Scenario 12 — Prompt Skill cross-node invocation:**

1. Start 3-node cluster
2. Node A registers `test/greet` with `EchoBackend::new()` and template `{ system: "...", user_template: "Hello, {{input}}!", max_tokens: 64, temperature: 0.0 }` (no `model` field)
3. Node B calls `call_prompt_skill("test", "greet", "world", {}, 5s)`
4. Assert `output = "echo: Hello, world!"` and `model_used = "echo"`
5. Node A calls `update_prompt("test", "greet", new_template)` with `user_template: "Hi {{input}}, from {{node_id}}"`
6. Node B calls again; assert response contains `"Hi world"` — confirms live template pickup
7. Concurrently issue 3 calls from nodes B and C; assert all 3 complete without blocking each other
8. Drop Node A's handle; subsequent call returns `NoProvider`; `prompts/test/greet` KV entry persists (TTL=604800s) but is undiscoverable (no live `cap/` entry)

---

## Non-goals for v1

- **Prompt versioning / history** — KV is LWW; old versions are not retained. A versioned
  prompt store is a higher-order concern (append-only log on Layer III consensus).
- **Multi-turn conversation state** — each `call_prompt_skill` is stateless. Conversation
  history is the caller's responsibility (can be held in Layer I KV if needed).
- **Fine-tuning / embedding integration** — out of scope; separate capability.
- **Cost tracking / rate limiting** — node-local concern; `LlmBackend` implementations
  can enforce their own limits. Cluster-wide rate limiting is a future cross-group consensus use case.
- **Prompt access control** — any node can update any `prompts/` KV entry in v1.
  mTLS (`tls` feature) restricts who can gossip at the transport level; per-prompt ACLs
  are a future concern.

---

## Resolved implementation decisions

1. **Feature flag — `llm = []`**
   `reqwest` is already an unconditional dependency (used by the MCP client role at line 75 of
   `Cargo.toml`). No new dependency is introduced by the `llm` feature. Use the same zero-dep
   pattern as `a2a = []`:
   ```toml
   llm = []
   ```
   All `LlmBackend`, `PromptTemplate`, HTTP endpoints, and Rust API methods are gated on
   `#[cfg(feature = "llm")]`. The feature is additive and does not affect non-llm builds.

2. **`async-trait` — add `async-trait = "0.1"`**
   Not currently present in `Cargo.toml`. Native `async fn` in traits (stable since Rust 1.75)
   is not object-safe, so `Arc<dyn LlmBackend>` would fail to compile without it.
   `async-trait` is a proc-macro with zero runtime overhead; add it as a non-optional dep:
   ```toml
   async-trait = "0.1"
   ```
   The codebase runs Rust 1.85.1; `async-trait` compiles cleanly on all supported versions.

3. **Streaming RPC — v1 (buffer at gateway)**
   `/gateway/llm/stream` calls the serving node via the existing request/response RPC
   (`rpc_call_ctx`), receives the full completion, then opens an SSE stream to the HTTP client
   and sends it as a single `{"type":"done"}` event. No changes to the RPC layer.
   True cross-node token streaming (v2) requires a chunked RPC extension and is a separate
   follow-up item.

---

## Files to create / modify (revised)

| File | Action | What changes |
|---|---|---|
| `src/signal.rs` | **Modify** | Add `signal_kind::LLM_INVOKE = "llm.invoke"` constant |
| `src/agent/prompt.rs` | **New** | `PromptTemplate`, `PromptSkillHandle`, `PromptSkillError`, template renderer |
| `src/agent/llm.rs` | **New** | `LlmResult`, `LlmBackend` trait (`#[async_trait]`), `LlmSkillRegistry` (`DashMap<String, Arc<dyn LlmBackend>>`), shared dispatch loop (reads template from KV on each call), `OpenAiBackend`, `EchoBackend` |
| `src/agent/mod.rs` | **Modify** | Add `llm_skills: LlmSkillRegistry` field to `GossipAgent` (behind `#[cfg(feature="llm")]`; **not** in `TaskCtx`); add `register_prompt_skill`, `call_prompt_skill`, `update_prompt`, `get_prompt`, `list_prompts`, `delete_prompt` |
| `src/agent/http.rs` | **Modify** | `/gateway/prompts/*` management endpoints; `/gateway/llm/call`; `/gateway/llm/stream` SSE (all `#[cfg(feature="llm")]`) |
| `src/lib.rs` | **Modify** | Export `PromptTemplate`, `PromptSkillError`, `LlmBackend`, `OpenAiBackend` (all `#[cfg(feature="llm")]`) |
| `Cargo.toml` | **Modify** | Add `async-trait = "0.1"` (non-optional); add `llm = []` feature |
| `mycelium-py/src/mycelium/prompt_skill.py` | **New** | `PromptTemplate`, `PromptSkillClient` |
| `mycelium-py/src/mycelium/__init__.py` | **Modify** | Re-export new types |
| `mycelium-ts/src/prompt_skill.ts` | **New** | `PromptTemplate`, `PromptSkillClient`, `CallResult` |
| `mycelium-ts/src/index.ts` | **Modify** | Re-export new types |

---

*Design authored 2026-05-28. Decisions resolved 2026-05-28. Implementation target: post-AAMAS 2027 paper submission.*
