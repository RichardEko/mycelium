# MCP Tool Discovery — Interactive Chat Demo

## Objective

Live MCP tool discovery: the LLM finds its tools by scanning the gossip KV store
at the start of every planning cycle — not from a config file. Start a new tool
node mid-session and the LLM sees it on the next message. No restart, no config
change, no coordinator. This is what "the mesh is the registry" means in practice.

Seven roles run in a single binary (`three_node_demo`), selected by the
`MYCELIUM_ROLE` environment variable: four tool providers, one LLM planner, one
management dashboard, one claims verifier.

```mermaid
graph TD
    TA["tool-a :57000<br/>weather(city)<br/>web_fetch(url)"]
    TB["tool-b :57001<br/>calculate(expr)<br/>wiki(topic)"]
    TSF["tool-sf :57004<br/>sf_lookup(query)<br/>(joins live)"]
    TBK["tool-book :57005<br/>book_plot(query)<br/>(joins live)"]
    VER["verifier :57006<br/>verify_answer<br/>(pipeline guard)"]
    LLM["llm :57002<br/>chat UI :8080<br/>plans with llama3.2"]
    MGT["mgmt :57003<br/>dashboard :8090"]

    TA -->|gossip| LLM
    TB -->|gossip| LLM
    TSF -->|gossip| LLM
    TBK -->|gossip| LLM
    VER -->|gossip| LLM
    LLM -->|rpc| TA
    LLM -->|rpc| TB
    LLM -->|rpc| TSF
    LLM -->|rpc| TBK
    LLM -->|verify| VER
    LLM -->|gossip| MGT
```

## How to run

See [shared setup](../README.md#shared-setup) for the Rust toolchain and Ollama.
This example also uses a second, optional model for the verifier:

```bash
cargo build --example three_node_demo
ollama pull llama3.1:8b  # verifier model (optional; falls back to llama3.2)
```

Any OpenAI-compatible endpoint works — set `OLLAMA_BASE_URL` and `OLLAMA_MODEL`
to use a different backend.

**Automated demo** — starts the base cluster, prints the tool list, then joins
two tools live:

```bash
cd examples/chat
./demo.sh
```

`demo.sh`:
1. Starts the base cluster (tool-a, tool-b, llm, mgmt, verifier)
2. Waits for the LLM node to be ready
3. Prints the initial tool list
4. Starts tool-sf live — LLM discovers it without restart
5. Starts tool-book live — same
6. Prints the final tool list

Open http://localhost:8080 while it runs to try the tools interactively.

**Manual cluster:**

```bash
cd examples/chat
./start.sh
# Open: http://localhost:8080  (chat UI)
# Open: http://localhost:8090  (mesh dashboard)
./stop.sh
```

## What it demonstrates

The demo inverts the usual static-MCP model: the planner queries the mesh at
call time instead of holding a fixed tool list. See the guide chapter
[`docs/guide/06-tool-discovery.md`](../../docs/guide/06-tool-discovery.md) for
the concept. Tools register under `tools/{name}/{node_id}` in the KV store; the
mechanism is `discover_tools()` — a `scan_prefix("tools/")` local read, no
network hop — in the example source
[`examples/three_node_demo.rs`](../three_node_demo.rs) (see `fn discover_tools`
and `fn register`).

Try each tool from the chat UI:

```
"what's the weather in Tokyo?"              → tool-a: weather
"what is 330 times 1024?"                   → tool-b: calculate
"how does Dan Simmons fit into 1990s SF?"   → tool-sf: sf_lookup
"what happens in Hyperion?"                 → tool-book: book_plot
"fetch https://example.com"                 → tool-a: web_fetch
```

Watch the live-join: `sf_lookup` and `book_plot` only answer after `demo.sh`
starts their nodes — the LLM picks them up on the next planning cycle, with no
restart. Check what tools the LLM currently sees:

```bash
curl -s http://localhost:8080/mesh | python3 -m json.tool
```

The **verifier** is a claims-checking pipeline guard (Microsoft Research-style).
After the LLM produces a draft answer from tool results, the verifier decomposes
it into atomic factual claims, checks each against the tool evidence, and removes
any not grounded in the results. It is filtered from the LLM's visible tool list
— the LLM cannot call it directly.

For the skills equivalent (LLM agents calling other LLM agents), see
[`examples/community/`](../community/README.md) and
[`docs/guide/05-skills.md`](../../docs/guide/05-skills.md).

## Dev notes

**Adding a new tool node live.** Add a handler function in
[`examples/three_node_demo.rs`](../three_node_demo.rs), register it with the
`register()` helper, and add a new role branch in `main()`. Start the node; the
LLM discovers it on the next planning cycle.

The `register()` helper writes `tools/{name}/{node_id}` to the KV store and
returns a `CapabilityReg`. Dropping the handle deregisters the tool:

```rust
let _handle = register(
    &agent, "my_lookup",
    "Look up X when the user asks about X",
    json!({"type":"object","properties":{"query":{"type":"string"}},"required":["query"]}),
    Arc::new(|args| Box::pin(my_lookup_fn(args))),
);
```

**Verifier tuning.** The verifier model is set via `VERIFIER_MODEL` (default
`llama3.1:8b`). Larger models produce more accurate claim decomposition.
To disable verification, start without the verifier role in the peer list or
unset `VERIFIER_MODEL`.

**Planning cycle.** The multi-turn loop in `planning_cycle()` runs until
the LLM emits a final answer (finish reason `stop` with no pending tool
calls). Each tool call is dispatched via `rpc_call` to the node that
registered it. The SSE stream at `GET /stream` emits `Thinking`, `ToolCall`,
`ToolResult`, and `Assistant` events so the browser UI shows intermediate
steps.

**Model and endpoint.** Set before running `start.sh`:

```bash
OLLAMA_BASE_URL=http://localhost:11434/v1 \
OLLAMA_MODEL=llama3.2 \
VERIFIER_MODEL=llama3.1:8b \
  ./start.sh
```
