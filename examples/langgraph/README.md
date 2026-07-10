# The LangGraph-on-Mycelium example ladder

## Objective

A seven-rung ladder that carries **LangGraph running *on* Mycelium** from a
local checkpointer up to a cross-node deploy/reheal flagship — each rung adds one
capability. Start with a Mycelium skill as a plain LangChain `Runnable` (rung 0),
add typed output (1), make graph state durable with `MyceliumCheckpointSaver` (2),
resume any thread on any node by gossip (3), route inference with failover (4),
replay the reasoning trace (5), and finally have a graph's model dependency follow
it across node death (6). The mesh is the durable substrate: checkpoints gossip as
KV index rows with payloads in the content-addressed blob tier, inference is
capability-routed with failover, and a run's reasoning is a replayable,
gossip-replicated trace. Concept: [guide chapter 15 · Reasoning &
LangGraph](../../docs/guide/15-reasoning-and-langgraph.md). Mechanism: the
checkpointer [`saver.py`](../../langgraph-checkpoint-mycelium/src/langgraph_checkpoint_mycelium/saver.py),
the router [`route.rs`](../../mycelium-reason/src/route.rs), the trace
[`trace.rs`](../../mycelium-reason/src/trace.rs). Design + rationale:
[`docs/plans/mycelium-reason-examples.md`](../../docs/plans/mycelium-reason-examples.md).

## How to run

See [shared setup](../README.md#shared-setup) for the Rust toolchain and the
Python tier. Rungs 0–5 run against a shared reason mesh you start once; rung 6
manages its own nodes (see its block).

**1 — Build the reason node**

```bash
cargo build -p mycelium-reason --features llm,gateway --example reason_node
```

**2 — Start a two-node mesh (B bootstrapped off A)**

```bash
# terminal 1 — node A
BIND_PORT=7101 HTTP_PORT=8101 MODEL=fable-mini BLOB_DIR=/tmp/blobs-a \
  ./target/debug/examples/reason_node
# terminal 2 — node B
BIND_PORT=7102 HTTP_PORT=8102 BOOTSTRAP=127.0.0.1:7101 MODEL=fable-mini BLOB_DIR=/tmp/blobs-b \
  ./target/debug/examples/reason_node
```

Each node prints `reason node ready on <http_port>` once its gateway answers
`/health`. `MODEL=fable-mini` is the echo model every rung routes to. Rung 3 needs
the second node (`MYCELIUM_TEST_PORT_B`); the rest need only the first.

**3 — Install the Python side**

```bash
pip install './mycelium-py[typed]' ./langgraph-checkpoint-mycelium langgraph httpx
```

**4 — Run a rung**

```bash
MYCELIUM_TEST_PORT=8101 MYCELIUM_TEST_PORT_B=8102 python examples/langgraph/03_cross_node.py
```

Rung 6 does **not** use the shared mesh — it boots and tears down its own nodes
(see its block below).

## What it demonstrates — the rungs

### 0 — `00_hello_skill.py`

A Mycelium skill **is** a LangChain `Runnable` — the minimal starter the a2a demo
skips (a skill wrapped as a plain Runnable, no agent, no tool-selection).

```bash
MYCELIUM_TEST_PORT=8101 python examples/langgraph/00_hello_skill.py
```

### 1 — `01_typed.py`

Typed (pydantic-validated) output through the mesh via `call_typed`. Needs the
`typed` extra: `pip install 'mycelium-py[typed]'`.

```bash
MYCELIUM_TEST_PORT=8101 python examples/langgraph/01_typed.py
```

### 2 — `02_durable_state.py`

LangGraph on Mycelium — graph state survives a fresh client. Swapping in
[`MyceliumCheckpointSaver`](../../langgraph-checkpoint-mycelium/src/langgraph_checkpoint_mycelium/saver.py)
is a one-line change to any LangGraph graph.

```bash
MYCELIUM_TEST_PORT=8101 python examples/langgraph/02_durable_state.py
```

### 3 — `03_cross_node.py`

Cross-node resume — any node resumes any thread, by gossip (no kill). Needs the
second node ([`resume.rs`](../../mycelium-reason/src/resume.rs)).

```bash
MYCELIUM_TEST_PORT=8101 MYCELIUM_TEST_PORT_B=8102 python examples/langgraph/03_cross_node.py
```

### 4 — `04_routed.py`

Routed inference — load-aware, failover-capable via `ReasonClient.route`
([`route.rs`](../../mycelium-reason/src/route.rs)).

```bash
MYCELIUM_TEST_PORT=8101 python examples/langgraph/04_routed.py
```

### 5 — `05_traces.py`

Fleet-reasoning traces — replay/narrate why the graph reasoned via
`ReasonClient.trace` ([`trace.rs`](../../mycelium-reason/src/trace.rs)).

```bash
MYCELIUM_TEST_PORT=8101 python examples/langgraph/05_traces.py
```

### 6 — `06_deploy_reheal.py` (flagship — manages its own nodes)

A graph's model dependency follows it across node death. The flagship **kills a
node mid-run**, so it does not use the shared mesh above: it boots and tears down
its own two-node mesh of `reheal_node`s (disjoint ports) `model_deploy`-style.
Build the node and run the script:

```bash
cargo build -p mycelium-reason --features llm,gateway --example reheal_node
python examples/langgraph/06_deploy_reheal.py
```

It prints `FLAGSHIP OK` and kills both nodes on every exit path. The shipped
variant is the **echo-CI** flagship — deterministic, wasmtime-free, proving the
require_model → mesh blob fetch → `serve_model` bridge → routed resume seam.

**Forthcoming (later PRs):** the rung-6 **Ollama-manual** variant (real GGUF
weights via `model_deploy`'s `BlobRuntime`, excluded from CI like `model_deploy`),
and the teaching write-up in guide chapter 15
(`docs/guide/15-reasoning-and-langgraph.md`).

## Dev notes

> **Anti-scatter note — two different layers, kept separate.**
> This ladder is LangGraph **on** Mycelium (Mycelium is the storage + routing
> substrate a graph runs on). The sibling example `examples/a2a_langchain/`
> ([README](../a2a_langchain/README.md)) is LangChain **calling** Mycelium — an
> LLM-driven agent discovering and invoking mesh skills as tools over A2A. Rung 0
> here is deliberately the rung *below* that: a skill wrapped as a plain Runnable,
> no agent, no tool-selection. If you want an agent picking mesh skills as tools,
> that is the a2a example, not this ladder.

Every rung is **env-gated** and skips cleanly (prints a note, exits 0) when its
port env vars are unset — so running the whole directory without a mesh is a
harmless no-op. With a mesh up, each prints `✓` markers and a final `RUNG N OK`,
and exits non-zero on any assertion failure. All rungs are deterministic against
the `reason_node`'s EchoBackend (`echo: {input}`), so they run in CI.
