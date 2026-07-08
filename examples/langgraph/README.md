# The LangGraph-on-Mycelium example ladder

## Concept

This is a runnable **rung ladder** that carries you from a one-line LangChain
starter up to the flagship *deploy/reheal* demo — each rung a single self-checking
Python file that runs against a Rust `reason_node` and prints `RUNG N OK`.

The through-line: **LangGraph runs *on* Mycelium.** The mesh is the durable
substrate — checkpoints gossip as KV index rows with payloads in the
content-addressed blob tier, inference is capability-routed with failover, and a
run's reasoning is a replayable, gossip-replicated trace. Swapping in
`MyceliumCheckpointSaver` is a one-line change to any LangGraph graph; the rest of
the ladder shows what that one line then buys you.

> **Anti-scatter note — two different layers, kept separate.**
> This ladder is LangGraph **on** Mycelium (Mycelium is the storage + routing
> substrate a graph runs on). The sibling example `examples/a2a_langchain/`
> ([README](../a2a_langchain/README.md)) is LangChain **calling** Mycelium — an
> LLM-driven agent discovering and invoking mesh skills as tools over A2A. Rung 0
> here is deliberately the rung *below* that: a skill wrapped as a plain Runnable,
> no agent, no tool-selection. If you want an agent picking mesh skills as tools,
> that is the a2a example, not this ladder.

---

## The rungs

Design + rationale: [`docs/plans/mycelium-reason-examples.md`](../../docs/plans/mycelium-reason-examples.md).

| Rung | File | Teaches | Run |
|---|---|---|---|
| 0 | `00_hello_skill.py` | a Mycelium skill **is** a LangChain `Runnable` (the minimal starter the a2a demo skips) | `MYCELIUM_TEST_PORT=8101 python examples/langgraph/00_hello_skill.py` |
| 1 | `01_typed.py` | typed (pydantic-validated) output through the mesh (`call_typed`) | `MYCELIUM_TEST_PORT=8101 python examples/langgraph/01_typed.py` |
| 2 | `02_durable_state.py` | LangGraph on Mycelium — graph state survives a fresh client (`MyceliumCheckpointSaver`) | `MYCELIUM_TEST_PORT=8101 python examples/langgraph/02_durable_state.py` |
| 3 | `03_cross_node.py` | cross-node resume — any node resumes any thread, by gossip (no kill) | `MYCELIUM_TEST_PORT=8101 MYCELIUM_TEST_PORT_B=8102 python examples/langgraph/03_cross_node.py` |
| 4 | `04_routed.py` | routed inference — load-aware, failover-capable (`ReasonClient.route`) | `MYCELIUM_TEST_PORT=8101 python examples/langgraph/04_routed.py` |
| 5 | `05_traces.py` | fleet-reasoning traces — replay/narrate why the graph reasoned (`ReasonClient.trace`) | `MYCELIUM_TEST_PORT=8101 python examples/langgraph/05_traces.py` |
| 6 | `06_deploy_reheal.py` | **flagship** — a graph's model dependency follows it across node death | `python examples/langgraph/06_deploy_reheal.py` (self-contained — see below) |

Every rung is **env-gated** and skips cleanly (prints a note, exits 0) when its
port env vars are unset — so running the whole directory without a mesh is a
harmless no-op. With a mesh up, each prints `✓` markers and a final `RUNG N OK`,
and exits non-zero on any assertion failure. All rungs are deterministic against
the `reason_node`'s EchoBackend (`echo: {input}`), so they run in CI.

Rung 1 needs the `typed` extra: `pip install 'mycelium-py[typed]'`.

---

## How to run rungs 0–5

Rungs 0–5 run against a shared reason mesh you start once. Rung 3 needs a second
node (`MYCELIUM_TEST_PORT_B`); the rest need only the first.

### 1 — Build the reason node

```bash
cargo build -p mycelium-reason --features llm,gateway --example reason_node
```

### 2 — Start a two-node mesh (B bootstrapped off A)

```bash
# terminal 1 — node A
BIND_PORT=7101 HTTP_PORT=8101 MODEL=fable-mini BLOB_DIR=/tmp/blobs-a \
  ./target/debug/examples/reason_node
# terminal 2 — node B
BIND_PORT=7102 HTTP_PORT=8102 BOOTSTRAP=127.0.0.1:7101 MODEL=fable-mini BLOB_DIR=/tmp/blobs-b \
  ./target/debug/examples/reason_node
```

Each node prints `reason node ready on <http_port>` once its gateway answers
`/health`. `MODEL=fable-mini` is the echo model every rung routes to.

### 3 — Install the Python side

```bash
pip install './mycelium-py[typed]' ./langgraph-checkpoint-mycelium langgraph httpx
```

### 4 — Run a rung

```bash
MYCELIUM_TEST_PORT=8101 MYCELIUM_TEST_PORT_B=8102 python examples/langgraph/03_cross_node.py
```

---

## Rung 6 is different — it manages its own nodes

The flagship (`06_deploy_reheal.py`) **kills a node mid-run**, so it does not use
the shared mesh above: it boots and tears down its own two-node mesh of
`reheal_node`s (disjoint ports) `model_deploy`-style. Just build the node and run
the script:

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
