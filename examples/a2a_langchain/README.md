# Mycelium × LangChain / AutoGen — A2A auto-discovery

Two agents (one LangChain, one AutoGen) that connect to a running Mycelium
cluster and automatically discover its skills as callable tools.

Neither agent has any hardcoded knowledge of what skills exist.  They read
`/.well-known/agent.json`, wrap each skill as a native tool, then answer a
query — routing calls through the mesh without knowing Mycelium is involved.

```
LangChain / AutoGen agent
        │
        │  GET /.well-known/agent.json   →  discover: llm/orchestrator,
        │                                            llm/researcher,
        │                                            llm/writer
        │
        │  POST /a2a  tasks/send  →  llm/orchestrator
                                          │
                                          │  rpc_call (mesh)
                                          ├──▶ llm/researcher
                                          └──▶ llm/writer
```

The orchestrator internally calls researcher and writer via the Mycelium mesh.
The Python agent sees exactly one tool call and gets back a finished article.

---

## Prerequisites

| Requirement | Notes |
|---|---|
| Rust toolchain | `cargo build --bin skillrunner --features a2a` |
| Python ≥ 3.11 | |
| Ollama (recommended) | `ollama pull llama3.2` — free, no API key |
| OpenAI key (optional) | Set `OPENAI_API_KEY` to use `gpt-4o-mini` instead |

---

## Quick start

### 1 — Build SkillRunner with A2A support

```bash
cargo build --bin skillrunner --features a2a
```

### 2 — Start the 3-skill community cluster

```bash
cd examples/community
./start.sh
```

Three SkillRunner processes start on ports 7950–7953.  The orchestrator
exposes an HTTP gateway on port **9050** with A2A routes enabled.

### 3 — Install Python dependencies

```bash
pip install -r examples/a2a_langchain/requirements.txt
```

### 4 — Run the LangChain agent

```bash
# Ollama (default)
python examples/a2a_langchain/langchain_agent.py

# OpenAI
OPENAI_API_KEY=sk-... python examples/a2a_langchain/langchain_agent.py

# Custom query
QUERY="Explain Byzantine fault tolerance in one paragraph" \
    python examples/a2a_langchain/langchain_agent.py
```

### 5 — Run the AutoGen agent

```bash
python examples/a2a_langchain/autogen_agent.py
```

---

## What you'll see

```
Connecting to Mycelium at http://localhost:9050 ...

  Connected to: Mycelium cluster
  Discovered 3 skill(s):
    · llm/orchestrator    Coordinates research and writing to produce articles on any topic
    · llm/researcher      Researches a topic and returns structured findings
    · llm/writer          Writes a polished article from research findings

Query: Write a short technical article about how gossip protocols achieve eventual consistency.

> Entering new AgentExecutor chain...
  Thought: I should use llm_orchestrator to write the article.
  Action: llm_orchestrator
  Action Input: {"topic": "gossip protocols and eventual consistency"}
  Observation: {"title": "...", "article": "...", "tldr": "..."}
  Thought: I have the article.
  Final Answer: ...

============================================================
Gossip Protocols and Eventual Consistency
...
```

---

## How it works

`A2aClient.fetch_card()` performs a single `GET /.well-known/agent.json`
against the Mycelium node.  The response is the A2A `AgentCard` — a JSON
document listing every capability currently advertised on the mesh.

Each skill becomes a Python callable wrapped as a framework tool:

```python
def tool_fn(message: str) -> str:
    return client.send(skill_id, message, timeout_secs=120.0)
```

`client.send()` posts `tasks/send` JSON-RPC to `/a2a`.  Mycelium resolves
the skill to a live node, calls it via nonce RPC, and returns the result.
If the cluster has multiple nodes advertising the same skill, Mycelium
picks one automatically — the agent never sees the difference.

Because discovery is live, adding or removing SkillRunner nodes updates
what the agent sees on the next `fetch_card()` call with no code changes.

---

## Customising the cluster

Point either agent at any Mycelium node with `http_port` set and the `a2a`
feature compiled in:

```bash
MYCELIUM_URL=http://my-node:8300 python langchain_agent.py
```

To add a skill, write a `.skill.toml` manifest and run:

```bash
./target/debug/skillrunner --skill my_skill.toml
```

It joins the mesh, advertises its capability, and appears in
`/.well-known/agent.json` within one gossip interval (~10 s).
