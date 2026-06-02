# Skills — LLM Agents as Mesh Citizens

## What is a Skill?

A **Skill** is an LLM agent that lives permanently in the Mycelium mesh with
its own identity, capability declaration, and prompt. It is not a function you
call from a single host — it is a node. Any other node on the mesh (Rust,
Python, another skillrunner, or a LangChain agent) can discover it by capability
and invoke it, without knowing its address in advance.

The critical difference from an MCP tool:

| | MCP Tool | Skill |
|---|---|---|
| What it is | A function registered on a node | An LLM agent *node* |
| Written in | Any language | TOML manifest — no code |
| Calls an LLM | Optionally | Always |
| Can call other skills | No | Yes — composition |
| Discovered via | `tools/` KV prefix | Capability system (ns/name) |
| Use when | API call, calculation, lookup | Reasoning step, agent role |

**Mental model:** MCP tool = "a function in the mesh." Skill = "an LLM agent
in the mesh." Skills can call other skills. That is the design.

---

## This Example: Article Production Pipeline

Three skills collaborate to research a topic and write a polished article.
No node knows the address of any other — each resolves its collaborators
by capability name through the gossip layer at call time.

```
You
 └─ invoke → llm/orchestrator  (port 7950, HTTP gateway 9050)
                ├─ tool_call → llm/researcher  (port 7952)
                └─ tool_call → llm/writer      (port 7953)
```

The orchestrator's prompt says `tools = ["llm/researcher", "llm/writer"]`.
At inference time, SkillRunner resolves those names against live capability
advertisements in the KV store and dispatches the sub-invocations through
the mesh. The orchestrator never hardcodes `127.0.0.1:7952` — it asks the
mesh "who provides `llm/researcher`?" and calls whoever answers.

This means: start a second researcher and the orchestrator automatically
load-balances across both, with no configuration change.

---

## Prerequisites

```sh
cargo build --bin skillrunner
ollama pull llama3.2
```

Any OpenAI-compatible endpoint works — swap `endpoint` and `model` in the
`.skill.toml` files to use OpenAI, Anthropic, or any other provider.

---

## Quick Start

```sh
cd examples/community

./start.sh           # starts orchestrator (7950), researcher (7952), writer (7953)
sleep 3              # wait for gossip to converge across all three nodes

./invoke.sh "gossip protocols"                    # default: technical style
./invoke.sh "Rust ownership" casual               # casual tone
./invoke.sh "large language models" executive 8  # executive tone, 8 findings

./stop.sh            # graceful shutdown
```

---

## Watching It Live

Open a second terminal while the cluster is running:

```sh
# Follow all three nodes at once
tail -f examples/community/logs/orchestrator.log \
        examples/community/logs/researcher.log \
        examples/community/logs/writer.log
```

You will see the full causal chain as it happens:

```
[orchestrator] Received invoke: topic="gossip protocols"
[orchestrator] → tool_call: llm/researcher  {"topic": "gossip protocols", "max_points": 5}
[researcher]   Received invoke: topic="gossip protocols"
[researcher]   LLM generating findings...
[researcher]   → reply: {"findings": ["Gossip spreads state O(log N)...", ...], "summary": "..."}
[orchestrator] ← tool_result: llm/researcher  (4 findings)
[orchestrator] → tool_call: llm/writer  {"topic": "...", "findings": [...]}
[writer]       Received invoke
[writer]       LLM generating article...
[writer]       → reply: {"title": "...", "article": "...", "tldr": "..."}
[orchestrator] ← tool_result: llm/writer
[orchestrator] → final reply to caller
```

Each arrow is a real RPC call through the Mycelium gossip layer. The
orchestrator does not drive these sequentially by polling — the mesh handles
routing, and the audit trail in the KV store captures the full causal chain.

---

## What Each Skill Does

| Skill | Port | Role | Prompt focus |
|-------|------|------|--------------|
| `orchestrator` | 7950 | Coordinates the other two | Coordination only — delegates all LLM work |
| `researcher` | 7952 | Produces structured findings | Extract N key facts, return JSON |
| `writer` | 7953 | Turns findings into an article | Title, article body, TL;DR |

The orchestrator's `max_tokens = 512` — it only coordinates. The researcher
and writer do the substantive LLM work. This is intentional: keep coordination
logic cheap; put reasoning in specialist skills.

---

## How Skill Discovery Works

When the orchestrator skill calls `llm/researcher`, SkillRunner:

1. Scans the gossip KV store for keys matching `cap/llm/researcher/*`
2. Picks a live provider (TTL-checked, locality-aware if configured)
3. Dispatches the RPC to that node's gossip port
4. Waits for the JSON reply

No service registry. No hardcoded address. No coordinator. The mesh *is*
the registry — capability advertisements gossip to every node within seconds
of a new skillrunner starting.

---

## Scaling — Add a Second Researcher

```sh
cp researcher.skill.toml researcher2.skill.toml
sed -i '' 's/bind_port = 7952/bind_port = 7954/' researcher2.skill.toml
../../target/debug/skillrunner --skill researcher2.skill.toml &
```

Within one gossip refresh interval (~5 s in practice), the orchestrator sees
two providers for `llm/researcher`. Subsequent invocations are distributed
across both. Remove one and the other takes over automatically — no
reconfiguration required anywhere.

---

## Audit Trail

After any invocation, every node in the cluster has the full audit record:

```sh
# From any Rust node connected to the mesh:
agent.scan_prefix("audit/")
# → [{skill_ns: "llm", skill_name: "orchestrator", duration_ms: 4200, ...}, ...]
```

The audit entries are signed with the node's Ed25519 key, causally ordered
by HLC timestamp, and replicated via gossip — not stored on a single server.

---

## Restricting Access

To allow only the orchestrator to call researcher and writer, add to each:

```toml
[capability.policy]
authorized_callers = ["orchestrator"]
```

SkillRunner enforces this before invoking the LLM — unauthorised callers
receive an error without consuming any LLM quota.

---

## Customising

- Change `model` in any `.skill.toml` to use a different Ollama model
- Set `[skill.llm.endpoint]` to an OpenAI or Anthropic-compatible URL
- Add `[skill.otel]` and build with `--features otel` for Jaeger/Grafana tracing
- Add more skills to the `tools` array in the orchestrator to extend the pipeline

---

## Next Steps

- **A2A integration** — [`examples/a2a_langchain/`](../a2a_langchain/): LangChain
  and AutoGen agents auto-discover these skills via `/.well-known/agent.json` and
  use them as native tools, with no hardcoded skill names.
- **Prompt Skills** — embed an LLM skill directly inside a Rust `GossipAgent`
  using `register_prompt_skill`. See the `--features llm` section in the main README.
- **Skill manifest reference** — [`docs/skillrunner.html`](../../docs/skillrunner.html):
  full TOML schema, OTEL integration, concurrency controls, audit trail format.
