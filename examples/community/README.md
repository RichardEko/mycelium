# 3-Skill Community Example

A minimal community of three locally-running LLM agents that collaborate to
research a topic and write an article about it.

```
Caller
  └─ rpc_call → llm/orchestrator  (port 7950)
       ├─ tool_call → llm/researcher  (port 7952)  ← gathers facts
       └─ tool_call → llm/writer      (port 7953)  ← writes the article
```

All three skills use `llama3.2` via Ollama. The orchestrator's `tools` list
names the other two skills; SkillRunner resolves them from the mesh at call
time and dispatches tool calls back through the gossip layer.

## Prerequisites

```sh
cargo build --bin skillrunner
ollama pull llama3.2
```

## Quick start

```sh
cd examples/community

./start.sh                                    # starts all three nodes
sleep 3                                       # wait for gossip to converge

./invoke.sh "gossip protocols"                # default: technical style
./invoke.sh "Rust ownership" casual           # casual tone
./invoke.sh "large language models" executive 8  # executive tone, 8 findings

./stop.sh                                     # graceful shutdown
```

## What each skill does

| Skill | Port | Role |
|-------|------|------|
| `orchestrator` | 7950 | Coordinates the other two; exposes HTTP gateway on 9050 |
| `researcher` | 7952 | Produces structured findings from LLM reasoning |
| `writer` | 7953 | Turns findings into a polished article |

## Scaling

Run a second researcher for automatic load balancing:

```sh
# In examples/community/researcher.skill.toml, change bind_port to 7954
cp researcher.skill.toml researcher2.skill.toml
sed -i '' 's/bind_port = 7952/bind_port = 7954/' researcher2.skill.toml
../../target/debug/skillrunner --skill researcher2.skill.toml &
```

The orchestrator's `resolve("llm", "researcher")` now has two providers and
will use whichever responds first. Capability advertisement gossips the new
node's availability within one refresh interval (60 s by default; sooner in
practice as gossip spreads on first connection).

## Audit trail

After an invocation, every node in the cluster has the audit record:

```sh
# From the invoke_skill example or any Rust code connected to the mesh:
agent.scan_prefix("audit/")
# → [{skill_ns: "llm", skill_name: "orchestrator", duration_ms: 4200, ...}, ...]
```

## Customising

- Change `model` in any `.skill.toml` to use a different Ollama model
- Set `[skill.llm.endpoint]` to an OpenAI or Anthropic-compatible URL
- Add `[skill.otel]` and build with `--features otel` for Jaeger/Grafana tracing
- Add `[capability.policy].authorized_callers = ["orchestrator"]` to the
  researcher and writer to restrict them to orchestrator-only access
