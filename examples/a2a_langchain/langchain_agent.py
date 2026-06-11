"""
Mycelium x LangChain  --  A2A auto-discovery demo
==================================================
A LangChain ReAct agent that discovers every skill advertised by a running
Mycelium cluster and uses them as tools.  No hardcoding of skill names:
the agent reads /.well-known/agent.json, wraps each skill as a LangChain
Tool, then answers a query that requires using them.

Quick start
-----------
1. Build SkillRunner with A2A support:
       cargo build --bin skillrunner --features a2a

2. Start the 3-skill community cluster:
       cd examples/community && ./start.sh

3. Install Python dependencies:
       pip install -r examples/a2a_langchain/requirements.txt

4. Run (Ollama, no API key needed):
       python examples/a2a_langchain/langchain_agent.py

   Or with OpenAI:
       OPENAI_API_KEY=sk-... python examples/a2a_langchain/langchain_agent.py

Environment variables
---------------------
MYCELIUM_URL    Base URL of any Mycelium node with http_port set.
                Default: http://localhost:9050
QUERY           Question to ask the agent.
                Default: article about gossip protocols
OPENAI_API_KEY  If set, uses gpt-4o-mini.  Otherwise uses Ollama llama3.2.
OLLAMA_MODEL    Ollama model name (default: llama3.2).
"""
from __future__ import annotations

import os
import sys

import json

from langchain.agents import create_agent
from langchain_core.tools import StructuredTool
from pydantic import BaseModel, ConfigDict, Field, create_model

from mycelium.a2a import A2aClient

MYCELIUM_URL = os.environ.get("MYCELIUM_URL", "http://localhost:9050")
QUERY = os.environ.get(
    "QUERY",
    (
        "Write a short technical article about how gossip protocols achieve "
        "eventual consistency in distributed systems."
    ),
)


def _build_llm():
    if key := os.environ.get("OPENAI_API_KEY"):
        from langchain_openai import ChatOpenAI
        return ChatOpenAI(model="gpt-4o-mini", temperature=0, api_key=key)
    from langchain_ollama import ChatOllama
    model = os.environ.get("OLLAMA_MODEL", "llama3.2")
    return ChatOllama(model=model, temperature=0)


class SkillArgs(BaseModel):
    """Fallback when a skill gossips no input schema: accept whatever fields
    the model supplies and forward them as JSON."""
    model_config = ConfigDict(extra="allow")


_TYPE_MAP = {"string": str, "integer": int, "number": float,
             "boolean": bool, "array": list, "object": dict}


def _args_model(tool_name: str, schema: dict | None):
    """Build a pydantic args model from the card's ``inputSchema`` extension
    so the LLM sees real typed fields (topic, max_points, …) instead of a
    free-form object."""
    props = (schema or {}).get("properties", {}) or {}
    if not props:
        return SkillArgs
    required = set((schema or {}).get("required", []))
    fields = {}
    for pname, spec in props.items():
        py = _TYPE_MAP.get(spec.get("type"), str)
        desc = spec.get("description")
        field = Field(description=desc) if pname in required \
            else Field(default=None, description=desc)
        fields[pname] = (py if pname in required else py | None, field)
    return create_model(f"{tool_name}_Args", **fields)


def _discover_tools(client: A2aClient) -> list[StructuredTool]:
    """Fetch /.well-known/agent.json and return one LangChain tool per skill."""
    card = client.fetch_card()
    print(f"\n  Connected to: {card.get('name', 'Mycelium cluster')}")
    skills = card.get("skills", [])
    print(f"  Discovered {len(skills)} skill(s):")
    tools: list[StructuredTool] = []
    for skill in skills:
        sid  = skill["id"]          # "ns/name"
        desc = skill.get("description") or f"Mycelium mesh skill: {sid}"
        print(f"    · {sid:<30}  {desc[:80]}")

        def _fn(_sid: str = sid, **kwargs) -> str:
            # Skills take a JSON object; the model's structured tool-call
            # arguments ARE that object (drop unset optionals).
            payload = {k: v for k, v in kwargs.items() if v is not None}
            return client.send(_sid, json.dumps(payload), timeout_secs=120.0)

        tools.append(StructuredTool(
            name=sid.replace("/", "_"),
            description=desc,
            func=_fn,
            args_schema=_args_model(sid.replace("/", "_"), skill.get("inputSchema")),
        ))
    print()
    return tools


def main() -> None:
    print(f"Connecting to Mycelium at {MYCELIUM_URL} ...")
    client = A2aClient(MYCELIUM_URL)

    try:
        tools = _discover_tools(client)
    except Exception as exc:
        print(f"\n  Error: {exc}")
        print("  Is the cluster running?  cd examples/community && ./start.sh")
        sys.exit(1)

    if not tools:
        print("  No skills found on the cluster.")
        sys.exit(1)

    llm   = _build_llm()
    # LangChain >= 1.0: create_agent builds a tool-calling agent loop
    # (initialize_agent/AgentType were removed with the legacy AgentExecutor).
    agent = create_agent(
        llm,
        tools,
        system_prompt=(
            "You are a helpful assistant. Use the available Mycelium mesh "
            "skills (tools) to answer; the llm_orchestrator tool runs the "
            "full researcher → writer pipeline and returns an article."
        ),
    )

    print(f"Query: {QUERY}\n")
    result = agent.invoke(
        {"messages": [{"role": "user", "content": QUERY}]},
        config={"recursion_limit": 12},
    )
    for msg in result["messages"]:
        kind = type(msg).__name__
        text = getattr(msg, "content", "")
        if kind == "ToolMessage":
            print(f"  [tool result] {str(text)[:120]}...")
        elif kind == "AIMessage" and getattr(msg, "tool_calls", None):
            for tc in msg.tool_calls:
                print(f"  [tool call]   {tc['name']}({str(tc['args'])[:100]})")
    print(f"\n{'=' * 60}")
    print(result["messages"][-1].content)


if __name__ == "__main__":
    main()
