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

from langchain.agents import AgentType, initialize_agent
from langchain.tools import Tool

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
    from langchain_community.llms import Ollama
    model = os.environ.get("OLLAMA_MODEL", "llama3.2")
    return Ollama(model=model, temperature=0)


def _discover_tools(client: A2aClient) -> list[Tool]:
    """Fetch /.well-known/agent.json and return one LangChain Tool per skill."""
    card = client.fetch_card()
    print(f"\n  Connected to: {card.get('name', 'Mycelium cluster')}")
    skills = card.get("skills", [])
    print(f"  Discovered {len(skills)} skill(s):")
    tools: list[Tool] = []
    for skill in skills:
        sid  = skill["id"]          # "ns/name"
        desc = skill.get("description") or f"Mycelium mesh skill: {sid}"
        print(f"    · {sid:<30}  {desc}")

        def _fn(message: str, _sid: str = sid) -> str:
            return client.send(_sid, message, timeout_secs=120.0)

        tools.append(Tool(
            name=sid.replace("/", "_"),
            description=desc,
            func=_fn,
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
    agent = initialize_agent(
        tools,
        llm,
        agent=AgentType.ZERO_SHOT_REACT_DESCRIPTION,
        verbose=True,
        max_iterations=6,
        handle_parsing_errors=True,
    )

    print(f"Query: {QUERY}\n")
    result = agent.run(QUERY)
    print(f"\n{'=' * 60}")
    print(result)


if __name__ == "__main__":
    main()
