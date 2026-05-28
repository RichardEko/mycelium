"""
Mycelium x AutoGen  --  A2A auto-discovery demo
================================================
An AutoGen AssistantAgent that discovers every skill advertised by a running
Mycelium cluster and registers them as callable tools.  Same auto-discovery
story as the LangChain example but using the AutoGen v0.4 API.

Quick start
-----------
1. Build SkillRunner with A2A support:
       cargo build --bin skillrunner --features a2a

2. Start the 3-skill community cluster:
       cd examples/community && ./start.sh

3. Install Python dependencies:
       pip install -r examples/a2a_langchain/requirements.txt

4. Run (Ollama, no API key needed):
       python examples/a2a_langchain/autogen_agent.py

   Or with OpenAI:
       OPENAI_API_KEY=sk-... python examples/a2a_langchain/autogen_agent.py

Environment variables
---------------------
MYCELIUM_URL    Base URL of any Mycelium node with http_port set.
                Default: http://localhost:9050
QUERY           Task to give the agent.
OPENAI_API_KEY  If set, uses gpt-4o-mini via OpenAI.
                Otherwise uses Ollama llama3.2 on localhost:11434.
OLLAMA_MODEL    Ollama model name (default: llama3.2).
"""
from __future__ import annotations

import asyncio
import inspect
import os
import sys
from typing import Callable

from autogen_agentchat.agents import AssistantAgent
from autogen_agentchat.ui import Console
from autogen_ext.models.openai import OpenAIChatCompletionClient

from mycelium.a2a import A2aClient

MYCELIUM_URL = os.environ.get("MYCELIUM_URL", "http://localhost:9050")
QUERY = os.environ.get(
    "QUERY",
    (
        "Write a short technical article about how gossip protocols achieve "
        "eventual consistency in distributed systems."
    ),
)


def _build_model_client() -> OpenAIChatCompletionClient:
    if key := os.environ.get("OPENAI_API_KEY"):
        return OpenAIChatCompletionClient(model="gpt-4o-mini", api_key=key)
    model = os.environ.get("OLLAMA_MODEL", "llama3.2")
    return OpenAIChatCompletionClient(
        model=model,
        base_url="http://localhost:11434/v1",
        api_key="ollama",
        model_capabilities={
            "json_output": False,
            "vision": False,
            "function_calling": True,
        },
    )


def _discover_tools(client: A2aClient) -> list[Callable]:
    """Fetch /.well-known/agent.json and return one callable per skill.

    Each callable has the correct __name__ and __doc__ for AutoGen tool
    registration.
    """
    card = client.fetch_card()
    print(f"\n  Connected to: {card.get('name', 'Mycelium cluster')}")
    skills = card.get("skills", [])
    print(f"  Discovered {len(skills)} skill(s):")
    fns: list[Callable] = []
    for skill in skills:
        sid  = skill["id"]
        desc = skill.get("description") or f"Mycelium mesh skill: {sid}"
        print(f"    · {sid:<30}  {desc}")

        def _make(s: str, d: str) -> Callable:
            def tool_fn(message: str) -> str:
                return client.send(s, message, timeout_secs=120.0)
            tool_fn.__name__ = s.replace("/", "_")
            tool_fn.__doc__  = d
            # Give the function a proper signature so AutoGen can generate
            # the JSON schema for it.
            tool_fn.__annotations__ = {"message": str, "return": str}
            return tool_fn

        fns.append(_make(sid, desc))
    print()
    return fns


async def main() -> None:
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

    model  = _build_model_client()
    agent  = AssistantAgent(
        name="mycelium_agent",
        model_client=model,
        tools=tools,
        system_message=(
            "You are a helpful assistant with access to Mycelium mesh skills. "
            "Use the available tools to answer the user's request."
        ),
    )

    print(f"Query: {QUERY}\n")
    await Console(agent.run_stream(task=QUERY))


if __name__ == "__main__":
    asyncio.run(main())
