#!/usr/bin/env python3
"""
Rung 0 of the LangGraph example ladder (docs/plans/mycelium-reason-examples.md):
the **minimal LangChain starter** — one mesh skill, wrapped as a LangChain Runnable.

The a2a demo (``examples/a2a_langchain/``) shows an LLM-*driven* agent picking a
Mycelium skill as a tool. This rung is the rung below it: no agent, no
tool-selection, no reasoning loop — just the one fact everything else builds on:

    **A Mycelium skill *is* a LangChain Runnable.**

We wrap a routed skill call as a ``RunnableLambda`` and invoke a one-step chain.
Because the CI ``reason_node`` serves an EchoBackend, the "model" just echoes the
input (``echo: {input}``) — the point is the wiring, not the model. Wrapping a
call as a Runnable is what lets it compose into any LangChain pipeline; the
LLM-driven tool-calling story is the a2a example's job, not this one.

LangChain Runnables here are **sync**, so the skill call is a sync ``httpx.post``
to ``/gateway/reason/route`` (``ReasonClient.route`` is async — used by the higher,
async rungs).

Run against a reason node::

    MYCELIUM_TEST_PORT=8101 python examples/langgraph/00_hello_skill.py

Skips cleanly (prints a note, exits 0) when MYCELIUM_TEST_PORT is unset.
"""

from __future__ import annotations

import os
import sys

import httpx
from langchain_core.runnables import RunnableLambda

HOST = os.getenv("MYCELIUM_TEST_HOST", "127.0.0.1")
PORT = os.getenv("MYCELIUM_TEST_PORT", "8101")
MODEL = os.getenv("MYCELIUM_TEST_MODEL", "fable-mini")


def sync_route(model: str, text: str) -> str:
    """One sync POST to the routed-inference gateway; returns the skill output."""
    resp = httpx.post(
        f"http://{HOST}:{PORT}/gateway/reason/route",
        json={"model": model, "input": text},
        timeout=35.0,
    )
    resp.raise_for_status()
    return resp.json()["output"]


def main() -> int:
    if os.getenv("MYCELIUM_TEST_PORT") is None:
        print("MYCELIUM_TEST_PORT unset — start a reason_node and set it. Skipping.")
        return 0

    # The skill IS a Runnable: wrap the mesh call and compose it into a chain.
    skill = RunnableLambda(lambda text: sync_route(MODEL, text))
    print("✓ wrapped the mesh skill as a LangChain RunnableLambda")

    output = skill.invoke("hello from rung 0")
    print(f"✓ chain output: {output!r}")

    # The EchoBackend renders `echo: {input}` — the routed call ran and carried the input.
    assert "hello from rung 0" in output, f"echo did not carry the input: {output!r}"

    print("RUNG 0 OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
