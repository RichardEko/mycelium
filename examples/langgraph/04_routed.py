#!/usr/bin/env python3
"""
Rung 4 of the LangGraph example ladder (docs/plans/mycelium-reason-examples.md):
**routed inference**.

A LangGraph LLM node normally calls the mesh via ``/gateway/llm/call`` — which
resolves *one* provider and does a single RPC: no load-ranking, no failover. This
rung uses ``ReasonClient.route`` (``POST /gateway/reason/route``, backed by the
Rust ``InferenceRouter``) instead: each call is routed to a healthy ``llm/{model}``
provider, ranked by pheromone load, and **fails over** down the candidate list if a
provider errors or times out. Same one-line call site — real routing underneath.

This is a minimal, honest demo: the CI `reason_node` serves an EchoBackend, so the
"model" just echoes the input (``echo: {input}``). The point is *which* node
answered and on *which* attempt — the routing surface, not the model.

Run against a reason node::

    MYCELIUM_TEST_PORT=8101 python examples/langgraph/04_routed.py

Skips cleanly (prints a note, exits 0) when MYCELIUM_TEST_PORT is unset.
"""

import asyncio
import os
import sys

from mycelium import NoProviderError, ReasonClient, RouteExhaustedError

HOST = os.getenv("MYCELIUM_TEST_HOST", "127.0.0.1")
PORT = os.getenv("MYCELIUM_TEST_PORT", "8101")
MODEL = os.getenv("MYCELIUM_TEST_MODEL", "fable-mini")


async def main() -> int:
    if os.getenv("MYCELIUM_TEST_PORT") is None:
        print("MYCELIUM_TEST_PORT unset — start a reason_node and set it. Skipping.")
        return 0

    async with ReasonClient(HOST, int(PORT)) as reason:
        try:
            result = await reason.route(MODEL, "hello from rung 4")
        except NoProviderError as e:
            print(f"no provider serves {e.model!r} — is the reason node serving it?")
            return 1
        except RouteExhaustedError as e:
            print(f"every candidate failed: {e.detail}")
            return 1

    print(f"routed output : {result['output']}")
    print(f"answered by   : {result['provider']} (attempt {result['attempt']})")
    print(f"model_used    : {result['model_used']}, tokens: {result['tokens_used']}")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
