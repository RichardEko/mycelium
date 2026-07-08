#!/usr/bin/env python3
"""
Rung 1 of the LangGraph example ladder (docs/plans/mycelium-reason-examples.md):
**typed output through the mesh**.

Rung 0 got a raw string back. Real graphs want *structured* output. This rung
calls a mesh skill through ``mycelium.call_typed``, which validates the skill's
output against a pydantic model (extracting the first balanced JSON object from
the raw text — LLMs, and the EchoBackend, wrap JSON in prose) and retries with
the validation error fed back to the skill on a miss.

Against the CI ``reason_node``'s EchoBackend the model just echoes its input
(``echo: {input}``), so we pass a JSON string as the input — the echo returns
that JSON verbatim, and ``call_typed`` validates it into the model instance. The
teaching point is the schema-validated *contract*, not the model: when your code
talks to an LLM provider **directly** (not through the mesh) reach for the
Tier-1 libraries instead — Instructor or Pydantic-AI — but for a mesh skill,
``call_typed`` is the schema boundary.

Run against a reason node::

    MYCELIUM_TEST_PORT=8101 python examples/langgraph/01_typed.py

Skips cleanly (prints a note, exits 0) when MYCELIUM_TEST_PORT is unset.
Requires the ``typed`` extra: ``pip install 'mycelium-py[typed]'``.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys

from pydantic import BaseModel

from mycelium import PromptSkillClient, TypedCallError, call_typed

HOST = os.getenv("MYCELIUM_TEST_HOST", "127.0.0.1")
PORT = os.getenv("MYCELIUM_TEST_PORT", "8101")
MODEL = os.getenv("MYCELIUM_TEST_MODEL", "fable-mini")


class Match(BaseModel):
    """A tiny structured contract the echoed JSON must satisfy."""

    lot: str
    pantry: str
    crates: int


async def main() -> int:
    if os.getenv("MYCELIUM_TEST_PORT") is None:
        print("MYCELIUM_TEST_PORT unset — start a reason_node and set it. Skipping.")
        return 0

    # The echo backend returns `echo: {input}`; feed it JSON that satisfies Match,
    # so the echoed JSON validates cleanly into the model instance.
    payload = json.dumps({"lot": "orchard-7", "pantry": "north", "crates": 12})

    async with PromptSkillClient(HOST, int(PORT)) as client:
        try:
            match = await call_typed(client, "llm", MODEL, payload, Match)
        except TypedCallError as e:
            print(f"typed call failed validation: {e}\n  last output: {e.last_output!r}")
            return 1

    print(f"✓ validated into {type(match).__name__}: {match!r}")
    assert isinstance(match, Match), "call_typed must return a Match instance"
    assert match.lot == "orchard-7" and match.pantry == "north" and match.crates == 12

    print("RUNG 1 OK")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
