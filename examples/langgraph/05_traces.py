#!/usr/bin/env python3
"""
Rung 5 of the LangGraph example ladder (docs/plans/mycelium-reason-examples.md):
**fleet-reasoning traces** — replayable, causal traces of routed inference.

Rung 4 routed inference and reported *which* node answered. This rung asks the
next question: *why did the fleet reason the way it did?* When a routed call
carries a ``run_id``, the node records the route decision + each ``llm_call``
attempt to a gossip-replicated, HLC-ordered log stream (``reason/{run_id}/…``).
``ReasonClient.trace(run_id)`` replays that stream — from **any** node, since the
records gossip — into ``events`` plus a human ``narrative``. It is the same story
the Rust ``narrate`` tells, over the gateway.

Against the CI ``reason_node``'s EchoBackend the inference is a trivial echo, but
the trace is real: a ``route`` event (candidates + chosen provider) and an
``llm_call`` event per attempt, HLC-ordered.

Run against a reason node::

    MYCELIUM_TEST_PORT=8101 python examples/langgraph/05_traces.py

Skips cleanly (prints a note, exits 0) when MYCELIUM_TEST_PORT is unset.
"""

from __future__ import annotations

import asyncio
import os
import sys
import uuid

from mycelium import NoProviderError, ReasonClient, RouteExhaustedError

HOST = os.getenv("MYCELIUM_TEST_HOST", "127.0.0.1")
PORT = os.getenv("MYCELIUM_TEST_PORT", "8101")
MODEL = os.getenv("MYCELIUM_TEST_MODEL", "fable-mini")


async def main() -> int:
    if os.getenv("MYCELIUM_TEST_PORT") is None:
        print("MYCELIUM_TEST_PORT unset — start a reason_node and set it. Skipping.")
        return 0

    run_id = f"rung5-{uuid.uuid4()}"

    async with ReasonClient(HOST, int(PORT)) as reason:
        # A couple of routed calls under one run_id — each records its route decision.
        try:
            for i in range(2):
                result = await reason.route(MODEL, f"reason step {i}", run_id=run_id)
                assert f"reason step {i}" in result["output"]
        except NoProviderError as e:
            print(f"no provider serves {e.model!r} — is the reason node serving it?")
            return 1
        except RouteExhaustedError as e:
            print(f"every candidate failed: {e.detail}")
            return 1
        print(f"✓ two routed calls recorded under run_id {run_id}")

        trace = await reason.trace(run_id)

    events = trace["events"]
    assert events, "the run recorded no events"
    kinds = [e["kind"] for e in events]
    assert "route" in kinds, f"expected a route event, got {kinds}"
    print(f"✓ trace replayed {len(events)} event(s); kinds: {kinds}")

    print("  narrative:")
    for line in trace["narrative"]:
        print(f"    {line}")

    print("RUNG 5 OK")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
