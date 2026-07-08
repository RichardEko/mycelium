#!/usr/bin/env python3
"""
Rung 3 of the LangGraph example ladder (docs/plans/mycelium-reason-examples.md):
**cross-node resume** — any node resumes any thread (no node failure needed).

Rung 2 proved a checkpoint survives a fresh client on the *same* node. This rung
proves the checkpointer's headline property: a thread checkpointed via node **A**
resumes via node **B**, purely by gossip. No kill, no reheal — just eventual
consistency doing its job. The graph runs partway on A (``interrupt_before`` its
second node), checkpoints, and B — after the thread head has gossiped in —
resumes it to completion. Metadata arrives via gossiped KV; payloads via the
mesh blob fetch (local-then-mesh).

The convergence wait is a **bounded structural poll** (never a fixed sleep as the
mechanism), mirroring the checkpointer's own cross-node test: B must see the same
head id before it may resume.

Run against a two-node reason mesh (A + B, B bootstrapped off A)::

    MYCELIUM_TEST_PORT=8101 MYCELIUM_TEST_PORT_B=8102 \
        python examples/langgraph/03_cross_node.py

Skips cleanly (prints a note, exits 0) when either port is unset.
"""

from __future__ import annotations

import os
import sys
import time
import uuid
from typing import Annotated

from typing_extensions import TypedDict

from langgraph.graph import END, START, StateGraph
from langgraph_checkpoint_mycelium import MyceliumCheckpointSaver

HOST = os.getenv("MYCELIUM_TEST_HOST", "127.0.0.1")
PORT_A = os.getenv("MYCELIUM_TEST_PORT")
PORT_B = os.getenv("MYCELIUM_TEST_PORT_B")

CONVERGE_TIMEOUT = 60.0


def appended(left: list, right: list) -> list:
    return left + right


class State(TypedDict):
    total: int
    steps: Annotated[list, appended]


def build() -> StateGraph:
    builder = StateGraph(State)
    builder.add_node("one", lambda s: {"total": s["total"] + 1, "steps": ["one"]})
    builder.add_node("two", lambda s: {"total": s["total"] + 10, "steps": ["two"]})
    builder.add_edge(START, "one")
    builder.add_edge("one", "two")
    builder.add_edge("two", END)
    return builder


def main() -> int:
    if PORT_A is None or PORT_B is None:
        print(
            "MYCELIUM_TEST_PORT / MYCELIUM_TEST_PORT_B unset — cross-node resume needs a "
            "two-node mesh. Skipping."
        )
        return 0

    thread_id = str(uuid.uuid4())
    config = {"configurable": {"thread_id": thread_id}}

    # ── Run partway on node A: it stops before `two`, checkpointing the head. ─────
    with MyceliumCheckpointSaver(HOST, int(PORT_A)) as saver_a:
        graph_a = build().compile(checkpointer=saver_a, interrupt_before=["two"])
        partial = graph_a.invoke({"total": 0, "steps": []}, config)
        assert partial["total"] == 1 and partial["steps"] == ["one"], f"unexpected partial: {partial!r}"
        head_a = saver_a.get_tuple(config)
        assert head_a is not None, "no checkpoint written on A"
        expected_id = head_a.checkpoint["id"]
        expected_writes = len(head_a.pending_writes or [])
    print(f"✓ ran partway on A (total={partial['total']}), checkpointed head {expected_id[:8]}…")

    # ── Node B: poll for the thread head to gossip in, then resume it. ────────────
    with MyceliumCheckpointSaver(HOST, int(PORT_B)) as saver_b:
        deadline = time.monotonic() + CONVERGE_TIMEOUT
        while True:
            head_b = saver_b.get_tuple(config)
            if (
                head_b is not None
                and head_b.checkpoint["id"] == expected_id
                and len(head_b.pending_writes or []) >= expected_writes
            ):
                break
            assert time.monotonic() < deadline, "node B never converged on the thread head"
            time.sleep(0.25)
        print(f"✓ head gossiped A → B; B sees {expected_id[:8]}…")

        graph_b = build().compile(checkpointer=saver_b, interrupt_before=["two"])
        final = graph_b.invoke(None, config)
        assert final["total"] == 11, f"resume produced wrong total: {final!r}"
        assert final["steps"] == ["one", "two"], f"resume lost list state: {final!r}"
        print(f"✓ resumed on B to completion (total={final['total']}, steps={final['steps']})")

        saver_b.delete_thread(thread_id)

    print("RUNG 3 OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
