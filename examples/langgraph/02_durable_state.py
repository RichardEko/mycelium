#!/usr/bin/env python3
"""
Rung 2 of the LangGraph example ladder (docs/plans/mycelium-reason-examples.md):
**LangGraph on Mycelium — state survives a fresh client**.

Rungs 0–1 called a skill. This is the first rung where LangGraph itself runs *on*
Mycelium: a ``StateGraph`` compiled with ``MyceliumCheckpointSaver`` writes its
checkpoint to the mesh — the metadata index into gossiped KV, the payloads into
the content-addressed blob tier. To prove the state is durable (not just held in
a live client), we run the graph, drop the saver, build a **fresh** saver against
the same node, and read the checkpoint back.

The graph is deterministic — no LLM reasoning, just a counter and a list — so the
run is repeatable in CI. The point is the checkpoint, not the computation.

Run against a reason node::

    MYCELIUM_TEST_PORT=8101 python examples/langgraph/02_durable_state.py

Skips cleanly (prints a note, exits 0) when MYCELIUM_TEST_PORT is unset.
"""

from __future__ import annotations

import os
import sys
import uuid
from typing import Annotated

from typing_extensions import TypedDict

from langgraph.graph import END, START, StateGraph
from langgraph_checkpoint_mycelium import MyceliumCheckpointSaver

HOST = os.getenv("MYCELIUM_TEST_HOST", "127.0.0.1")
PORT = os.getenv("MYCELIUM_TEST_PORT", "8101")


def appended(left: list, right: list) -> list:
    return left + right


class State(TypedDict):
    total: int
    steps: Annotated[list, appended]


def build() -> StateGraph:
    """A tiny deterministic graph: two nodes bump a counter and append a marker."""
    builder = StateGraph(State)
    builder.add_node("one", lambda s: {"total": s["total"] + 1, "steps": ["one"]})
    builder.add_node("two", lambda s: {"total": s["total"] + 10, "steps": ["two"]})
    builder.add_edge(START, "one")
    builder.add_edge("one", "two")
    builder.add_edge("two", END)
    return builder


def main() -> int:
    if os.getenv("MYCELIUM_TEST_PORT") is None:
        print("MYCELIUM_TEST_PORT unset — start a reason_node and set it. Skipping.")
        return 0

    thread_id = str(uuid.uuid4())
    config = {"configurable": {"thread_id": thread_id}}

    # ── Run the graph to completion, checkpointing into the mesh. ─────────────────
    saver = MyceliumCheckpointSaver(HOST, int(PORT))
    graph = build().compile(checkpointer=saver)
    final = graph.invoke({"total": 0, "steps": []}, config)
    assert final["total"] == 11 and final["steps"] == ["one", "two"], f"unexpected run: {final!r}"
    head = saver.get_tuple(config)
    assert head is not None, "no checkpoint written"
    expected_id = head.checkpoint["id"]
    saver.close()
    print(f"✓ graph ran and checkpointed on the mesh (total={final['total']}, id={expected_id[:8]}…)")

    # ── A FRESH saver against the same node reads the durable state back. ─────────
    saver2 = MyceliumCheckpointSaver(HOST, int(PORT))
    reread = saver2.get_tuple(config)
    saver2.close()
    assert reread is not None, "fresh saver could not find the checkpoint"
    assert reread.checkpoint["id"] == expected_id, "fresh saver read a different head"
    assert reread.checkpoint["channel_values"]["total"] == 11, "state did not persist"
    assert reread.checkpoint["channel_values"]["steps"] == ["one", "two"], "list state lost"
    print(f"✓ fresh client re-read the same checkpoint: total={reread.checkpoint['channel_values']['total']}")

    # Clean up the thread's index rows (payload blobs stay content-addressed).
    with MyceliumCheckpointSaver(HOST, int(PORT)) as s:
        s.delete_thread(thread_id)

    print("RUNG 2 OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
