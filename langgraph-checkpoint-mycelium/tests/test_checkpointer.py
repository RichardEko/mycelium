"""
Integration tests for MyceliumCheckpointSaver.

Requires a running reason node (the `reason_node` example) with its HTTP
gateway port in MYCELIUM_TEST_PORT; the cross-node test additionally needs a
second, mesh-joined node's port in MYCELIUM_TEST_PORT_B. All tests skip
cleanly when the env vars are unset.

Run with:
    # terminal 1 — node A
    BIND_PORT=7101 HTTP_PORT=8101 BLOB_DIR=/tmp/blobs-a \
      cargo run -p mycelium-reason --features llm,gateway --example reason_node
    # terminal 2 — node B
    BIND_PORT=7102 HTTP_PORT=8102 BOOTSTRAP=127.0.0.1:7101 BLOB_DIR=/tmp/blobs-b \
      cargo run -p mycelium-reason --features llm,gateway --example reason_node
    # terminal 3
    MYCELIUM_TEST_PORT=8101 MYCELIUM_TEST_PORT_B=8102 pytest tests/ -v
"""

import base64
import json
import os
import time
import uuid

import httpx
import pytest

from langgraph.checkpoint.base import empty_checkpoint
from langgraph.checkpoint.base.id import uuid6

from langgraph_checkpoint_mycelium import MyceliumCheckpointSaver
from langgraph_checkpoint_mycelium.saver import _seg

TEST_HOST = os.getenv("MYCELIUM_TEST_HOST", "127.0.0.1")
TEST_PORT = os.getenv("MYCELIUM_TEST_PORT")
TEST_PORT_B = os.getenv("MYCELIUM_TEST_PORT_B")

pytestmark = pytest.mark.skipif(
    TEST_PORT is None,
    reason="MYCELIUM_TEST_PORT not set — no reason node to test against",
)


@pytest.fixture
def saver() -> MyceliumCheckpointSaver:
    with MyceliumCheckpointSaver(TEST_HOST, int(TEST_PORT)) as s:
        yield s


def fresh_thread() -> str:
    return f"t-{uuid.uuid4()}"


def make_checkpoint(channel_values: dict, ts: str = "2026-07-08T00:00:00+00:00") -> dict:
    c = empty_checkpoint()
    c["id"] = str(uuid6())
    c["ts"] = ts
    c["channel_values"] = channel_values
    c["channel_versions"] = {ch: 1 for ch in channel_values}
    return c


def config_for(thread_id: str, checkpoint_ns: str = "", checkpoint_id: str | None = None) -> dict:
    configurable = {"thread_id": thread_id, "checkpoint_ns": checkpoint_ns}
    if checkpoint_id is not None:
        configurable["checkpoint_id"] = checkpoint_id
    return {"configurable": configurable}


def kv_row(port: int, key: str) -> dict | None:
    """Read a raw KV index row through the gateway (for storage-shape asserts)."""
    resp = httpx.get(f"http://{TEST_HOST}:{port}/gateway/kv", params={"key": key})
    resp.raise_for_status()
    data = resp.json()
    if not data.get("found"):
        return None
    return json.loads(base64.b64decode(data["value_b64"]))


class TestCrud:
    def test_put_get_tuple_roundtrip_with_parent_chain(self, saver) -> None:
        thread = fresh_thread()
        ckpt1 = make_checkpoint({"messages": ["hello"], "count": 1})
        meta1 = {"source": "input", "step": -1, "parents": {}}
        cfg1 = saver.put(config_for(thread), ckpt1, meta1, {"messages": 1, "count": 1})
        assert cfg1["configurable"]["checkpoint_id"] == ckpt1["id"]

        tup = saver.get_tuple(cfg1)
        assert tup is not None
        assert tup.checkpoint["id"] == ckpt1["id"]
        assert tup.checkpoint["channel_values"] == {"messages": ["hello"], "count": 1}
        assert tup.metadata["source"] == "input"
        assert tup.metadata["step"] == -1
        assert tup.parent_config is None

        # A child checkpoint: parent linkage comes from the put() config.
        ckpt2 = make_checkpoint({"messages": ["hello", "world"], "count": 2})
        meta2 = {"source": "loop", "step": 0, "parents": {"": ckpt1["id"]}}
        cfg2 = saver.put(cfg1, ckpt2, meta2, {"messages": 2, "count": 2})

        # No checkpoint_id in the config → the latest (UUID6 lexicographic max).
        latest = saver.get_tuple(config_for(thread))
        assert latest.checkpoint["id"] == ckpt2["id"]
        assert latest.parent_config["configurable"]["checkpoint_id"] == ckpt1["id"]
        assert latest.checkpoint["channel_values"]["messages"] == ["hello", "world"]

        saver.delete_thread(thread)

    def test_get_tuple_missing_returns_none(self, saver) -> None:
        assert saver.get_tuple(config_for(fresh_thread())) is None
        assert saver.get_tuple(config_for(fresh_thread(), checkpoint_id=str(uuid6()))) is None

    def test_list_limit_before_and_filter(self, saver) -> None:
        thread = fresh_thread()
        cfg = config_for(thread)
        ids = []
        for step in range(3):
            ckpt = make_checkpoint({"count": step})
            meta = {"source": "input" if step == 0 else "loop", "step": step - 1, "parents": {}}
            cfg = saver.put(cfg, ckpt, meta, {"count": 1})
            ids.append(ckpt["id"])

        listed = list(saver.list(config_for(thread)))
        assert [t.checkpoint["id"] for t in listed] == list(reversed(ids))

        limited = list(saver.list(config_for(thread), limit=1))
        assert [t.checkpoint["id"] for t in limited] == [ids[-1]]

        before = list(saver.list(config_for(thread), before=config_for(thread, checkpoint_id=ids[-1])))
        assert [t.checkpoint["id"] for t in before] == [ids[1], ids[0]]

        filtered = list(saver.list(config_for(thread), filter={"source": "input"}))
        assert [t.checkpoint["id"] for t in filtered] == [ids[0]]

        saver.delete_thread(thread)

    def test_put_writes_visible_on_get_tuple(self, saver) -> None:
        thread = fresh_thread()
        ckpt = make_checkpoint({"count": 0})
        cfg = saver.put(config_for(thread), ckpt, {"source": "input", "step": -1, "parents": {}}, {"count": 1})

        saver.put_writes(cfg, [("count", 41), ("messages", ["pending"])], task_id="task-1")
        tup = saver.get_tuple(cfg)
        assert tup.pending_writes == [
            ("task-1", "count", 41),
            ("task-1", "messages", ["pending"]),
        ]

        saver.delete_thread(thread)

    def test_delete_thread_removes_everything(self, saver) -> None:
        thread = fresh_thread()
        ckpt = make_checkpoint({"count": 7})
        cfg = saver.put(config_for(thread), ckpt, {"source": "input", "step": -1, "parents": {}}, {"count": 1})
        saver.put_writes(cfg, [("count", 8)], task_id="task-1")
        assert saver.get_tuple(cfg) is not None

        saver.delete_thread(thread)
        assert saver.get_tuple(config_for(thread)) is None
        assert list(saver.list(config_for(thread))) == []

    def test_empty_checkpoint_ns_is_distinct_from_named(self, saver) -> None:
        thread = fresh_thread()
        root = make_checkpoint({"count": 1})
        sub = make_checkpoint({"count": 2})
        saver.put(config_for(thread, ""), root, {"source": "input", "step": -1, "parents": {}}, {"count": 1})
        saver.put(config_for(thread, "child:1"), sub, {"source": "input", "step": -1, "parents": {}}, {"count": 1})

        assert saver.get_tuple(config_for(thread, "")).checkpoint["id"] == root["id"]
        assert saver.get_tuple(config_for(thread, "child:1")).checkpoint["id"] == sub["id"]
        # list scoped by ns sees only its own namespace.
        assert [t.checkpoint["id"] for t in saver.list(config_for(thread, ""))] == [root["id"]]

        saver.delete_thread(thread)


class TestAsyncVariants:
    @pytest.mark.asyncio
    async def test_aput_aget_alist_adelete(self) -> None:
        async with MyceliumCheckpointSaver(TEST_HOST, int(TEST_PORT)) as saver:
            thread = fresh_thread()
            ckpt = make_checkpoint({"messages": ["async"]})
            cfg = await saver.aput(
                config_for(thread), ckpt, {"source": "input", "step": -1, "parents": {}}, {"messages": 1}
            )
            await saver.aput_writes(cfg, [("messages", ["a-pending"])], task_id="task-a")

            tup = await saver.aget_tuple(config_for(thread))
            assert tup.checkpoint["id"] == ckpt["id"]
            assert tup.checkpoint["channel_values"] == {"messages": ["async"]}
            assert tup.pending_writes == [("task-a", "messages", ["a-pending"])]

            listed = [t async for t in saver.alist(config_for(thread))]
            assert len(listed) == 1

            await saver.adelete_thread(thread)
            assert await saver.aget_tuple(config_for(thread)) is None


class TestBlobDedup:
    def test_unchanged_channel_value_shares_one_blob(self, saver) -> None:
        """Content addressing: the same channel bytes across two checkpoints
        resolve to the same blob id — visible in the KV index rows."""
        thread = fresh_thread()
        shared = {"transcript": ["a long shared message"] * 50}
        ckpt1 = make_checkpoint({**shared, "count": 1})
        cfg1 = saver.put(
            config_for(thread), ckpt1, {"source": "input", "step": -1, "parents": {}}, {"transcript": 1, "count": 1}
        )
        ckpt2 = make_checkpoint({**shared, "count": 2})
        saver.put(cfg1, ckpt2, {"source": "loop", "step": 0, "parents": {}}, {"count": 2})

        row1 = kv_row(int(TEST_PORT), f"ckpt/{_seg(thread)}/__root__/{ckpt1['id']}")
        row2 = kv_row(int(TEST_PORT), f"ckpt/{_seg(thread)}/__root__/{ckpt2['id']}")
        assert row1["channels"]["transcript"][0] == row2["channels"]["transcript"][0], (
            "the unchanged channel dedups to one content address"
        )
        assert row1["channels"]["count"][0] != row2["channels"]["count"][0], (
            "the changed channel gets a new content address"
        )

        saver.delete_thread(thread)


@pytest.mark.skipif(
    TEST_PORT_B is None,
    reason="MYCELIUM_TEST_PORT_B not set — cross-node resume needs a two-node mesh",
)
class TestCrossNodeResume:
    def test_langgraph_run_checkpointed_on_a_resumes_on_b(self) -> None:
        """The whole point: a REAL StateGraph run interrupted while
        checkpointing via node A's gateway continues via node B's — metadata
        arrives by gossip, payloads by the mesh blob fetch."""
        from typing import Annotated
        from typing_extensions import TypedDict
        from langgraph.graph import END, START, StateGraph

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

        thread = fresh_thread()
        config = config_for(thread)

        with MyceliumCheckpointSaver(TEST_HOST, int(TEST_PORT)) as saver_a:
            graph_a = build().compile(checkpointer=saver_a, interrupt_before=["two"])
            partial = graph_a.invoke({"total": 0, "steps": []}, config)
            assert partial["total"] == 1 and partial["steps"] == ["one"]

            head_a = saver_a.get_tuple(config)
            assert head_a is not None
            expected_id = head_a.checkpoint["id"]
            expected_writes = len(head_a.pending_writes or [])

        with MyceliumCheckpointSaver(TEST_HOST, int(TEST_PORT_B)) as saver_b:
            # Structural convergence poll: node B must gossip in the SAME head
            # (and its pending writes) before the resume — bounded, no fixed sleeps.
            deadline = time.monotonic() + 60.0
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

            graph_b = build().compile(checkpointer=saver_b, interrupt_before=["two"])
            final = graph_b.invoke(None, config)
            assert final["total"] == 11
            assert final["steps"] == ["one", "two"]

            saver_b.delete_thread(thread)
