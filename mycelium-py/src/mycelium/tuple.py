"""
mycelium.tuple — HTTP gateway client for the Mycelium TupleSpace.

Wraps the ``/gateway/tuple/*`` endpoints exposed by a Rust node running the
``mycelium-tuple-space`` companion crate with the ``gateway`` feature.

The tuple space is the pull-based work distribution pattern: workers
``take()`` only when ready, so load balance emerges from worker readiness —
no coordinator predicts anything.

Note — lanes, not Linda matching: unlike classic Linda's associative
template retrieval, this space is lane-addressed. Every op names a *stage*
(a per-stage FIFO lane); payloads are opaque and never matched. An item's
pipeline position is the lane it sits in, and ``complete()`` moves it
atomically to the next lane. A worker's only "filter" is choosing which
lane to ``take()`` from — use ``depth()`` to pick the deepest.

Worker pattern for heavy AI flows::

    import asyncio
    from mycelium.tuple import TupleSpace

    async def main():
        ts = TupleSpace("127.0.0.1", 7946, ns="news-pipeline")
        while True:
            item_id, payload = await ts.take("stage-a", timeout_secs=60)
            try:
                result = run_llm(payload)                     # seconds of compute
                await ts.complete(item_id, "stage-b", result) # atomic: no crash window
            except Exception:
                pass  # inflight deadline re-queues automatically; no ack needed

    asyncio.run(main())
"""

from __future__ import annotations

import asyncio
import base64
from typing import Any, Optional

import httpx


class TupleBackpressureError(Exception):
    """The primary is saturated (HTTP 503). Back off and retry."""

    def __init__(self, retry_after_ms: int = 500):
        self.retry_after_ms = retry_after_ms
        super().__init__(f"tuple-space backpressure; retry after {retry_after_ms} ms")


class TupleNotFoundError(Exception):
    """Unknown item id — already acked, expired back to the queue, or never existed."""


class TupleSpace:
    """Async client for one tuple space namespace via a node's HTTP gateway."""

    def __init__(self, host: str, port: int, ns: str = "pipeline"):
        self._base_url = f"http://{host}:{port}"
        self._ns = ns

    # ── Producer API ─────────────────────────────────────────────────────────

    async def put(
        self,
        stage: str,
        payload: bytes,
        *,
        backpressure: str = "raise",          # "raise" | "block"
        backpressure_timeout_secs: float = 30.0,
    ) -> int:
        """Write an item to ``stage``. Returns the item id.

        ``backpressure="raise"``: raises :class:`TupleBackpressureError`
        immediately when the primary is saturated.
        ``backpressure="block"``: retries with exponential backoff until
        ``backpressure_timeout_secs``, then raises.
        """
        deadline = asyncio.get_event_loop().time() + backpressure_timeout_secs
        delay = 0.1
        while True:
            try:
                return await self._put_once(stage, payload)
            except TupleBackpressureError:
                if backpressure != "block":
                    raise
                if asyncio.get_event_loop().time() + delay >= deadline:
                    raise
                await asyncio.sleep(delay)
                delay = min(delay * 2, 5.0)

    async def _put_once(self, stage: str, payload: bytes) -> int:
        async with httpx.AsyncClient(base_url=self._base_url, timeout=15.0) as c:
            r = await c.post("/gateway/tuple/put", json={
                "ns": self._ns,
                "stage": stage,
                "payload_b64": base64.b64encode(payload).decode(),
            })
        if r.status_code == 503:
            retry_ms = int(float(r.headers.get("Retry-After", "1")) * 1000)
            raise TupleBackpressureError(retry_ms)
        r.raise_for_status()
        return int(r.json()["id"])

    # ── Worker API ───────────────────────────────────────────────────────────

    async def take(self, stage: str, timeout_secs: float = 30.0) -> tuple[int, bytes]:
        """Blocking claim. Returns ``(item_id, payload)``.

        Raises :class:`TimeoutError` when no item arrives in time. The HTTP
        request blocks server-side for up to ``timeout_secs``.
        """
        async with httpx.AsyncClient(
            base_url=self._base_url,
            timeout=timeout_secs + 5.0,  # park decides, not the transport
        ) as c:
            r = await c.post("/gateway/tuple/take", json={
                "ns": self._ns,
                "stage": stage,
                "timeout_secs": int(timeout_secs),
            })
        if r.status_code == 408:
            raise TimeoutError(f"no item on stage {stage!r} within {timeout_secs}s")
        r.raise_for_status()
        body = r.json()
        return int(body["id"]), base64.b64decode(body["payload_b64"])

    async def complete(self, item_id: int, next_stage: str, payload: bytes) -> int:
        """Atomic pipeline advance: acks ``item_id`` AND puts ``next_stage``
        in one WAL record — no crash window between stages. PREFERRED over
        separate put + ack for every mid-pipeline transition. Returns the
        new item id."""
        async with httpx.AsyncClient(base_url=self._base_url, timeout=15.0) as c:
            r = await c.post("/gateway/tuple/complete", json={
                "ns": self._ns,
                "id": item_id,
                "next_stage": next_stage,
                "next_payload_b64": base64.b64encode(payload).decode(),
            })
        if r.status_code == 404:
            raise TupleNotFoundError(f"unknown item id {item_id}")
        r.raise_for_status()
        return int(r.json()["next_id"])

    async def ack(self, item_id: int) -> None:
        """Terminal ack: last stage of a pipeline or explicit abandonment."""
        async with httpx.AsyncClient(base_url=self._base_url, timeout=15.0) as c:
            r = await c.post("/gateway/tuple/ack", json={
                "ns": self._ns,
                "id": item_id,
            })
        if r.status_code == 404:
            raise TupleNotFoundError(f"unknown item id {item_id}")
        r.raise_for_status()

    # ── Inspection ───────────────────────────────────────────────────────────

    async def depth(self, stage: Optional[str] = None) -> dict[str, dict[str, int]]:
        """Returns ``{stage: {depth, waiters, inflight}}`` for one or all stages."""
        params: dict[str, Any] = {"ns": self._ns}
        if stage is not None:
            params["stage"] = stage
        async with httpx.AsyncClient(base_url=self._base_url, timeout=15.0) as c:
            r = await c.get("/gateway/tuple/depth", params=params)
        r.raise_for_status()
        return {
            s["stage"]: {
                "depth": s["depth"],
                "waiters": s["waiters"],
                "inflight": s["inflight"],
            }
            for s in r.json()["stages"]
        }
