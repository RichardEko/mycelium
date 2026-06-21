"""
mycelium.blackboard — HTTP gateway client for the Mycelium Blackboard.

Wraps the ``/gateway/bb/*`` endpoints exposed by a Rust node running the
``mycelium-blackboard`` companion crate with the ``gateway`` feature.

The blackboard is shared working memory: agents ``post`` typed facts that any
agent can ``read`` (non-destructive, concurrent — Linda's ``rd``), and a finite
fact is consumed by exactly one agent via ``claim`` (competitive, destructive —
Linda's ``in``). Routing is by *content*: a claim names a **predicate** over
fact attributes, and which agent acts is decided by the fact, not by where it
was put.

Worker pattern::

    import asyncio
    from mycelium.blackboard import Blackboard

    async def main():
        bb = Blackboard("127.0.0.1", 7946, ns="microgrid")
        while True:
            claimed = await bb.claim(eq={"kind": "surplus", "feeder": "4"})
            if claimed is None:
                await asyncio.sleep(1)   # nothing to act on yet
                continue
            fact_id, attributes, payload = claimed
            try:
                act_on(payload)                 # consume the finite surplus
                await bb.ack(fact_id)           # terminal — consumed once
            except Exception:
                await bb.release(fact_id)       # give it back; another agent claims it

    asyncio.run(main())
"""

from __future__ import annotations

import base64
from typing import Optional

import httpx


class BlackboardNotFoundError(Exception):
    """Unknown claim id — already acked, released, re-queued by the deadline, or never claimed."""


class Blackboard:
    """Async client for one board namespace via a node's HTTP gateway."""

    def __init__(self, host: str, port: int, ns: str = "board"):
        self._base_url = f"http://{host}:{port}"
        self._ns = ns

    async def post(self, attributes: dict[str, str], payload: bytes) -> int:
        """Post a fact (Linda ``out``) — non-destructive; readable + claimable cluster-wide.
        Returns the fact id."""
        async with httpx.AsyncClient(base_url=self._base_url, timeout=15.0) as c:
            r = await c.post("/gateway/bb/post", json={
                "ns": self._ns,
                "attributes": attributes,
                "payload_b64": base64.b64encode(payload).decode(),
            })
        r.raise_for_status()
        return int(r.json()["id"])

    async def read(
        self, eq: Optional[dict[str, str]] = None, present: Optional[list[str]] = None
    ) -> list[tuple[int, dict[str, str], bytes]]:
        """Non-destructive read (Linda ``rd``): all facts matching the predicate
        (attribute equality ``eq`` + presence ``present``). Returns
        ``[(id, attributes, payload), …]``."""
        async with httpx.AsyncClient(base_url=self._base_url, timeout=15.0) as c:
            r = await c.post("/gateway/bb/read", json={
                "ns": self._ns, "eq": eq or {}, "present": present or [],
            })
        r.raise_for_status()
        return [_decode_fact(f) for f in r.json()["facts"]]

    async def claim(
        self, eq: Optional[dict[str, str]] = None, present: Optional[list[str]] = None
    ) -> Optional[tuple[int, dict[str, str], bytes]]:
        """Competitive destructive claim (Linda ``in``): claim one fact matching the
        predicate, or ``None`` if none match. Returns ``(id, attributes, payload)``."""
        async with httpx.AsyncClient(base_url=self._base_url, timeout=15.0) as c:
            r = await c.post("/gateway/bb/claim", json={
                "ns": self._ns, "eq": eq or {}, "present": present or [],
            })
        r.raise_for_status()
        body = r.json()
        if not body.get("claimed"):
            return None
        return _decode_fact(body["fact"])

    async def ack(self, fact_id: int) -> None:
        """Terminal ack: the claimed fact was consumed."""
        async with httpx.AsyncClient(base_url=self._base_url, timeout=15.0) as c:
            r = await c.post("/gateway/bb/ack", json={"ns": self._ns, "id": fact_id})
        if r.status_code == 404:
            raise BlackboardNotFoundError(f"unknown claim id {fact_id}")
        r.raise_for_status()

    async def release(self, fact_id: int) -> None:
        """Release a claim: the fact returns to claimable (the abort path)."""
        async with httpx.AsyncClient(base_url=self._base_url, timeout=15.0) as c:
            r = await c.post("/gateway/bb/release", json={"ns": self._ns, "id": fact_id})
        if r.status_code == 404:
            raise BlackboardNotFoundError(f"unknown claim id {fact_id}")
        r.raise_for_status()

    async def depth(self) -> tuple[int, int]:
        """Live ``(available, inflight)`` counts for the board."""
        async with httpx.AsyncClient(base_url=self._base_url, timeout=15.0) as c:
            r = await c.get("/gateway/bb/depth", params={"ns": self._ns})
        r.raise_for_status()
        body = r.json()
        return int(body["available"]), int(body["inflight"])


def _decode_fact(f: dict) -> tuple[int, dict[str, str], bytes]:
    return int(f["id"]), dict(f["attributes"]), base64.b64decode(f["payload_b64"])
