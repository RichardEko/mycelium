"""
mycelium.wiki — HTTP gateway client for the Mycelium group wiki.

Wraps the ``/gateway/wiki/*`` endpoints exposed by a Rust node running the
``mycelium-wiki`` companion crate with the ``gateway`` feature.

The wiki is the group's durable, curated knowledge canon — the long-term-memory
sibling of the blackboard's working memory. ``read``/``query`` are served
directly from the store (any node, in parallel); ``propose`` enqueues an edit
that the group's elected **curator** applies (single writer of record). It
composes with an external metrics store (Postgres) and RAG by a shared id
namespace — it is the *authoritative, maintained-meaning* layer, not a
similarity index.

Example::

    import asyncio
    from mycelium.wiki import Wiki

    async def main():
        wiki = Wiki("127.0.0.1", 7946, group="council")
        await wiki.propose(page="decisions/elm-street",
                           heading="Resolution 2026-14",
                           body="protected bike lane approved",
                           attributes={"topic": "transport"})
        page = await wiki.read("decisions/elm-street")     # served from the store
        hits = await wiki.query(equals={"topic": "transport"})

    asyncio.run(main())
"""

from __future__ import annotations

from typing import Optional

import httpx


class Wiki:
    """Async client for one group's wiki via a node's HTTP gateway."""

    def __init__(self, host: str, port: int, group: str = "wiki"):
        self._base_url = f"http://{host}:{port}"
        self._group = group

    async def read(self, page: str) -> Optional[dict]:
        """Read a page (manifest joined with its live sections, in render order), or ``None`` if the
        page has no manifest. Served directly from the store."""
        async with httpx.AsyncClient(base_url=self._base_url, timeout=15.0) as c:
            r = await c.post("/gateway/wiki/read", json={"group": self._group, "page": page})
        r.raise_for_status()
        return r.json()["page"]

    async def query(self, equals: Optional[dict[str, str]] = None) -> list[dict]:
        """Query sections by attribute (all-of equality — a structured filter, not similarity search).
        Returns ``[{page, id, heading, attributes}, …]``."""
        async with httpx.AsyncClient(base_url=self._base_url, timeout=15.0) as c:
            r = await c.post("/gateway/wiki/query", json={"group": self._group, "equals": equals or {}})
        r.raise_for_status()
        return r.json()["hits"]

    async def propose(
        self,
        page: str,
        body: str,
        heading: str = "",
        section: Optional[str] = None,
        attributes: Optional[dict[str, str]] = None,
    ) -> dict:
        """Propose an edit; the curator applies it (single writer of record). Omit ``section`` to mint a
        new one, or pass an existing id to edit it. Returns ``{"proposal": key, "section": id}``."""
        payload = {
            "group": self._group,
            "page": page,
            "heading": heading,
            "body": body,
            "attributes": attributes or {},
        }
        if section is not None:
            payload["section"] = section
        async with httpx.AsyncClient(base_url=self._base_url, timeout=15.0) as c:
            r = await c.post("/gateway/wiki/propose", json=payload)
        r.raise_for_status()
        return r.json()
