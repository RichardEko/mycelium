"""
mycelium.reason — HTTP gateway client for the ``mycelium-reason`` surface.

Wraps the ``/gateway/reason/*`` endpoints exposed by a Rust Mycelium node
compiled with ``--features gateway`` (the ``route`` endpoint) — plus ``llm``
for a node that actually serves models. Unlike :meth:`PromptSkillClient.call`
(``/gateway/llm/call``, single-shot: one resolved provider, one RPC, no
failover), :meth:`ReasonClient.route` is **load-aware and failover-capable** —
it routes each call to a healthy ``llm/{model}`` provider and fails over down
the ranked candidate list (wedge ①, `InferenceRouter`).

Example::

    import asyncio
    from mycelium import ReasonClient

    async def main():
        async with ReasonClient("127.0.0.1", 8080) as reason:
            result = await reason.route("fable-mini", "Hello!")
            print(result["output"], "from", result["provider"])

    asyncio.run(main())
"""

from __future__ import annotations

from typing import Any, Optional

import httpx


class ReasonError(Exception):
    """Base class for ``mycelium-reason`` gateway errors."""


class NoProviderError(ReasonError):
    """No live provider advertises ``llm/{model}`` (gateway returned 404)."""

    def __init__(self, model: str) -> None:
        self.model = model
        super().__init__(f"no provider for model {model!r}")


class RouteExhaustedError(ReasonError):
    """Every attempted provider failed (gateway returned 502); ``detail`` carries
    the per-node failure strings in attempt order."""

    def __init__(self, detail: str) -> None:
        self.detail = detail
        super().__init__(f"all attempted providers failed: {detail}")


class ReasonClient:
    """
    HTTP client for the Mycelium reason gateway (``/gateway/reason/*``).

    Talks to a Rust node compiled with ``--features gateway`` (routing needs no
    local ``llm`` feature — the router's call side is core-only, so a gateway
    node can route inference to models served elsewhere).

    :param host: Hostname or IP of the Mycelium HTTP gateway.
    :param port: HTTP port (default 8080).
    :param timeout: Default request timeout in seconds.
    """

    def __init__(
        self,
        host: str,
        port: int = 8080,
        timeout: float = 30.0,
    ) -> None:
        self._base = f"http://{host}:{port}"
        self._timeout = timeout
        self._client = httpx.AsyncClient(base_url=self._base, timeout=timeout)

    # ── Routed inference (wedge ①) ─────────────────────────────────────────────

    async def route(
        self,
        model: str,
        input: str,
        *,
        context: Optional[dict[str, str]] = None,
        timeout_ms: int = 30_000,
    ) -> dict[str, Any]:
        """
        Route one inference to a healthy ``llm/{model}`` provider, failing over
        down the load-ranked candidate list.

        Returns the result dict::

            {"output": "...", "model_used": "...", "tokens_used": N,
             "provider": "ip:port", "attempt": N}

        ``provider`` is the node that answered; ``attempt`` is 1-based (1 = the
        first candidate answered, higher = failover).

        :param model:      The model id (capability ``llm/{model}``).
        :param input:      The value rendered into the served prompt template.
        :param context:    Optional extra ``{{variable}}`` substitutions.
        :param timeout_ms: Per-attempt RPC timeout in milliseconds.

        :raises NoProviderError:    no live provider serves the model (404).
        :raises RouteExhaustedError: every attempted provider failed (502).
        """
        body: dict[str, Any] = {
            "model": model,
            "input": input,
            "context": context or {},
        }
        resp = await self._client.post(
            "/gateway/reason/route",
            json=body,
            timeout=timeout_ms / 1000.0 + 5.0,
        )
        if resp.status_code == 404:
            raise NoProviderError(model)
        if resp.status_code == 502:
            detail = _error_detail(resp)
            raise RouteExhaustedError(detail)
        resp.raise_for_status()
        return resp.json()

    # ── Fleet-reasoning traces (wedge ②) ───────────────────────────────────────

    async def trace(self, run_id: str) -> dict[str, Any]:
        """
        Fetch the replayed trace for ``run_id`` from this node's KV view
        (gossip-replicated — any node can serve any run's trace).

        Returns ``{"run_id", "events", "narrative"}``. An unknown run yields an
        empty ``events`` list (not an error).
        """
        resp = await self._client.get(f"/gateway/reason/trace/{run_id}")
        resp.raise_for_status()
        return resp.json()

    # ── Content-addressed blob tier ────────────────────────────────────────────

    async def blob_put(self, data: bytes) -> str:
        """
        Store raw bytes in the content-addressed blob tier.

        Returns the blob id (hex content address).
        """
        resp = await self._client.put("/gateway/reason/blob", content=data)
        resp.raise_for_status()
        return resp.json()["id"]

    async def blob_get(self, blob_id: str) -> Optional[bytes]:
        """
        Fetch a blob by its content address (local-then-mesh on the Rust side).

        Returns the bytes, or ``None`` if no node holds the blob (404).
        """
        resp = await self._client.get(f"/gateway/reason/blob/{blob_id}")
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        return resp.content

    async def close(self) -> None:
        """Close the underlying HTTP client."""
        await self._client.aclose()

    async def __aenter__(self) -> "ReasonClient":
        return self

    async def __aexit__(self, *_: Any) -> None:
        await self.close()


def _error_detail(resp: httpx.Response) -> str:
    """Best-effort ``detail`` from a gateway error body (``{"error","detail"}``)."""
    try:
        return str(resp.json().get("detail", ""))
    except (ValueError, KeyError):
        return resp.text
