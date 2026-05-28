"""
mycelium.prompt_skill — HTTP gateway client for LLM Prompt Skills.

Wraps the ``/gateway/prompts/*`` and ``/gateway/llm/*`` endpoints exposed
by a Rust Mycelium node compiled with ``--features llm``.

Example::

    import asyncio
    from mycelium.prompt_skill import PromptSkillClient, PromptTemplate

    async def main():
        client = PromptSkillClient("127.0.0.1", 7946)

        # Update a template already registered by a Rust node
        await client.update_prompt("ai", "chat", PromptTemplate(
            system="You are a helpful assistant.",
            user_template="{{input}}",
            max_tokens=512,
            temperature=0.7,
        ))

        # Call a skill
        result = await client.call("ai", "chat", "Hello!")
        print(result["output"])

        # List all visible skills
        for entry in await client.list():
            print(entry["ns"], entry["name"])

    asyncio.run(main())
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional

import httpx


@dataclass
class PromptTemplate:
    """Mirror of the Rust ``PromptTemplate`` struct stored in cluster KV."""

    system: str
    user_template: str
    max_tokens: int = 512
    temperature: float = 0.7
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "system": self.system,
            "user_template": self.user_template,
            "max_tokens": self.max_tokens,
            "temperature": self.temperature,
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "PromptTemplate":
        return cls(
            system=d.get("system", ""),
            user_template=d.get("user_template", ""),
            max_tokens=int(d.get("max_tokens", 512)),
            temperature=float(d.get("temperature", 0.7)),
            metadata=d.get("metadata", {}),
        )


class PromptSkillClient:
    """
    HTTP client for the Mycelium Prompt Skills gateway.

    Talks to ``/gateway/prompts/*`` and ``/gateway/llm/*`` on the Rust node.
    Requires the node to be compiled with ``--features llm``.

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

    # ── Template management ────────────────────────────────────────────────────

    async def list(self) -> list[dict[str, Any]]:
        """
        List all prompt templates visible in the local KV snapshot.

        Returns a list of dicts with keys ``ns``, ``name``, ``max_tokens``,
        ``temperature``, ``metadata``.
        """
        resp = await self._client.get("/gateway/prompts")
        resp.raise_for_status()
        result = resp.json()
        return result if isinstance(result, list) else []

    async def get(self, ns: str, name: str) -> Optional[PromptTemplate]:
        """
        Retrieve a specific prompt template from the local KV snapshot.

        Returns ``None`` if the key does not exist.
        """
        resp = await self._client.get(f"/gateway/prompts/{ns}/{name}")
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        return PromptTemplate.from_dict(resp.json())

    async def update_prompt(self, ns: str, name: str, template: PromptTemplate) -> None:
        """
        Write (or overwrite) a prompt template in the cluster KV.

        The change propagates to all nodes via gossip. Serving nodes read the
        template fresh from KV on every invocation, so the update takes effect
        immediately without restarting any skill handler.
        """
        resp = await self._client.put(
            f"/gateway/prompts/{ns}/{name}",
            json=template.to_dict(),
        )
        resp.raise_for_status()

    async def delete_prompt(self, ns: str, name: str) -> None:
        """
        Tombstone a prompt template in the cluster KV.

        The skill becomes unreachable once all serving nodes' capability entries
        expire (within 30 s). For a graceful drain, drop all ``PromptSkillHandle``
        objects on the Rust side first so capability entries evaporate naturally.
        """
        resp = await self._client.delete(f"/gateway/prompts/{ns}/{name}")
        resp.raise_for_status()

    # ── Skill invocation ───────────────────────────────────────────────────────

    async def call(
        self,
        ns: str,
        name: str,
        input: str,
        context: Optional[dict[str, str]] = None,
        timeout_ms: int = 30_000,
    ) -> dict[str, Any]:
        """
        Invoke a prompt skill via the gateway.

        Resolves a provider for capability ``(ns, name)`` on the Rust node, sends
        an ``llm.invoke`` RPC, and returns the result dict::

            {"output": "...", "provider": "ip:port"}

        On error returns::

            {"error": "...", "detail": "..."}

        :param ns:         Capability namespace (e.g. ``"ai"``).
        :param name:       Capability name (e.g. ``"chat"``).
        :param input:      The ``{{input}}`` value rendered into the template.
        :param context:    Optional extra ``{{variable}}`` substitutions.
        :param timeout_ms: RPC timeout in milliseconds.
        """
        body: dict[str, Any] = {
            "ns": ns,
            "name": name,
            "input": input,
            "context": context or {},
            "timeout_ms": timeout_ms,
        }
        resp = await self._client.post(
            "/gateway/llm/call",
            json=body,
            timeout=timeout_ms / 1000.0 + 5.0,
        )
        resp.raise_for_status()
        return resp.json()

    async def close(self) -> None:
        """Close the underlying HTTP client."""
        await self._client.aclose()

    async def __aenter__(self) -> "PromptSkillClient":
        return self

    async def __aexit__(self, *_: Any) -> None:
        await self.close()
