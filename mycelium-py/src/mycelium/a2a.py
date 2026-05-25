"""A2A (Agent-to-Agent) protocol client for Mycelium.

Allows calling external A2A-speaking agents (AutoGen, LangChain, etc.) from
Python without knowing they are on Mycelium, and discovering their skills.

Usage::

    from mycelium.a2a import A2aClient

    client = A2aClient("http://host:8300")
    card   = client.fetch_card()          # discover available skills
    reply  = client.send("compute/gpu", "hello")
    for event in client.stream("compute/gpu", "hello"):
        print(event)
"""

from __future__ import annotations

import json
import uuid
from typing import Dict, Iterator, Optional

import httpx

__all__ = ["A2aClient"]


class A2aClient:
    """HTTP client for A2A-protocol nodes (requires the ``a2a`` cargo feature).

    Parameters
    ----------
    base_url:
        Base URL of the target node's HTTP gateway, e.g. ``"http://localhost:8300"``.
        The ``/.well-known/agent.json`` and ``/a2a`` paths are derived from this.
    timeout_secs:
        Default RPC timeout in seconds (can be overridden per call).
    """

    def __init__(self, base_url: str, *, timeout_secs: float = 30.0) -> None:
        self._base   = base_url.rstrip("/")
        self._timeout = timeout_secs
        self._http   = httpx.Client(timeout=timeout_secs)

    # ── Discovery ─────────────────────────────────────────────────────────────

    def fetch_card(self) -> Dict:
        """Fetch the AgentCard from ``/.well-known/agent.json``.

        Returns
        -------
        dict
            Parsed AgentCard JSON with at minimum ``name``, ``url``, and ``skills``.

        Raises
        ------
        httpx.HTTPStatusError
            If the server returns a non-2xx status.
        """
        resp = self._http.get(f"{self._base}/.well-known/agent.json")
        resp.raise_for_status()
        return resp.json()

    # ── Synchronous task dispatch ─────────────────────────────────────────────

    def send(
        self,
        skill_id: str,
        message:  str,
        *,
        timeout_secs: Optional[float] = None,
        task_id: Optional[str] = None,
    ) -> str:
        """Send a ``tasks/send`` request and return the reply text.

        Parameters
        ----------
        skill_id:
            Skill identifier in ``"ns/name"`` format, e.g. ``"compute/gpu"``.
        message:
            Plain-text message to the skill.
        timeout_secs:
            Override the per-client default timeout.
        task_id:
            Optional explicit task ID; auto-generated if omitted.

        Returns
        -------
        str
            The first text part of the completed task's first artifact.

        Raises
        ------
        KeyError
            If the server responds with a JSON-RPC error.
        """
        tid      = task_id or str(uuid.uuid4())
        timeout  = timeout_secs or self._timeout
        payload  = {
            "jsonrpc": "2.0",
            "id":      1,
            "method":  "tasks/send",
            "params":  {
                "id":       tid,
                "skillId":  skill_id,
                "message":  {"role": "user", "parts": [{"type": "text", "text": message}]},
            },
        }
        resp = self._http.post(
            f"{self._base}/a2a",
            json=payload,
            timeout=timeout + 5.0,  # network headroom
        )
        resp.raise_for_status()
        body = resp.json()
        if "error" in body:
            raise KeyError(f"A2A error {body['error']['code']}: {body['error']['message']}")
        task = body.get("result", {})
        return _extract_text(task)

    # ── SSE streaming ─────────────────────────────────────────────────────────

    def stream(
        self,
        skill_id: str,
        message:  str,
        *,
        timeout_secs: Optional[float] = None,
        task_id: Optional[str] = None,
    ) -> Iterator[Dict]:
        """Send a ``tasks/sendSubscribe`` request and yield SSE status events.

        Each yielded dict is a deserialized ``task_status_update`` SSE event,
        e.g. ``{"id": "…", "status": {"state": "working"}}``.

        The iterator terminates when the stream closes (completed or failed).

        Parameters
        ----------
        skill_id:
            Skill identifier, e.g. ``"llm/chat"``.
        message:
            Plain-text input.
        timeout_secs:
            Override the streaming read timeout.
        task_id:
            Optional explicit task ID.

        Yields
        ------
        dict
            Task status update events.
        """
        tid     = task_id or str(uuid.uuid4())
        timeout = timeout_secs or self._timeout
        payload = {
            "jsonrpc": "2.0",
            "id":      1,
            "method":  "tasks/sendSubscribe",
            "params":  {
                "id":       tid,
                "skillId":  skill_id,
                "message":  {"role": "user", "parts": [{"type": "text", "text": message}]},
            },
        }
        with httpx.stream(
            "POST",
            f"{self._base}/a2a",
            json=payload,
            timeout=timeout + 5.0,
        ) as resp:
            resp.raise_for_status()
            for line in resp.iter_lines():
                if line.startswith("data:"):
                    data = line[len("data:"):].strip()
                    if data:
                        try:
                            yield json.loads(data)
                        except json.JSONDecodeError:
                            pass

    def close(self) -> None:
        """Close the underlying HTTP client."""
        self._http.close()

    def __enter__(self) -> "A2aClient":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()


# ── Helpers ───────────────────────────────────────────────────────────────────

def _extract_text(task: Dict) -> str:
    """Return the first text part from a completed task artifact, or empty string."""
    artifacts = task.get("artifacts", [])
    if not artifacts:
        return ""
    parts = artifacts[0].get("parts", [])
    if not parts:
        return ""
    return parts[0].get("text", "")
