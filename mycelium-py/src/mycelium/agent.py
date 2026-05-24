"""
mycelium.agent — HTTP gateway client for the Mycelium gossip mesh.

Connects to a running Rust Mycelium node's HTTP gateway (``/gateway/*``
endpoints) and exposes a Python-native API for capability advertisement,
signal emission, signal subscription, demand pressure, RPC calls, KV
operations, scatter-gather, and mailbox event delivery.

The gateway is the sidecar described in the Layer 4 architecture:

    Python agent  →  HTTP (loopback, ~1 ms)  →  Mycelium Rust node
                         /gateway/capability/*
                         /gateway/signal/*
                         /gateway/demand
                         /gateway/rpc/*
                         /gateway/scatter
                         /gateway/kv[/keys]
                         /gateway/mailbox/*

Example::

    import asyncio
    from mycelium import MyceliumAgent

    async def main():
        agent = MyceliumAgent("127.0.0.1", 7946)

        handle = agent.advertise_capability("compute", "gpu",
            interval_secs=30,
            attributes={"model": "A100"},
            authorized_callers=["orchestrator"],
        )

        providers = agent.resolve_capability("compute", "gpu",
            caller_id="orchestrator")
        print(providers)

        async for signal in agent.on_signal("render-job"):
            print("received:", signal)
            break  # handle one then stop

        handle.drop()

    asyncio.run(main())
"""

from __future__ import annotations

import asyncio
import base64
from dataclasses import dataclass, field
from typing import Any, AsyncIterator, Optional

import httpx
from httpx_sse import aconnect_sse


@dataclass
class CapabilityHandle:
    """Returned by :meth:`MyceliumAgent.advertise_capability`.

    Drop this handle (call :meth:`drop`) to retract the advertisement and
    tombstone the capability in the mesh. Use as a context manager for
    automatic cleanup::

        async with agent.advertise_capability("compute", "gpu") as handle:
            ...  # capability is live here
        # tombstoned here
    """

    _agent:     "MyceliumAgent"
    handle_id:  str

    def drop(self) -> None:
        """Retract the advertised capability synchronously."""
        import httpx as _httpx
        with _httpx.Client(base_url=self._agent._base_url, timeout=5.0) as c:
            c.delete(f"/gateway/capability/{self.handle_id}")

    async def adrop(self) -> None:
        """Retract the advertised capability asynchronously."""
        async with httpx.AsyncClient(base_url=self._agent._base_url, timeout=5.0) as c:
            await c.delete(f"/gateway/capability/{self.handle_id}")

    def __enter__(self) -> "CapabilityHandle":
        return self

    def __exit__(self, *_: Any) -> None:
        self.drop()

    async def __aenter__(self) -> "CapabilityHandle":
        return self

    async def __aexit__(self, *_: Any) -> None:
        await self.adrop()


@dataclass
class Signal:
    """A signal received from the mesh via :meth:`MyceliumAgent.on_signal`."""

    kind:        str
    sender:      str
    payload:     bytes
    nonce:       int


@dataclass
class DemandStatus:
    """Demand-pressure snapshot returned by :meth:`MyceliumAgent.demand`."""

    ns:               str
    name:             str
    providers:        int
    requirers:        int
    demand_pressure:  float  # requirers / max(providers, 1)


@dataclass
class RpcRequest:
    """An incoming RPC request received via :meth:`MyceliumAgent.rpc_serve`.

    Pass this to :meth:`MyceliumAgent.rpc_respond` to complete the round-trip.
    """

    kind:        str
    nonce_hex:   str
    sender:      str
    payload:     bytes


@dataclass
class MailboxEvent:
    """An event received from this node's mailbox via :meth:`MyceliumAgent.mailbox`."""

    kind:        str
    sender:      str
    payload:     bytes


class MyceliumAgent:
    """HTTP gateway client for a Mycelium mesh node.

    All operations go through the node's HTTP gateway (``/gateway/*``
    endpoints). The gateway is started automatically when the node is
    configured with an ``http_port``.

    Args:
        host: Gateway host (usually ``"127.0.0.1"``).
        port: HTTP port the Mycelium node is listening on.
        timeout: Default request timeout in seconds.
    """

    def __init__(
        self,
        host:    str = "127.0.0.1",
        port:    int = 7946,
        timeout: float = 30.0,
    ) -> None:
        self._base_url = f"http://{host}:{port}"
        self._timeout  = timeout

    # ── Capability advertisement ────────────────────────────────────────────

    def advertise_capability(
        self,
        ns:                 str,
        name:               str,
        *,
        interval_secs:      int                    = 30,
        attributes:         dict[str, Any]         | None = None,
        authorized_callers: list[str]              | None = None,
    ) -> CapabilityHandle:
        """Advertise a capability on the mesh.

        The capability is re-asserted on every ``interval_secs`` tick so late
        joiners discover it.  Drop the returned :class:`CapabilityHandle` to
        tombstone the advertisement.

        Args:
            ns:                 Capability namespace (e.g. ``"compute"``).
            name:               Capability name (e.g. ``"gpu"``).
            interval_secs:      Re-assertion interval.
            attributes:         Typed key-value annotations.
            authorized_callers: If non-empty, only callers whose identity is
                                in this list will see the capability via
                                :meth:`resolve_capability`.  Leave empty for
                                unrestricted access.

        Returns:
            A :class:`CapabilityHandle`; call :meth:`CapabilityHandle.drop`
            or use as a context manager to retract.
        """
        body: dict[str, Any] = {"ns": ns, "name": name, "interval_secs": interval_secs}
        if attributes:
            body["attributes"] = attributes
        if authorized_callers:
            body["authorized_callers"] = authorized_callers

        with httpx.Client(base_url=self._base_url, timeout=self._timeout) as c:
            resp = c.post("/gateway/capability/advertise", json=body)
            resp.raise_for_status()
            handle_id = resp.json()["handle_id"]

        return CapabilityHandle(_agent=self, handle_id=handle_id)

    # ── Capability resolution ───────────────────────────────────────────────

    def resolve_capability(
        self,
        ns:        str,
        name:      str,
        *,
        caller_id: str | None = None,
    ) -> list[dict[str, Any]]:
        """Return all live providers matching ``(ns, name)``.

        If ``caller_id`` is given, capabilities with a non-empty
        ``authorized_callers`` list are filtered to only those that include
        this identity — preventing token-bloat and confused-deputy exposure
        in LLM tool-discovery flows.

        Returns:
            List of provider dicts: ``{"node_id", "ns", "name", "attributes"}``.
        """
        params: dict[str, str] = {"ns": ns, "name": name}
        if caller_id is not None:
            params["caller_id"] = caller_id

        with httpx.Client(base_url=self._base_url, timeout=self._timeout) as c:
            resp = c.get("/gateway/capability/resolve", params=params)
            resp.raise_for_status()
            return resp.json()["providers"]

    # ── Signal emission ─────────────────────────────────────────────────────

    def emit(
        self,
        kind:        str,
        payload:     bytes = b"",
        *,
        scope:       str = "system",
    ) -> bool:
        """Emit a signal into the mesh.

        Args:
            kind:    Signal kind string (e.g. ``"render-job"``).
            payload: Raw bytes payload.
            scope:   ``"system"``, ``"group:NAME"``, or ``"node:IP:PORT"``.

        Returns:
            ``True`` if the signal was queued; ``False`` if the gossip shard
            was full (local delivery still occurred).
        """
        body = {
            "kind":        kind,
            "scope":       scope,
            "payload_b64": base64.b64encode(payload).decode(),
        }
        with httpx.Client(base_url=self._base_url, timeout=self._timeout) as c:
            resp = c.post("/gateway/signal/emit", json=body)
            resp.raise_for_status()
            return bool(resp.json().get("ok", False))

    # ── Signal subscription (SSE) ───────────────────────────────────────────

    async def on_signal(self, kind: str) -> AsyncIterator[Signal]:
        """Async generator that yields admitted signals of ``kind``.

        Streams Server-Sent Events from the gateway until the caller breaks
        the loop or the connection is closed::

            async for sig in agent.on_signal("render-job"):
                result = await process(sig.payload)
                if done:
                    break
        """
        url = f"{self._base_url}/gateway/signal/sse/{kind}"
        async with httpx.AsyncClient(timeout=None) as client:
            async with aconnect_sse(client, "GET", url) as event_source:
                async for event in event_source.aiter_sse():
                    import json as _json
                    data   = _json.loads(event.data)
                    payload = base64.b64decode(data.get("payload_b64", ""))
                    yield Signal(
                        kind    = event.event or kind,
                        sender  = data.get("sender", ""),
                        payload = payload,
                        nonce   = int(data.get("nonce", 0)),
                    )

    # ── Demand pressure ─────────────────────────────────────────────────────

    def demand(self, ns: str, name: str) -> DemandStatus:
        """Return the demand-pressure snapshot for a capability filter.

        ``demand_pressure > 1.0`` means more requirers than providers —
        a supply gap that may warrant spinning up additional nodes.
        """
        with httpx.Client(base_url=self._base_url, timeout=self._timeout) as c:
            resp = c.get("/gateway/demand", params={"ns": ns, "name": name})
            resp.raise_for_status()
            data = resp.json()
            return DemandStatus(
                ns              = data["ns"],
                name            = data["name"],
                providers       = data["providers"],
                requirers       = data["requirers"],
                demand_pressure = data["demand_pressure"],
            )

    # ── RPC call ────────────────────────────────────────────────────────────

    def rpc_call(
        self,
        target:       str,
        method:       str,
        payload:      bytes          = b"",
        *,
        timeout_secs: int            = 30,
    ) -> bytes:
        """Blocking RPC call to a named node.

        Args:
            target:       Node ID string (``"IP:PORT"``).
            method:       Signal kind used for the RPC (e.g. ``"mcp.invoke"``).
            payload:      Request payload bytes.
            timeout_secs: Maximum wait time.

        Returns:
            Response payload bytes.

        Raises:
            TimeoutError: If the node does not respond within ``timeout_secs``.
            httpx.HTTPStatusError: For other HTTP errors.
        """
        body = {
            "target":       target,
            "method":       method,
            "payload_b64":  base64.b64encode(payload).decode(),
            "timeout_secs": timeout_secs,
        }
        with httpx.Client(base_url=self._base_url, timeout=timeout_secs + 5.0) as c:
            resp = c.post("/gateway/rpc/call", json=body)
            if resp.status_code == 504:
                raise TimeoutError(f"rpc_call to {target} timed out after {timeout_secs}s")
            resp.raise_for_status()
            data = resp.json()
            if not data.get("ok"):
                raise RuntimeError(f"rpc_call failed: {data.get('error')}")
            return base64.b64decode(data.get("result_b64", ""))

    # ── KV store ────────────────────────────────────────────────────────────

    def get(self, key: str) -> bytes | None:
        """Read a KV entry by key.

        Returns the raw bytes value, or ``None`` when the key is absent or
        tombstoned.
        """
        with httpx.Client(base_url=self._base_url, timeout=self._timeout) as c:
            resp = c.get("/gateway/kv", params={"key": key})
            resp.raise_for_status()
            data = resp.json()
            if not data.get("found"):
                return None
            return base64.b64decode(data.get("value_b64", ""))

    def set(self, key: str, value: bytes) -> None:
        """Write a KV entry.

        The write is gossiped to all peers. Existing values are overwritten
        when the local HLC timestamp is strictly greater (LWW semantics).
        """
        body = {
            "key":       key,
            "value_b64": base64.b64encode(value).decode(),
        }
        with httpx.Client(base_url=self._base_url, timeout=self._timeout) as c:
            c.post("/gateway/kv", json=body).raise_for_status()

    def delete(self, key: str) -> None:
        """Tombstone a KV entry.

        The tombstone is gossiped so all live nodes remove the key.
        """
        with httpx.Client(base_url=self._base_url, timeout=self._timeout) as c:
            c.delete("/gateway/kv", params={"key": key}).raise_for_status()

    def keys(self, prefix: str | None = None) -> list[str]:
        """Return all live KV keys, optionally filtered by prefix.

        Args:
            prefix: When given, only keys starting with this string are returned.
        """
        params: dict[str, str] = {}
        if prefix is not None:
            params["prefix"] = prefix
        with httpx.Client(base_url=self._base_url, timeout=self._timeout) as c:
            resp = c.get("/gateway/kv/keys", params=params)
            resp.raise_for_status()
            return resp.json()["keys"]

    def scan_prefix(self, prefix: str) -> dict[str, bytes]:
        """Return all live KV entries whose key starts with ``prefix``.

        Returns a ``{key: value_bytes}`` dict. Requires one HTTP call per key
        (keys + individual gets) — use sparingly for large keyspaces.
        """
        result: dict[str, bytes] = {}
        for key in self.keys(prefix=prefix):
            val = self.get(key)
            if val is not None:
                result[key] = val
        return result

    # ── RPC serve / respond ─────────────────────────────────────────────────

    async def rpc_serve(self, kind: str) -> "AsyncIterator[RpcRequest]":
        """Async generator that yields incoming RPC requests of ``kind``.

        For each yielded :class:`RpcRequest`, call :meth:`rpc_respond` to
        complete the round-trip before processing the next request::

            async for req in agent.rpc_serve("my.method"):
                result = process(req.payload)
                agent.rpc_respond(req, result)
        """
        url = f"{self._base_url}/gateway/rpc/serve/{kind}"
        async with httpx.AsyncClient(timeout=None) as client:
            async with aconnect_sse(client, "GET", url) as event_source:
                async for event in event_source.aiter_sse():
                    import json as _json
                    data    = _json.loads(event.data)
                    payload = base64.b64decode(data.get("payload_b64", ""))
                    yield RpcRequest(
                        kind      = event.event or kind,
                        nonce_hex = data.get("nonce_hex", ""),
                        sender    = data.get("sender", ""),
                        payload   = payload,
                    )

    def rpc_respond(self, request: "RpcRequest", result: bytes = b"") -> None:
        """Send a reply to an incoming RPC request.

        Args:
            request: The :class:`RpcRequest` received from :meth:`rpc_serve`.
            result:  Raw bytes reply payload.
        """
        body = {
            "nonce_hex":  request.nonce_hex,
            "sender":     request.sender,
            "result_b64": base64.b64encode(result).decode(),
        }
        with httpx.Client(base_url=self._base_url, timeout=self._timeout) as c:
            c.post("/gateway/rpc/respond", json=body).raise_for_status()

    # ── Scatter-gather ──────────────────────────────────────────────────────

    def scatter_gather(
        self,
        targets:      list[str],
        method:       str,
        payload:      bytes = b"",
        *,
        min_ok:       int   = 1,
        timeout_secs: int   = 10,
    ) -> list[dict[str, Any]]:
        """Fan-out an RPC to multiple targets and collect at least ``min_ok`` replies.

        Args:
            targets:      List of target node IDs (``"IP:PORT"``).
            method:       Signal kind (e.g. ``"echo"``).
            payload:      Request payload bytes.
            min_ok:       Minimum number of successful replies to wait for.
            timeout_secs: Maximum wait time.

        Returns:
            List of ``{"sender": "IP:PORT", "result_b64": "…"}`` dicts.

        Raises:
            TimeoutError: Fewer than ``min_ok`` replies arrived.
            httpx.HTTPStatusError: For other HTTP errors.
        """
        body = {
            "targets":      targets,
            "method":       method,
            "payload_b64":  base64.b64encode(payload).decode(),
            "timeout_secs": timeout_secs,
            "min_ok":       min_ok,
        }
        with httpx.Client(base_url=self._base_url, timeout=timeout_secs + 5.0) as c:
            resp = c.post("/gateway/scatter", json=body)
            if resp.status_code == 504:
                raise TimeoutError(
                    f"scatter_gather: fewer than {min_ok} replies in {timeout_secs}s"
                )
            resp.raise_for_status()
            data = resp.json()
            if not data.get("ok"):
                raise TimeoutError(
                    f"scatter_gather: {data.get('error', 'insufficient replies')}"
                )
            return [
                {
                    "sender":    r["sender"],
                    "result":    base64.b64decode(r.get("result_b64", "")),
                }
                for r in data.get("replies", [])
            ]

    # ── Mailbox ─────────────────────────────────────────────────────────────

    async def mailbox(self, kind: str) -> "AsyncIterator[MailboxEvent]":
        """Async generator that yields mailbox events of ``kind`` for this node.

        Events are delivered in HLC-causal order and tombstoned after delivery
        (at-least-once within the gossip TTL window)::

            async for event in agent.mailbox("task.result"):
                print(event.sender, event.payload)
        """
        url = f"{self._base_url}/gateway/mailbox/{kind}"
        async with httpx.AsyncClient(timeout=None) as client:
            async with aconnect_sse(client, "GET", url) as event_source:
                async for event in event_source.aiter_sse():
                    import json as _json
                    data    = _json.loads(event.data)
                    payload = base64.b64decode(data.get("payload_b64", ""))
                    yield MailboxEvent(
                        kind    = data.get("kind", kind),
                        sender  = data.get("sender", ""),
                        payload = payload,
                    )

    def deliver_event(
        self,
        target:  str,
        kind:    str,
        payload: bytes = b"",
    ) -> None:
        """Deliver a mailbox event to a target node.

        The event is written to the gossip KV store at
        ``mailbox/{target}/{kind}/{hlc_ts}`` and gossiped to all peers.
        The target's :meth:`mailbox` watcher picks it up and tombstones it
        on delivery (at-least-once within the gossip TTL).

        Args:
            target:  Target node ID (``"IP:PORT"``).
            kind:    Event kind string.
            payload: Raw bytes payload.
        """
        body = {
            "target":      target,
            "kind":        kind,
            "payload_b64": base64.b64encode(payload).decode(),
        }
        with httpx.Client(base_url=self._base_url, timeout=self._timeout) as c:
            c.post("/gateway/mailbox/deliver", json=body).raise_for_status()

    # ── Health / introspection ──────────────────────────────────────────────

    def health(self) -> dict[str, Any]:
        """Return the node's health response."""
        with httpx.Client(base_url=self._base_url, timeout=5.0) as c:
            return c.get("/health").raise_for_status().json()

    def stats(self) -> dict[str, Any]:
        """Return the node's stats (store entries, dropped frames, etc.)."""
        with httpx.Client(base_url=self._base_url, timeout=5.0) as c:
            return c.get("/stats").raise_for_status().json()
