"""Shared test helpers for overlay integration scenarios."""

from __future__ import annotations

import os
import time
import httpx


NODE_A_HOST    = os.environ.get("NODE_A_HOST",    "overlay-a")
NODE_B_HOST    = os.environ.get("NODE_B_HOST",    "overlay-b")
NODE_C_HOST    = os.environ.get("NODE_C_HOST",    "overlay-c")
NODE_HTTP_PORT = int(os.environ.get("NODE_HTTP_PORT", "8300"))

ALL_HOSTS = [NODE_A_HOST, NODE_B_HOST, NODE_C_HOST]


def node_url(host: str) -> str:
    return f"http://{host}:{NODE_HTTP_PORT}"


def wait_for_health(host: str, timeout: int = 60) -> None:
    url = f"{node_url(host)}/health"
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            r = httpx.get(url, timeout=3.0)
            if r.status_code == 200:
                return
        except Exception:
            pass
        time.sleep(1.0)
    raise RuntimeError(f"Node {host} did not become healthy within {timeout}s")


def wait_for_cluster_ready(hosts: list[str] | None = None, timeout: int = 60) -> None:
    """Wait until gossip has fully converged across all overlay nodes.

    Writes a sentinel KV key from each node, then polls until every node can
    read all sentinels — proving bidirectional gossip connectivity.
    """
    if hosts is None:
        hosts = ALL_HOSTS

    sentinel_prefix = "test/cluster-ready/"

    # Step 1: write one sentinel per node
    for host in hosts:
        sentinel_key = f"{sentinel_prefix}{host}"
        with httpx.Client(base_url=node_url(host), timeout=5.0) as c:
            c.post("/gateway/kv", json={"key": sentinel_key, "value": host})

    # Step 2: wait until every node sees all sentinels
    expected = len(hosts)
    encoded_prefix = sentinel_prefix.replace("/", "%2F")
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            converged = all(
                len(
                    httpx.get(
                        f"{node_url(h)}/gateway/kv/keys?prefix={encoded_prefix}",
                        timeout=3.0,
                    ).json().get("keys", [])
                ) >= expected
                for h in hosts
            )
            if converged:
                return
        except Exception:
            pass
        time.sleep(1.0)
    raise RuntimeError(
        f"Cluster did not converge (sentinel propagation) within {timeout}s"
    )


def wait_for_ready(host: str, timeout: int = 60) -> None:
    """Wait for a single node to be healthy (alias kept for callsite compatibility)."""
    wait_for_health(host, timeout)


def poll_until(condition_fn, timeout: int = 30, interval: float = 0.5) -> bool:
    """Return True if condition_fn() returns True before timeout, else False."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if condition_fn():
                return True
        except Exception:
            pass
        time.sleep(interval)
    return False


def assert_eq(actual, expected, msg: str = "") -> None:
    if actual != expected:
        prefix = f"{msg}: " if msg else ""
        raise AssertionError(f"{prefix}expected {expected!r}, got {actual!r}")


def assert_ge(actual: int, minimum: int, msg: str = "") -> None:
    if actual < minimum:
        prefix = f"{msg}: " if msg else ""
        raise AssertionError(f"{prefix}expected >= {minimum}, got {actual}")
