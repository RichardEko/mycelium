"""
Agentic Flow Networks — Fluid Worker

Advertises all four stage capabilities at startup, then serves incoming RPC
requests concurrently.  The worker has no knowledge of the pipeline topology —
it just responds to whatever skill the coordinator routes to it.

This is the fluid allocation property: no static stage assignment. When the
coordinator is draining stage-C, all 10 workers are handling score() calls.
When it moves to stage-D, the same workers handle aggregate() calls — without
any reconfiguration, restart, or redeployment.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor

from mycelium import MyceliumAgent, RpcRequest
from stages.parse     import parse_article
from stages.enrich    import enrich_article
from stages.score     import score_article
from stages.aggregate import aggregate_article

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("worker")

MYCELIUM_PORT = int(os.environ.get("MYCELIUM_HTTP_PORT", "8300"))

# (stage_in_prefix, stage_out_prefix, sync_handler_fn)
STAGE_MAP: dict[str, tuple[str, str, object]] = {
    "stage_a.parse":     ("stage-a", "stage-b", parse_article),
    "stage_b.enrich":    ("stage-b", "stage-c", enrich_article),
    "stage_c.score":     ("stage-c", "stage-d", score_article),
    "stage_d.aggregate": ("stage-d", "done",    aggregate_article),
}


def wait_ready(agent: MyceliumAgent, timeout: int = 90) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if agent.health().get("status") == "ok":
                return
        except Exception:
            pass
        time.sleep(1)
    raise RuntimeError("Mycelium node not ready within timeout")


async def serve_stage(
    agent:    MyceliumAgent,
    method:   str,
    stage_in: str,
    stage_out: str,
    handler,
    executor: ThreadPoolExecutor,
    node_id:  str,
) -> None:
    """Serve an RPC stream for one stage method, processing one request at a time."""
    loop = asyncio.get_event_loop()
    log.info("serving %s", method)

    async for req in agent.rpc_serve(method):
        try:
            payload  = json.loads(req.payload)
            item_id  = payload.get("id", "unknown")

            # Run the sync handler in a thread so it doesn't block the event loop
            result = await loop.run_in_executor(executor, handler, payload)

            # Write result to the next stage's KV buffer.
            # This write propagates to ALL nodes via gossip — every worker
            # already holds the buffer replica, no external queue needed.
            agent.set(f"pipeline/{stage_out}/{item_id}", json.dumps(result).encode())
            agent.delete(f"pipeline/{stage_in}/{item_id}")
            agent.delete(f"pipeline/claiming/{item_id}")

            agent.rpc_respond(req, json.dumps({"status": "ok", "id": item_id}).encode())
            log.info("  %s  %s → %s", method, item_id, stage_out)

        except Exception as exc:
            log.error("  %s error: %s", method, exc)
            try:
                agent.rpc_respond(req, json.dumps({"status": "error", "error": str(exc)}).encode())
            except Exception:
                pass


async def main() -> None:
    agent = MyceliumAgent("127.0.0.1", MYCELIUM_PORT)
    wait_ready(agent)

    node_id = agent.health().get("node_id", os.environ.get("HOSTNAME", "?"))
    log.info("worker up — node_id=%s", node_id)

    # Advertise all four stage capabilities into the capability ring.
    # The coordinator's resolve_capability() will discover this worker for
    # any of these four namespaces.
    for ns in ("stage_a", "stage_b", "stage_c", "stage_d"):
        agent.advertise_capability(ns, "worker", interval_secs=15)
    log.info("capabilities advertised: stage_a/worker, stage_b/worker, stage_c/worker, stage_d/worker")

    executor = ThreadPoolExecutor(max_workers=8)

    # Serve all four stage methods concurrently.  One item at a time per method —
    # the SSE stream delivers the next request only after rpc_respond() is called.
    await asyncio.gather(*[
        serve_stage(agent, method, stage_in, stage_out, handler, executor, node_id)
        for method, (stage_in, stage_out, handler) in STAGE_MAP.items()
    ])


asyncio.run(main())
