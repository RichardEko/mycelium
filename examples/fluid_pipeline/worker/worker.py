"""
Agentic Flow Networks — Fluid Worker

Two modes, selected by PIPELINE_MODE (default "pull"):

pull (canonical AFN pattern)
    The worker `take()`s from the tuple space when ready — readiness is
    self-announcing, nobody predicts who is free. Fluidity is self-selection:
    each flow loop reads the per-stage depths and takes from the deepest
    stage it can serve, so the pool automatically masses on the bottleneck.
    A>B>C>D exists only as tuple-space stages — this worker has no position
    in the flow.

push (pre-refinement baseline — the coordinator-trap contrast case)
    The worker advertises all four stage capabilities and serves RPCs; the
    coordinator resolves providers and dispatches every item. Kept runnable
    so the two distribution models can be compared on the same stages and
    workload (see README "Comparing the two modes").

Both modes carry the same repertoire: every worker can run every stage.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor

from mycelium import MyceliumAgent
from mycelium.tuple import TupleSpace
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
MODE          = os.environ.get("PIPELINE_MODE", "pull").lower()
TUPLE_NS      = os.environ.get("MYCELIUM_TUPLE_NS", "pipeline")
CONCURRENCY   = int(os.environ.get("WORKER_CONCURRENCY", "2"))

# Ordered pipeline: stage name → (handler, next stage or None for terminal).
PIPELINE: dict[str, tuple[object, str | None]] = {
    "stage-a": (parse_article,     "stage-b"),
    "stage-b": (enrich_article,    "stage-c"),
    "stage-c": (score_article,     "stage-d"),
    "stage-d": (aggregate_article, None),
}

# Push-mode RPC methods (kept identical to the original demo).
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


# ── Pull mode ──────────────────────────────────────────────────────────────────

async def flow_loop(
    agent:    MyceliumAgent,
    ts:       TupleSpace,
    executor: ThreadPoolExecutor,
    loop_id:  int,
) -> None:
    """One fluid flow loop: settle wherever pressure is highest.

    Reads per-stage depths, takes from the deepest stage, processes, and
    advances the item with an atomic complete() (take-ack + next-put in one
    WAL record). Terminal items are ack()ed and marked done in the KV ring.
    """
    loop = asyncio.get_event_loop()
    while True:
        try:
            depths = await ts.depth()
        except Exception as exc:
            log.warning("flow[%d] depth probe failed: %s", loop_id, exc)
            await asyncio.sleep(1.0)
            continue

        # Self-selection: deepest stage this worker can serve wins.
        candidates = [
            (info["depth"], stage)
            for stage, info in depths.items()
            if stage in PIPELINE and info["depth"] > 0
        ]
        if not candidates:
            # Nothing queued anywhere — park briefly on the first stage so a
            # fresh seed wakes us via the blocking take.
            target = "stage-a"
        else:
            target = max(candidates)[1]

        try:
            item_id, payload = await ts.take(target, timeout_secs=5)
        except TimeoutError:
            continue
        except Exception as exc:
            log.warning("flow[%d] take(%s) failed: %s", loop_id, target, exc)
            await asyncio.sleep(0.5)
            continue

        handler, next_stage = PIPELINE[target]
        try:
            item   = json.loads(payload)
            result = await loop.run_in_executor(executor, handler, item)

            if next_stage is not None:
                await ts.complete(item_id, next_stage, json.dumps(result).encode())
            else:
                await ts.ack(item_id)
                # Done marker in the KV ring — the seeder's completion signal
                # and the all-node-visible progress counter.
                agent.set(f"pipeline/done/{result['id']}", json.dumps(result).encode())
            log.info("flow[%d]  %s  %s → %s", loop_id, target, item.get("id", "?"), next_stage or "done")

        except Exception as exc:
            # No ack: the in-flight deadline re-queues the item automatically
            # (at-least-once; stage handlers and the done marker are idempotent).
            log.error("flow[%d] %s failed on %s: %s — item re-queues", loop_id, target, item_id, exc)
            await asyncio.sleep(0.5)


async def main_pull(agent: MyceliumAgent) -> None:
    ts = TupleSpace("127.0.0.1", MYCELIUM_PORT, ns=TUPLE_NS)
    executor = ThreadPoolExecutor(max_workers=max(CONCURRENCY * 2, 4))

    # Advertise the repertoire as one capability — pure observability in pull
    # mode (nobody routes on it; the seeder uses it to count the pool).
    agent.advertise_capability("pipeline", "worker", interval_secs=15)
    log.info("pull worker up — ns=%s, %d flow loops", TUPLE_NS, CONCURRENCY)

    await asyncio.gather(*[
        flow_loop(agent, ts, executor, i) for i in range(CONCURRENCY)
    ])


# ── Push mode (baseline) ───────────────────────────────────────────────────────

async def serve_stage(
    agent:    MyceliumAgent,
    method:   str,
    stage_in: str,
    stage_out: str,
    handler,
    executor: ThreadPoolExecutor,
) -> None:
    """Serve an RPC stream for one stage method, processing one request at a time."""
    loop = asyncio.get_event_loop()
    log.info("serving %s", method)

    async for req in agent.rpc_serve(method):
        try:
            payload  = json.loads(req.payload)
            item_id  = payload.get("id", "unknown")

            result = await loop.run_in_executor(executor, handler, payload)

            # Write result to the next stage's KV buffer; tombstone the input.
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


async def main_push(agent: MyceliumAgent) -> None:
    # Advertise all four stage capabilities; the coordinator's resolver
    # discovers this worker for any of them.
    for ns in ("stage_a", "stage_b", "stage_c", "stage_d"):
        agent.advertise_capability(ns, "worker", interval_secs=15)
    log.info("push worker up — capabilities: stage_a..stage_d / worker")

    executor = ThreadPoolExecutor(max_workers=8)
    await asyncio.gather(*[
        serve_stage(agent, method, stage_in, stage_out, handler, executor)
        for method, (stage_in, stage_out, handler) in STAGE_MAP.items()
    ])


# ── Entry ──────────────────────────────────────────────────────────────────────

async def main() -> None:
    agent = MyceliumAgent("127.0.0.1", MYCELIUM_PORT)
    wait_ready(agent)
    node_id = agent.health().get("node_id", os.environ.get("HOSTNAME", "?"))
    log.info("worker node ready — id=%s mode=%s", node_id, MODE)

    if MODE == "push":
        await main_push(agent)
    else:
        await main_pull(agent)


asyncio.run(main())
