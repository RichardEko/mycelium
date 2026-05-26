"""
Agentic Flow Networks — Coordinator

Seeds 200 synthetic news articles into the Mycelium KV ring and fans work
out across all four pipeline stages.  Every stage is drained before the next
starts, so the pool of workers visibly shifts: Parse → Enrich → Score →
Aggregate.

Key pattern — the KV ring IS the distributed buffer:
    pipeline/stage-a/{id}   ← seeded here
    pipeline/stage-b/{id}   ← written by stage-A workers
    pipeline/stage-c/{id}   ← written by stage-B workers
    pipeline/stage-d/{id}   ← written by stage-C workers
    pipeline/done/{id}      ← written by stage-D workers (also in Postgres)

Work-item claiming (pipeline/claiming/{id}) prevents double-dispatch when
multiple coordinators run or when a worker crashes mid-task.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor

from mycelium import MyceliumAgent

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [coordinator] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

MYCELIUM_PORT = int(os.environ.get("MYCELIUM_HTTP_PORT", "8300"))
MIN_WORKERS   = int(os.environ.get("MIN_WORKERS", "3"))

# Stage pipeline: (kv_in_prefix, kv_out_prefix, capability_ns, rpc_method)
STAGES = [
    ("stage-a", "stage-b", "stage_a", "stage_a.parse"),
    ("stage-b", "stage-c", "stage_b", "stage_b.enrich"),
    ("stage-c", "stage-d", "stage_c", "stage_c.score"),
    ("stage-d", "done",    "stage_d", "stage_d.aggregate"),
]


# ── Article dataset ────────────────────────────────────────────────────────────

SOURCES = [
    "bbc.co.uk", "guardian.com", "reuters.com", "ft.com",
    "independent.co.uk", "theconversation.com", "carbonbrief.org",
    "plantbasednews.org", "vegnews.com", "foodnavigator.com",
]

ARTICLE_TEMPLATES = [
    # (topic-tag, sentence-1, sentence-2)
    ("plant-based",  "The plant-based food sector expanded by 12% last quarter as consumers cut red meat.",
                     "Major supermarkets are doubling shelf space for vegan alternatives to meet growing demand."),
    ("livestock",    "Livestock agriculture accounts for roughly 14.5% of global greenhouse gas emissions.",
                     "Cattle methane output remains a primary focus of new national climate action plans."),
    ("food-policy",  "Proposed legislation would require carbon-footprint labelling on all packaged foods.",
                     "Food manufacturers are lobbying against mandatory sustainability ratings on packaging."),
    ("alt-protein",  "Fermentation-derived protein startups raised record funding in the first half of the year.",
                     "Mycoprotein and insect protein are gaining traction in European markets."),
    ("climate",      "Extreme droughts in southern Europe have reduced cereal harvests by up to 30%.",
                     "Scientists warn that cascading crop failures could threaten global food security by 2040."),
    ("animal-welfare","New welfare standards for factory-farmed pigs are set to come into force next spring.",
                      "Animal rights groups argue the proposed reforms fall far short of meaningful change."),
    ("health",       "A new longitudinal study links daily red-meat consumption to elevated colon cancer risk.",
                     "Dietitians recommend replacing processed meat with legumes and whole grains."),
    ("economics",    "Rising grain prices are squeezing margins for livestock farmers across the Midwest.",
                     "Some beef producers are diversifying into alternative crops amid shifting consumer tastes."),
    ("corporate",    "Three of the world's largest food conglomerates announced net-zero supply-chain targets.",
                     "Critics question whether voluntary pledges without binding audits will deliver results."),
    ("policy-intl",  "Delegates at the climate summit agreed on non-binding text to address food-system emissions.",
                     "The Plant-Based Treaty coalition called the outcome insufficient but a step forward."),
]


def load_articles() -> list[dict]:
    """Return 200 synthetic news article stubs (20 per topic)."""
    articles: list[dict] = []
    for variation in range(20):
        for idx, (tag, s1, s2) in enumerate(ARTICLE_TEMPLATES):
            art_id = f"article-{len(articles):04d}"
            source = SOURCES[(len(articles)) % len(SOURCES)]
            month  = (len(articles) % 12) + 1
            day    = (len(articles) % 28) + 1
            articles.append({
                "id":     art_id,
                "raw":    f"{s1} {s2} [variant {variation + 1}]",
                "source": source,
                "date":   f"2025-{month:02d}-{day:02d}",
                "tag":    tag,
            })
    return articles[:200]


# ── Startup helpers ────────────────────────────────────────────────────────────

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


def wait_for_workers(agent: MyceliumAgent, min_count: int = MIN_WORKERS, timeout: int = 120) -> int:
    deadline = time.time() + timeout
    while time.time() < deadline:
        providers = agent.resolve_capability("stage_a", "worker")
        if len(providers) >= min_count:
            log.info("%d worker(s) in the capability ring", len(providers))
            return len(providers)
        log.info("waiting for workers… (%d/%d)", len(providers), min_count)
        time.sleep(3)
    raise RuntimeError(f"fewer than {min_count} workers available after {timeout}s")


# ── Work seeding ───────────────────────────────────────────────────────────────

def seed_articles(agent: MyceliumAgent, articles: list[dict]) -> None:
    """Write all articles into pipeline/stage-a/{id} in the KV ring.

    Each write propagates epidemically to every worker node.  By the time
    the first drain_stage call runs, every worker already holds a replica of
    the full stage-a buffer — no separate queue service needed.
    """
    log.info("seeding %d articles into pipeline/stage-a/… (KV ring is the buffer)", len(articles))
    for article in articles:
        agent.set(f"pipeline/stage-a/{article['id']}", json.dumps(article).encode())
    log.info("seed complete — %d work items live in the distributed KV buffer", len(articles))


# ── Stage fan-out ──────────────────────────────────────────────────────────────

async def drain_stage(
    agent:     MyceliumAgent,
    stage_in:  str,
    stage_out: str,
    cap_ns:    str,
    method:    str,
    executor:  ThreadPoolExecutor,
) -> None:
    """Drain all items from *stage_in* by dispatching RPC calls to free workers.

    Uses coordinator-side in-flight tracking so each worker gets at most one
    in-flight task at a time.  Workers write results to pipeline/{stage_out}/{id}
    and tombstone the pipeline/{stage_in}/{id} entry when done.

    Loop terminates when:
      - No unclaimed items remain in pipeline/{stage_in}/
      - No tasks are still in flight
    """
    log.info("==> draining %s → %s  (method=%s)", stage_in, stage_out, method)
    loop        = asyncio.get_event_loop()
    in_flight:  dict[str, asyncio.Task] = {}   # worker_id → task
    dispatched  = 0

    while True:
        # Reap finished tasks
        done_ids = [wid for wid, t in in_flight.items() if t.done()]
        for wid in done_ids:
            del in_flight[wid]

        # Local KV scan — zero network latency (reads from this node's replica)
        all_keys   = agent.keys(prefix=f"pipeline/{stage_in}/")
        claimed_ids = {k.split("/")[-1] for k in agent.keys(prefix="pipeline/claiming/")}
        available  = [k for k in all_keys if k.split("/")[-1] not in claimed_ids]

        if not available and not in_flight:
            log.info("<== %s drained  (%d items dispatched)", stage_in, dispatched)
            break

        if not available:
            await asyncio.sleep(0.05)
            continue

        # Capability resolution — routes to non-opaque, non-busy workers
        providers = agent.resolve_capability(cap_ns, "worker")
        free_ids  = [p["node_id"] for p in providers if p["node_id"] not in in_flight]

        if not free_ids:
            await asyncio.sleep(0.05)
            continue

        item_key  = available[0]
        item_id   = item_key.split("/")[-1]
        worker_id = free_ids[0]

        payload = agent.get(item_key)
        if payload is None:
            # Tombstone raced us — item already processed
            continue

        # Write claim to prevent double-dispatch
        agent.set(f"pipeline/claiming/{item_id}", worker_id.encode())

        async def do_rpc(wid: str, mid: str, pld: bytes) -> None:
            try:
                await loop.run_in_executor(
                    executor,
                    lambda: agent.rpc_call(wid, method, pld, timeout_secs=90),
                )
                log.debug("  ok %s → %s", mid, stage_out)
            except TimeoutError:
                log.warning("  timeout routing %s to %s — will retry", mid, wid[:30])
                agent.delete(f"pipeline/claiming/{mid}")
            except Exception as exc:
                log.warning("  error on %s: %s — will retry", mid, exc)
                agent.delete(f"pipeline/claiming/{mid}")
            finally:
                in_flight.pop(wid, None)

        task = asyncio.create_task(do_rpc(worker_id, item_id, payload))
        in_flight[worker_id] = task
        dispatched += 1

        if dispatched % 20 == 0:
            done_count = len(agent.keys(prefix="pipeline/done/"))
            log.info("  progress: %d dispatched, %d/200 done", dispatched, done_count)


# ── Main ───────────────────────────────────────────────────────────────────────

def main() -> None:
    agent = MyceliumAgent("127.0.0.1", MYCELIUM_PORT)

    wait_ready(agent)
    node_id = agent.health().get("node_id", "?")
    log.info("coordinator node ready — id=%s", node_id)

    wait_for_workers(agent)

    articles = load_articles()
    seed_articles(agent, articles)

    executor = ThreadPoolExecutor(max_workers=30)

    async def run() -> None:
        t0 = time.time()
        for stage_in, stage_out, cap_ns, method in STAGES:
            await drain_stage(agent, stage_in, stage_out, cap_ns, method, executor)
            done = len(agent.keys(prefix="pipeline/done/"))
            workers = len(agent.resolve_capability(cap_ns, "worker"))
            log.info("  pool shifting: %d workers now free for next stage", workers)

        elapsed = time.time() - t0
        total_done = len(agent.keys(prefix="pipeline/done/"))
        log.info(
            "=== pipeline complete: %d/200 articles in %.1fs (%.1f items/s) ===",
            total_done, elapsed, total_done / max(elapsed, 0.001),
        )

    asyncio.run(run())


if __name__ == "__main__":
    main()
