"""
Agentic Flow Networks — Seeder / Sink (pull mode) or Coordinator (push mode)

Two modes, selected by PIPELINE_MODE (default "pull"):

pull (canonical AFN pattern)
    This process is NOT a coordinator. It is an edge client: it puts the
    seed items into the tuple space's first stage, then watches the done
    markers accumulate. All distribution decisions are made by the workers
    themselves — they take() when ready, from whichever stage is deepest.
    The pipeline A>B>C>D lives in the tuple-space stages, not here.

push (pre-refinement baseline — the coordinator-trap contrast case)
    The original demo: seed the KV ring, then drain each stage by resolving
    free workers and dispatching RPCs to them. Every item flows through this
    process's decisions — the architecture Paper 1 names "the coordinator
    trap", kept runnable as the comparison baseline.

The node this process talks to hosts the tuple-space primary in pull mode
(MYCELIUM_TUPLE_ROLE=primary on the sidecar binary).
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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [seeder] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

MYCELIUM_PORT = int(os.environ.get("MYCELIUM_HTTP_PORT", "8300"))
MODE          = os.environ.get("PIPELINE_MODE", "pull").lower()
TUPLE_NS      = os.environ.get("MYCELIUM_TUPLE_NS", "pipeline")
MIN_WORKERS   = int(os.environ.get("MIN_WORKERS", "3"))
ITEM_COUNT    = int(os.environ.get("ITEM_COUNT", "200"))
RUN_TIMEOUT   = int(os.environ.get("PIPELINE_TIMEOUT_SECS", "600"))

# Push-mode stage table: (kv_in_prefix, kv_out_prefix, capability_ns, rpc_method)
STAGES = [
    ("stage-a", "stage-b", "stage_a", "stage_a.parse"),
    ("stage-b", "stage-c", "stage_b", "stage_b.enrich"),
    ("stage-c", "stage-d", "stage_c", "stage_c.score"),
    ("stage-d", "done",    "stage_d", "stage_d.aggregate"),
]

PULL_STAGES = ["stage-a", "stage-b", "stage-c", "stage-d"]


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


def load_articles(count: int = ITEM_COUNT) -> list[dict]:
    """Return `count` synthetic news article stubs (round-robin over templates)."""
    articles: list[dict] = []
    variation = 0
    while len(articles) < count:
        for tag, s1, s2 in ARTICLE_TEMPLATES:
            if len(articles) >= count:
                break
            art_id = f"article-{len(articles):04d}"
            source = SOURCES[len(articles) % len(SOURCES)]
            month  = (len(articles) % 12) + 1
            day    = (len(articles) % 28) + 1
            articles.append({
                "id":     art_id,
                "raw":    f"{s1} {s2} [variant {variation + 1}]",
                "source": source,
                "date":   f"2025-{month:02d}-{day:02d}",
                "tag":    tag,
            })
        variation += 1
    return articles


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


def wait_for_workers(agent: MyceliumAgent, cap_ns: str, min_count: int = MIN_WORKERS, timeout: int = 120) -> int:
    deadline = time.time() + timeout
    while time.time() < deadline:
        providers = agent.resolve_capability(cap_ns, "worker")
        if len(providers) >= min_count:
            log.info("%d worker(s) in the capability ring", len(providers))
            return len(providers)
        log.info("waiting for workers… (%d/%d)", len(providers), min_count)
        time.sleep(3)
    raise RuntimeError(f"fewer than {min_count} workers available after {timeout}s")


# ── Pull mode: seed, then watch ────────────────────────────────────────────────

async def run_pull(agent: MyceliumAgent) -> None:
    ts = TupleSpace("127.0.0.1", MYCELIUM_PORT, ns=TUPLE_NS)

    # Optional but tidy: wait for the pool before seeding so the demo logs
    # show the fluid behaviour from item 1. Items would happily wait in the
    # space otherwise — pull has no startup ordering requirement.
    wait_for_workers(agent, "pipeline")

    articles = load_articles()
    log.info("seeding %d articles into tuple stage-a (ns=%s)", len(articles), TUPLE_NS)
    t0 = time.time()
    for article in articles:
        await ts.put("stage-a", json.dumps(article).encode(), backpressure="block")
    log.info("seed complete — workers are already draining (no dispatch step exists)")

    # Sink: watch done markers; narrate the pressure front A→B→C→D.
    deadline = time.time() + RUN_TIMEOUT
    last_log = 0.0
    while time.time() < deadline:
        done = len(agent.keys(prefix="pipeline/done/"))
        if done >= len(articles):
            break
        if time.time() - last_log >= 2.0:
            try:
                depths = await ts.depth()
                front = "  ".join(
                    f"{s}={depths.get(s, {}).get('depth', 0)}"
                    f"(+{depths.get(s, {}).get('inflight', 0)} inflight)"
                    for s in PULL_STAGES
                )
                log.info("  pressure: %s   done=%d/%d", front, done, len(articles))
            except Exception:
                pass
            last_log = time.time()
        await asyncio.sleep(0.25)

    elapsed = time.time() - t0
    done = len(agent.keys(prefix="pipeline/done/"))
    if done < len(articles):
        raise RuntimeError(f"pipeline incomplete: {done}/{len(articles)} after {RUN_TIMEOUT}s")
    log.info(
        "=== pipeline complete: %d/%d articles in %.1fs (%.1f items/s) ===",
        done, len(articles), elapsed, done / max(elapsed, 0.001),
    )


# ── Push mode: the original coordinator (baseline) ─────────────────────────────

def seed_articles_kv(agent: MyceliumAgent, articles: list[dict]) -> None:
    log.info("seeding %d articles into pipeline/stage-a/… (KV ring is the buffer)", len(articles))
    for article in articles:
        agent.set(f"pipeline/stage-a/{article['id']}", json.dumps(article).encode())
    log.info("seed complete — %d work items live in the distributed KV buffer", len(articles))


async def drain_stage(
    agent:     MyceliumAgent,
    stage_in:  str,
    stage_out: str,
    cap_ns:    str,
    method:    str,
    executor:  ThreadPoolExecutor,
) -> None:
    """Drain all items from *stage_in* by dispatching RPC calls to free workers."""
    log.info("==> draining %s → %s  (method=%s)", stage_in, stage_out, method)
    loop        = asyncio.get_event_loop()
    in_flight:  dict[str, asyncio.Task] = {}   # worker_id → task
    dispatched  = 0

    while True:
        done_ids = [wid for wid, t in in_flight.items() if t.done()]
        for wid in done_ids:
            del in_flight[wid]

        all_keys    = agent.keys(prefix=f"pipeline/{stage_in}/")
        claimed_ids = {k.split("/")[-1] for k in agent.keys(prefix="pipeline/claiming/")}
        available   = [k for k in all_keys if k.split("/")[-1] not in claimed_ids]

        if not available and not in_flight:
            log.info("<== %s drained  (%d items dispatched)", stage_in, dispatched)
            break

        if not available:
            await asyncio.sleep(0.05)
            continue

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
            continue  # tombstone raced us — already processed

        agent.set(f"pipeline/claiming/{item_id}", worker_id.encode())

        async def do_rpc(wid: str, mid: str, pld: bytes) -> None:
            try:
                await loop.run_in_executor(
                    executor,
                    lambda: agent.rpc_call(wid, method, pld, timeout_secs=90),
                )
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
            log.info("  progress: %d dispatched, %d/%d done", dispatched, done_count, ITEM_COUNT)


async def run_push(agent: MyceliumAgent) -> None:
    wait_for_workers(agent, "stage_a")

    articles = load_articles()
    seed_articles_kv(agent, articles)

    executor = ThreadPoolExecutor(max_workers=30)
    t0 = time.time()
    for stage_in, stage_out, cap_ns, method in STAGES:
        await drain_stage(agent, stage_in, stage_out, cap_ns, method, executor)
        workers = len(agent.resolve_capability(cap_ns, "worker"))
        log.info("  pool shifting: %d workers now free for next stage", workers)

    elapsed = time.time() - t0
    total_done = len(agent.keys(prefix="pipeline/done/"))
    if total_done < len(articles):
        raise RuntimeError(f"pipeline incomplete: {total_done}/{len(articles)}")
    log.info(
        "=== pipeline complete: %d/%d articles in %.1fs (%.1f items/s) ===",
        total_done, len(articles), elapsed, total_done / max(elapsed, 0.001),
    )


# ── Main ───────────────────────────────────────────────────────────────────────

def main() -> None:
    agent = MyceliumAgent("127.0.0.1", MYCELIUM_PORT)

    wait_ready(agent)
    node_id = agent.health().get("node_id", "?")
    log.info("node ready — id=%s mode=%s items=%d", node_id, MODE, ITEM_COUNT)

    # Outer watchdog so a stalled run exits non-zero instead of hanging the
    # CI harness (run_pull also enforces RUN_TIMEOUT internally on the sink).
    runner = run_push(agent) if MODE == "push" else run_pull(agent)
    asyncio.run(asyncio.wait_for(runner, timeout=RUN_TIMEOUT + 30))


if __name__ == "__main__":
    main()
