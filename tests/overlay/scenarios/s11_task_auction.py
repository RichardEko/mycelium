"""S11 — Task Auction (exact-once delivery).

A coordinator appends 5 tasks to the "tasks" log stream on node-a.
Two workers (talking to node-b and node-c respectively) race to claim
each task using the consumer-group API (subscribe_log_group).

Verification:
  - All 5 tasks are received in total across both workers.
  - Each task value is received exactly once (no duplicates).
  - Workers see tasks in HLC order (monotonically non-decreasing HLC).
"""

from __future__ import annotations

import asyncio
import threading
from mycelium import MyceliumAgent, LogEntry
from .helpers import (
    NODE_A_HOST, NODE_B_HOST, NODE_C_HOST, NODE_HTTP_PORT,
    wait_for_cluster_ready,
)

STREAM = "s11-tasks"
TASKS  = [f"task-{i}".encode() for i in range(5)]


def _run_worker(host: str, results: list[LogEntry], n_expected: int) -> None:
    """Subscribe to the consumer group and collect entries until n_expected received."""
    import warnings
    agent = MyceliumAgent(host, NODE_HTTP_PORT)

    async def collect() -> None:
        count = 0
        async for entry in agent.subscribe_log_group(STREAM, "workers"):
            results.append(entry)
            count += 1
            if count >= n_expected:
                break

    # Suppress harmless asyncio SSE-cleanup warnings when the generator is
    # abruptly stopped from a non-async context (no running event loop on exit).
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", RuntimeWarning)
        asyncio.run(collect())


def run() -> None:
    # Step 1 — cluster must already be converged (run.py ensures this before scenarios run)
    wait_for_cluster_ready(timeout=5)

    coord = MyceliumAgent(NODE_A_HOST, NODE_HTTP_PORT)

    # Step 2 — coordinator appends tasks
    for task in TASKS:
        coord.append(STREAM, task)

    # Step 3 — two workers race to consume; each may get 0–5 tasks, total must be 5
    results_b: list[LogEntry] = []
    results_c: list[LogEntry] = []

    # Run both workers concurrently; each stops once it has received its share.
    # We give each worker a limit of 5 (all tasks) — they'll stop when channel
    # dries up naturally after 5 total have been consumed.
    t_b = threading.Thread(target=_run_worker, args=(NODE_B_HOST, results_b, 5), daemon=True)
    t_c = threading.Thread(target=_run_worker, args=(NODE_C_HOST, results_c, 5), daemon=True)
    t_b.start()
    t_c.start()
    t_b.join(timeout=30)
    t_c.join(timeout=30)

    all_results = results_b + results_c

    # Exactly 5 deliveries total
    if len(all_results) != 5:
        raise AssertionError(
            f"Expected 5 total deliveries, got {len(all_results)} "
            f"(worker-b={len(results_b)}, worker-c={len(results_c)})"
        )

    # No duplicates — each task value delivered once
    values = [e.value for e in all_results]
    for task in TASKS:
        count = values.count(task)
        if count != 1:
            raise AssertionError(
                f"Task {task!r} delivered {count} times — expected exactly once"
            )

    # HLC order non-decreasing within each worker's stream
    for label, results in [("worker-b", results_b), ("worker-c", results_c)]:
        for i in range(1, len(results)):
            if results[i].hlc < results[i - 1].hlc:
                raise AssertionError(
                    f"{label}: HLC order violated at index {i}: "
                    f"{results[i-1].hlc} > {results[i].hlc}"
                )
