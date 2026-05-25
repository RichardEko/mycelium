"""S13 — Shared Reasoning Log (multi-writer, gossip convergence).

Each of the three overlay nodes appends 3 "observations" to the shared
"observations" stream, for 9 entries total.

Verification:
  - Every node can scan the full log and see all 9 entries.
  - Entries are returned in non-decreasing HLC order on every node.
  - compact_log removes old entries without corrupting the remainder.
"""

from __future__ import annotations

import time
from mycelium import MyceliumAgent
from .helpers import (
    NODE_A_HOST, NODE_B_HOST, NODE_C_HOST, NODE_HTTP_PORT,
    wait_for_cluster_ready, poll_until, assert_ge,
)

STREAM       = "s13-observations"
PER_NODE     = 3
TOTAL        = 9


def run() -> None:
    wait_for_cluster_ready(timeout=5)

    agents = [
        MyceliumAgent(NODE_A_HOST, NODE_HTTP_PORT),
        MyceliumAgent(NODE_B_HOST, NODE_HTTP_PORT),
        MyceliumAgent(NODE_C_HOST, NODE_HTTP_PORT),
    ]
    labels = ["node-a", "node-b", "node-c"]

    # Step 1 — each node appends its own observations
    hlc_markers: list[int] = []
    for i, (agent, label) in enumerate(zip(agents, labels)):
        for j in range(PER_NODE):
            hlc = agent.append(STREAM, f"obs/{label}/{j}".encode())
            hlc_markers.append(hlc)

    # Step 2 — wait for all 9 entries to appear on every node
    def all_converged() -> bool:
        for agent in agents:
            entries = agent.scan_log(STREAM)
            if len(entries) < TOTAL:
                return False
        return True

    if not poll_until(all_converged, timeout=30):
        counts = [len(a.scan_log(STREAM)) for a in agents]
        raise AssertionError(
            f"Log did not converge to {TOTAL} entries within 30s; "
            f"counts per node: {counts}"
        )

    # Step 3 — verify HLC ordering on every node
    for agent, label in zip(agents, labels):
        entries = agent.scan_log(STREAM)
        assert_ge(len(entries), TOTAL, f"{label} entry count")
        for i in range(1, len(entries)):
            if entries[i].hlc < entries[i - 1].hlc:
                raise AssertionError(
                    f"{label}: HLC order violated at index {i}: "
                    f"{entries[i-1].hlc} > {entries[i].hlc}"
                )

    # Step 4 — compact entries older than the median HLC; verify remainder intact
    sorted_hlcs = sorted(hlc_markers)
    compact_before = sorted_hlcs[len(sorted_hlcs) // 2]

    agents[0].compact_log(STREAM, compact_before)

    # After compaction: entries with HLC >= compact_before must still be present
    time.sleep(1.0)  # let tombstone gossip propagate
    remaining = agents[0].scan_log(STREAM, from_hlc=compact_before)
    expected_remaining = sum(1 for h in hlc_markers if h >= compact_before)
    if len(remaining) < expected_remaining:
        raise AssertionError(
            f"compact_log removed too many entries: "
            f"expected >= {expected_remaining} remaining, got {len(remaining)}"
        )

    # Compacted entries are gone from node-a
    compacted = agents[0].scan_log(STREAM, from_hlc=0, to_hlc=compact_before)
    if compacted:
        raise AssertionError(
            f"compact_log left {len(compacted)} entries that should be tombstoned"
        )
