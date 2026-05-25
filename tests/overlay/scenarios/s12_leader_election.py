"""S12 — Leader Election + Linearizable Config.

All three nodes concurrently call elect_leader("demo") via the gossip consensus.
The cluster must converge on exactly one leader — all nodes must return the same
leader string.

The elected leader then writes a config value via consistent_set. All three
nodes verify they see the same value via consistent_get.
"""

from __future__ import annotations

import concurrent.futures
from mycelium import MyceliumAgent
from .helpers import (
    NODE_A_HOST, NODE_B_HOST, NODE_C_HOST, NODE_HTTP_PORT,
    wait_for_cluster_ready, poll_until, assert_eq,
)

GROUP  = "s12-demo"
CONFIG_KEY = "s12/config/endpoint"
CONFIG_VAL = b"https://api.example.com/v2"


def _elect(host: str) -> str:
    agent = MyceliumAgent(host, NODE_HTTP_PORT)
    return agent.elect_leader(GROUP)


def run() -> None:
    # Cluster is already converged — quick re-check (run.py waits at startup)
    wait_for_cluster_ready(timeout=5)

    agents = {
        NODE_A_HOST: MyceliumAgent(NODE_A_HOST, NODE_HTTP_PORT),
        NODE_B_HOST: MyceliumAgent(NODE_B_HOST, NODE_HTTP_PORT),
        NODE_C_HOST: MyceliumAgent(NODE_C_HOST, NODE_HTTP_PORT),
    }

    # Step 1 — all three nodes call elect_leader concurrently
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
        futures = {h: pool.submit(_elect, h) for h in agents}
        leaders = {h: f.result(timeout=30) for h, f in futures.items()}

    # All must agree on the same leader
    unique_leaders = set(leaders.values())
    if len(unique_leaders) != 1:
        raise AssertionError(
            f"Nodes disagree on leader: {leaders}"
        )

    elected = next(iter(unique_leaders))
    if not elected:
        raise AssertionError("elect_leader returned empty string")

    # Step 2 — whichever node is the elected leader writes config
    # (Use node-a as the writer regardless; consistent_set is linearizable
    # from any node — it goes through consensus, not just the elected host.)
    agents[NODE_A_HOST].consistent_set(CONFIG_KEY, CONFIG_VAL)

    # Step 3 — all nodes must read the same config value via consistent_get
    def all_agree() -> bool:
        vals = [a.consistent_get(CONFIG_KEY) for a in agents.values()]
        return all(v == CONFIG_VAL for v in vals)

    if not poll_until(all_agree, timeout=20):
        vals = {h: a.consistent_get(CONFIG_KEY) for h, a in agents.items()}
        raise AssertionError(
            f"Nodes did not converge on config value within 20s: {vals}"
        )
