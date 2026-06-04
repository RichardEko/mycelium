# Overlay Integration Scenarios

Three end-to-end agentic scenarios that exercise Mycelium's opt-in consistency and
ordering overlay. Each scenario is a **copy-paste template** for a real production
pattern; read the source as documentation, not just a test.

## Running

```sh
make test-overlay   # builds images, starts 3-node cluster, runs all scenarios, tears down
```

Docker cache is keyed on source files — re-runs after code changes typically take < 30 s.

## Cluster topology

```
overlay-a :57000/:8300  ─┐
overlay-b :57000/:8301  ─┼─ fully meshed, all are consensus voters
overlay-c :57000/:8302  ─┘
runner (Python 3.11 + mycelium-py)
```

All three nodes run `MYCELIUM_ROLE=overlay`, which starts the consensus listener and
exposes the full overlay HTTP API on `MYCELIUM_HTTP_PORT`. The Python runner talks to
any node's gateway — the overlay APIs are location-independent.

---

## S11 — Task Auction (exact-once delivery)

**File:** [`scenarios/s11_task_auction.py`](scenarios/s11_task_auction.py)

**Pattern:** Coordinator queues work; multiple workers race to claim each item exactly once.

### What it does

1. A coordinator node (`overlay-a`) appends 5 tasks to the `"s11-tasks"` log stream.
2. Two worker nodes (`overlay-b`, `overlay-c`) subscribe via the `"workers"` consumer group.
3. Consumer-group semantics ensure each task is delivered to exactly one worker.
4. Verification: 5 total deliveries, no duplicates, HLC order non-decreasing.

### Key API

```python
# Coordinator
hlc = agent.append("tasks", b"task-0")

# Worker
async for entry in agent.subscribe_log_group("tasks", "workers"):
    process(entry.value)   # only one worker receives each entry
```

### When to use this pattern

- Distributed job queues where duplicate processing is costly
- Multi-consumer fanout where each item must be processed once
- Any scenario requiring at-most-once or exactly-once delivery over gossip

---

## S12 — Leader Election + Consensus-Durable Config

**File:** [`scenarios/s12_leader_election.py`](scenarios/s12_leader_election.py)

**Pattern:** Concurrent election + winner writes a consensus-durable config value read by all nodes.

### What it does

1. All three nodes call `elect_leader("s12-demo")` concurrently.
2. Consensus ensures all three calls return the same leader string.
3. One node writes a config value via `consistent_set`.
4. All three nodes call `consistent_get` and verify they see the same value.

### Key API

```python
# All nodes race — consensus picks one winner
leader = agent.elect_leader("my-group")

# Winner (or any node) writes shared config
agent.consistent_set("config/endpoint", b"https://api.v2/")

# All nodes read the same committed value
val = agent.consistent_get("config/endpoint")
```

### When to use this pattern

- Shard assignment in a storage cluster
- Configuration propagation where split-brain is unacceptable
- Any single-writer / multi-reader coordination that needs consensus backing

---

## S13 — Shared Reasoning Log

**File:** [`scenarios/s13_shared_reasoning_log.py`](scenarios/s13_shared_reasoning_log.py)

**Pattern:** Multiple writers append to a shared log; all readers see entries in causal order.

### What it does

1. Each of the three nodes appends 3 "observation" entries to `"s13-observations"` (9 total).
2. The test polls until every node's `scan_log` returns all 9 entries — proving gossip convergence.
3. Entries on each node are verified to be in non-decreasing HLC order.
4. `compact_log` removes the oldest half; the test verifies the correct entries survive.

### Key API

```python
# Any node can write
hlc = agent.append("observations", f"obs/{my_id}/0".encode())

# Any node can read — eventually consistent after gossip propagation
entries = agent.scan_log("observations")          # full log, sorted by HLC
recent  = agent.scan_log("observations", from_hlc=cursor)

# Prune old entries
agent.compact_log("observations", checkpoint_hlc)
```

### When to use this pattern

- Distributed audit / reasoning trail written by multiple agents
- Event sourcing where causal ordering matters but strict serialisation is not required
- Append-only observation logs in multi-agent systems (e.g. LLM planning pipelines)

---

## Adapting these scenarios

All three scenarios follow the same structure you can reuse:

```python
from mycelium import MyceliumAgent

agent = MyceliumAgent("overlay-node-host", 8300)

# 1. Check cluster is ready (helpers.wait_for_cluster_ready)
# 2. Write / coordinate
# 3. Poll for convergence (helpers.poll_until)
# 4. Assert correctness (helpers.assert_eq, assert_ge)
```

The `scenarios/helpers.py` module provides `wait_for_cluster_ready`, `poll_until`,
`assert_eq`, and `assert_ge` — copy it into your own test suite as a starting point.

To add a new scenario:
1. Create `scenarios/s14_my_pattern.py` with a `run()` function.
2. Import and register it in `run.py`.
3. `make test-overlay` will pick it up automatically.
