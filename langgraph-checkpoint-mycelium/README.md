# langgraph-checkpoint-mycelium

A [LangGraph](https://langchain-ai.github.io/langgraph/) checkpointer backed by the
[Mycelium](https://github.com/RichardEko/mycelium) mesh. LangGraph runs **on**
Mycelium: graph state becomes coordinator-free, gossip-replicated, and resumable
across nodes — kill the node a thread was running on and any other node in the mesh
can pick it up.

## The one-line swap

```python
from langgraph_checkpoint_mycelium import MyceliumCheckpointSaver

# was: checkpointer = InMemorySaver()  /  PostgresSaver(...)
checkpointer = MyceliumCheckpointSaver("127.0.0.1", 8101)

graph = builder.compile(checkpointer=checkpointer)
graph.invoke(inputs, {"configurable": {"thread_id": "my-thread"}})
```

Both the sync and async LangGraph paths are supported (`get_tuple`/`list`/`put`/
`put_writes`/`delete_thread` and their `a*` variants, over `httpx.Client` /
`httpx.AsyncClient`).

## Installation

```sh
pip install langgraph-checkpoint-mycelium     # PyPI (when published)

# Or from source:
pip install ./langgraph-checkpoint-mycelium
```

Requires Python ≥ 3.10 and a running Mycelium node whose gateway mounts the
`mycelium-reason` routes (see the `reason_node` example in that crate).

## The storage split

Naïve checkpoint-blobs-in-KV would flood every node with every agent's channel
state; Mycelium's KV is also size-gated. So the saver splits storage the way the
substrate wants (`docs/plans/mycelium-reason.md`):

- **Metadata / index → gossiped KV** (small rows only): one row per checkpoint at
  `ckpt/{thread_id}/{checkpoint_ns}/{checkpoint_id}`, one per pending write at
  `ckptw/{thread_id}/{checkpoint_ns}/{checkpoint_id}/{task_id}/{idx}`. Checkpoint
  metadata (source / step / parents) stays inline in the row, so `list()`
  filtering never fetches a payload. The empty namespace (LangGraph's default
  `checkpoint_ns=""`) is encoded as the sentinel segment `__root__`.
- **Payloads → the content-addressed blob tier** (`PUT/GET /gateway/reason/blob`):
  the checkpoint skeleton is one blob and **each channel value is its own blob**.
  A blob's id is its SHA-256, so an unchanged channel value across super-steps
  dedups to a single stored blob — chatty graphs don't pay for their transcripts
  twice.

## Cross-node resume (the point)

A saver pointed at node **B**'s gateway resumes a thread checkpointed via node
**A**: the index rows arrive by gossip, and payload blobs are fetched through the
gateway's local-then-mesh path (`reason/blob-cache` providers, hash-verified).
No coordinator, no shared database — the mesh *is* the checkpoint store.

## Honest limits (v1)

- **≤ 8 MiB per blob** — the single-frame mesh-fetch ceiling; chunked transfer is
  the named follow-up in `mycelium-reason`.
- **Gossip-eventual metadata** — read-your-writes holds only against the *same*
  node's gateway. A cross-node reader polls until the thread head has gossiped in
  (the test suite shows the structural convergence loop).
- **`delete_thread` tombstones index rows only** — content-addressed blobs may be
  shared across threads; unreferenced blobs are a GC concern, not a correctness
  one.
- The package claims the **`ckpt/*` and `ckptw/*` KV prefixes** — treat them as
  reserved next to the substrate's own (`docs/guide/building-on-mycelium.md`).

## Where this sits — the Mycelium × LangChain/LangGraph integration map

One coherent story, four touchpoints (anti-scatter — these are different layers,
not competing examples):

| Touchpoint | Direction | Use when |
|---|---|---|
| `examples/a2a_langchain/` (A2A interop) | LangChain → Mycelium | a LangChain/AutoGen agent should *call Mycelium skills* as tools |
| **this package** (state backend) | LangGraph **on** Mycelium | a LangGraph graph should *survive node loss and hand off across the fleet* |
| `mycelium-reason` (Tier-3 wedges) | substrate-native | you want capability-routed inference, fleet-reasoning traces, artifact-aware resume |
| `mycelium.call_typed` (typed output) | through-the-mesh calls | you want schema-validated output from a *mesh-routed* prompt skill (use Instructor / Pydantic AI when talking to a provider directly) |
