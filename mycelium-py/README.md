# mycelium-py

Python SDK for the [Mycelium](https://github.com/RichardEko/mycelium) gossip mesh.

Connects to a running Rust Mycelium node over loopback HTTP. No native extension —
the HTTP gateway sidecar adds ~1 ms per call, invisible next to LLM inference latency.

## Installation

```sh
pip install mycelium-py            # PyPI (when published)

# Or from source:
cd mycelium-py
pip install -e ".[dev]"
```

**Requires Python ≥ 3.10** and a running Mycelium node with `http_port` set.

## Quick start

```python
import asyncio
from mycelium import MyceliumAgent

async def main():
    agent = MyceliumAgent("127.0.0.1", 8300)

    # Advertise a capability; drop the handle to retract it
    with agent.advertise_capability("compute", "gpu", attributes={"model": "A100"}):
        providers = agent.resolve_capability("compute", "gpu")
        print(providers)  # [{"node_id": "...", "ns": "compute", "name": "gpu", "attributes": {...}}]

        # Emit a signal
        agent.emit("render-job", b"payload", scope="system")

        # Subscribe to signals
        async for sig in agent.on_signal("render-job"):
            print(sig.sender, sig.payload)
            break

asyncio.run(main())
```

## API reference

### `MyceliumAgent(host, port, timeout)`

| Parameter | Default | Description |
|-----------|---------|-------------|
| `host` | `"127.0.0.1"` | Gateway host |
| `port` | `7946` | HTTP port the Mycelium node listens on |
| `timeout` | `30.0` | Default request timeout (seconds) |

---

### Capability advertisement

#### `advertise_capability(ns, name, *, interval_secs, attributes, authorized_callers) → CapabilityHandle`

Advertises a capability on the mesh. Re-asserted every `interval_secs` so late joiners
discover it. Returns a `CapabilityHandle`; call `.drop()` or use as a context manager to retract.

```python
handle = agent.advertise_capability(
    "compute", "gpu",
    interval_secs=30,
    attributes={"model": "A100", "vram_gb": 80},
    authorized_callers=["orchestrator"],  # empty = unrestricted
)
handle.drop()  # tombstones the KV entry

# or
with agent.advertise_capability("compute", "gpu") as h:
    ...  # live inside the block
```

#### `resolve_capability(ns, name, *, caller_id) → list[dict]`

Returns all live providers matching `(ns, name)`. Pass `caller_id` to respect
`authorized_callers` restrictions.

```python
providers = agent.resolve_capability("compute", "gpu", caller_id="orchestrator")
# [{"node_id": "127.0.0.1:57001", "ns": "compute", "name": "gpu", "attributes": {...}}]
```

#### `demand(ns, name) → DemandStatus`

Returns demand pressure: `DemandStatus(ns, name, providers, requirers, demand_pressure)`.
`demand_pressure > 1.0` signals a supply gap.

---

### Signal mesh

#### `emit(kind, payload, *, scope) → bool`

Fires a signal into the mesh.

- `scope`: `"system"` (default), `"group:NAME"`, or `"node:IP:PORT"`
- Returns `True` if queued for gossip; `False` if the gossip shard was full (local delivery still occurred).

#### `on_signal(kind) → AsyncIterator[Signal]`

Async generator yielding admitted signals of `kind` as SSE events.

```python
async for sig in agent.on_signal("render-job"):
    print(sig.kind, sig.sender, sig.payload, sig.nonce)
    break
```

`Signal` fields: `kind: str`, `sender: str`, `payload: bytes`, `nonce: int`.

---

### RPC

#### `rpc_call(target, method, payload, *, timeout_secs) → bytes`

Blocking point-to-point RPC call. Raises `TimeoutError` if no reply arrives.

```python
result = agent.rpc_call("127.0.0.1:57001", "echo", b"hello", timeout_secs=5)
```

#### `rpc_serve(kind) → AsyncIterator[RpcRequest]`

Async generator yielding incoming RPC requests of `kind`. For each request, call
`rpc_respond` to complete the round-trip.

```python
async for req in agent.rpc_serve("echo"):
    agent.rpc_respond(req, req.payload + b"-reply")
```

`RpcRequest` fields: `kind: str`, `nonce_hex: str`, `sender: str`, `payload: bytes`.

#### `rpc_respond(request, result)`

Sends a reply to an in-flight RPC request.

#### `scatter_gather(targets, method, payload, *, min_ok, timeout_secs) → list[dict]`

Fan-out RPC to multiple targets; waits for at least `min_ok` replies. Raises `TimeoutError`
if the threshold is not met.

```python
replies = agent.scatter_gather(
    ["127.0.0.1:57001", "127.0.0.1:57002"],
    "vote",
    b"proposal",
    min_ok=2,
    timeout_secs=5,
)
# [{"sender": "127.0.0.1:57001", "result": b"yes"}, ...]
```

---

### KV store

```python
agent.set("my/key", b"value")              # write + gossip
val   = agent.get("my/key")               # → bytes | None
agent.delete("my/key")                    # tombstone + gossip
keys  = agent.keys(prefix="my/")          # → list[str]
data  = agent.scan_prefix("my/")          # → dict[str, bytes]
```

All writes are gossiped to peers with last-write-wins (HLC) semantics.

---

### Mailbox (Actor/Event delivery)

#### `deliver_event(target, kind, payload)`

Delivers a mailbox event to `target`'s mailbox at key
`mailbox/{target}/{kind}/{hlc_ts}`. Gossiped to all peers; at-least-once within the TTL.

```python
agent.deliver_event("127.0.0.1:57001", "task.result", b"done")
```

#### `mailbox(kind) → AsyncIterator[MailboxEvent]`

Streams events of `kind` addressed to this node. Events are delivered in HLC-causal
order and tombstoned on delivery (won't reappear after a restart).

```python
async for event in agent.mailbox("task.result"):
    print(event.sender, event.kind, event.payload)
```

`MailboxEvent` fields: `kind: str`, `sender: str`, `payload: bytes`.

---

### Introspection

```python
agent.health()  # → {"status": "ok", "node_id": "..."}
agent.stats()   # → {"node_id": "...", "store_entries": N, "dropped_frames": N}
```

---

## Running the tests

Tests require a live Mycelium node. Start one with the demo binary or a custom config:

```sh
# Start a node on port 8300
cargo run --example three_node_demo  # or any node with http_port=8300

# Run the gateway tests
cd mycelium-py
pip install -e ".[dev]"
MYCELIUM_TEST_HOST=127.0.0.1 MYCELIUM_TEST_PORT=8300 pytest tests/ -v
```

## Gateway endpoint reference

All methods talk to the embedded HTTP gateway on the Rust node:

| Method | Endpoint | Description |
|--------|----------|-------------|
| `advertise_capability` | `POST /gateway/capability/advertise` | |
| `resolve_capability` | `GET /gateway/capability/resolve` | |
| `emit` | `POST /gateway/signal/emit` | |
| `on_signal` | `GET /gateway/signal/sse/{kind}` | SSE stream |
| `demand` | `GET /gateway/demand` | |
| `rpc_call` | `POST /gateway/rpc/call` | |
| `rpc_serve` | `GET /gateway/rpc/serve/{kind}` | SSE stream |
| `rpc_respond` | `POST /gateway/rpc/respond` | |
| `scatter_gather` | `POST /gateway/scatter` | |
| `get` | `GET /gateway/kv?key=K` | |
| `set` | `POST /gateway/kv` | |
| `delete` | `DELETE /gateway/kv?key=K` | |
| `keys` | `GET /gateway/kv/keys?prefix=P` | |
| `mailbox` | `GET /gateway/mailbox/{kind}` | SSE stream |
| `deliver_event` | `POST /gateway/mailbox/deliver` | |
| `health` | `GET /health` | |
| `stats` | `GET /stats` | |
