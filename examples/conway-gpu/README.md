# Conway on GPU — 256 gossip agents + a Metal/wgpu compute shader

## Objective

The [chapter 01](../../docs/guide/01-gossip-kv.md) convergence visual at scale: **256 agents**,
each owning one cell of Conway's Life, share state purely by gossip KV while a wgpu compute
shader renders the board. Demonstrates that Layer I convergence behaves identically whether 2
agents share one value ([`hello_mesh`](../hello_mesh.rs)) or 256 agents share a live grid — and
that a render loop can treat the gossiped KV as its single source of truth.

## How to run

Toolchain only (no LLM, no Docker — [shared setup](../README.md#shared-setup)):

```bash
cargo run --release -p conway-gpu
```

A window opens with the board; the terminal logs generation/convergence stats. (CI builds this
crate on every PR — the "Conway GPU demo (compile)" job — but the window itself is manual.)

## What it demonstrates

- **KV as the only coordination medium**: no agent talks to the renderer; the shader samples
  the same gossiped keys the agents write (`examples/conway-gpu/src/main.rs`).
- **Convergence under churn at 256 nodes in-process** — the same LWW+HLC mechanics as
  [guide ch. 01](../../docs/guide/01-gossip-kv.md), at a scale where drift would be visible
  as flicker.

## Dev notes

- Metal on macOS; other platforms via wgpu's Vulkan/DX12 backends (untested here).
- The CPU sibling ([`conway.rs`](../conway.rs), 16×16, terminal-rendered) is the 30-second
  version — start there if you just want the concept.
