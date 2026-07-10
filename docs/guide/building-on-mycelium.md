# Building on Mycelium — an integrator on-ramp

You're building a **use case on top of Mycelium** (a coordinator, an agent fleet, a
replication layer), not working on Mycelium itself. This page is your contract: what to
depend on, what you must not break, and the fastest path to working code. It's the
outward-facing counterpart to [`CLAUDE.md`](../../CLAUDE.md) (which is for contributors *to*
the substrate).

New to the project? Read the [FAQ](faq.md) first (is-this-for-me / which-primitive), then
come back here.

---

## 1. The dependency

```toml
# Full runtime (KV + signals + consensus + capabilities + gateway/MCP/A2A + TLS).
mycelium = "2"

# Or the minimal substrate — Layers I+II only, ~⅓ the dep tree, no Axum:
mycelium-core = "2"
```

Feature choice on the full crate:
- **`default`** = `cli,gateway,consensus` — most integrators want this.
- **`default-features = false`** drops the gateway (bare-metal / WASM / no-std host).
- add **`tls`** for mTLS + signed consensus, **`metrics`** for a Prometheus endpoint,
  **`a2a`** for Agent-to-Agent discovery.
- Depend on **`mycelium-core`** directly only for a minimal embed that needs LWW-KV +
  the scoped event mesh but *not* RPC, consensus, capabilities, or the gateway.

**SemVer / wire:** the crate follows SemVer; the gossip **wire format** is versioned
separately (`WIRE_VERSION`, currently 12) with a one-step rolling-upgrade window
(`PREV_WIRE_VERSION`). Nodes one wire-step apart interoperate; two apart do not — see
[13-cluster-topology.md](13-cluster-topology.md). Never enable the `fuzz-internals` feature
in application code — it is explicitly **not** part of the stable API.

## 2. The composability contract — public API only

Every companion crate (`mycelium-tuple-space`, `-blackboard`, `-wiki`, `-agentfacts`,
`-wasm-host`) is built **entirely on Mycelium's public API — no internal access**. That
dependency line is the composability proof, and it's the contract for *your* crate too: if
you find yourself wanting a `pub(crate)` internal, that's a signal to reach for a different
public primitive (or open an issue), not to fork. Your use case is *another companion* — the
substrate is meant to be extended from outside, not patched from within.

Your public toolkit is the eight sub-handles off `GossipAgent`: `kv()`, `mesh()`,
`capabilities()`, `consensus()`, `service()`, `schemas()`, `llm()`, `mcp()`. See the
[crate-root doc](../../src/lib.rs) (`GossipAgent`) for the full surface and
[00-concepts.md](00-concepts.md) for the model.

## 3. KV namespace — reserve your own prefix

The KV store is **one shared substrate** and ownership is *promise-strength, not
mechanism-strength*: any node can write any key and LWW will accept it — nothing stops a
collision, so the discipline is yours to keep. The substrate and its layers own these
prefixes; **do not write under them:**

> `grp/` · `sys/` · `consensus/` · `cap/` · `req/` · `cap-group/` · `gcap/` · `mailbox/` ·
> `schemas/` · `tools/` · `agent/` · `svc/` · `log/` · `clog/` · `lock/` · `prompts/` ·
> `skills/` (the full authoritative table with per-key semantics is in
> [`src/lib.rs`](../../src/lib.rs) → *KV namespace ownership*). Note `log/` in particular:
> `KvHandle::append` writes `log/{stream}/…`, so give your streams an app-scoped name —
> don't write raw `log/` keys.
>
> Companion claims: `tuple/` (`mycelium-tuple-space`) · `wiki/` (`mycelium-wiki`) ·
> `installable/` + `comp/` (`mycelium-wasm-host`) · `ckpt/` + `ckptw/` (checkpoint index rows,
> `langgraph-checkpoint-mycelium`) · `log/reason/` (trace substreams, `mycelium-reason`). The
> `reason/blob-cache` **capability** marks blob-tier providers (`mycelium-reason`).

Pick a distinct top-level prefix for your app's state (e.g. `myapp/…`) and keep all your
writes under it. Companions follow this: the tuple space owns `tuple/…`, so yours owns
something else.

## 4. Invariants you must respect

These are the ones an integrator (or an agent generating integration code) gets wrong:

- **Call `shutdown()`** on the agent and on any companion handle with background loops
  (curators, primaries). Drop alone does not stop tasks — it can leak an `Arc` cycle.
- **KV writes are size-gated** (`framing::MAX_KV_WRITE_BYTES`). Chunk large state yourself;
  an oversized frame is dropped, not fragmented.
- **Signals flood; boundaries scope.** Forwarding is *unconditional* — only whether a node
  *acts* on a signal is scoped by its `Boundary`. Don't design as if non-members never see a
  signal; design as if they see it and ignore it.
- **Consistency is opt-in.** `kv()` is eventually consistent (LWW + HLC). Reach for
  `consensus()` (`consistent_set`/`consistent_get`/lock) only where you truly need
  linearisability — [04-consensus.md](04-consensus.md).
- **Don't ask Layer I to enforce a higher-layer law.** The substrate detects and makes
  violations legible (tripwires, counters); it does not prevent them. Build the same way.
- **Opacity is emergent.** You don't mark a node down; nodes advertise load and peers route
  around them.

Deeper: [14-patterns-and-pitfalls.md](14-patterns-and-pitfalls.md),
[09-security.md](09-security.md) (the gateway has no auth by default — front it with mTLS/a
proxy on untrusted networks).

## 5. Start from a template, not a blank file

Your use case almost certainly resembles one of the shipped companions. **Clone the nearest
one** — each is a complete, CI-gated crate built only on the public API:

| Your shape | Template |
|---|---|
| Staged pull-based pipeline | [`redistribution.rs`](../../mycelium-tuple-space/examples/redistribution.rs) / `mycelium-tuple-space` |
| One shared pool of facts, many readers | [`microgrid.rs`](../../mycelium-blackboard/examples/microgrid.rs) / `mycelium-blackboard` |
| Durable, curated, queryable memory | [`wiki_chat.rs`](../../mycelium-wiki/examples/wiki_chat.rs) / `mycelium-wiki` |

Read order: [FAQ](faq.md) → [00-concepts.md](00-concepts.md) → the nearest companion's
`src/lib.rs` (it shows the sub-handle idioms end to end) → its example.

---

## 6. Drop-in `CLAUDE.md` snippet for your project

If your project is agent-driven, paste this into *your* repo's `CLAUDE.md` so your assistant
inherits the contract without reading everything:

```markdown
## Building on Mycelium (dependency)

- Mycelium is a broker-less embedded substrate; we build on its **public API only**
  (the eight `GossipAgent` sub-handles: kv/mesh/capabilities/consensus/service/schemas/
  llm/mcp). Never reach for crate internals — pick a different public primitive instead.
- **Our KV keys live under `myapp/…`.** Never write under the substrate's reserved
  prefixes: grp/ sys/ consensus/ cap/ req/ cap-group/ gcap/ mailbox/ schemas/ tools/
  agent/ svc/ log/ clog/ lock/ prompts/ skills/ installable/ comp/ tuple/ wiki/ ckpt/ ckptw/
  (authoritative table: mycelium `src/lib.rs` → KV namespace ownership).
- Always `shutdown()` the agent + any companion handle (background loops won't stop on
  drop). KV writes are size-gated (chunk large values). Consistency is opt-in — default
  to eventually-consistent kv(); use consensus() only where linearisability is required.
- Signals flood unconditionally; a node's Boundary only decides whether it *acts*. Don't
  assume non-members never receive a signal.
- Template to copy: the mycelium-{tuple-space,blackboard,wiki} companion closest to our
  use case — each is a full crate built entirely on the public API.
```

(Replace `myapp` with your prefix.) For the substrate's own internals, point your agent at
Mycelium's [`docs/wiki/`](../wiki/wiki.md) — but for building *on* Mycelium, this page and a
companion crate are all you need.
