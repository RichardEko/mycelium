# 14 · Patterns & Pitfalls

Every entry here is **grounded in a shipped example**: it shows how that example
does it *the right way*, names the anti-pattern it deliberately avoids, and
explains *why*. These are real lessons from building the
[Food-Rescue Co-op suite](../../examples/coop/) — not invented cautionary tales.
Several were found the hard way (a flaky test, a CI failure, a substrate bug
filed as an issue).

When you hit a "why won't this converge / why is this flaky" moment, scan this
chapter first.

---

## 1 · Host an invokable skill on a *separate* node from its caller

**Pattern** — In [`mailbox_llm`](../../examples/coop/src/bin/mailbox_llm.rs), the
`routing/suggest` skill lives on `kitchen-router`; the agent that *calls* it
(`depot-triage`) is a different node. The call is a genuine cross-node RPC.

**Anti-pattern** — Having a node resolve and RPC *its own* capability (a
self-call).

**Why** — An Individual-scoped frame to yourself needs the node to have ≥1
usable peer to deliver, and it flakes before peering settles. A cross-node call
falls back to flood-relay if the target isn't directly peered, so it's robust.
(Found by hammering the smoke: a self-RPC version went 1/4 → the cross-node
restructure went 6/6.)

---

## 2 · Gate readiness on capability-visible **and** peers-formed

**Pattern** — `mailbox_llm`
([line ~118](../../examples/coop/src/bin/mailbox_llm.rs)) waits until
`skill_visible && peered` before sending the first request.

**Anti-pattern** — Polling only `resolve(&filter).is_empty()` and firing as soon
as the capability appears.

**Why** — KV anti-entropy gossips the capability advertisement *before* the
outbound peer list is populated. Gate on capability alone and the RPC races
ahead of peering — you get `Individual-scoped frame dropped: no peers at all`.
Wait for both.

---

## 3 · Use structural polls, never fixed sleeps, for convergence

**Pattern** — Every coop demo uses a `wait_until(secs, || <observable>)` helper
that polls real state (peers formed, capability visible, count converged).

**Anti-pattern** — `tokio::time::sleep(Duration::from_millis(300)).await;` "to
let it settle."

**Why** — A fixed sleep passes by luck on a fast machine and hides the race on a
slow one (a constrained CI runner, say). A structural poll fails deterministically
and points at the cause. This is the CLAUDE.md testing convention, and it's why
the `coop-smoke` CI job is stable. (See also §7 for when even a structural poll
needs a faster substrate tick under it.)

---

## 4 · Allocate N ports by binding N listeners at once

**Pattern** — `common::bootstrap::alloc_ports(n)`
([bootstrap.rs](../../examples/coop/src/common/bootstrap.rs)) binds all `n`
ephemeral listeners simultaneously, *then* returns the ports.

**Anti-pattern** — Calling a single `alloc_port()` `n` times in a row.

**Why** — Bind-`:0`, drop, bind-`:0` again can hand back the *same* just-freed
ephemeral port, so two agents collide on startup. Holding all `n` open at once
guarantees distinct ports. (A small TOCTOU window remains before the agents bind
— fine for a demo, and the reason production code uses fixed/configured ports.)

---

## 5 · Model a node's load as its **own** backlog (stigmergy)

**Pattern** — In [`stigmergy`](../../examples/coop/src/bin/stigmergy.rs)
([line ~140](../../examples/coop/src/bin/stigmergy.rs)) a depot becomes opaque
because *it* emits work into *its own* `work.intake` queue; the governor reads
the local fill and writes the `sys/load/` pheromone.

**Anti-pattern** — Flooding a *remote* node's queue (sending it Individual-scoped
signals) to drive its opacity from the outside.

**Why** — Cross-node Individual-scoped **signals** don't currently land in a
remote node's `signal_rx` handler ([issue #55](https://github.com/RichardEko/mycelium/issues/55)).
More fundamentally, stigmergy *is* self-report: a node advertises its own
saturation; others read the trail and decide locally. Load is never injected
from outside.

---

## 6 · Let the MembershipGovernor *own* a group under intent

**Pattern** — [`elastic_intent`](../../examples/coop/src/bin/elastic_intent.rs)
publishes a `MembershipIntent` for `rush-pool`; the governor converges the
member count into the band, and the emergent-group watcher **defers** for that
group.

**Anti-pattern** — Expecting a governor's `max` / `drain` to hold while the
always-on emergent auto-join is still free to re-join every eligible node.

**Why** — Until [PR #57](https://github.com/RichardEko/mycelium/pull/57) the
emergent watcher auto-joined every cap-matching node *unconditionally* and
re-joined anything the governor shed — so `max` was unenforceable. The fix makes
a group with a live intent governor-owned; the demo only works because of it.
(Found while building this very example.)

---

## 7 · Use a faster anti-entropy tick when reading a freshly-signed fact across startup

**Pattern** — [`rotation`](../../examples/coop/src/bin/rotation.rs) runs its two
depots with `health_secs: Some(2)` so anti-entropy re-delivers a dropped frame
quickly.

**Anti-pattern** — Publishing a signed fact and asserting a peer reads it within
a tight window at the default (10 s) health interval.

**Why** — At startup a peer can *drop* a signer's signed KV frame before it has
processed that signer's `sys/identity` (`SignedData from unknown signer`).
Anti-entropy re-delivers it on the next sweep — so a short interval closes the
gap promptly instead of waiting out the default. (This demo went 2/6 flaky → 8/8
after the faster tick.)

---

## 8 · Keep fan-in joins to a single synthesizer (today)

**Pattern** — [`llm_council`](../../examples/coop/src/bin/llm_council.rs)
([line ~207](../../examples/coop/src/bin/llm_council.rs)) has **one**
synthesizer that drains `partials` and accumulates them *by donation id in its
own memory* until it holds a complete set.

**Anti-pattern** — Scaling out *competing* synthesizers and expecting each
donation's three partials to land together.

**Why** — Competing synthesizers would each `take` fragments of one donation's
partial set, and an unkeyed lane can't correlate them for you. True
keyed-correlation fan-in is **`take_by_key`** — **M13, shipped** (Paper 1 §9.4):
`put_keyed(stage, key, …)` + `take_by_key(stage, key)` claim by an O(1)
correlation key, so competing synthesizers each rendezvous on the *same*
donation id. The demo predates the primitive and keeps the single-synthesizer
shape; with M13 you can now scale out keyed synthesizers instead.

> **Content-routed competition is the blackboard, not the tuple space.** When
> *which* consumer acts depends on a fact's *content* (not its lane), reach for
> [`mycelium-blackboard`](../../mycelium-blackboard/): `claim(predicate)` is a
> competitive, exactly-once claim over facts matching an attribute predicate
> (Linda's `in`), with non-destructive shared `read` (`rd`) for the agents that
> only observe. Lanes route by position; the blackboard routes by content.

---

## 9 · Bridge an MCP tool by *also* advertising a `tool/` capability

**Pattern** — In [`mcp_toolgrowth`](../../examples/coop/src/bin/mcp_toolgrowth.rs)
the host calls `register_mcp_tool(...)` **and** `advertise_capability(Capability::new("tool", …))`
together ([line ~85–91](../../examples/coop/src/bin/mcp_toolgrowth.rs)).

**Anti-pattern** — Calling `declare_requirement(tool/…)` and expecting
`register_mcp_tool` *alone* to satisfy it.

**Why** — MCP tools live under `tools/{name}/{node}`, a **separate namespace**
from the `cap/` demand system. The requirement creates demand on `cap/`; only the
companion capability advertisement makes that demand resolvable. (See
[00-concepts.md](00-concepts.md) on why `tools/` ≠ `cap/`.)

---

## 10 · Ship artifacts via the gossip catalogue, not a node-local source

**Pattern** — The catalogue example (coop step 11) publishes to the cluster-wide
gossip catalogue (`installable/` + `MeshArtifactSource`) so any provider node can
pull the bytes.

**Anti-pattern** — Using `InMemorySource` in a real cluster — which the
[`provisioning`](../../examples/coop/src/bin/provisioning.rs) flagship does
deliberately as a *single-process* shortcut
([line ~61](../../examples/coop/src/bin/provisioning.rs)).

**Why** — `InMemorySource` holds the artifact bytes in *one process's* memory;
no other node can fetch from it. For a real cluster the artifact must be
discoverable (`publish_installable` → `installable/` gossiped KV) and its bytes
distributable (`MeshArtifactSource` / `serve_artifacts`). `provisioning` uses the
shortcut because it runs every role in one process; don't copy that into a
distributed deployment. The cluster-wide path — `publish_installable` → `from_kv`
→ `MeshArtifactSource` — is shown end to end in the
[`catalog`](../../examples/coop/src/bin/catalog.rs) example and documented in
[operations/artifacts.md](../operations/artifacts.md).

---

## The meta-pattern

Eight of these ten were found *by building and running the examples*, not by
reading code — three of them as CI failures or filed bugs (#55, #57). That is
the point of an executable example suite: it exercises the seams between
subsystems (peering vs. capability gossip, governor vs. emergent join, MCP `tools/`
vs. `cap/`) where the real pitfalls live. When you compose Mycelium primitives,
**run the composition** — the seam is where it bites.

**Next:** back to the [guide index](README.md), or [00 · Concepts](00-concepts.md)
for the vocabulary these patterns assume.
