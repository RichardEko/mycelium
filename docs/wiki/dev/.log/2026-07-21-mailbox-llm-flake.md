# 2026-07-21 — mailbox_llm flake: deliverability ≠ visibility (+ a watch-channel RMW race)

CI failed 3/3 smoke attempts on `01 · mailbox_llm` (~1-in-10 locally). Three findings, one push
(`1ffe9ea`, CI green after):

1. **Demo lacked the identity gate** — the cfb4d0a consensus-demo lesson had not been applied to
   the other TLS demos doing early Individual-scoped sends. Applied to mailbox_llm.
2. **KV gates prove visibility, not deliverability** — with peers + caps + identities all
   verified, the router→triage RPC *response* still dropped ~1-in-10 cold starts: each hop's
   active forwarding set is event-driven pre-first-health-reconcile. Durable pattern (ingested to
   [testing](../testing/testing.md)): an RPC-asserting demo must warm-up-probe the exact
   round-trip it asserts. 20/20 after.
3. **`watch::Sender` borrow()+send() is an unserialised RMW** — the peer-list publish could lose
   one of two concurrently-dialing peers. Fixed with `send_if_modified`; rule ingested to
   [lock-free-and-atomics](../concurrency/lock-free-and-atomics.md).

**Open question (not closed by this):** what leaves a node's sendable set empty seconds into life
with peering + identity established? The probe makes the demo immune; the substrate cold-start
liveness question deserves an instrumented session. `catalog` / `provisioning` / `mcp_toolgrowth`
share the exposure (no gate, no probe), so far unexpressed.
