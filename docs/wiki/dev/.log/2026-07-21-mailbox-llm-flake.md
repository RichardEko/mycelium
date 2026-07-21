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

**Open question — NARROWED same day (follow-up session):** what leaves a node's sendable set empty
seconds into life with peering + identity established? Root cause localized to a structural fact:
**fan-out activation is exclusively Ping-borne.** The `sender_is_new` → `peer_list_tx` publish
lives only in the `Ping` arm (`connection.rs`), and necessarily so — `Data`/`SignedData` carry
only the sender's `u64` id-hash, not a `NodeId`, so those arms *cannot* activate a peer even
after signature verification. KV-level state (identities, caps — what readiness gates can see)
arrives gossip-borne and races ahead of Ping-borne activation; until a Ping lands, the node is
mute toward that peer (worst case ~one `health_check_interval`). **Remaining fix is a design
decision, not a patch** — candidates, each with M4-boundedness / self-connection-guard
implications: (1) a hello frame carrying the full `NodeId` at connection-open that runs the same
bounded activation; (2) seed the watch with bootstrap peers at `start()`; (3) a zero-jitter
immediate first health tick. Deserves its own session. Meanwhile all four susceptible demos
(`mailbox_llm` · `catalog` · `provisioning` · `mcp_toolgrowth`) now carry the identity gate, and
the RPC-asserting ones a round-trip probe/retry.
