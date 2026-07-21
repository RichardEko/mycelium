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
mute toward that peer (worst case ~one `health_check_interval`). **RESOLVED (same day, follow-up session — option 1 implemented).** Instrumentation overturned
part of the narrowing: with `swim_failure_detector: true` (the DEFAULT), the TCP Ping arm never
runs at all — discovery rides SWIM UDP — and the true mechanism was that **SWIM's
`ApplyEffect::BecameAlive` updated the peers map but never published to the forwarding watch**;
only a health-monitor tick reconcile did, and the first tick lands at startup-jitter (0–5 s).
The watch is bootstrap-seeded at creation, so only the SEED node (no bootstrap) sat mute — its
RPC *responses* dropped when its jitter exceeded the caller's RPC budget: the exact 1-in-10.
Fix (all shipped):
- **SWIM path:** `BecameAlive` now runs the same bounded fan-out activation as the TCP Ping arm
  (`swim.rs::apply_effect`; `resolved_fanout`-capped, health monitor stays the reconciler).
- **Non-SWIM path:** ping-before-pull (startup Ping announcing our `NodeId` ahead of the
  StateRequest, which is ignored from unknown peers) + a Ping-arm ping-back on first learn
  (loop-safe via `sender_is_new`; ≤3 frames/pair).
- **Deterministic gates:** `test_cold_start_rpc_both_directions_before_first_tick_{swim,no_swim}`
  — health interval cranked to 3600 s so no tick can rescue the handshake; red on the old code,
  both directions RPC-green in <1 s on the new.
The demo-level identity gates + round-trip probes (all four susceptible demos) stay as defensive
practice — the testing.md deliverability corollary still applies to any demo asserting on a
freshly-formed path.

**Act 3 (same day): the fix's own regression, caught by CI.** The startup ping and ping-back
shipped with `known_peers: Vec::new()` — the tick ping's peer-exchange piggyback was the part of
the protocol the new pings didn't copy. Consequence: peering gates now pass in milliseconds on
direct links alone, compressing test/startup timelines *inside* the first health interval — and
in `failover_preserves_items_and_ids` (non-SWIM) the primary, sole introducer of client↔secondary,
died before its first tick ping ever carried the introduction. Both survivors knew only the dead
node; staleness eviction then emptied their maps to zero — total isolation, no rediscovery.
Root-caused via in-test diagnostics (client peers `[dead-primary]` → `[]`). Fix: **every Ping
carries the peer-exchange sample** — the empty-`known_peers` ping was the anomaly, not the norm.
Also fixed en route: the activation cap sized by the map alone could refuse the only live member
when the watch was bootstrap-seeded (`sizing = known.max(current+1)`, unit-gated by
`became_alive_activates_despite_bootstrap_seeded_watch`). Verified: failover 8/8 (was ~1-in-3
green), cold-start pair 0.25 s (peer exchange restored instant convergence), full suites + make
check green. Meta-lesson for the ledger: a liveness fix that *accelerates* a gate can invalidate
every downstream assumption timed against the old, slower gate — grep for gates whose semantics
silently strengthened. Companion artifacts: `loom-spike/tests/bounded_append.rs` (models the
borrow+send lost update; broken twin fails with a printed schedule) and the `/wiki-lint` §1
watch-RMW mechanical sweep.

**Epilogue — the workarounds stay (deliberate decision, 2026-07-21).** With the substrate fixed,
the question was whether to strip the demo-level gates/probes and the older pinning workarounds.
Decision: **keep all of them** — do not remove as "dead" in a future cleanup pass:
- The **identity gates** guard TLS's fail-closed unknown-signer drop — a live precondition, not
  the fixed bug.
- The **warm-up probes / RPC retries** now defend only inherent timing (loaded runners, gossip
  latency). They cannot mask a substrate regression because the deterministic gates
  (`test_cold_start_rpc_*`, the failover suite, the loom models) detect one directly — masking
  requires being the sole detector. Cost on a healthy substrate ≈ zero. Requirement: the belt is
  **loud when it takes load** (the mailbox probe now prints its retry count; the RPC retries
  already did) — a retrying probe is a bug report, never silent absorption.
- **`connect_peer` / primary pinning (#150)**: never only a workaround — under bounded fan-out a
  de-pinned peer legitimately degrades to flood-relay, too slow for RPC-heavy relationships at
  steady state. The cold-start motivation evaporated; the steady-state one is permanent.
- The **consensus demo's ballot headroom** serves the stricter correct quorum on slow runners —
  unrelated family, stays.
