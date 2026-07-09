# 2026-07-09 — connect_peer + active warm: the S13 flake root-caused and fixed (#150, #155)

The long-running S13 integration flake (tuple-space pull pipeline, ~50% in CI, filed as a
"possible convergence issue") was **root-caused from node logs** to a routing/connection-latency
problem, not convergence, and fixed at the substrate layer.

- **Root cause:** the forwarding-target set deliberately de-pins non-active peers (seed
  scalability, WS-B M4). Individual-scoped **RPC** traffic to an un-pinned peer degrades to
  **flood-relay** — too slow for a request-response RPC, which times out (HTTP 408). The
  tuple-space secondary→primary `take`/`put`/`complete` are exactly such RPCs.
- **Fix (PR #155, main @ 32b4677):** new primitive `GossipAgent::connect_peer`/`disconnect_peer`
  (`src/agent/kv.rs`) — a lock-free `pinned_peers` set the flood-fallback honours on every
  rebuild (preserves the seed de-pinning; only *specifically*-pinned peers stay direct). It also
  **actively warms** the link (writers connect lazily on first frame, so it spawns the writer +
  sends a Ping on call). The tuple-space pins both directions and runs a secondary **warm-keeper**
  (`become_secondary`) so the link is up before the first client op after readiness.
- **Verification:** `make test-overlay` (S11/S12/S13) **5/5 clean**; S13 progression ~50%
  baseline → ~75% (pin only) → **100%** (pin + active warm). `make check` clean across the full
  feature matrix; lib 355/0; tuple-space lib 31/0; failover 3/3.
- **No lock-order row:** `pinned_peers` is `papaya::HashMap` (lock-free), not a Mutex/RwLock.

Pages touched: `dev/architecture/runtime-invariants.md` (new subsection under "Individual-scope
routing: forwarding stays unconditional" — the RPC-heavy-pair lesson + the `connect_peer` primitive).

Reusable lesson: **an Individual-scoped RPC's latency depends on whether its target is a direct
forwarding target; for a hot pair, pin the route and warm it — don't rely on flood-relay.** Also a
methodology win: three earlier hypotheses (04-restart disruption, cap-discovery latency, counter
window) were each refuted by data; the fix only landed once diagnosis was grounded in captured
node logs (the "flooding via relay" warning) rather than reasoning about the code.
