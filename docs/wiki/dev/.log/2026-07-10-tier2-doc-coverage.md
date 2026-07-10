# 2026-07-10 — Tier-2 doc-coverage close (audit follow-through)

Closed the doc-coverage audit's Tier-2 (Dev-guide HOW trapped elsewhere). All verified vs code.

- **error-handling.md — `GossipError` was stale** (invented `Network`/`Config`; omitted 5 real
  variants incl. `FrameTooLarge`). Rewrote to the real 10-variant re-exported enum
  (`mycelium-core/src/error.rs`) + size-gating note. Fixed a pre-existing broken anchor.
- **04-consensus.md** — new "Leased commits" section (`committed_lease_secs`, lease-aware reads,
  reopen-on-expiry) + a converged-holder/optimistic-commit row in the design table (was wiki-only).
- **06-tool-discovery.md** — new "Bridging an external MCP server" (`connect_mcp_server`); the
  chapter now matches its title. Bridged tools land in the same `tools/` namespace; egress-gated.
- **cookbook.md** — RPC recipe → service-layer reference; artifacts recipe → the Solution/Dev +
  DevOps anchors (was a bare file link).
- **operations/companions.md (NEW)** — the operator runbook for the three companions. Key verified
  facts: all use **capability-ring failover** (lowest node-id self-elects; not consensus/lease);
  tuple-space + blackboard WAL is **off by default**; the **wiki's external store is the record of
  record** (failover transfers nothing — node-local store + node death = corpus lost); **none emit
  Prometheus metrics** (`BoardStats` is aspirational, not wired). Wired into the operations index +
  docs map; folder-note here points to it.

No code changed in this batch (the consensus/lock *metric* family was the earlier A pass).
