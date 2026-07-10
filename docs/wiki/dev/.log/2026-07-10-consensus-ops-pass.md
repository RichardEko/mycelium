# 2026-07-10 — consensus/lock operations pass (doc-coverage audit follow-through)

The doc-coverage audit (4 parallel auditors, WHAT/WHY/HOW × Dev/Ops) found the consensus/lock
family had strong Dev docs but thin Ops runbooks, and was the only family with no Prometheus
surface. Closed the Tier-1 Ops holes.

## Metric family (new) — `mycelium_consensus_*` + `mycelium_schema_mismatch`
- `mycelium_consensus_timeouts_total{reason}` — **counter**, event-emitted at all four
  `ConsensusResult::Timeout` construction sites in `src/consensus.rs` (propose ×2, cross_propose
  ×2). `reason` ∈ {`no_voters` (partition), `quorum_short` (overload / quorum > live membership),
  `all_opaque`, `empty_groups`}. The one series **independent of the detector loop**.
- `mycelium_consensus_commit_conflicts`, `mycelium_schema_mismatch` — **gauges** mirroring the
  existing `/stats` scalars, set on the emergent detector tick (`src/agent/emergent.rs`) → they
  need `GOSSIP_EMERGENT_DETECTORS=1`. All emission `#[cfg(feature = "metrics")]`.
- **No per-lock gauge by design** (cardinality) — locks are consensus slots (`lock/{name}`);
  inspect one via `GET /consensus/lock/{name}` → `{committed, ballot, lease_ms, lease_expired}`.

## Runbooks (new pathologies in operations/diagnostics.md)
- **Consensus stalled — quorum unavailable** — the common CP-blocks case (distinct from the
  existing *commit conflict* = two commits, not zero). Read the `reason` label; leased commits
  self-heal on the next quorum-available round.
- **Stuck / contended distributed lock** — a crashed holder's lock self-clears at lease expiry;
  repeated `Superseded` is contention (converged-holder discipline), not a bug.
- **Schema mismatch** — `NoMigrationPath` version skew; publish the missing `SchemaMigration`.
- Prometheus rule group `mycelium-consensus` added; gauges alert on `delta(...[10m])>0` (they are
  cumulative scalars surfaced as gauges), the counter on `rate(...[5m])>0`.

Doc-vs-code fix landed the same day: `LockGuard::token` doc drift (called a "ballot" in
lock_service.rs:46 + 04-consensus.md:366) corrected to the commit HLC (#164). `make check` green
across the matrix incl. `--no-default-features`.
