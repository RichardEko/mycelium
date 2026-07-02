## [2026-07-02] ingest | curator crash + failover worked example (§3.5)

Added §3.5 to wiki-concurrent-edit.md: three Auto nodes (X=curator id1, Y/Z candidates);
X crashes mid-reconcile AFTER writing MERGED but BEFORE tombstoning proposal P. Walks the
timeline: X's ads evaporate at 3× cap_refresh → current_curator None (reads still work, I3)
→ Y (lowest live id) promotes with no log replay (state is derivable KV, §3.4) → Y re-drains
P, re-3-way-merges against current(=MERGED), writes idempotent MERGED', tombstones P → P
applied exactly once. Invariant walkthrough (I1 no two writers across handoff because
promotion waits for the curator ad to EVAPORATE not merely go quiet; I2 P survives because
tombstoned only after incorporation; I3 reads never pause). Documented the liveness caveat:
promotion latency ≈ 3× cap_refresh (ring-as-failure-detector), eventual-single not strict-
single (a slow-X blip converges via total-order tie-break + idempotent drain).
