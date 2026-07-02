## [2026-07-02] ingest | legible-emergence Phase 0 taxonomy delivered

Phase 0 of the diagnosability plan shipped as design record
docs/design/legible-emergence-taxonomy.md (no code). Classifies 7 emergent pathologies
(P1 governed-group conflict/#56, P2 failover flap, P3 opacity oscillation, P4 opacity storm,
P5 convergence stall, P6 capability-coverage gap, P7 consensus livelock) by detection tier
(a node-local / b KV-fleet-view / c cross-node temporal), each with a grounded trip condition
+ evaporation/partition tolerance. Bakes in the four red-team findings: the ViewConfidence
header (RT1/RT2 — every diagnostic is a per-node estimate labelled with its own view health),
the RT3 evaporation tolerance per detector (P6 must wait past 3× refresh; "visible from here"
not "exists"), and the RT4 always-on-ring decision (~128 KB/node fixed ring, always-on when
the feature is enabled, so post-hoc explain works). Gate MET: KV-view (b) tier is 5 of 7 →
Phases 1-2 are cheap/collector-free. Sources grounded (SystemStats counters, sys/govern/
membership MembershipIntent, is_fresh, scatter InsufficientReplies). Plan status → "Phase 0
done; Phase 1 first code, awaiting go-ahead." Next: Phase 1 = the P1 (#56) detector on /stats.
