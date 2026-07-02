## [2026-07-02] ingest | classified-page clearance worked example (+ the served-path caveat)

Added §4.3.1 to wiki-concurrent-edit.md: a worked classified-section example (L3 root-cause
section; L2 agent redacted, L3 agent admitted) grounded in the real WS1 API (advertise_roles
/ roles_of / clearance_at_least, Ed25519-signed RoleClaim). The load-bearing clarification:
per-page clearance on a GOSSIP substrate is a **served-path gate** (detection-not-prevention;
withholds the convenient read path + audits, but the bytes already replicated to every group
member's store). Governance ≠ confidentiality — for bytes-never-reach-them you need a tighter
sub-group boundary ({group}.l3) or WS3 per-page encryption, not a per-key ACL within one
group. Recorded as a decision table so a builder can't ship "L3 hidden from L2 members" as if
a label alone enforced it.
