## [2026-07-02] ingest | classified-page confidentiality — CORRECTED to cluster boundary

Added §4.3.2 to wiki-concurrent-edit.md AND corrected a mechanism error in §4.3.1's table.
The error: §4.3.1 claimed a capability sub-group `{group}.l3` makes classified bytes "never
gossip to non-cleared nodes." **Wrong** — verified against connection.rs: `WireMessage::Data`
always forwards `ForwardHint::All`, so Layer-I KV floods the *whole cluster* unconditionally;
`Boundary::admits` gates only Signal *acting*, never KV propagation. A capability group
organises *who participates*, never scopes *what replicates* — it is NOT a data-isolation
boundary. Corrected: the genuine isolation boundary is the **cluster/mesh** (peer admission /
TLS mutual-auth — the domain-level self-election from coordinator-free-recursion), or WS3
encryption. §4.3.2 works the RIGHT pattern (a separate `ir-classified` cluster whose KV never
peers into the public one; bridge competence at the AgentFacts edge, never raw KV). Governance
(in-cluster clearance gate, §4.3.1) vs confidentiality (separate cluster / encryption) is the
distinction; conflating them was the trap the example now closes. Caught by the user asking
for the sub-group example, which forced verifying the gossip scope.
