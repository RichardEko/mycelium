# Cross-Group Consensus Federation — Design Reference

> **HTML version:** [`docs/cross_group_consensus.html`](../cross_group_consensus.html)

## What this is

**Phase 8** of the original locality/topology capabilities plan — deliberately deferred
because the single-group and whole-cluster cases covered all immediate needs.

The existing consensus API has two scopes:

| Method | Voters | Use case |
|---|---|---|
| `group_propose(group, slot, value)` | All members of **one** named capability group | Decisions owned by a single role |
| `system_propose(slot, value)` | **All** nodes in the cluster | Cluster-wide coordination |

The gap: decisions that require ratification from **multiple distinct groups**, where each
group acts as an independent voting bloc and the proposal commits only when all blocs
reach their required quorum. Neither existing method covers this without manually chaining
calls — which is not atomic.

---

## Use Cases and Applicability

### 1. Multi-AZ durability gates
Require quorum from both an `az-east` group and an `az-west` group before committing
a persistent state change. If one AZ is partitioned, the proposal waits rather than
committing with only one AZ's acknowledgement.

### 2. Multi-stakeholder AI pipeline configuration
An LLM inference fleet (`group: llm-workers`) and a retrieval fleet (`group: retrievers`)
must both agree before a new retrieval schema is deployed — because the schema change
affects both groups' behaviour and a unilateral commit by one group breaks the other.

### 3. Hierarchical approval workflows
A `proposers` group submits a value; a `reviewers` group must ratify it. Both groups
must reach majority. This maps directly to human-in-the-loop AI agent workflows where
automated agents propose but human-proxy agents ratify.

### 4. Regulatory / compliance gates
A `trading` group of execution agents cannot commit a new strategy without quorum from
a `compliance` group of audit agents. The compliance group effectively holds a veto:
unanimous rejection by compliance blocks the proposal regardless of trading's votes.

### 5. Cross-department agent fleets
In a large organisation with separate agent clusters per department, a shared resource
allocation (e.g. GPU quota redistribution) requires buy-in from each affected
department's group, not just a global majority that one large department could dominate.

### 6. Byzantine fault isolation
If one group is partially compromised, requiring independent quorum from a second group
ensures that the compromised group alone cannot force a commit. Each group's quorum
requirement acts as an independent safety gate.

### 7. Operator / worker separation
In Mycelium's own AFN pipeline pattern: `coordinator` nodes and `worker` nodes form
distinct capability groups. A pipeline reconfiguration (stage count, timeout policy)
could require sign-off from both — coordinators own the topology, workers must confirm
they can satisfy the new requirements.

---

## Proposed API

```rust
/// Per-group quorum requirement for a cross-group proposal.
pub struct GroupQuorum {
    pub group:  String,
    /// Fraction of non-opaque group members required to accept (default 0.5 = majority).
    pub quorum: f32,
    /// If true: unanimous rejection by this group vetoes the proposal regardless of
    /// other groups' votes. Use for compliance/ratification roles.
    pub veto:   bool,
}

impl GossipAgent {
    /// Propose a value that commits only when ALL specified groups reach their
    /// individual quorum requirements.
    pub async fn cross_group_propose(
        &self,
        slot:   &str,
        value:  Bytes,
        groups: Vec<GroupQuorum>,
        config: ConsensusConfig,
    ) -> Result<ConsensusResult, ConsensusError>
}
```

---

## Protocol Design

### Why a single ballot round (not N sequential `group_propose` calls)

Chaining N `group_propose` calls is not atomic: group A could commit while group B
times out. The committed slot value is then inconsistent across groups. A single
ballot round where votes from all groups are collected and tallied independently
by the proposer is both simpler and correct.

### Ballot broadcast

The proposer broadcasts to the **union** of all specified groups' members using
`SignalScope::System` with a group membership filter, or a new
`SignalScope::Groups(Vec<Arc<str>>)` variant (one-line addition to `signal.rs`):

```rust
// signal.rs addition
SignalScope::Groups(Vec<Arc<str>>),   // union of named groups

// admits() addition
SignalScope::Groups(names) => names.iter().any(|n| self.groups.contains(n)),
```

Each ballot message includes the `groups` specification so every recipient knows
which voting blocs are involved.

### Vote tallying (proposer side)

Proposer maintains a per-group tally:

```
for each incoming vote (Accept | Reject) from node N:
    for each group G in the groups spec where N is a member of G:
        tally[G].accepts += 1   // or rejects
```

Commit condition: for every `GroupQuorum` in `groups`:
- `tally[G].accepts / group_size(G) >= quorum.quorum`

Abort condition (any of):
- `tally[G].rejects / group_size(G) > (1 - quorum.quorum)` — rejection threshold met
- `quorum.veto == true && tally[G].rejects == group_size(G)` — unanimous veto

Timeout: same `ConsensusConfig.timeout_ms` as existing protocol.

### What does NOT change

- The ballot wire format (`WireMessage::ConsensusVote`) — unchanged
- The `ConsensusResult` type — unchanged  
- `group_propose` and `system_propose` — unchanged, no regression
- The `ConsensusEngine` receive path — already records votes by sender node ID;
  the proposer-side grouping is post-processing on what's already available

### Minimal new code

1. `GroupQuorum` struct (4 fields)
2. `SignalScope::Groups(Vec<Arc<str>>)` variant + `admits()` branch
3. `cross_group_propose` method in `src/agent/consensus_ops.rs` — the proposer loop
   from `group_propose` with per-group tally added (~60 lines)
4. HTTP gateway endpoint: `POST /gateway/consensus/cross_group_propose`
5. Python + TypeScript SDK methods

---

## Files to Modify

| File | Change |
|---|---|
| `src/signal.rs` | Add `SignalScope::Groups(Vec<Arc<str>>)` + `admits()` branch |
| `src/agent/consensus_ops.rs` | Add `cross_group_propose` (~60 lines) |
| `src/consensus.rs` | Add `GroupQuorum` struct; export from crate |
| `src/lib.rs` | Re-export `GroupQuorum` |
| `src/agent/http.rs` | `POST /gateway/consensus/cross_group_propose` |
| `mycelium-py/src/mycelium/agent.py` | `cross_group_propose(slot, value, groups)` |
| `mycelium-ts/src/agent.ts` | `crossGroupPropose(slot, value, groups)` |

---

## Completion Criteria

| Signal | Detail |
|---|---|
| `cargo test --lib` green | Unit tests: determinism, veto path, timeout, mixed accept/reject across groups |
| `cross_group_propose` in HTTP gateway | `curl -X POST /gateway/consensus/cross_group_propose` |
| Python + TS SDK methods present | `agent.cross_group_propose(...)` works against live node |
| CLAUDE.md test count updated | Expect ~6 new unit tests |

---

## When to Implement

When a concrete use case demands it — the most likely triggers are:

- A multi-AZ deployment where per-AZ quorum is a safety requirement
- A human-in-the-loop AI pipeline where a `compliance` group must ratify agent proposals
- A multi-department fleet where no single department should be able to dominate a vote

The implementation is modest (~100 lines of new Rust) relative to the capability it
adds. The deferral was about priority, not complexity.
