//! Self-imposed application of a [`Policy`] onto a live node.
//!
//! **Self-imposed stance.** [`apply`] configures THIS node — it joins the node's own
//! boundary groups, installs its own `AgentStateMachine`, and records the caller allowlist
//! it will gate its own served capabilities with. There is no remote authority: no path here
//! sets another node's policy (the coordinator-free thesis; `docs/plans/mycelium-guardrails.md`
//! non-goals). A supervisor may *observe* the resulting `agent/{node}/policy` KV entry, never
//! impose one.

use std::sync::Arc;

use mycelium::{AgentStateMachine, GossipAgent};

use crate::policy::Policy;

/// Compile a [`Policy`] onto `agent` and return the live handle.
///
/// - **Tier A** — joins each declared group on the receiver boundary (`mesh().join_group`),
///   leaving the node participating in exactly those groups.
/// - **Tier B** — installs `policy.to_agent_policy()` via `agent.agent_state_machine`, held as
///   an `Arc<AgentStateMachine>` for the policy's lifetime.
/// - **Tier C** — records the `authorized_callers` list; the invoke-time gate that uses it is
///   [`check_caller`](crate::check_caller) / [`guarded_rpc_serve`](crate::guarded_rpc_serve)
///   (feature `compliance`).
pub async fn apply(policy: Policy, agent: &Arc<GossipAgent>) -> AppliedPolicy {
    // Tier A: join the declared boundary groups. `join_group` is idempotent and publishes
    // `grp/{group}/{node}`; the mesh API leaves the node joined (no RAII handle), so the groups
    // stay joined for the node's lifetime unless explicitly left.
    for group in policy.groups() {
        agent.mesh().join_group(Arc::clone(group));
    }

    // Tier B: install the compiled AgentPolicy on this node's state machine.
    let state_machine = agent.agent_state_machine(policy.to_agent_policy());

    // Tier C: hold the caller allowlist for the guard surface to consult at invoke time.
    let authorized_callers = policy.authorized_callers_list().to_vec();

    AppliedPolicy {
        state_machine,
        authorized_callers,
        agent: Arc::clone(agent),
    }
}

/// The live result of [`apply`] — the installed state machine plus the caller allowlist the
/// Tier-C guard surface consults.
///
/// Holding this keeps the `AgentStateMachine` alive. Boundary group membership (Tier A) is
/// managed on the node directly and is not owned here; leave a group via
/// `agent.mesh().leave_group(..)` if needed.
pub struct AppliedPolicy {
    state_machine: Arc<AgentStateMachine>,
    authorized_callers: Vec<Arc<str>>,
    agent: Arc<GossipAgent>,
}

impl AppliedPolicy {
    /// The installed state machine — drive the agent's execution through it so the Tier-B
    /// guards are enforced at every transition.
    pub fn state_machine(&self) -> &Arc<AgentStateMachine> {
        &self.state_machine
    }

    /// The Tier-C caller allowlist (node-ids or role names). Empty = unrestricted.
    pub fn authorized_callers(&self) -> &[Arc<str>] {
        &self.authorized_callers
    }

    /// The agent this policy was applied to.
    pub fn agent(&self) -> &Arc<GossipAgent> {
        &self.agent
    }
}
