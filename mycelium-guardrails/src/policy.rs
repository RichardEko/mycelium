//! The policy declaration and its strength-tier legibility.
//!
//! A [`Policy`] is one declaration of *what an agent may do*; [`Policy::strength_report`]
//! discloses exactly which clause compiles to which guarantee tier, and
//! [`Policy::to_agent_policy`] compiles the Tier-B clauses into a `mycelium::AgentPolicy`
//! (pure, testable without an agent). Application onto a live node is in `apply`.

use std::sync::Arc;

use mycelium::{AgentPolicy, CapFilter};

/// The strength tier a policy clause compiles to (legibility — the design's core honesty).
///
/// Every clause of a [`Policy`] falls in exactly one tier; a guardrail that hid the
/// difference would over-claim. See `docs/plans/mycelium-guardrails.md` (2026-07-08
/// addendum) for the code-verified bindings behind each.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Strength {
    /// Tier C — hard prevention: an unauthorized action is *rejected at the provider*, not just
    /// detected. Applies to invocations of the node's own served capabilities (authorized_callers).
    HardPrevention,
    /// Tier A — structural prevention for an honest node (drop-before-handler at the boundary), but
    /// coarse (by group/scope) and self-imposed (a malicious node could ignore its own boundary).
    SelfImposedPrevention,
    /// Tier B — self-imposed, enforced at agent state transitions (AgentPolicy); a side effect not
    /// preceded by a policed transition is not caught. Legible, not hard.
    SelfImposedTransition,
}

impl Strength {
    /// A short, stable label for the tier — for logs and operator reports.
    pub fn label(&self) -> &'static str {
        match self {
            Strength::HardPrevention        => "Tier C — hard prevention (provider-rejected)",
            Strength::SelfImposedPrevention => "Tier A — self-imposed prevention (boundary drop)",
            Strength::SelfImposedTransition => "Tier B — self-imposed (state transition)",
        }
    }
}

/// One active clause of a [`Policy`], reported with its compiled strength tier.
///
/// Returned by [`Policy::strength_report`], one per *active* clause, so an operator sees
/// exactly what is hard-prevented vs self-imposed vs detection.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Clause {
    /// The builder name that set this clause (e.g. `"deny_tools"`).
    pub name: &'static str,
    /// The guarantee tier this clause compiles to.
    pub tier: Strength,
    /// A human-readable summary of the clause's content.
    pub detail: String,
}

/// A self-imposed, tier-labelled guardrail declaration.
///
/// One declaration of *what an agent may do* — which groups it acts within (Tier A), which
/// tools/budgets/approvals gate its state transitions (Tier B), and which callers may invoke
/// its guarded capabilities (Tier C). [`apply`](crate::apply) compiles it onto THIS node;
/// there is no remote authority — a node applies a policy to itself.
#[derive(Clone, Debug, Default)]
pub struct Policy {
    // Tier A — the boundary.
    groups: Vec<Arc<str>>,
    // Tier B — the AgentPolicy state-transition guards.
    allowed_tools: Vec<String>,
    denied_tools: Vec<String>,
    tool_budget: Option<usize>,
    max_turns: Option<usize>,
    require_approval_for: Vec<String>,
    required_capabilities: Vec<CapFilter>,
    // Tier C — the provider-side hard gate.
    authorized_callers: Vec<Arc<str>>,
}

impl Policy {
    /// An empty policy — permits everything; its [`strength_report`](Self::strength_report)
    /// is empty.
    pub fn new() -> Self {
        Self::default()
    }

    // ── Tier A — Boundary (drop-before-handler, coarse, self-imposed) ───────────

    /// Tier A. The groups the agent participates in (its receiver boundary). After
    /// [`apply`](crate::apply) the node structurally acts only on signals whose scope its
    /// boundary admits (`System`, `Individual(self)`, or a joined `Group`). Coarse (by
    /// scope, not by tool/argument) and self-imposed.
    pub fn act_within_groups<S: Into<Arc<str>>>(mut self, groups: impl IntoIterator<Item = S>) -> Self {
        self.groups = groups.into_iter().map(Into::into).collect();
        self
    }

    // ── Tier B — AgentPolicy (self-imposed, enforced at state transitions) ──────

    /// Tier B. Only the listed tools may be invoked (empty = all allowed). Enforced at the
    /// `→ Invoking{tool}` transition.
    pub fn allow_tools<S: Into<String>>(mut self, tools: impl IntoIterator<Item = S>) -> Self {
        self.allowed_tools = tools.into_iter().map(Into::into).collect();
        self
    }

    /// Tier B. Tools that are always blocked, regardless of the allow-list.
    pub fn deny_tools<S: Into<String>>(mut self, tools: impl IntoIterator<Item = S>) -> Self {
        self.denied_tools = tools.into_iter().map(Into::into).collect();
        self
    }

    /// Tier B. Maximum total tool calls per task; blocks any `→ Invoking` once exceeded.
    pub fn tool_budget(mut self, n: usize) -> Self {
        self.tool_budget = Some(n);
        self
    }

    /// Tier B. Maximum LLM turns per task; blocks any `→ Invoking` once exceeded.
    pub fn max_turns(mut self, n: usize) -> Self {
        self.max_turns = Some(n);
        self
    }

    /// Tier B. Tools that require a supervisor approval signal before `Invoking` is committed.
    pub fn require_approval_for<S: Into<String>>(mut self, tools: impl IntoIterator<Item = S>) -> Self {
        self.require_approval_for = tools.into_iter().map(Into::into).collect();
        self
    }

    /// Tier B. Capability filters that must each resolve to ≥ 1 provider before the agent may
    /// leave `Idle → Planning`.
    pub fn require_capabilities(mut self, filters: impl IntoIterator<Item = CapFilter>) -> Self {
        self.required_capabilities = filters.into_iter().collect();
        self
    }

    // ── Tier C — authorized_callers (provider-rejected hard prevention) ─────────

    /// Tier C. Node-ids or role names that may invoke this node's guarded capabilities. The
    /// real gate is invoke-time [`check_caller`](crate::check_caller); an empty list is open.
    /// Requires the `compliance` feature to enforce (and seal denials).
    pub fn authorized_callers<S: Into<Arc<str>>>(mut self, callers: impl IntoIterator<Item = S>) -> Self {
        self.authorized_callers = callers.into_iter().map(Into::into).collect();
        self
    }

    // ── Accessors (crate-internal — `apply` reads these) ────────────────────────

    pub(crate) fn groups(&self) -> &[Arc<str>] {
        &self.groups
    }

    pub(crate) fn authorized_callers_list(&self) -> &[Arc<str>] {
        &self.authorized_callers
    }

    // ── Legibility + compilation ────────────────────────────────────────────────

    /// One [`Clause`] per *active* clause, each tagged with the strength tier it compiles to —
    /// so an operator sees exactly what is hard-prevented (Tier C) vs self-imposed at the
    /// boundary (Tier A) vs self-imposed at a state transition (Tier B). The differentiator of
    /// this API: the honest disclosure of guarantee strength.
    pub fn strength_report(&self) -> Vec<Clause> {
        let mut out = Vec::new();

        // Tier A.
        if !self.groups.is_empty() {
            out.push(Clause {
                name: "act_within_groups",
                tier: Strength::SelfImposedPrevention,
                detail: format!("boundary admits groups: {}", join_arc(&self.groups)),
            });
        }

        // Tier B.
        if !self.allowed_tools.is_empty() {
            out.push(Clause {
                name: "allow_tools",
                tier: Strength::SelfImposedTransition,
                detail: format!("only these tools may be invoked: {}", self.allowed_tools.join(", ")),
            });
        }
        if !self.denied_tools.is_empty() {
            out.push(Clause {
                name: "deny_tools",
                tier: Strength::SelfImposedTransition,
                detail: format!("these tools are always blocked: {}", self.denied_tools.join(", ")),
            });
        }
        if let Some(n) = self.tool_budget {
            out.push(Clause {
                name: "tool_budget",
                tier: Strength::SelfImposedTransition,
                detail: format!("at most {n} tool calls per task"),
            });
        }
        if let Some(n) = self.max_turns {
            out.push(Clause {
                name: "max_turns",
                tier: Strength::SelfImposedTransition,
                detail: format!("at most {n} LLM turns per task"),
            });
        }
        if !self.require_approval_for.is_empty() {
            out.push(Clause {
                name: "require_approval_for",
                tier: Strength::SelfImposedTransition,
                detail: format!("supervisor approval required for: {}", self.require_approval_for.join(", ")),
            });
        }
        if !self.required_capabilities.is_empty() {
            let names: Vec<String> = self
                .required_capabilities
                .iter()
                .map(|f| format!("{}/{}", f.namespace, f.name))
                .collect();
            out.push(Clause {
                name: "require_capabilities",
                tier: Strength::SelfImposedTransition,
                detail: format!("planning blocked until these resolve: {}", names.join(", ")),
            });
        }

        // Tier C.
        if !self.authorized_callers.is_empty() {
            out.push(Clause {
                name: "authorized_callers",
                tier: Strength::HardPrevention,
                detail: format!("only these callers may invoke guarded capabilities: {}", join_arc(&self.authorized_callers)),
            });
        }

        out
    }

    /// Compile the Tier-B clauses into a `mycelium::AgentPolicy` — the object installed on the
    /// node's [`AgentStateMachine`](mycelium::AgentStateMachine). Pure: no agent required, so
    /// the mapping is unit-testable. Tier A (boundary) and Tier C (authorized_callers) are not
    /// AgentPolicy concerns and are applied separately in [`apply`](crate::apply).
    pub fn to_agent_policy(&self) -> AgentPolicy {
        AgentPolicy {
            allowed_tools: self.allowed_tools.clone(),
            denied_tools: self.denied_tools.clone(),
            tool_budget: self.tool_budget,
            max_turns: self.max_turns,
            require_approval_for: self.require_approval_for.clone(),
            required_capabilities: self.required_capabilities.clone(),
            ..AgentPolicy::default()
        }
    }
}

fn join_arc(items: &[Arc<str>]) -> String {
    items.iter().map(|s| s.as_ref()).collect::<Vec<_>>().join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_policy_reports_nothing() {
        assert!(Policy::new().strength_report().is_empty());
        let p = Policy::new().to_agent_policy();
        assert!(p.allowed_tools.is_empty());
        assert!(p.denied_tools.is_empty());
        assert_eq!(p.tool_budget, None);
        assert_eq!(p.max_turns, None);
    }

    #[test]
    fn report_tags_each_clause_with_its_tier() {
        let p = Policy::new()
            .act_within_groups(["ops"])
            .allow_tools(["search"])
            .deny_tools(["shell"])
            .tool_budget(5)
            .max_turns(3)
            .require_approval_for(["deploy"])
            .authorized_callers(["node-a"]);

        let report = p.strength_report();
        let by_name = |name: &str| report.iter().find(|c| c.name == name).map(|c| c.tier);

        // Tier A.
        assert_eq!(by_name("act_within_groups"), Some(Strength::SelfImposedPrevention));
        // Tier B.
        assert_eq!(by_name("allow_tools"), Some(Strength::SelfImposedTransition));
        assert_eq!(by_name("deny_tools"), Some(Strength::SelfImposedTransition));
        assert_eq!(by_name("tool_budget"), Some(Strength::SelfImposedTransition));
        assert_eq!(by_name("max_turns"), Some(Strength::SelfImposedTransition));
        assert_eq!(by_name("require_approval_for"), Some(Strength::SelfImposedTransition));
        // Tier C.
        assert_eq!(by_name("authorized_callers"), Some(Strength::HardPrevention));
    }

    #[test]
    fn report_omits_inactive_clauses() {
        let report = Policy::new().deny_tools(["shell"]).strength_report();
        assert_eq!(report.len(), 1);
        assert_eq!(report[0].name, "deny_tools");
        assert!(report[0].detail.contains("shell"));
    }

    #[test]
    fn to_agent_policy_maps_tier_b_fields() {
        let p = Policy::new()
            .allow_tools(["a", "b"])
            .deny_tools(["c"])
            .tool_budget(7)
            .max_turns(2)
            .require_approval_for(["risky"]);
        let ap = p.to_agent_policy();
        assert_eq!(ap.allowed_tools, vec!["a".to_string(), "b".to_string()]);
        assert_eq!(ap.denied_tools, vec!["c".to_string()]);
        assert_eq!(ap.tool_budget, Some(7));
        assert_eq!(ap.max_turns, Some(2));
        assert_eq!(ap.require_approval_for, vec!["risky".to_string()]);
    }

    #[test]
    fn tier_c_and_a_are_not_agent_policy_concerns() {
        // authorized_callers (Tier C) and groups (Tier A) never leak into the AgentPolicy.
        let ap = Policy::new()
            .authorized_callers(["node-a"])
            .act_within_groups(["ops"])
            .to_agent_policy();
        assert!(ap.allowed_tools.is_empty());
        assert!(ap.denied_tools.is_empty());
        assert_eq!(ap.tool_budget, None);
    }
}
