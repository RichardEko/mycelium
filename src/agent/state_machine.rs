//! Agent state machine: typed execution states, policy guards, and mesh-visible
//! state propagation via the KV + signal substrate.
//!
//! Obtain an [`AgentStateMachine`] via
//! [`GossipAgent::agent_state_machine`](crate::GossipAgent::agent_state_machine).

use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Weak,
    },
    time::Duration,
};
use bytes::Bytes;
use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use tokio::{sync::watch, task::JoinHandle};

use crate::CapFilter;
use crate::NodeId;
use crate::framing::{dispatch_gossip_try_send, ForwardHint, WireMessage, make_gossip_update};
use crate::signal::{SignalScope, signal_kind};
use crate::store::apply_and_notify;
use super::capability_ops::{resolve_filter_against_kv, subscribe_prefix_on_kv};
use super::helpers::emit_signal;
use super::TaskCtx;

// ── ExecutionState ────────────────────────────────────────────────────────────────

/// Execution state of an LLM agent node.
///
/// Propagated to the gossip mesh via KV key `agent/{node}/state` on every
/// committed [`AgentStateMachine::transition`] call. Remote observers can read
/// live states from that key prefix or use [`AgentStateMachine::watch_mesh_states`].
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ExecutionState {
    /// Agent is idle — no active task.
    Idle,
    /// Calling the LLM to plan the next action.
    Planning,
    /// Executing a tool call.
    Invoking { tool: String },
    /// Processing a tool result before the next planning step.
    Reflecting,
    /// Task checkpointed; waiting to be resumed.
    Suspended { task_id: String },
    /// Task completed successfully.
    Done,
    /// Task failed with a reason string.
    Failed { reason: String },
    /// Application-defined state.
    Custom(String),
}

impl ExecutionState {
    /// Serialize to the compact KV wire format used in `agent/{node}/state`.
    pub fn to_kv_str(&self) -> String {
        match self {
            ExecutionState::Idle               => "Idle".to_string(),
            ExecutionState::Planning           => "Planning".to_string(),
            ExecutionState::Reflecting         => "Reflecting".to_string(),
            ExecutionState::Done               => "Done".to_string(),
            ExecutionState::Invoking  { tool }    => format!("Invoking:{tool}"),
            ExecutionState::Failed    { reason }  => format!("Failed:{reason}"),
            ExecutionState::Suspended { task_id } => format!("Suspended:{task_id}"),
            ExecutionState::Custom(s)             => format!("Custom:{s}"),
        }
    }

    /// Parse from the KV wire format produced by [`to_kv_str`](Self::to_kv_str).
    pub fn from_kv_str(s: &str) -> Self {
        if s == "Idle"       { return ExecutionState::Idle; }
        if s == "Planning"   { return ExecutionState::Planning; }
        if s == "Reflecting" { return ExecutionState::Reflecting; }
        if s == "Done"       { return ExecutionState::Done; }
        if let Some(t)  = s.strip_prefix("Invoking:")   { return ExecutionState::Invoking   { tool:    t.to_string() }; }
        if let Some(r)  = s.strip_prefix("Failed:")     { return ExecutionState::Failed     { reason:  r.to_string() }; }
        if let Some(id) = s.strip_prefix("Suspended:")  { return ExecutionState::Suspended  { task_id: id.to_string() }; }
        if let Some(c)  = s.strip_prefix("Custom:")     { return ExecutionState::Custom(c.to_string()); }
        ExecutionState::Custom(s.to_string())
    }
}

impl std::fmt::Display for ExecutionState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.to_kv_str())
    }
}

// ── AgentPolicy ───────────────────────────────────────────────────────────────

/// Policy rules enforced before any state transition is committed.
///
/// All fields are optional — the zero value permits all transitions.
///
/// The active policy is written to `agent/{node}/policy` in the gossip KV store
/// whenever it changes (construction or [`AgentStateMachine::set_policy`]), so
/// supervisors and monitors can read the current rules from any mesh node.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct AgentPolicy {
    /// Maximum number of LLM turns (Planning→…→Reflecting cycles) per task.
    /// Blocks any `→ Invoking` transition once exceeded.
    pub max_turns: Option<usize>,

    /// Maximum total tool calls per task.
    /// Blocks any `→ Invoking` transition once exceeded.
    pub tool_budget: Option<usize>,

    /// If non-empty, only the listed tool names may be invoked.
    /// Empty = all tools allowed.
    pub allowed_tools: Vec<String>,

    /// Tool names that are always blocked, regardless of `allowed_tools`.
    pub denied_tools: Vec<String>,

    /// Auto-transition to `Failed` after spending longer than the specified
    /// duration in the given state. The timer is armed on each committed
    /// transition and cancelled when the agent moves to a new state.
    pub state_timeouts: HashMap<ExecutionState, Duration>,

    /// Tools that require a supervisor approval signal before `Invoking` is
    /// committed. Emits [`AGENT_APPROVE`](signal_kind::AGENT_APPROVE); blocks
    /// up to 30 s for [`AGENT_VETO`](signal_kind::AGENT_VETO).
    /// Silence within the window = approved; veto = [`PolicyViolation::ApprovalVetoed`].
    pub require_approval_for: Vec<String>,

    /// Blocks `Idle → Planning` until every filter resolves to ≥ 1 capability
    /// provider. Checked synchronously inside `transition(Planning)` via a
    /// live KV scan — fails fast if required dependencies are not yet advertised.
    pub required_capabilities: Vec<CapFilter>,
}

// ── PolicyViolation ───────────────────────────────────────────────────────────

/// A policy guard rejected a transition. The requested transition was **not** committed.
#[derive(Debug, Clone)]
pub enum PolicyViolation {
    RequiredCapabilityMissing(String),
    TurnBudgetExceeded,
    ToolBudgetExceeded,
    ToolNotAllowed(String),
    ToolDenied(String),
    ApprovalTimedOut(String),
    ApprovalVetoed(String),
}

impl std::fmt::Display for PolicyViolation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PolicyViolation::RequiredCapabilityMissing(s) => write!(f, "required capability missing: {s}"),
            PolicyViolation::TurnBudgetExceeded           => write!(f, "turn budget exceeded"),
            PolicyViolation::ToolBudgetExceeded           => write!(f, "tool budget exceeded"),
            PolicyViolation::ToolNotAllowed(t)            => write!(f, "tool not in allow-list: {t}"),
            PolicyViolation::ToolDenied(t)                => write!(f, "tool is denied: {t}"),
            PolicyViolation::ApprovalTimedOut(t)          => write!(f, "approval timed out for: {t}"),
            PolicyViolation::ApprovalVetoed(t)            => write!(f, "approval vetoed for: {t}"),
        }
    }
}

impl std::error::Error for PolicyViolation {}

// ── AgentStateMachine ─────────────────────────────────────────────────────────

/// Drives an agent through typed execution states with policy guards and
/// mesh-visible state propagation.
///
/// See [`AgentPolicy`] for available guards. See the module docs for the full
/// state transition diagram.
///
/// Obtain via [`GossipAgent::agent_state_machine`](crate::GossipAgent::agent_state_machine).
pub struct AgentStateMachine {
    ctx:            Arc<TaskCtx>,
    policy:         RwLock<AgentPolicy>,
    current:        Mutex<ExecutionState>,
    /// Incremented on every `→ Planning` transition; reset on Done/Failed/Idle.
    turn_count:     AtomicUsize,
    /// Incremented on every `→ Invoking` transition; reset on Done/Failed/Idle.
    call_count:     AtomicUsize,
    /// Task identifier used to namespace `agent/{node}/task/{id}/turn|calls` KV keys.
    task_id:        Mutex<Arc<str>>,
    /// Back-reference used by the state-timeout executor task to call `transition`.
    weak_self:      Weak<Self>,
    /// Handle for the active state-timeout task; replaced on every committed transition.
    timeout_handle: Mutex<Option<JoinHandle<()>>>,
}

impl AgentStateMachine {
    pub(super) fn new(ctx: Arc<TaskCtx>, policy: AgentPolicy) -> Arc<Self> {
        let sm = Arc::new_cyclic(|weak| Self {
            ctx,
            policy:         RwLock::new(policy),
            current:        Mutex::new(ExecutionState::Idle),
            turn_count:     AtomicUsize::new(0),
            call_count:     AtomicUsize::new(0),
            task_id:        Mutex::new(Arc::from("current")),
            weak_self:      Weak::clone(weak),
            timeout_handle: Mutex::new(None),
        });
        sm.write_policy_kv();
        sm
    }

    /// Replace the active policy atomically.
    ///
    /// The new policy takes effect on the next call to [`transition`](Self::transition).
    /// In-flight transitions that have already passed a guard complete under the old policy.
    /// Writes `agent/{node}/policy` to the gossip KV store so supervisors observe the change.
    pub fn set_policy(&self, policy: AgentPolicy) {
        *self.policy.write() = policy;
        self.write_policy_kv();
    }

    /// Read a snapshot of the current policy.
    pub fn policy(&self) -> AgentPolicy {
        self.policy.read().clone()
    }

    /// Set the task identifier used in the gossip KV counter keys
    /// (`agent/{node}/task/{id}/turn` and `agent/{node}/task/{id}/calls`).
    ///
    /// Call before transitioning to `Planning` to namespace a new task's counters.
    /// Defaults to `"current"` if never set.
    pub fn set_task_id(&self, id: impl Into<Arc<str>>) {
        *self.task_id.lock() = id.into();
    }

    /// Current state — O(1) in-memory read.
    ///
    /// The KV key `agent/{node}/state` is the durable, mesh-visible copy;
    /// this field is always in sync with it after a successful `transition`.
    pub fn state(&self) -> ExecutionState {
        self.current.lock().clone()
    }

    /// Attempt a state transition.
    ///
    /// Runs all policy guards. On success: updates in-memory state, writes
    /// `agent/{node}/state` to the gossip KV store, and emits an `agent.state`
    /// signal so the whole mesh observes the change. On failure: returns
    /// [`PolicyViolation`] and leaves state unchanged.
    ///
    /// **Commit discipline (M2 Run-28 fix):** guards are fast-failed against a
    /// snapshot, but the *authoritative* check happens at commit time inside
    /// [`try_commit`](Self::try_commit) — a validate-and-swap under the state
    /// lock, retried if the state moved (e.g. a timeout fired during the
    /// approval `await`). Budget counters are checked *and reserved* while the
    /// lock is held, so two racing transitions can never both pass the same
    /// last budget slot, and a state committed concurrently is never silently
    /// overwritten — the transition is re-validated against it instead.
    pub async fn transition(&self, to: ExecutionState) -> Result<(), PolicyViolation> {
        // Fast-fail guards against a snapshot (cheap early exit before the
        // approval round-trip; the commit re-validates authoritatively).
        let from_snapshot = self.current.lock().clone();
        self.check_sync_guards(&from_snapshot, &to)?;

        // Async guard: supervisor approval. The approval concerns the *tool*,
        // not the state, so it is requested once — not per commit retry.
        let needs_approval = if let ExecutionState::Invoking { tool } = &to {
            self.policy.read().require_approval_for.contains(tool)
        } else {
            false
        };
        if needs_approval
            && let ExecutionState::Invoking { tool } = &to {
                self.await_approval(tool).await?;
            }

        // Snapshot the budget limits before the commit loop: `try_commit` must
        // not take `policy` while holding `current` (single-lock discipline).
        let (max_turns, tool_budget) = {
            let p = self.policy.read();
            (p.max_turns, p.tool_budget)
        };

        // Validate-and-swap commit loop: re-read the state, re-run the guards
        // against it, and commit only if it is still the state we validated.
        let from = loop {
            let expected = self.current.lock().clone();
            self.check_sync_guards(&expected, &to)?;
            match self.try_commit(&expected, &to, max_turns, tool_budget)? {
                Some(from) => break from,
                None       => continue, // state moved between validate and lock — re-validate
            }
        };

        // Post-commit effects: mesh-visible counter KV writes (counters were
        // already reserved atomically inside try_commit).
        match &to {
            ExecutionState::Planning => {
                self.write_counter_kv("turn", self.turn_count.load(Ordering::Relaxed));
            }
            ExecutionState::Invoking { .. } => {
                self.write_counter_kv("calls", self.call_count.load(Ordering::Relaxed));
            }
            ExecutionState::Idle | ExecutionState::Done | ExecutionState::Failed { .. } => {
                self.write_counter_kv("turn", 0);
                self.write_counter_kv("calls", 0);
            }
            _ => {}
        }

        self.write_kv_and_signal(&from, &to);

        // Cancel any prior timeout; arm a new one if policy specifies one for this state.
        if let Some(h) = self.timeout_handle.lock().take() { h.abort(); }
        if let Some(dur) = self.policy.read().state_timeouts.get(&to).copied() {
            let weak = Weak::clone(&self.weak_self);
            let state_snap = to.clone();
            let h = tokio::spawn(async move {
                tokio::time::sleep(dur).await;
                // After sleeping, call the sync commit path — never await on `transition()`
                // from a spawned task (the returned future is !Send due to internal guards).
                if let Some(sm) = weak.upgrade() {
                    sm.force_failed_transition(&state_snap, dur);
                }
            });
            *self.timeout_handle.lock() = Some(h);
        }

        Ok(())
    }

    /// Returns a `watch::Receiver` whose value is the current snapshot of all
    /// agent states visible in the mesh (read from `agent/*/state` KV entries).
    ///
    /// The receiver is updated whenever any `agent/` KV key changes. Callers
    /// typically `await changed()` then read the new value.
    pub fn watch_mesh_states(&self) -> watch::Receiver<Vec<(NodeId, ExecutionState)>> {
        let kv = Arc::clone(&self.ctx.kv_state);
        let initial = scan_agent_states(&kv);
        let (tx, rx) = watch::channel(initial);
        let mut change_rx = subscribe_prefix_on_kv(&kv, Arc::from("agent/"));
        tokio::spawn(async move {
            loop {
                if change_rx.changed().await.is_err() { break; }
                let states = scan_agent_states(&kv);
                if tx.send(states).is_err() { break; }
            }
        });
        rx
    }

    // ─── private ──────────────────────────────────────────────────────────────

    /// Validate-and-swap commit: under the state lock, verifies the state is still
    /// `expected`, re-checks the budget guards against the *current* counters, commits
    /// `to`, and reserves the counters — one atomic step. Returns `Ok(Some(from))` on
    /// commit, `Ok(None)` when the state changed since `expected` was read (the caller
    /// re-validates and retries), `Err` when a budget is exhausted.
    ///
    /// Budget counters only move while this lock is held (here and in
    /// [`force_failed_transition`](Self::force_failed_transition)), which is what makes
    /// the check-then-reserve atomic — the M2 Run-28 race (two approval-gated
    /// transitions both passing `tool_budget = 1`) is closed by this.
    /// `max_turns` / `tool_budget` are passed in pre-read: never take `policy` while
    /// holding `current` (single-lock discipline, CLAUDE.md lock-order table).
    fn try_commit(
        &self,
        expected:    &ExecutionState,
        to:          &ExecutionState,
        max_turns:   Option<usize>,
        tool_budget: Option<usize>,
    ) -> Result<Option<ExecutionState>, PolicyViolation> {
        let mut guard = self.current.lock();
        if *guard != *expected {
            return Ok(None);
        }
        if matches!(to, ExecutionState::Invoking { .. }) {
            if let Some(max) = max_turns
                && self.turn_count.load(Ordering::Relaxed) >= max {
                    return Err(PolicyViolation::TurnBudgetExceeded);
                }
            if let Some(max) = tool_budget
                && self.call_count.load(Ordering::Relaxed) >= max {
                    return Err(PolicyViolation::ToolBudgetExceeded);
                }
        }
        let from = std::mem::replace(&mut *guard, to.clone());
        match to {
            ExecutionState::Planning        => { self.turn_count.fetch_add(1, Ordering::Relaxed); }
            ExecutionState::Invoking { .. } => { self.call_count.fetch_add(1, Ordering::Relaxed); }
            ExecutionState::Idle | ExecutionState::Done | ExecutionState::Failed { .. } => {
                self.turn_count.store(0, Ordering::Relaxed);
                self.call_count.store(0, Ordering::Relaxed);
            }
            _ => {}
        }
        Ok(Some(from))
    }

    fn check_sync_guards(&self, from: &ExecutionState, to: &ExecutionState) -> Result<(), PolicyViolation> {
        let policy = self.policy.read();

        // required_capabilities: block Idle → Planning until all filters resolve
        if *from == ExecutionState::Idle
            && let ExecutionState::Planning = to {
                for filter in &policy.required_capabilities {
                    if resolve_filter_against_kv(&self.ctx.kv_state, filter).is_empty() {
                        return Err(PolicyViolation::RequiredCapabilityMissing(
                            format!("{}/{}", filter.namespace, filter.name),
                        ));
                    }
                }
            }

        if let ExecutionState::Invoking { tool } = &to {
            if let Some(max) = policy.max_turns
                && self.turn_count.load(Ordering::Relaxed) >= max {
                    return Err(PolicyViolation::TurnBudgetExceeded);
                }
            if let Some(max) = policy.tool_budget
                && self.call_count.load(Ordering::Relaxed) >= max {
                    return Err(PolicyViolation::ToolBudgetExceeded);
                }
            if !policy.allowed_tools.is_empty() && !policy.allowed_tools.contains(tool) {
                return Err(PolicyViolation::ToolNotAllowed(tool.clone()));
            }
            if policy.denied_tools.contains(tool) {
                return Err(PolicyViolation::ToolDenied(tool.clone()));
            }
        }

        Ok(())
    }

    /// Emits `AGENT_APPROVE` and waits up to 30 s for a matching `AGENT_VETO`.
    /// Silence = approved (no violation). Explicit veto = `ApprovalVetoed`.
    async fn await_approval(&self, tool: &str) -> Result<(), PolicyViolation> {
        let mut veto_rx = self.ctx.signal_handlers.register(signal_kind::AGENT_VETO.into());
        let payload = serde_json::json!({
            "tool": tool,
            "node": self.ctx.node_id.to_string(),
        });
        emit_signal(
            &self.ctx,
            signal_kind::AGENT_APPROVE.into(),
            SignalScope::Cluster,
            Bytes::from(payload.to_string()),
        );
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        while let Ok(Some(sig)) = tokio::time::timeout_at(deadline, veto_rx.recv()).await {
            if let Ok(v) = serde_json::from_slice::<serde_json::Value>(&sig.payload)
                && v.get("tool").and_then(|t| t.as_str()) == Some(tool) {
                    return Err(PolicyViolation::ApprovalVetoed(tool.to_string()));
                }
            // Veto for a different tool — keep waiting
        }
        Ok(())
    }

    fn write_kv_and_signal(&self, from: &ExecutionState, to: &ExecutionState) {
        let state_str = to.to_kv_str();
        let key: Arc<str> = format!("agent/{}/state", self.ctx.node_id).into();
        let update = make_gossip_update(
            &self.ctx.node_id,
            self.ctx.default_ttl,
            key,
            Bytes::copy_from_slice(state_str.as_bytes()),
            false,
            &self.ctx.hlc,
        );
        apply_and_notify(&self.ctx.kv_state, &update);
        dispatch_gossip_try_send(
            &self.ctx.gossip_txs,
            WireMessage::Data(update),
            self.ctx.node_id.id_hash(),
            ForwardHint::All,
            &self.ctx.kv_state.dropped_frames,
        );
        let payload = serde_json::json!({
            "node": self.ctx.node_id.to_string(),
            "from": from.to_kv_str(),
            "to":   &state_str,
        });
        emit_signal(
            &self.ctx,
            signal_kind::AGENT_STATE.into(),
            SignalScope::Cluster,
            Bytes::from(payload.to_string()),
        );
    }

    /// Bypass-policy commit used exclusively by the state-timeout executor.
    ///
    /// Only commits if `self.state() == expected` to guard against races where
    /// the agent already transitioned away before the timer fired.
    fn force_failed_transition(&self, expected: &ExecutionState, dur: Duration) {
        let to = ExecutionState::Failed {
            reason: format!("timeout after {dur:?} in state {}", expected.to_kv_str()),
        };
        let from = {
            let mut guard = self.current.lock();
            if *guard != *expected { return; }
            let from = std::mem::replace(&mut *guard, to.clone());
            // Counters only move under the state lock (see try_commit).
            self.turn_count.store(0, Ordering::Relaxed);
            self.call_count.store(0, Ordering::Relaxed);
            from
        };
        self.write_counter_kv("turn", 0);
        self.write_counter_kv("calls", 0);
        self.write_kv_and_signal(&from, &to);
        if let Some(h) = self.timeout_handle.lock().take() { h.abort(); }
    }

    fn write_counter_kv(&self, suffix: &str, value: usize) {
        let task_id = self.task_id.lock().clone();
        let key: Arc<str> = format!("agent/{}/task/{}/{}", self.ctx.node_id, task_id, suffix).into();
        let update = make_gossip_update(
            &self.ctx.node_id,
            self.ctx.default_ttl,
            key,
            Bytes::from(value.to_string()),
            false,
            &self.ctx.hlc,
        );
        apply_and_notify(&self.ctx.kv_state, &update);
        dispatch_gossip_try_send(
            &self.ctx.gossip_txs,
            WireMessage::Data(update),
            self.ctx.node_id.id_hash(),
            ForwardHint::All,
            &self.ctx.kv_state.dropped_frames,
        );
    }

    fn write_policy_kv(&self) {
        let encoded = match serde_json::to_vec(&*self.policy.read()) {
            Ok(v) => Bytes::from(v),
            Err(_) => return,
        };
        let key: Arc<str> = format!("agent/{}/policy", self.ctx.node_id).into();
        let update = make_gossip_update(
            &self.ctx.node_id,
            self.ctx.default_ttl,
            key,
            encoded,
            false,
            &self.ctx.hlc,
        );
        apply_and_notify(&self.ctx.kv_state, &update);
        dispatch_gossip_try_send(
            &self.ctx.gossip_txs,
            WireMessage::Data(update),
            self.ctx.node_id.id_hash(),
            ForwardHint::All,
            &self.ctx.kv_state.dropped_frames,
        );
    }
}

// ── helpers ───────────────────────────────────────────────────────────────────

impl Drop for AgentStateMachine {
    fn drop(&mut self) {
        if let Some(h) = self.timeout_handle.get_mut().take() {
            h.abort();
        }
    }
}

/// Snapshot all `agent/{node}/state` entries from the local KV store.
fn scan_agent_states(kv: &crate::store::KvState) -> Vec<(NodeId, ExecutionState)> {
    let guard = kv.store.pin();
    guard
        .iter()
        .filter_map(|(key, entry)| {
            if !key.starts_with("agent/") || !key.ends_with("/state") {
                return None;
            }
            let data = entry.data.as_ref()?;
            // Strip "agent/" prefix and "/state" suffix to get the node_id string.
            let inner = key.get("agent/".len()..key.len() - "/state".len())?;
            let node_id: NodeId = inner.parse().ok()?;
            let state = ExecutionState::from_kv_str(std::str::from_utf8(data).unwrap_or(""));
            Some((node_id, state))
        })
        .collect()
}

// ── tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kv_roundtrip() {
        let cases = [
            ExecutionState::Idle,
            ExecutionState::Planning,
            ExecutionState::Reflecting,
            ExecutionState::Done,
            ExecutionState::Invoking   { tool:    "weather".into() },
            ExecutionState::Failed     { reason:  "budget exceeded".into() },
            ExecutionState::Suspended  { task_id: "task-42".into() },
            ExecutionState::Custom("my-state".into()),
        ];
        for s in &cases {
            assert_eq!(*s, ExecutionState::from_kv_str(&s.to_kv_str()), "roundtrip failed for {s:?}");
        }
    }

    #[tokio::test]
    async fn policy_tool_budget() {
        let agent = crate::lib_tests::make_agent_for_sm_tests();
        let policy = AgentPolicy { tool_budget: Some(2), ..Default::default() };
        let sm = agent.agent_state_machine(policy);
        sm.transition(ExecutionState::Planning).await.unwrap();
        sm.transition(ExecutionState::Invoking { tool: "a".into() }).await.unwrap();
        sm.transition(ExecutionState::Reflecting).await.unwrap();
        sm.transition(ExecutionState::Invoking { tool: "b".into() }).await.unwrap();
        sm.transition(ExecutionState::Reflecting).await.unwrap();
        // Third call exceeds budget
        let err = sm.transition(ExecutionState::Invoking { tool: "c".into() }).await;
        assert!(matches!(err, Err(PolicyViolation::ToolBudgetExceeded)), "expected ToolBudgetExceeded");
    }

    #[tokio::test]
    async fn policy_tool_denied() {
        let agent = crate::lib_tests::make_agent_for_sm_tests();
        let policy = AgentPolicy {
            denied_tools: vec!["dangerous".into()],
            ..Default::default()
        };
        let sm = agent.agent_state_machine(policy);
        sm.transition(ExecutionState::Planning).await.unwrap();
        let err = sm.transition(ExecutionState::Invoking { tool: "dangerous".into() }).await;
        assert!(matches!(err, Err(PolicyViolation::ToolDenied(_))), "expected ToolDenied");
        let ok = sm.transition(ExecutionState::Invoking { tool: "safe".into() }).await;
        assert!(ok.is_ok());
    }

    #[tokio::test]
    async fn kv_written_on_transition() {
        let agent = crate::lib_tests::make_agent_for_sm_tests();
        let sm = agent.agent_state_machine(AgentPolicy::default());
        sm.transition(ExecutionState::Planning).await.unwrap();
        let key = format!("agent/{}/state", agent.node_id());
        let val = agent.kv().get(&key);
        assert_eq!(val.as_deref().map(|b| std::str::from_utf8(b).unwrap_or("")), Some("Planning"));
    }

    #[tokio::test]
    async fn counter_kv_written() {
        let agent = crate::lib_tests::make_agent_for_sm_tests();
        let sm = agent.agent_state_machine(AgentPolicy::default());
        let node = agent.node_id();

        sm.transition(ExecutionState::Planning).await.unwrap();
        sm.transition(ExecutionState::Invoking { tool: "a".into() }).await.unwrap();
        sm.transition(ExecutionState::Reflecting).await.unwrap();
        sm.transition(ExecutionState::Planning).await.unwrap();
        sm.transition(ExecutionState::Invoking { tool: "b".into() }).await.unwrap();
        sm.transition(ExecutionState::Done).await.unwrap();

        let kv_turn  = agent.kv().get(&format!("agent/{node}/task/current/turn"));
        let kv_calls = agent.kv().get(&format!("agent/{node}/task/current/calls"));
        let read = |v: Option<bytes::Bytes>| -> usize {
            std::str::from_utf8(&v.unwrap()).unwrap().parse().unwrap()
        };
        // Done resets both to 0
        assert_eq!(read(kv_turn),  0);
        assert_eq!(read(kv_calls), 0);

        // Mid-task values: verify by inspecting after second Planning (turn=2)
        let sm2 = agent.agent_state_machine(AgentPolicy::default());
        sm2.transition(ExecutionState::Planning).await.unwrap();
        sm2.transition(ExecutionState::Planning).await.unwrap();
        let kv_turn2 = agent.kv().get(&format!("agent/{node}/task/current/turn"));
        assert_eq!(read(kv_turn2), 2);
    }

    #[tokio::test]
    async fn state_timeout_fires() {
        let agent = crate::lib_tests::make_agent_for_sm_tests();
        let mut policy = AgentPolicy::default();
        policy.state_timeouts.insert(ExecutionState::Planning, Duration::from_millis(50));
        let sm = agent.agent_state_machine(policy);

        sm.transition(ExecutionState::Planning).await.unwrap();
        // Sleep well past the 50 ms timeout so the spawned task has time to run.
        tokio::time::sleep(Duration::from_millis(150)).await;

        assert!(
            matches!(sm.state(), ExecutionState::Failed { .. }),
            "expected Failed after timeout, got {:?}", sm.state()
        );
    }

    #[tokio::test]
    async fn state_timeout_cancelled_on_transition() {
        let agent = crate::lib_tests::make_agent_for_sm_tests();
        let mut policy = AgentPolicy::default();
        policy.state_timeouts.insert(ExecutionState::Planning, Duration::from_millis(100));
        let sm = agent.agent_state_machine(policy);

        sm.transition(ExecutionState::Planning).await.unwrap();
        // Transition away well before the 100 ms timeout fires.
        tokio::time::sleep(Duration::from_millis(20)).await;
        sm.transition(ExecutionState::Invoking { tool: "x".into() }).await.unwrap();
        // Sleep well past the original deadline — the cancelled task must not fire.
        tokio::time::sleep(Duration::from_millis(250)).await;

        assert!(
            !matches!(sm.state(), ExecutionState::Failed { .. }),
            "timeout fired after transition away; state is {:?}", sm.state()
        );
    }

    #[tokio::test]
    async fn set_policy_takes_effect_and_writes_kv() {
        let agent = crate::lib_tests::make_agent_for_sm_tests();
        let sm = agent.agent_state_machine(AgentPolicy::default());

        // Initial policy is written to KV at construction
        let policy_key = format!("agent/{}/policy", agent.node_id());
        assert!(agent.kv().get(&policy_key).is_some(), "policy KV not written at construction");

        // Tighten: deny "dangerous"
        sm.set_policy(AgentPolicy {
            denied_tools: vec!["dangerous".into()],
            ..Default::default()
        });

        // KV is updated
        let kv_bytes = agent.kv().get(&policy_key).expect("policy KV missing after set_policy");
        let kv_policy: AgentPolicy = serde_json::from_slice(&kv_bytes).expect("policy KV not valid JSON");
        assert_eq!(kv_policy.denied_tools, vec!["dangerous".to_string()]);

        // New policy is enforced on next transition
        sm.transition(ExecutionState::Planning).await.unwrap();
        let err = sm.transition(ExecutionState::Invoking { tool: "dangerous".into() }).await;
        assert!(matches!(err, Err(PolicyViolation::ToolDenied(_))));
    }

    /// M2 Run-28 probe (dim 9 — concurrency), **flipped to a regression gate**
    /// same-day: `transition()` was check-then-act (budget counters read before
    /// the approval `await`, incremented only after commit; commit never
    /// re-read `current`), so two approval-gated `Invoking` transitions racing
    /// through the await both passed `tool_budget = 1`. Fixed by `try_commit`:
    /// a validate-and-swap under the state lock with budget check + reserve as
    /// one atomic step. Exactly one racer may now win the last budget slot; the
    /// loser gets `ToolBudgetExceeded`.
    #[tokio::test(start_paused = true)]
    async fn tool_budget_enforced_under_concurrent_approval_gated_transitions() {
        let agent = crate::lib_tests::make_agent_for_sm_tests();
        let policy = AgentPolicy {
            tool_budget: Some(1),
            require_approval_for: vec!["a".into(), "b".into()],
            ..Default::default()
        };
        let sm = agent.agent_state_machine(policy);
        sm.transition(ExecutionState::Planning).await.unwrap();

        // Both futures pass the fast-fail check (call_count = 0 < 1) and park on
        // the 30 s approval window (paused time auto-advances) — the commit loop
        // must serialise them.
        let (r1, r2) = tokio::join!(
            sm.transition(ExecutionState::Invoking { tool: "a".into() }),
            sm.transition(ExecutionState::Invoking { tool: "b".into() }),
        );
        let ok = u32::from(r1.is_ok()) + u32::from(r2.is_ok());
        assert_eq!(
            ok, 1,
            "tool_budget=1 must admit exactly one of two racing Invoking transitions, got {ok} \
             (r1={r1:?}, r2={r2:?})"
        );
        let (winner_tool, loser) = if r1.is_ok() { ("a", r2) } else { ("b", r1) };
        assert!(
            matches!(loser, Err(PolicyViolation::ToolBudgetExceeded)),
            "the losing transition must fail with ToolBudgetExceeded, got {loser:?}"
        );
        assert_eq!(sm.state(), ExecutionState::Invoking { tool: winner_tool.into() });
    }
}
