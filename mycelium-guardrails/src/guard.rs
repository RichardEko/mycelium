//! Tier-C hard prevention — the provider-side `authorized_callers` gate plus denial sealing.
//!
//! This is the reusable gate the 2026-07-08 reassessment flagged (bindings #2/#3): the core
//! mesh RPC does **not** auto-enforce `authorized_callers`; a provider must check it in its own
//! serve loop, and today the only production site doing so is SkillRunner
//! (`src/bin/skillrunner/runner.rs`). [`check_caller`] composes into any `rpc_rx` loop;
//! [`guarded_rpc_serve`] spawns the loop for you. Both **seal a `Denied` audit record** with the
//! verified caller as principal — the "prove X was stopped" foundation. All behind `compliance`.

use std::future::Future;
use std::sync::Arc;

use mycelium::{AuditAction, AuditOutcome, Capability, GossipAgent, RpcRequest};
use tokio::task::JoinHandle;

use crate::apply::AppliedPolicy;

/// The outcome of a Tier-C caller check.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CallerVerdict {
    /// The caller is authorized — the provider should honor the invocation.
    Admitted,
    /// The caller is not authorized — a `Denied` record has been sealed; the provider should
    /// answer with an error, never honor the invocation.
    Denied,
}

impl AppliedPolicy {
    /// Stamp this policy's `authorized_callers` onto `cap` before advertising it, so resolvers
    /// see the allowlist. This is a *visibility* hint only — the authoritative gate is invoke-time
    /// [`check_caller`] / [`guarded_rpc_serve`], because a caller controls its own resolve.
    pub fn guard(&self, cap: Capability) -> Capability {
        cap.with_authorized_callers(self.authorized_callers().iter().cloned())
    }
}

/// Check whether `req`'s (signature-verified) sender may invoke a capability guarded by
/// `applied`'s allowlist. On denial, seals an `Invoke`/`Denied` audit record — verified
/// principal, `authorized_callers` reason — into the provider's tamper-evident chain before
/// returning [`CallerVerdict::Denied`]. Call this in a provider's own `rpc_rx` loop and answer
/// denied callers with an error.
///
/// An empty allowlist admits everyone (and seals nothing).
pub fn check_caller(applied: &AppliedPolicy, req: &RpcRequest) -> CallerVerdict {
    let agent = applied.agent();
    if agent.caller_authorized(req.sender(), applied.authorized_callers()) {
        metrics::counter!("mycelium_guardrails_admits_total").increment(1);
        CallerVerdict::Admitted
    } else {
        seal_denial(agent, req);
        CallerVerdict::Denied
    }
}

/// RAII guard over a spawned [`guarded_rpc_serve`] loop. Dropping it aborts the loop.
pub struct GuardHandle {
    task: JoinHandle<()>,
}

impl Drop for GuardHandle {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Serve RPC `kind` with a Tier-C gate in front of `handler`: each request is checked against
/// `applied`'s allowlist; authorized callers reach `handler`, unauthorized callers get a sealed
/// `Denied` record and an error reply — `handler` never runs for them. Returns a [`GuardHandle`]
/// whose drop aborts the loop.
///
/// `handler` receives the agent (to respond via `agent.service().rpc_respond`) and the admitted
/// request. Modelled on SkillRunner's reject-and-seal loop.
pub fn guarded_rpc_serve<F, Fut>(
    applied: &AppliedPolicy,
    kind: impl Into<Arc<str>>,
    handler: F,
) -> GuardHandle
where
    F: Fn(Arc<GossipAgent>, RpcRequest) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    let agent = Arc::clone(applied.agent());
    let allow: Vec<Arc<str>> = applied.authorized_callers().to_vec();
    let kind = kind.into();
    let handler = Arc::new(handler);

    let task = tokio::spawn(async move {
        let mut rx = agent.service().rpc_rx(Arc::clone(&kind));
        while let Some(req) = rx.recv().await {
            if agent.caller_authorized(req.sender(), &allow) {
                metrics::counter!("mycelium_guardrails_admits_total").increment(1);
                let h = Arc::clone(&handler);
                let a = Arc::clone(&agent);
                tokio::spawn(async move {
                    h(a, req).await;
                });
            } else {
                seal_denial(&agent, &req);
                let err = br#"{"error":"unauthorized: caller not in authorized_callers"}"#.to_vec();
                agent.service().rpc_respond(&req, err);
            }
        }
    });

    GuardHandle { task }
}

/// Seal one `Invoke`/`Denied` record with the verified caller as principal and the RPC kind as
/// target. Best-effort: a node without a tls identity cannot sign, so the seal is dropped (the
/// gate itself still denies).
fn seal_denial(agent: &Arc<GossipAgent>, req: &RpcRequest) {
    // Operator signal: how many unauthorized invokes the Tier-C gate stopped (visible on the
    // node's /metrics when the embedder enables mycelium's `metrics` feature; a no-op otherwise).
    metrics::counter!("mycelium_guardrails_denials_sealed_total").increment(1);
    let detail = format!(
        r#"{{"nonce":{},"reason":"authorized_callers"}}"#,
        req.nonce()
    );
    let _ = agent.audit(
        AuditAction::Invoke,
        req.sender().to_string(),
        req.kind().to_string(),
        AuditOutcome::Denied,
        Some(detail),
    );
}
