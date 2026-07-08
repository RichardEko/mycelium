//! Wedge ③ — artifact-aware resume, **demand half only**.
//!
//! A resumed graph's model dependencies follow it: the node picking up a suspended
//! thread declares `req/{node}/llm/{model}` (a gossiped [`RequirementHandle`]) and
//! structurally awaits a provider. The *install* half — provisioner, resource probes,
//! self-election, streaming the model in — is deployment wiring that already shipped in
//! `mycelium-wasm-host` (`model_deploy`); this crate only expresses the demand and
//! surfaces the provisioner's `llm/loading` progress tier (`pct` Integer attribute)
//! while the install runs.

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use mycelium::{CapFilter, GossipAgent, NodeId, RequirementHandle};

/// A declared model dependency. RAII: dropping retracts the gossiped requirement.
pub struct ModelDependency {
    agent: Arc<GossipAgent>,
    model: String,
    _req: RequirementHandle,
}

/// Why [`ModelDependency::await_ready`] gave up.
#[derive(Debug)]
pub enum ResumeError {
    /// No provider appeared within the timeout. `last_progress` is the last observed
    /// `llm/loading` percentage, if any install was visibly underway.
    Timeout { last_progress: Option<i64> },
}

impl fmt::Display for ResumeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ResumeError::Timeout { last_progress: Some(pct) } => {
                write!(f, "timed out awaiting a model provider (install at {pct}%)")
            }
            ResumeError::Timeout { last_progress: None } => {
                write!(f, "timed out awaiting a model provider (no install in progress)")
            }
        }
    }
}

impl std::error::Error for ResumeError {}

/// Declare that this node needs `llm/{model}` served somewhere on the mesh. The
/// requirement gossips at `interval` and evaporates when the returned handle drops.
pub fn require_model(agent: &Arc<GossipAgent>, model: &str, interval: Duration) -> ModelDependency {
    let req = agent
        .capabilities()
        .declare_requirement(CapFilter::new("llm", model), interval);
    ModelDependency { agent: Arc::clone(agent), model: model.to_string(), _req: req }
}

impl ModelDependency {
    /// The model this dependency names.
    pub fn model(&self) -> &str {
        &self.model
    }

    /// Live providers of `llm/{model}` in the local capability view.
    pub fn providers(&self) -> Vec<NodeId> {
        self.agent
            .capabilities()
            .resolve(&CapFilter::new("llm", self.model.as_str()))
            .into_iter()
            .map(|(n, _)| n)
            .collect()
    }

    /// Progress of an in-flight model install, if a provisioner advertises the
    /// `llm/loading` tier: the max `pct` Integer attribute across loading ads.
    pub fn loading_progress(&self) -> Option<i64> {
        self.agent
            .capabilities()
            .resolve(&CapFilter::new("llm", "loading"))
            .into_iter()
            .filter_map(|(_, cap)| match cap.attributes.get("pct") {
                Some(mycelium::CapValue::Integer(pct)) => Some(*pct),
                _ => None,
            })
            .max()
    }

    /// Structurally poll (~250 ms period) until at least one provider is live, or
    /// `timeout` elapses. Never a fixed sleep: returns as soon as the capability view
    /// shows a provider.
    pub async fn await_ready(&self, timeout: Duration) -> Result<Vec<NodeId>, ResumeError> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let providers = self.providers();
            if !providers.is_empty() {
                return Ok(providers);
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(ResumeError::Timeout { last_progress: self.loading_progress() });
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resume_error_display() {
        assert_eq!(
            ResumeError::Timeout { last_progress: Some(40) }.to_string(),
            "timed out awaiting a model provider (install at 40%)",
        );
        assert_eq!(
            ResumeError::Timeout { last_progress: None }.to_string(),
            "timed out awaiting a model provider (no install in progress)",
        );
    }
}
