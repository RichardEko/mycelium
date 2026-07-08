//! Wedge ① — capability-routed inference: a load-aware routing policy over the mesh.
//!
//! Capability **resolution is load-blind** (`resolve` ranks by freshness/attributes/
//! locality only — an overloaded node's entry ages out, nothing more), so this module is
//! the routing layer the substrate deliberately does not bake in: *resolve → drop opaque
//! nodes → rank by pheromone fill → fail over down the candidate list.*
//!
//! Convention (bound in `docs/plans/mycelium-reason.md`, 2026-07-08 addendum): **a model
//! is a prompt skill** — capability `llm/{model-id}` via `register_prompt_skill`
//! (matching the `model_deploy` precedent) — plus a parallel **attributed metadata ad**
//! `llm-meta/{model-id}` (ctx window, family, extras). The second ad exists because
//! re-advertising the same `(node, ns, name)` key with attributes would LWW-churn
//! against the skill's own persist task.

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use mycelium::signal::signal_kind;
use mycelium::{CapConstraint, CapFilter, GossipAgent, NodeId};

use crate::trace::TraceRecorder;

// ── Serve side (feature `llm`) ────────────────────────────────────────────────

/// What a serving node says about its model — the payload of the `llm-meta/{model}` ad.
#[cfg(feature = "llm")]
pub struct ModelProfile {
    /// Model id — becomes the capability name in both `llm/{model}` and `llm-meta/{model}`.
    pub model: String,
    /// Context window in tokens (advertised as an `Integer` attribute `ctx_window`).
    pub ctx_window: Option<i64>,
    /// Model family (advertised as a `Text` attribute `family`).
    pub family: Option<String>,
    /// Additional typed attributes, advertised as-is.
    pub extra: Vec<(String, mycelium::CapValue)>,
}

/// RAII registration for a served model: the prompt skill + the metadata ad.
/// Dropping retracts both (skill dispatch entry, `llm/…` cap, `llm-meta/…` cap).
#[cfg(feature = "llm")]
pub struct ModelReg {
    _skill: mycelium::PromptSkillHandle,
    _meta: mycelium::CapabilityReg,
}

/// Serve `profile.model` on this node: register the prompt skill (capability
/// `llm/{model}`, template in KV, `llm.invoke` dispatch) and advertise the parallel
/// attributed `llm-meta/{model}` ad that [`ModelQuery::constraints`] are tested against.
#[cfg(feature = "llm")]
pub async fn serve_model(
    agent: &Arc<GossipAgent>,
    profile: ModelProfile,
    template: mycelium::PromptTemplate,
    backend: Arc<dyn mycelium::LlmBackend>,
) -> Result<ModelReg, mycelium::PromptSkillError> {
    let skill = agent.llm().register_prompt_skill("llm", &profile.model, template, backend).await?;

    let mut meta = mycelium::Capability::new("llm-meta", profile.model.as_str());
    if let Some(ctx) = profile.ctx_window {
        meta = meta.with("ctx_window", mycelium::CapValue::Integer(ctx));
    }
    if let Some(family) = &profile.family {
        meta = meta.with("family", mycelium::CapValue::Text(Arc::from(family.as_str())));
    }
    for (k, v) in profile.extra {
        meta = meta.with(k.as_str(), v);
    }
    let meta_reg = agent.capabilities().advertise_capability(meta, Duration::from_secs(30));

    Ok(ModelReg { _skill: skill, _meta: meta_reg })
}

// ── Call side (core-only — no feature gate) ───────────────────────────────────

/// Routing policy knobs.
#[derive(Clone, Debug)]
pub struct RouterConfig {
    /// How many candidates to try before giving up (failover depth).
    pub max_attempts: usize,
    /// Per-attempt RPC timeout.
    pub call_timeout: Duration,
    /// Freshness window for opacity + pheromone load reads.
    pub load_max_age: Duration,
}

impl Default for RouterConfig {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            call_timeout: Duration::from_secs(30),
            load_max_age: Duration::from_secs(10),
        }
    }
}

/// What to route: a model id plus optional constraints over the `llm-meta/{model}` ad
/// (e.g. `("ctx_window", CapConstraint::Gte(CapValue::Integer(32_768)))`). Empty
/// constraints skip the metadata lookup entirely.
#[derive(Clone, Debug)]
pub struct ModelQuery {
    pub model: String,
    pub constraints: Vec<(String, CapConstraint)>,
}

impl ModelQuery {
    pub fn new(model: impl Into<String>) -> Self {
        Self { model: model.into(), constraints: Vec::new() }
    }
}

/// A successfully routed inference.
#[derive(Debug, Clone)]
pub struct Routed {
    pub output: String,
    pub model_used: String,
    pub tokens_used: u32,
    /// The provider that answered.
    pub provider: NodeId,
    /// 1-based attempt index (1 = first candidate answered).
    pub attempt: usize,
}

/// Why routing failed.
#[derive(Debug)]
pub enum RouteError {
    /// No live provider advertises `llm/{model}` (after constraint + opacity filtering).
    NoProvider,
    /// Every attempted candidate failed; per-node error strings in attempt order.
    Exhausted(Vec<(NodeId, String)>),
}

impl fmt::Display for RouteError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RouteError::NoProvider => write!(f, "no provider for the requested model"),
            RouteError::Exhausted(fails) => {
                write!(f, "all {} attempted provider(s) failed: ", fails.len())?;
                let mut first = true;
                for (node, err) in fails {
                    if !first {
                        write!(f, "; ")?;
                    }
                    write!(f, "{node}: {err}")?;
                    first = false;
                }
                Ok(())
            }
        }
    }
}

impl std::error::Error for RouteError {}

/// Load-aware, failover-capable router over `llm/{model}` providers. Core-only: a node
/// needs no `llm` feature (and no local backend) to *call* models served elsewhere.
pub struct InferenceRouter {
    agent: Arc<GossipAgent>,
    cfg: RouterConfig,
}

impl InferenceRouter {
    pub fn new(agent: Arc<GossipAgent>, cfg: RouterConfig) -> Self {
        Self { agent, cfg }
    }

    /// The ranked candidate list for `q`: resolve `llm/{model}`, intersect with the
    /// `llm-meta` ad when constraints are given, drop opaque nodes, then sort by
    /// (pheromone fill, node id) — the id tiebreak makes the order deterministic.
    /// Fill is the max `fill_ratio` across the node's fresh load entries; a node with
    /// no pheromone trail is 0.0 (transparent).
    pub fn candidates(&self, q: &ModelQuery) -> Vec<(NodeId, f32)> {
        let caps = self.agent.capabilities();
        let mut nodes: Vec<NodeId> =
            caps.resolve(&CapFilter::new("llm", q.model.as_str())).into_iter().map(|(n, _)| n).collect();

        if !q.constraints.is_empty() {
            let mut meta_filter = CapFilter::new("llm-meta", q.model.as_str());
            for (attr, c) in &q.constraints {
                meta_filter = meta_filter.with(attr.as_str(), c.clone());
            }
            let meta_nodes: Vec<NodeId> =
                caps.resolve(&meta_filter).into_iter().map(|(n, _)| n).collect();
            nodes.retain(|n| meta_nodes.contains(n));
        }

        nodes.retain(|n| !caps.is_node_opaque(n, signal_kind::LLM_INVOKE, self.cfg.load_max_age));

        // Pheromone fill per node: max fill_ratio over that node's fresh load entries.
        let load = caps.peer_load(self.cfg.load_max_age);
        let mut ranked: Vec<(NodeId, f32)> = nodes
            .into_iter()
            .map(|n| {
                let ns = n.to_string();
                let fill = load
                    .iter()
                    .filter(|(node, _, _)| node.as_ref() == ns)
                    .map(|(_, _, s)| s.fill_ratio)
                    .fold(0.0_f32, f32::max);
                (n, fill)
            })
            .collect();
        ranked.sort_by(|(na, fa), (nb, fb)| {
            fa.total_cmp(fb).then_with(|| na.to_string().cmp(&nb.to_string()))
        });
        ranked
    }

    /// Route one inference: walk [`candidates`](Self::candidates) up to
    /// `max_attempts`, one RPC per candidate, failing over on error replies and RPC
    /// timeouts. When `trace` is given, the route decision is recorded once and each
    /// attempt as an `llm_call` event.
    pub async fn call(
        &self,
        q: &ModelQuery,
        input: &str,
        context: &HashMap<String, String>,
        trace: Option<&TraceRecorder>,
    ) -> Result<Routed, RouteError> {
        let candidates = self.candidates(q);
        let Some((chosen, _)) = candidates.first() else {
            return Err(RouteError::NoProvider);
        };
        if let Some(t) = trace {
            t.route(&q.model, &candidates, chosen);
        }

        // Same JSON the core's `llm.invoke` dispatch parses and `gw_llm_call` speaks
        // over the gateway (the structs are pub(crate) in core; the shape is wire-public).
        let request = serde_json::json!({
            "prompt": format!("llm/{}", q.model),
            "input": input,
            "context": context,
        });
        let payload = Bytes::from(request.to_string().into_bytes());

        let mut failures: Vec<(NodeId, String)> = Vec::new();
        for (attempt, (node, _fill)) in candidates.iter().take(self.cfg.max_attempts).enumerate() {
            let started = std::time::Instant::now();
            let reply = self
                .agent
                .service()
                .rpc_call(node.clone(), signal_kind::LLM_INVOKE, payload.clone(), self.cfg.call_timeout)
                .await;
            let duration_ms = started.elapsed().as_millis() as u64;

            let err = match reply {
                Ok(bytes) => match serde_json::from_slice::<serde_json::Value>(&bytes) {
                    Ok(v) => {
                        if let Some(e) = v.get("error").and_then(|e| e.as_str()) {
                            let detail = v.get("detail").and_then(|d| d.as_str()).unwrap_or("");
                            format!("{e}: {detail}")
                        } else {
                            let output = v["output"].as_str().unwrap_or_default().to_owned();
                            let model_used = v["model_used"].as_str().unwrap_or_default().to_owned();
                            let tokens_used = v["tokens_used"].as_u64().unwrap_or(0) as u32;
                            if let Some(t) = trace {
                                t.llm_call(node, true, tokens_used, duration_ms, None);
                            }
                            return Ok(Routed {
                                output,
                                model_used,
                                tokens_used,
                                provider: node.clone(),
                                attempt: attempt + 1,
                            });
                        }
                    }
                    Err(e) => format!("undecodable reply: {e}"),
                },
                Err(e) => e.to_string(),
            };
            if let Some(t) = trace {
                t.llm_call(node, false, 0, duration_ms, Some(&err));
            }
            failures.push((node.clone(), err));
        }
        Err(RouteError::Exhausted(failures))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The candidate ordering contract, tested as a pure sort (the same comparator
    /// `candidates()` applies): fill ascending, then node-id string for determinism.
    #[test]
    fn candidate_ordering_is_deterministic() {
        let n = |p: u16| NodeId::new("127.0.0.1", p).unwrap();
        let mut ranked = vec![
            (n(9003), 0.5_f32),
            (n(9002), 0.0),
            (n(9001), 0.5),
            (n(9000), 0.0),
        ];
        ranked.sort_by(|(na, fa), (nb, fb)| {
            fa.total_cmp(fb).then_with(|| na.to_string().cmp(&nb.to_string()))
        });
        let order: Vec<String> = ranked.iter().map(|(n, _)| n.to_string()).collect();
        assert_eq!(
            order,
            vec!["127.0.0.1:9000", "127.0.0.1:9002", "127.0.0.1:9001", "127.0.0.1:9003"],
        );
        // Re-sorting an already-sorted list is a fixpoint (stability under repetition).
        let again = {
            let mut r = ranked.clone();
            r.sort_by(|(na, fa), (nb, fb)| {
                fa.total_cmp(fb).then_with(|| na.to_string().cmp(&nb.to_string()))
            });
            r
        };
        assert_eq!(
            again.iter().map(|(n, _)| n.to_string()).collect::<Vec<_>>(),
            order,
        );
    }

    #[test]
    fn route_error_display() {
        let n = |p: u16| NodeId::new("127.0.0.1", p).unwrap();
        assert_eq!(RouteError::NoProvider.to_string(), "no provider for the requested model");
        let e = RouteError::Exhausted(vec![
            (n(9000), "timeout".into()),
            (n(9001), "llm_error: boom".into()),
        ]);
        let s = e.to_string();
        assert!(s.contains("all 2 attempted provider(s) failed"));
        assert!(s.contains("127.0.0.1:9000: timeout"));
        assert!(s.contains("127.0.0.1:9001: llm_error: boom"));
    }
}
