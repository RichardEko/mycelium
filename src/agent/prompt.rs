//! Prompt template type, rendering, handle, and error for the `llm` feature.

use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use crate::capability::CapabilityReg;

/// A prompt template stored in the cluster KV at `prompts/{ns}/{name}`.
///
/// Does not contain a model identifier — model availability is node-local
/// knowledge held by the `LlmBackend`. Use capability attributes for model-
/// based routing: `CapFilter::with_attribute("model", "claude-sonnet-4-6")`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PromptTemplate {
    /// System prompt. May contain `{{variable}}` placeholders.
    pub system: String,
    /// User message template. Must contain at least `{{input}}`.
    pub user_template: String,
    /// Maximum tokens in the LLM response.
    pub max_tokens: u32,
    /// Sampling temperature. `0.0` = deterministic.
    pub temperature: f32,
    /// Arbitrary metadata (tags, version notes, model hints).
    /// Use `metadata["model_hint"]` as a non-binding annotation;
    /// use capability attributes for hard model routing.
    pub metadata: HashMap<String, serde_json::Value>,
}

/// Errors from prompt skill operations.
#[derive(Debug, thiserror::Error)]
pub enum PromptSkillError {
    #[error("no provider for skill {ns}/{name}")]
    NoProvider { ns: String, name: String },
    #[error("template not found: {0}")]
    TemplateNotFound(String),
    #[error("render error in template '{template}': unknown variable '{var}'")]
    RenderError { template: String, var: String },
    #[error("LLM error: {0}")]
    LlmError(String),
    #[error("RPC error: {0}")]
    Rpc(#[from] crate::agent::rpc::RpcError),
}

/// Returned by `register_prompt_skill`. Dropping retracts the skill immediately.
///
/// Holds:
/// - `_cap`: the `CapabilityReg` — dropping tombstones `cap/` and stops the 30s heartbeat
/// - `_handler_cancel`: `oneshot::Sender<()>` — dropping signals the dispatch loop to
///   remove this entry from `GossipAgent::llm_skills`
#[must_use = "dropping PromptSkillHandle retracts the skill immediately"]
pub struct PromptSkillHandle {
    pub(crate) _cap: CapabilityReg,
    pub(crate) _handler_cancel: tokio::sync::oneshot::Sender<()>,
}

/// Renders a template string by substituting `{{variable}}` placeholders.
///
/// Reserved variables populated automatically: `input`, `node_id`, `skill_name`, `timestamp`.
/// Additional variables from `context`. Returns `RenderError` for unknown placeholders.
pub(crate) fn render_template(
    template: &str,
    skill_name: &str,
    node_id: &str,
    input: &str,
    context: &HashMap<String, String>,
) -> Result<String, PromptSkillError> {
    let timestamp = chrono_or_fallback();
    let mut result = template.to_owned();
    // collect all {{...}} occurrences
    let mut pos = 0;
    while let Some(start) = result[pos..].find("{{") {
        let abs_start = pos + start;
        let Some(end_rel) = result[abs_start..].find("}}") else { break };
        let abs_end = abs_start + end_rel + 2;
        let var = &result[abs_start + 2..abs_start + end_rel].trim().to_owned().clone();
        let replacement = match var.as_str() {
            "input"      => input.to_owned(),
            "node_id"    => node_id.to_owned(),
            "skill_name" => skill_name.to_owned(),
            "timestamp"  => timestamp.clone(),
            other => {
                if let Some(v) = context.get(other) {
                    v.clone()
                } else {
                    return Err(PromptSkillError::RenderError {
                        template: template.to_owned(),
                        var:      other.to_owned(),
                    });
                }
            }
        };
        result.replace_range(abs_start..abs_end, &replacement);
        pos = abs_start + replacement.len();
    }
    Ok(result)
}

fn chrono_or_fallback() -> String {
    // ISO-8601 UTC timestamp without pulling in chrono as a dep.
    // Uses UNIX_EPOCH; good enough for template context variables.
    use std::time::{SystemTime, UNIX_EPOCH};
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    format!("{}Z", secs)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn empty_ctx() -> HashMap<String, String> { HashMap::new() }

    #[test]
    fn render_basic() {
        let out = render_template("Hello {{input}}!", "ns/name", "node1", "world", &empty_ctx()).unwrap();
        assert_eq!(out, "Hello world!");
    }

    #[test]
    fn render_context_vars() {
        let mut ctx = HashMap::new();
        ctx.insert("lang".into(), "Rust".into());
        let out = render_template("{{input}} in {{lang}}", "ns/name", "node1", "hello", &ctx).unwrap();
        assert_eq!(out, "hello in Rust");
    }

    #[test]
    fn render_unknown_var() {
        let err = render_template("{{unknown}}", "ns/name", "node1", "x", &empty_ctx());
        assert!(matches!(err, Err(PromptSkillError::RenderError { .. })));
    }

    #[test]
    fn render_reserved_node_id() {
        let out = render_template("from {{node_id}}", "ns/name", "mynode", "x", &empty_ctx()).unwrap();
        assert_eq!(out, "from mynode");
    }

    #[test]
    fn render_skill_name() {
        let out = render_template("skill={{skill_name}}", "ai/chat", "n", "x", &empty_ctx()).unwrap();
        assert_eq!(out, "skill=ai/chat");
    }

    #[test]
    fn template_roundtrip() {
        let t = PromptTemplate {
            system: "sys".into(),
            user_template: "user {{input}}".into(),
            max_tokens: 512,
            temperature: 0.7,
            metadata: HashMap::new(),
        };
        let json = serde_json::to_string(&t).unwrap();
        let back: PromptTemplate = serde_json::from_str(&json).unwrap();
        assert_eq!(back.system, t.system);
        assert_eq!(back.max_tokens, t.max_tokens);
    }
}
