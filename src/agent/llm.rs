//! LLM backend abstraction and skill registry for the `llm` feature.

use std::{collections::HashMap, sync::Arc};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use tracing::warn;

use crate::signal::signal_kind;
use super::TaskCtx;
use super::rpc::rpc_respond_ctx;
use super::prompt::{PromptTemplate, render_template};

// ── Result type ──────────────────────────────────────────────────────────────

/// Returned by `LlmBackend::complete` and `LlmBackend::stream`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LlmResult {
    pub output:      String,
    /// Which model actually responded — populated by the backend.
    pub model_used:  String,
    pub tokens_used: u32,
}

// ── Error type ───────────────────────────────────────────────────────────────

#[derive(Debug, thiserror::Error)]
pub enum LlmError {
    #[error("HTTP error: {0}")]
    Http(String),
    #[error("API error: {0}")]
    Api(String),
    #[error("parse error: {0}")]
    Parse(String),
}

// ── Backend trait ─────────────────────────────────────────────────────────────

#[async_trait::async_trait]
pub trait LlmBackend: Send + Sync {
    async fn complete(
        &self,
        system:      &str,
        user:        &str,
        max_tokens:  u32,
        temperature: f32,
    ) -> Result<LlmResult, LlmError>;

    /// Optional streaming. Default: call `complete`, send as single chunk.
    async fn stream(
        &self,
        system:      &str,
        user:        &str,
        max_tokens:  u32,
        temperature: f32,
        tx:          tokio::sync::mpsc::Sender<String>,
    ) -> Result<LlmResult, LlmError> {
        let r = self.complete(system, user, max_tokens, temperature).await?;
        let _ = tx.send(r.output.clone()).await;
        Ok(r)
    }

    /// The outbound endpoint URL this backend reaches, if any — used by the WS3
    /// egress gate before dispatch. `None` (the default) means no outbound reach
    /// (e.g. an in-process / echo backend) and is never egress-gated.
    fn endpoint(&self) -> Option<&str> {
        None
    }
}

// ── Built-in backends ─────────────────────────────────────────────────────────

/// OpenAI-compatible REST API backend (Claude API, OpenAI, Mistral, Ollama).
/// Model is baked in at construction — not read from `PromptTemplate`.
pub struct OpenAiBackend {
    pub base_url: String,
    pub api_key:  String,
    pub model:    String,
    pub client:   reqwest::Client,
}

impl OpenAiBackend {
    pub fn new(base_url: impl Into<String>, api_key: impl Into<String>, model: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into(),
            api_key:  api_key.into(),
            model:    model.into(),
            client:   reqwest::Client::new(),
        }
    }
}

#[async_trait::async_trait]
impl LlmBackend for OpenAiBackend {
    fn endpoint(&self) -> Option<&str> {
        Some(&self.base_url)
    }

    async fn complete(
        &self,
        system:      &str,
        user:        &str,
        max_tokens:  u32,
        temperature: f32,
    ) -> Result<LlmResult, LlmError> {
        let body = serde_json::json!({
            "model": self.model,
            "max_tokens": max_tokens,
            "temperature": temperature,
            "messages": [
                {"role": "system",  "content": system},
                {"role": "user",    "content": user},
            ]
        });
        let resp = self.client
            .post(format!("{}/chat/completions", self.base_url.trim_end_matches('/')))
            .bearer_auth(&self.api_key)
            .json(&body)
            .send()
            .await
            .map_err(|e| LlmError::Http(e.to_string()))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(LlmError::Api(format!("{status}: {text}")));
        }

        let json: serde_json::Value = resp.json().await
            .map_err(|e| LlmError::Parse(e.to_string()))?;

        let output = json["choices"][0]["message"]["content"]
            .as_str()
            .unwrap_or("")
            .to_owned();
        let tokens_used = json["usage"]["total_tokens"]
            .as_u64()
            .unwrap_or(0) as u32;
        let model_used = json["model"]
            .as_str()
            .unwrap_or(&self.model)
            .to_owned();

        Ok(LlmResult { output, model_used, tokens_used })
    }
}

/// Test double — returns `"echo: {input}"` synchronously.
pub struct EchoBackend;

#[async_trait::async_trait]
impl LlmBackend for EchoBackend {
    async fn complete(
        &self,
        _system:     &str,
        user:        &str,
        _max_tokens: u32,
        _temperature: f32,
    ) -> Result<LlmResult, LlmError> {
        Ok(LlmResult {
            output:      format!("echo: {}", user),
            model_used:  "echo".into(),
            tokens_used: 0,
        })
    }
}

// ── Registry ──────────────────────────────────────────────────────────────────

/// Per-node registry mapping `"{ns}/{name}"` to an LLM backend.
/// `PromptTemplate` is NOT stored here — it is read fresh from KV on every invocation.
pub type LlmSkillRegistry = Arc<papaya::HashMap<String, Arc<dyn LlmBackend>>>;

// ── RPC payload types ─────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub(crate) struct LlmInvokeRequest {
    pub prompt:  String,   // "{ns}/{name}"
    pub input:   String,
    pub context: HashMap<String, String>,
}

#[derive(Debug, Serialize)]
pub(crate) struct LlmInvokeResponse {
    pub output:      String,
    pub model_used:  String,
    pub tokens_used: u32,
}

#[derive(Debug, Serialize)]
pub(crate) struct LlmInvokeError {
    pub error:  String,
    pub detail: String,
}

// ── Shared dispatch loop ──────────────────────────────────────────────────────

/// Spawns the single `llm.invoke` dispatch loop for this node.
/// Each invocation is handled in its own `tokio::spawn` so slow LLM calls
/// do not block the loop from receiving the next request.
pub(super) fn spawn_llm_dispatch_loop(
    ctx:      &Arc<TaskCtx>,
    registry: LlmSkillRegistry,
) {
    let ctx_clone = Arc::clone(ctx);
    let shutdown  = ctx.shutdown_tx.subscribe();
    let registry  = Arc::clone(&registry);

    ctx.spawn_task(run_llm_dispatch(ctx_clone, registry, shutdown));
}

async fn run_llm_dispatch(
    ctx:      Arc<TaskCtx>,
    registry: LlmSkillRegistry,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    let mut rx = ctx.signal_handlers.register_with_capacity(
        std::sync::Arc::from(signal_kind::LLM_INVOKE),
        64,
    );

    loop {
        tokio::select! {
            biased;
            _ = async { while !*shutdown.borrow() { if shutdown.changed().await.is_err() { return } } } => break,
            sig = rx.recv() => {
                let Some(sig) = sig else { break };
                let ctx      = Arc::clone(&ctx);
                let registry = Arc::clone(&registry);
                tokio::spawn(handle_llm_invoke(ctx, registry, sig));
            }
        }
    }
}

async fn handle_llm_invoke(
    ctx:      Arc<TaskCtx>,
    registry: LlmSkillRegistry,
    signal:   crate::signal::Signal,
) {
    use super::rpc::RpcRequest;
    let req = RpcRequest(signal);

    // Parse request
    let invoke_req: LlmInvokeRequest = match serde_json::from_slice(&req.payload()) {
        Ok(r)  => r,
        Err(e) => {
            let err = serde_json::to_vec(&LlmInvokeError {
                error: "parse_error".into(), detail: e.to_string(),
            }).unwrap_or_default();
            rpc_respond_ctx(&ctx, &req, Bytes::from(err));
            return;
        }
    };

    let skill_id = &invoke_req.prompt;

    // Look up backend
    let backend = match registry.pin().get(skill_id.as_str()) {
        Some(b) => Arc::clone(b),
        None => {
            let err = serde_json::to_vec(&LlmInvokeError {
                error: "skill_not_found".into(),
                detail: format!("no backend registered for {}", skill_id),
            }).unwrap_or_default();
            rpc_respond_ctx(&ctx, &req, Bytes::from(err));
            return;
        }
    };

    // WS3 egress gate: an LLM-backend call is an outbound reach the node chooses.
    if let Some(url) = backend.endpoint()
        && !ctx.config.egress.permits_url(url)
    {
        tracing::warn!(url = %url, skill = %skill_id, "llm backend call blocked by egress policy");
        let err = serde_json::to_vec(&LlmInvokeError {
            error: "egress_denied".into(),
            detail: format!("egress policy denies the LLM endpoint for skill {skill_id}"),
        }).unwrap_or_default();
        rpc_respond_ctx(&ctx, &req, Bytes::from(err));
        return;
    }

    // Read template fresh from KV
    let kv_key = format!("prompts/{}", skill_id);
    let template_bytes = ctx.kv_state.store.pin().get(kv_key.as_str())
        .and_then(|e| e.data.clone());
    let template_bytes = match template_bytes {
        Some(b) => b,
        None => {
            let err = serde_json::to_vec(&LlmInvokeError {
                error: "template_not_found".into(),
                detail: format!("no KV entry at prompts/{}", skill_id),
            }).unwrap_or_default();
            rpc_respond_ctx(&ctx, &req, Bytes::from(err));
            return;
        }
    };
    let template: PromptTemplate = match serde_json::from_slice(&template_bytes) {
        Ok(t)  => t,
        Err(e) => {
            let err = serde_json::to_vec(&LlmInvokeError {
                error: "template_decode_error".into(), detail: e.to_string(),
            }).unwrap_or_default();
            rpc_respond_ctx(&ctx, &req, Bytes::from(err));
            return;
        }
    };

    // Render
    let node_id_str = ctx.node_id.to_string();
    let rendered_system = match render_template(
        &template.system, skill_id, &node_id_str, &invoke_req.input, &invoke_req.context,
    ) {
        Ok(s)  => s,
        Err(e) => {
            let err = serde_json::to_vec(&LlmInvokeError {
                error: "render_error".into(), detail: e.to_string(),
            }).unwrap_or_default();
            rpc_respond_ctx(&ctx, &req, Bytes::from(err));
            return;
        }
    };
    let rendered_user = match render_template(
        &template.user_template, skill_id, &node_id_str, &invoke_req.input, &invoke_req.context,
    ) {
        Ok(s)  => s,
        Err(e) => {
            let err = serde_json::to_vec(&LlmInvokeError {
                error: "render_error".into(), detail: e.to_string(),
            }).unwrap_or_default();
            rpc_respond_ctx(&ctx, &req, Bytes::from(err));
            return;
        }
    };

    // Call backend
    match backend.complete(&rendered_system, &rendered_user, template.max_tokens, template.temperature).await {
        Ok(result) => {
            let resp = serde_json::to_vec(&LlmInvokeResponse {
                output:      result.output,
                model_used:  result.model_used,
                tokens_used: result.tokens_used,
            }).unwrap_or_default();
            rpc_respond_ctx(&ctx, &req, Bytes::from(resp));
        }
        Err(e) => {
            warn!("llm.invoke backend error for {}: {}", skill_id, e);
            let err = serde_json::to_vec(&LlmInvokeError {
                error: "llm_error".into(), detail: e.to_string(),
            }).unwrap_or_default();
            rpc_respond_ctx(&ctx, &req, Bytes::from(err));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn echo_backend_complete() {
        let b = EchoBackend;
        let r = b.complete("sys", "hello world", 100, 0.0).await.unwrap();
        assert_eq!(r.output, "echo: hello world");
        assert_eq!(r.model_used, "echo");
        assert_eq!(r.tokens_used, 0);
    }

    #[test]
    fn echo_backend_no_model_param() {
        // Compile-time: LlmBackend::complete takes no `model` argument.
        // The signature below must compile without a model parameter.
        fn assert_sig<B: LlmBackend>(_b: B) {}
        assert_sig(EchoBackend);
    }

    #[test]
    fn backend_endpoint_drives_the_egress_gate() {
        // OpenAiBackend exposes its outbound URL (egress-gated); EchoBackend has
        // no outbound reach (never gated). The gate itself is EgressPolicy.
        let oa = OpenAiBackend::new("https://api.blocked.example/v1", "k", "m");
        assert_eq!(oa.endpoint(), Some("https://api.blocked.example/v1"));
        assert_eq!(EchoBackend.endpoint(), None);

        let policy = crate::config::EgressPolicy { allow_hosts: vec!["api.allowed.example".into()] };
        // The dispatch gate denies a backend whose endpoint host isn't allowed…
        assert!(!policy.permits_url(oa.endpoint().unwrap()));
        // …and an endpoint-less backend is never gated (None → skip the check).
        assert!(EchoBackend.endpoint().is_none());
        // An allowed endpoint passes.
        let ok = OpenAiBackend::new("https://api.allowed.example/v1", "k", "m");
        assert!(policy.permits_url(ok.endpoint().unwrap()));
    }
}
