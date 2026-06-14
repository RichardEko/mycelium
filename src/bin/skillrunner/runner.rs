use bytes::Bytes;
use serde_json::Value;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Semaphore;

use mycelium::{GossipAgent, CapFilter, CapValue};
use mycelium::RpcRequest;

use super::audit::{self, AuditRecord};
use super::config::SkillFile;
use super::llm::{self, LlmRequest, ToolSchema};

#[cfg(feature = "otel")]
use super::audit::otel as otel_mod;
#[cfg(feature = "otel")]
use opentelemetry_sdk::trace::TracerProvider;

pub(crate) struct SkillRunner {
    pub agent:  Arc<GossipAgent>,
    pub skill:  Arc<SkillFile>,
    pub client: Arc<reqwest::Client>,
    #[cfg(feature = "otel")]
    pub otel:   Option<TracerProvider>,
}

impl SkillRunner {
    pub(crate) async fn run(self) {
        let max_conc = self.skill.capability.policy.as_ref()
            .and_then(|p| p.max_concurrent)
            .unwrap_or(usize::MAX);
        let sem = Arc::new(Semaphore::new(max_conc));

        let mut rx = self.agent.service().rpc_rx("skill.invoke");
        let runner = Arc::new(self);

        while let Some(req) = rx.recv().await {
            let permit = match Arc::clone(&sem).try_acquire_owned() {
                Ok(p)  => p,
                Err(_) => {
                    let busy = serde_json::json!({"error": "skill saturated"});
                    let bytes = serde_json::to_vec(&busy).unwrap_or_default();
                    runner.agent.service().rpc_respond(&req, Bytes::from(bytes));
                    continue;
                }
            };

            let r = Arc::clone(&runner);
            tokio::spawn(async move {
                let _permit = permit;
                r.handle(req).await;
            });
        }
    }

    async fn handle(&self, req: RpcRequest) {
        let start = Instant::now();
        let nonce = req.nonce();
        let caller = req.sender().to_string();
        let ns = self.skill.capability.ns.clone();
        let name = self.skill.capability.name.clone();

        // Provider-side authorization (WS1, compliance feature). Under the tls
        // identity the incoming RPC sender is signature-verified at the
        // connection layer, so `req.sender()` is trustworthy here — the only
        // place `authorized_callers` can be *enforced* (the caller controls its
        // own resolve). An empty allowlist is open. Denial is logged to the
        // audit trail and answered with an error, never silently dropped.
        #[cfg(feature = "compliance")]
        {
            let allow: Vec<std::sync::Arc<str>> = self
                .skill
                .capability
                .policy
                .as_ref()
                .map(|p| p.authorized_callers.iter().map(|s| std::sync::Arc::<str>::from(s.as_str())).collect())
                .unwrap_or_default();
            if !self.agent.caller_authorized(req.sender(), &allow) {
                tracing::warn!("skill {ns}/{name}: denied unauthorized caller {caller}");
                let rec = AuditRecord::new(&ns, &name, &caller, nonce, false, 0, &[]);
                audit::write_audit(&self.agent, &rec);
                let err = serde_json::json!({"error": "unauthorized: caller not in authorized_callers"});
                self.agent.service().rpc_respond(&req, Bytes::from(serde_json::to_vec(&err).unwrap_or_default()));
                return;
            }
        }

        let input: Value = match serde_json::from_slice(&req.payload()) {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!("skill.invoke: invalid JSON from {caller}: {e}");
                let err = serde_json::json!({"error": format!("invalid input: {e}")});
                self.agent.service().rpc_respond(&req, Bytes::from(serde_json::to_vec(&err).unwrap_or_default()));
                return;
            }
        };

        let tools = self.resolve_tools().await;
        let agent_ref = Arc::clone(&self.agent);
        let llm_cfg = self.skill.skill.llm.clone();

        let result = llm::call_openai_compatible(
            &self.client,
            &self.skill.skill.llm,
            LlmRequest {
                system_prompt: self.skill.skill.prompt.clone(),
                user_input:    input,
                tools,
                model:         llm_cfg.model.clone(),
                max_tokens:    llm_cfg.max_tokens,
                temperature:   llm_cfg.temperature,
            },
            move |tool_name, args| {
                let agent2 = Arc::clone(&agent_ref);
                let cfg2 = llm_cfg.clone();
                async move {
                    invoke_mesh_tool(&agent2, &cfg2, &tool_name, args).await
                }
            },
        ).await;

        let duration_ms = start.elapsed().as_millis() as u64;

        match result {
            Ok(llm_resp) => {
                let rec = AuditRecord::new(
                    &ns, &name, &caller, nonce, true, duration_ms, &llm_resp.tool_calls,
                );
                audit::write_audit(&self.agent, &rec);

                #[cfg(feature = "otel")]
                if let (Some(provider), Some(otel_cfg)) =
                    (&self.otel, &self.skill.skill.otel)
                {
                    otel_mod::emit_span(provider, otel_cfg, &rec);
                }

                let out = serde_json::to_vec(&llm_resp.output).unwrap_or_default();
                self.agent.service().rpc_respond(&req, Bytes::from(out));
            }
            Err(e) => {
                tracing::error!("skill {ns}/{name}: LLM error: {e}");
                let rec = AuditRecord::new(&ns, &name, &caller, nonce, false, duration_ms, &[]);
                audit::write_audit(&self.agent, &rec);

                let err = serde_json::json!({"error": e.to_string()});
                self.agent.service().rpc_respond(&req, Bytes::from(serde_json::to_vec(&err).unwrap_or_default()));
            }
        }
    }

    /// Resolve declared tool names to `ToolSchema` by scanning the mesh KV store.
    async fn resolve_tools(&self) -> Vec<ToolSchema> {
        if self.skill.skill.tools.is_empty() {
            return Vec::new();
        }

        let mut schemas = Vec::new();
        for tool_name in &self.skill.skill.tools {
            // Tools are advertised under skills/{ns}/{name}/{node_id}/input
            // tool_name is "ns/name"; the LM sees the bare name (slashes invalid in OpenAI function names)
            let entries = self.agent.kv().scan_prefix("skills/");
            let bare = tool_name.split_once('/').map(|x| x.1).unwrap_or(tool_name.as_str());

            for (key, val) in &entries {
                // key: skills/{ns}/{name}/{node_id}/input
                // parts[0]="skills", parts[1]={ns}, parts[2]={name}, parts[3]={node_id}, parts[4]="input"
                let parts: Vec<&str> = key.split('/').collect();
                let mesh_ns_name = if parts.len() >= 5 {
                    format!("{}/{}", parts[1], parts[2])
                } else {
                    continue;
                };
                if mesh_ns_name == *tool_name && parts.last() == Some(&"input")
                    && let Ok(schema) = serde_json::from_slice::<Value>(val) {
                        schemas.push(ToolSchema {
                            name:        bare.to_string(),
                            description: format!("Mesh capability {}/{}", parts[1], parts[2]),
                            input:       schema,
                        });
                        break;
                    }
            }

            // Fallback: try resolve() for a description attribute
            if !schemas.iter().any(|s| s.name == bare) {
                let parts: Vec<&str> = tool_name.splitn(2, '/').collect();
                let (ns, cname) = if parts.len() == 2 {
                    (parts[0], parts[1])
                } else {
                    ("", tool_name.as_str())
                };
                if !ns.is_empty() {
                    let filter = CapFilter::new(ns, cname);
                    if let Some((_, cap)) = self.agent.capabilities().resolve(&filter).into_iter().next() {
                        let desc = cap.attributes.get("description")
                            .and_then(|v| if let CapValue::Text(t) = v { Some(t.as_ref().to_string()) } else { None })
                            .unwrap_or_else(|| cname.to_string());
                        schemas.push(ToolSchema {
                            name:        cname.to_string(),
                            description: desc,
                            input:       serde_json::json!({"type": "object"}),
                        });
                    }
                }
            }
        }
        schemas
    }
}

/// Invoke a named mesh capability via rpc_call and return the result as JSON.
async fn invoke_mesh_tool(
    agent:     &Arc<GossipAgent>,
    _llm_cfg:  &super::config::LlmSection,
    tool_name: &str,
    args:      Value,
) -> Value {
    // tool_name is either "ns/name" or just "name" (search all namespaces)
    let parts: Vec<&str> = tool_name.splitn(2, '/').collect();
    let (ns, cname) = if parts.len() == 2 { (parts[0], parts[1]) } else { ("", tool_name) };

    // Resolve namespace: if the LLM called a bare name, scan skills/ KV to find the ns
    let (resolved_ns, resolved_cname): (String, String) = if ns.is_empty() {
        let entries = agent.kv().scan_prefix("skills/");
        let mut found: Option<(String, String)> = None;
        for (key, _) in &entries {
            let kparts: Vec<&str> = key.split('/').collect();
            // skills/{ns}/{name}/{node_id}/input
            if kparts.len() >= 5 && kparts[2] == cname && kparts.last() == Some(&"input") {
                found = Some((kparts[1].to_string(), kparts[2].to_string()));
                break;
            }
        }
        match found {
            Some(f) => f,
            None => return Value::String(format!("tool '{tool_name}': not found on mesh")),
        }
    } else {
        (ns.to_string(), cname.to_string())
    };

    let filter = CapFilter::new(resolved_ns.as_str(), resolved_cname.as_str());

    let providers = agent.capabilities().resolve(&filter);
    let Some((target, _)) = providers.into_iter().next() else {
        return Value::String(format!("tool '{tool_name}': no provider on mesh"));
    };

    let payload = match serde_json::to_vec(&args) {
        Ok(b) => Bytes::from(b),
        Err(e) => return Value::String(format!("tool '{tool_name}': serialise error: {e}")),
    };

    match agent.service().rpc_call(target, "skill.invoke", payload, std::time::Duration::from_secs(90)).await {
        Ok(resp) => serde_json::from_slice(&resp)
            .unwrap_or(Value::String(String::from_utf8_lossy(&resp).into_owned())),
        Err(e) => Value::String(format!("tool '{tool_name}': rpc error: {e:?}")),
    }
}
