//! LLM prompt-skill operations — [`LlmHandle`].
//!
//! Obtain a handle via [`GossipAgent::llm()`](crate::GossipAgent::llm).

use std::{collections::HashMap, sync::Arc, time::Duration};
use bytes::Bytes;

use super::TaskCtx;
use super::capability_handle::CapabilitiesHandle;
use super::helpers::{kv_delete, kv_scan_prefix, kv_set};
use super::{prompt, llm};
use super::rpc;
use crate::capability::{Capability, CapFilter};
use crate::signal::{kv_ns, signal_kind};

/// Domain handle for LLM prompt-skill operations.
/// Obtained via [`GossipAgent::llm()`](crate::GossipAgent::llm).
///
/// Covers prompt skill registration, invocation, template management,
/// and the node-local LLM backend registry.
///
/// The handle is `Clone + Send + Sync` and can be stored, moved across tasks,
/// or captured in closures.
#[derive(Clone)]
pub struct LlmHandle {
    pub(crate) ctx: Arc<TaskCtx>,
}

impl LlmHandle {
    /// Publish a prompt template to the cluster KV and register this node as a
    /// provider. The skill is discoverable via the capability ring immediately.
    /// Dropping the returned handle retracts the capability and removes the backend.
    pub async fn register_prompt_skill(
        &self,
        ns:       &str,
        name:     &str,
        template: prompt::PromptTemplate,
        backend:  Arc<dyn llm::LlmBackend>,
    ) -> Result<prompt::PromptSkillHandle, prompt::PromptSkillError> {
        // 1. Write template to KV — configuration, not heartbeat.
        let kv_key = format!("{}{}/{}", kv_ns::PROMPTS, ns, name);
        let bytes  = serde_json::to_vec(&template)
            .map_err(|e| prompt::PromptSkillError::LlmError(e.to_string()))?;
        kv_set(&self.ctx, Arc::from(kv_key.as_str()), Bytes::from(bytes));

        // 2. Advertise capability — presence heartbeat, evaporates when node dies.
        let cap_handle = CapabilitiesHandle { ctx: Arc::clone(&self.ctx) }
            .advertise_capability(Capability::new(ns, name), Duration::from_secs(30));

        // 3. Register backend in the shared registry.
        let skill_id = format!("{}/{}", ns, name);
        self.ctx.llm_skills.pin().insert(skill_id.clone(), Arc::clone(&backend));

        // 4. Spawn the dispatch loop exactly once. An `is_empty()` check
        // before the insert was a check-then-act: two first registrations
        // racing could both observe an empty registry and spawn two loops,
        // each receiving every `llm.invoke` signal → duplicate RPC
        // responses. The atomic swap admits exactly one winner.
        if !self.ctx.llm_dispatch_spawned.swap(true, std::sync::atomic::Ordering::AcqRel) {
            llm::spawn_llm_dispatch_loop(&self.ctx, Arc::clone(&self.ctx.llm_skills));
        }

        // 5. Create cancellation channel for this skill's registry entry.
        // The removal is conditional on Arc identity: if the skill was
        // re-registered under the same id while this handle was alive,
        // dropping the OLD handle must not delete the NEW backend.
        let (cancel_tx, cancel_rx) = tokio::sync::oneshot::channel::<()>();
        let registry   = Arc::clone(&self.ctx.llm_skills);
        let skill_id2  = skill_id.clone();
        let registered = Arc::clone(&backend);
        tokio::spawn(async move {
            let _ = cancel_rx.await;
            registry.pin().compute(skill_id2, |existing| match existing {
                Some((_, current)) if Arc::ptr_eq(current, &registered) =>
                    papaya::Operation::Remove,
                _ => papaya::Operation::Abort(()),
            });
        });

        Ok(prompt::PromptSkillHandle {
            _cap:            cap_handle,
            _handler_cancel: cancel_tx,
        })
    }

    /// Call a prompt skill. Resolves a provider via the capability ring,
    /// sends an RPC `llm.invoke` call, returns the LLM's output string.
    pub async fn call_prompt_skill(
        &self,
        ns:      &str,
        name:    &str,
        input:   &str,
        context: HashMap<String, String>,
        timeout: Duration,
    ) -> Result<String, prompt::PromptSkillError> {
        let providers = CapabilitiesHandle { ctx: Arc::clone(&self.ctx) }
            .resolve(&CapFilter::new(ns, name));
        let (target, _) = providers.into_iter().next()
            .ok_or_else(|| prompt::PromptSkillError::NoProvider {
                ns: ns.into(), name: name.into(),
            })?;

        let req = serde_json::json!({
            "prompt":  format!("{}/{}", ns, name),
            "input":   input,
            "context": context,
        });
        let payload = Bytes::from(req.to_string().into_bytes());

        let reply = rpc::rpc_call_ctx(
            &self.ctx,
            target,
            Arc::from(signal_kind::LLM_INVOKE),
            payload,
            timeout,
        ).await?;

        let v: serde_json::Value = serde_json::from_slice(&reply)
            .map_err(|e| prompt::PromptSkillError::LlmError(e.to_string()))?;
        if let Some(err) = v.get("error").and_then(|e| e.as_str()) {
            let detail = v.get("detail").and_then(|d| d.as_str()).unwrap_or("");
            return Err(prompt::PromptSkillError::LlmError(format!("{}: {}", err, detail)));
        }
        v["output"].as_str()
            .map(|s| s.to_owned())
            .ok_or_else(|| prompt::PromptSkillError::LlmError("missing output field".into()))
    }

    /// Update a prompt template in the cluster KV. All serving nodes pick up
    /// the change on their next invocation (they read from KV, not a local cache).
    pub fn update_prompt(
        &self,
        ns:       &str,
        name:     &str,
        template: prompt::PromptTemplate,
    ) -> Result<(), prompt::PromptSkillError> {
        let kv_key = format!("{}{}/{}", kv_ns::PROMPTS, ns, name);
        let bytes  = serde_json::to_vec(&template)
            .map_err(|e| prompt::PromptSkillError::LlmError(e.to_string()))?;
        kv_set(&self.ctx, Arc::from(kv_key.as_str()), Bytes::from(bytes));
        Ok(())
    }

    /// Retrieve the current prompt template from the local KV snapshot.
    /// Synchronous — reads in-memory state, same as `kv().get()`.
    pub fn get_prompt(&self, ns: &str, name: &str) -> Option<prompt::PromptTemplate> {
        let key = format!("{}{}/{}", kv_ns::PROMPTS, ns, name);
        let bytes = self.ctx.kv_state.store.pin().get(key.as_str())
            .and_then(|e| e.data.clone())?;
        serde_json::from_slice(&bytes).ok()
    }

    /// List all prompt skills currently visible in the local KV snapshot.
    pub fn list_prompts(&self) -> Vec<(String, String)> {
        kv_scan_prefix(&self.ctx, kv_ns::PROMPTS)
            .into_iter()
            .filter_map(|(k, _)| {
                let rest = k.strip_prefix(kv_ns::PROMPTS)?;
                let mut parts = rest.splitn(2, '/');
                let ns   = parts.next()?.to_owned();
                let name = parts.next()?.to_owned();
                if name.is_empty() { return None; }
                Some((ns, name))
            })
            .collect()
    }

    /// Tombstone the prompt template KV entry. The skill becomes unreachable
    /// once all serving nodes' capability entries expire (≤30s).
    pub fn delete_prompt(&self, ns: &str, name: &str) {
        let key = format!("{}{}/{}", kv_ns::PROMPTS, ns, name);
        kv_delete(&self.ctx, Arc::from(key.as_str()));
    }
}
