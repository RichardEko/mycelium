//! Wedge ② — fleet-reasoning traces on the event-log overlay.
//!
//! Trace records ride `kv().append(…)`, gossip-replicated and HLC-ordered, so a run's
//! causal story is replayable from **any** node, not just the one that reasoned.
//! (`EventRing`/`/gateway/explain` are `pub(crate)` in core; the log overlay is the
//! public-API path, and this crate mounts its own `/gateway/reason/trace/{run_id}`.)
//!
//! **One substream per writer** — `reason/{run_id}/{node}` (KV keys
//! `log/reason/{run_id}/{node}/{hlc:016x}`), merged on HLC at replay. A single shared
//! stream cannot host multiple writers: the HLC packs 48-bit milliseconds + a 16-bit
//! *per-node* logical counter, so two nodes appending in the same millisecond both mint
//! `(ms, 0)` — identical keys, and LWW silently drops a record. Per-node substreams
//! restore key uniqueness (`append` is monotonic per node) while the merged replay
//! stays HLC-ordered; equal-stamp events across nodes are concurrent by definition and
//! tie-break on node id for determinism.
//!
//! Records are compact JSON `{"node","kind","detail"}` — the HLC comes from the log key
//! on replay, never duplicated into the value. Keep `detail` small: every record is a
//! size-gated KV write that floods the fleet; payloads belong in the blob tier.

use std::sync::Arc;

use mycelium::{GossipAgent, NodeId};
use serde::{Deserialize, Serialize};
use serde_json::json;

/// Records reasoning events into this node's substream of run `run_id`
/// (`reason/{run_id}/{node}` — see the module doc for why substreams).
///
/// Stateless — each `record` is one KV append; clone-cheap, no locks, safe to share
/// across the tasks of a run.
#[derive(Clone)]
pub struct TraceRecorder {
    agent: Arc<GossipAgent>,
    run_id: String,
}

/// The on-wire record value (`hlc` lives in the log key).
#[derive(Serialize, Deserialize)]
struct RecordValue {
    node: String,
    kind: String,
    detail: serde_json::Value,
}

/// One replayed trace event, HLC-ordered within its run.
#[derive(Debug, Clone)]
pub struct TraceEvent {
    pub hlc: u64,
    pub node: String,
    pub kind: String,
    pub detail: serde_json::Value,
}

impl TraceRecorder {
    pub fn new(agent: Arc<GossipAgent>, run_id: impl Into<String>) -> Self {
        Self { agent, run_id: run_id.into() }
    }

    pub fn run_id(&self) -> &str {
        &self.run_id
    }

    /// Append one event to this node's substream; returns its HLC (the replay cursor).
    pub fn record(&self, kind: &str, detail: serde_json::Value) -> u64 {
        let node = self.agent.node_id().to_string();
        let value = RecordValue { node: node.clone(), kind: kind.to_string(), detail };
        let bytes = serde_json::to_vec(&value).unwrap_or_default();
        self.agent.kv().append(&format!("reason/{}/{node}", self.run_id), bytes)
    }

    /// A routing decision — candidate fills and the chosen provider (wedge ①, recorded once per call).
    pub fn route(&self, model: &str, candidates: &[(NodeId, f32)], chosen: &NodeId) -> u64 {
        self.record(
            "route",
            json!({
                "model": model,
                "candidates": candidates.iter()
                    .map(|(n, fill)| json!({ "node": n.to_string(), "fill": fill }))
                    .collect::<Vec<_>>(),
                "chosen": chosen.to_string(),
            }),
        )
    }

    /// One inference attempt against one provider (wedge ①, recorded per attempt).
    pub fn llm_call(
        &self,
        provider: &NodeId,
        ok: bool,
        tokens: u32,
        duration_ms: u64,
        error: Option<&str>,
    ) -> u64 {
        self.record(
            "llm_call",
            json!({
                "provider": provider.to_string(),
                "ok": ok,
                "tokens": tokens,
                "duration_ms": duration_ms,
                "error": error,
            }),
        )
    }

    /// A tool invocation by the reasoning agent.
    pub fn tool_call(&self, name: &str, ok: bool) -> u64 {
        self.record("tool_call", json!({ "name": name, "ok": ok }))
    }

    /// A thread resumed after awaiting its model dependency (wedge ③).
    pub fn resume(&self, model: &str, waited_ms: u64, providers: &[NodeId]) -> u64 {
        self.record(
            "resume",
            json!({
                "model": model,
                "waited_ms": waited_ms,
                "providers": providers.iter().map(|n| n.to_string()).collect::<Vec<_>>(),
            }),
        )
    }

    /// Seal the replayed trace's chained SHA-256 into this node's WS2 audit chain.
    ///
    /// Anchoring requires a TLS identity (audit records are Ed25519-signed) — a node
    /// without `GossipConfig::tls` gets `GossipError::InvalidField`. Verification is:
    /// replay the run, rehash (chained SHA-256 over each event's canonical bytes, in
    /// HLC order), and compare against the hash sealed in the audit chain.
    #[cfg(feature = "compliance")]
    pub fn anchor(&self) -> Result<[u8; 32], mycelium::GossipError> {
        use sha2::{Digest, Sha256};
        let events = replay(&self.agent, &self.run_id);
        let mut chain = [0u8; 32];
        for e in &events {
            let canonical = serde_json::to_vec(&json!({
                "hlc": e.hlc, "node": e.node, "kind": e.kind, "detail": e.detail,
            }))
            .unwrap_or_default();
            let mut h = Sha256::new();
            h.update(chain);
            h.update(&canonical);
            chain = h.finalize().into();
        }
        let hex: String = chain.iter().map(|b| format!("{b:02x}")).collect();
        self.agent.audit(
            mycelium::AuditAction::Invoke,
            "mycelium-reason",
            format!("reason/{}", self.run_id),
            mycelium::AuditOutcome::Success,
            Some(json!({ "events": events.len(), "sha256": hex }).to_string()),
        )
    }
}

/// Replay a run's full trace from the local KV view: every node's substream, merged
/// and HLC-ordered (ties break on node id — equal stamps are concurrent events).
/// Tolerant of undecodable entries (a foreign writer, a truncated record): skipped
/// with a warning, never a panic — replay is a read path.
pub fn replay(agent: &GossipAgent, run_id: &str) -> Vec<TraceEvent> {
    // Keys are `log/reason/{run_id}/{node}/{hlc:016x}` — a prefix scan over the run
    // sees every substream; the HLC is the segment after the last `/`.
    let prefix = format!("log/reason/{run_id}/");
    let mut events: Vec<TraceEvent> = agent
        .kv()
        .scan_prefix(&prefix)
        .into_iter()
        .filter_map(|(key, value)| {
            let hlc_hex = key.rsplit('/').next()?;
            let Ok(hlc) = u64::from_str_radix(hlc_hex, 16) else {
                tracing::warn!(run_id, %key, "skipping trace key with a non-HLC suffix");
                return None;
            };
            match serde_json::from_slice::<RecordValue>(&value) {
                Ok(v) => Some(TraceEvent { hlc, node: v.node, kind: v.kind, detail: v.detail }),
                Err(e) => {
                    tracing::warn!(run_id, hlc, error = %e, "skipping undecodable trace record");
                    None
                }
            }
        })
        .collect();
    events.sort_by(|a, b| a.hlc.cmp(&b.hlc).then_with(|| a.node.cmp(&b.node)));
    events
}

/// One human line per event — the same shape as the core's explain narrative:
/// `[hlc N] {node} — {gloss} ({key detail})`, with typed glosses and a raw fallback.
pub fn narrate(events: &[TraceEvent]) -> Vec<String> {
    events
        .iter()
        .map(|e| {
            let gloss = match e.kind.as_str() {
                "route" => format!(
                    "routed {} → {} ({} candidate(s))",
                    e.detail["model"].as_str().unwrap_or("?"),
                    e.detail["chosen"].as_str().unwrap_or("?"),
                    e.detail["candidates"].as_array().map(Vec::len).unwrap_or(0),
                ),
                "llm_call" => {
                    let provider = e.detail["provider"].as_str().unwrap_or("?");
                    if e.detail["ok"].as_bool().unwrap_or(false) {
                        format!(
                            "llm call ok on {provider} ({} tokens, {} ms)",
                            e.detail["tokens"].as_u64().unwrap_or(0),
                            e.detail["duration_ms"].as_u64().unwrap_or(0),
                        )
                    } else {
                        format!(
                            "llm call failed on {provider} ({})",
                            e.detail["error"].as_str().unwrap_or("unknown error"),
                        )
                    }
                }
                "tool_call" => format!(
                    "tool {} ({})",
                    e.detail["name"].as_str().unwrap_or("?"),
                    if e.detail["ok"].as_bool().unwrap_or(false) { "ok" } else { "failed" },
                ),
                "resume" => format!(
                    "resumed with {} (waited {} ms, {} provider(s))",
                    e.detail["model"].as_str().unwrap_or("?"),
                    e.detail["waited_ms"].as_u64().unwrap_or(0),
                    e.detail["providers"].as_array().map(Vec::len).unwrap_or(0),
                ),
                other => format!("{other} ({})", e.detail),
            };
            format!("[hlc {}] {} — {}", e.hlc, e.node, gloss)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ev(hlc: u64, kind: &str, detail: serde_json::Value) -> TraceEvent {
        TraceEvent { hlc, node: "127.0.0.1:9000".into(), kind: kind.into(), detail }
    }

    #[test]
    fn narrate_typed_glosses() {
        let events = vec![
            ev(1, "route", json!({ "model": "fable-mini", "chosen": "127.0.0.1:9001",
                "candidates": [{ "node": "127.0.0.1:9001", "fill": 0.0 }] })),
            ev(2, "llm_call", json!({ "provider": "127.0.0.1:9001", "ok": true,
                "tokens": 42, "duration_ms": 12, "error": null })),
            ev(3, "llm_call", json!({ "provider": "127.0.0.1:9002", "ok": false,
                "tokens": 0, "duration_ms": 0, "error": "rpc timeout" })),
            ev(4, "tool_call", json!({ "name": "demand-forecast", "ok": true })),
            ev(5, "resume", json!({ "model": "fable-mini", "waited_ms": 900,
                "providers": ["127.0.0.1:9001"] })),
            ev(6, "custom", json!({ "x": 1 })),
        ];
        let lines = narrate(&events);
        assert_eq!(lines.len(), 6);
        assert!(lines[0].contains("routed fable-mini → 127.0.0.1:9001 (1 candidate(s))"));
        assert!(lines[1].contains("llm call ok on 127.0.0.1:9001 (42 tokens, 12 ms)"));
        assert!(lines[2].contains("llm call failed on 127.0.0.1:9002 (rpc timeout)"));
        assert!(lines[3].contains("tool demand-forecast (ok)"));
        assert!(lines[4].contains("resumed with fable-mini (waited 900 ms, 1 provider(s))"));
        assert!(lines[5].starts_with("[hlc 6] 127.0.0.1:9000 — custom ("));
    }

    #[test]
    fn record_value_json_tolerance() {
        // A well-formed value decodes…
        let ok: Result<RecordValue, _> =
            serde_json::from_slice(br#"{"node":"n","kind":"k","detail":{"a":1}}"#);
        assert!(ok.is_ok());
        // …and the malformed shapes replay() must skip do fail, not panic.
        for bad in [&b"not json"[..], br#"{"kind":"k"}"#, br#"[1,2,3]"#] {
            assert!(serde_json::from_slice::<RecordValue>(bad).is_err());
        }
    }
}
