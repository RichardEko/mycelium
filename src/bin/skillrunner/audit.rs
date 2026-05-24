use bytes::Bytes;
use serde::Serialize;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use mycelium::GossipAgent;

use super::llm::ToolCall;

#[derive(Debug, Serialize)]
pub(crate) struct ToolCallSummary {
    pub name:    String,
    pub success: bool,
}

#[derive(Debug, Serialize)]
pub(crate) struct AuditRecord {
    pub skill_ns:      String,
    pub skill_name:    String,
    pub caller:        String,
    /// RPC correlation nonce — use as trace ID to correlate request/response.
    pub nonce:         u64,
    pub success:       bool,
    pub duration_ms:   u64,
    pub tool_calls:    Vec<ToolCallSummary>,
    pub ts_unix_nanos: u128,
}

impl AuditRecord {
    pub(crate) fn new(
        skill_ns:    &str,
        skill_name:  &str,
        caller:      &str,
        nonce:       u64,
        success:     bool,
        duration_ms: u64,
        tool_calls:  &[ToolCall],
    ) -> Self {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);

        AuditRecord {
            skill_ns:    skill_ns.to_string(),
            skill_name:  skill_name.to_string(),
            caller:      caller.to_string(),
            nonce,
            success,
            duration_ms,
            tool_calls: tool_calls.iter().map(|tc| ToolCallSummary {
                name:    tc.name.clone(),
                success: true,
            }).collect(),
            ts_unix_nanos: ts,
        }
    }
}

/// Write one invocation audit record to the gossip KV store.
///
/// Key format: `audit/{ts_unix_nanos}/{node_id}` — lexicographic order gives
/// time-sorted prefix scans across the whole cluster.
///
/// TTL is deliberately not set here; the record ages out via normal KV gossip
/// TTL decrements over cluster hops (default_ttl from GossipConfig).
pub(crate) fn write_audit(agent: &Arc<GossipAgent>, rec: &AuditRecord) {
    let key = format!("audit/{}/{}", rec.ts_unix_nanos, agent.node_id());
    match serde_json::to_vec(rec) {
        Ok(json) => { let _ = agent.set(key, Bytes::from(json)); }
        Err(e)   => tracing::warn!("audit: serialisation failed: {e}"),
    }
}

// ── Optional OTEL export ──────────────────────────────────────────────────────

#[cfg(feature = "otel")]
pub(crate) mod otel {
    use opentelemetry::trace::{Span, SpanKind, Tracer, TracerProvider as _};
    use opentelemetry::KeyValue;
    use opentelemetry_otlp::WithExportConfig;
    use opentelemetry_sdk::trace::TracerProvider;

    use super::super::config::OtelSection;
    use super::AuditRecord;

    pub(crate) fn init_tracer(cfg: &OtelSection) -> Result<TracerProvider, Box<dyn std::error::Error>> {
        let exporter = opentelemetry_otlp::new_exporter()
            .tonic()
            .with_endpoint(&cfg.endpoint)
            .build_span_exporter()?;

        let provider = TracerProvider::builder()
            .with_simple_exporter(exporter)
            .build();

        Ok(provider)
    }

    pub(crate) fn emit_span(provider: &TracerProvider, cfg: &OtelSection, rec: &AuditRecord) {
        let tracer = provider.tracer(cfg.service_name.clone());
        let mut span = tracer
            .span_builder(format!("{}/{}", rec.skill_ns, rec.skill_name))
            .with_kind(SpanKind::Server)
            .start(&tracer);

        span.set_attribute(KeyValue::new("skill.ns",       rec.skill_ns.clone()));
        span.set_attribute(KeyValue::new("skill.name",     rec.skill_name.clone()));
        span.set_attribute(KeyValue::new("caller",         rec.caller.clone()));
        span.set_attribute(KeyValue::new("nonce",          rec.nonce as i64));
        span.set_attribute(KeyValue::new("success",        rec.success));
        span.set_attribute(KeyValue::new("duration_ms",    rec.duration_ms as i64));
        span.set_attribute(KeyValue::new("tool_calls",     rec.tool_calls.len() as i64));
        span.end();
    }
}
