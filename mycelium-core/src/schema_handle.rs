//! Schema registry operations — [`SchemaHandle`].
//!
//! Schemas are stored under the `schemas/` KV prefix as raw JSON bytes.
//! The key is `schemas/{schema_id}` where `schema_id` may contain `/`
//! characters for namespacing (e.g. `acme/ml/v2`).
//!
//! # Lifecycle
//!
//! 1. Define your JSON Schema in a `.json` file checked into source control.
//! 2. Call [`publish_schema`](SchemaHandle::publish_schema) (or
//!    [`seed_schemas_from_dir`](SchemaHandle::seed_schemas_from_dir)) at node
//!    startup or in CI to write the schema into the KV ring.
//! 3. Anti-entropy propagates the schema to every node automatically.
//! 4. Callers inspect schemas from [`resolve`](crate::GossipAgent::resolve) results
//!    via the inline `input_schema` / `output_schema` fields — no separate
//!    lookup required. [`SchemaHandle::get_schema`] provides the authoritative KV
//!    record; [`SchemaHandle::list_schemas`] enumerates the full catalogue.

use bytes::Bytes;
use std::{path::Path, sync::Arc};

use crate::context::CoreCtx;
use crate::ops::{kv_get, kv_scan_prefix, kv_set_async};

// ── Public types ─────────────────────────────────────────────────────────────

/// Result of a [`SchemaHandle::publish_schema`] call.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchemaPublishResult {
    /// Schema written for the first time.
    Published,
    /// Identical content was already present — no write performed.
    Unchanged,
    /// Different content exists under the same `schema_id`. The existing bytes
    /// are returned for inspection. The conflicting write is **not** applied;
    /// call [`SchemaHandle::force_publish_schema`] to overwrite.
    ///
    /// **`Conflict` is advisory, not a guarantee.** The check is a read-then-write
    /// with no atomic mutual exclusion: two concurrent publishers can both observe
    /// `None` on the read, both proceed to write, and LWW will silently pick one
    /// winner. `Conflict` fires only when a prior write has already propagated to
    /// this node before the check runs. Restrict schema publishing to a single
    /// authority (e.g. your CI pipeline) for a hard guarantee.
    Conflict { existing: Bytes },
}

/// Error returned by [`SchemaHandle::publish_schema`] and
/// [`SchemaHandle::seed_schemas_from_dir`].
#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum SchemaError {
    #[error("invalid JSON: {0}")]
    InvalidJson(String),
    #[error("schema must be a JSON object (got {kind})")]
    NotAnObject { kind: &'static str },
    /// Returned when `schema_id` contains characters outside
    /// `[A-Za-z0-9_\-./]`, is empty, starts or ends with `/`, or contains `//`.
    #[error("invalid schema_id {id:?}: {reason}")]
    InvalidSchemaId { id: String, reason: &'static str },
    #[error("I/O error reading {path}: {source}")]
    Io {
        path: std::path::PathBuf,
        #[source]
        source: std::io::Error,
    },
}

// ── Handle ────────────────────────────────────────────────────────────────────

/// Typed handle for schema registry operations.
///
/// Obtained via `GossipAgent::schemas` in the full crate. Zero-cost: wraps a single
/// `Arc<CoreCtx>` clone — a Layer-I substrate handle (v2 M3), so it is also obtainable
/// directly from a `mycelium-core` embedding.
pub struct SchemaHandle {
    pub(crate) ctx: Arc<CoreCtx>,
}

impl SchemaHandle {
    /// Construct from a core context. Used by the full crate's `GossipAgent::schemas`
    /// accessor (which passes `Arc::clone(&task_ctx.core)`) and by core embedders.
    #[doc(hidden)]
    pub fn from_core(ctx: Arc<CoreCtx>) -> Self {
        Self { ctx }
    }

    /// Validates `schema_json` and writes it to `schemas/{schema_id}` in the
    /// gossip KV ring.
    ///
    /// # Returns
    ///
    /// - [`SchemaPublishResult::Published`] — written successfully.
    /// - [`SchemaPublishResult::Unchanged`] — identical content already present;
    ///   no write performed.
    /// - [`SchemaPublishResult::Conflict`] — a different schema is already
    ///   visible in this node's local KV view under `schema_id`. The existing
    ///   bytes are returned; the caller must decide whether to force-overwrite
    ///   via [`force_publish_schema`](Self::force_publish_schema).
    ///
    /// # Atomicity
    ///
    /// The conflict check is a **read-then-write**, not an atomic compare-and-swap.
    /// Two nodes publishing different schemas for the same ID concurrently can
    /// both observe `None` from the read, both proceed to write, and LWW resolves
    /// the winner silently. `Conflict` is returned only when a prior write has
    /// already propagated to this node before the check runs. For a hard
    /// serialisation guarantee, restrict publishing to a single node (e.g. your
    /// CI pipeline) or use [`consistent_set`](crate::GossipAgent::consistent_set) directly.
    ///
    /// # Errors
    ///
    /// Returns [`SchemaError::InvalidJson`] if `schema_json` is not valid JSON,
    /// or [`SchemaError::NotAnObject`] if the top-level value is not a JSON object.
    pub async fn publish_schema(
        &self,
        schema_id: impl Into<Arc<str>>,
        schema_json: &[u8],
    ) -> Result<SchemaPublishResult, SchemaError> {
        let schema_id: Arc<str> = schema_id.into();
        validate_schema_id(&schema_id)?;
        validate_schema_json(schema_json)?;
        let key = schema_key(&schema_id);
        let incoming = Bytes::copy_from_slice(schema_json);

        if let Some(existing) = kv_get(&self.ctx, &key) {
            if existing == incoming {
                return Ok(SchemaPublishResult::Unchanged);
            }
            return Ok(SchemaPublishResult::Conflict { existing });
        }

        let _ = kv_set_async(&self.ctx, key.into(), incoming).await;
        Ok(SchemaPublishResult::Published)
    }

    /// Like [`publish_schema`](Self::publish_schema) but overwrites any existing
    /// entry without conflict detection. Use in CI with `--force` semantics or
    /// when intentionally replacing a schema during development.
    pub async fn force_publish_schema(
        &self,
        schema_id: impl Into<Arc<str>>,
        schema_json: &[u8],
    ) -> Result<(), SchemaError> {
        let schema_id: Arc<str> = schema_id.into();
        validate_schema_id(&schema_id)?;
        validate_schema_json(schema_json)?;
        let key = schema_key(&schema_id);
        let _ = kv_set_async(&self.ctx, key.into(), Bytes::copy_from_slice(schema_json)).await;
        Ok(())
    }

    /// Returns the authoritative JSON Schema bytes for `schema_id`, or `None`
    /// if no schema has been published under that ID on this node's KV view.
    pub fn get_schema(&self, schema_id: &str) -> Option<Bytes> {
        kv_get(&self.ctx, &schema_key(schema_id))
    }

    /// Returns all schemas currently visible in this node's KV view as
    /// `(schema_id, json_bytes)` pairs, sorted by `schema_id`.
    ///
    /// Each `schema_id` has the `schemas/` prefix stripped.
    pub fn list_schemas(&self) -> Vec<(Arc<str>, Bytes)> {
        let mut entries: Vec<(Arc<str>, Bytes)> = kv_scan_prefix(&self.ctx, "schemas/")
            .into_iter()
            .map(|(key, val)| {
                let id: Arc<str> = key
                    .strip_prefix("schemas/")
                    .unwrap_or(&key)
                    .into();
                (id, val)
            })
            .collect();
        entries.sort_by(|(a, _), (b, _)| a.as_ref().cmp(b.as_ref()));
        entries
    }

    /// Reads every `*.json` file under `dir` (recursively) and calls
    /// [`publish_schema`](Self::publish_schema) for each.
    ///
    /// The `schema_id` is derived from the file's path relative to `dir` with
    /// the `.json` extension stripped and OS separators replaced by `/`:
    ///
    /// ```text
    /// schemas/acme-ml-v2.json     →  schema_id "acme-ml-v2"
    /// schemas/acme/ml/v2.json     →  schema_id "acme/ml/v2"
    /// ```
    ///
    /// Returns one `(schema_id, Result)` per `.json` file found. Files that
    /// fail validation produce an `Err` entry; the remaining files are still
    /// processed.
    pub async fn seed_schemas_from_dir(
        &self,
        dir: impl AsRef<Path>,
    ) -> Vec<(String, Result<SchemaPublishResult, SchemaError>)> {
        let dir = dir.as_ref();
        let mut results = Vec::new();
        collect_json_files(dir, dir, &mut results, self).await;
        results.sort_by(|(a, _), (b, _)| a.cmp(b));
        results
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn schema_key(schema_id: &str) -> String {
    format!("schemas/{schema_id}")
}

/// Rejects schema IDs that would produce surprising `scan_prefix("schemas/")`
/// results or look like path traversal attempts. Allowed characters:
/// `[A-Za-z0-9_\-./]`. The ID must be non-empty, must not start or end with
/// `/`, and must not contain `//`.
fn validate_schema_id(id: &str) -> Result<(), SchemaError> {
    if id.is_empty() {
        return Err(SchemaError::InvalidSchemaId { id: id.to_owned(), reason: "must not be empty" });
    }
    if id.starts_with('/') || id.ends_with('/') {
        return Err(SchemaError::InvalidSchemaId { id: id.to_owned(), reason: "must not start or end with '/'" });
    }
    if id.contains("//") {
        return Err(SchemaError::InvalidSchemaId { id: id.to_owned(), reason: "must not contain '//'" });
    }
    if !id.chars().all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.' | '/')) {
        return Err(SchemaError::InvalidSchemaId {
            id: id.to_owned(),
            reason: "only ASCII letters, digits, '_', '-', '.', '/' are allowed",
        });
    }
    // Reject '.' and '..' path segments to prevent path-traversal confusion.
    if id.split('/').any(|seg| seg == "." || seg == "..") {
        return Err(SchemaError::InvalidSchemaId {
            id: id.to_owned(),
            reason: "'.' and '..' path segments are not allowed",
        });
    }
    Ok(())
}

fn validate_schema_json(bytes: &[u8]) -> Result<(), SchemaError> {
    let v: serde_json::Value = serde_json::from_slice(bytes)
        .map_err(|e| SchemaError::InvalidJson(e.to_string()))?;
    if !v.is_object() {
        let kind = match &v {
            serde_json::Value::Array(_)  => "array",
            serde_json::Value::String(_) => "string",
            serde_json::Value::Number(_) => "number",
            serde_json::Value::Bool(_)   => "bool",
            serde_json::Value::Null      => "null",
            serde_json::Value::Object(_) => unreachable!(),
        };
        return Err(SchemaError::NotAnObject { kind });
    }
    Ok(())
}

// Async-recursive directory walker. Uses Box::pin to break the recursive
// async cycle. All I/O goes through tokio::fs to avoid blocking the runtime.
fn collect_json_files<'a>(
    base:    &'a Path,
    current: &'a Path,
    results: &'a mut Vec<(String, Result<SchemaPublishResult, SchemaError>)>,
    handle:  &'a SchemaHandle,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
    Box::pin(async move {
        let mut dir = match tokio::fs::read_dir(current).await {
            Ok(d) => d,
            Err(e) => {
                results.push((
                    current.display().to_string(),
                    Err(SchemaError::Io { path: current.to_path_buf(), source: e }),
                ));
                return;
            }
        };

        loop {
            let entry = match dir.next_entry().await {
                Ok(Some(e)) => e,
                Ok(None)    => break,
                Err(_)      => continue,
            };
            let path      = entry.path();
            let file_type = match entry.file_type().await {
                Ok(ft) => ft,
                Err(_) => continue,
            };
            if file_type.is_dir() {
                collect_json_files(base, &path, results, handle).await;
            } else if file_type.is_file() && path.extension().is_some_and(|e| e == "json") {
                let schema_id = path_to_schema_id(base, &path);
                let outcome = match tokio::fs::read(&path).await {
                    Err(e)    => Err(SchemaError::Io { path: path.clone(), source: e }),
                    Ok(bytes) => handle.publish_schema(schema_id.as_str(), &bytes).await,
                };
                results.push((schema_id, outcome));
            }
        }
    })
}

/// Converts a file path relative to `base` into a schema_id string.
/// Extension is stripped; path separators become `/`.
fn path_to_schema_id(base: &Path, file: &Path) -> String {
    let rel = file.strip_prefix(base).unwrap_or(file);
    rel.with_extension("")
        .components()
        .map(|c| c.as_os_str().to_string_lossy().into_owned())
        .collect::<Vec<_>>()
        .join("/")
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // GossipAgent-based publish/get/list/validation tests moved to the full crate
    // (mycelium) with M3 — they need `GossipAgent`. The pure path-parsing tests below
    // stay here (core-only).

    #[test]
    fn path_to_schema_id_flat_file() {
        let base = Path::new("/repo/schemas");
        let file = Path::new("/repo/schemas/acme-ml-v2.json");
        assert_eq!(path_to_schema_id(base, file), "acme-ml-v2");
    }

    #[test]
    fn path_to_schema_id_nested_dirs() {
        let base = Path::new("/repo/schemas");
        let file = Path::new("/repo/schemas/acme/ml/v2.json");
        assert_eq!(path_to_schema_id(base, file), "acme/ml/v2");
    }
}
