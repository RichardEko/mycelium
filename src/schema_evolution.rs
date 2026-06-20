//! Schema evolution (WS-F / [Schema-Evo]) — the runtime-migration machinery for the schema
//! registry, built tier by tier per the ROADMAP §*Schema-registry evolution*.
//!
//! The governing rule is **explicit, registered migrations — never silent best-effort coercion.**
//! Silent coercion would mask real incompatibilities and break the explicit-contract /
//! detection-not-prevention posture. When no migration path exists, **detect**, do not guess.
//!
//! Tiers (delivered incrementally — see `docs/plans/v2-wsf-schema-evolution.md`):
//! - **E1 · additive tolerance** — *largely already true* on the JSON payload paths via serde
//!   ignore-unknown + `#[serde(default)]`. This module documents and **verifies** that property
//!   (the tests below); it is not new mechanism.
//! - **E2 · compatibility detection** — a `schema_mismatch` tripwire (counter + `warn!`).
//! - **E3 · registered migrations** — declarative `vN → vN+1` transforms, gossip-distributed and
//!   composed on the receive path.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use mycelium_core::kv_handle::KvHandle;

/// KV namespace owned by the migration registry: `schemas/migrations/{id}` (the id is derived from
/// `(from, to)` so a lookup needs no scan; the entry's value carries the full migration). Lives
/// *under* the schema registry's `schemas/` prefix.
pub const MIGRATION_PREFIX: &str = "schemas/migrations/";

/// One declarative transform rule. **Declarative data, never code** — so a migration is gossipable
/// and safe to apply (no execution of remote logic). Paths are dot-addressed into the JSON object
/// (`"a.b.c"`); a single segment addresses a top-level field.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum MigrationRule {
    /// Move the value at `from` to `to` (a field rename). No-op if `from` is absent.
    Rename { from: String, to: String },
    /// Set `path` to `value` **only if absent** (fill a newly-required field).
    Default { path: String, value: Value },
    /// Remove `path` (drop a field a newer schema no longer has).
    Drop { path: String },
    /// Coerce the value at `path` to `to_type` (`"string"` | `"number"` | `"bool"`). No-op if absent
    /// or already that type; leaves it unchanged if the value can't be coerced (the result still
    /// fails to parse downstream → tier-2 detection, never a silent wrong value).
    Coerce { path: String, to_type: String },
}

/// A registered, named migration from schema `from` to schema `to`, an ordered list of rules applied
/// in sequence. Published into the registry alongside the schemas and composed on the receive side.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaMigration {
    pub from:  String,
    pub to:    String,
    pub rules: Vec<MigrationRule>,
}

impl SchemaMigration {
    fn encode(&self) -> bytes::Bytes {
        bytes::Bytes::from(serde_json::to_vec(self).unwrap_or_default())
    }
    fn decode(bytes: &[u8]) -> Option<Self> {
        serde_json::from_slice(bytes).ok()
    }
}

/// Deterministic registry key for the `from → to` migration: `schemas/migrations/{hex(from)}-{hex(to)}`.
/// Derived (not a scan) so `get_migration` is O(1). Schema ids may contain `/`, so the ids are
/// hex-encoded into the key segment — collision-free and free of path-separator hazards.
pub fn migration_key(from: &str, to: &str) -> String {
    let hex = |s: &str| -> String { s.bytes().map(|b| format!("{b:02x}")).collect() };
    format!("{MIGRATION_PREFIX}{}-{}", hex(from), hex(to))
}

// ── Dot-path helpers over a JSON object ────────────────────────────────────────

/// Mutable reference to the value at dot-path `path` within `root` (object traversal). `None` if any
/// segment is missing or not an object.
fn path_get_mut<'a>(root: &'a mut Value, path: &str) -> Option<&'a mut Value> {
    let mut cur = root;
    for seg in path.split('.') {
        cur = cur.as_object_mut()?.get_mut(seg)?;
    }
    Some(cur)
}

/// Remove and return the value at dot-path `path` (creating no intermediate objects). `None` if the
/// parent path is missing or not an object.
fn path_remove(root: &mut Value, path: &str) -> Option<Value> {
    let (parent, last) = match path.rsplit_once('.') {
        Some((p, l)) => (path_get_mut(root, p)?, l),
        None => (root, path),
    };
    parent.as_object_mut()?.remove(last)
}

/// Set dot-path `path` to `value`, creating intermediate objects as needed. Returns `false` if a
/// segment exists but is not an object (a structural conflict — left unchanged).
fn path_set(root: &mut Value, path: &str, value: Value) -> bool {
    let mut cur = root;
    let segs: Vec<&str> = path.split('.').collect();
    for seg in &segs[..segs.len() - 1] {
        if !cur.is_object() {
            return false;
        }
        cur = cur.as_object_mut().unwrap().entry(seg.to_string()).or_insert_with(|| Value::Object(Default::default()));
    }
    let Some(obj) = cur.as_object_mut() else { return false };
    obj.insert(segs[segs.len() - 1].to_string(), value);
    true
}

fn coerce(value: &Value, to_type: &str) -> Option<Value> {
    match to_type {
        "string" => match value {
            Value::String(_) => Some(value.clone()),
            Value::Number(n) => Some(Value::String(n.to_string())),
            Value::Bool(b) => Some(Value::String(b.to_string())),
            _ => None,
        },
        "number" => match value {
            Value::Number(_) => Some(value.clone()),
            Value::String(s) => s.parse::<f64>().ok().and_then(serde_json::Number::from_f64).map(Value::Number),
            _ => None,
        },
        "bool" => match value {
            Value::Bool(_) => Some(value.clone()),
            Value::String(s) => match s.as_str() {
                "true" => Some(Value::Bool(true)),
                "false" => Some(Value::Bool(false)),
                _ => None,
            },
            _ => None,
        },
        _ => None,
    }
}

/// Apply an ordered list of declarative rules to a JSON value **in place**. Pure (no I/O); the unit
/// of a migration. A rule whose target is absent is a no-op (the migration is forward-tolerant);
/// `Coerce` that can't convert leaves the value unchanged (so a genuine incompatibility still
/// surfaces downstream rather than being silently mis-coerced).
pub fn apply_rules(value: &mut Value, rules: &[MigrationRule]) {
    for rule in rules {
        match rule {
            MigrationRule::Rename { from, to } => {
                if let Some(v) = path_remove(value, from) {
                    path_set(value, to, v);
                }
            }
            MigrationRule::Default { path, value: dv } => {
                if path_get_mut(value, path).is_none() {
                    path_set(value, path, dv.clone());
                }
            }
            MigrationRule::Drop { path } => {
                let _ = path_remove(value, path);
            }
            MigrationRule::Coerce { path, to_type } => {
                if let Some(slot) = path_get_mut(value, path)
                    && let Some(coerced) = coerce(slot, to_type)
                {
                    *slot = coerced;
                }
            }
        }
    }
}

// ── Registry (KV-backed, gossiped) ─────────────────────────────────────────────

/// Publish a migration into the registry (gossiped KV). Any node may publish; every node then sees
/// it via [`get_migration`] / [`list_migrations`]. Returns whether the write was queued.
pub fn publish_migration(kv: &KvHandle, migration: &SchemaMigration) -> bool {
    kv.set(migration_key(&migration.from, &migration.to), migration.encode())
}

/// The registered `from → to` migration from the local gossip view, if any.
pub fn get_migration(kv: &KvHandle, from: &str, to: &str) -> Option<SchemaMigration> {
    kv.get(&migration_key(from, to)).and_then(|b| SchemaMigration::decode(&b))
}

/// Every registered migration in the local gossip view (for path resolution + drift inspection).
pub fn list_migrations(kv: &KvHandle) -> Vec<SchemaMigration> {
    kv.scan_prefix(MIGRATION_PREFIX)
        .into_iter()
        .filter_map(|(_k, b)| SchemaMigration::decode(&b))
        .collect()
}

// ── Path resolution + composition (E3b) ────────────────────────────────────────

/// Why a migration could not be performed.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum MigrationError {
    /// No registered migration chain connects `from` to `to`. **Detect, don't guess** — the caller
    /// surfaces this (e.g. the `schema_mismatch` tripwire), never a partial/coerced result.
    #[error("no registered migration path from {from:?} to {to:?}")]
    NoMigrationPath { from: String, to: String },
    /// The payload was not valid JSON.
    #[error("payload is not valid JSON: {0}")]
    InvalidJson(String),
}

/// Shortest `from → to` path over the directed migration graph (BFS), as the ordered migrations to
/// apply. `Some(vec![])` for `from == to` (identity). `None` if no path exists — **never** a
/// best-effort partial.
pub fn resolve_path<'a>(
    migrations: &'a [SchemaMigration],
    from: &str,
    to: &str,
) -> Option<Vec<&'a SchemaMigration>> {
    use std::collections::{HashMap, HashSet, VecDeque};
    if from == to {
        return Some(Vec::new());
    }
    let mut adj: HashMap<&str, Vec<&SchemaMigration>> = HashMap::new();
    for m in migrations {
        adj.entry(m.from.as_str()).or_default().push(m);
    }
    let mut queue = VecDeque::from([from]);
    let mut visited = HashSet::from([from]);
    let mut pred: HashMap<&str, &SchemaMigration> = HashMap::new();
    while let Some(node) = queue.pop_front() {
        if node == to {
            // Reconstruct the edge path from `to` back to `from`.
            let mut path = Vec::new();
            let mut cur = to;
            while cur != from {
                let edge = pred.get(cur)?;
                path.push(*edge);
                cur = edge.from.as_str();
            }
            path.reverse();
            return Some(path);
        }
        for m in adj.get(node).into_iter().flatten() {
            if visited.insert(m.to.as_str()) {
                pred.insert(m.to.as_str(), m);
                queue.push_back(m.to.as_str());
            }
        }
    }
    None
}

/// Migrate a JSON `value` from schema `from` to `to` by composing the registered migration chain.
/// `Err(NoMigrationPath)` when no chain connects them — the migration is *explicit*, never guessed.
pub fn migrate_value(
    migrations: &[SchemaMigration],
    from: &str,
    to: &str,
    mut value: Value,
) -> Result<Value, MigrationError> {
    let path = resolve_path(migrations, from, to)
        .ok_or_else(|| MigrationError::NoMigrationPath { from: from.to_string(), to: to.to_string() })?;
    for m in path {
        apply_rules(&mut value, &m.rules);
    }
    Ok(value)
}

#[cfg(test)]
mod additive_tolerance_tests {
    //! E1 / Gate G-E1: verify that the JSON payload paths are **additively tolerant** — a consumer
    //! compiled against an older schema still parses a newer producer's payload (unknown fields
    //! ignored), and a newer consumer still parses an older payload (missing fields defaulted).
    //!
    //! This is a property of serde over JSON (ignore-unknown by default + `#[serde(default)]`), not
    //! Mycelium mechanism — Layer-I capability/filter types already rely on it (`#[serde(default)]`
    //! throughout `capability.rs` for forward-compatible decoding). These tests pin the contract so
    //! a future `#[serde(deny_unknown_fields)]` on a payload path is caught.

    use serde::{Deserialize, Serialize};

    /// A consumer compiled against schema **v1** of a payload.
    #[derive(Debug, PartialEq, Eq, Deserialize)]
    struct ConsumerV1 {
        donor: String,
        /// New in a later schema from the consumer's POV — defaulted when a *producer* omits it.
        #[serde(default)]
        priority: u8,
    }

    /// A producer compiled against schema **v2** — adds an `origin_zone` field the v1 consumer has
    /// never heard of.
    #[derive(Debug, Serialize)]
    struct ProducerV2 {
        donor: String,
        priority: u8,
        origin_zone: String, // unknown to ConsumerV1
    }

    #[test]
    fn newer_producer_field_is_ignored_by_older_consumer() {
        // v2 producer → v1 consumer: the unknown `origin_zone` is ignored (additive tolerance).
        let wire = serde_json::to_vec(&ProducerV2 {
            donor: "borough-market".into(),
            priority: 3,
            origin_zone: "southwark".into(),
        }).unwrap();
        let parsed: ConsumerV1 = serde_json::from_slice(&wire).expect("unknown field must be ignored");
        assert_eq!(parsed, ConsumerV1 { donor: "borough-market".into(), priority: 3 });
    }

    #[test]
    fn missing_field_defaults_for_newer_consumer() {
        // An older producer's payload omits `priority`; the consumer defaults it (additive tolerance
        // the other direction).
        let wire = br#"{"donor":"borough-market"}"#;
        let parsed: ConsumerV1 = serde_json::from_slice(wire).expect("missing field must default");
        assert_eq!(parsed, ConsumerV1 { donor: "borough-market".into(), priority: 0 });
    }

    #[test]
    fn additive_tolerance_does_not_extend_to_type_changes() {
        // The boundary: additive tolerance covers *added/removed* fields, NOT type changes. A
        // type-incompatible payload fails to parse — which is exactly why tier 3 (registered
        // migrations) exists for renames/coercions, and tier 2 (detection) for the no-path case.
        let wire = br#"{"donor":"borough-market","priority":"high"}"#; // priority should be u8
        let parsed: Result<ConsumerV1, _> = serde_json::from_slice(wire);
        assert!(parsed.is_err(), "a type change is NOT silently coerced (migration territory)");
    }
}

#[cfg(test)]
mod migration_tests {
    //! E3a / Gate G-E3a: the declarative migration data model + `apply_rules` perform each rule kind
    //! deterministically; the registry round-trips; malformed migrations decode to `None`.
    use super::*;
    use serde_json::json;

    #[test]
    fn rename_moves_a_field() {
        let mut v = json!({ "origin": "southwark", "kg": 12 });
        apply_rules(&mut v, &[MigrationRule::Rename { from: "origin".into(), to: "origin_zone".into() }]);
        assert_eq!(v, json!({ "origin_zone": "southwark", "kg": 12 }));
    }

    #[test]
    fn default_fills_only_when_absent() {
        let mut v = json!({ "kg": 12 });
        apply_rules(&mut v, &[MigrationRule::Default { path: "priority".into(), value: json!(0) }]);
        assert_eq!(v["priority"], json!(0));
        // Present value is untouched.
        apply_rules(&mut v, &[MigrationRule::Default { path: "priority".into(), value: json!(9) }]);
        assert_eq!(v["priority"], json!(0));
    }

    #[test]
    fn drop_removes_a_field() {
        let mut v = json!({ "kg": 12, "legacy": true });
        apply_rules(&mut v, &[MigrationRule::Drop { path: "legacy".into() }]);
        assert_eq!(v, json!({ "kg": 12 }));
    }

    #[test]
    fn coerce_changes_type_or_leaves_unchanged() {
        let mut v = json!({ "kg": "12", "ok": "true" });
        apply_rules(&mut v, &[
            MigrationRule::Coerce { path: "kg".into(), to_type: "number".into() },
            MigrationRule::Coerce { path: "ok".into(), to_type: "bool".into() },
        ]);
        assert_eq!(v["kg"], json!(12.0));
        assert_eq!(v["ok"], json!(true));
        // An un-coercible value is left unchanged (genuine incompatibility surfaces downstream).
        let mut bad = json!({ "kg": "twelve" });
        apply_rules(&mut bad, &[MigrationRule::Coerce { path: "kg".into(), to_type: "number".into() }]);
        assert_eq!(bad["kg"], json!("twelve"));
    }

    #[test]
    fn nested_dot_paths_work() {
        let mut v = json!({ "certification": { "scheme": "self-certified" } });
        apply_rules(&mut v, &[
            MigrationRule::Default { path: "certification.schemaVersion".into(), value: json!("v2") },
            MigrationRule::Rename { from: "certification.scheme".into(), to: "certification.cert_scheme".into() },
        ]);
        assert_eq!(v["certification"]["schemaVersion"], json!("v2"));
        assert_eq!(v["certification"]["cert_scheme"], json!("self-certified"));
        assert!(v["certification"].get("scheme").is_none());
    }

    #[test]
    fn migration_encodes_and_decodes() {
        let m = SchemaMigration {
            from: "donation@v1".into(),
            to: "donation@v2".into(),
            rules: vec![MigrationRule::Rename { from: "origin".into(), to: "origin_zone".into() }],
        };
        let round = SchemaMigration::decode(&m.encode()).expect("round-trips");
        assert_eq!(round, m);
        assert!(SchemaMigration::decode(b"not json").is_none(), "malformed → None, never a guess");
        // The registry key is deterministic for a (from, to) pair.
        assert_eq!(migration_key("donation@v1", "donation@v2"), migration_key("donation@v1", "donation@v2"));
        assert_ne!(migration_key("donation@v1", "donation@v2"), migration_key("donation@v2", "donation@v1"));
    }

    // ── E3b / Gate G-E3b: path resolution + composition ──────────────────────

    fn mig(from: &str, to: &str, rules: Vec<MigrationRule>) -> SchemaMigration {
        SchemaMigration { from: from.into(), to: to.into(), rules }
    }

    #[test]
    fn composes_a_multi_step_path() {
        // v1→v2 renames origin→origin_zone; v2→v3 defaults priority. v1→v3 composes both.
        let migs = vec![
            mig("d@v1", "d@v2", vec![MigrationRule::Rename { from: "origin".into(), to: "origin_zone".into() }]),
            mig("d@v2", "d@v3", vec![MigrationRule::Default { path: "priority".into(), value: json!(0) }]),
        ];
        let out = migrate_value(&migs, "d@v1", "d@v3", json!({ "origin": "southwark", "kg": 12 })).unwrap();
        assert_eq!(out, json!({ "origin_zone": "southwark", "kg": 12, "priority": 0 }));
    }

    #[test]
    fn identity_path_is_unchanged() {
        let out = migrate_value(&[], "d@v1", "d@v1", json!({ "kg": 12 })).unwrap();
        assert_eq!(out, json!({ "kg": 12 }));
    }

    #[test]
    fn missing_link_yields_no_path_never_a_guess() {
        // v1→v2 exists but v2→v3 does not: v1→v3 must NOT be partially applied — it errors.
        let migs = vec![mig("d@v1", "d@v2", vec![MigrationRule::Drop { path: "legacy".into() }])];
        let err = migrate_value(&migs, "d@v1", "d@v3", json!({ "legacy": true, "kg": 1 })).unwrap_err();
        assert_eq!(err, MigrationError::NoMigrationPath { from: "d@v1".into(), to: "d@v3".into() });
    }

    #[test]
    fn resolve_path_finds_shortest_and_handles_cycles() {
        // A cycle (v2→v1) must not trap the BFS; a direct v1→v3 shortcut wins over the long way.
        let migs = vec![
            mig("v1", "v2", vec![]),
            mig("v2", "v1", vec![]), // cycle
            mig("v2", "v3", vec![]),
            mig("v1", "v3", vec![]), // shortcut
        ];
        let path = resolve_path(&migs, "v1", "v3").expect("a path exists");
        assert_eq!(path.len(), 1, "the direct v1→v3 shortcut is shortest");
        assert!(resolve_path(&migs, "v3", "v1").is_none(), "no path back to v1 from v3");
    }
}
