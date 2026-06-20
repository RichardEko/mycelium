# Chapter 12 — Schema Lifecycle Management

Mycelium's capability subsystem gossips contract definitions alongside capability
advertisements. This chapter explains how an organisation manages those contracts
across the full lifecycle: from authoring through deployment, versioning, and
retirement.

---

## The two-layer model

Every capability may carry two JSON Schema strings:

| Field | Where it lives | Purpose |
|-------|---------------|---------|
| `input_schema` / `output_schema` | Gossip-propagated with each `cap/` entry | Inline snapshot — available instantly from `resolve()` |
| `schemas/{schema_id}` KV entry | Gossip KV; WAL-persisted | Authoritative source of truth; owned by the publishing node |

The inline fields are the fast path: callers inspect the contract from `resolve()`
results without any extra network call. The `schemas/` KV entry is the governance
record: the canonical bytes that survived CI, that every node converges to via
anti-entropy, and that tooling validates against.

They should match. When they do, the inline fields are a gossip-propagated cache
of the authoritative record.

---

## Schema naming convention

Schema IDs follow `{org}/{service}/v{N}`:

```
acme/ml-inference/v1
acme/ml-inference/v2
acme/data-pipeline/v1
platform/auth/v3
```

Rules:
- **Immutable once published.** A published schema ID is never redefined. Breaking
  changes require a new version suffix (`v2`, `v3`, …). The old ID remains live
  during the rollout window so existing consumers keep working.
- **Backward-compatible changes** (adding optional fields) may reuse the same ID
  only if the existing bytes in the ring are identical — `publish_schema` returns
  `Unchanged` in this case.
- **Namespacing** is purely conventional. The substrate treats `/` as a normal
  character in the schema ID string.

---

## Publishing a schema

### Single schema

```rust
use mycelium::SchemaPublishResult;

let schema = br#"{
  "type": "object",
  "required": ["prompt"],
  "properties": {
    "prompt":     { "type": "string" },
    "max_tokens": { "type": "integer" }
  }
}"#;

match agent.schemas().publish_schema("acme/ml-inference/v1", schema).await? {
    SchemaPublishResult::Published  => println!("registered"),
    SchemaPublishResult::Unchanged  => println!("already up to date"),
    SchemaPublishResult::Conflict { existing } => {
        // A different schema already exists under this ID.
        // In CI: treat as build failure.
        // In dev: inspect `existing` and decide whether to force.
        eprintln!("conflict!\nexisting: {}", String::from_utf8_lossy(&existing));
    }
}
```

### Seeding a directory at startup

Keep all schema definitions in a `schemas/` directory in source control. Each
`.json` file becomes a schema ID derived from its path relative to the directory:

```
schemas/
  acme/
    ml-inference/
      v1.json     →  schema_id "acme/ml-inference/v1"
      v2.json     →  schema_id "acme/ml-inference/v2"
    data-pipeline/
      v1.json     →  schema_id "acme/data-pipeline/v1"
```

At startup, seed the whole catalogue in one call:

```rust
let results = agent.schemas().seed_schemas_from_dir("./schemas").await;

for (id, result) in &results {
    match result {
        Ok(SchemaPublishResult::Published)  => println!("{id}: published"),
        Ok(SchemaPublishResult::Unchanged)  => println!("{id}: unchanged"),
        Ok(SchemaPublishResult::Conflict { .. }) => eprintln!("{id}: CONFLICT"),
        Err(e) => eprintln!("{id}: error — {e}"),
    }
}

// Abort startup if any conflict was detected in production
let has_conflict = results.iter().any(|(_, r)| {
    matches!(r, Ok(SchemaPublishResult::Conflict { .. }))
});
if has_conflict { panic!("schema conflict — check logs"); }
```

Anti-entropy propagates the seeded schemas to every other node automatically.
Nodes that start later receive the full catalogue without any additional step.

---

## Advertising capabilities with schemas

Once a schema is in the ring, capabilities reference it by ID and embed the
inline snapshot:

```rust
let schema_bytes = agent.schemas().get_schema("acme/ml-inference/v1")
    .expect("schema must be published before advertising");

let cap = Capability::new("ml", "inference")
    .with_schema_id("acme/ml-inference/v1")
    .with_input_schema(std::str::from_utf8(&schema_bytes).unwrap())
    .with_output_schema(r#"{"type":"object","required":["result"]}"#);

let _handle = agent.capabilities().advertise_capability(cap, Duration::from_secs(60));
```

Callers inspect the contract from `resolve()` without any extra lookup:

```rust
let providers = agent.capabilities().resolve(
    &CapFilter::new("ml", "inference").with_schema("acme/ml-inference/v1")
);

if let Some((node, cap)) = providers.first() {
    // input_schema is available immediately — no round-trip
    let schema: serde_json::Value = serde_json::from_str(
        cap.input_schema.as_deref().unwrap_or("{}")
    )?;
    // validate your payload against `schema` before calling
    agent.service().rpc_call(node.clone(), "ml.invoke", payload, timeout).await?;
}
```

---

## CI / CD gate

Schema changes follow a git-first workflow. No developer writes directly to the
mesh's `schemas/` KV prefix in production.

```yaml
# .github/workflows/ci.yml (excerpt)
- name: Publish schemas
  run: |
    cargo run --bin schema-seed -- \
      --node ${{ secrets.SCHEMA_NODE }} \
      --dir ./schemas \
      --fail-on-conflict   # exits 1 if any Conflict result is returned
```

The `--fail-on-conflict` flag causes `seed_schemas_from_dir` to return a non-zero
exit code on any conflict — blocking the merge if a schema redefinition is
attempted without a version bump.

**Rollout window**: during a `v1 → v2` migration, both schema IDs coexist in the
ring. Providers gradually switch from advertising `v1` to `v2`; consumers switch
their `CapFilter::with_schema` at their own pace. Once all consumers have
migrated, `v1` providers can be retired.

---

## Schema discovery

```rust
// Look up one schema by ID
let bytes: Option<Bytes> = agent.schemas().get_schema("acme/ml-inference/v1");

// Enumerate the full catalogue
let catalogue: Vec<(Arc<str>, Bytes)> = agent.schemas().list_schemas();
for (id, json) in &catalogue {
    println!("{id}: {} bytes", json.len());
}
```

`list_schemas()` returns entries sorted by schema ID, with the `schemas/` prefix
stripped. It reads from the local KV view — eventually consistent, no network
round-trip.

---

## Force-overwrite (development only)

During active development you may need to redefine a schema before incrementing
its version. Use `force_publish_schema`:

```rust
// Overwrites without conflict detection — dev / migration tooling only.
// In production, always bump the version suffix instead.
agent.schemas().force_publish_schema("acme/ml-inference/v1", new_schema_bytes).await?;
```

---

## Durability

Schema KV entries follow the same durability rules as all other KV writes:

- **At least one persistent node** must be running with `PersistenceConfig` set
  for schemas to survive a full-cluster restart.
- Nodes without persistence recover their schema view via anti-entropy from live
  peers on reconnect.
- Schema entries have no TTL — they remain until explicitly deleted or
  force-overwritten.

See the [Durability contract](../../README.md#durability-contract) section of the
README for the full persistence model.

---

## Schema evolution

Schemas drift. Mycelium handles version skew in three tiers (ROADMAP
§*Schema-registry evolution*; delivery plan
[`docs/plans/v2-wsf-schema-evolution.md`](../plans/v2-wsf-schema-evolution.md)),
governed by one rule: **explicit, registered migrations — never silent
coercion.** When no migration path exists, the mismatch is *detected*, not
guessed.

### Additive tolerance (already true on JSON payloads)

The **JSON payload paths** (gateway, A2A, prompt skills, AgentFacts) are
*additively tolerant* by virtue of serde:

- a consumer compiled against an **older** schema **ignores** fields a newer
  producer added (serde ignores unknown fields unless `deny_unknown_fields`);
- a consumer compiled against a **newer** schema **defaults** fields an older
  producer omitted (`#[serde(default)]`, used throughout `capability.rs`).

So *adding optional fields* and *dropping fields a peer defaults* are
backward/forward compatible with **no migration needed**. (This is a *payload*
property — the gossip wire frame uses the in-tree fixed-int codec, not JSON, and
is versioned by `WIRE_VERSION`.) Verified by `schema_evolution::additive_tolerance_tests`.

**The boundary:** additive tolerance does **not** cover *type changes* or
*renames* — a `priority: "high"` where `priority: u8` is expected fails to parse,
deliberately. That is what the next two tiers are for.

### Detection (tier 2) and registered migrations (tier 3)

When a schema version genuinely changes shape (rename, type coercion,
cross-version mapping), Mycelium's answer is an **explicit, registered,
gossip-distributed migration** — a declarative `vN → vN+1` transform published
into the registry alongside the schemas and composed `v1 → v2 → v3` on the
receive side — and, where no such migration is registered, a **`schema_mismatch`
tripwire** (`/stats`) that surfaces the drift rather than silently coercing it.
See the [delivery plan](../plans/v2-wsf-schema-evolution.md) for status.

---

## Summary

| Task | API |
|------|-----|
| Publish one schema | `publish_schema(id, bytes)` |
| Seed from a directory | `seed_schemas_from_dir(path)` |
| Force-overwrite (dev) | `force_publish_schema(id, bytes)` |
| Look up by ID | `get_schema(id)` |
| List all schemas | `list_schemas()` |
| Filter capability by schema | `CapFilter::new(...).with_schema(id)` |
| Inline schema on a capability | `Capability::with_input_schema(json)` |
