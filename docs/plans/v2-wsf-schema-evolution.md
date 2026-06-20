# Delivery plan — WS-F · Schema-registry evolution (runtime migration)

**Status:** ✅ **COMPLETE** (2026-06-20). All increments shipped:

| Increment | What | PR |
|---|---|---|
| E1 | Tier 1 — additive tolerance (verify + document) | #84 |
| E2 | Tier 2 — `schema_mismatch` detection tripwire | #85 |
| E3a | Tier 3 — declarative migration data model + registry | #86 |
| E3b | path resolution + composition (`NoMigrationPath`, never guess) | #87 |
| E3c | end-to-end cross-version interop + AgentFacts tie-in | #88 |

**Done-when met:** a producer and consumer compiled against different schema versions interoperate
via an *explicitly registered* migration chain composed on the receive side (G-E3c); a missing path
is *detected* (`NoMigrationPath` + `schema_mismatch` tripwire), never silently coerced. Governing rule
held throughout: **registered + explicit + detect-don't-guess.** Design record follows.

---

Executes the v2.0 plan's WS-F *"Schema-registry evolution — runtime migration"* item ([Schema-Evo]),
the last open M16/WS-F piece. Canonical design: ROADMAP §*Schema-registry evolution — runtime schema
migration, the Mycelium way*.

**Why now.** WS-F federation (M16-A/B AgentFacts) shipped; AgentFacts are *semantically versioned*
JSON-LD that will drift. The schema-evolution machinery is exactly what evolvable AgentFacts need —
the ROADMAP places it "riding alongside M16."

**The house-style constraint (load-bearing):** **explicit, registered migrations — never silent
best-effort coercion.** Silent coercion masks real incompatibilities and violates the
explicit-contract / detection-not-prevention posture. When no migration path exists, **detect**
(tier 2), do not guess. Migrations are *declarative data* (gossipable, safe), not arbitrary code.

**Done when:** a producer and consumer compiled against different schema versions interoperate via
an *explicitly registered* migration chain composed on the receive side; a missing migration path is
*detected and surfaced*, never silently coerced.

**Reuse:** the existing schema registry (`SchemaHandle::{publish_schema, get_schema, list_schemas,
seed_schemas_from_dir}`, `schemas/` KV prefix), capability `input_schema`/`output_schema`/`schema_id`
(`with_schema_id` / `CapFilter::with_schema`), and the tripwire idiom (`commit_conflicts` etc.).

---

## E1 · Tier 1 — Additive tolerance (verify + document)

Per the ROADMAP this is *"largely already true"* on the JSON payload paths (gateway, A2A, prompt
skills) via serde defaults / ignore-unknown — **"verify + document the property, not a milestone."**

- Add a focused test proving additive tolerance on a JSON payload path: a producer adds an optional
  field a consumer doesn't know (ignored), and omits a field the consumer defaults — round-trips
  without error.
- Document the property in `docs/guide/12-schema-lifecycle.md` (where it holds, where it doesn't —
  wire frames are the in-tree fixed-int codec, not JSON; this is a *payload* property).

**Gate G-E1:** the additive-tolerance round-trip test passes.

---

## E2 · Tier 2 — Compatibility detection (the tripwire)

A schema-version mismatch is **detected and made legible**, never silently accepted — the exact
idiom of `commit_conflicts` / `sys_namespace_violations` / `cap_authz_violations`.

- A `schema_mismatch` cumulative counter on `SystemStats` + `/stats`.
- Hook: when a consumer resolves against a `CapFilter::with_schema(expected)` and a provider matches
  `ns/name` but advertises a **different** `schema_id`, that exclusion is *counted* (and `warn!`-ed)
  rather than silently dropped — so schema drift is visible, not invisible. (Detection-not-prevention:
  the provider still advertises; the consumer routes around it, now with a legible signal.)
- A public read so an operator/SDK can see the per-`(ns,name)` advertised schema versions in the
  cluster (drift visibility).

**Gate G-E2:** a provider advertising `ns/name@v2` while a consumer filters for `@v1` ⇒ the consumer
excludes it **and** `schema_mismatch` increments; matching versions ⇒ no increment.

---

## E3 · Tier 3 — Registered, gossip-distributed migrations (the real feature)

Declarative `vN → vN+1` transforms published into the registry *alongside* the schemas, composed
`v1 → v2 → v3` on the receive path. Three sub-increments.

### E3a · The migration data model + registry

- `SchemaMigration { from: schema_id, to: schema_id, rules: Vec<MigrationRule> }` where
  `MigrationRule` is a small **declarative** set: `Rename { from_path, to_path }`,
  `Default { path, value }`, `Drop { path }`, `Coerce { path, to_type }`. (Declarative ⇒
  gossipable + safe; no code execution.) JSON-path addressed (flat first; nested later).
- Published to an owned registry namespace `schemas/migrations/{from}→{to}` (gossiped like schemas),
  via `SchemaHandle::publish_migration` / read via `get_migration` / `list_migrations`.
- Pure `apply_rules(value, &rules) -> Value` — unit-tested exhaustively.

**Gate G-E3a:** publish a migration; another node reads it; `apply_rules` performs each rule kind
deterministically; an unknown rule / malformed migration is rejected (not silently skipped).

### E3b · Migration-path resolution + composition

- Build the version graph from the published migrations; resolve a path `from → … → to` (BFS over
  the directed migration graph). Compose the rule chain.
- `migrate_payload(registry_view, from, to, payload) -> Result<Value, MigrationError>`:
  - path found → apply the composed chain;
  - **no path → `Err(NoMigrationPath)`** (tier-2 detection, *never* a guess);
  - cycle / ambiguity handled deterministically.
- Public API on `SchemaHandle` (pure over the local gossip view).

**Gate G-E3b:** `v1 → v3` composes `v1→v2` then `v2→v3` correctly; a missing `v2→v3` yields
`NoMigrationPath` (and increments `schema_mismatch`), never a partial/guessed result.

### E3c · Explicit application + AgentFacts tie-in

- A consumer applies migration **explicitly** before parsing (a `migrate_payload` call on the
  received bytes) — *not* an automatic silent transform on every signal (there is no per-message
  schema tag on the hot path, and auto-coercion is the anti-pattern). Document the pattern + a coop
  example or guide snippet.
- AgentFacts: a quilt fetcher reading a `certification.schemaVersion` it doesn't know can migrate the
  document via a published migration chain — the M16 pairing the ROADMAP calls out.

**Gate G-E3c:** an end-to-end test — producer emits `@v1`, consumer expects `@v3`, applies the
registered `v1→v2→v3` chain, and parses successfully; with the chain removed it surfaces
`NoMigrationPath` rather than mis-parsing.

---

## Sequencing & PRs

1. **E1** — verify + document additive tolerance (small).
2. **E2** — `schema_mismatch` tripwire (cheap, idiomatic).
3. **E3a** — migration data model + registry.
4. **E3b** — path resolution + composition.
5. **E3c** — explicit application + AgentFacts tie-in.

Each is its own PR. The migration engine (E3) is `default`-buildable (it's schema-registry
functionality, not `compliance`); E2's counter follows the always-present tripwire pattern. The
governing rule throughout: **registered + explicit + detect-don't-guess.**
