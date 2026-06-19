# mycelium-agentfacts

WS-F **M16-A**: self-certified **AgentFacts** emission for a Mycelium domain — the "sovereign
patch" a NANDA-style agent-discovery quilt **pulls** at the edge. Built entirely on Mycelium's
public API (companion-crate contract, same as `mycelium-tuple-space` / `mycelium-wasm-host`).

## What it does

A domain self-elects to publish (run-dark by default). [`signed_agent_facts`] builds a document
from **live substrate state** (this node's `cap/` capabilities + schemas, locality, identity) and
**self-signs it with the node Ed25519 identity** (`GossipAgent::sign_with_identity`). A fetcher
verifies the signature against the embedded public key — **self-certified, no issuer/TRS authority**
(Core Principle 1; trust is the fetcher's decision). It is a superset of the A2A Agent Card.

## Decoupled from NANDA's churning field names

`AgentFacts` is a **stable, substrate-shaped** struct (our names: `capabilities`, `locality`,
`identity_pubkey`, `ttl_secs`). The NANDA JSON-LD mapping lives in **one place** — `to_nanda_jsonld`.
When the moving v0.3 spec renames a field (AgentFacts may even become "Agent Metadata Layer"), only
that serializer changes; the substrate-derived core never does. (ROADMAP §16 precursor rule:
"never couple to AgentFacts field/schema names".)

## Edge endpoint

`agent_facts_router(agent, opts)` mounts a public, un-gated `GET /.well-known/agent-facts.json` on
the agent's embedded gateway (via `with_http_routes`) that serves the freshly-built signed document
with `Cache-Control: max-age=<ttl>` — the TTL-scoped `facts_url` the quilt **pulls**. Deliberately
outside the `/gateway` scope wall (publicly fetchable + cryptographically verified, never
token-gated). Run-dark: nothing is published until the operator mounts it.

```rust,ignore
let opts = FactsOptions { endpoints: vec![..], locality: Some("eu-west".into()), ttl_secs: 300 };
agent.with_http_routes(mycelium_agentfacts::agent_facts_router(Arc::clone(&agent), opts));
agent.start().await?;   // GET /.well-known/agent-facts.json
```

## M16-B — the CRDT update layer (intra-domain, PUSH)

NANDA names a "CRDT-based update protocol" its v0.3 body doesn't deliver, because whole-document
signing is in tension with field-level merge. Mycelium's substrate *is* that protocol: each field
is an independently **node-signed** KV entry at `facts/{node}/{field}`, and **LWW + HLC +
anti-entropy** is the convergent, concurrent-safe merge.

- `publish_field(agent, field, value)` — self-sign one field, gossip it (LWW by HLC; distinct
  fields never conflict, same-field newest-wins).
- `read_verified_fields(agent, node, ttl_ms)` — read + **verify** a node's fields against its
  identity key, drop forged or stale ones, return the merged `{field: value}`. A forged
  `facts/{node}/…` write (LWW-accepted by the substrate — detection-not-prevention) reads back as
  absent. Late joiners catch up via anti-entropy. **Push intra-domain; pull at the edge.**

## Status

- **M16-A complete:** model + `from_agent` builder + `to_nanda_jsonld` + `SignedFacts`/`verify` +
  edge endpoint (`agent_facts_router`).
- **M16-B complete:** per-field signed CRDT layer (`publish_field` / `read_verified_fields`),
  proven single-node (LWW + forgery rejection) and **cross-node** (intra-domain gossip + verify).
- **Remaining WS-F:** OR-Map CRDT design note; schema-registry runtime migrations.

`evaluations`/`telemetry` provenance (when added) cites the WS2 audit trail's stable `content_hash`
— *self-attested-with-audit*, per the ROADMAP precursor criterion.

## Build / test

```bash
cargo test  -p mycelium-agentfacts
cargo clippy -p mycelium-agentfacts --all-targets -- -D warnings
```

[`signed_agent_facts`]: src/lib.rs
