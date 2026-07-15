# Ops Console

## Objective

A generic, read-only **dashboard over any gateway-enabled Mycelium node's operational endpoints**, in
one place — so you can watch a cluster (a demo, a showcase, or a real fleet) without modifying it. It is
a **dev/reference tool, explicitly not a shipped control plane**: the library-not-platform line holds —
a cluster is emergent from reachability + CA admission, there is no central runtime, and a customer forks
this or points Grafana at `/metrics` rather than running "the console" in production.

## How to run

Needs the `gateway` feature (on by default). See [shared setup](../README.md#shared-setup) for the
toolchain.

```bash
cargo run --example ops_console            # → http://127.0.0.1:8099/  (default target 127.0.0.1:9050)
```

Then point the **host box** at any gateway-enabled node:

- the **community** skills cluster (`:9050`),
- a **coop** demo,
- or a **showcase** — `conway` (`:9090`); `stigmergy_viz` / `llm_council_viz` print their gateway URL at
  startup; `microgrid_viz` (`:9091`) / `redistribution_viz` (`:9093`) *run with `--features gateway`*
  (those companion crates have the gateway off by default); `llm_agent` (`:9100`–`:9102`),
  `guardrail_viz` (`:9096`).

A server-side proxy sidesteps CORS, so the browser just points at the console.

## What it demonstrates

The console reads only **substrate** endpoints — never an app-specific `/state` — which is what makes it
generic:

| Tab | Endpoint | Shows |
|---|---|---|
| Stats | `/stats` | node runtime + tripwire counters |
| Fleet | `/gateway/fleet` | cluster snapshot (members, load, opacity) |
| Diagnose | `/gateway/diagnose` | the Legible-Emergence **fleet narrative** — "why is the fleet in this state", in plain English |
| Audit | `/gateway/audit` | the tamper-evident **signed audit trail** — nodes built `--features compliance` (verified badge · chain-tip hash · records) |
| KV | `/gateway/kv/keys` | the key set, with a value peek |
| Metrics | `/metrics` | Prometheus series (climbs live on `metrics`-built nodes) |

**Two-way linking (the `ui/viz` convention).** Every browser demo self-advertises its UI as two KV keys
— `ui/viz` = `http://host:port/`, `ui/label` = a short name — which the console reads from the target and
surfaces as a live **"↗ label"** link. Each dashboard carries the reverse **"⚙ Ops Console"** button
pre-targeted at its own gateway. One KV convention closes the loop both ways; the console hard-codes no
demo (a node that advertises nothing just hides the link).

## Dev notes

- **Read-only by design.** No endpoint here mutates the target — it observes. That is the whole point of
  the library-not-platform stance.
- The Audit and Metrics tabs degrade honestly: a node built without `compliance` / `metrics` shows an
  explicit "built without --features …" empty state rather than a blank panel.
- Source: [`main.rs`](main.rs) (the proxy + page) + [`ops_console.html`](ops_console.html) (the dashboard).
