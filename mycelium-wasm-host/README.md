# mycelium-wasm-host

WASM Component host for Mycelium — the **install mechanism** (M12) plus the **catalog
selection step** (M15) of the autonomic-provisioning chain (v2.0 **WS-E** M12 → M15 → M14;
see [`docs/plans/v2.0.md`](../docs/plans/v2.0.md) §WS-E).

A capability can be shipped as a sandboxed **WASM component**, pulled by content address,
instantiated here, and made to provide a capability on the local node — the OSGi-bundle
analogue Rust natively lacks. This crate is built **entirely on Mycelium's public API**;
`wasmtime` is confined to it and never leaks into `mycelium` / `mycelium-core` (the WS-A
dep-tree invariant — same companion-crate posture as `mycelium-tuple-space`).

## The host ⇄ component boundary

A component runs **sandboxed, in-process** (wasmtime). It touches the node **only** through the
WIT world in [`wit/host.wit`] — in-process FFI over the Component-Model canonical ABI, *not* a
socket:

- **Host imports** (what a component may call) — a *capability-scoped projection* of Mycelium's
  public handles: confined `kv`, `mesh` emit, `log`. Component KV is confined to
  `comp/{node}/{namespace}/…`; a component can never escape its subtree or reach the capability
  registry. This is the one place the substrate's *detection-not-prevention* posture flips to
  genuine **prevention** — the guest is untrusted foreign code in the node's own process, so the
  host mediating every import is legitimate. The host also provides a **restricted, deny-by-default
  WASI** context (no filesystem, network, env, or inherited stdio) — std-based guests link `wasi:*`
  at init, but their only real doors are the scoped imports above.
- **Component export** `handle(request) -> response` — the capability entry point the host calls
  on an inbound invocation.

WIT imports = the component's *requires*; WIT exports = its *provides* (M15's one-hop contract):
a component imports the **mesh**, not other capabilities — a call to another skill is
runtime-mesh-resolved, never link-time-bound into a deployment set.

## Status (M12, in progress)

**Landed:** crate scaffold; the WIT contract; `confine` (the enforcement point, unit-tested);
`HostState` scoped operations proven against a live node; restricted WASI; `bindgen!` host-import
impls; the `WasmHost::instantiate` / `Instance::invoke` path; **pull + verify + instantiate**
end to end — `ArtifactId` (content address = SHA-256), `verify_artifact` (run before the engine
ever sees the bytes), pluggable untrusted [`ArtifactSource`] (`InMemorySource`); and a
**real-guest end-to-end test** (`tests/e2e.rs`) — an actual WASM component (built from
`tests/fixtures/echo-component/`, committed as `tests/fixtures/echo_component.wasm`) is
instantiated, invoked, and its `kv` import is observed crossing into the confined subtree. The
`.wasm` is committed so CI needs no wasm toolchain; regenerate with the fixture's `build.sh`.

**M15 selection (landed):** `InstallableCatalog` / `InstallableEntry` (`src/catalog.rs`) resolve a
requirement against installable artifacts using the **same `CapFilter::matches`** the live resolver
uses — pointed at each entry's declared-provide `Capability` instead of a running `cap/` entry —
and pick the cheapest match. `WasmHost::provision_for(catalog, filter, source, state)` ties it to
M12: resolve → pull → verify → instantiate (`Ok(None)` if nothing satisfies the requirement). This
is **one hop, not a constraint solver** by design — *service* dependencies are runtime-mesh-resolved
(a component imports the mesh), never frozen into an install closure.

**Provisioner (landed — the loop closes):** `Provisioner` (`src/provisioner.rs`) is the app-layer
agent that watches demand, resolves unmet requirements against the catalog, and pulls + verifies +
instantiates + advertises — relieving demand. `provision_round()` is the testable convergence pass;
it self-elects probabilistically (herd damping) and is idempotent. **Core Principle 1:** it is a
regular agent on the public API, never a substrate mechanism — no coordinator assigns provisioning
duty; each node runs its own and self-elects. The full autonomic loop (declare requirement → demand
→ provision → advertise → demand relieved) is proven by `provisioner::tests`.

**Serve path (landed — provisioned capabilities are callable):** when the provisioner brings a
capability live it registers an RPC handler (`cap_invoke_kind(ns, name)`) and spawns a serve task
that owns the component instance (wasmtime stores are single-threaded → one task per instance) and
routes each inbound invocation to the component's `handle`, replying with its output. A caller
resolves the capability to a provider, then `rpc_call(provider, cap_invoke_kind(ns, name), …)`.

**M14 supervision (landed):** `Provisioner::supervise(filter, min_providers)` adds a
capability-presence invariant — keep ≥ `min_providers` live providers of `filter` alive. The same
`provision_round` reconciles it: a freshness-aware provider count below the floor triggers a
catalog resolve + `bring_live` — with **no organic demand**. Self-healing falls out for free: a
crashed provider's `cap/` entry evaporates, the count drops, the invariant re-provisions —
**restart and first-time provisioning are the same resolve-and-pull path**.

**Gossip-backed catalog (landed):** `publish_installable(kv, entry)` writes an installable artifact
to the cluster-wide `installable/{ns}/{name}/{artifact-hex}` KV prefix (declared-provide `Capability`
+ `ArtifactId` + cost, framed on the public `Capability::encode`); `InstallableCatalog::from_kv(kv)`
rebuilds the catalog from the gossiped view. So any node publishes artifacts and every provisioner
resolves against the live cluster catalog — no embedder-supplied in-memory list required.

**Fuel metering (landed):** `WasmHost::with_fuel_per_call(n)` grants each `invoke` a budget of `n`
wasm instructions; a runaway component **traps** (`WasmHostError::Invoke`) instead of hanging the
serve task. Instantiation runs with unlimited fuel so only `invoke` is bounded; `WasmHost::new()`
stays unmetered (zero overhead). Recommended when serving untrusted components.

**Follow-up:** a **mesh-bulk `ArtifactSource`** (surfacing the content-addressed bulk-fetch
client, §E.4.4); `spawn_blocking` (+ epoch interruption for wall-clock deadlines) for long-running
handlers; leased-consensus for strict singletons; optional Ed25519 signed-provenance.

[`ArtifactSource`]: src/artifact.rs

## Build / test

```bash
cargo test  -p mycelium-wasm-host
cargo clippy -p mycelium-wasm-host --all-targets -- -D warnings
```

[`wit/host.wit`]: wit/host.wit
