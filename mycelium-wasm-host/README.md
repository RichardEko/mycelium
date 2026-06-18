# mycelium-wasm-host

WASM Component host for Mycelium — the **install mechanism** of the autonomic-provisioning
chain (v2.0 **WS-E M12** → M15 → M14; see [`docs/plans/v2.0.md`](../docs/plans/v2.0.md) §WS-E).

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
  host mediating every import is legitimate.
- **Component export** `handle(request) -> response` — the capability entry point the host calls
  on an inbound invocation.

WIT imports = the component's *requires*; WIT exports = its *provides* (M15's one-hop contract):
a component imports the **mesh**, not other capabilities — a call to another skill is
runtime-mesh-resolved, never link-time-bound into a deployment set.

## Status (M12, in progress)

**Landed (this slice):** crate scaffold; the WIT contract; `confine` (the enforcement point,
unit-tested); `HostState` scoped operations proven against a live node; `bindgen!` host-import
impls; and the `WasmHost::instantiate` / `Instance::invoke` path (compiles against the generated
bindings; a negative test exercises it).

**Follow-up:** a positive end-to-end test with a real guest component (needs the wasm guest
toolchain — `cargo-component` / `wasm32-wasip2`), plus the content-addressed **pull/verify**
front of the loop. M15 (catalog resolve) and M14 (supervision) build on this.

## Build / test

```bash
cargo test  -p mycelium-wasm-host
cargo clippy -p mycelium-wasm-host --all-targets -- -D warnings
```

[`wit/host.wit`]: wit/host.wit
