//! `mycelium-wasm-host` — the WASM Component host for Mycelium (v2.0 **WS-E M12**).
//!
//! This is the *install mechanism* of the autonomic-provisioning chain
//! (M12 → M15 → M14; see `docs/plans/v2.0.md` §WS-E). A capability can be shipped as a
//! sandboxed **WASM component**, pulled by content address, instantiated here, and made to
//! provide a capability on the local node — the OSGi-bundle analogue Rust natively lacks.
//!
//! ## The host ⇄ component boundary (the load-bearing artifact)
//!
//! A component runs **sandboxed, in-process** (wasmtime). It touches the node **only** through a
//! **WIT world** — the typed host/guest contract in [`wit/host.wit`]. This is in-process FFI over
//! the Component-Model canonical ABI, **not** a socket:
//!
//! - The host's **imports** (what a component may call) are a *capability-scoped projection* of
//!   Mycelium's public handles — confined KV, signal emit, log. This is the one place the
//!   substrate's detection-not-prevention posture flips to genuine **prevention**: the guest is
//!   untrusted foreign code in the node's own process, so the host mediating every import is
//!   legitimate (reject out-of-namespace writes, etc.).
//! - The component's **export** `handle(request) -> response` is the capability entry point the
//!   host calls on an inbound invocation.
//!
//! WIT imports = the component's *requires*; WIT exports = its *provides* (M15's one-hop contract).
//!
//! ## Companion-crate contract
//!
//! Built **entirely on Mycelium's public API**; `wasmtime` is confined to this crate and never
//! leaks into `mycelium` / `mycelium-core` (WS-A dep-tree invariant). Same posture as
//! `mycelium-tuple-space`.

mod artifact;
mod catalog;
mod confine;
mod host;
mod librarian;
mod mesh_source;
mod provisioner;
mod runtime;

pub use artifact::{
    verify_artifact, ArtifactId, ArtifactIdError, ArtifactKind, ArtifactSource, FsLibrarySource,
    InMemorySource, RangedArtifactSource, VerifyError,
};
pub use catalog::{
    publish_installable, InstallableCatalog, InstallableEntry, Manifest, ManifestError,
    ENTRY_FORMAT_VERSION, INSTALLABLE_PREFIX, MANIFEST_FILE,
};
pub use confine::{confine_key, ConfinementError, COMPONENT_KV_PREFIX};
pub use host::{HostState, Instance, Request, Response, WasmHost, WasmHostError};
pub use librarian::{
    librarian_filter, spawn_librarian, LibrarianConfig, LibrarianHandle, LIBRARIAN_NAME,
    LIBRARIAN_NS,
};
pub use mesh_source::{pull_artifact, serve_artifacts, MeshArtifactSource, ARTIFACT_FETCH_KIND};
pub use provisioner::{Provisioner, SupervisionPolicy};
pub use runtime::{
    cap_invoke_kind, ArtifactRuntime, BlobRuntime, InstallError, Installed, ProgressFn,
    RuntimeCtx, WasmComponentRuntime,
};
