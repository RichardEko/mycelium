# 2026-07-11 — mycelium-core tests now run in CI + wire back-compat gate

While building the mixed-version wire-compat gate, found that **mycelium-core's entire unit suite
was never RUN in CI** — only clippy-compiled (`clippy -p mycelium-core --lib --tests`). `cargo test
--lib` tests the root `mycelium` package only (core is a compiled dependency, `#[cfg(test)]`
invisible across the crate boundary), and there was no `-p mycelium-core` test job though every
companion crate had one. So codec/framing/hlc/store/swim (131 tests) — incl. the existing wire
back-compat tests — were unenforced. Same class as the decoder mini-fuzz gap (M2 Run-20).

**Fix (enforce):** added `-p mycelium-core` to the CI `Test` job + `make check-full`. Green
(131 pass, ~2 s), no pre-existing red.

**Fix (widen):** the v12 golden corpus already covered all 6 `WireMessage` variants, but the PREV
decoder `decode_wire_v11` was exercised on only StateRequest + Signal. Added
`decode_wire_v11_agrees_with_v12_on_every_shared_variant` (Data/Ping/StateResponse/Signal/SignedData).
Named regression gate; corpus discipline documented in `dev/testing/testing.md` + the test doc.

Not built (documented follow-up): a two-binary live mixed-version *cluster* test (v11 tag ↔ v12
HEAD) — slow/flake-prone nightly tier, separate from this deterministic codec gate.

Commit 979f8d6.
