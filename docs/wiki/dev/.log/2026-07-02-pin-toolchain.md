## [2026-07-02] ingest | pin rust-toolchain to 1.96.0

`rust-toolchain.toml` pinned `stable` → `1.96.0` (+ `components = ["clippy","rustfmt"]`,
`profile = "minimal"`), and all 10 CI `dtolnay/rust-toolchain@stable` → `@1.96.0` (fuzz
stays `@nightly`). Pinning the file alone is insufficient: CI installs the clippy component
for whatever `@stable` resolves to, which would then mismatch the file's override and break
`cargo clippy` — hence both are pinned in lockstep. Rationale + bump procedure captured in
the testing conventions page. Root cause: new-stable clippy lints reddening `-D warnings`
on unrelated PRs (bit twice: int_plus_one, manual_is_multiple_of — Runs 28–29).
