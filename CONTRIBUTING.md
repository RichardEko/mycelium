# Contributing to Mycelium

## Contributor License Agreement — required before any PR is merged

Mycelium is dual-licensed: AGPL-3.0 for open-source use, and a commercial license for
proprietary use. To keep dual licensing viable, the project needs to own the copyright to
all contributions.

**Before your first pull request is merged, you must sign the CLA.**

The CLA is an inbound = outbound agreement: you grant the project a perpetual,
irrevocable, royalty-free licence to use, reproduce, modify, sublicense, and distribute
your contribution under any licence (including commercial licences). You retain your own
copyright. See [`CLA.md`](CLA.md) for the full text.

A CLA bot will prompt you automatically when you open a PR. PRs from contributors who
have not signed will not be merged.

## What to contribute

**Welcome:**
- Bug fixes with a clear reproduction case
- Performance improvements with benchmarks
- Documentation fixes and clarifications
- New examples that demonstrate existing features
- Test coverage improvements

**Out of scope (already planned):**
The following items are tracked in the active plan files and will be implemented by the
core team — please do not open PRs for these without coordinating first:

- Signal reorder buffer (`plan_signal_reorder_buffer.md`)
- Watcher scalability C2 residual (`plan_watcher_scalability_c2.md`)
- `--features compliance` (compliance and audit features)

See [`ROADMAP.md`](ROADMAP.md) for the full feature history and what is deferred.

## How to contribute

1. Fork the repository and create a branch from `main`
2. Make your changes; see the code style section below
3. Open a pull request — the CLA bot will prompt you if this is your first contribution

Keep PRs focused. A PR that fixes one bug is easier to review than one that fixes three.

## Code style

```bash
# Format
cargo fmt

# Lint — must pass with zero new warnings
cargo clippy --lib --tests -- -D warnings

# Tests — must all pass
cargo test --lib

# With optional features
cargo test --lib --features tls,metrics,a2a,llm
```

The baseline clippy warning count is 61 pre-existing `field_reassign_with_default`
warnings in test code. New warnings of any kind are a hard failure.

Comments: only when the *why* is non-obvious. No docstring novels. No TODO comments in
submitted code — open an issue instead.

## Licensing

By contributing you agree that your contribution is licensed under AGPL-3.0 (for
open-source use) and under the commercial license (for proprietary use), as described in
the CLA. The project's `LICENSE` file contains the full AGPL-3.0 text.

If you have questions about the licensing model, see [`GO_TO_MARKET.md`](GO_TO_MARKET.md)
— note this file is not tracked in the public repository; contact the maintainers
directly.

## Code of Conduct

This project follows the [Contributor Covenant v2.1](CODE_OF_CONDUCT.md). Please read it
before participating.
