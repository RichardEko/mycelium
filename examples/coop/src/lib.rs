//! Food-Rescue Co-op example suite — shared harness (`common`).
//!
//! A network of **depot** nodes rescues surplus food and routes it to community kitchens with
//! **no central dispatcher**. Each example binary is a facet of this one world; this library is
//! the shared harness they all build on. See `docs/plans/example-suite.md` for the full plan.
//!
//! - [`common::domain`] — the constructive domain vocabulary (`Donation`, zones).
//! - [`common::facts_lens`] — mounts the WS-F AgentFacts edge endpoint on every depot, so any
//!   running example can be inspected live at `/.well-known/agent-facts.json`.
//! - [`common::bootstrap`] — spins up depot agents with consistent config (gateway + tls identity).

pub mod common;
