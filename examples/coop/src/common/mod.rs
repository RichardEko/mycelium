//! Shared harness for the Food-Rescue Co-op suite.

pub mod bootstrap;
pub mod domain;
pub mod facts_lens;

pub use bootstrap::{alloc_ports, spawn_depot, Depot, DepotOpts};
pub use domain::Donation;
