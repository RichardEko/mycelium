//! Shared harness for the Food-Rescue Co-op suite.

pub mod bootstrap;
pub mod domain;
pub mod facts_lens;
pub mod loads;

pub use bootstrap::{alloc_ports, spawn_depot, Depot, DepotOpts};
pub use domain::Donation;
pub use loads::{announce_loads, Loads};
