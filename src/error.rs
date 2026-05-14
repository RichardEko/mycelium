//! Error type for gossip protocol operations.
//!
//! All fallible public APIs return [`GossipError`]. The most common variant in production
//! is [`GossipError::Config`] (invalid configuration at startup); [`GossipError::Network`]
//! and [`GossipError::Io`] indicate connectivity problems.

use thiserror::Error;

#[derive(Error, Debug)]
pub enum GossipError {
    #[error("Network error: {0}")]
    Network(String),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("State error: {0}")]
    State(String),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("TOML deserialization error: {0}")]
    Toml(#[from] toml::de::Error),

    #[error("Parsing error: {0}")]
    Parse(#[from] std::num::ParseIntError),
}
