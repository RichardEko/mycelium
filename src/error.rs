//! Error type for gossip protocol operations.
//!
//! All fallible public APIs return [`GossipError`]. The most common variant in production
//! is [`GossipError::Config`] (invalid configuration at startup); [`GossipError::Network`]
//! and [`GossipError::Io`] indicate connectivity problems.
//!
//! Lifecycle errors ([`GossipError::AlreadyRunning`], [`GossipError::Shutdown`]) are
//! returned by [`GossipAgent::start`] when it is called in the wrong agent state and
//! can be matched without parsing strings.

use thiserror::Error;

#[non_exhaustive]
#[derive(Error, Debug)]
pub enum GossipError {
    #[error("Network error: {0}")]
    Network(String),

    #[error("Configuration error: {0}")]
    Config(String),

    /// `start()` was called on an agent that is already running.
    #[error("Agent is already running; call start() only once")]
    AlreadyRunning,

    /// `start()` was called on an agent that has already been shut down.
    /// Create a new [`GossipAgent`] to restart.
    #[error("Agent has been shut down and cannot be restarted")]
    Shutdown,

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("TOML deserialization error: {0}")]
    Toml(#[from] toml::de::Error),

    #[error("Parsing error: {0}")]
    Parse(#[from] std::num::ParseIntError),
}
