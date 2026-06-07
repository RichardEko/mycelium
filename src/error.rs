//! Error type for gossip protocol operations.
//!
//! All fallible public APIs return [`GossipError`]. Variants are fully typed so callers
//! can match specific failure modes without parsing strings:
//!
//! - **Configuration errors** ([`InvalidField`], [`FieldConflict`], [`NodeIdMismatch`]) are
//!   returned by [`GossipAgent::start`] and [`GossipConfig::validate`] when a config field
//!   has an invalid or conflicting value.
//! - **Framing errors** ([`FrameTooLarge`], [`UnsupportedWireVersion`]) surface when a TCP
//!   frame exceeds the size limit or arrives with an unsupported wire-protocol version.
//! - **Lifecycle errors** ([`AlreadyRunning`], [`Shutdown`]) are returned by
//!   [`GossipAgent::start`] when it is called in the wrong agent state.
//! - **I/O and parsing errors** ([`Io`], [`Toml`], [`Parse`]) wrap lower-level failures.

use thiserror::Error;

#[non_exhaustive]
#[derive(Error, Debug)]
pub enum GossipError {
    /// A configuration field has an invalid value.
    ///
    /// `field` is the field name (e.g. `"bind_port"`); `reason` is a human-readable
    /// explanation. Check the field's allowed range in [`GossipConfig`](crate::GossipConfig).
    #[error("Configuration error: field '{field}' — {reason}")]
    InvalidField { field: &'static str, reason: String },

    /// Two configuration fields have incompatible values.
    ///
    /// For example, `http_port` and `bind_port` must differ.
    #[error("Configuration conflict: '{field_a}' and '{field_b}' — {reason}")]
    FieldConflict { field_a: &'static str, field_b: &'static str, reason: String },

    /// The configured `node_id` does not match the resolved bind address.
    ///
    /// The node ID encodes the bind address and port; they must be identical.
    /// Recreate the `NodeId` using the actual bind address, or fix `bind_address` /
    /// `bind_port` so they match.
    #[error("node_id '{node_id}' does not match bind address '{bind_addr}'")]
    NodeIdMismatch { node_id: String, bind_addr: String },

    /// A gossip frame exceeds the maximum allowed size ([`MAX_FRAME_BYTES`](crate::MAX_FRAME_BYTES)).
    ///
    /// Reduce the value size or split the write into smaller keys.
    #[error("Frame {size} bytes exceeds maximum {limit} bytes")]
    FrameTooLarge { size: usize, limit: usize },

    /// A peer sent a frame using an unsupported wire-protocol version.
    ///
    /// `received` is the peer's version; `current` and `prev` are the versions this node
    /// accepts. `hint` suggests whether the peer is too old or too new.
    #[error("Unsupported wire version {received} (expected {current} or {prev}; {hint})")]
    UnsupportedWireVersion { received: u8, current: u8, prev: u8, hint: &'static str },

    /// `start()` was called on an agent that is already running.
    #[error("Agent is already running; call start() only once")]
    AlreadyRunning,

    /// `start()` was called on an agent that has already been shut down.
    /// Create a new [`GossipAgent`](crate::GossipAgent) to restart.
    #[error("Agent has been shut down and cannot be restarted")]
    Shutdown,

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("TOML deserialization error: {0}")]
    Toml(#[from] toml::de::Error),

    #[error("Parsing error: {0}")]
    Parse(#[from] std::num::ParseIntError),
}
