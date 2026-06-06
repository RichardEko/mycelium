/// Result of a reliable delivery attempt via [`ServiceHandle::emit_reliable`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AckResult {
    Acknowledged,
    Timeout,
}
