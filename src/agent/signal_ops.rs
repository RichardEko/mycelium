use crate::signal::SignalScope;
use bytes::Bytes;
use std::{sync::Arc, time::Duration};

use super::GossipAgent;

impl GossipAgent {
    /// Routes a request to the group member best suited to handle `kind`.
    ///
    /// Selects the target using [`suggest_leader`](Self::suggest_leader) — the member
    /// with the lowest `sys/load/{member}/{kind}` fill ratio within `max_age`. Ties are
    /// broken by `id_hash()`. Emits the request as [`SignalScope::Individual`] so only that
    /// member's handler fires, then awaits the first `result_kind` signal whose first 8
    /// payload bytes match the correlation nonce. Returns `None` on timeout.
    pub fn route_to(
        &self,
        group:       &str,
        kind:        impl Into<Arc<str>>,
        payload:     impl Into<Bytes>,
        result_kind: impl Into<Arc<str>>,
        max_age:     Duration,
        timeout:     Duration,
    ) -> impl std::future::Future<Output = Option<crate::signal::Signal>> {
        let kind_arc: Arc<str> = kind.into();
        let target = self.consensus().suggest_leader(group, &kind_arc, max_age);
        self.mesh().request(kind_arc, SignalScope::Individual(target), payload, result_kind, timeout)
    }
}
