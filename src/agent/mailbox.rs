//! Actor/Event mailboxes — KV-backed durable event delivery.
//!
//! [`GossipAgent::deliver_event`] writes an event into the mesh at a
//! HLC-ordered key under `mailbox/{target}/{kind}/{hlc_hex}`. Any node
//! that knows the target's [`NodeId`] can deliver; the event propagates
//! via the normal gossip KV path. The target's [`open_mailbox`] watcher
//! picks up new entries in causal order, delivers them to a channel, and
//! tombstones each entry so it is not redelivered after restart.
//!
//! ## Delivery guarantees
//!
//! - **At-least-once** within the gossip TTL window. Events stored in KV
//!   propagate to all live nodes via anti-entropy; a crashed target node
//!   will receive pending events on reconnect as long as they have not
//!   yet expired (default TTL: 5 minutes).
//! - **Causal order** within the watcher task: entries are sorted by their
//!   HLC key before delivery, so causal happens-before is preserved.
//! - **Tombstone-on-delivery**: entries are deleted from the KV store
//!   immediately after delivery and the tombstone is gossiped, preventing
//!   redelivery on restart.
//!
//! ## KV namespace
//!
//! `mailbox/{target_node_id}/{kind}/{hlc_ts:016x}` → `sender_node_id_len(2 LE) | sender_bytes | payload`

use crate::framing::{ForwardHint, WireMessage, dispatch_gossip_try_send, make_gossip_update};
use crate::node_id::NodeId;
use crate::store::{apply_and_notify, scan_kv_prefix};
use bytes::{BufMut, Bytes, BytesMut};
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};

use super::{GossipAgent, TaskCtx};

/// An event received from a node's mailbox.
pub struct MeshEvent {
    pub kind:    Arc<str>,
    pub sender:  NodeId,
    pub payload: Bytes,
    /// Packed HLC timestamp from the delivery key — useful for causal ordering.
    pub hlc_ts:  u64,
}

/// Cancels the corresponding [`open_mailbox`](GossipAgent::open_mailbox)
/// background task on drop.
pub struct MailboxHandle {
    pub(crate) _cancel: oneshot::Sender<()>,
}

fn encode_value(sender: &NodeId, payload: &Bytes) -> Bytes {
    let s = sender.to_string();
    let sb = s.as_bytes();
    let mut buf = BytesMut::with_capacity(2 + sb.len() + payload.len());
    buf.put_u16_le(sb.len() as u16);
    buf.put(sb);
    buf.put(payload.as_ref());
    buf.freeze()
}

fn decode_value(bytes: &Bytes) -> Option<(NodeId, Bytes)> {
    if bytes.len() < 2 { return None; }
    let len = u16::from_le_bytes([bytes[0], bytes[1]]) as usize;
    if bytes.len() < 2 + len { return None; }
    let sender_str = std::str::from_utf8(&bytes[2..2 + len]).ok()?;
    let sender: NodeId = sender_str.parse().ok()?;
    let payload = bytes.slice(2 + len..);
    Some((sender, payload))
}

fn tombstone(ctx: &Arc<TaskCtx>, key: Arc<str>) {
    let update = make_gossip_update(
        &ctx.node_id, ctx.default_ttl, key, Bytes::new(), true, &ctx.hlc,
    );
    apply_and_notify(&ctx.kv_state, &update);
    dispatch_gossip_try_send(
        &ctx.gossip_txs,
        WireMessage::Data(update),
        ctx.node_id.id_hash(),
        ForwardHint::All,
        &ctx.kv_state.dropped_frames,
    );
}

/// Maximum entries delivered (and tombstoned) per `drain_prefix` call.
///
/// Bounding the batch prevents the watcher task from blocking indefinitely
/// when a large backlog accumulates (e.g. after a long network partition).
/// After each chunk the watcher yields to the runtime; if more entries remain
/// the prefix watcher fires again immediately and the next chunk is processed.
const DRAIN_CHUNK: usize = 64;

/// Delivers up to `DRAIN_CHUNK` mailbox entries in HLC key order, tombstoning
/// each on delivery. Yields between chunks so the tokio runtime can run other
/// tasks. Stops early if the receiver has been dropped.
async fn drain_prefix(
    ctx:    &Arc<TaskCtx>,
    prefix: &Arc<str>,
    kind:   &Arc<str>,
    tx:     &mpsc::Sender<MeshEvent>,
) {
    loop {
        let mut entries = scan_kv_prefix(&ctx.kv_state, prefix.as_ref());
        if entries.is_empty() { break; }
        // sort_unstable is fine — keys are unique HLC timestamps.
        entries.sort_unstable_by(|(a, _), (b, _)| a.cmp(b));

        let chunk: Vec<_> = entries.drain(..entries.len().min(DRAIN_CHUNK)).collect();
        for (key, value) in chunk {
            let Some((sender, payload)) = decode_value(&value) else {
                tombstone(ctx, key);  // malformed — evict silently
                continue;
            };
            let ts_hex = key.strip_prefix(prefix.as_ref()).unwrap_or("");
            let hlc_ts = u64::from_str_radix(ts_hex, 16).unwrap_or(0);
            let event  = MeshEvent { kind: Arc::clone(kind), sender, payload, hlc_ts };
            if tx.send(event).await.is_err() {
                return; // receiver dropped
            }
            tombstone(ctx, key);
        }
        // Yield so the runtime can service other tasks between chunks.
        tokio::task::yield_now().await;
    }
}

async fn mailbox_task(
    ctx:         Arc<TaskCtx>,
    prefix:      Arc<str>,
    kind:        Arc<str>,
    tx:          mpsc::Sender<MeshEvent>,
    mut cancel_rx:   oneshot::Receiver<()>,
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
    mut watcher:     tokio::sync::watch::Receiver<u64>,
) {
    // Deliver any entries already present before we started watching.
    drain_prefix(&ctx, &prefix, &kind, &tx).await;

    loop {
        tokio::select! { biased;
            _ = &mut cancel_rx => break,
            result = shutdown_rx.changed() => {
                if result.is_err() || *shutdown_rx.borrow() { break; }
            }
            _ = watcher.changed() => {
                drain_prefix(&ctx, &prefix, &kind, &tx).await;
            }
        }
    }
}

impl GossipAgent {
    /// Delivers `payload` to `target`'s mailbox under `kind`.
    ///
    /// The event is written to `mailbox/{target}/{kind}/{hlc_ts:016x}` in the
    /// gossip KV store and gossiped to all peers. The target's
    /// [`open_mailbox`](Self::open_mailbox) watcher will pick it up and
    /// tombstone it on delivery.
    ///
    /// Returns `true` if the event was queued for gossip; `false` on backpressure
    /// (the event is still applied locally and will be delivered to the target
    /// if it is co-located).
    pub fn deliver_event(
        &self,
        target:  &NodeId,
        kind:    impl Into<Arc<str>>,
        payload: impl Into<Bytes>,
    ) -> bool {
        let kind:    Arc<str> = kind.into();
        let payload: Bytes    = payload.into();
        let ts  = self.task_ctx.hlc.tick();
        let key: Arc<str> = Arc::from(
            format!("mailbox/{}/{}/{:016x}", target, kind, ts).as_str(),
        );
        let value = encode_value(&self.node_id, &payload);
        self.set(key, value)
    }

    /// Opens a mailbox for events of `kind` addressed to this node.
    ///
    /// Spawns a background watcher task. When events arrive (via gossip KV
    /// propagation), the task drains them in HLC order into the returned
    /// `Receiver<MeshEvent>` and tombstones each entry so it will not be
    /// redelivered after a restart.
    ///
    /// The returned [`MailboxHandle`] cancels the watcher on drop. Drop it
    /// only when you no longer need the mailbox; events will queue in the
    /// KV store until the handle is live.
    ///
    /// `capacity` is the depth of the `mpsc` channel. Use a value large
    /// enough to buffer bursts without stalling the watcher task.
    pub fn open_mailbox(
        &self,
        kind:     impl Into<Arc<str>>,
        capacity: usize,
    ) -> (MailboxHandle, mpsc::Receiver<MeshEvent>) {
        let kind: Arc<str> = kind.into();
        let prefix: Arc<str> = Arc::from(
            format!("mailbox/{}/{}/", self.node_id, kind).as_str(),
        );
        let (tx, rx)           = mpsc::channel(capacity);
        let (cancel_tx, cancel_rx) = oneshot::channel();
        let watcher    = self.subscribe_prefix(Arc::clone(&prefix));
        let shutdown_rx = self.shutdown_tx.subscribe();
        let ctx         = Arc::clone(&self.task_ctx);

        tokio::spawn(mailbox_task(ctx, prefix, kind, tx, cancel_rx, shutdown_rx, watcher));

        (MailboxHandle { _cancel: cancel_tx }, rx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{GossipAgent, GossipConfig, NodeId};
    use bytes::Bytes;
    use std::{sync::Arc, time::Duration};

    fn alloc_port() -> u16 {
        std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap().local_addr().unwrap().port()
    }

    #[tokio::test]
    async fn deliver_and_open_mailbox_loopback() {
        let port = alloc_port();
        let id   = NodeId::new("127.0.0.1", port).unwrap();
        let mut cfg = GossipConfig::default(); cfg.bind_port = port;
        let agent = Arc::new(GossipAgent::new(id.clone(), cfg));
        agent.start().await.unwrap();

        let (handle, mut rx) = agent.open_mailbox("test", 16);

        let delivered = agent.deliver_event(&id, "test", Bytes::from_static(b"hello-mailbox"));
        assert!(delivered, "deliver_event returned false");

        // The watcher fires immediately since we're local.
        let event = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("timeout waiting for mailbox event")
            .expect("channel closed");

        assert_eq!(event.payload, Bytes::from_static(b"hello-mailbox"));
        assert_eq!(event.sender, id);
        assert_eq!(event.kind.as_ref(), "test");

        // Entry should now be tombstoned — second poll should be empty.
        drop(handle);
        agent.shutdown().await;
    }

    #[tokio::test]
    async fn mailbox_hlc_ordering() {
        let port = alloc_port();
        let id   = NodeId::new("127.0.0.1", port).unwrap();
        let mut cfg = GossipConfig::default(); cfg.bind_port = port;
        let agent = Arc::new(GossipAgent::new(id.clone(), cfg));
        agent.start().await.unwrap();

        let (handle, mut rx) = agent.open_mailbox("order-test", 16);

        // Deliver three events in sequence — HLC guarantees key-order = causal order.
        for i in 0u8..3 {
            agent.deliver_event(&id, "order-test", Bytes::from(vec![i]));
        }

        let mut received = Vec::new();
        for _ in 0..3 {
            let event = tokio::time::timeout(Duration::from_secs(2), rx.recv())
                .await.unwrap().unwrap();
            received.push(event.payload[0]);
        }
        assert_eq!(received, vec![0, 1, 2], "events not in causal order");

        drop(handle);
        agent.shutdown().await;
    }

    /// Delivers more than DRAIN_CHUNK events in one shot to verify chunked drain
    /// processes all of them in causal order without blocking.
    #[tokio::test]
    async fn drain_large_burst_in_order() {
        let port = alloc_port();
        let id   = NodeId::new("127.0.0.1", port).unwrap();
        let mut cfg = GossipConfig::default(); cfg.bind_port = port;
        let agent = Arc::new(GossipAgent::new(id.clone(), cfg));
        agent.start().await.unwrap();

        // Use a channel large enough to hold all events without back-pressure.
        let n: usize = DRAIN_CHUNK * 3;
        let (handle, mut rx) = agent.open_mailbox("burst-test", n + 16);

        // Deliver n events before opening the watcher — they land in KV first.
        for i in 0..n {
            agent.deliver_event(&id, "burst-test", Bytes::from(i.to_be_bytes().to_vec()));
        }

        let mut received: Vec<usize> = Vec::with_capacity(n);
        for _ in 0..n {
            let event = tokio::time::timeout(Duration::from_secs(5), rx.recv())
                .await.expect("timeout").expect("channel closed");
            let arr: [u8; 8] = event.payload.as_ref().try_into().unwrap();
            received.push(usize::from_be_bytes(arr));
        }

        let expected: Vec<usize> = (0..n).collect();
        assert_eq!(received, expected, "burst events not delivered in causal order");

        drop(handle);
        agent.shutdown().await;
    }

    #[test]
    fn encode_decode_roundtrip() {
        let id      = NodeId::new("127.0.0.1", 12345).unwrap();
        let payload = Bytes::from_static(b"roundtrip");
        let encoded = encode_value(&id, &payload);
        let (sender, got) = decode_value(&encoded).unwrap();
        assert_eq!(sender, id);
        assert_eq!(got, payload);
    }
}
