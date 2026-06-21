//! RPC wire encoding and primary-side handlers (WS-G / G3 · Phase 3).
//!
//! Kinds are namespaced per board — `blackboard.{ns}.{op}` — so two boards on one agent never share
//! a handler channel. Payloads are compact length-prefixed binary; base64/JSON appears only at the
//! HTTP gateway boundary (Phase 4).
//!
//! **Replication is `Post`/`Ack`-only.** A `Claim` (and `Release`) does not change a mirror's
//! *liveness*: a claimed-but-unacked fact stays claimable in the mirror, which is exactly the
//! at-least-once re-queue a promotion wants. So the primary replicates only `Post` (a new fact) and
//! `Ack` (a consumed fact); the mirror is always a complete live view with no byte-offset replay.

use bytes::Bytes;
use std::collections::BTreeMap;
use std::sync::Arc;

use crate::{AttrMatch, Blackboard, Fact, Predicate};

pub(crate) const ST_OK: u8 = 0;
pub(crate) const ST_NOT_FOUND: u8 = 3;
pub(crate) const ST_ERR: u8 = 4;

const REP_POST: u8 = 1;
const REP_ACK: u8 = 2;

pub(crate) fn rpc_kind(ns: &str, op: &str) -> Arc<str> {
    Arc::from(format!("blackboard.{ns}.{op}"))
}

// ── Primitive codecs ─────────────────────────────────────────────────────────

fn put_str(buf: &mut Vec<u8>, s: &str) {
    buf.extend_from_slice(&(s.len() as u16).to_le_bytes());
    buf.extend_from_slice(s.as_bytes());
}
fn get_str(p: &[u8], off: &mut usize) -> Option<String> {
    let len = u16::from_le_bytes(p.get(*off..*off + 2)?.try_into().ok()?) as usize;
    *off += 2;
    let s = std::str::from_utf8(p.get(*off..*off + len)?).ok()?.to_string();
    *off += len;
    Some(s)
}
fn put_payload(buf: &mut Vec<u8>, b: &[u8]) {
    buf.extend_from_slice(&(b.len() as u32).to_le_bytes());
    buf.extend_from_slice(b);
}
fn get_payload(p: &[u8], off: &mut usize) -> Option<Bytes> {
    let len = u32::from_le_bytes(p.get(*off..*off + 4)?.try_into().ok()?) as usize;
    *off += 4;
    let b = Bytes::copy_from_slice(p.get(*off..*off + len)?);
    *off += len;
    Some(b)
}
fn put_attrs(buf: &mut Vec<u8>, attrs: &BTreeMap<String, String>) {
    buf.extend_from_slice(&(attrs.len() as u16).to_le_bytes());
    for (k, v) in attrs {
        put_str(buf, k);
        put_str(buf, v);
    }
}
fn get_attrs(p: &[u8], off: &mut usize) -> Option<BTreeMap<String, String>> {
    let n = u16::from_le_bytes(p.get(*off..*off + 2)?.try_into().ok()?) as usize;
    *off += 2;
    let mut m = BTreeMap::new();
    for _ in 0..n {
        let k = get_str(p, off)?;
        let v = get_str(p, off)?;
        m.insert(k, v);
    }
    Some(m)
}
fn put_predicate(buf: &mut Vec<u8>, pred: &Predicate) {
    buf.extend_from_slice(&(pred.attrs.len() as u16).to_le_bytes());
    for (k, m) in &pred.attrs {
        put_str(buf, k);
        match m {
            AttrMatch::Present => buf.push(0),
            AttrMatch::Equals(v) => {
                buf.push(1);
                put_str(buf, v);
            }
        }
    }
}
fn get_predicate(p: &[u8], off: &mut usize) -> Option<Predicate> {
    let n = u16::from_le_bytes(p.get(*off..*off + 2)?.try_into().ok()?) as usize;
    *off += 2;
    let mut pred = Predicate::new();
    for _ in 0..n {
        let k = get_str(p, off)?;
        let kind = *p.get(*off)?;
        *off += 1;
        match kind {
            0 => pred = pred.present(k),
            1 => {
                let v = get_str(p, off)?;
                pred = pred.eq(k, v);
            }
            _ => return None,
        }
    }
    Some(pred)
}
fn put_fact(buf: &mut Vec<u8>, f: &Fact) {
    buf.extend_from_slice(&f.id.to_le_bytes());
    put_attrs(buf, &f.attributes);
    put_payload(buf, &f.payload);
}
fn get_fact(p: &[u8], off: &mut usize) -> Option<Fact> {
    let id = u64::from_le_bytes(p.get(*off..*off + 8)?.try_into().ok()?);
    *off += 8;
    let attributes = get_attrs(p, off)?;
    let payload = get_payload(p, off)?;
    Some(Fact { id, attributes, payload })
}

// ── Request encoders (client side) ───────────────────────────────────────────

pub(crate) fn enc_post_req(attrs: &BTreeMap<String, String>, payload: &Bytes) -> Bytes {
    let mut buf = Vec::new();
    put_attrs(&mut buf, attrs);
    put_payload(&mut buf, payload);
    Bytes::from(buf)
}
pub(crate) fn enc_predicate_req(pred: &Predicate) -> Bytes {
    let mut buf = Vec::new();
    put_predicate(&mut buf, pred);
    Bytes::from(buf)
}
pub(crate) fn enc_id_req(id: u64) -> Bytes {
    Bytes::copy_from_slice(&id.to_le_bytes())
}
pub(crate) fn enc_replicate_post(f: &Fact) -> Bytes {
    let mut buf = vec![REP_POST];
    put_fact(&mut buf, f);
    Bytes::from(buf)
}
pub(crate) fn enc_replicate_ack(id: u64) -> Bytes {
    let mut buf = vec![REP_ACK];
    buf.extend_from_slice(&id.to_le_bytes());
    Bytes::from(buf)
}

// ── Response decoders (client side) ──────────────────────────────────────────

pub(crate) fn dec_id_resp(resp: &Bytes) -> Result<u64, crate::BlackboardError> {
    match resp.first() {
        Some(&ST_OK) => {
            let b: [u8; 8] = resp.get(1..9).and_then(|s| s.try_into().ok())
                .ok_or_else(|| crate::BlackboardError::Rpc("malformed id response".into()))?;
            Ok(u64::from_le_bytes(b))
        }
        _ => Err(status_err(resp)),
    }
}
pub(crate) fn dec_unit_resp(resp: &Bytes) -> Result<(), crate::BlackboardError> {
    match resp.first() {
        Some(&ST_OK) => Ok(()),
        Some(&ST_NOT_FOUND) => Err(crate::BlackboardError::NotFound),
        _ => Err(status_err(resp)),
    }
}
pub(crate) fn dec_facts_resp(resp: &Bytes) -> Result<Vec<Fact>, crate::BlackboardError> {
    if resp.first() != Some(&ST_OK) {
        return Err(status_err(resp));
    }
    let mut off = 1;
    let n = u32::from_le_bytes(resp.get(off..off + 4)
        .and_then(|s| s.try_into().ok())
        .ok_or_else(|| crate::BlackboardError::Rpc("malformed facts response".into()))?) as usize;
    off += 4;
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        out.push(get_fact(resp, &mut off).ok_or_else(|| crate::BlackboardError::Rpc("malformed fact".into()))?);
    }
    Ok(out)
}
/// `[ST_OK][u8 present][fact?]` → `Option<Fact>`.
pub(crate) fn dec_opt_fact_resp(resp: &Bytes) -> Result<Option<Fact>, crate::BlackboardError> {
    if resp.first() != Some(&ST_OK) {
        return Err(status_err(resp));
    }
    match resp.get(1) {
        Some(0) => Ok(None),
        Some(1) => {
            let mut off = 2;
            Ok(Some(get_fact(resp, &mut off).ok_or_else(|| crate::BlackboardError::Rpc("malformed claim".into()))?))
        }
        _ => Err(crate::BlackboardError::Rpc("malformed claim response".into())),
    }
}

fn status_err(resp: &Bytes) -> crate::BlackboardError {
    match resp.first() {
        Some(&ST_NOT_FOUND) => crate::BlackboardError::NotFound,
        _ => crate::BlackboardError::Rpc("board primary returned error".into()),
    }
}

fn facts_to_bytes(facts: &[Fact]) -> Vec<u8> {
    let mut buf = vec![ST_OK];
    buf.extend_from_slice(&(facts.len() as u32).to_le_bytes());
    for f in facts {
        put_fact(&mut buf, f);
    }
    buf
}

// ── Primary handlers ─────────────────────────────────────────────────────────

pub(crate) fn spawn_primary_handlers(bb: &Arc<Blackboard>) -> Vec<tokio::task::JoinHandle<()>> {
    let ns = &bb.cfg().namespace;
    let mut handles = Vec::with_capacity(6);

    macro_rules! handler {
        ($op:literal, $req:ident, $body:expr) => {{
            let me = Arc::clone(bb);
            let mut rx = bb.agent().service().rpc_rx(rpc_kind(ns, $op));
            handles.push(tokio::spawn(async move {
                while let Some($req) = rx.recv().await {
                    let reply: Vec<u8> = $body(&me, &$req);
                    me.agent().service().rpc_respond(&$req, Bytes::from(reply));
                }
            }));
        }};
    }

    handler!("post", req, |me: &Arc<Blackboard>, req: &mycelium::RpcRequest| {
        let p = req.payload();
        let mut off = 0;
        match (get_attrs(&p, &mut off), get_payload(&p, &mut off)) {
            (Some(attrs), Some(payload)) => match me.serve_post(attrs, payload) {
                Ok(id) => { let mut b = vec![ST_OK]; b.extend_from_slice(&id.to_le_bytes()); b }
                Err(_) => vec![ST_ERR],
            },
            _ => vec![ST_ERR],
        }
    });

    handler!("read", req, |me: &Arc<Blackboard>, req: &mycelium::RpcRequest| {
        let p = req.payload();
        let mut off = 0;
        match get_predicate(&p, &mut off) {
            Some(pred) => facts_to_bytes(&me.serve_read(&pred)),
            None => vec![ST_ERR],
        }
    });

    handler!("claim", req, |me: &Arc<Blackboard>, req: &mycelium::RpcRequest| {
        let p = req.payload();
        let mut off = 0;
        match get_predicate(&p, &mut off) {
            Some(pred) => match me.serve_claim(&pred) {
                Ok(Some(f)) => { let mut b = vec![ST_OK, 1]; put_fact(&mut b, &f); b }
                Ok(None) => vec![ST_OK, 0],
                Err(_) => vec![ST_ERR],
            },
            None => vec![ST_ERR],
        }
    });

    handler!("ack", req, |me: &Arc<Blackboard>, req: &mycelium::RpcRequest| {
        let p = req.payload();
        match p.get(..8).and_then(|s| <[u8; 8]>::try_from(s).ok()) {
            Some(b) => match me.serve_ack(u64::from_le_bytes(b)) {
                Ok(()) => vec![ST_OK],
                Err(crate::BlackboardError::NotFound) => vec![ST_NOT_FOUND],
                Err(_) => vec![ST_ERR],
            },
            None => vec![ST_ERR],
        }
    });

    handler!("release", req, |me: &Arc<Blackboard>, req: &mycelium::RpcRequest| {
        let p = req.payload();
        match p.get(..8).and_then(|s| <[u8; 8]>::try_from(s).ok()) {
            Some(b) => match me.serve_release(u64::from_le_bytes(b)) {
                Ok(()) => vec![ST_OK],
                Err(crate::BlackboardError::NotFound) => vec![ST_NOT_FOUND],
                Err(_) => vec![ST_ERR],
            },
            None => vec![ST_ERR],
        }
    });

    handler!("snapshot", req, |me: &Arc<Blackboard>, _req: &mycelium::RpcRequest| {
        match me.store() {
            Some(store) => facts_to_bytes(&store.snapshot_live()),
            None => vec![ST_ERR],
        }
    });

    handler!("depth", req, |me: &Arc<Blackboard>, _req: &mycelium::RpcRequest| {
        match me.store() {
            Some(store) => {
                let d = store.depth();
                let mut b = vec![ST_OK];
                b.extend_from_slice(&d.available.to_le_bytes());
                b.extend_from_slice(&d.inflight.to_le_bytes());
                b
            }
            None => vec![ST_ERR],
        }
    });

    handles
}

pub(crate) fn dec_depth_resp(resp: &Bytes) -> Result<crate::BoardDepth, crate::BlackboardError> {
    if resp.first() != Some(&ST_OK) {
        return Err(status_err(resp));
    }
    let available = u64::from_le_bytes(resp.get(1..9).and_then(|s| s.try_into().ok())
        .ok_or_else(|| crate::BlackboardError::Rpc("malformed depth".into()))?);
    let inflight = u64::from_le_bytes(resp.get(9..17).and_then(|s| s.try_into().ok())
        .ok_or_else(|| crate::BlackboardError::Rpc("malformed depth".into()))?);
    Ok(crate::BoardDepth { available, inflight })
}

/// The mirror's `replicate` handler: applies `Post`/`Ack` shipped by the live primary.
pub(crate) fn spawn_mirror_handlers(bb: &Arc<Blackboard>) -> Vec<tokio::task::JoinHandle<()>> {
    let ns = &bb.cfg().namespace;
    let me = Arc::clone(bb);
    let mut rx = bb.agent().service().rpc_rx(rpc_kind(ns, "replicate"));
    vec![tokio::spawn(async move {
        while let Some(req) = rx.recv().await {
            let p = req.payload();
            me.apply_replicated(&p);
            me.agent().service().rpc_respond(&req, Bytes::from_static(&[ST_OK]));
        }
    })]
}

impl Blackboard {
    /// Apply one replicated record (`Post` or `Ack`) into the mirror store.
    pub(crate) fn apply_replicated(&self, p: &[u8]) {
        let Some(store) = self.store() else { return };
        match p.first() {
            Some(&REP_POST) => {
                let mut off = 1;
                if let Some(f) = get_fact(p, &mut off)
                    && self.mark_mirrored(f.id)
                    && let Err(e) = store.post_with_id(f.id, f.attributes, f.payload)
                {
                    tracing::error!(error = %e, "blackboard: mirror post failed");
                }
            }
            Some(&REP_ACK) => {
                if let Some(b) = p.get(1..9).and_then(|s| <[u8; 8]>::try_from(s).ok()) {
                    let _ = store.discard(u64::from_le_bytes(b));
                }
            }
            _ => {}
        }
    }
}
