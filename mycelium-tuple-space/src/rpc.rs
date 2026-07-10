//! RPC wire encoding and primary-side handlers.
//!
//! RPC kinds are namespaced per tuple space — `tuple.{ns}.put` etc. — so two
//! `TupleSpace` instances with different namespaces can coexist on one agent
//! without sharing a handler channel.
//!
//! Payloads are compact binary (length-prefixed fields), not JSON: items can
//! be tens of megabytes and the internal RPC path must stay zero-inflation.
//! Base64/JSON appears only at the HTTP gateway boundary (Phase 4).

use bytes::Bytes;
use std::sync::Arc;
use std::time::Duration;

use crate::store::{Record, TupleStore, decode_records};
use crate::{TupleError, TupleSpace};

// ─── Status codes (first byte of every response) ────────────────────────────

pub(crate) const ST_OK: u8 = 0;
pub(crate) const ST_TIMEOUT: u8 = 1;
pub(crate) const ST_BACKPRESSURE: u8 = 2;
pub(crate) const ST_NOT_FOUND: u8 = 3;
pub(crate) const ST_ERR: u8 = 4;

pub(crate) fn rpc_kind(ns: &str, op: &str) -> Arc<str> {
    Arc::from(format!("tuple.{ns}.{op}"))
}

// ─── Request encoding (client side) ──────────────────────────────────────────

pub(crate) fn enc_put_req(stage: &str, payload: &Bytes) -> Bytes {
    let mut buf = Vec::with_capacity(2 + stage.len() + payload.len());
    buf.extend_from_slice(&(stage.len() as u16).to_le_bytes());
    buf.extend_from_slice(stage.as_bytes());
    buf.extend_from_slice(payload);
    Bytes::from(buf)
}

pub(crate) fn enc_take_req(stage: &str, timeout: Duration) -> Bytes {
    let mut buf = Vec::with_capacity(10 + stage.len());
    buf.extend_from_slice(&(stage.len() as u16).to_le_bytes());
    buf.extend_from_slice(stage.as_bytes());
    buf.extend_from_slice(&(timeout.as_millis() as u64).to_le_bytes());
    Bytes::from(buf)
}

pub(crate) fn enc_complete_req(id: u64, next_stage: &str, payload: &Bytes) -> Bytes {
    let mut buf = Vec::with_capacity(10 + next_stage.len() + payload.len());
    buf.extend_from_slice(&id.to_le_bytes());
    buf.extend_from_slice(&(next_stage.len() as u16).to_le_bytes());
    buf.extend_from_slice(next_stage.as_bytes());
    buf.extend_from_slice(payload);
    Bytes::from(buf)
}

pub(crate) fn enc_ack_req(id: u64) -> Bytes {
    Bytes::copy_from_slice(&id.to_le_bytes())
}

// ─── Keyed (M13 / WS-G) request encoding ─────────────────────────────────────
// Wire shape reuses the `[u16 len][str]` stage prefix for both stage and key.

pub(crate) fn enc_put_keyed_req(stage: &str, key: &str, payload: &Bytes) -> Bytes {
    let mut buf = Vec::with_capacity(4 + stage.len() + key.len() + payload.len());
    buf.extend_from_slice(&(stage.len() as u16).to_le_bytes());
    buf.extend_from_slice(stage.as_bytes());
    buf.extend_from_slice(&(key.len() as u16).to_le_bytes());
    buf.extend_from_slice(key.as_bytes());
    buf.extend_from_slice(payload);
    Bytes::from(buf)
}

pub(crate) fn enc_take_by_key_req(stage: &str, key: &str, timeout: Duration) -> Bytes {
    let mut buf = Vec::with_capacity(12 + stage.len() + key.len());
    buf.extend_from_slice(&(stage.len() as u16).to_le_bytes());
    buf.extend_from_slice(stage.as_bytes());
    buf.extend_from_slice(&(key.len() as u16).to_le_bytes());
    buf.extend_from_slice(key.as_bytes());
    buf.extend_from_slice(&(timeout.as_millis() as u64).to_le_bytes());
    Bytes::from(buf)
}

pub(crate) fn enc_complete_keyed_req(id: u64, next_stage: &str, key: &str, payload: &Bytes) -> Bytes {
    let mut buf = Vec::with_capacity(12 + next_stage.len() + key.len() + payload.len());
    buf.extend_from_slice(&id.to_le_bytes());
    buf.extend_from_slice(&(next_stage.len() as u16).to_le_bytes());
    buf.extend_from_slice(next_stage.as_bytes());
    buf.extend_from_slice(&(key.len() as u16).to_le_bytes());
    buf.extend_from_slice(key.as_bytes());
    buf.extend_from_slice(payload);
    Bytes::from(buf)
}

pub(crate) fn enc_depth_req(stage: Option<&str>) -> Bytes {
    let s = stage.unwrap_or("");
    let mut buf = Vec::with_capacity(2 + s.len());
    buf.extend_from_slice(&(s.len() as u16).to_le_bytes());
    buf.extend_from_slice(s.as_bytes());
    Bytes::from(buf)
}

// ─── Response decoding (client side) ─────────────────────────────────────────

/// `[status][u64 id]` → item id.
pub(crate) fn dec_id_resp(resp: &Bytes) -> Result<u64, TupleError> {
    match status_of(resp)? {
        ST_OK => {
            let b: [u8; 8] = resp
                .get(1..9)
                .and_then(|s| s.try_into().ok())
                .ok_or_else(|| malformed("id response"))?;
            Ok(u64::from_le_bytes(b))
        }
        other => Err(status_err(other, resp)),
    }
}

/// `[status][u64 id][payload…]` → `(id, payload)`.
pub(crate) fn dec_take_resp(resp: &Bytes) -> Result<(u64, Bytes), TupleError> {
    match status_of(resp)? {
        ST_OK => {
            let b: [u8; 8] = resp
                .get(1..9)
                .and_then(|s| s.try_into().ok())
                .ok_or_else(|| malformed("take response"))?;
            Ok((u64::from_le_bytes(b), resp.slice(9..)))
        }
        other => Err(status_err(other, resp)),
    }
}

pub(crate) fn dec_unit_resp(resp: &Bytes) -> Result<(), TupleError> {
    match status_of(resp)? {
        ST_OK => Ok(()),
        other => Err(status_err(other, resp)),
    }
}

/// `[status][u32 count] × ([u16 len][stage][u32 depth][u32 waiters][u32 inflight])`
pub(crate) fn dec_depth_resp(resp: &Bytes) -> Result<Vec<crate::TupleDepth>, TupleError> {
    if status_of(resp)? != ST_OK {
        return Err(status_err(resp[0], resp));
    }
    let mut out = Vec::new();
    let data = &resp[1..];
    let count = u32::from_le_bytes(
        data.get(..4)
            .and_then(|s| s.try_into().ok())
            .ok_or_else(|| malformed("depth response"))?,
    );
    let mut p = 4usize;
    for _ in 0..count {
        let len = u16::from_le_bytes(
            data.get(p..p + 2)
                .and_then(|s| s.try_into().ok())
                .ok_or_else(|| malformed("depth entry"))?,
        ) as usize;
        p += 2;
        let stage = std::str::from_utf8(
            data.get(p..p + len).ok_or_else(|| malformed("depth stage"))?,
        )
        .map_err(|_| malformed("depth stage utf8"))?;
        p += len;
        let next_u32 = |p: &mut usize| -> Result<u32, TupleError> {
            let v = u32::from_le_bytes(
                data.get(*p..*p + 4)
                    .and_then(|s| s.try_into().ok())
                    .ok_or_else(|| malformed("depth field"))?,
            );
            *p += 4;
            Ok(v)
        };
        let depth = next_u32(&mut p)?;
        let waiters = next_u32(&mut p)?;
        let inflight = next_u32(&mut p)?;
        out.push(crate::TupleDepth {
            stage: Arc::from(stage),
            depth,
            waiters,
            inflight,
        });
    }
    Ok(out)
}

fn status_of(resp: &Bytes) -> Result<u8, TupleError> {
    resp.first().copied().ok_or_else(|| malformed("empty response"))
}

fn status_err(status: u8, resp: &Bytes) -> TupleError {
    match status {
        ST_TIMEOUT => TupleError::Timeout,
        ST_NOT_FOUND => TupleError::NotFound,
        ST_BACKPRESSURE => {
            let retry = resp
                .get(1..9)
                .and_then(|s| <[u8; 8]>::try_from(s).ok())
                .map_or(500, u64::from_le_bytes);
            TupleError::Backpressure { retry_after_ms: retry }
        }
        _ => TupleError::Rpc("primary returned error status".into()),
    }
}

fn malformed(what: &str) -> TupleError {
    TupleError::Rpc(format!("malformed {what}"))
}

// ─── Replication / replay wire ───────────────────────────────────────────────

/// One replicated record, encoded with the WAL framing (single source of
/// truth for both the log format and the replication wire).
pub(crate) fn enc_record(rec: &Record) -> Bytes {
    let mut buf = Vec::with_capacity(64);
    rec.encode(&mut buf);
    Bytes::from(buf)
}

pub(crate) struct WalChunk {
    pub epoch: u64,
    pub done: bool,
    pub next_offset: u64,
    pub raw: Bytes,
}

/// `[u64 epoch][u64 from][u32 max_entries][u32 max_bytes]`
pub(crate) fn enc_wal_replay_req(
    epoch: u64,
    from: u64,
    max_entries: usize,
    max_bytes: usize,
) -> Bytes {
    let mut buf = Vec::with_capacity(24);
    buf.extend_from_slice(&epoch.to_le_bytes());
    buf.extend_from_slice(&from.to_le_bytes());
    buf.extend_from_slice(&(max_entries as u32).to_le_bytes());
    buf.extend_from_slice(&(max_bytes as u32).to_le_bytes());
    Bytes::from(buf)
}

/// `[ST_OK][u64 epoch][u8 done][u64 next_offset][raw records…]`
pub(crate) fn dec_wal_replay_resp(resp: &Bytes) -> Result<WalChunk, TupleError> {
    if status_of(resp)? != ST_OK {
        return Err(status_err(resp[0], resp));
    }
    let epoch = u64::from_le_bytes(
        resp.get(1..9)
            .and_then(|s| s.try_into().ok())
            .ok_or_else(|| malformed("replay epoch"))?,
    );
    let done = *resp.get(9).ok_or_else(|| malformed("replay done flag"))? == 1;
    let next_offset = u64::from_le_bytes(
        resp.get(10..18)
            .and_then(|s| s.try_into().ok())
            .ok_or_else(|| malformed("replay offset"))?,
    );
    Ok(WalChunk {
        epoch,
        done,
        next_offset,
        raw: resp.slice(18..),
    })
}

// ─── Primary-side handlers ───────────────────────────────────────────────────

/// Upper bound on a single parked take. Workers needing longer should re-call;
/// this keeps a forgotten handler task from parking forever.
const MAX_TAKE_PARK: Duration = Duration::from_secs(600);

/// Registers the six primary RPC handlers (put/take/complete/ack/depth/
/// wal_replay) and returns their task handles. Handlers exit when the agent
/// shuts down (`rpc_rx` yields `None`).
pub(crate) fn spawn_primary_handlers(
    ts: &Arc<TupleSpace>,
) -> Vec<tokio::task::JoinHandle<()>> {
    let ns = &ts.cfg().namespace;
    let mut handles = Vec::with_capacity(9);

    // put — fast, handled inline.
    {
        let me = Arc::clone(ts);
        let mut rx = ts.agent().service().rpc_rx(rpc_kind(ns, "put"));
        handles.push(tokio::spawn(async move {
            while let Some(req) = rx.recv().await {
                let p = req.payload();
                let reply = match parse_stage_prefix(&p) {
                    Some((stage, rest)) => match me.serve_put(&stage, rest) {
                        Ok(id) => ok_id(id),
                        Err(e) => err_resp(&e),
                    },
                    None => vec![ST_ERR],
                };
                me.agent().service().rpc_respond(&req, Bytes::from(reply));
            }
        }));
    }

    // take — parks; one task per request so a parked take never blocks the
    // handler loop.
    {
        let me = Arc::clone(ts);
        let mut rx = ts.agent().service().rpc_rx(rpc_kind(ns, "take"));
        handles.push(tokio::spawn(async move {
            while let Some(req) = rx.recv().await {
                let me2 = Arc::clone(&me);
                tokio::spawn(async move {
                    let p = req.payload();
                    let reply = match parse_take_req(&p) {
                        Some((stage, timeout)) => {
                            let timeout = timeout.min(MAX_TAKE_PARK);
                            match me2.serve_take(&stage, timeout, req.sender()).await {
                                Ok((id, payload)) => {
                                    let mut buf =
                                        Vec::with_capacity(9 + payload.len());
                                    buf.push(ST_OK);
                                    buf.extend_from_slice(&id.to_le_bytes());
                                    buf.extend_from_slice(&payload);
                                    buf
                                }
                                Err(TupleError::Timeout) => vec![ST_TIMEOUT],
                                Err(e) => err_resp(&e),
                            }
                        }
                        None => vec![ST_ERR],
                    };
                    me2.agent().service().rpc_respond(&req, Bytes::from(reply));
                });
            }
        }));
    }

    // complete — atomic ack + advance, inline.
    {
        let me = Arc::clone(ts);
        let mut rx = ts.agent().service().rpc_rx(rpc_kind(ns, "complete"));
        handles.push(tokio::spawn(async move {
            while let Some(req) = rx.recv().await {
                let p = req.payload();
                let reply = match parse_complete_req(&p) {
                    Some((id, stage, rest)) => match me.serve_complete(id, &stage, rest)
                    {
                        Ok(new_id) => ok_id(new_id),
                        Err(e) => err_resp(&e),
                    },
                    None => vec![ST_ERR],
                };
                me.agent().service().rpc_respond(&req, Bytes::from(reply));
            }
        }));
    }

    // put_keyed (M13) — inline.
    {
        let me = Arc::clone(ts);
        let mut rx = ts.agent().service().rpc_rx(rpc_kind(ns, "put_keyed"));
        handles.push(tokio::spawn(async move {
            while let Some(req) = rx.recv().await {
                let p = req.payload();
                let reply = match parse_put_keyed_req(&p) {
                    Some((stage, key, payload)) => match me.serve_put_keyed(&stage, key, payload) {
                        Ok(id) => ok_id(id),
                        Err(e) => err_resp(&e),
                    },
                    None => vec![ST_ERR],
                };
                me.agent().service().rpc_respond(&req, Bytes::from(reply));
            }
        }));
    }

    // take_by_key (M13) — parks; one task per request like `take`.
    {
        let me = Arc::clone(ts);
        let mut rx = ts.agent().service().rpc_rx(rpc_kind(ns, "take_by_key"));
        handles.push(tokio::spawn(async move {
            while let Some(req) = rx.recv().await {
                let me2 = Arc::clone(&me);
                tokio::spawn(async move {
                    let p = req.payload();
                    let reply = match parse_take_by_key_req(&p) {
                        Some((stage, key, timeout)) => {
                            let timeout = timeout.min(MAX_TAKE_PARK);
                            match me2.serve_take_by_key(&stage, &key, timeout, req.sender()).await {
                                Ok((id, payload)) => {
                                    let mut buf = Vec::with_capacity(9 + payload.len());
                                    buf.push(ST_OK);
                                    buf.extend_from_slice(&id.to_le_bytes());
                                    buf.extend_from_slice(&payload);
                                    buf
                                }
                                Err(TupleError::Timeout) => vec![ST_TIMEOUT],
                                Err(e) => err_resp(&e),
                            }
                        }
                        None => vec![ST_ERR],
                    };
                    me2.agent().service().rpc_respond(&req, Bytes::from(reply));
                });
            }
        }));
    }

    // complete_keyed (M13) — atomic ack + keyed advance, inline.
    {
        let me = Arc::clone(ts);
        let mut rx = ts.agent().service().rpc_rx(rpc_kind(ns, "complete_keyed"));
        handles.push(tokio::spawn(async move {
            while let Some(req) = rx.recv().await {
                let p = req.payload();
                let reply = match parse_complete_keyed_req(&p) {
                    Some((id, stage, key, payload)) => match me.serve_complete_keyed(id, &stage, key, payload) {
                        Ok(new_id) => ok_id(new_id),
                        Err(e) => err_resp(&e),
                    },
                    None => vec![ST_ERR],
                };
                me.agent().service().rpc_respond(&req, Bytes::from(reply));
            }
        }));
    }

    // ack — terminal, inline.
    {
        let me = Arc::clone(ts);
        let mut rx = ts.agent().service().rpc_rx(rpc_kind(ns, "ack"));
        handles.push(tokio::spawn(async move {
            while let Some(req) = rx.recv().await {
                let p = req.payload();
                let reply = match p.get(..8).and_then(|s| <[u8; 8]>::try_from(s).ok()) {
                    Some(b) => match me.serve_ack(u64::from_le_bytes(b)) {
                        Ok(()) => vec![ST_OK],
                        Err(e) => err_resp(&e),
                    },
                    None => vec![ST_ERR],
                };
                me.agent().service().rpc_respond(&req, Bytes::from(reply));
            }
        }));
    }

    // depth — read-only snapshot, inline.
    {
        let me = Arc::clone(ts);
        let mut rx = ts.agent().service().rpc_rx(rpc_kind(ns, "depth"));
        handles.push(tokio::spawn(async move {
            while let Some(req) = rx.recv().await {
                let p = req.payload();
                let reply = match (parse_stage_prefix(&p), me.store()) {
                    (Some((stage, _)), Some(store)) => {
                        let filter =
                            if stage.is_empty() { None } else { Some(stage.as_ref()) };
                        enc_depth_resp_from_store(&store, filter)
                    }
                    _ => vec![ST_ERR],
                };
                me.agent().service().rpc_respond(&req, Bytes::from(reply));
            }
        }));
    }

    // wal_replay — paginated catch-up; the secondary drives the loop.
    {
        let me = Arc::clone(ts);
        let mut rx = ts.agent().service().rpc_rx(rpc_kind(ns, "wal_replay"));
        handles.push(tokio::spawn(async move {
            while let Some(req) = rx.recv().await {
                let p = req.payload();
                let reply = match (parse_wal_replay_req(&p), me.store()) {
                    (Some((epoch, from, max_e, max_b)), Some(store)) => {
                        match store.wal_read_chunk(epoch, from, max_e, max_b) {
                            // Transient store: no WAL history to page — serve a *state*
                            // chunk instead (a joiner needs live items, not history; the
                            // succession-chain test found the old done-empty reply silently
                            // left late secondaries with a partial mirror). Offsets are
                            // id-cursors here; the epoch of a transient store is always 0,
                            // matching wal_position(), so the driver's epoch check holds.
                            Ok(None) => {
                                let (raw, next, done) = store.state_chunk(from, max_e, max_b);
                                enc_wal_replay_ok(0, done, next, &raw)
                            }
                            Ok(Some(c)) => {
                                enc_wal_replay_ok(c.epoch, c.done, c.next_offset, &c.raw)
                            }
                            Err(_) => vec![ST_ERR],
                        }
                    }
                    _ => vec![ST_ERR],
                };
                me.agent().service().rpc_respond(&req, Bytes::from(reply));
            }
        }));
    }

    handles
}

/// Registers the mirror's `replicate` handler: applies records shipped by
/// the live primary into the local mirror store.
pub(crate) fn spawn_mirror_handlers(
    ts: &Arc<TupleSpace>,
) -> Vec<tokio::task::JoinHandle<()>> {
    let ns = &ts.cfg().namespace;
    let me = Arc::clone(ts);
    let mut rx = ts.agent().service().rpc_rx(rpc_kind(ns, "replicate"));
    vec![tokio::spawn(async move {
        while let Some(req) = rx.recv().await {
            let p = req.payload();
            me.apply_records(&decode_records(&p));
            me.agent()
                .service()
                .rpc_respond(&req, Bytes::from_static(&[ST_OK]));
        }
    })]
}

fn enc_wal_replay_ok(epoch: u64, done: bool, next: u64, raw: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(18 + raw.len());
    buf.push(ST_OK);
    buf.extend_from_slice(&epoch.to_le_bytes());
    buf.push(u8::from(done));
    buf.extend_from_slice(&next.to_le_bytes());
    buf.extend_from_slice(raw);
    buf
}

fn parse_wal_replay_req(p: &Bytes) -> Option<(u64, u64, usize, usize)> {
    let epoch = u64::from_le_bytes(p.get(..8)?.try_into().ok()?);
    let from = u64::from_le_bytes(p.get(8..16)?.try_into().ok()?);
    let max_e = u32::from_le_bytes(p.get(16..20)?.try_into().ok()?) as usize;
    let max_b = u32::from_le_bytes(p.get(20..24)?.try_into().ok()?) as usize;
    Some((epoch, from, max_e, max_b))
}

pub(crate) fn enc_depth_resp_from_store(
    store: &TupleStore,
    stage: Option<&str>,
) -> Vec<u8> {
    let depths = store.depth(stage);
    let by_stage = store.inflight_by_stage();
    let mut buf = vec![ST_OK];
    buf.extend_from_slice(&(depths.len() as u32).to_le_bytes());
    for d in &depths {
        buf.extend_from_slice(&(d.stage.len() as u16).to_le_bytes());
        buf.extend_from_slice(d.stage.as_bytes());
        buf.extend_from_slice(&d.depth.to_le_bytes());
        buf.extend_from_slice(&d.waiters.to_le_bytes());
        let inflight = by_stage.get(d.stage.as_ref()).copied().unwrap_or(0);
        buf.extend_from_slice(&inflight.to_le_bytes());
    }
    buf
}

fn ok_id(id: u64) -> Vec<u8> {
    let mut buf = Vec::with_capacity(9);
    buf.push(ST_OK);
    buf.extend_from_slice(&id.to_le_bytes());
    buf
}

fn err_resp(e: &TupleError) -> Vec<u8> {
    match e {
        TupleError::Backpressure { retry_after_ms } => {
            let mut buf = vec![ST_BACKPRESSURE];
            buf.extend_from_slice(&retry_after_ms.to_le_bytes());
            buf
        }
        TupleError::NotFound => vec![ST_NOT_FOUND],
        TupleError::Timeout => vec![ST_TIMEOUT],
        _ => vec![ST_ERR],
    }
}

/// `[u16 stage_len][stage][rest…]` → `(stage, rest)`.
fn parse_stage_prefix(p: &Bytes) -> Option<(Arc<str>, Bytes)> {
    let len = u16::from_le_bytes(p.get(..2)?.try_into().ok()?) as usize;
    let stage = std::str::from_utf8(p.get(2..2 + len)?).ok()?;
    Some((Arc::from(stage), p.slice(2 + len..)))
}

fn parse_take_req(p: &Bytes) -> Option<(Arc<str>, Duration)> {
    let (stage, rest) = parse_stage_prefix(p)?;
    let ms = u64::from_le_bytes(rest.get(..8)?.try_into().ok()?);
    Some((stage, Duration::from_millis(ms)))
}

fn parse_complete_req(p: &Bytes) -> Option<(u64, Arc<str>, Bytes)> {
    let id = u64::from_le_bytes(p.get(..8)?.try_into().ok()?);
    let rest = p.slice(8..);
    let (stage, payload) = parse_stage_prefix(&rest)?;
    Some((id, stage, payload))
}

// Keyed (M13 / WS-G) — reuse the stage prefix for both stage and key.
fn parse_put_keyed_req(p: &Bytes) -> Option<(Arc<str>, Arc<str>, Bytes)> {
    let (stage, rest) = parse_stage_prefix(p)?;
    let (key, payload) = parse_stage_prefix(&rest)?;
    Some((stage, key, payload))
}
fn parse_take_by_key_req(p: &Bytes) -> Option<(Arc<str>, Arc<str>, Duration)> {
    let (stage, rest) = parse_stage_prefix(p)?;
    let (key, rest2) = parse_stage_prefix(&rest)?;
    let ms = u64::from_le_bytes(rest2.get(..8)?.try_into().ok()?);
    Some((stage, key, Duration::from_millis(ms)))
}
fn parse_complete_keyed_req(p: &Bytes) -> Option<(u64, Arc<str>, Arc<str>, Bytes)> {
    let id = u64::from_le_bytes(p.get(..8)?.try_into().ok()?);
    let rest = p.slice(8..);
    let (stage, rest2) = parse_stage_prefix(&rest)?;
    let (key, payload) = parse_stage_prefix(&rest2)?;
    Some((id, stage, key, payload))
}
