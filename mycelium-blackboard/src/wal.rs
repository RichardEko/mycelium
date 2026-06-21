//! Write-ahead log for the board (WS-G / G3 · Phase 2) — durability for the claim discipline.
//!
//! Built against the documented exactly-once-effect contract (`docs/design/exactly-once-effect.md`)
//! and mirroring `mycelium-tuple-space`'s WAL shape: a magic + versioned header (a *newer* format is
//! refused, never silently truncated), length-framed records, corrupt-tail truncation on open, and
//! a compaction epoch. The blackboard's model is simpler than the tuple space's: there are **no
//! stage transitions**, so there is no compound `Complete` record — `Post` / `Claim` / `Ack` /
//! `Release` are each one indivisible record.
//!
//! **Replay liveness.** A fact is live (claimable) iff it was `Post`ed and not `Ack`ed. A
//! claimed-but-unacked fact (the claimer crashed) re-queues to claimable — at-least-once, exactly
//! the tuple space's "taken-but-unacked re-queues as abandoned" rule. `Ack` is the only terminal.

use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use bytes::Bytes;
use parking_lot::Mutex;

const WAL_MAGIC: &[u8; 6] = b"MBBWAL";
/// v1. The header refuses any version this build does not understand (no silent truncation of a
/// future format); a `PREV_WAL_VERSION` read window opens when v2 ever ships.
const WAL_VERSION: u16 = 1;
const WAL_HEADER_LEN: u64 = 8; // magic(6) + u16 LE version

const REC_POST: u8 = 1;
const REC_CLAIM: u8 = 2;
const REC_ACK: u8 = 3;
const REC_RELEASE: u8 = 4;

fn wal_header() -> [u8; WAL_HEADER_LEN as usize] {
    let mut h = [0u8; WAL_HEADER_LEN as usize];
    h[..6].copy_from_slice(WAL_MAGIC);
    h[6..8].copy_from_slice(&WAL_VERSION.to_le_bytes());
    h
}

/// One WAL record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum WalRecord {
    Post { id: u64, attributes: BTreeMap<String, String>, payload: Bytes },
    Claim { id: u64 },
    Ack { id: u64 },
    Release { id: u64 },
}

impl WalRecord {
    pub(crate) fn encode(&self, buf: &mut Vec<u8>) {
        let body_start = buf.len() + 5; // [kind u8][len u32]
        match self {
            WalRecord::Post { id, attributes, payload } => {
                buf.push(REC_POST);
                buf.extend_from_slice(&[0; 4]);
                buf.extend_from_slice(&id.to_le_bytes());
                buf.extend_from_slice(&(attributes.len() as u16).to_le_bytes());
                for (k, v) in attributes {
                    buf.extend_from_slice(&(k.len() as u16).to_le_bytes());
                    buf.extend_from_slice(k.as_bytes());
                    buf.extend_from_slice(&(v.len() as u32).to_le_bytes());
                    buf.extend_from_slice(v.as_bytes());
                }
                buf.extend_from_slice(&(payload.len() as u32).to_le_bytes());
                buf.extend_from_slice(payload);
            }
            WalRecord::Claim { id } => { buf.push(REC_CLAIM); buf.extend_from_slice(&[0; 4]); buf.extend_from_slice(&id.to_le_bytes()); }
            WalRecord::Ack { id } => { buf.push(REC_ACK); buf.extend_from_slice(&[0; 4]); buf.extend_from_slice(&id.to_le_bytes()); }
            WalRecord::Release { id } => { buf.push(REC_RELEASE); buf.extend_from_slice(&[0; 4]); buf.extend_from_slice(&id.to_le_bytes()); }
        }
        let body_len = (buf.len() - body_start) as u32;
        buf[body_start - 4..body_start].copy_from_slice(&body_len.to_le_bytes());
    }

    /// Decode one record; `None` on a truncated tail.
    fn decode(data: &[u8]) -> Option<(WalRecord, usize)> {
        if data.len() < 5 {
            return None;
        }
        let kind = data[0];
        let body_len = u32::from_le_bytes(data[1..5].try_into().ok()?) as usize;
        let body = data.get(5..5 + body_len)?;
        let rec = match kind {
            REC_POST => {
                let id = u64::from_le_bytes(body.get(..8)?.try_into().ok()?);
                let attr_count = u16::from_le_bytes(body.get(8..10)?.try_into().ok()?) as usize;
                let mut p = 10;
                let mut attributes = BTreeMap::new();
                for _ in 0..attr_count {
                    let kl = u16::from_le_bytes(body.get(p..p + 2)?.try_into().ok()?) as usize;
                    p += 2;
                    let k = std::str::from_utf8(body.get(p..p + kl)?).ok()?.to_string();
                    p += kl;
                    let vl = u32::from_le_bytes(body.get(p..p + 4)?.try_into().ok()?) as usize;
                    p += 4;
                    let v = std::str::from_utf8(body.get(p..p + vl)?).ok()?.to_string();
                    p += vl;
                    attributes.insert(k, v);
                }
                let pl = u32::from_le_bytes(body.get(p..p + 4)?.try_into().ok()?) as usize;
                let payload = body.get(p + 4..p + 4 + pl)?;
                WalRecord::Post { id, attributes, payload: Bytes::copy_from_slice(payload) }
            }
            REC_CLAIM => WalRecord::Claim { id: u64::from_le_bytes(body.get(..8)?.try_into().ok()?) },
            REC_ACK => WalRecord::Ack { id: u64::from_le_bytes(body.get(..8)?.try_into().ok()?) },
            REC_RELEASE => WalRecord::Release { id: u64::from_le_bytes(body.get(..8)?.try_into().ok()?) },
            _ => return None, // unknown kind — treat as corrupt tail
        };
        Some((rec, 5 + body_len))
    }
}

struct WalInner {
    file: File,
    file_len: u64,
    ops_since_sync: u64,
    /// Live records (Post). Terminal records (Ack) bump `acked`; compaction fires past a ratio.
    total: u64,
    acked: u64,
    /// Bumped on every compaction; a replay cursor from a prior epoch must restart.
    epoch: u64,
}

pub(crate) struct WalWriter {
    inner: Mutex<WalInner>,
    path: PathBuf,
    checkpoint_every: u64,
}

/// A live fact recovered from the WAL: `(id, attributes, payload)`.
pub(crate) type LiveFact = (u64, BTreeMap<String, String>, Bytes);

impl WalWriter {
    /// Open (or create) + replay. Returns the writer, the live (claimable) facts in id order, and
    /// the highest id seen (for the `next_id` fence).
    pub(crate) fn open(path: &Path, checkpoint_every: u64) -> io::Result<(Self, Vec<LiveFact>, Option<u64>)> {
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent)?;
        }
        let mut file = OpenOptions::new().read(true).create(true).append(true).open(path)?;
        let mut data = Vec::new();
        file.seek(SeekFrom::Start(0))?;
        file.read_to_end(&mut data)?;

        if data.is_empty() {
            file.write_all(&wal_header())?;
            data.extend_from_slice(&wal_header());
        } else if data.len() < WAL_HEADER_LEN as usize || &data[..WAL_MAGIC.len()] != WAL_MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("{} is not a mycelium-blackboard WAL (missing MBBWAL magic); refusing to open", path.display()),
            ));
        } else {
            let version = u16::from_le_bytes(data[6..8].try_into().expect("two header bytes"));
            if version != WAL_VERSION {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("{} is WAL format v{version}, but this build supports v{WAL_VERSION}; refusing to open (no silent truncation of newer formats)", path.display()),
                ));
            }
        }

        // Replay. Post adds a fact; Claim/Release are runtime-only (a claimed-unacked fact
        // re-queues as claimable — at-least-once); Ack is the sole terminal.
        struct FactState {
            attributes: BTreeMap<String, String>,
            payload: Bytes,
            acked: bool,
        }
        let mut facts: BTreeMap<u64, FactState> = BTreeMap::new();
        let mut total = 0u64;
        let mut acked = 0u64;
        let mut offset = WAL_HEADER_LEN as usize;
        while offset < data.len() {
            match WalRecord::decode(&data[offset..]) {
                None => break, // truncated tail
                Some((rec, consumed)) => {
                    offset += consumed;
                    match rec {
                        WalRecord::Post { id, attributes, payload } => {
                            total += 1;
                            facts.insert(id, FactState { attributes, payload, acked: false });
                        }
                        WalRecord::Ack { id } => {
                            acked += 1;
                            if let Some(f) = facts.get_mut(&id) {
                                f.acked = true;
                            }
                        }
                        // Claim / Release do not change replay liveness.
                        WalRecord::Claim { .. } | WalRecord::Release { .. } => {}
                    }
                }
            }
        }
        if offset < data.len() {
            file.set_len(offset as u64)?; // drop the corrupt/truncated tail
        }
        let max_id = facts.keys().next_back().copied();
        let live: Vec<LiveFact> = facts
            .into_iter()
            .filter(|(_, f)| !f.acked)
            .map(|(id, f)| (id, f.attributes, f.payload))
            .collect();
        let file_len = offset as u64;
        Ok((
            Self {
                inner: Mutex::new(WalInner { file, file_len, ops_since_sync: 0, total, acked, epoch: 0 }),
                path: path.to_path_buf(),
                checkpoint_every,
            },
            live,
            max_id,
        ))
    }

    pub(crate) fn append(&self, rec: &WalRecord) -> io::Result<()> {
        let mut g = self.inner.lock();
        let mut buf = Vec::new();
        rec.encode(&mut buf);
        g.file.write_all(&buf)?;
        g.file_len += buf.len() as u64;
        match rec {
            WalRecord::Post { .. } => g.total += 1,
            WalRecord::Ack { .. } => g.acked += 1,
            _ => {}
        }
        g.ops_since_sync += 1;
        if g.ops_since_sync >= self.checkpoint_every {
            g.file.sync_data()?;
            g.ops_since_sync = 0;
        }
        Ok(())
    }

    /// True once acked records dominate — a compaction would reclaim meaningful space.
    pub(crate) fn wants_compaction(&self) -> bool {
        let g = self.inner.lock();
        g.total >= 64 && g.acked * 2 >= g.total
    }

    /// Rewrite the WAL to contain only the live facts (supplied by the store under its lock) and
    /// atomically swap it in. Bumps the epoch.
    pub(crate) fn compact(&self, live: &[WalRecord]) -> io::Result<()> {
        let mut g = self.inner.lock();
        let tmp = self.path.with_extension("wal.compact");
        let mut buf = wal_header().to_vec();
        for rec in live {
            rec.encode(&mut buf);
        }
        std::fs::write(&tmp, &buf)?;
        std::fs::rename(&tmp, &self.path)?;
        let mut file = OpenOptions::new().read(true).append(true).open(&self.path)?;
        file.seek(SeekFrom::End(0))?;
        g.file = file;
        g.file_len = buf.len() as u64;
        g.total = live.len() as u64;
        g.acked = 0;
        g.ops_since_sync = 0;
        g.epoch += 1;
        Ok(())
    }

    pub(crate) fn epoch(&self) -> u64 {
        self.inner.lock().epoch
    }

    /// Force a durable sync (the periodic checkpoint task).
    pub(crate) fn sync(&self) -> io::Result<()> {
        let mut g = self.inner.lock();
        g.file.sync_data()?;
        g.ops_since_sync = 0;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_round_trips() {
        let attrs = BTreeMap::from([("k".to_string(), "v".to_string()), ("feeder".to_string(), "4".to_string())]);
        for rec in [
            WalRecord::Post { id: 7, attributes: attrs.clone(), payload: Bytes::from("p") },
            WalRecord::Claim { id: 7 },
            WalRecord::Ack { id: 7 },
            WalRecord::Release { id: 9 },
        ] {
            let mut buf = Vec::new();
            rec.encode(&mut buf);
            let (got, n) = WalRecord::decode(&buf).expect("decodes");
            assert_eq!(got, rec);
            assert_eq!(n, buf.len());
        }
    }

    #[test]
    fn truncated_tail_decodes_to_none() {
        let mut buf = Vec::new();
        WalRecord::Post { id: 1, attributes: BTreeMap::new(), payload: Bytes::from("xyz") }.encode(&mut buf);
        buf.truncate(buf.len() - 2);
        assert!(WalRecord::decode(&buf).is_none());
    }
}
