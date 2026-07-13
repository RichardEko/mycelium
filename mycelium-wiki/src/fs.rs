//! `FsStore` — the filesystem-directory reference implementation of [`WikiStore`]. Trivial,
//! dependency-light, Docker-free, CI-testable; the shape an on-site datacentre's shared directory (or,
//! swapped for `S3Store`, a bucket) takes. Layout, per group:
//!
//! ```text
//! {root}/{group}/pages/{page-path}/manifest.v{N}.json     (Manifest, version N)
//! {root}/{group}/pages/{page-path}/sec/{section}.v{N}.json (Section, version N)
//! ```
//!
//! **Objects are versioned in the filename and immutable once published.** A write publishes a *new*
//! version with [`std::fs::hard_link`], which is atomic and **fails if that version already exists** —
//! so it is a true compare-and-swap: two writers racing for the same next version, exactly one wins and
//! the other gets [`WikiError::Conflict`]. There are no in-place overwrites and no lock files (so no
//! stale-lock deadlock on a writer crash — a crashed writer leaves at worst an ignored temp file). The
//! highest version present is authoritative; older versions are GC'd once a newer one commits. A reader
//! always reads a *complete* object (the content is fully written before the atomic link publishes it),
//! and entering via the manifest never observes a half-applied multi-section edit.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use serde::de::DeserializeOwned;

use crate::model::{Manifest, Page, Predicate, Section, SectionId, SectionRef, WikiError};
use crate::store::{VersionedPage, WikiStore};

/// A filesystem-backed group wiki rooted at `{root}/{group}`.
pub struct FsStore {
    group_root: PathBuf,
    tmp_seq:    AtomicU64,
}

/// Parse a versioned object filename `{base}.v{N}.json` → `(base, N)`. `None` if it doesn't match
/// (a stray non-versioned file, a temp file, an id with no version suffix). `base` may itself contain
/// dots — the version is taken from the *last* `.v<digits>` before `.json`.
fn parse_versioned(fname: &str) -> Option<(&str, u64)> {
    let stem = fname.strip_suffix(".json")?;
    let vpos = stem.rfind(".v")?;
    let (base, vpart) = stem.split_at(vpos);
    let digits = &vpart[2..];
    if base.is_empty() || digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    Some((base, digits.parse().ok()?))
}

impl FsStore {
    /// Open (creating if absent) the store for `group` under `root`. The group's pages live under
    /// `{root}/{group}/pages/`.
    pub fn open(root: impl AsRef<Path>, group: &str) -> Result<Self, WikiError> {
        let group_root = root.as_ref().join(group);
        fs::create_dir_all(group_root.join("pages"))?;
        Ok(Self { group_root, tmp_seq: AtomicU64::new(0) })
    }

    fn pages_root(&self) -> PathBuf { self.group_root.join("pages") }

    /// Resolve a page path safely under `pages/` — reject `..`, absolute, or empty components.
    fn page_dir(&self, page: &str) -> Result<PathBuf, WikiError> {
        let mut dir = self.pages_root();
        for comp in page.split('/') {
            if comp.is_empty() || comp == "." || comp == ".." || comp.contains('\\') {
                return Err(WikiError::BadPath(page.to_string()));
            }
            dir.push(comp);
        }
        Ok(dir)
    }

    fn sec_dir(page_dir: &Path) -> PathBuf { page_dir.join("sec") }

    fn tmp_name(&self) -> String {
        format!(".tmp.{}.{}", std::process::id(), self.tmp_seq.fetch_add(1, Ordering::Relaxed))
    }

    /// The highest committed version of object `base` in `dir`, with its path. `None` if `dir` is
    /// absent or holds no version of `base`.
    fn highest(&self, dir: &Path, base: &str) -> Result<Option<(u64, PathBuf)>, WikiError> {
        let rd = match fs::read_dir(dir) {
            Ok(rd) => rd,
            Err(e) if e.kind() == ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        let mut best: Option<(u64, PathBuf)> = None;
        for entry in rd {
            let entry = entry?;
            let name = entry.file_name();
            if let Some((b, n)) = parse_versioned(&name.to_string_lossy())
                && b == base
                && best.as_ref().is_none_or(|(bn, _)| n > *bn)
            {
                best = Some((n, entry.path()));
            }
        }
        Ok(best)
    }

    /// Read the highest committed version of object `base` in `dir` and deserialise it, returning
    /// `(version, value)`. Robust to a concurrent GC removing the head between the directory scan and
    /// the open (re-scans); `None` if no version exists.
    fn read_object<T: DeserializeOwned>(&self, dir: &Path, base: &str) -> Result<Option<(u64, T)>, WikiError> {
        for _ in 0..8 {
            let Some((n, path)) = self.highest(dir, base)? else { return Ok(None) };
            match fs::read(&path) {
                Ok(bytes) => return Ok(Some((n, serde_json::from_slice(&bytes)?))),
                Err(e) if e.kind() == ErrorKind::NotFound => continue, // GC raced the head away; re-scan
                Err(e) => return Err(e.into()),
            }
        }
        Ok(None) // repeatedly lost to GC (astronomically unlikely) — treat as absent
    }

    /// Every distinct section id that has an object in `sec_dir` (including in-flight orphans not yet
    /// in a manifest).
    fn section_ids(sec_dir: &Path) -> Result<Vec<String>, WikiError> {
        let mut ids = BTreeSet::new();
        match fs::read_dir(sec_dir) {
            Ok(rd) => for entry in rd {
                let entry = entry?;
                if let Some((base, _)) = parse_versioned(&entry.file_name().to_string_lossy()) {
                    ids.insert(base.to_string());
                }
            },
            Err(e) if e.kind() == ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
        Ok(ids.into_iter().collect())
    }

    /// Publish `bytes` as version `target` of `base` in `dir`, atomically via hard-link. `Ok(true)` if
    /// this call created it; `Ok(false)` if that version already existed (a concurrent writer won the
    /// slot). The content is fully written to a temp file first, so the published object is never torn.
    fn cas_publish(&self, dir: &Path, base: &str, target: u64, bytes: &[u8]) -> Result<bool, WikiError> {
        fs::create_dir_all(dir)?;
        let tmp = dir.join(self.tmp_name());
        fs::write(&tmp, bytes)?;
        let dst = dir.join(format!("{base}.v{target}.json"));
        let created = match fs::hard_link(&tmp, &dst) {
            Ok(()) => true,
            Err(e) if e.kind() == ErrorKind::AlreadyExists => false,
            Err(e) => { let _ = fs::remove_file(&tmp); return Err(e.into()); }
        };
        let _ = fs::remove_file(&tmp); // drop the temp name; the hard-linked object keeps the content
        Ok(created)
    }

    /// Compare-and-swap write: publish `bytes` as the successor of version `expected` (`None` ⇒ the
    /// first version). Returns the new version, or [`WikiError::Conflict`] if the slot was taken or the
    /// write did not become the head (a stale writer filling a GC gap below the real head — that must
    /// not count as a commit, or its content would be silently shadowed).
    fn write_object_cas(&self, dir: &Path, base: &str, expected: Option<u64>, bytes: &[u8]) -> Result<u64, WikiError> {
        let target = expected.unwrap_or(0) + 1;
        if !self.cas_publish(dir, base, target, bytes)? {
            return Err(WikiError::Conflict);
        }
        // Confirm we are the new head. If someone else is already higher, our version is either a
        // superseded intermediate or a gap-fill below the head; in both cases the safe move is to roll
        // back and report Conflict so the caller re-reads and re-reconciles against the true head.
        let head = self.highest(dir, base)?.map_or(0, |(n, _)| n);
        if head != target {
            let _ = fs::remove_file(dir.join(format!("{base}.v{target}.json")));
            return Err(WikiError::Conflict);
        }
        self.gc_below(dir, base, target); // bound disk to the live version (best effort)
        Ok(target)
    }

    /// Best-effort non-CAS write: publish `bytes` as head, retrying past any concurrent advance. For
    /// the single-writer convenience paths ([`write_page`](WikiStore::write_page)); not the curator.
    fn force_write(&self, dir: &Path, base: &str, bytes: &[u8]) -> Result<u64, WikiError> {
        for _ in 0..32 {
            let cur = self.highest(dir, base)?.map(|(n, _)| n);
            match self.write_object_cas(dir, base, cur, bytes) {
                Err(WikiError::Conflict) => continue,
                other => return other,
            }
        }
        Err(WikiError::Conflict)
    }

    /// Remove every version of `base` in `dir` below `keep_from` (best effort).
    fn gc_below(&self, dir: &Path, base: &str, keep_from: u64) {
        if let Ok(rd) = fs::read_dir(dir) {
            for entry in rd.flatten() {
                if let Some((b, n)) = parse_versioned(&entry.file_name().to_string_lossy())
                    && b == base && n < keep_from
                {
                    let _ = fs::remove_file(entry.path());
                }
            }
        }
    }

    /// Remove every version of `base` in `dir` (best effort) — used to drop a section a page edit no
    /// longer references.
    fn gc_all(&self, dir: &Path, base: &str) { self.gc_below(dir, base, u64::MAX); }

    /// Recursively collect `(page-path, page-dir)` for every dir under `pages/` that has a manifest.
    fn walk_pages(&self) -> Result<Vec<(String, PathBuf)>, WikiError> {
        let root = self.pages_root();
        let mut out = Vec::new();
        let mut stack = vec![root.clone()];
        while let Some(dir) = stack.pop() {
            if self.highest(&dir, "manifest")?.is_some()
                && let Ok(rel) = dir.strip_prefix(&root)
            {
                out.push((rel.to_string_lossy().replace('\\', "/"), dir.clone()));
            }
            for entry in fs::read_dir(&dir)? {
                let p = entry?.path();
                // `sec/` holds section objects, not sub-pages — don't descend into it.
                if p.is_dir() && p.file_name().map(|n| n != "sec").unwrap_or(false) {
                    stack.push(p);
                }
            }
        }
        Ok(out)
    }
}

impl WikiStore for FsStore {
    fn location(&self) -> String {
        self.group_root.to_string_lossy().into_owned()
    }

    fn read(&self, page: &str) -> Result<Option<Page>, WikiError> {
        let dir = self.page_dir(page)?;
        let Some((_mv, manifest)) = self.read_object::<Manifest>(&dir, "manifest")? else { return Ok(None) };
        let sec_dir = Self::sec_dir(&dir);
        let mut sections = Vec::with_capacity(manifest.order.len());
        for id in &manifest.order {
            // A manifest id whose section object is missing (mid-write / GC) is skipped, not an error.
            if let Some((_v, sec)) = self.read_object::<Section>(&sec_dir, id)? {
                sections.push(sec);
            }
        }
        Ok(Some(Page { path: page.to_string(), attributes: manifest.attributes, sections }))
    }

    fn read_versioned(&self, page: &str) -> Result<Option<VersionedPage>, WikiError> {
        let dir = self.page_dir(page)?;
        let Some((manifest_version, manifest)) = self.read_object::<Manifest>(&dir, "manifest")? else {
            return Ok(None);
        };
        let sec_dir = Self::sec_dir(&dir);
        let mut sections = BTreeMap::new();
        for id in Self::section_ids(&sec_dir)? {
            if let Some((v, sec)) = self.read_object::<Section>(&sec_dir, &id)? {
                sections.insert(sec.id.clone(), (v, sec));
            }
        }
        Ok(Some(VersionedPage {
            order: manifest.order, attributes: manifest.attributes, manifest_version, sections,
        }))
    }

    fn query(&self, predicate: &Predicate) -> Result<Vec<SectionRef>, WikiError> {
        let mut hits = Vec::new();
        for (page, dir) in self.walk_pages()? {
            let Some((_mv, manifest)) = self.read_object::<Manifest>(&dir, "manifest")? else { continue };
            let sec_dir = Self::sec_dir(&dir);
            for id in &manifest.order {
                if let Some((_v, sec)) = self.read_object::<Section>(&sec_dir, id)?
                    && predicate.matches(&sec.attributes)
                {
                    hits.push(SectionRef {
                        page: page.clone(), id: sec.id, heading: sec.heading, attributes: sec.attributes,
                    });
                }
            }
        }
        Ok(hits)
    }

    fn write_section(&self, page: &str, section: &Section, expected: Option<u64>) -> Result<u64, WikiError> {
        let sec_dir = Self::sec_dir(&self.page_dir(page)?);
        self.write_object_cas(&sec_dir, &section.id, expected, &serde_json::to_vec_pretty(section)?)
    }

    fn update_manifest(
        &self, page: &str, order: &[SectionId], attributes: &BTreeMap<String, String>, expected: Option<u64>,
    ) -> Result<u64, WikiError> {
        let dir = self.page_dir(page)?;
        let manifest = Manifest { order: order.to_vec(), attributes: attributes.clone() };
        self.write_object_cas(&dir, "manifest", expected, &serde_json::to_vec_pretty(&manifest)?)
    }

    fn write_page(
        &self, page: &str, sections: &[Section], attributes: &BTreeMap<String, String>,
    ) -> Result<(), WikiError> {
        let dir = self.page_dir(page)?;
        let sec_dir = Self::sec_dir(&dir);
        // 1. Section objects first (each force-published) — so the manifest's referents all exist.
        for sec in sections {
            self.force_write(&sec_dir, &sec.id, &serde_json::to_vec_pretty(sec)?)?;
        }
        // 2. Manifest LAST — the commit point a reader enters through.
        let manifest = Manifest {
            order: sections.iter().map(|s| s.id.clone()).collect(),
            attributes: attributes.clone(),
        };
        self.force_write(&dir, "manifest", &serde_json::to_vec_pretty(&manifest)?)?;
        // 3. GC section objects this edit no longer references (best effort).
        let keep: BTreeSet<&str> = sections.iter().map(|s| &*s.id).collect();
        if let Ok(ids) = Self::section_ids(&sec_dir) {
            for id in ids {
                if !keep.contains(id.as_str()) { self.gc_all(&sec_dir, &id); }
            }
        }
        Ok(())
    }

    fn list_pages(&self) -> Result<Vec<String>, WikiError> {
        let mut pages: Vec<String> = self.walk_pages()?.into_iter().map(|(p, _)| p).collect();
        pages.sort();
        Ok(pages)
    }
}

#[cfg(test)]
mod tests;
