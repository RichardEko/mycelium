//! `FsStore` — the filesystem-directory reference implementation of [`WikiStore`]. Trivial,
//! dependency-light, Docker-free, CI-testable; the shape an on-site datacentre's shared directory (or,
//! swapped for `S3Store`, a bucket) takes. Layout, per group:
//!
//! ```text
//! {root}/{group}/pages/{page-path}/manifest.json     (Manifest — written LAST)
//! {root}/{group}/pages/{page-path}/sec/{section}.json (Section)
//! ```
//!
//! Writes are atomic per object (write-temp-then-rename) and the manifest lands last, so a concurrent
//! reader — who reads the manifest, then the sections it names — never observes a half-applied edit.
//! Sections dropped by an edit are left as (invisible, manifest-unreferenced) orphans for a later GC
//! pass; nothing reads them.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::model::{Manifest, Page, Predicate, Section, SectionRef, WikiError};
use crate::store::WikiStore;

/// A filesystem-backed group wiki rooted at `{root}/{group}`.
pub struct FsStore {
    group_root: PathBuf,
    tmp_seq:    AtomicU64,
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

    /// Atomic write: serialise `value` to a temp file in the same directory, then rename over `target`.
    fn write_atomic<T: serde::Serialize>(&self, target: &Path, value: &T) -> Result<(), WikiError> {
        let parent = target.parent().expect("target has a parent");
        fs::create_dir_all(parent)?;
        let seq = self.tmp_seq.fetch_add(1, Ordering::Relaxed);
        let tmp = parent.join(format!(".tmp.{}.{}", std::process::id(), seq));
        let bytes = serde_json::to_vec_pretty(value)?;
        fs::write(&tmp, &bytes)?;
        fs::rename(&tmp, target)?;
        Ok(())
    }

    fn read_manifest(&self, page_dir: &Path) -> Result<Option<Manifest>, WikiError> {
        let mf = page_dir.join("manifest.json");
        match fs::read(&mf) {
            Ok(bytes) => Ok(Some(serde_json::from_slice(&bytes)?)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    fn read_section(&self, page_dir: &Path, id: &str) -> Result<Option<Section>, WikiError> {
        let f = page_dir.join("sec").join(format!("{id}.json"));
        match fs::read(&f) {
            Ok(bytes) => Ok(Some(serde_json::from_slice(&bytes)?)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Recursively collect `(page-path, page-dir)` for every dir under `pages/` that has a manifest.
    fn walk_pages(&self) -> Result<Vec<(String, PathBuf)>, WikiError> {
        let root = self.pages_root();
        let mut out = Vec::new();
        let mut stack = vec![root.clone()];
        while let Some(dir) = stack.pop() {
            if dir.join("manifest.json").is_file()
                && let Ok(rel) = dir.strip_prefix(&root)
            {
                let path = rel.to_string_lossy().replace('\\', "/");
                out.push((path, dir.clone()));
            }
            for entry in fs::read_dir(&dir)? {
                let entry = entry?;
                let p = entry.path();
                // `sec/` holds section files, not sub-pages — don't descend into it.
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
        let Some(manifest) = self.read_manifest(&dir)? else { return Ok(None) };
        let mut sections = Vec::with_capacity(manifest.order.len());
        for id in &manifest.order {
            // A manifest id whose section object is missing (mid-write / GC) is skipped, not an error.
            if let Some(sec) = self.read_section(&dir, id)? {
                sections.push(sec);
            }
        }
        Ok(Some(Page { path: page.to_string(), attributes: manifest.attributes, sections }))
    }

    fn query(&self, predicate: &Predicate) -> Result<Vec<SectionRef>, WikiError> {
        let mut hits = Vec::new();
        for (page, dir) in self.walk_pages()? {
            let Some(manifest) = self.read_manifest(&dir)? else { continue };
            for id in &manifest.order {
                if let Some(sec) = self.read_section(&dir, id)?
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

    fn write_page(
        &self, page: &str, sections: &[Section], attributes: &BTreeMap<String, String>,
    ) -> Result<(), WikiError> {
        let dir = self.page_dir(page)?;
        fs::create_dir_all(dir.join("sec"))?;
        // 1. Section objects first (atomic each) — so the manifest's referents all exist.
        for sec in sections {
            self.write_atomic(&dir.join("sec").join(format!("{}.json", sec.id)), sec)?;
        }
        // 2. Manifest LAST (atomic) — the commit point a reader enters through.
        let manifest = Manifest {
            order: sections.iter().map(|s| s.id.clone()).collect(),
            attributes: attributes.clone(),
        };
        self.write_atomic(&dir.join("manifest.json"), &manifest)?;
        // (Sections dropped by this edit are left as invisible orphans for a later GC pass.)
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
