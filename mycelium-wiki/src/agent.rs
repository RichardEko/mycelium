//! The **control plane** (Phase 2) — the Mycelium side. A group's wiki is served by a single elected
//! **curator** discovered on the capability ring, with emergent ring-failover. The curator serialises
//! writes (drains the evaporating KV proposal queue and applies each to the store — the single writer
//! of record) and advertises the store location; every agent **reads the store directly**. Because the
//! store is node-independent, failover transfers nothing: a promoted curator resumes against the
//! *same* store and re-drains the *same* proposals.
//!
//! Feature-gated (`control-plane`) so Phase 1's pure data plane stays Mycelium-agnostic.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use parking_lot::Mutex;
use tokio::task::JoinHandle;

use mycelium::{CapFilter, Capability, CapabilityReg, GossipAgent, NodeId};

use crate::model::{mint_section_id, Page, Predicate, Section, SectionId, SectionRef, WikiError};
use crate::store::WikiStore;

/// A node's intended role in a group's wiki (mirrors `TupleRole` / `BoardRole`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WikiRole {
    /// Advertise as candidate, settle, then become curator (lowest candidate id) or a reader that
    /// watches for the curator to evaporate. No coordinator assigns roles.
    Auto,
    /// Force the curator role (single serving writer) — for a deployment that pins it.
    Curator,
    /// Read-only: never writes, never curates; reads the store directly and can still `propose`.
    Reader,
}

/// Configuration for an agent-backed [`Wiki`].
#[derive(Debug, Clone)]
pub struct WikiConfig {
    /// The group this wiki is scoped to (the capability + KV namespace segment).
    pub group:          Arc<str>,
    pub role:           WikiRole,
    /// Capability advertisement / refresh interval (also the failover-detection granularity).
    pub cap_refresh:    Duration,
    /// How often the curator drains the proposal queue.
    pub drain_interval: Duration,
}

impl WikiConfig {
    pub fn new(group: impl Into<Arc<str>>) -> Self {
        Self {
            group: group.into(),
            role: WikiRole::Auto,
            cap_refresh: Duration::from_secs(2),
            drain_interval: Duration::from_millis(200),
        }
    }
    pub fn role(mut self, role: WikiRole) -> Self { self.role = role; self }
}

/// A queued edit proposal — serialised into `wiki/{group}/proposal/{id}` (evaporating soft-state).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct WireProposal {
    page:       String,
    section:    SectionId,
    heading:    String,
    body:       String,
    #[serde(default)]
    attributes: BTreeMap<String, String>,
    author:     String,
}

/// An agent-backed group wiki: propose/read/query over a coordinator-free curator discovered on the
/// capability ring, with emergent failover. The **data plane** is the injected [`WikiStore`] (each
/// node holds a handle to the *same* node-independent store). Construct after `agent.start()`.
pub struct Wiki<S: WikiStore + 'static> {
    agent:            Arc<GossipAgent>,
    cfg:              WikiConfig,
    store:            Arc<S>,
    is_curator:       AtomicBool,
    curator_reg:      Mutex<Option<CapabilityReg>>,
    candidate_reg:    Mutex<Option<CapabilityReg>>,
    next_proposal_id: AtomicU64,
    tasks:            Mutex<Vec<JoinHandle<()>>>,
}

impl<S: WikiStore + 'static> Wiki<S> {
    /// Construct and start whatever the configured role needs.
    pub async fn new(agent: Arc<GossipAgent>, cfg: WikiConfig, store: Arc<S>) -> Arc<Self> {
        let w = Arc::new(Self {
            agent,
            cfg,
            store,
            is_curator:       AtomicBool::new(false),
            curator_reg:      Mutex::new(None),
            candidate_reg:    Mutex::new(None),
            next_proposal_id: AtomicU64::new(0),
            tasks:            Mutex::new(Vec::new()),
        });
        match w.cfg.role {
            WikiRole::Curator => w.become_curator(),
            WikiRole::Reader  => {}
            WikiRole::Auto    => {
                let me = Arc::clone(&w);
                w.tasks.lock().push(tokio::spawn(async move { me.run_election().await }));
            }
        }
        w
    }

    /// The group this wiki is scoped to.
    pub fn group(&self) -> &Arc<str> { &self.cfg.group }
    /// Is this node currently the serving curator?
    pub fn is_curator(&self) -> bool { self.is_curator.load(Ordering::Acquire) }
    /// The store handle (reads go here directly — the data plane).
    pub fn store(&self) -> &Arc<S> { &self.store }

    /// Read a page directly from the store (any role — reads never go through the curator).
    pub fn read(&self, page: &str) -> Result<Option<Page>, WikiError> { self.store.read(page) }
    /// Query sections by attribute directly from the store.
    pub fn query(&self, pred: &Predicate) -> Result<Vec<SectionRef>, WikiError> { self.store.query(pred) }

    /// Mint a fresh, stable section id for a **new** section on `page`.
    pub fn new_section_id(&self, page: &str) -> SectionId {
        let n = self.next_proposal_id.load(Ordering::Relaxed);
        mint_section_id(&self.cfg.group, page, n, self.agent.node_id().id_hash())
    }

    /// **Propose** an edit to `section` on `page` (a fresh id from [`new_section_id`](Self::new_section_id)
    /// for a new section, or an existing id for an edit). Writes an evaporating proposal to KV; the
    /// curator drains and applies it. Returns the proposal key.
    pub fn propose(
        &self, page: &str, section: SectionId, heading: impl Into<String>, body: impl Into<String>,
        attributes: BTreeMap<String, String>,
    ) -> String {
        let id = self.next_proposal_id.fetch_add(1, Ordering::Relaxed);
        // Globally-unique proposal id: node hash + local counter (two proposers never collide).
        let key = format!("wiki/{}/proposal/{:x}-{}", self.cfg.group, self.agent.node_id().id_hash(), id);
        let p = WireProposal {
            page: page.to_string(), section, heading: heading.into(), body: body.into(),
            attributes, author: self.agent.node_id().to_string(),
        };
        if let Ok(bytes) = serde_json::to_vec(&p) {
            let _ = self.agent.kv().set(key.clone(), bytes);
        }
        key
    }

    // ── roles ─────────────────────────────────────────────────────────────────

    fn resolve_role(&self, role: &str) -> Vec<NodeId> {
        let filter = CapFilter::new("wiki", format!("{}.{role}", self.cfg.group));
        self.agent.capabilities().resolve(&filter).into_iter().map(|(n, _)| n).collect()
    }

    fn become_curator(self: &Arc<Self>) {
        let reg = self.agent.capabilities().advertise_capability(
            Capability::new("wiki", format!("{}.curator", self.cfg.group)),
            self.cfg.cap_refresh,
        );
        *self.curator_reg.lock() = Some(reg);
        *self.candidate_reg.lock() = None; // retract the candidate ad
        self.is_curator.store(true, Ordering::Release);

        // The single-writer drain loop: drain the proposal queue → apply to the store.
        let me = Arc::clone(self);
        let h = tokio::spawn(async move {
            let mut tick = tokio::time::interval(me.cfg.drain_interval);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tick.tick().await;
                me.drain_once();
            }
        });
        self.tasks.lock().push(h);
        tracing::info!(group = %self.cfg.group, "wiki: serving as curator");
    }

    async fn run_election(self: Arc<Self>) {
        let reg = self.agent.capabilities().advertise_capability(
            Capability::new("wiki", format!("{}.candidate", self.cfg.group)),
            self.cfg.cap_refresh,
        );
        *self.candidate_reg.lock() = Some(reg);

        // Let candidate ads propagate before deciding (split-brain guard).
        tokio::time::sleep((self.cfg.cap_refresh * 2).max(Duration::from_secs(2))).await;

        loop {
            if !self.resolve_role("curator").is_empty() {
                // A curator exists — become a reader that watches for it to evaporate.
                self.watch_and_promote();
                return;
            }
            let mut candidates = self.resolve_role("candidate");
            candidates.sort_by_key(NodeId::to_string);
            let self_id = self.agent.node_id().to_string();
            match candidates.first() {
                Some(lowest) if lowest.to_string() == self_id => { self.become_curator(); return; }
                _ => tokio::time::sleep(self.cfg.cap_refresh).await,
            }
        }
    }

    /// Reader failover watch: the capability ring is the failure detector. Two consecutive empty
    /// resolves of `curator` (one refresh apart — split-brain guard) → re-run the election to promote.
    fn watch_and_promote(self: &Arc<Self>) {
        let me = Arc::clone(self);
        let h = tokio::spawn(async move {
            let mut tick = tokio::time::interval(me.cfg.cap_refresh);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tick.tick().await;
                if !me.resolve_role("curator").is_empty() { continue; }
                tokio::time::sleep(me.cfg.cap_refresh).await;
                if !me.resolve_role("curator").is_empty() { continue; }
                tracing::warn!(group = %me.cfg.group, "wiki: curator evaporated — re-electing");
                me.run_election().await;
                return;
            }
        });
        self.tasks.lock().push(h);
    }

    // ── the single-writer apply ────────────────────────────────────────────────

    /// Drain every pending proposal and apply it to the store. Only the curator runs this. Idempotent:
    /// an apply re-run after a crash (proposal not yet deleted) upserts the same section → same result.
    fn drain_once(&self) {
        let prefix = format!("wiki/{}/proposal/", self.cfg.group);
        for (key, value) in self.agent.kv().scan_prefix(&prefix) {
            let Ok(p) = serde_json::from_slice::<WireProposal>(&value) else {
                let _ = self.agent.kv().delete(key); // undecodable → drop, don't wedge the queue
                continue;
            };
            if self.apply(&p).is_ok() {
                let _ = self.agent.kv().delete(key); // tombstone only after the store write landed
            }
        }
    }

    /// Apply one proposal: upsert its section into the target page and write the page (manifest-last).
    /// The Phase-2 apply is a direct upsert; the LLM 3-way reconcile for same-section conflicts lands
    /// in Phase 3.
    fn apply(&self, p: &WireProposal) -> Result<(), WikiError> {
        let existing = self.store.read(&p.page)?;
        let (mut sections, page_attrs) = match existing {
            Some(page) => (page.sections, page.attributes),
            None        => (Vec::new(), BTreeMap::new()),
        };
        let sec = Section {
            id: p.section.clone(), heading: p.heading.clone(), body: p.body.clone(),
            attributes: p.attributes.clone(),
        };
        match sections.iter_mut().find(|s| s.id == p.section) {
            Some(slot) => *slot = sec,          // edit an existing section
            None        => sections.push(sec),  // new section
        }
        self.store.write_page(&p.page, &sections, &page_attrs)
    }
}
