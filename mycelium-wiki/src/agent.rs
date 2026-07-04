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

use crate::broker::{AccessError, AccessReply, Membership, StoreGrant};
use crate::lint::{structural_lint, LintReport, SemanticLinter};
use crate::model::{mint_section_id, Page, Predicate, Section, SectionId, SectionRef, WikiError};
use crate::reconcile::{DirectReconciler, ProposalEdit, Reconciler};
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
    /// How often the curator runs the lint pass (the group-function health check).
    pub lint_interval:  Duration,
}

impl WikiConfig {
    pub fn new(group: impl Into<Arc<str>>) -> Self {
        Self {
            group: group.into(),
            role: WikiRole::Auto,
            cap_refresh: Duration::from_secs(2),
            drain_interval: Duration::from_millis(200),
            lint_interval: Duration::from_secs(30),
        }
    }
    pub fn role(mut self, role: WikiRole) -> Self { self.role = role; self }
}

/// The curator's decision logic, bundled so a single [`Wiki::with_brain`] constructor carries both the
/// [`Reconciler`] (how proposals merge) and the optional semantic [`SemanticLinter`] (LLM
/// self-consistency). The structural lint is always on and needs no brain. [`Default`] is the no-LLM
/// curator: append-merge, structural lint only.
pub struct CuratorBrain {
    pub reconciler:    Box<dyn Reconciler>,
    pub semantic_lint: Option<Box<dyn SemanticLinter>>,
    /// The membership gate the curator applies to store-access requests (default [`Membership::Open`]).
    pub membership:    Membership,
}

impl Default for CuratorBrain {
    fn default() -> Self {
        Self { reconciler: Box::new(DirectReconciler), semantic_lint: None, membership: Membership::Open }
    }
}

impl CuratorBrain {
    /// A brain with a custom reconciler and no semantic lint.
    pub fn new(reconciler: Box<dyn Reconciler>) -> Self {
        Self { reconciler, semantic_lint: None, membership: Membership::Open }
    }
    /// Add the LLM self-consistency pass to the periodic lint.
    pub fn with_semantic_lint(mut self, linter: Box<dyn SemanticLinter>) -> Self {
        self.semantic_lint = Some(linter);
        self
    }
    /// Set the curator's membership gate for store-access requests.
    pub fn with_membership(mut self, membership: Membership) -> Self {
        self.membership = membership;
        self
    }
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

/// One drain's worth of proposals for a single section: the KV keys to tombstone once applied, and the
/// edits in queue order for the [`Reconciler`].
#[derive(Default)]
struct SectionBatch {
    keys:  Vec<Arc<str>>,
    edits: Vec<ProposalEdit>,
}

/// An agent-backed group wiki: propose/read/query over a coordinator-free curator discovered on the
/// capability ring, with emergent failover. The **data plane** is the injected [`WikiStore`] (each
/// node holds a handle to the *same* node-independent store). Construct after `agent.start()`.
pub struct Wiki<S: WikiStore + 'static> {
    agent:            Arc<GossipAgent>,
    cfg:              WikiConfig,
    store:            Arc<S>,
    /// How the curator merges a batch of same-section proposals (Phase 3). Default: the deterministic
    /// append-merge ([`DirectReconciler`]); a custom [`CuratorBrain`] injects the LLM curator.
    reconciler:       Box<dyn Reconciler>,
    /// The optional LLM self-consistency lint (structural lint is always on, needs no injection).
    semantic_lint:    Option<Box<dyn SemanticLinter>>,
    /// The curator's membership gate for store-access requests (only consulted while curating).
    membership:       Membership,
    /// The latest lint report (refreshed each lint tick while curating) — the group-function output.
    last_lint:        Mutex<LintReport>,
    /// Set whenever the curator writes the store; the periodic lint loop only runs a (whole-corpus)
    /// pass when this is set, so an idle wiki does **no** lint work (the Run-32 scalability fix). Starts
    /// `true` so the curator establishes a baseline over any pre-existing corpus on startup.
    lint_dirty:       AtomicBool,
    /// How many lint passes the curator has run (observability + the dirty-skip regression test).
    lint_passes:      AtomicU64,
    is_curator:       AtomicBool,
    curator_reg:      Mutex<Option<CapabilityReg>>,
    candidate_reg:    Mutex<Option<CapabilityReg>>,
    next_proposal_id: AtomicU64,
    tasks:            Mutex<Vec<JoinHandle<()>>>,
}

impl<S: WikiStore + 'static> Wiki<S> {
    /// Construct with the default (no-LLM) curator — append-merge reconcile + structural lint — and
    /// start whatever the role needs.
    pub async fn new(agent: Arc<GossipAgent>, cfg: WikiConfig, store: Arc<S>) -> Arc<Self> {
        Self::with_brain(agent, cfg, store, CuratorBrain::default()).await
    }

    /// Construct with a custom [`Reconciler`] and no semantic lint (convenience over [`with_brain`]).
    pub async fn with_reconciler(
        agent: Arc<GossipAgent>, cfg: WikiConfig, store: Arc<S>, reconciler: Box<dyn Reconciler>,
    ) -> Arc<Self> {
        Self::with_brain(agent, cfg, store, CuratorBrain::new(reconciler)).await
    }

    /// Construct with a full [`CuratorBrain`] (reconciler + optional semantic lint) and start whatever
    /// the role needs.
    pub async fn with_brain(
        agent: Arc<GossipAgent>, cfg: WikiConfig, store: Arc<S>, brain: CuratorBrain,
    ) -> Arc<Self> {
        let w = Arc::new(Self {
            agent,
            cfg,
            store,
            reconciler:       brain.reconciler,
            semantic_lint:    brain.semantic_lint,
            membership:       brain.membership,
            last_lint:        Mutex::new(LintReport::default()),
            lint_dirty:       AtomicBool::new(true),
            lint_passes:      AtomicU64::new(0),
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
    /// The underlying agent (used by the MCP tool registration in [`crate::mcp`]).
    pub(crate) fn agent(&self) -> &Arc<GossipAgent> { &self.agent }

    /// Stop this node's wiki background tasks (election / curator drain / lint / failover-watch) and
    /// retract its capability advertisements. **Idempotent.**
    ///
    /// This is required for a `Wiki` to be reclaimed: each background loop holds an `Arc<Self>` and
    /// runs unconditionally, so without `shutdown` the `Wiki` sits in a strong-reference cycle and its
    /// tasks run until the agent's runtime ends — a leak for any process that creates and discards
    /// wikis. Mirrors `Blackboard::shutdown`. Aborting the tasks releases their `Arc<Self>`, breaking
    /// the cycle.
    pub async fn shutdown(&self) {
        let handles: Vec<JoinHandle<()>> = std::mem::take(&mut *self.tasks.lock());
        for h in &handles {
            h.abort();
        }
        for h in handles {
            let _ = h.await; // await cancellation so the task's Arc<Self> is dropped before we return
        }
        *self.curator_reg.lock() = None; // drop the CapabilityReg → retract the ad
        *self.candidate_reg.lock() = None;
        self.is_curator.store(false, Ordering::Release);
    }

    /// Read a page directly from the store (any role — reads never go through the curator).
    pub fn read(&self, page: &str) -> Result<Option<Page>, WikiError> { self.store.read(page) }
    /// Query sections by attribute directly from the store.
    pub fn query(&self, pred: &Predicate) -> Result<Vec<SectionRef>, WikiError> { self.store.query(pred) }

    /// **Request store access** from the curator — the one-time broker handshake. Resolves the elected
    /// curator, RPCs it, and (if the curator's membership gate grants this node) returns a [`StoreGrant`]
    /// naming *where* to read. After this, open the store and read **directly** — the broker is not on
    /// the read path. The curator self-grants. Idempotent; safe to retry on [`AccessError::NoCurator`]
    /// (failover in progress) or [`AccessError::Rpc`] (transient).
    pub async fn request_store_access(&self) -> Result<StoreGrant, AccessError> {
        let group = self.cfg.group.to_string();
        if self.is_curator() {
            return Ok(StoreGrant { group, location: self.store.location() }); // I hold the store
        }
        let curator = self.resolve_role("curator").into_iter().next().ok_or(AccessError::NoCurator)?;
        let kind = format!("wiki.{}.access", self.cfg.group);
        let raw = self.agent.service()
            .rpc_call(curator, kind, Vec::<u8>::new(), Duration::from_secs(5))
            .await
            .map_err(|e| AccessError::Rpc(e.to_string()))?;
        let reply: AccessReply = serde_json::from_slice(&raw).map_err(|e| AccessError::Decode(e.to_string()))?;
        match (reply.granted, reply.location) {
            (true, Some(location)) => Ok(StoreGrant { group, location }),
            _                      => Err(AccessError::Denied),
        }
    }

    /// The most recent lint report (the curator refreshes it after a change; empty until the first
    /// pass, and on a non-curator node). Advisory — findings are surfaced, never auto-applied.
    pub fn last_lint(&self) -> LintReport { self.last_lint.lock().clone() }

    /// How many lint passes the curator has run since construction. Observability, and the anchor for
    /// the dirty-skip regression test (an idle wiki does not advance this).
    pub fn lint_pass_count(&self) -> u64 { self.lint_passes.load(Ordering::Relaxed) }

    /// Run a lint pass now over the whole corpus: the always-on [`structural_lint`] plus the injected
    /// semantic pass (if any). Stores and returns the report. Any node may call it on demand; the
    /// curator's loop runs it only after a write (see `lint_dirty`).
    pub async fn lint_now(&self) -> LintReport {
        self.lint_passes.fetch_add(1, Ordering::Relaxed);
        let pages = self.read_all_pages();
        let mut report = structural_lint(&pages);
        if let Some(linter) = &self.semantic_lint {
            report.findings.extend(linter.lint(&pages).await);
        }
        if !report.is_clean() {
            tracing::warn!(group = %self.cfg.group, findings = report.len(), "wiki: lint findings");
        }
        *self.last_lint.lock() = report.clone();
        report
    }

    /// Read every page the store lists (skipping any that error) — the corpus snapshot the lint runs on.
    fn read_all_pages(&self) -> Vec<Page> {
        self.store.list_pages().unwrap_or_default().into_iter()
            .filter_map(|path| self.store.read(&path).ok().flatten())
            .collect()
    }

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
        let drain = tokio::spawn(async move {
            let mut tick = tokio::time::interval(me.cfg.drain_interval);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tick.tick().await;
                me.drain_once().await;
            }
        });
        // The lint loop: the group-function health check runs only on the curator (one lint of record,
        // like one writer of record). Findings are surfaced, never auto-applied. It runs a (whole-corpus)
        // pass **only when the corpus changed** since the last one (`lint_dirty`) — an idle wiki does no
        // lint work (Run-32 scalability fix). `swap(false)` before the pass so a write landing *during*
        // the pass re-arms it for the next tick (no missed change).
        let me = Arc::clone(self);
        let lint = tokio::spawn(async move {
            let mut tick = tokio::time::interval(me.cfg.lint_interval);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tick.tick().await;
                if me.lint_dirty.swap(false, Ordering::AcqRel) {
                    me.lint_now().await;
                }
            }
        });
        // The access broker: answer store-access requests, gated on membership. Point-to-point RPC so a
        // grant (and, for a real object store, its scoped credential) never floods the cluster.
        let me = Arc::clone(self);
        let broker = tokio::spawn(async move {
            let mut rx = me.agent.service().rpc_rx(format!("wiki.{}.access", me.cfg.group));
            while let Some(req) = rx.recv().await {
                let requester = req.sender().to_string();
                let granted = me.membership.permits(&requester);
                let reply = AccessReply { granted, location: granted.then(|| me.store.location()) };
                me.agent.service().rpc_respond(&req, serde_json::to_vec(&reply).unwrap_or_default());
                tracing::info!(group = %me.cfg.group, requester = %requester, granted, "wiki: store-access request");
            }
        });
        let mut tasks = self.tasks.lock();
        tasks.push(drain);
        tasks.push(lint);
        tasks.push(broker);
        drop(tasks);
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

    /// Drain every pending proposal and apply it to the store. Only the curator runs this. Proposals
    /// are **grouped by target section** so a same-section conflict reaches the [`Reconciler`] as one
    /// batch (the whole point of a single writer — no lost update, no CRDT). Idempotent: a batch
    /// re-drained after a crash reconciles to the same result (the append-merge skips contained edits).
    async fn drain_once(&self) {
        let prefix = format!("wiki/{}/proposal/", self.cfg.group);
        let mut groups: BTreeMap<(String, SectionId), SectionBatch> = BTreeMap::new();
        for (key, value) in self.agent.kv().scan_prefix(&prefix) {
            let Ok(p) = serde_json::from_slice::<WireProposal>(&value) else {
                let _ = self.agent.kv().delete(key); // undecodable → drop, don't wedge the queue
                continue;
            };
            let batch = groups.entry((p.page, p.section)).or_default();
            batch.keys.push(key);
            batch.edits.push(ProposalEdit { heading: p.heading, body: p.body, attributes: p.attributes, author: p.author });
        }
        for ((page, section), batch) in groups {
            if self.apply_group(&page, &section, &batch.edits).await.is_ok() {
                for key in batch.keys {
                    let _ = self.agent.kv().delete(key); // tombstone only after the store write landed
                }
            }
        }
    }

    /// Reconcile one section's batch of proposals against its current text and write the page back
    /// (manifest-last). The reconcile is [`DirectReconciler`] by default (lossless append-merge) or the
    /// injected LLM curator.
    async fn apply_group(&self, page: &str, section: &SectionId, edits: &[ProposalEdit]) -> Result<(), WikiError> {
        let existing = self.store.read(page)?;
        let (mut sections, page_attrs) = match existing {
            Some(pg) => (pg.sections, pg.attributes),
            None      => (Vec::new(), BTreeMap::new()),
        };
        // Clone the current section so the immutable borrow ends before the reconcile await + the upsert.
        let current = sections.iter().find(|s| &s.id == section).cloned();
        let merged = self.reconciler.reconcile(page, section, current.as_ref(), edits).await;
        let sec = Section {
            id: section.clone(), heading: merged.heading, body: merged.body, attributes: merged.attributes,
        };
        match sections.iter_mut().find(|s| &s.id == section) {
            Some(slot) => *slot = sec,          // edit an existing section
            None        => sections.push(sec),  // new section
        }
        let r = self.store.write_page(page, &sections, &page_attrs);
        if r.is_ok() {
            self.lint_dirty.store(true, Ordering::Release); // the corpus changed → re-lint next tick
        }
        r
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::field_reassign_with_default)]
    use super::*;
    use std::sync::Weak;
    use mycelium::{GossipAgent, GossipConfig, NodeId};
    use crate::fs::FsStore;

    fn free_port() -> u16 {
        std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
    }

    /// Canary for the 2026-07-03 resource finding: the curator's background loops hold `Arc<Self>`, so
    /// without `shutdown` the `Wiki` is a strong-ref cycle that never drops. After `shutdown` aborts the
    /// tasks, the last strong ref frees the `Wiki` — a `Weak` no longer upgrades. (Pre-fix this
    /// assertion failed: `upgrade()` stayed `Some` because the tasks pinned an `Arc<Self>` forever.)
    #[tokio::test]
    async fn shutdown_breaks_the_task_cycle_and_frees_the_wiki() {
        let dir = tempfile::tempdir().unwrap();
        let port = free_port();
        let mut cfg = GossipConfig::default();
        cfg.bind_port = port;
        let agent = Arc::new(GossipAgent::new(NodeId::new("127.0.0.1", port).unwrap(), cfg));
        agent.start().await.unwrap();
        let store = Arc::new(FsStore::open(dir.path(), "ops").unwrap());
        let wcfg = WikiConfig {
            group: "ops".into(), role: WikiRole::Curator,
            cap_refresh: Duration::from_millis(300), drain_interval: Duration::from_millis(100),
            lint_interval: Duration::from_millis(200),
        };
        let wiki = Wiki::new(Arc::clone(&agent), wcfg, store).await;
        let weak: Weak<Wiki<FsStore>> = Arc::downgrade(&wiki);
        // Let the curator's drain + lint loops actually start and capture their Arc<Self>.
        tokio::time::sleep(Duration::from_millis(150)).await;

        wiki.shutdown().await;
        drop(wiki);
        assert!(weak.upgrade().is_none(), "after shutdown the Wiki is reclaimed (task cycle broken)");

        agent.shutdown_with_timeout(Duration::from_secs(5)).await;
    }

    /// Run-32 scalability fix: the curator runs a whole-corpus lint pass only after a change — an idle
    /// wiki does no lint work. Asserts the pass counter stays flat while idle, then advances on a write.
    #[tokio::test]
    async fn curator_lints_only_after_a_change() {
        let dir = tempfile::tempdir().unwrap();
        let port = free_port();
        let mut cfg = GossipConfig::default();
        cfg.bind_port = port;
        let agent = Arc::new(GossipAgent::new(NodeId::new("127.0.0.1", port).unwrap(), cfg));
        agent.start().await.unwrap();
        let store = Arc::new(FsStore::open(dir.path(), "ops").unwrap());
        let wcfg = WikiConfig {
            group: "ops".into(), role: WikiRole::Curator,
            cap_refresh: Duration::from_millis(300), drain_interval: Duration::from_millis(80),
            lint_interval: Duration::from_millis(120),
        };
        let wiki = Wiki::new(Arc::clone(&agent), wcfg, store).await;

        // Baseline pass (constructed dirty=true → the first lint tick establishes it).
        for _ in 0..50 {
            if wiki.lint_pass_count() >= 1 { break; }
            tokio::time::sleep(Duration::from_millis(40)).await;
        }
        let baseline = wiki.lint_pass_count();
        assert!(baseline >= 1, "the curator ran a baseline lint");

        // Idle: several lint intervals with no writes → the counter must not advance.
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert_eq!(wiki.lint_pass_count(), baseline, "an idle wiki runs no further lint passes");

        // A write re-arms the dirty flag → exactly the change triggers the next pass.
        let sid = wiki.new_section_id("p");
        wiki.propose("p", sid, "H", "body", BTreeMap::new());
        let mut advanced = false;
        for _ in 0..100 {
            if wiki.lint_pass_count() > baseline { advanced = true; break; }
            tokio::time::sleep(Duration::from_millis(40)).await;
        }
        assert!(advanced, "a write triggers exactly one further lint pass");

        wiki.shutdown().await;
        agent.shutdown_with_timeout(Duration::from_secs(5)).await;
    }
}
