# Group-wiki concurrent edits — section addressing, merge semantics, curator state machine, identity/access

**Status:** 📐 **design record, pre-build** (2026-07-02). This is the Phase 0 artifact the
[`mycelium-wiki` sketch](../plans/mycelium-wiki.md) calls for — it formalises the load-bearing areas
the sketch flagged: (1) the **section addressing + merge unit** (§1–§2), (2) the **curator role
state machine** (§3), and (3) the **identity/access mapping** — how a wiki relates to
Capability / Skill / Group, and the normative "competence is a capability, knowledge is not" rule
(§4). §6 is a **pre-build `lib.rs` API sketch** of the curator surface (the `WikiRole`/`CuratorState`
shape). Written *before* the crate exists so the mechanism is settled and reviewable; promotes to the
crate's `lib.rs` doc + a `v2-…-wiki.md` build plan when the demand trigger fires. Nothing here
changes core — it is a discipline over the public KV/signal/capability API, the way
[`exactly-once-effect.md`](exactly-once-effect.md) is.

The whole record exists to answer one question: **how does a group of LLM agents edit a shared prose
wiki concurrently without either losing edits (naive LWW on whole pages) or diverging forever
(free-for-all LLM merge)?** The answer is *make the common case a convergent CRDT and the rare case a
serialised single-writer*, and prove each half.

---

## 0. Notation & invariants to preserve

- The substrate gives **per-key LWW + HLC**: for one key, all replicas converge to the write with the
  greatest packed-HLC timestamp (deterministic byte tiebreak on equal stamps —
  `store.rs::lww_wins`). This is the *only* convergence primitive we get, and everything below is
  built to stay inside it.
- **I1 (convergence):** any two replicas that have seen the same set of writes hold the same wiki.
  Non-negotiable — it is the substrate's core guarantee; a design that breaks it is rejected.
- **I2 (no lost durable edit):** an accepted contribution is never silently dropped by a concurrent
  contribution. (Naive whole-page LWW violates I2; that is the entire problem.)
- **I3 (curator-optional readability):** with no live curator, the wiki is fully **readable** and
  contributions **persist** (as evaporating proposals); only *reconciliation* pauses. This is the
  management-as-intent litmus ([`../wiki/domain/theory/management-as-intent.md`](../wiki/domain/theory/management-as-intent.md)).

---

## 1. Section addressing — the merge unit

A page is not one key. A page is an **ordered list of sections**, each an independently-keyed LWW
value. The page is the reconciliation of its sections at read time.

### 1.1 Keys

```
wiki/{group}/page/{page-path}/meta                 → PageMeta   (section order + page-level fields)
wiki/{group}/page/{page-path}/sec/{section-id}     → SectionBody (the prose of one section)
```

- `{page-path}` is a kebab path (`incidents/cert-rotation`). `{section-id}` is a **stable opaque
  id**, *not* a heading text and *not* a line/ordinal number (see §1.3 — this is the crux of
  stability under concurrent edits).
- `PageMeta.order: Vec<SectionId>` gives the render order; section bodies are addressed by id, so a
  reorder is a `meta` write that touches no body, and a body edit is a `sec/{id}` write that touches
  no `meta`. **Different concerns → different keys → no false LWW collision.** This is the same move
  the KV store already relies on (secondary indices keyed separately from data) generalised to prose.

### 1.2 Why section-granular satisfies I2 for the common case

Two contributors editing *different* sections of the same page write *different keys* — both writes
survive, LWW never sees a conflict, I2 holds trivially, and I1 holds because each key converges
independently. The empirical claim behind "common case": in a curated knowledge base, near-all
concurrent edits touch different sections (an incident page's "symptoms" vs its "resolution"), the
same way near-all KV writes touch different keys. Section granularity converts the *default* editing
pattern into a lost-update-free CRDT with zero LLM involvement.

### 1.3 Section ids must be stable and content-independent

A section id is minted **once**, at section creation, and never changes — it is content-addressed at
birth but identity-stable after (`sid = base32(hash(group ‖ page-path ‖ mint-hlc ‖ nonce))`,
truncated). It is **not** derived from the heading or the body, because:

- deriving from the heading means renaming a heading re-keys the section → the old key tombstones,
  the new key inserts, and a concurrent body edit to the "same" section lands on the *old* key and is
  lost (I2 violation);
- deriving from ordinal position means inserting a section above shifts every id below → mass
  spurious re-key, same failure.

Stable ids are what make "edit section X" and "rename/move section X" independent, non-conflicting
operations. The id lives in `PageMeta.order` and in the `sec/{id}` key; a heading is just a field of
`SectionBody`, freely editable.

### 1.4 Section create / delete / reorder

- **Create:** contributor mints `sid`, writes `sec/{sid}`, and proposes a `meta` update inserting
  `sid` into `order` at a position. Two concurrent creates → two `sec/` keys (both survive) + two
  concurrent `meta` writes (§2.2 handles the `order` merge).
- **Delete:** tombstone `sec/{sid}` **and** propose `sid`'s removal from `order`. A body edit racing a
  delete: LWW on `sec/{sid}` decides (delete = tombstone; a later body write with a greater HLC
  resurrects — deliberate: "someone kept editing while I deleted" resolves to keep, the safe default
  for knowledge, and it converges because it is ordinary LWW).
- **Reorder:** a pure `meta.order` permutation, no body writes.

---

## 2. Merge semantics

Three cases, in increasing cost. The design goal is to keep the expensive case rare and *serialised*.

### 2.1 Different sections (the common case) — pure CRDT, no curator

Covered by §1.2. Convergent by the substrate; no reconciliation needed; works with **no live
curator** (I3). Contributors write section bodies directly for this case — a contribution that its
author knows targets a distinct/new section need not queue (it can't lose-update). The crate's
`propose` API decides "direct write vs queue" by whether the target section id already exists and is
being concurrently touched — conservatively, a *new* section is a direct write; an edit to an
*existing* section is a queued proposal (§2.2).

### 2.2 Same section, or `meta.order` (the rare case) — serialised reconcile

Two contributions to the *same* `sec/{sid}` (or two `meta.order` mutations) genuinely conflict on one
key — LWW would drop one (I2). These do **not** write the live key directly. They append a
**proposal**:

```
wiki/{group}/proposal/{proposal-id}   → Proposal { target: SectionRef|Meta, base_hlc, edit, author, mint_hlc }
```

Proposals are **evaporating soft-state** (short `refresh_interval` — a crashed author's proposal ages
out; I3). The **curator** (§3) drains proposals for a given target, reconciles them into the live key,
and the reconcile is the *only* writer of that key while the curator is live. Because there is **one
writer of record per section at a time**, the same-section case degrades to a single-writer sequence —
no concurrent LWW conflict on the live key, I2 preserved (every proposal is either merged or explicitly
superseded, never silently dropped), I1 preserved (the live key is written by one node in HLC order).

`base_hlc` is the HLC the author last read for that section — it lets the curator detect "this
proposal was written against a stale version" and hand the LLM both the base and the current text to
do a **3-way** reconcile (base + current + proposed), the same shape as a git 3-way merge, rather than
a blind overwrite.

### 2.3 Why free-for-all LLM merge is rejected (the declined alternative)

The tempting shortcut — every agent's LLM reconciles concurrent edits inline, no curator — **violates
I1**. LLM reconciliation is non-deterministic: two replicas that both observe proposals {P, Q} and
reconcile locally produce *different* prose, and since each writes its own result to `sec/{sid}`, the
key now has two different values racing on LWW — they never converge to the *same* text, only to
"whichever HLC happened to be larger," which is not a reconciliation, it is a coin-flip that also
threw away content. Determinising it (pinned model + pinned prompt + canonical proposal ordering) is
possible in principle but (a) fragile across model/version drift, (b) still forces every replica to
run the LLM on every conflict (cost O(replicas × conflicts) vs the curator's O(conflicts)), and (c)
turns a model upgrade into a convergence-breaking event. **Declined-with-evidence.** The curator makes
the LLM step *serialised at one node*, which is exactly what buys back I1: one writer ⇒ one value ⇒
convergence, regardless of the LLM's non-determinism.

This is the direct analogue of the substrate's own posture: the KV store never lets two winning
writers interleave a derived effect (the M2 Run-18 stripe-lock finding —
[`../wiki/dev/concurrency/lock-free-and-atomics.md`](../wiki/dev/concurrency/lock-free-and-atomics.md));
here the "derived effect" is an LLM reconcile and the "stripe lock" is the single-curator role.

---

## 3. The curator role state machine

Exactly one **live** curator serves a group's reconcile + lint; `WikiRole::Auto` nodes elect and fail
over. The role is *recallable* (I3): losing it pauses reconciliation, nothing else.

### 3.1 States

```
        ┌─────────┐  advertise wiki.{group}.candidate
        │  Reader │──────────────┐
        └─────────┘              ▼
                          ┌────────────┐  lowest live candidate-id?
                          │ Candidate  │───────────── no ──┐
                          └─────┬──────┘                   │
                        yes │   ▲  higher candidate appears │ (stay)
                            ▼   │ & I'm not lowest          │
                       ┌─────────────┐                      │
                  ┌───▶│   Curator   │◀─────────────────────┘
                  │    └─────┬───────┘
     reconcile /  │          │  own capability evaporates (crash) OR
     lint tick    │          │  a strictly-lower candidate-id appears (yield)
     (self-loop)  └──────────┤
                             ▼
                       ┌────────────┐
                       │  Stepping  │  drain in-flight reconcile, retract wiki.{group}.curator
                       │   Down     │
                       └─────┬──────┘
                             ▼  back to Candidate/Reader
```

- **Reader** — read-only; never advertises curator candidacy. Most agents.
- **Candidate** — advertises `wiki.{group}.candidate` (evaporating capability). Watches the candidate
  set; if it holds the **lowest candidate-id** among live candidates, transitions to Curator.
  (Lowest-id tie-break = the tuple-space/blackboard `Auto` election, reused verbatim — no new
  mechanism, and it is the coordinator-free election the ring already does.)
- **Curator** — advertises `wiki.{group}.curator`; runs the reconcile loop (§3.2) and the lint loop
  (§3.3). Yields if a strictly-lower candidate-id appears (deterministic, converges to a single
  curator without flapping because the order is total).
- **Stepping Down** — finishes any in-flight reconcile (a reconcile is a single LWW write, so
  "in-flight" is sub-millisecond), retracts the curator capability, returns to Candidate/Reader.

### 3.2 The reconcile loop (idempotent, at-least-once)

```
loop while Curator:
  proposals = scan(wiki/{group}/proposal/*)            # evaporating; crashed authors' age out
  group proposals by target (section-id or meta)
  for each target with ≥1 proposal:
     base    = proposal.base_hlc's text (or current if absent)
     current = live sec/{sid} (or meta)
     merged  = if single proposal and base==current:  apply directly (no LLM)
               else:                                   LLM 3-way reconcile(base, current, [edits])
     write  sec/{sid} = merged        # ONE LWW write, HLC-stamped, keyed by section id
     tombstone each consumed proposal # idempotent: a re-drained proposal re-reconciles to the same text
  sleep(reconcile_interval)
```

**Idempotence / at-least-once (the exactly-once-effect contract, third instance —
[`exactly-once-effect.md`](exactly-once-effect.md)):** a curator that crashes mid-drain is re-elected
(or a peer promotes); the new curator re-scans the *still-present* proposals (they were tombstoned
only after the write, and a proposal reconciled-but-not-yet-tombstoned simply reconciles again to the
same section text — LWW-idempotent because the merged content is a deterministic function of
(base, current, edits) *for the no-LLM fast path*, and for the LLM path a second reconcile of the same
inputs produces *a* valid merge that overwrites the first; either way the key ends single-valued and
convergent because there is still only one writer). Delivery is at-least-once; the *effect* (the
section reaches a merged state incorporating every proposal) is once, because proposals are only
tombstoned after they are incorporated.

Note the LLM path is **not** claimed to be deterministic — it does not need to be. The convergence
guarantee (I1) comes from *single-writer*, not from merge determinism: whatever text the one live
curator writes is what every replica converges to. This is the precise reason the curator exists.

### 3.3 The lint loop (the group-function generalisation of `/wiki-lint`)

On a slower tick the curator runs the fleet analog of the project's `/wiki-lint`: dead cross-links
(a `[[...]]` to a non-existent page/section), orphan sections (in a `sec/` key but absent from any
`meta.order`), staleness (a page whose cited external fact — a config key, a capability name — the
curator can probe and no longer finds), and coverage gaps. Findings are written durably to
`wiki/{group}/.lint/{hlc}` (the group analog of `docs/wiki/**/.log/`), where any agent can read them
and file a corrective proposal. Lint is detection-not-prevention (it never blocks a write), matching
the substrate's posture everywhere else.

### 3.4 Failover has no heartbeat / WAL cursor

Unlike the tuple space (whose secondary needs a replicated log + heartbeat), the curator holds **no
authoritative state** — pages and proposals are durable KV; the curator's "state" is entirely
*derivable* by scanning them. So a promoted curator just starts its reconcile loop; there is nothing
to replay or hand off. This is the blackboard's `Post`/`Ack` simplification (snapshot-derivable
mirror, no heartbeat) applied one level up — recorded here as the reason `mycelium-wiki` will *not*
copy the tuple space's WAL-cursor machinery.

---

## 4. Identity & access — competence is a capability, knowledge is not

Who may read and edit a group wiki, and how that ties to Mycelium's discovery model. The
load-bearing distinction (normative — a build that blurs it is wrong): **an agent's
*competence* and *role* are Capabilities; the *knowledge content* is not — it is the
group-scoped Layer-I state this whole record is about.** The native atoms
([`../guide/00-concepts.md`](../guide/00-concepts.md)): a **Capability** is a declarative
advertisement ("this node provides `ns/name`" — the discovery atom, *found not called*); a
**Skill** is a Capability plus an executable handler.

### 4.1 The mapping

| Concept | Layer | Role here | Prefix |
|---|---|---|---|
| **Group** | II (scope) | the knowledge community + admission boundary; self-elected by a `CapabilityGroupDef` filter, no coordinator | `gcap/{group}/…` |
| **Wiki / domain** | I (state) | the group's durable knowledge — owned by the *group*, not any node | `wiki/{group}/…` |
| **Capability — competence** | discovery | "I qualify for / am competent in this domain"; the filter that auto-joins the group | `cap/{node}/…` |
| **Capability — role** | discovery | curator / candidate (the §3 election); reused `tuple.{ns}.primary` shape | `cap/{node}/wiki.{group}.curator\|candidate` |
| **Skill** | invocation | the invocable handler that reads the group wiki (+ blackboard) and calls the LLM | backed by a `cap/` |
| **Knowledge content** | I (state) | **not a Capability** — the prose; accessed by group membership | inside `wiki/{group}/…` |

### 4.2 The composition — how an agent reaches a domain's knowledge

1. The agent advertises a **competence capability** (`cap/{node}/domain/{d}`).
2. It matches the `CapabilityGroupDef` filter for group `{d}` → the agent **self-joins**
   (no coordinator; core design rule 4).
3. Group membership is what grants access: `Boundary::admits` now passes group-scoped
   signals **and** reads of `wiki/{group}/*`. **Access to a specific wiki/domain = group
   membership**, and membership is *earned by advertising the qualifying capability.*
4. The agent's **skill** (a `cap/`-backed handler) runs by reading the group wiki
   (long-term memory) + the blackboard (working memory) + the LLM.
5. The **curator/candidate role** (§3) is *also* a capability — because roles are
   discoverable and electable — but it advertises a *role*, not knowledge.

At no point does wiki *content* enter the `cap/` namespace. Contributing to the wiki
(§2 propose/reconcile) is a distinct act from advertising a capability.

### 4.3 Access control layers (only when the knowledge is sensitive)

Group membership is the default gate; it composes with the existing authz surface for
classified knowledge, both *refining* the capability→group→boundary chain, never replacing
it:

- **`authorized_callers`** (WS-D) restricts *who* may invoke a domain skill — enforced
  where the skill is served (`caller_authorized`), the one place it is genuinely
  enforceable.
- **RBAC clearance** (WS1, data-classification-aware L1/L2/L3) can gate an individual page:
  an L3-classified section is *served* only to a caller whose *verified* signed role claim
  carries L3 clearance. A group wiki is a natural home for per-page classification (the
  "different clearance for different layers of the twin" framing). Worked example + the
  crucial served-path-vs-confidentiality distinction: §4.3.1.

#### 4.3.1 Worked example — a classified section, and what the gate does *not* do

`classification: u8` (0/1/2/3) is a field on `SectionBody` (page-level default in `PageMeta`,
per-section override). The example: the `incident-response` group's postmortem page has a
`root-cause` section classified **L3** because it names the exact SPOF chain (crown-jewel
topology); the rest of the page is L1.

```text
# The curator (or an authorised contributor) classifies the section:
wiki.set_classification("postmortems/payment-outage#root-cause", 3)   # writes SectionBody.classification

# Agent B — a triage bot — holds a signed L2 claim; Agent C — incident commander — L3.
# (advertise_roles writes an Ed25519-SIGNED RoleClaim to sys/role/{node}; needs tls identity.)
agent_b.advertise_roles(["responder"], 2)?    # clearance = 2
agent_c.advertise_roles(["commander"], 3)?    # clearance = 3

# B reads the page over the served path (GET /gateway/wiki/read, or the wiki read RPC).
# The serving node, per L3 section, runs the UNFORGEABLE check:
#     claim = agent.roles_of(&caller_b)         // Some(_) ONLY if B's Ed25519 sig verifies
#                                               // against B's cluster-learned identity key
#     admit = claim.map_or(false, |c| c.clearance_at_least(3))
#   → admit == false  ⇒ the root-cause section is REDACTED (placeholder: "L3 — clearance
#                        required"); B still gets the L1 sections. The denial is AUDITED (WS2).

# C reads the same page:  C's verified claim clearance_at_least(3) == true  ⇒ section included.
```

**What this gate guarantees — and, load-bearingly, what it does *not*.** This is a
**served-path gate** — detection-not-prevention, exactly like the rest of the RBAC surface
(§ the security posture): it is enforced where the section is *served* (the gateway read
endpoint and the wiki read RPC), using `roles_of`, which is unforgeable because it verifies
the Ed25519 signature against the caller's *cluster-learned* identity key, never the raw KV
bytes. A forged `sys/role/` write reads back as `None`, so a self-upgraded clearance fails.

But Mycelium is a **gossip** substrate: once B joined the group, `Boundary::admits` already
delivered the whole `wiki/{group}/` namespace — **including the L3 section's bytes** — to B's
local store. The clearance gate withholds the section from the *convenient* path and audits
the attempt; it does **not** stop a group member who bypasses the read path and inspects raw
gossiped KV. **Per-page clearance gates *access*, not *replication*.**

So pick the tool by what you actually need:

| Need | Mechanism | What it costs |
|---|---|---|
| **Governance** — "group members shouldn't *casually* read L3, and it's audited if they try" | this served-path clearance gate (§4.3.1) | nothing extra; rides WS1 + WS2 |
| **Confidentiality** — "non-cleared members must never *hold* the bytes" | a **sub-group boundary**: L3 pages live in group `{group}.l3` whose `CapabilityGroupDef` filter only cleared members satisfy → the bytes never gossip to non-cleared nodes (classification becomes a more-exclusive *group*, not a per-page ACL — the clean coordinator-free option) | a second group + cleared members re-join it |
| **Confidentiality at rest, in-group** | **WS3 per-page/section encryption** — store ciphertext; only cleared members hold the key | key custody + a decrypt step on read |

Conflating governance and confidentiality is the trap: a gossip substrate replicates to the
whole group by construction, so a per-key ACL *within* a group is a governance gate, not an
isolation boundary. The wiki's own §4 mapping still holds — **access is group membership**;
clearance *refines* the convenient path, and true isolation is a *tighter group* (or
encryption), never a promise that a per-page label alone hides bytes from a member who
already has them.

### 4.4 Federation boundary

At the edge, **AgentFacts publishes the agent's capabilities** (its competence) as the
outward contract; the **wiki content stays internal to the group**. A federation partner
discovers "this cluster has domain-D competence," never domain D's pages — the boundary
primitive one level up (advertise *what you can do*, never *what you know*), the MCB/exit
invariant of [`../wiki/domain/theory/coordinator-free-recursion.md`](../wiki/domain/theory/coordinator-free-recursion.md).

### 4.5 The anti-pattern (normative)

**Never advertise knowledge *content* as capabilities.** Capabilities are for "I can" /
"I may access" (competence, role, qualification); the wiki is for "here is what we know"
(state). A capability minted per fact collapses the discovery layer into the storage layer
and explodes the `cap/` namespace. Keep them on opposite layers — it is what makes
discovery scalable (a bounded, filterable competence set) and access governable (one
boundary + optional clearance, not N per-fact ACLs).

## 5. What this buys, restated against the invariants

| Case | Mechanism | I1 (converge) | I2 (no lost edit) | I3 (curator-optional) |
|---|---|---|---|---|
| Different sections | independent LWW keys | ✓ per-key | ✓ different keys | ✓ direct write, no curator |
| New section | new `sec/` key + `meta` insert | ✓ | ✓ | ✓ direct write |
| Same section | proposal queue → serialised curator reconcile | ✓ single writer | ✓ every proposal merged or superseded | ⏸ reconcile pauses, reads + proposals persist |
| `meta.order` conflict | proposal → curator reconcile of the order vector | ✓ single writer | ✓ | ⏸ |
| Curator crash | re-election + re-drain of live proposals | ✓ | ✓ at-least-once idempotent | ✓ self-heals |

The design's one sentence: **section granularity turns the common case into a lock-free CRDT that
needs no curator, and the curator turns the rare same-section case into a single-writer sequence —
so the LLM's non-determinism is quarantined at one serialised node and never threatens convergence.**

## 6. API reference sketch — the curator surface as it would appear in `lib.rs`

Pre-build draft of the curator-role surface, in the doc-commented idiom the `mycelium-tuple-space`
(`TupleRole`) and `mycelium-blackboard` (`BoardRole`) crates use in their `lib.rs`. Signatures may
shift in the build; this pins the *shape* — the configured-role vs observable-state split (§3), the
**full `WikiConfig`** (curator-loop + data-plane knobs, with the deliberate non-knobs noted), the
curator loop entry points, and the read-only accessors that make the role legible. The
`read`/`propose`/`reconcile` *data methods* are gestured, not drafted — but their configuration is
complete.

```rust
//! Role model (see the module-doc table, tuple/blackboard style):
//!
//! | [`WikiRole::Auto`]        | Advertise as candidate, observe the ring, self-elect Curator (lowest candidate id) or fall back to Contributor — no coordinator |
//! | [`WikiRole::Curator`]     | Serve reconcile + lint immediately (skip the election; single-curator deployments / tests) |
//! | [`WikiRole::Contributor`] | Propose edits + read; never reconciles |
//! | [`WikiRole::Reader`]      | Read-only; never advertises candidacy |

use std::sync::Arc;
use std::time::Duration;
use bytes::Bytes;
use mycelium::GossipAgent;

/// The role this node is *configured* to play for one group wiki — the operator/agent's
/// **intent**, distinct from the observable [`CuratorState`] the node currently occupies
/// (an `Auto` node moves Reader→Candidate→Curator over time; see §3). Mirrors
/// [`TupleRole`]/[`BoardRole`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WikiRole {
    /// Advertise `wiki.{group}.candidate`, observe the candidate ring, and self-elect the
    /// single Curator by lowest candidate id — otherwise serve as a Contributor. The
    /// coordinator-free default; the ring is the failure detector (§3.1, §3.4).
    Auto,
    /// Serve the reconcile + lint loops immediately, skipping the election. For
    /// single-curator deployments and tests; **do not** run two of these for one group
    /// (two writers-of-record reintroduce the divergence §2.3 rules out).
    Curator,
    /// Propose edits (§2.2) and read; never reconciles. The common agent role.
    Contributor,
    /// Read-only. Never advertises candidacy, never proposes.
    Reader,
}

/// The **observable** state of an `Auto`/`Curator` node's curator machine (§3.1) — exposed
/// read-only for legibility (a fleet operator can see who currently holds curation). Do not
/// confuse with [`WikiRole`]: the role is the intent, this is where the machine sits now.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CuratorState {
    /// Not seeking curation (a `Reader`/`Contributor`, or an `Auto` node that has not yet
    /// advertised candidacy).
    Idle,
    /// Advertising `wiki.{group}.candidate`; waiting to learn whether it holds the lowest
    /// live candidate id.
    Candidate,
    /// The live curator: advertising `wiki.{group}.curator`, running the reconcile (§3.2)
    /// and lint (§3.3) loops. Exactly one per group at convergence.
    Curator,
    /// Finishing an in-flight reconcile, retracting the curator capability, returning to
    /// `Candidate`/`Idle` (yielded to a strictly-lower candidate id, or shutting down).
    SteppingDown,
}

/// Full wiki configuration — grouped by concern. The curator loop reads the first two groups;
/// the data-plane groups govern read/propose/section lifecycle and access.
#[derive(Clone, Debug)]
pub struct WikiConfig {
    // ── identity & role ─────────────────────────────────────────────────────────────────
    /// Group namespace, e.g. `"tls-ops"`. Must not contain `/` (capability key segments
    /// reject it, like the tuple space). Advertised as `wiki.{ns}.curator|candidate`;
    /// pages live under `wiki/{ns}/…` (§1.1).
    pub namespace: Arc<str>,
    /// This node's configured role.
    pub role: WikiRole,

    // ── curator loop (read by the curator; §3) ──────────────────────────────────────────
    /// How often the live curator drains the proposal queue and reconciles (§3.2). The
    /// same-section edit-to-visible latency floor.
    pub reconcile_interval: Duration,
    /// How often the live curator runs the lint pass (§3.3) — dead links, orphan sections,
    /// stale cited facts. Typically ≫ `reconcile_interval`.
    pub lint_interval: Duration,
    /// Capability advertisement refresh for the candidate/curator ads. Readers evaporate at
    /// 3×, so **curator failover latency after a crash ≈ 3 × `cap_refresh`** (§3.4).
    pub cap_refresh: Duration,

    // ── data plane: sections & proposals ────────────────────────────────────────────────
    /// Max bytes for one **section** body. A section is a paragraph-scale merge unit (§1.1),
    /// so this is set *far below* the substrate's `MAX_KV_WRITE_BYTES` frame limit on purpose:
    /// a huge section defeats section-granular convergence (every edit to it drags through the
    /// curator, §2.2) and coarsens the merge grain. A `propose`/write over this is rejected
    /// (`SectionTooLarge` — split the section). Default 64 KiB.
    pub max_section_bytes: usize,
    /// Proposal evaporation window (§2.2). A proposal not drained by the curator ages out at
    /// the 3× read-side factor — the crashed-author path — bounding queue growth with no
    /// coordinator. Set a few × `reconcile_interval` so a live curator always drains before
    /// evaporation. Default `5 × reconcile_interval`.
    pub proposal_ttl: Duration,
    /// When `true` (default), a **new** section is written directly (it cannot lose-update,
    /// §2.1) and only an edit to an **existing** section queues as a proposal. When `false`,
    /// *every* edit routes through the curator queue — no direct writes at all: safer where
    /// near-all edits collide, at the cost of one `reconcile_interval` of latency on every
    /// change (and unavailability of writes while there is no live curator, I3).
    pub direct_new_sections: bool,

    // ── data plane: lint scope ──────────────────────────────────────────────────────────
    /// Whether the curator's lint (§3.3) probes **external** cited facts — a config key, a
    /// capability name, an endpoint a page cites — the doc-vs-code check generalised. Costs
    /// I/O per probe and is the only lint check with side effects; the intra-wiki checks
    /// (dead cross-links, orphan sections) are always on. Default `true`.
    pub lint_probes_cited_facts: bool,

    // ── access (compliance feature; §4.3) ───────────────────────────────────────────────
    /// Default read-clearance for pages in this wiki (WS1 data-classification). `None` =
    /// group membership is the only gate. A page may **raise** its own classification above
    /// this floor; it never lowers below it. Compiles away without `compliance`.
    #[cfg(feature = "compliance")]
    pub default_read_clearance: Option<Clearance>,
}

// Deliberate NON-knobs (divergences from TupleConfig / BoardConfig — recorded so a builder
// doesn't add them by pattern-matching the siblings):
//
// • No `persist` / `wal_path`. Wiki *content* is ordinary durable KV — the substrate already
//   persists it (WAL + snapshot) and heals it (Merkle anti-entropy); there is no crate-owned
//   log. Proposals are *intentionally* ephemeral (evaporating soft-state), so they are the one
//   thing NOT persisted. This is the same "no crate WAL" stance §3.4 takes for curator failover.
// • No page-assembly cache knob. Pages assemble from their sections at read time (§1.1); a
//   cached assembled `page/{path}/rendered` is deferred (§7 open question) — it would be a
//   curator-written derived effect, so it is not a v1 config surface.
// • No reconcile-batch knob. Whether the curator batches multiple same-section proposals per
//   LLM call (§7) is an open build decision, not a pinned default.
// • No backpressure knob. The optional `sys/wiki/{node}/{group}/…` pressure pheromone (§7) is
//   deferred; `proposal_ttl` is the v1 bound on queue growth.
// • No LLM/model field. The reconcile uses the *agent's* configured `LlmBackend` (the `llm`
//   feature); a per-wiki model override is deferred until a real need appears.

/// Errors from the curator surface. (Shares the crate `WikiError`; curator-relevant variants.)
#[derive(Debug)]
#[non_exhaustive]
pub enum WikiError {
    /// No node currently serves as curator for this group (none started, or the curator's
    /// ad evaporated and no candidate has promoted yet). Reads still succeed; only
    /// reconcile is paused (invariant **I3**).
    NoCurator,
    /// The `llm` feature is off (or no backend configured) and a same-section reconcile that
    /// needs a 3-way merge was required. Fast-path (single proposal, base==current) still
    /// applies; only genuine conflicts surface this. See the no-LLM fallback, §3.2.
    NoReconciler,
    /// A section body (or a proposed edit) exceeds `WikiConfig::max_section_bytes`. Split the
    /// section — see the data-plane knobs. Rejected before any write (a section is a
    /// paragraph-scale merge unit, not a document).
    SectionTooLarge { size: usize, limit: usize },
    /// Transport error talking to the live curator.
    Transport(String),
}

/// An agent-backed group wiki: durable pages over the KV substrate, with a coordinator-free
/// curator discovered on the capability ring and emergent failover. Construct after
/// `agent.start()`. (Read/propose/reconcile data methods elided — this sketch is the curator
/// surface.)
pub struct Wiki {
    // agent, cfg, curator state cell, loop handles …
}

impl Wiki {
    /// Construct the wiki and start whatever machinery the configured [`WikiRole`] needs:
    /// `Auto` begins candidacy; `Curator` starts the loops immediately; `Contributor`/`Reader`
    /// start none. Idempotent per (agent, namespace).
    pub async fn new(agent: Arc<GossipAgent>, cfg: WikiConfig) -> Result<Arc<Self>, WikiError> {
        unimplemented!("build")
    }

    /// This wiki's group namespace.
    pub fn namespace(&self) -> &Arc<str> { unimplemented!("build") }

    /// The **configured** role (the intent). Immutable for the handle's life.
    pub fn role(&self) -> WikiRole { unimplemented!("build") }

    /// The **current observable** curator state (§3.1) — read-only, for legibility. Changes
    /// over time for an `Auto` node as the ring converges.
    pub fn curator_state(&self) -> CuratorState { unimplemented!("build") }

    /// True iff this node is the live curator right now (`curator_state() == Curator`).
    pub fn is_curator(&self) -> bool { matches!(self.curator_state(), CuratorState::Curator) }

    /// The NodeId of the group's current live curator as seen from this node's KV view
    /// (`wiki.{ns}.curator` advertiser), or `None` if none is live (**I3** — reads still work).
    /// The fleet-legibility accessor: "who is curating this domain?"
    pub fn current_curator(&self) -> Option<mycelium::NodeId> { unimplemented!("build") }

    // ── Curator machinery (private; spawned by `new` for Auto/Curator roles) ─────────────
    //
    // async fn run_election(self: Arc<Self>)   // Reader→Candidate→Curator, lowest-id, yields on lower id (§3.1)
    // async fn run_reconcile(self: Arc<Self>)  // drain proposals → per-target 3-way merge → one LWW write (§3.2)
    // async fn run_lint(self: Arc<Self>)       // dead links / orphans / stale cited facts → wiki/{ns}/.lint/ (§3.3)
    //
    // Invariants these must uphold (§0): I1 convergence (single writer-of-record per section),
    // I2 no-lost-edit (every proposal merged or explicitly superseded, never dropped),
    // I3 curator-optional (losing the role pauses only reconcile; reads + proposals persist).

    /// Graceful stop: if curator, transition `SteppingDown` (finish any in-flight reconcile —
    /// sub-ms, one LWW write), retract the curator/candidate capability, then stop the loops.
    /// Pending proposals persist as evaporating soft-state; a peer promotes (I3).
    pub async fn shutdown(&self) { unimplemented!("build") }
}
```

**Why the role/state split is a public distinction, not an internal one:** the *intent*
([`WikiRole`]) is what an operator/agent sets and what governs failover eligibility; the *state*
([`CuratorState`] / `current_curator()`) is what a fleet observer reads to answer "who is curating
`tls-ops` right now, and is anyone?" — the emergent-legibility question (adjacent to
[`../plans/legible-emergence.md`](../plans/legible-emergence.md)). Exposing both, read-only, is the
same posture as the tuple space's `TupleRole` + ring observability; a curator is a *recallable
participant*, so its identity is legible but never authoritative state (§3.4).

## 7. Open questions for the build plan (not decided here)

- **Section-split/merge edits** (an author wants to split one section into two, or fuse two): these
  touch multiple `sec/` keys + `meta.order` atomically-ish. Proposed as a single multi-key proposal
  the curator applies in one reconcile pass; the *degenerate* interleaving with a concurrent
  same-target edit needs a worked example before v1.
- **Proposal starvation / ordering fairness** when one hot section accumulates proposals faster than
  the curator drains — bounded by proposal evaporation, but a backpressure pheromone
  (`sys/wiki/{node}/{group}/…`, mirroring the tuple space's) may be warranted.
- **LLM cost governance** — the reconcile is the only LLM call; rate/lint intervals are governor
  knobs (management-as-intent). Whether reconcile should batch multiple same-section proposals per
  LLM call (cheaper, but larger blast radius on a bad merge) is a tuning decision for the build.
- **Read-time vs write-time page assembly** — this record assembles a page from its sections at
  *read* time (cheap keys, assembly on demand). A cached assembled `page/{path}/rendered` is a
  possible optimisation, explicitly deferred (it reintroduces a derived-effect-to-serialise, so it
  would be curator-written if added).
