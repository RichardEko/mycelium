//! `FsStore` contract tests — round-trip, manifest-authoritative reads (torn-write safety), edits,
//! attribute query, and the path-traversal guard. All on a tempdir; no cluster.

use std::collections::BTreeMap;
use std::sync::Arc;

use super::FsStore;
use crate::model::{mint_section_id, Predicate, Section, SectionId, WikiError};
use crate::store::WikiStore;

fn store() -> (tempfile::TempDir, FsStore) {
    let dir = tempfile::tempdir().unwrap();
    let s = FsStore::open(dir.path(), "ops").unwrap();
    (dir, s)
}

fn attrs(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
    pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
}

fn sec(id: &str, heading: &str, body: &str, a: &[(&str, &str)]) -> Section {
    Section { id: Arc::from(id), heading: heading.into(), body: body.into(), attributes: attrs(a) }
}

#[test]
fn write_then_read_round_trips_in_order_with_attributes() {
    let (_d, s) = store();
    let sa = sec("s-a", "Symptoms", "gateway 503s", &[("node", "e_rl_rk")]);
    let sb = sec("s-b", "Resolution", "rolled cert", &[("node", "e_rl_rk"), ("topic", "resolution")]);
    s.write_page("incidents/cert-rotation", &[sa.clone(), sb.clone()], &attrs(&[("domain", "retail-lending")])).unwrap();

    let page = s.read("incidents/cert-rotation").unwrap().unwrap();
    assert_eq!(page.path, "incidents/cert-rotation");
    assert_eq!(page.attributes.get("domain").map(String::as_str), Some("retail-lending"));
    assert_eq!(page.sections, vec![sa, sb], "sections round-trip in manifest order");
    assert_eq!(s.read("nope").unwrap(), None, "absent page reads as None");
}

#[test]
fn read_is_manifest_authoritative_a_stray_section_is_invisible() {
    // Torn-write safety proxy: a section object present on disk but NOT in the manifest is never
    // returned — which is exactly why a reader entering via the manifest can't see a half-applied edit.
    let (_d, s) = store();
    let a = sec("s-a", "H", "b", &[]);
    s.write_page("p", std::slice::from_ref(&a), &BTreeMap::new()).unwrap();
    // Write a section object that no manifest references (an in-flight orphan — a section written
    // before its membership add commits).
    s.write_section("p", &sec("s-stray", "X", "y", &[]), None).unwrap();

    let page = s.read("p").unwrap().unwrap();
    assert_eq!(page.sections, vec![a], "only the manifest-referenced section is visible");
}

#[test]
fn editing_a_page_drops_removed_sections_from_the_read() {
    let (_d, s) = store();
    let a = sec("s-a", "A", "1", &[]);
    let b = sec("s-b", "B", "2", &[]);
    s.write_page("p", &[a.clone(), b.clone()], &BTreeMap::new()).unwrap();
    // Re-write the page WITHOUT section b (an edit that removed it).
    s.write_page("p", std::slice::from_ref(&a), &BTreeMap::new()).unwrap();
    let page = s.read("p").unwrap().unwrap();
    assert_eq!(page.sections, vec![a], "the dropped section is gone from the manifest → invisible");
}

#[test]
fn query_filters_sections_by_attribute_across_pages() {
    let (_d, s) = store();
    s.write_page("retail-lending/deps",
        &[sec("s1", "feature-data", "Central Data dependency", &[("node", "e_rl_rk"), ("topic", "coupling")])],
        &BTreeMap::new()).unwrap();
    s.write_page("retail-lending/risk",
        &[sec("s2", "sign-off gate", "Risk Lead authorises", &[("node", "risk"), ("topic", "governance")])],
        &BTreeMap::new()).unwrap();
    s.write_page("platform/compute",
        &[sec("s3", "compute", "shared platform", &[("node", "platform"), ("topic", "coupling")])],
        &BTreeMap::new()).unwrap();

    // By shared id (join key to the metrics store).
    let by_node = s.query(&Predicate::new().with("node", "e_rl_rk")).unwrap();
    assert_eq!(by_node.len(), 1);
    assert_eq!(&*by_node[0].id, "s1");
    assert_eq!(by_node[0].page, "retail-lending/deps");

    // By cross-cutting tag, across pages.
    let mut by_topic: Vec<String> = s.query(&Predicate::new().with("topic", "coupling")).unwrap()
        .into_iter().map(|r| r.id.to_string()).collect();
    by_topic.sort();
    assert_eq!(by_topic, ["s1", "s3"], "tag query spans pages");

    // Empty predicate matches all sections.
    assert_eq!(s.query(&Predicate::new()).unwrap().len(), 3);
}

#[test]
fn list_pages_returns_every_page_sorted() {
    let (_d, s) = store();
    s.write_page("b/second", &[sec("s1", "H", "x", &[])], &BTreeMap::new()).unwrap();
    s.write_page("a/first", &[sec("s2", "H", "y", &[])], &BTreeMap::new()).unwrap();
    assert_eq!(s.list_pages().unwrap(), vec!["a/first".to_string(), "b/second".to_string()]);
}

#[test]
fn page_path_traversal_is_rejected() {
    let (_d, s) = store();
    assert!(matches!(s.write_page("../escape", &[], &BTreeMap::new()), Err(WikiError::BadPath(_))));
    assert!(matches!(s.read("a/../../etc"), Err(WikiError::BadPath(_))));
    assert!(matches!(s.read("a//b"), Err(WikiError::BadPath(_))), "empty component rejected");
}

// ── compare-and-swap: the airtight concurrent-writer contract ───────────────────

#[test]
fn concurrent_curators_editing_different_sections_dont_clobber() {
    // The lost-update regression. With the old whole-page write, two curators (a transient
    // split-brain) that both read the same page and then wrote it back clobbered each other even on
    // DIFFERENT sections — the second writer's full-page snapshot reverted the first's section.
    // Section-granular CAS makes each section an independent slot, so this interleaving is lossless.
    let (_d, s) = store();
    s.write_page("p", &[sec("x", "X", "x0", &[]), sec("y", "Y", "y0", &[])], &BTreeMap::new()).unwrap();

    // Both curators read the SAME versioned snapshot (the pre-condition for the old clobber).
    let vp = s.read_versioned("p").unwrap().unwrap();
    let vx = vp.sections.get(&SectionId::from("x")).unwrap().0;
    let vy = vp.sections.get(&SectionId::from("y")).unwrap().0;

    // Curator A edits section x against its read version; curator B edits section y against its.
    s.write_section("p", &sec("x", "X", "x1", &[]), Some(vx)).unwrap();
    s.write_section("p", &sec("y", "Y", "y1", &[]), Some(vy)).unwrap();

    // Both edits survive — neither write clobbered the other.
    let page = s.read("p").unwrap().unwrap();
    let body = |id: &str| page.sections.iter().find(|s| &*s.id == id).map(|s| s.body.clone());
    assert_eq!(body("x").as_deref(), Some("x1"));
    assert_eq!(body("y").as_deref(), Some("y1"), "the different-section edit was NOT lost");
}

#[test]
fn same_section_stale_write_is_rejected_not_silently_lost() {
    // Two curators race the SAME section from the same base version. One commits; the other's write,
    // still carrying the stale base, must Conflict (not silently overwrite) — the signal the curator
    // uses to re-read and re-reconcile so no acknowledged edit is dropped.
    let (_d, s) = store();
    s.write_page("p", &[sec("x", "X", "x0", &[])], &BTreeMap::new()).unwrap();
    let base = s.read_versioned("p").unwrap().unwrap().sections.get(&SectionId::from("x")).unwrap().0;

    s.write_section("p", &sec("x", "X", "first", &[]), Some(base)).unwrap();
    let stale = s.write_section("p", &sec("x", "X", "second", &[]), Some(base));
    assert!(matches!(stale, Err(WikiError::Conflict)), "stale-based write conflicts: {stale:?}");

    // The committed value stands; the stale write did not shadow it.
    assert_eq!(s.read("p").unwrap().unwrap().sections[0].body, "first");
    // …and re-reading gives a fresh version the loser can build on.
    let fresh = s.read_versioned("p").unwrap().unwrap().sections.get(&SectionId::from("x")).unwrap().0;
    assert!(fresh > base, "the committed write advanced the version");
    s.write_section("p", &sec("x", "X", "second-retried", &[]), Some(fresh)).unwrap();
    assert_eq!(s.read("p").unwrap().unwrap().sections[0].body, "second-retried");
}

#[test]
fn creating_a_section_that_already_exists_conflicts() {
    // `expected = None` means "must not exist yet". A create that races a create loses cleanly.
    let (_d, s) = store();
    s.write_page("p", &[sec("x", "X", "x0", &[])], &BTreeMap::new()).unwrap();
    let dup = s.write_section("p", &sec("x", "X", "dupe", &[]), None);
    assert!(matches!(dup, Err(WikiError::Conflict)), "create-over-existing conflicts: {dup:?}");
    assert_eq!(s.read("p").unwrap().unwrap().sections[0].body, "x0", "the existing value is untouched");
}

#[test]
fn manifest_membership_is_compare_and_swap() {
    // Adding a section to the order is a manifest CAS: a stale manifest version is rejected so two
    // concurrent membership adds serialise instead of one dropping the other's section.
    let (_d, s) = store();
    s.write_page("p", &[sec("x", "X", "x0", &[])], &BTreeMap::new()).unwrap();
    let vp = s.read_versioned("p").unwrap().unwrap();
    let mver = vp.manifest_version;
    let mut order = vp.order.clone();

    // Curator A splices in section y and commits the manifest.
    s.write_section("p", &sec("y", "Y", "y0", &[]), None).unwrap();
    let mut order_a = order.clone();
    order_a.push(SectionId::from("y"));
    s.update_manifest("p", &order_a, &vp.attributes, Some(mver)).unwrap();

    // Curator B, still holding the pre-add manifest version, tries to add z — Conflict.
    s.write_section("p", &sec("z", "Z", "z0", &[]), None).unwrap();
    order.push(SectionId::from("z"));
    let stale = s.update_manifest("p", &order, &vp.attributes, Some(mver));
    assert!(matches!(stale, Err(WikiError::Conflict)), "stale manifest add conflicts: {stale:?}");

    // B re-reads and re-adds z on top of A's manifest — now both y and z are members.
    let vp2 = s.read_versioned("p").unwrap().unwrap();
    let mut order2 = vp2.order.clone();
    order2.push(SectionId::from("z"));
    s.update_manifest("p", &order2, &vp2.attributes, Some(vp2.manifest_version)).unwrap();

    let ids: Vec<String> = s.read("p").unwrap().unwrap().sections.iter().map(|s| s.id.to_string()).collect();
    assert_eq!(ids, ["x", "y", "z"], "both concurrent membership adds survived");
}

#[test]
fn a_stale_write_into_a_gc_gap_is_rejected_not_shadowed() {
    // The subtle head-check branch. A slow writer holding a very old version can hard-link a version
    // number that was created then GC'd — a gap *below* the real head. That must Conflict: otherwise
    // its content would sit below the head, silently shadowed (a lost update the naive CAS would miss).
    let (_d, s) = store();
    s.write_page("p", &[sec("x", "X", "v0", &[])], &BTreeMap::new()).unwrap(); // section x at version 1
    let stale = s.read_versioned("p").unwrap().unwrap().sections.get(&SectionId::from("x")).unwrap().0;

    // Advance the section twice; the GC drops versions 1 and 2, leaving only the head.
    let v2 = s.write_section("p", &sec("x", "X", "a", &[]), Some(stale)).unwrap();
    let _head = s.write_section("p", &sec("x", "X", "b", &[]), Some(v2)).unwrap();

    // The slow writer, still on version 1, targets version 2 — now a GC gap below the head.
    let gap = s.write_section("p", &sec("x", "X", "shadow", &[]), Some(stale));
    assert!(matches!(gap, Err(WikiError::Conflict)), "a gap-fill below the head conflicts: {gap:?}");
    assert_eq!(s.read("p").unwrap().unwrap().sections[0].body, "b", "the head is untouched, not shadowed");
}

#[test]
fn concurrent_idempotent_appends_deliver_every_edit_exactly_once() {
    // The airtightness proof, at the real contract. Many threads race the SAME section through the
    // curator's exact discipline: read_versioned → merge → CAS write_section → retry-on-conflict, with
    // an *idempotent* append (skip a line already present — the DirectReconciler rule). The store CAS is
    // **at-least-once, never-lose**: a version can be consumed by a follower and then re-applied on the
    // original writer's retry (see `exactly-once-effect.md` — the same contract the tuple space and
    // blackboard use). Exactly-once *effect* therefore comes from the append being idempotent. Assert
    // both edges: every edit present (**none lost**) and each present once (**the retries deduped**).
    use std::sync::Arc;
    let dir = tempfile::tempdir().unwrap();
    let s = Arc::new(FsStore::open(dir.path(), "ops").unwrap());
    s.write_page("p", &[sec("c", "Log", "", &[])], &BTreeMap::new()).unwrap();

    const THREADS: usize = 8;
    const LINES: usize = 25;
    std::thread::scope(|scope| {
        for t in 0..THREADS {
            let s = Arc::clone(&s);
            scope.spawn(move || {
                for l in 0..LINES {
                    let line = format!("t{t}-l{l}");
                    loop {
                        let vp = s.read_versioned("p").unwrap().unwrap();
                        let (ver, cur) = vp.sections.get(&SectionId::from("c")).cloned().unwrap();
                        if cur.body.lines().any(|x| x == line) { break; } // idempotent: already landed
                        let mut body = cur.body.clone();
                        if !body.is_empty() { body.push('\n'); }
                        body.push_str(&line);
                        match s.write_section("p", &sec("c", "Log", &body, &[]), Some(ver)) {
                            Ok(_)                    => break,      // committed
                            Err(WikiError::Conflict) => continue,   // lost the race — re-read and retry
                            Err(e)                   => panic!("unexpected store error: {e:?}"),
                        }
                    }
                }
            });
        }
    });

    let body = s.read("p").unwrap().unwrap().sections[0].body.clone();
    let mut lines: Vec<&str> = body.lines().collect();
    let total = lines.len();
    lines.sort_unstable();
    lines.dedup();
    assert_eq!(lines.len(), THREADS * LINES, "every edit landed — none lost under real thread contention");
    assert_eq!(total, THREADS * LINES, "no edit duplicated — the idempotent append deduped the at-least-once retries");
}

#[test]
fn many_threads_editing_different_sections_all_survive() {
    // The other side of contention: many writers on the SAME page but DISTINCT sections. With the old
    // whole-page write these clobbered each other; with per-section CAS slots they must not contend at
    // all — every section ends with its writer's value and the page carries all of them.
    use std::sync::Arc;
    let dir = tempfile::tempdir().unwrap();
    let s = Arc::new(FsStore::open(dir.path(), "ops").unwrap());
    const N: usize = 12;
    // Seed the page with N sections s0..sN-1.
    let seeds: Vec<Section> = (0..N).map(|i| sec(&format!("s{i}"), "H", "seed", &[])).collect();
    s.write_page("p", &seeds, &BTreeMap::new()).unwrap();

    std::thread::scope(|scope| {
        for i in 0..N {
            let s = Arc::clone(&s);
            scope.spawn(move || {
                let id = format!("s{i}");
                for r in 0..20 {
                    loop {
                        let vp = s.read_versioned("p").unwrap().unwrap();
                        let ver = vp.sections.get(&SectionId::from(id.as_str())).unwrap().0;
                        let next = sec(&id, "H", &format!("r{r}"), &[]);
                        match s.write_section("p", &next, Some(ver)) {
                            Ok(_)                    => break,
                            Err(WikiError::Conflict) => continue, // only ever conflicts with itself; bounded
                            Err(e)                   => panic!("unexpected store error: {e:?}"),
                        }
                    }
                }
            });
        }
    });

    let page = s.read("p").unwrap().unwrap();
    assert_eq!(page.sections.len(), N, "no section was dropped");
    for i in 0..N {
        let want = format!("s{i}");
        let body = page.sections.iter().find(|s| *s.id == want).map(|s| s.body.as_str());
        assert_eq!(body, Some("r19"), "section s{i} carries its final write, un-clobbered by its neighbours");
    }
}

#[test]
fn section_ids_are_stable_opaque_and_unique() {
    let a = mint_section_id("ops", "p", 42, 7);
    assert_eq!(a, mint_section_id("ops", "p", 42, 7), "same coordinates → same id (stable)");
    assert_ne!(a, mint_section_id("ops", "p", 42, 8), "nonce differs");
    assert_ne!(a, mint_section_id("ops", "q", 42, 7), "page differs");
    assert_ne!(a, mint_section_id("dev", "p", 42, 7), "group differs");
    assert!(a.starts_with('s') && a.len() == 13, "opaque fixed-width id: {a}");
}
