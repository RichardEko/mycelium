//! `FsStore` contract tests — round-trip, manifest-authoritative reads (torn-write safety), edits,
//! attribute query, and the path-traversal guard. All on a tempdir; no cluster.

use std::collections::BTreeMap;
use std::sync::Arc;

use super::FsStore;
use crate::model::{mint_section_id, Predicate, Section, WikiError};
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
    // Drop a stray section file that the manifest does not reference.
    let stray = sec("s-stray", "X", "y", &[]);
    s.write_atomic(&s.page_dir("p").unwrap().join("sec").join("s-stray.json"), &stray).unwrap();

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

#[test]
fn section_ids_are_stable_opaque_and_unique() {
    let a = mint_section_id("ops", "p", 42, 7);
    assert_eq!(a, mint_section_id("ops", "p", 42, 7), "same coordinates → same id (stable)");
    assert_ne!(a, mint_section_id("ops", "p", 42, 8), "nonce differs");
    assert_ne!(a, mint_section_id("ops", "q", 42, 7), "page differs");
    assert_ne!(a, mint_section_id("dev", "p", 42, 7), "group differs");
    assert!(a.starts_with('s') && a.len() == 13, "opaque fixed-width id: {a}");
}
