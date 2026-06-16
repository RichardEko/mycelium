//! `GossipAgent`-driven tests for `SchemaHandle` (moved to `mycelium-core` in v2 M3).
//! These exercise the handle through a live agent, so they live in the full crate;
//! the pure path-parsing tests stay in `mycelium-core::schema_handle`.

use crate::{GossipAgent, GossipConfig, NodeId, SchemaError, SchemaPublishResult};
use bytes::Bytes;

fn make_agent() -> GossipAgent {
    let node = NodeId::new("127.0.0.1", 19300).unwrap();
    GossipAgent::new(node, GossipConfig::default())
}

const SCHEMA_V1: &[u8] = br#"{"type":"object","required":["prompt"],"properties":{"prompt":{"type":"string"}}}"#;
const SCHEMA_V1_ALT: &[u8] = br#"{"type":"object","required":["prompt","model"],"properties":{"prompt":{"type":"string"},"model":{"type":"string"}}}"#;

#[tokio::test]
async fn publish_new_schema() {
    let a = make_agent();
    let r = a.schemas().publish_schema("acme/ml/v1", SCHEMA_V1).await.unwrap();
    assert_eq!(r, SchemaPublishResult::Published);
}

#[tokio::test]
async fn publish_identical_returns_unchanged() {
    let a = make_agent();
    a.schemas().publish_schema("acme/ml/v1", SCHEMA_V1).await.unwrap();
    let r = a.schemas().publish_schema("acme/ml/v1", SCHEMA_V1).await.unwrap();
    assert_eq!(r, SchemaPublishResult::Unchanged);
}

#[tokio::test]
async fn publish_conflict_returns_existing() {
    let a = make_agent();
    a.schemas().publish_schema("acme/ml/v1", SCHEMA_V1).await.unwrap();
    let r = a.schemas().publish_schema("acme/ml/v1", SCHEMA_V1_ALT).await.unwrap();
    match r {
        SchemaPublishResult::Conflict { existing } => {
            assert_eq!(existing, Bytes::from_static(SCHEMA_V1));
        }
        other => panic!("expected Conflict, got {other:?}"),
    }
}

#[tokio::test]
async fn force_publish_overwrites_existing() {
    let a = make_agent();
    a.schemas().publish_schema("acme/ml/v1", SCHEMA_V1).await.unwrap();
    a.schemas().force_publish_schema("acme/ml/v1", SCHEMA_V1_ALT).await.unwrap();
    let stored = a.schemas().get_schema("acme/ml/v1").unwrap();
    assert_eq!(stored, Bytes::from_static(SCHEMA_V1_ALT));
}

#[tokio::test]
async fn get_schema_returns_stored() {
    let a = make_agent();
    a.schemas().publish_schema("acme/ml/v1", SCHEMA_V1).await.unwrap();
    let got = a.schemas().get_schema("acme/ml/v1").unwrap();
    assert_eq!(got, Bytes::from_static(SCHEMA_V1));
}

#[tokio::test]
async fn get_schema_missing_returns_none() {
    let a = make_agent();
    assert!(a.schemas().get_schema("does/not/exist").is_none());
}

#[tokio::test]
async fn list_schemas_sorted_prefix_stripped() {
    let a = make_agent();
    a.schemas().publish_schema("z/last", SCHEMA_V1).await.unwrap();
    a.schemas().publish_schema("a/first", SCHEMA_V1).await.unwrap();
    a.schemas().publish_schema("m/middle", SCHEMA_V1).await.unwrap();
    let list = a.schemas().list_schemas();
    assert_eq!(list.len(), 3);
    assert_eq!(list[0].0.as_ref(), "a/first");
    assert_eq!(list[1].0.as_ref(), "m/middle");
    assert_eq!(list[2].0.as_ref(), "z/last");
}

#[tokio::test]
async fn publish_invalid_json_err() {
    let a = make_agent();
    let err = a.schemas().publish_schema("acme/ml/v1", b"not json {{{").await.unwrap_err();
    assert!(matches!(err, SchemaError::InvalidJson(_)));
}

#[tokio::test]
async fn publish_json_array_err() {
    let a = make_agent();
    let err = a.schemas().publish_schema("acme/ml/v1", b"[1,2,3]").await.unwrap_err();
    assert!(matches!(err, SchemaError::NotAnObject { kind: "array" }));
}

#[tokio::test]
async fn publish_json_null_err() {
    let a = make_agent();
    let err = a.schemas().publish_schema("acme/ml/v1", b"null").await.unwrap_err();
    assert!(matches!(err, SchemaError::NotAnObject { kind: "null" }));
}

#[tokio::test]
async fn publish_empty_schema_id_err() {
    let a = make_agent();
    let err = a.schemas().publish_schema("", SCHEMA_V1).await.unwrap_err();
    assert!(matches!(err, SchemaError::InvalidSchemaId { .. }));
}

#[tokio::test]
async fn publish_schema_id_leading_slash_err() {
    let a = make_agent();
    let err = a.schemas().publish_schema("/acme/ml/v1", SCHEMA_V1).await.unwrap_err();
    assert!(matches!(err, SchemaError::InvalidSchemaId { .. }));
}

#[tokio::test]
async fn publish_schema_id_trailing_slash_err() {
    let a = make_agent();
    let err = a.schemas().publish_schema("acme/ml/v1/", SCHEMA_V1).await.unwrap_err();
    assert!(matches!(err, SchemaError::InvalidSchemaId { .. }));
}

#[tokio::test]
async fn publish_schema_id_double_slash_err() {
    let a = make_agent();
    let err = a.schemas().publish_schema("acme//ml", SCHEMA_V1).await.unwrap_err();
    assert!(matches!(err, SchemaError::InvalidSchemaId { .. }));
}

#[tokio::test]
async fn publish_schema_id_dotdot_err() {
    let a = make_agent();
    let err = a.schemas().publish_schema("../../etc/passwd", SCHEMA_V1).await.unwrap_err();
    assert!(matches!(err, SchemaError::InvalidSchemaId { .. }));
}

#[tokio::test]
async fn publish_schema_id_space_err() {
    let a = make_agent();
    let err = a.schemas().publish_schema("acme ml v1", SCHEMA_V1).await.unwrap_err();
    assert!(matches!(err, SchemaError::InvalidSchemaId { .. }));
}

#[tokio::test]
async fn publish_schema_id_valid_chars() {
    let a = make_agent();
    let r = a.schemas().publish_schema("Org_Name/my-service.v2/v1", SCHEMA_V1).await.unwrap();
    assert_eq!(r, SchemaPublishResult::Published);
}
