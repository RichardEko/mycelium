//! Schema evolution (WS-F / [Schema-Evo]) — the runtime-migration machinery for the schema
//! registry, built tier by tier per the ROADMAP §*Schema-registry evolution*.
//!
//! The governing rule is **explicit, registered migrations — never silent best-effort coercion.**
//! Silent coercion would mask real incompatibilities and break the explicit-contract /
//! detection-not-prevention posture. When no migration path exists, **detect**, do not guess.
//!
//! Tiers (delivered incrementally — see `docs/plans/v2-wsf-schema-evolution.md`):
//! - **E1 · additive tolerance** — *largely already true* on the JSON payload paths via serde
//!   ignore-unknown + `#[serde(default)]`. This module documents and **verifies** that property
//!   (the tests below); it is not new mechanism.
//! - **E2 · compatibility detection** — a `schema_mismatch` tripwire (counter + `warn!`).
//! - **E3 · registered migrations** — declarative `vN → vN+1` transforms, gossip-distributed and
//!   composed on the receive path.

#[cfg(test)]
mod additive_tolerance_tests {
    //! E1 / Gate G-E1: verify that the JSON payload paths are **additively tolerant** — a consumer
    //! compiled against an older schema still parses a newer producer's payload (unknown fields
    //! ignored), and a newer consumer still parses an older payload (missing fields defaulted).
    //!
    //! This is a property of serde over JSON (ignore-unknown by default + `#[serde(default)]`), not
    //! Mycelium mechanism — Layer-I capability/filter types already rely on it (`#[serde(default)]`
    //! throughout `capability.rs` for forward-compatible decoding). These tests pin the contract so
    //! a future `#[serde(deny_unknown_fields)]` on a payload path is caught.

    use serde::{Deserialize, Serialize};

    /// A consumer compiled against schema **v1** of a payload.
    #[derive(Debug, PartialEq, Eq, Deserialize)]
    struct ConsumerV1 {
        donor: String,
        /// New in a later schema from the consumer's POV — defaulted when a *producer* omits it.
        #[serde(default)]
        priority: u8,
    }

    /// A producer compiled against schema **v2** — adds an `origin_zone` field the v1 consumer has
    /// never heard of.
    #[derive(Debug, Serialize)]
    struct ProducerV2 {
        donor: String,
        priority: u8,
        origin_zone: String, // unknown to ConsumerV1
    }

    #[test]
    fn newer_producer_field_is_ignored_by_older_consumer() {
        // v2 producer → v1 consumer: the unknown `origin_zone` is ignored (additive tolerance).
        let wire = serde_json::to_vec(&ProducerV2 {
            donor: "borough-market".into(),
            priority: 3,
            origin_zone: "southwark".into(),
        }).unwrap();
        let parsed: ConsumerV1 = serde_json::from_slice(&wire).expect("unknown field must be ignored");
        assert_eq!(parsed, ConsumerV1 { donor: "borough-market".into(), priority: 3 });
    }

    #[test]
    fn missing_field_defaults_for_newer_consumer() {
        // An older producer's payload omits `priority`; the consumer defaults it (additive tolerance
        // the other direction).
        let wire = br#"{"donor":"borough-market"}"#;
        let parsed: ConsumerV1 = serde_json::from_slice(wire).expect("missing field must default");
        assert_eq!(parsed, ConsumerV1 { donor: "borough-market".into(), priority: 0 });
    }

    #[test]
    fn additive_tolerance_does_not_extend_to_type_changes() {
        // The boundary: additive tolerance covers *added/removed* fields, NOT type changes. A
        // type-incompatible payload fails to parse — which is exactly why tier 3 (registered
        // migrations) exists for renames/coercions, and tier 2 (detection) for the no-path case.
        let wire = br#"{"donor":"borough-market","priority":"high"}"#; // priority should be u8
        let parsed: Result<ConsumerV1, _> = serde_json::from_slice(wire);
        assert!(parsed.is_err(), "a type change is NOT silently coerced (migration territory)");
    }
}
