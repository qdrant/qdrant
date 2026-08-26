//! Explicit tests asserting behavior of individual consensus operations
//! and tests for cases that `proptest` is unlikely to generate or reach

use std::collections::{BTreeMap, BTreeSet, HashMap};

use collection::operations::types::PeerMetadata;
use segment::data_types::vector_name_config::*;
use segment::types::*;
use serde_json::{Value, json};

use super::*;
use crate::content_manager::collection_meta_ops::*;
use crate::content_manager::consensus_ops::ConsensusOperations;
use crate::content_manager::consensus_state_machine::*;
use crate::content_manager::errors::StorageError;
use crate::quota::QuotaConfig;

const COLLECTION: &str = "alpha";

#[test]
fn nop() {
    let state = cluster_state(Vec::new());

    let mut machine = state_machine(state.clone());
    let outcome = machine.apply(&collection_meta_op(CollectionMetaOperations::Nop {
        token: 42,
    }));

    let ApplyOutcome::Accepted(actions) = outcome else {
        panic!("a nop should be accepted, got {outcome:?}");
    };

    assert!(actions.is_empty());
    assert_eq!(machine.state(), &state);
}

#[test]
fn create_alias() {
    let state = cluster_state(Vec::new());

    let mut machine = state_machine(state);
    let outcome = machine.apply(&change_aliases_op(vec![create_alias_action(
        "alias", COLLECTION,
    )]));

    let ApplyOutcome::Accepted(actions) = outcome else {
        panic!("creating an alias should be accepted, got {outcome:?}");
    };

    assert_eq!(actions, vec![set_aliases(vec![("alias", COLLECTION)])]);

    let aliases = &machine.state().aliases;

    assert_eq!(aliases.get("alias").map(String::as_str), Some(COLLECTION));
}

#[test]
fn create_alias_replay() {
    let mut state = cluster_state(Vec::new());
    state.aliases.insert("alias".into(), COLLECTION.into());

    let mut machine = state_machine(state.clone());
    let outcome = machine.apply(&change_aliases_op(vec![create_alias_action(
        "alias", COLLECTION,
    )]));

    let ApplyOutcome::Accepted(actions) = outcome else {
        panic!("replay of an applied alias should be accepted, got {outcome:?}");
    };

    // The mapping already holds what the operation writes, so there is nothing to save
    assert!(actions.is_empty());

    assert_eq!(machine.state(), &state, "replay should not change anything");
}

#[test]
fn create_alias_reject_missing_collection() {
    let state = cluster_state(Vec::new());

    let mut machine = state_machine(state.clone());
    let outcome = machine.apply(&change_aliases_op(vec![create_alias_action(
        "alias", "missing",
    )]));

    assert!(matches!(
        outcome,
        ApplyOutcome::Rejected(StorageError::NotFound { .. })
    ));

    assert_eq!(machine.state(), &state);
}

#[test]
fn create_alias_reject_alias_target() {
    let mut state = cluster_state(Vec::new());
    state.aliases.insert("alias".into(), COLLECTION.into());

    let mut machine = state_machine(state.clone());
    let outcome = machine.apply(&change_aliases_op(vec![create_alias_action(
        "other", "alias",
    )]));

    // An alias of an alias is rejected: the target is not resolved
    assert!(matches!(
        outcome,
        ApplyOutcome::Rejected(StorageError::NotFound { .. })
    ));

    assert_eq!(machine.state(), &state);
}

#[test]
fn create_alias_reject_collection_name() {
    let state = cluster_state(Vec::new());

    let mut machine = state_machine(state.clone());
    let outcome = machine.apply(&change_aliases_op(vec![create_alias_action(
        COLLECTION, COLLECTION,
    )]));

    assert!(matches!(
        outcome,
        ApplyOutcome::Rejected(StorageError::AlreadyExists { .. })
    ));

    assert_eq!(machine.state(), &state);
}

#[test]
fn delete_alias() {
    let mut state = cluster_state(Vec::new());
    state.aliases.insert("alias".into(), COLLECTION.into());

    let mut machine = state_machine(state);
    let outcome = machine.apply(&change_aliases_op(vec![delete_alias_action("alias")]));

    let ApplyOutcome::Accepted(actions) = outcome else {
        panic!("deleting an alias should be accepted, got {outcome:?}");
    };

    assert_eq!(actions, vec![remove_aliases(vec!["alias"])]);

    assert!(machine.state().aliases.get("alias").is_none());
}

#[test]
fn delete_alias_missing() {
    let state = cluster_state(Vec::new());

    let mut machine = state_machine(state.clone());
    let outcome = machine.apply(&change_aliases_op(vec![delete_alias_action("alias")]));

    let ApplyOutcome::Accepted(actions) = outcome else {
        panic!("deleting an alias that does not exist should be accepted, got {outcome:?}");
    };

    // Nothing to remove, so nothing to save
    assert!(actions.is_empty());

    assert_eq!(machine.state(), &state);
}

#[test]
fn rename_alias() {
    let mut state = cluster_state(Vec::new());
    state.aliases.insert("alias".into(), COLLECTION.into());

    let mut machine = state_machine(state);
    let outcome = machine.apply(&change_aliases_op(vec![rename_alias_action(
        "alias", "other",
    )]));

    let ApplyOutcome::Accepted(actions) = outcome else {
        panic!("renaming an alias should be accepted, got {outcome:?}");
    };

    // A rename resolves to the value it moves, and the alias it takes it from
    assert_eq!(
        actions,
        vec![Action::UpdateAliases {
            set: BTreeMap::from([("other".to_string(), COLLECTION.to_string())]),
            remove: BTreeSet::from(["alias".to_string()]),
        }]
    );

    let aliases = &machine.state().aliases;

    assert!(aliases.get("alias").is_none());
    assert_eq!(aliases.get("other").map(String::as_str), Some(COLLECTION));
}

#[test]
fn rename_alias_reject_missing() {
    let state = cluster_state(Vec::new());

    let mut machine = state_machine(state.clone());
    let outcome = machine.apply(&change_aliases_op(vec![rename_alias_action(
        "alias", "other",
    )]));

    assert!(matches!(
        outcome,
        ApplyOutcome::Rejected(StorageError::NotFound { .. })
    ));

    assert_eq!(machine.state(), &state);
}

#[test]
fn change_aliases_reject_missing_rename() {
    let state = cluster_state(Vec::new());

    let mut machine = state_machine(state.clone());
    let outcome = machine.apply(&change_aliases_op(vec![
        create_alias_action("new", COLLECTION),
        rename_alias_action("missing", "other"),
    ]));

    assert!(matches!(
        outcome,
        ApplyOutcome::Rejected(StorageError::NotFound { .. })
    ));

    // The action before the rename is validated, never emitted
    assert_eq!(machine.state(), &state);
}

#[test]
fn change_aliases_in_order() {
    let state = cluster_state(Vec::new());

    let mut machine = state_machine(state);
    let outcome = machine.apply(&change_aliases_op(vec![
        create_alias_action("alias", COLLECTION),
        rename_alias_action("alias", "other"),
    ]));

    let ApplyOutcome::Accepted(actions) = outcome else {
        panic!("renaming an alias the operation just created should be accepted, got {outcome:?}");
    };

    // The alias the operation creates and renames never reaches the mapping
    assert_eq!(actions, vec![set_aliases(vec![("other", COLLECTION)])]);

    let aliases = &machine.state().aliases;

    assert!(aliases.get("alias").is_none());
    assert_eq!(aliases.get("other").map(String::as_str), Some(COLLECTION));
}

#[test]
fn change_aliases_reject_whole_operation() {
    let mut state = cluster_state(Vec::new());
    state.aliases.insert("alias".into(), COLLECTION.into());

    let mut machine = state_machine(state.clone());
    let outcome = machine.apply(&change_aliases_op(vec![
        delete_alias_action("alias"),
        create_alias_action("other", "missing"),
    ]));

    assert!(matches!(
        outcome,
        ApplyOutcome::Rejected(StorageError::NotFound { .. })
    ));

    // The delete before the failing action is validated, never emitted
    assert_eq!(machine.state(), &state);
}

#[test]
fn create_named_vector_dense() {
    let machine = create_named_vector_impl(dense(4, Distance::Cosine));

    let params = &machine
        .state()
        .collection(COLLECTION)
        .expect("collection exists")
        .config
        .params;

    assert!(params.vectors.get_params("text").is_some());
}

#[test]
fn create_named_vector_sparse() {
    let machine = create_named_vector_impl(sparse());

    let params = &machine
        .state()
        .collection(COLLECTION)
        .expect("collection exists")
        .config
        .params;

    let sparse = params
        .sparse_vectors
        .as_ref()
        .expect("sparse vector config exists");

    assert!(sparse.contains_key("text"));
}

fn create_named_vector_impl(config: VectorNameConfig) -> ConsensusStateMachine {
    let state = cluster_state(Vec::new());

    let mut machine = state_machine(state);
    let outcome = machine.apply(&create_named_vector_op("text", config));

    let ApplyOutcome::Accepted(actions) = outcome else {
        panic!("creating a new named vector should be accepted, got {outcome:?}");
    };

    assert!(matches!(
        actions.as_slice(),
        [Action::AddNamedVector { .. }],
    ));

    machine
}

#[test]
fn create_named_vector_replay() {
    let config = dense(4, Distance::Cosine);
    let state = cluster_state(vec![("text", config.clone())]);

    let mut machine = state_machine(state.clone());
    let outcome = machine.apply(&create_named_vector_op("text", config));

    let ApplyOutcome::Accepted(actions) = outcome else {
        panic!("replay of an applied named vector should be accepted, got {outcome:?}");
    };

    assert!(matches!(
        actions.as_slice(),
        [Action::AddNamedVector { .. }],
    ));

    assert_eq!(machine.state(), &state, "replay should not change anything");
}

#[test]
fn create_named_vector_reject_existing_diff_dim() {
    create_named_vector_reject_existing(dense(8, Distance::Cosine));
}

#[test]
fn create_named_vector_reject_existing_diff_type() {
    create_named_vector_reject_existing(sparse());
}

fn create_named_vector_reject_existing(config: VectorNameConfig) {
    let state = cluster_state(vec![("text", dense(4, Distance::Cosine))]);

    let mut machine = state_machine(state.clone());
    let outcome = machine.apply(&create_named_vector_op("text", config));

    assert!(matches!(
        outcome,
        ApplyOutcome::Rejected(StorageError::BadInput { .. })
    ));

    assert_eq!(machine.state(), &state);
}

#[test]
fn delete_named_vector_dense() {
    delete_named_vector_impl(dense(4, Distance::Cosine));
}

#[test]
fn delete_named_vector_sparse() {
    delete_named_vector_impl(sparse());
}

fn delete_named_vector_impl(config: VectorNameConfig) {
    let state = cluster_state(vec![("text", config)]);

    let mut machine = state_machine(state);
    let outcome = machine.apply(&delete_named_vector_op("text"));

    let ApplyOutcome::Accepted(actions) = outcome else {
        panic!("deleting an existing vector should be accepted, got {outcome:?}");
    };

    assert!(matches!(
        actions.as_slice(),
        [Action::DropNamedVector { .. }],
    ));

    let params = &machine
        .state()
        .collection(COLLECTION)
        .expect("collection exists")
        .config
        .params;

    assert!(params.vectors.get_params("text").is_none());

    assert!(
        params
            .sparse_vectors
            .as_ref()
            .is_none_or(|sparse| !sparse.contains_key("text")),
    );
}

#[test]
fn delete_named_vector_missing() {
    let state = cluster_state(Vec::new());

    let mut machine = state_machine(state.clone());
    let outcome = machine.apply(&delete_named_vector_op("text"));

    let ApplyOutcome::Accepted(actions) = outcome else {
        panic!("deleting a vector that does not exist should be accepted, got {outcome:?}");
    };

    // Action is emitted even if state already matches
    assert!(matches!(
        actions.as_slice(),
        [Action::DropNamedVector { .. }],
    ));

    assert_eq!(machine.state(), &state);
}

#[test]
fn create_payload_index() {
    let state = cluster_state(Vec::new());

    let mut machine = state_machine(state);
    let outcome = machine.apply(&create_payload_index_op("city", PayloadSchemaType::Keyword));

    let ApplyOutcome::Accepted(actions) = outcome else {
        panic!("creating payload index should be accepted, got {outcome:?}");
    };

    assert!(matches!(
        actions.as_slice(),
        [Action::SetPayloadIndex { .. }],
    ));

    let schema = &machine
        .state()
        .collection(COLLECTION)
        .expect("collection exists")
        .payload_index_schema
        .schema;

    assert!(schema.contains_key(&field_name("city")));
}

#[test]
fn create_payload_index_replay() {
    let state = cluster_state_with_index("city", PayloadSchemaType::Keyword);

    let mut machine = state_machine(state.clone());
    let outcome = machine.apply(&create_payload_index_op("city", PayloadSchemaType::Keyword));

    let ApplyOutcome::Accepted(actions) = outcome else {
        panic!("replay of an applied payload index should be accepted, got {outcome:?}");
    };

    assert!(matches!(
        actions.as_slice(),
        [Action::SetPayloadIndex { .. }],
    ));

    assert_eq!(machine.state(), &state, "replay should not change anything");
}

#[test]
fn create_payload_index_replace_schema() {
    let state = cluster_state_with_index("city", PayloadSchemaType::Keyword);

    let mut machine = state_machine(state);
    let outcome = machine.apply(&create_payload_index_op("city", PayloadSchemaType::Integer));

    let ApplyOutcome::Accepted(actions) = outcome else {
        panic!("indexing a field again with another schema should be accepted, got {outcome:?}");
    };

    assert!(matches!(
        actions.as_slice(),
        [Action::SetPayloadIndex { .. }],
    ));

    let schema = &machine
        .state()
        .collection(COLLECTION)
        .expect("collection exists")
        .payload_index_schema
        .schema;

    // A field indexed with a different schema is replaced, where a named vector is rejected
    assert_eq!(
        schema.get(&field_name("city")),
        Some(&PayloadFieldSchema::FieldType(PayloadSchemaType::Integer)),
    );
}

#[test]
fn drop_payload_index() {
    let state = cluster_state_with_index("city", PayloadSchemaType::Keyword);

    let mut machine = state_machine(state);
    let outcome = machine.apply(&drop_payload_index_op("city"));

    let ApplyOutcome::Accepted(actions) = outcome else {
        panic!("dropping an indexed field should be accepted, got {outcome:?}");
    };

    assert!(matches!(
        actions.as_slice(),
        [Action::DropPayloadIndex { .. }],
    ));

    let schema = &machine
        .state()
        .collection(COLLECTION)
        .expect("collection exists")
        .payload_index_schema
        .schema;

    assert!(!schema.contains_key(&field_name("city")));
}

#[test]
fn drop_payload_index_missing() {
    let state = cluster_state(Vec::new());

    let mut machine = state_machine(state.clone());
    let outcome = machine.apply(&drop_payload_index_op("city"));

    let ApplyOutcome::Accepted(actions) = outcome else {
        panic!("dropping a field that is not indexed should be accepted, got {outcome:?}");
    };

    // Action is emitted even if state already matches
    assert!(matches!(
        actions.as_slice(),
        [Action::DropPayloadIndex { .. }],
    ));

    assert_eq!(machine.state(), &state);
}

#[test]
fn update_peer_metadata() {
    let mut machine = state_machine(ClusterState::default());
    let outcome = machine.apply(&update_peer_metadata_op(PEER_ID, "1.15.0"));

    let ApplyOutcome::Accepted(actions) = outcome else {
        panic!("metadata of a peer without any should be accepted, got {outcome:?}");
    };

    assert!(matches!(
        actions.as_slice(),
        [Action::SetPeerMetadata { .. }],
    ));

    assert_eq!(
        machine.state().peer_metadata_by_id.get(&PEER_ID),
        Some(&peer_metadata("1.15.0")),
    );
}

#[test]
fn update_peer_metadata_replace() {
    let mut state = ClusterState::default();

    state
        .peer_metadata_by_id
        .insert(PEER_ID, peer_metadata("1.14.0"));

    let mut machine = state_machine(state);
    let outcome = machine.apply(&update_peer_metadata_op(PEER_ID, "1.15.0"));

    let ApplyOutcome::Accepted(actions) = outcome else {
        panic!("a peer reporting a new version should be accepted, got {outcome:?}");
    };

    assert!(matches!(
        actions.as_slice(),
        [Action::SetPeerMetadata { .. }],
    ));

    assert_eq!(
        machine.state().peer_metadata_by_id.get(&PEER_ID),
        Some(&peer_metadata("1.15.0")),
    );
}

#[test]
fn update_peer_metadata_replay() {
    let mut state = ClusterState::default();

    state
        .peer_metadata_by_id
        .insert(PEER_ID, peer_metadata("1.15.0"));

    let mut machine = state_machine(state.clone());
    let outcome = machine.apply(&update_peer_metadata_op(PEER_ID, "1.15.0"));

    let ApplyOutcome::Accepted(actions) = outcome else {
        panic!("replay of applied metadata should be accepted, got {outcome:?}");
    };

    // Nothing left to do: metadata is absolute and the applier only writes it
    assert!(actions.is_empty());

    assert_eq!(machine.state(), &state);
}

#[test]
fn update_cluster_metadata() {
    let mut machine = state_machine(ClusterState::default());
    let outcome = machine.apply(&update_cluster_metadata_op("region", json!("eu")));

    let ApplyOutcome::Accepted(actions) = outcome else {
        panic!("a new metadata key should be accepted, got {outcome:?}");
    };

    assert!(matches!(
        actions.as_slice(),
        [Action::SetClusterMetadataKey { .. }],
    ));

    assert_eq!(
        machine.state().cluster_metadata.get("region"),
        Some(&json!("eu")),
    );
}

#[test]
fn update_cluster_metadata_replace() {
    let mut machine = state_machine(cluster_metadata_state("region", json!("eu")));
    let outcome = machine.apply(&update_cluster_metadata_op("region", json!("us")));

    let ApplyOutcome::Accepted(actions) = outcome else {
        panic!("another value for a key should be accepted, got {outcome:?}");
    };

    assert!(matches!(
        actions.as_slice(),
        [Action::SetClusterMetadataKey { .. }],
    ));

    assert_eq!(
        machine.state().cluster_metadata.get("region"),
        Some(&json!("us")),
    );
}

#[test]
fn update_cluster_metadata_replay() {
    let state = cluster_metadata_state("region", json!("eu"));

    let mut machine = state_machine(state.clone());
    let outcome = machine.apply(&update_cluster_metadata_op("region", json!("eu")));

    let ApplyOutcome::Accepted(actions) = outcome else {
        panic!("replay of an applied key should be accepted, got {outcome:?}");
    };

    assert!(actions.is_empty());
    assert_eq!(machine.state(), &state);
}

#[test]
fn update_cluster_metadata_remove() {
    let mut machine = state_machine(cluster_metadata_state("region", json!("eu")));
    let outcome = machine.apply(&update_cluster_metadata_op("region", Value::Null));

    let ApplyOutcome::Accepted(actions) = outcome else {
        panic!("a null value should be accepted, got {outcome:?}");
    };

    assert!(matches!(
        actions.as_slice(),
        [Action::SetClusterMetadataKey { .. }],
    ));

    assert!(!machine.state().cluster_metadata.contains_key("region"));
}

#[test]
fn update_cluster_metadata_remove_missing() {
    let state = ClusterState::default();

    let mut machine = state_machine(state.clone());
    let outcome = machine.apply(&update_cluster_metadata_op("region", Value::Null));

    let ApplyOutcome::Accepted(actions) = outcome else {
        panic!("removing a key that does not exist should be accepted, got {outcome:?}");
    };

    assert!(actions.is_empty());
    assert_eq!(machine.state(), &state);
}

#[test]
fn set_quota_config() {
    let mut machine = state_machine(ClusterState::default());
    let outcome = machine.apply(&ConsensusOperations::SetQuotaConfig(quota_config(true)));

    let ApplyOutcome::Accepted(actions) = outcome else {
        panic!("a quota config should be accepted, got {outcome:?}");
    };

    assert!(matches!(
        actions.as_slice(),
        [Action::SetQuotaConfig { .. }],
    ));

    assert_eq!(machine.state().quota_config, Some(quota_config(true)));
}

#[test]
fn set_quota_config_replace() {
    let state = ClusterState {
        quota_config: Some(quota_config(true)),
        ..Default::default()
    };

    let mut machine = state_machine(state);
    let outcome = machine.apply(&ConsensusOperations::SetQuotaConfig(quota_config(false)));

    let ApplyOutcome::Accepted(actions) = outcome else {
        panic!("another quota config should be accepted, got {outcome:?}");
    };

    assert!(matches!(
        actions.as_slice(),
        [Action::SetQuotaConfig { .. }],
    ));

    assert_eq!(machine.state().quota_config, Some(quota_config(false)));
}

#[test]
fn set_quota_config_replay() {
    let state = ClusterState {
        quota_config: Some(quota_config(true)),
        ..Default::default()
    };

    let mut machine = state_machine(state.clone());
    let outcome = machine.apply(&ConsensusOperations::SetQuotaConfig(quota_config(true)));

    let ApplyOutcome::Accepted(actions) = outcome else {
        panic!("replay of an applied quota config should be accepted, got {outcome:?}");
    };

    // Action is emitted even if state already matches: applying it also drops recorded verdicts
    assert!(matches!(
        actions.as_slice(),
        [Action::SetQuotaConfig { .. }],
    ));

    assert_eq!(machine.state(), &state);
}

#[cfg(feature = "staging")]
#[test]
fn test_slow_down() {
    let operation = CollectionMetaOperations::TestSlowDown(TestSlowDown {
        peer_id: Some(PEER_ID),
        duration_ms: 10,
    });

    staging_operation_changes_nothing(operation);
}

#[cfg(feature = "staging")]
#[test]
fn test_transient_error() {
    let operation = CollectionMetaOperations::TestTransientError(TestTransientError {
        peer_id: Some(PEER_ID),
        failure_probability_percent: 100,
    });

    staging_operation_changes_nothing(operation);
}

#[cfg(feature = "staging")]
fn staging_operation_changes_nothing(operation: CollectionMetaOperations) {
    let state = cluster_state(Vec::new());

    let mut machine = state_machine(state.clone());
    let outcome = machine.apply(&collection_meta_op(operation));

    let ApplyOutcome::Accepted(actions) = outcome else {
        panic!("a staging operation should be accepted, got {outcome:?}");
    };

    assert!(actions.is_empty());
    assert_eq!(machine.state(), &state);
}

#[test]
fn reject_missing_collection() {
    let mut machine = state_machine(ClusterState::default());
    let outcome = machine.apply(&create_named_vector_op("text", dense(4, Distance::Cosine)));

    assert!(matches!(
        outcome,
        ApplyOutcome::Rejected(StorageError::NotFound { .. })
    ));
}

#[test]
fn resolve_alias() {
    let mut state = cluster_state(Vec::new());
    state.aliases.insert("alias".into(), COLLECTION.into());

    let mut machine = state_machine(state);
    let outcome = machine.apply(&collection_meta_op(
        CollectionMetaOperations::CreateNamedVector(CreateNamedVector {
            collection_name: "alias".into(),
            vector_name: "text".into(),
            config: dense(4, Distance::Cosine),
        }),
    ));

    let ApplyOutcome::Accepted(actions) = outcome else {
        panic!("an alias should resolve to its collection, got {outcome:?}");
    };

    assert_eq!(
        actions
            .first()
            .and_then(Action::collection)
            .expect("action modifies collection"),
        &COLLECTION,
    );

    let vectors = &machine
        .state()
        .collection(COLLECTION)
        .expect("collection exists")
        .config
        .params
        .vectors;

    assert!(vectors.get_params("text").is_some());
}

#[test]
fn reject_dangling_alias() {
    let mut state = cluster_state(Vec::new());
    state.aliases.insert("dangling".into(), "missing".into());

    let mut machine = state_machine(state.clone());
    let outcome = machine.apply(&collection_meta_op(
        CollectionMetaOperations::CreateNamedVector(CreateNamedVector {
            collection_name: "dangling".into(),
            vector_name: "text".into(),
            config: dense(4, Distance::Cosine),
        }),
    ));

    assert!(matches!(
        outcome,
        ApplyOutcome::Rejected(StorageError::NotFound { .. })
    ));

    assert_eq!(machine.state(), &state);
}

fn cluster_state(vectors: Vec<(&str, VectorNameConfig)>) -> ClusterState {
    let vectors = vectors
        .into_iter()
        .map(|(name, config)| (name.into(), config))
        .collect();

    let collection_state = collection_state(vectors);

    ClusterState {
        collections: HashMap::from([(COLLECTION.into(), collection_state)]),
        ..Default::default()
    }
}

fn cluster_state_with_index(field: &str, field_type: PayloadSchemaType) -> ClusterState {
    let mut state = cluster_state(Vec::new());

    state
        .collections
        .get_mut(COLLECTION)
        .expect("collection exists")
        .payload_index_schema
        .schema
        .insert(field_name(field), PayloadFieldSchema::FieldType(field_type));

    state
}

fn collection_meta_op(op: CollectionMetaOperations) -> ConsensusOperations {
    ConsensusOperations::CollectionMeta(Box::new(op))
}

fn change_aliases_op(actions: Vec<AliasOperations>) -> ConsensusOperations {
    collection_meta_op(CollectionMetaOperations::ChangeAliases(
        ChangeAliasesOperation { actions },
    ))
}

fn set_aliases(aliases: Vec<(&str, &str)>) -> Action {
    Action::UpdateAliases {
        set: aliases
            .into_iter()
            .map(|(alias, collection)| (alias.to_string(), collection.to_string()))
            .collect(),
        remove: BTreeSet::new(),
    }
}

fn remove_aliases(aliases: Vec<&str>) -> Action {
    Action::UpdateAliases {
        set: BTreeMap::new(),
        remove: aliases.into_iter().map(String::from).collect(),
    }
}

fn create_alias_action(alias: &str, collection: &str) -> AliasOperations {
    CreateAlias {
        collection_name: collection.into(),
        alias_name: alias.into(),
    }
    .into()
}

fn delete_alias_action(alias: &str) -> AliasOperations {
    DeleteAlias {
        alias_name: alias.into(),
    }
    .into()
}

fn rename_alias_action(old_alias: &str, new_alias: &str) -> AliasOperations {
    RenameAlias {
        old_alias_name: old_alias.into(),
        new_alias_name: new_alias.into(),
    }
    .into()
}

fn create_named_vector_op(vector_name: &str, config: VectorNameConfig) -> ConsensusOperations {
    collection_meta_op(CollectionMetaOperations::CreateNamedVector(
        CreateNamedVector {
            collection_name: COLLECTION.into(),
            vector_name: VectorNameBuf::from(vector_name),
            config,
        },
    ))
}

fn dense(size: usize, distance: Distance) -> VectorNameConfig {
    VectorNameConfig::dense(DenseVectorConfig {
        size,
        distance,
        multivector_config: None,
        datatype: None,
    })
}

fn sparse() -> VectorNameConfig {
    VectorNameConfig::sparse(SparseVectorConfig {
        modifier: None,
        datatype: None,
    })
}

fn delete_named_vector_op(vector_name: &str) -> ConsensusOperations {
    collection_meta_op(CollectionMetaOperations::DeleteNamedVector(
        DeleteNamedVector {
            collection_name: COLLECTION.into(),
            vector_name: VectorNameBuf::from(vector_name),
        },
    ))
}

fn create_payload_index_op(field: &str, field_type: PayloadSchemaType) -> ConsensusOperations {
    collection_meta_op(CollectionMetaOperations::CreatePayloadIndex(
        CreatePayloadIndex {
            collection_name: COLLECTION.to_string(),
            field_name: field_name(field),
            field_schema: PayloadFieldSchema::FieldType(field_type),
        },
    ))
}

fn drop_payload_index_op(field: &str) -> ConsensusOperations {
    collection_meta_op(CollectionMetaOperations::DropPayloadIndex(
        DropPayloadIndex {
            collection_name: COLLECTION.to_string(),
            field_name: field_name(field),
        },
    ))
}

fn update_peer_metadata_op(peer_id: PeerId, version: &str) -> ConsensusOperations {
    ConsensusOperations::UpdatePeerMetadata {
        peer_id,
        metadata: peer_metadata(version),
    }
}

fn peer_metadata(version: &str) -> PeerMetadata {
    PeerMetadata::new(version.parse().expect("valid version"))
}

fn cluster_metadata_state(key: &str, value: Value) -> ClusterState {
    ClusterState {
        cluster_metadata: HashMap::from([(key.into(), value)]),
        ..Default::default()
    }
}

fn update_cluster_metadata_op(key: &str, value: Value) -> ConsensusOperations {
    ConsensusOperations::UpdateClusterMetadata {
        key: key.into(),
        value,
    }
}

fn quota_config(enabled: bool) -> QuotaConfig {
    QuotaConfig {
        enabled,
        max_resident_memory_percent: None,
        max_disk_usage_percent: None,
        release_margin_percent: None,
    }
}

fn field_name(field: &str) -> PayloadKeyType {
    field.parse().expect("valid field name")
}
