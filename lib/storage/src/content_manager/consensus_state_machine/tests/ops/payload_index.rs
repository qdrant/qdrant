//! `CreatePayloadIndex` and `DropPayloadIndex` operations

use segment::types::*;

use super::*;
use crate::content_manager::collection_meta_ops::*;
use crate::content_manager::consensus_ops::ConsensusOperations;
use crate::content_manager::consensus_state_machine::*;

#[test]
fn create_payload_index() {
    let state = cluster_state(Vec::new());

    let mut machine = state_machine(state);
    let outcome = machine.apply(&create_payload_index_op("city", PayloadSchemaType::Keyword));

    let Ok(Plan { actions, .. }) = outcome else {
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

    let Ok(Plan { actions, .. }) = outcome else {
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

    let Ok(Plan { actions, .. }) = outcome else {
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

    let Ok(Plan { actions, .. }) = outcome else {
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

    let Ok(Plan { actions, .. }) = outcome else {
        panic!("dropping a field that is not indexed should be accepted, got {outcome:?}");
    };

    // Action is emitted even if state already matches
    assert!(matches!(
        actions.as_slice(),
        [Action::DropPayloadIndex { .. }],
    ));

    assert_eq!(machine.state(), &state);
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

fn field_name(field: &str) -> PayloadKeyType {
    field.parse().expect("valid field name")
}
