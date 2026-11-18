//! `UpdateClusterMetadata` operation

use std::collections::HashMap;

use serde_json::{Value, json};

use super::*;
use crate::content_manager::consensus_ops::ConsensusOperations;
use crate::content_manager::consensus_state_machine::*;

#[test]
fn update_cluster_metadata() {
    let mut machine = state_machine(ClusterState::default());
    let outcome = machine.apply(&update_cluster_metadata_op("region", json!("eu")));

    let Ok(Plan { actions, .. }) = outcome else {
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

    let Ok(Plan { actions, .. }) = outcome else {
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

    let Ok(Plan { actions, .. }) = outcome else {
        panic!("replay of an applied key should be accepted, got {outcome:?}");
    };

    assert!(actions.is_empty());
    assert_eq!(machine.state(), &state);
}

#[test]
fn update_cluster_metadata_remove() {
    let mut machine = state_machine(cluster_metadata_state("region", json!("eu")));
    let outcome = machine.apply(&update_cluster_metadata_op("region", Value::Null));

    let Ok(Plan { actions, .. }) = outcome else {
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

    let Ok(Plan { actions, .. }) = outcome else {
        panic!("removing a key that does not exist should be accepted, got {outcome:?}");
    };

    assert!(actions.is_empty());
    assert_eq!(machine.state(), &state);
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
