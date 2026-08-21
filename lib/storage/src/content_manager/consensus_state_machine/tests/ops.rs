//! Explicit tests asserting behavior of individual consensus operations
//! and tests for cases that `proptest` is unlikely to generate or reach

use std::collections::HashMap;

use segment::data_types::vector_name_config::*;

use super::*;
use crate::content_manager::collection_meta_ops::*;
use crate::content_manager::consensus_ops::ConsensusOperations;
use crate::content_manager::consensus_state_machine::*;

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

fn collection_meta_op(op: CollectionMetaOperations) -> ConsensusOperations {
    ConsensusOperations::CollectionMeta(Box::new(op))
}
