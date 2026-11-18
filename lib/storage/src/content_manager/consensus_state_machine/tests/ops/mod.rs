//! Explicit tests asserting behavior of individual consensus operations
//! and tests for cases that `proptest` is unlikely to generate or reach

mod alias;
mod cluster_metadata;
mod create_collection;
mod delete_collection;
mod general;
mod named_vector;
mod payload_index;
mod peer;
mod quota;
mod replica_state;
mod resharding;
mod shard_key;
#[cfg(feature = "staging")]
mod staging;
mod transfer;
mod update_collection;

use std::collections::HashMap;
use std::num::NonZeroU32;

use ahash::AHashMap;
use collection::collection_state::ShardInfo;
use collection::config::ShardingMethod;
use collection::operations::cluster_ops::ReshardingDirection;
use collection::operations::config_diff::OptimizersConfigDiff;
use collection::operations::types::PeerMetadata;
use collection::shards::replica_set::replica_set_state::ReplicaState;
use collection::shards::resharding::{ReshardKey, ReshardState};
use collection::shards::shard::ShardId;
use collection::shards::transfer::{ShardTransfer, ShardTransferMethod};
use segment::data_types::vector_name_config::*;
use segment::types::*;
use serde_json::Value;
use uuid::Uuid;

use super::*;
use crate::content_manager::collection_meta_ops::*;
use crate::content_manager::consensus_ops::ConsensusOperations;
use crate::content_manager::consensus_state_machine::*;

const COLLECTION: &str = "alpha";

fn set_resharding(state: &mut ClusterState, shard_key: ShardKey, shard_id: ShardId) {
    let resharding = ReshardState::new(
        Uuid::nil(),
        ReshardingDirection::Up,
        PEER_ID,
        shard_id,
        Some(shard_key),
    );

    state
        .collections
        .get_mut(COLLECTION)
        .expect("collection exists")
        .resharding = Some(resharding);
}

fn auto_resharding_state(shard_count: u32) -> ClusterState {
    let mut state = cluster_state(Vec::new());
    let collection = state.collections.get_mut(COLLECTION).expect("collection");
    collection.config.params.shard_number = NonZeroU32::new(shard_count).unwrap();

    for shard_id in 0..shard_count {
        let shard = ShardInfo {
            replicas: HashMap::from([(PEER_ID, ReplicaState::Active)]),
        };

        collection.shards.insert(shard_id, shard);
    }

    state
}

fn resharding_key(direction: ReshardingDirection, shard_id: ShardId) -> ReshardKey {
    ReshardKey {
        uuid: Uuid::nil(),
        direction,
        peer_id: PEER_ID,
        shard_id,
        shard_key: None,
    }
}

fn transfer_state() -> ClusterState {
    let mut state = auto_resharding_state(1);
    add_peer(&mut state, PEER_ID, Some("1.18.0"));
    add_peer(&mut state, OTHER_PEER_ID, Some("1.18.0"));
    state
}

fn shard_transfer(
    from: PeerId,
    to: PeerId,
    sync: bool,
    method: ShardTransferMethod,
) -> ShardTransfer {
    ShardTransfer {
        shard_id: 0,
        to_shard_id: None,
        from,
        to,
        sync,
        method: Some(method),
        filter: None,
    }
}

fn set_replica_state_op(
    shard_id: ShardId,
    peer_id: PeerId,
    state: ReplicaState,
    from_state: Option<ReplicaState>,
) -> ConsensusOperations {
    collection_meta_op(CollectionMetaOperations::SetShardReplicaState(
        SetShardReplicaState {
            collection_name: COLLECTION.into(),
            shard_id,
            peer_id,
            state,
            from_state,
        },
    ))
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

fn drop_shard_key_op(shard_key: ShardKey) -> ConsensusOperations {
    collection_meta_op(CollectionMetaOperations::DropShardKey(DropShardKey {
        collection_name: COLLECTION.into(),
        shard_key,
    }))
}

fn custom_sharding_state() -> ClusterState {
    let mut state = cluster_state(Vec::new());
    state
        .collections
        .get_mut(COLLECTION)
        .expect("collection exists")
        .config
        .params
        .sharding_method = Some(ShardingMethod::Custom);
    state
}

fn add_shard_key(state: &mut ClusterState, shard_key: ShardKey, shard_ids: &[ShardId]) {
    let collection = state
        .collections
        .get_mut(COLLECTION)
        .expect("collection exists");

    for &shard_id in shard_ids {
        collection.shards.insert(
            shard_id,
            ShardInfo {
                replicas: HashMap::from([(PEER_ID, ReplicaState::Active)]),
            },
        );
        collection
            .shards_key_mapping
            .entry(shard_key.clone())
            .or_default()
            .insert(shard_id);
    }
}

fn add_peer(state: &mut ClusterState, peer_id: PeerId, version: Option<&str>) {
    let address = format!("http://peer-{peer_id}")
        .parse()
        .expect("valid peer URI");
    state.peer_address_by_id.insert(peer_id, address);

    if let Some(version) = version {
        state.peer_metadata_by_id.insert(
            peer_id,
            PeerMetadata::new(version.parse().expect("valid version")),
        );
    }
}

/// Shards a collection is created with, one replica per peer of each placement entry
fn shards(placement: Vec<Vec<PeerId>>) -> AHashMap<ShardId, ShardInfo> {
    placement
        .into_iter()
        .enumerate()
        .map(|(idx, peers)| {
            let replicas = peers
                .into_iter()
                .map(|peer_id| (peer_id, ReplicaState::Initializing))
                .collect();

            (idx as ShardId, ShardInfo { replicas })
        })
        .collect()
}

fn optimizers_diff() -> OptimizersConfigDiff {
    OptimizersConfigDiff {
        deleted_threshold: Some(0.5),
        vacuum_min_vector_number: None,
        default_segment_number: None,
        max_segment_size: None,
        #[expect(deprecated)]
        memmap_threshold: None,
        indexing_threshold: None,
        flush_interval_sec: None,
        max_optimization_threads: None,
        prevent_unoptimized: None,
    }
}

fn metadata(value: Value) -> Payload {
    serde_json::from_value(value).expect("valid metadata")
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
