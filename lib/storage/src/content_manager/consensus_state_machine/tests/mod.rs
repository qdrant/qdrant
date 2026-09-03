//! Common test fixtures

mod context;
mod ops;
mod prop;
mod replay;

use std::num::NonZeroU32;

use collection::collection::vector_name_schema;
use collection::collection_state;
use collection::config::{CollectionConfigInternal, CollectionParams};
use collection::operations::types::VectorsConfig;
use collection::optimizers_builder::OptimizersConfig;
use segment::types::*;
use shard::operations::VectorNameConfig;

use super::*;

const PEER_ID: u64 = 42;
const OTHER_PEER_ID: u64 = 43;

fn state_machine(state: ClusterState) -> ConsensusStateMachine {
    ConsensusStateMachine::new(state, node_context())
}

/// Node config is fixed, except for the parts used in tests. Extend it as needed.
fn node_context() -> NodeContext {
    NodeContext {
        peer_id: PEER_ID,
        is_distributed: true,
        collection_defaults: None,
        default_shard_transfer_method: None,
        max_collections: None,
        wal: Default::default(),
        optimizers: optimizers_config(),
        hnsw_index: Default::default(),
        payload: None,
        on_disk_payload: false,
    }
}

/// Request leaving everything to defaults, for a test to set the parts it covers
fn create_collection_request() -> CreateCollection {
    CreateCollection {
        vectors: VectorsConfig::Multi(Default::default()),
        shard_number: None,
        sharding_method: None,
        replication_factor: None,
        write_consistency_factor: None,
        #[expect(deprecated)]
        on_disk_payload: None,
        payload: None,
        hnsw_config: None,
        wal_config: None,
        optimizers_config: None,
        quantization_config: None,
        sparse_vectors: None,
        strict_mode_config: None,
        uuid: None,
        metadata: None,
    }
}

fn collection_state(vectors: Vec<(VectorNameBuf, VectorNameConfig)>) -> collection_state::State {
    let mut params = collection_params(VectorsConfig::Multi(Default::default()));

    for (name, config) in vectors {
        vector_name_schema::add_vector_to_config(&mut params, &name, &config)
            .expect("vector added");
    }

    collection_state::State {
        config: collection_config(params),
        shards: Default::default(),
        resharding: None,
        transfers: Default::default(),
        shards_key_mapping: Default::default(),
        payload_index_schema: Default::default(),
    }
}

/// Collection config is fixed, except for the parts used in tests. Extend it as needed.
fn collection_config(params: CollectionParams) -> CollectionConfigInternal {
    CollectionConfigInternal {
        params,
        hnsw_config: Default::default(),
        optimizer_config: optimizers_config(),
        wal_config: Default::default(),
        quantization_config: None,
        strict_mode_config: None,
        uuid: None,
        metadata: None,
    }
}

/// Same config a collection created by the machine gets, since `NodeContext` carries it
fn optimizers_config() -> OptimizersConfig {
    OptimizersConfig {
        deleted_threshold: 0.1,
        vacuum_min_vector_number: 1000,
        default_segment_number: 0,
        max_segment_size: None,
        #[expect(deprecated)]
        memmap_threshold: None,
        indexing_threshold: Some(100_000),
        flush_interval_sec: 60,
        max_optimization_threads: Some(0),
        prevent_unoptimized: None,
    }
}

fn collection_params(vectors: VectorsConfig) -> CollectionParams {
    CollectionParams {
        vectors,
        sparse_vectors: None,
        shard_number: NonZeroU32::new(1).unwrap(),
        replication_factor: NonZeroU32::new(1).unwrap(),
        write_consistency_factor: NonZeroU32::new(1).unwrap(),
        sharding_method: None,
        read_fan_out_factor: None,
        read_fan_out_delay_ms: None,
        #[expect(deprecated)]
        on_disk_payload: Some(false),
        payload: None,
    }
}
