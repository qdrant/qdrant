//! Test fixtures shared across this module

use std::num::NonZeroU32;

use collection::collection_state;
use collection::config::{CollectionConfigInternal, CollectionParams};
use collection::operations::types::VectorsConfig;
use collection::optimizers_builder::OptimizersConfig;
use collection::shards::shard::PeerId;

pub(super) const COLLECTION: &str = "books";
pub(super) const PEER_ID: PeerId = 42;

/// Collection state with every field left empty, for a test to fill the one it covers
pub(super) fn collection_state() -> collection_state::State {
    collection_state::State {
        config: collection_config(),
        shards: Default::default(),
        resharding: None,
        transfers: Default::default(),
        shards_key_mapping: Default::default(),
        payload_index_schema: Default::default(),
    }
}

/// Config is fixed: no test reads more of it than whether two configs are equal
fn collection_config() -> CollectionConfigInternal {
    let params = CollectionParams {
        vectors: VectorsConfig::Multi(Default::default()),
        sparse_vectors: None,
        shard_number: NonZeroU32::new(1).unwrap(),
        sharding_method: None,
        #[expect(deprecated)]
        on_disk_payload: Some(false),
        payload: None,
        replication_factor: NonZeroU32::new(1).unwrap(),
        write_consistency_factor: NonZeroU32::new(1).unwrap(),
        read_fan_out_factor: None,
        read_fan_out_delay_ms: None,
    };

    CollectionConfigInternal {
        params,
        hnsw_config: Default::default(),
        optimizer_config: OptimizersConfig {
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
        },
        wal_config: Default::default(),
        quantization_config: None,
        strict_mode_config: None,
        uuid: None,
        metadata: None,
    }
}
