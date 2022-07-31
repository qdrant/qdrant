use std::num::{NonZeroU32, NonZeroU64};
use std::path::Path;

use collection::collection::Collection;
use collection::config::{CollectionConfig, CollectionParams, WalConfig};
use collection::operations::types::CollectionError;
use collection::optimizers_builder::OptimizersConfig;
use collection::shard::collection_shard_distribution::CollectionShardDistribution;
use collection::shard::{ChannelService, CollectionId};
use segment::types::Distance;

/// Test collections for this upper bound of shards.
/// Testing with more shards is problematic due to `number of open files problem`
/// See https://github.com/qdrant/qdrant/issues/379
#[allow(dead_code)]
pub const N_SHARDS: u32 = 3;

pub const TEST_OPTIMIZERS_CONFIG: OptimizersConfig = OptimizersConfig {
    deleted_threshold: 0.9,
    vacuum_min_vector_number: 1000,
    default_segment_number: 2,
    max_segment_size: 100_000,
    memmap_threshold: 100_000,
    indexing_threshold: 50_000,
    flush_interval_sec: 30,
    max_optimization_threads: 2,
};

#[allow(dead_code)]
pub async fn simple_collection_fixture(collection_path: &Path, shard_number: u32) -> Collection {
    let wal_config = WalConfig {
        wal_capacity_mb: 1,
        wal_segments_ahead: 0,
    };

    let collection_params = CollectionParams {
        vector_size: NonZeroU64::new(4).unwrap(),
        distance: Distance::Dot,
        shard_number: NonZeroU32::new(shard_number).expect("Shard number can not be zero"),
        on_disk_payload: false,
    };

    let collection_config = CollectionConfig {
        params: collection_params,
        optimizer_config: TEST_OPTIMIZERS_CONFIG.clone(),
        wal_config,
        hnsw_config: Default::default(),
    };

    let snapshot_path = collection_path.join("snapshots");

    // Default to a collection with all the shards local
    new_local_collection(
        "test".to_string(),
        collection_path,
        &snapshot_path,
        &collection_config,
    )
    .await
    .unwrap()
}

/// Default to a collection with all the shards local
pub async fn new_local_collection(
    id: CollectionId,
    path: &Path,
    snapshots_path: &Path,
    config: &CollectionConfig,
) -> Result<Collection, CollectionError> {
    Collection::new(
        id,
        path,
        snapshots_path,
        config,
        CollectionShardDistribution::all_local(Some(config.params.shard_number.into())),
        ChannelService::default(),
    )
    .await
}

/// Default to a collection with all the shards local
#[allow(dead_code)]
pub async fn load_local_collection(
    id: CollectionId,
    path: &Path,
    snapshots_path: &Path,
) -> Collection {
    Collection::load(id, path, snapshots_path, ChannelService::default()).await
}
