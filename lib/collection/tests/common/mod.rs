use collection::config::{CollectionConfig, CollectionParams, WalConfig};
use collection::optimizers_builder::OptimizersConfig;
use collection::{ChannelService, Collection, CollectionId, CollectionShardDistribution};
use segment::types::Distance;
use std::num::NonZeroU32;
use std::path::Path;

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
pub async fn simple_collection_fixture(
    collection_path: &Path,
    total_shard_number: u32,
    remote_shard_number: u32,
) -> Collection {
    assert!(total_shard_number >= remote_shard_number);
    let wal_config = WalConfig {
        wal_capacity_mb: 1,
        wal_segments_ahead: 0,
    };

    let collection_params = CollectionParams {
        vector_size: 4,
        distance: Distance::Dot,
        shard_number: NonZeroU32::new(total_shard_number).expect("Shard number can not be zero"),
        on_disk_payload: false,
    };

    let collection_config = CollectionConfig {
        params: collection_params,
        optimizer_config: TEST_OPTIMIZERS_CONFIG.clone(),
        wal_config,
        hnsw_config: Default::default(),
    };

    let snapshot_path = collection_path.join("snapshots");

    let shard_distribution = if remote_shard_number == 0 {
        CollectionShardDistribution::AllLocal
    } else {
        let local = total_shard_number - remote_shard_number;
        let mut remote = Vec::with_capacity(remote_shard_number as usize);
        for r in 1..=remote_shard_number {
            remote.push((total_shard_number - r, 1)) // all on PeerId 1
        }
        CollectionShardDistribution::Distribution {
            local: (0..local).collect(),
            remote,
        }
    };

    Collection::new(
        "test".to_string(),
        collection_path,
        &snapshot_path,
        &collection_config,
        shard_distribution,
        ChannelService::default(),
    )
    .await
    .unwrap()
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
