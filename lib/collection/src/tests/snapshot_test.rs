use std::num::{NonZeroU32, NonZeroU64};

use segment::types::Distance;

use crate::collection::Collection;
use crate::config::{CollectionConfig, CollectionParams, WalConfig};
use crate::optimizers_builder::OptimizersConfig;
use crate::shard::collection_shard_distribution::CollectionShardDistribution;
use crate::shard::{ChannelService, Shard};

const TEST_OPTIMIZERS_CONFIG: OptimizersConfig = OptimizersConfig {
    deleted_threshold: 0.9,
    vacuum_min_vector_number: 1000,
    default_segment_number: 2,
    max_segment_size: None,
    memmap_threshold: None,
    indexing_threshold: 50_000,
    flush_interval_sec: 30,
    max_optimization_threads: 2,
};

#[tokio::test]
async fn test_snapshot_collection() {
    let wal_config = WalConfig {
        wal_capacity_mb: 1,
        wal_segments_ahead: 0,
    };

    let collection_params = CollectionParams {
        vector_size: NonZeroU64::new(4).unwrap(),
        distance: Distance::Dot,
        shard_number: NonZeroU32::new(3).expect("Shard number can not be zero"),
        on_disk_payload: false,
    };

    let config = CollectionConfig {
        params: collection_params,
        optimizer_config: TEST_OPTIMIZERS_CONFIG.clone(),
        wal_config,
        hnsw_config: Default::default(),
    };

    let snapshots_path = tempdir::TempDir::new("test_snapshots").unwrap();
    let collection_dir = tempdir::TempDir::new("test_collection").unwrap();
    let recover_dir = tempdir::TempDir::new("test_collection_rec").unwrap();
    let collection_name = "test".to_string();
    let collection_name_rec = "test_rec".to_string();

    let mut collection = Collection::new(
        collection_name,
        collection_dir.path(),
        snapshots_path.path(),
        &config,
        CollectionShardDistribution::new(vec![0, 1], vec![(2, 10000)]),
        ChannelService::default(),
    )
    .await
    .unwrap();

    let snapshots_tmp_dir = collection_dir.path().join("snapshots_tmp");
    std::fs::create_dir_all(&snapshots_tmp_dir).unwrap();
    let snapshot_description = collection
        .create_snapshot(&snapshots_tmp_dir)
        .await
        .unwrap();

    Collection::restore_snapshot(
        &snapshots_path.path().join(snapshot_description.name),
        recover_dir.path(),
    )
    .unwrap();

    let mut recovered_collection = Collection::load(
        collection_name_rec,
        recover_dir.path(),
        snapshots_path.path(),
        ChannelService::default(),
    )
    .await;

    {
        let shards_holder = &recovered_collection.shards_holder.read().await;

        let shard_0 = shards_holder.get_shard(&0).unwrap();
        assert!(matches!(shard_0, Shard::Local(_)));
        let shard_1 = shards_holder.get_shard(&1).unwrap();
        assert!(matches!(shard_1, Shard::Local(_)));
        let shard_2 = shards_holder.get_shard(&2).unwrap();
        assert!(matches!(shard_2, Shard::Remote(_)));
    }

    collection.before_drop().await;
    recovered_collection.before_drop().await;
}
