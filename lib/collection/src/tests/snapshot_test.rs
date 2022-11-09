use std::collections::{HashMap, HashSet};
use std::num::{NonZeroU32, NonZeroU64};
use std::sync::Arc;

use segment::types::Distance;
use tempfile::Builder;

use crate::collection::{Collection, RequestShardTransfer};
use crate::config::{CollectionConfig, CollectionParams, WalConfig};
use crate::operations::types::{VectorParams, VectorsConfig};
use crate::optimizers_builder::OptimizersConfig;
use crate::shards::channel_service::ChannelService;
use crate::shards::collection_shard_distribution::CollectionShardDistribution;
use crate::shards::replica_set::OnPeerFailure;

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

pub fn dummy_on_replica_failure() -> OnPeerFailure {
    Arc::new(move |_peer_id, _shard_id| {})
}

pub fn dummy_request_shard_transfer() -> RequestShardTransfer {
    Arc::new(move |_transfer| {})
}

#[tokio::test]
async fn test_snapshot_collection() {
    let wal_config = WalConfig {
        wal_capacity_mb: 1,
        wal_segments_ahead: 0,
    };

    let collection_params = CollectionParams {
        vectors: VectorsConfig::Single(VectorParams {
            size: NonZeroU64::new(4).unwrap(),
            distance: Distance::Dot,
        }),
        shard_number: NonZeroU32::new(4).unwrap(),
        replication_factor: NonZeroU32::new(3).unwrap(),
        write_consistency_factor: NonZeroU32::new(2).unwrap(),
        on_disk_payload: false,
    };

    let config = CollectionConfig {
        params: collection_params,
        optimizer_config: TEST_OPTIMIZERS_CONFIG.clone(),
        wal_config,
        hnsw_config: Default::default(),
    };

    let snapshots_path = Builder::new().prefix("test_snapshots").tempdir().unwrap();
    let collection_dir = Builder::new().prefix("test_collection").tempdir().unwrap();
    let recover_dir = Builder::new()
        .prefix("test_collection_rec")
        .tempdir()
        .unwrap();
    let collection_name = "test".to_string();
    let collection_name_rec = "test_rec".to_string();
    let mut shards = HashMap::new();
    shards.insert(0, HashSet::from([1]));
    shards.insert(1, HashSet::from([1]));
    shards.insert(2, HashSet::from([10_000])); // remote shard
    shards.insert(3, HashSet::from([1, 20_000, 30_000]));

    let mut collection = Collection::new(
        collection_name,
        1,
        collection_dir.path(),
        snapshots_path.path(),
        &config,
        CollectionShardDistribution { shards },
        ChannelService::default(),
        dummy_on_replica_failure(),
        dummy_request_shard_transfer(),
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
        1,
        recover_dir.path(),
        snapshots_path.path(),
        ChannelService::default(),
        dummy_on_replica_failure(),
        dummy_request_shard_transfer(),
    )
    .await;

    {
        let shards_holder = &recovered_collection.shards_holder.read().await;

        let replica_ser_0 = shards_holder.get_shard(&0).unwrap();
        assert!(replica_ser_0.is_local().await);
        let replica_ser_1 = shards_holder.get_shard(&1).unwrap();
        assert!(replica_ser_1.is_local().await);
        let replica_ser_2 = shards_holder.get_shard(&2).unwrap();
        assert!(!replica_ser_2.is_local().await);
        assert_eq!(replica_ser_2.peers().len(), 1);

        let replica_ser_3 = shards_holder.get_shard(&3).unwrap();

        assert!(replica_ser_3.is_local().await);
        assert_eq!(replica_ser_3.peers().len(), 3); // 2 remotes + 1 local
    }

    collection.before_drop().await;
    recovered_collection.before_drop().await;
}
