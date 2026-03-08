use std::sync::Arc;

use collection::collection::Collection;
use collection::config::{CollectionConfigInternal, CollectionParams, WalConfig};
use collection::operations::shared_storage_config::SharedStorageConfig;
use collection::operations::types::{NodeType, VectorsConfig};
use collection::operations::vector_params_builder::VectorParamsBuilder;
use collection::shards::channel_service::ChannelService;
use collection::shards::collection_shard_distribution::CollectionShardDistribution;
use collection::shards::replica_set::replica_set_state::ReplicaState;
use common::budget::ResourceBudget;
use segment::types::Distance;
use tempfile::Builder;

use crate::common::{
    REST_PORT, TEST_OPTIMIZERS_CONFIG, dummy_abort_shard_transfer, dummy_on_replica_failure,
    dummy_request_shard_transfer,
};

#[tokio::test(flavor = "multi_thread")]
async fn test_create_shard_snapshot_cleanup() {
    let wal_config = WalConfig {
        wal_capacity_mb: 1,
        wal_segments_ahead: 0,
        wal_retain_closed: 1,
    };

    let collection_params = CollectionParams {
        vectors: VectorsConfig::Single(VectorParamsBuilder::new(4, Distance::Dot).build()),
        ..CollectionParams::empty()
    };

    let config = CollectionConfigInternal {
        params: collection_params,
        optimizer_config: TEST_OPTIMIZERS_CONFIG.clone(),
        wal_config,
        hnsw_config: Default::default(),
        quantization_config: Default::default(),
        strict_mode_config: Default::default(),
        uuid: None,
        metadata: None,
    };

    let snapshots_path = Builder::new().prefix("test_snapshots").tempdir().unwrap();
    let collection_dir = Builder::new().prefix("test_collection").tempdir().unwrap();
    let collection_name = "test".to_string();

    let storage_config: SharedStorageConfig = SharedStorageConfig {
        node_type: NodeType::Normal,
        ..Default::default()
    };

    let this_peer_id = 0;
    let shard_distribution = CollectionShardDistribution::all_local(
        Some(config.params.shard_number.into()),
        this_peer_id,
    );

    let collection = Collection::new(
        collection_name,
        this_peer_id,
        collection_dir.path(),
        snapshots_path.path(),
        &config,
        Arc::new(storage_config),
        shard_distribution,
        None,
        ChannelService::new(REST_PORT, false, None, None),
        dummy_on_replica_failure(),
        dummy_request_shard_transfer(),
        dummy_abort_shard_transfer(),
        None,
        None,
        ResourceBudget::default(),
        None,
    )
    .await
    .unwrap();

    let shard_id = 0;
    collection
        .set_shard_replica_state(shard_id, 0, ReplicaState::Active, None)
        .await
        .unwrap();

    // Create a snapshot but drop the guard
    let temp_dir = Builder::new().prefix("temp_dir").tempdir().unwrap();

    let (_desc, temp_paths) = collection
        .create_shard_snapshot(shard_id, temp_dir.path())
        .await
        .unwrap();

    let mut file_paths = Vec::new();
    for temp_path in &temp_paths {
        let path = temp_path.to_path_buf();
        assert!(path.exists());
        file_paths.push(path);
    }

    // Drop the guards - all files should be deleted
    drop(temp_paths);

    for path in file_paths {
        assert!(
            !path.exists(),
            "File {:?} should be deleted on guard drop",
            path
        );
    }

    // Check snapshots directory is also empty
    let shard_snapshots_path = snapshots_path.path().join(format!("shards/{}", shard_id));
    if shard_snapshots_path.exists() {
        let entries = std::fs::read_dir(shard_snapshots_path).unwrap();
        let count = entries.count();
        assert_eq!(
            count, 0,
            "Shards snapshots directory should be empty after drop"
        );
    }
}
