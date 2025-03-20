use std::collections::{HashMap, HashSet};
use std::num::NonZeroU32;
use std::sync::Arc;

use common::budget::ResourceBudget;
use segment::types::Distance;
use tempfile::Builder;

use crate::collection::{Collection, RequestShardTransfer};
use crate::config::{CollectionConfigInternal, CollectionParams, WalConfig};
use crate::operations::shared_storage_config::SharedStorageConfig;
use crate::operations::types::{NodeType, VectorsConfig};
use crate::operations::vector_params_builder::VectorParamsBuilder;
use crate::shards::channel_service::ChannelService;
use crate::shards::collection_shard_distribution::CollectionShardDistribution;
use crate::shards::replica_set::{AbortShardTransfer, ChangePeerFromState};
use crate::tests::fixtures::TEST_OPTIMIZERS_CONFIG;

pub fn dummy_on_replica_failure() -> ChangePeerFromState {
    Arc::new(move |_peer_id, _shard_id, _from_state| {})
}

pub fn dummy_request_shard_transfer() -> RequestShardTransfer {
    Arc::new(move |_transfer| {})
}

pub fn dummy_abort_shard_transfer() -> AbortShardTransfer {
    Arc::new(|_transfer, _reason| {})
}

fn init_logger() {
    let _ = env_logger::builder().is_test(true).try_init();
}

async fn _test_snapshot_collection(node_type: NodeType) {
    let wal_config = WalConfig {
        wal_capacity_mb: 1,
        wal_segments_ahead: 0,
    };

    let collection_params = CollectionParams {
        vectors: VectorsConfig::Single(VectorParamsBuilder::new(4, Distance::Dot).build()),
        shard_number: NonZeroU32::new(4).unwrap(),
        replication_factor: NonZeroU32::new(3).unwrap(),
        write_consistency_factor: NonZeroU32::new(2).unwrap(),
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
    };

    let snapshots_path = Builder::new().prefix("test_snapshots").tempdir().unwrap();
    let collection_dir = Builder::new().prefix("test_collection").tempdir().unwrap();

    let collection_name = "test".to_string();
    let collection_name_rec = "test_rec".to_string();
    let mut shards = HashMap::new();
    shards.insert(0, HashSet::from([1]));
    shards.insert(1, HashSet::from([1]));
    shards.insert(2, HashSet::from([10_000])); // remote shard
    shards.insert(3, HashSet::from([1, 20_000, 30_000]));

    let storage_config: SharedStorageConfig = SharedStorageConfig {
        node_type,
        ..Default::default()
    };

    let collection = Collection::new(
        collection_name,
        1,
        collection_dir.path(),
        snapshots_path.path(),
        &config,
        Arc::new(storage_config),
        CollectionShardDistribution { shards },
        None,
        ChannelService::default(),
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

    let snapshots_temp_dir = Builder::new().prefix("temp_dir").tempdir().unwrap();
    let snapshot_description = collection
        .create_snapshot(snapshots_temp_dir.path(), 0)
        .await
        .unwrap();

    assert_eq!(snapshot_description.checksum.unwrap().len(), 64);

    {
        let recover_dir = Builder::new()
            .prefix("test_collection_rec")
            .tempdir()
            .unwrap();
        // Do not recover in local mode if some shards are remote
        assert!(
            Collection::restore_snapshot(
                &snapshots_path.path().join(&snapshot_description.name),
                recover_dir.path(),
                0,
                false,
            )
            .is_err(),
        );
    }

    let recover_dir = Builder::new()
        .prefix("test_collection_rec")
        .tempdir()
        .unwrap();

    if let Err(err) = Collection::restore_snapshot(
        &snapshots_path.path().join(snapshot_description.name),
        recover_dir.path(),
        0,
        true,
    ) {
        panic!("Failed to restore snapshot: {err}")
    }

    let recovered_collection = Collection::load(
        collection_name_rec,
        1,
        recover_dir.path(),
        snapshots_path.path(),
        Default::default(),
        ChannelService::default(),
        dummy_on_replica_failure(),
        dummy_request_shard_transfer(),
        dummy_abort_shard_transfer(),
        None,
        None,
        ResourceBudget::default(),
        None,
    )
    .await;

    {
        let shards_holder = &recovered_collection.shards_holder.read().await;

        let replica_ser_0 = shards_holder.get_shard(0).unwrap();
        assert!(replica_ser_0.is_local().await);
        let replica_ser_1 = shards_holder.get_shard(1).unwrap();
        assert!(replica_ser_1.is_local().await);
        let replica_ser_2 = shards_holder.get_shard(2).unwrap();
        assert!(!replica_ser_2.is_local().await);
        assert_eq!(replica_ser_2.peers().len(), 1);

        let replica_ser_3 = shards_holder.get_shard(3).unwrap();

        assert!(replica_ser_3.is_local().await);
        assert_eq!(replica_ser_3.peers().len(), 3); // 2 remotes + 1 local
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_snapshot_collection_normal() {
    init_logger();
    _test_snapshot_collection(NodeType::Normal).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_snapshot_collection_listener() {
    init_logger();
    _test_snapshot_collection(NodeType::Listener).await;
}
