use std::sync::Arc;

use api::rest::SearchRequestInternal;
use collection::collection::Collection;
use collection::config::{CollectionConfigInternal, CollectionParams, WalConfig};
use collection::operations::CollectionUpdateOperations;
use collection::operations::point_ops::{
    PointInsertOperationsInternal, PointOperations, PointStructPersisted, VectorStructPersisted,
    WriteOrdering,
};
use collection::operations::shard_selector_internal::ShardSelectorInternal;
use collection::operations::shared_storage_config::SharedStorageConfig;
use collection::operations::types::{NodeType, VectorsConfig};
use collection::operations::vector_params_builder::VectorParamsBuilder;
use collection::shards::channel_service::ChannelService;
use collection::shards::collection_shard_distribution::CollectionShardDistribution;
use collection::shards::replica_set::ReplicaState;
use common::budget::ResourceBudget;
use common::counter::hardware_accumulator::HwMeasurementAcc;
use segment::types::{Distance, WithPayloadInterface, WithVector};
use tempfile::Builder;

use crate::common::{
    REST_PORT, TEST_OPTIMIZERS_CONFIG, dummy_abort_shard_transfer, dummy_on_replica_failure,
    dummy_request_shard_transfer,
};

async fn _test_snapshot_and_recover_collection(node_type: NodeType) {
    let wal_config = WalConfig {
        wal_capacity_mb: 1,
        wal_segments_ahead: 0,
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
    };

    let snapshots_path = Builder::new().prefix("test_snapshots").tempdir().unwrap();
    let collection_dir = Builder::new().prefix("test_collection").tempdir().unwrap();
    let recover_dir = Builder::new()
        .prefix("test_collection_rec")
        .tempdir()
        .unwrap();
    let collection_name = "test".to_string();
    let collection_name_rec = "test_rec".to_string();

    let storage_config: SharedStorageConfig = SharedStorageConfig {
        node_type,
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
        ChannelService::new(REST_PORT, None),
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

    let local_shards = collection.get_local_shards().await;
    for shard_id in local_shards {
        collection
            .set_shard_replica_state(shard_id, 0, ReplicaState::Active, None)
            .await
            .unwrap();
    }

    // Upload 1000 random vectors to the collection
    let mut points = Vec::new();
    for i in 0..100 {
        points.push(PointStructPersisted {
            id: i.into(),
            vector: VectorStructPersisted::Single(vec![i as f32, 0.0, 0.0, 0.0]),
            payload: Some(serde_json::from_str(r#"{"number": "John Doe"}"#).unwrap()),
        });
    }
    let insert_points = CollectionUpdateOperations::PointOperation(PointOperations::UpsertPoints(
        PointInsertOperationsInternal::PointsList(points),
    ));
    let hw_counter = HwMeasurementAcc::new();
    collection
        .update_from_client_simple(insert_points, true, WriteOrdering::default(), hw_counter)
        .await
        .unwrap();

    // Take a snapshot
    let snapshots_temp_dir = Builder::new().prefix("temp_dir").tempdir().unwrap();
    let snapshot_description = collection
        .create_snapshot(snapshots_temp_dir.path(), 0)
        .await
        .unwrap();

    if let Err(err) = Collection::restore_snapshot(
        &snapshots_path.path().join(snapshot_description.name),
        recover_dir.path(),
        0,
        false,
    ) {
        panic!("Failed to restore snapshot: {err}")
    }

    let recovered_collection = Collection::load(
        collection_name_rec,
        this_peer_id,
        recover_dir.path(),
        snapshots_path.path(),
        Default::default(),
        ChannelService::new(REST_PORT, None),
        dummy_on_replica_failure(),
        dummy_request_shard_transfer(),
        dummy_abort_shard_transfer(),
        None,
        None,
        ResourceBudget::default(),
        None,
    )
    .await;

    let query_vector = vec![1.0, 0.0, 0.0, 0.0];

    let full_search_request = SearchRequestInternal {
        vector: query_vector.clone().into(),
        filter: None,
        limit: 100,
        offset: None,
        with_payload: Some(WithPayloadInterface::Bool(true)),
        with_vector: Some(WithVector::Bool(true)),
        params: None,
        score_threshold: None,
    };

    let hw_acc = HwMeasurementAcc::new();
    let reference_result = collection
        .search(
            full_search_request.clone().into(),
            None,
            &ShardSelectorInternal::All,
            None,
            hw_acc,
        )
        .await
        .unwrap();

    let hw_acc = HwMeasurementAcc::new();
    let recovered_result = recovered_collection
        .search(
            full_search_request.into(),
            None,
            &ShardSelectorInternal::All,
            None,
            hw_acc,
        )
        .await
        .unwrap();

    assert_eq!(reference_result.len(), recovered_result.len());

    for (reference, recovered) in reference_result.iter().zip(recovered_result.iter()) {
        assert_eq!(reference.id, recovered.id);
        assert_eq!(reference.payload, recovered.payload);
        assert_eq!(reference.vector, recovered.vector);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_snapshot_and_recover_collection_normal() {
    _test_snapshot_and_recover_collection(NodeType::Normal).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_snapshot_and_recover_collection_listener() {
    _test_snapshot_and_recover_collection(NodeType::Listener).await;
}
