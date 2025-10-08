use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use collection::collection::Collection;
use collection::config::{CollectionConfigInternal, CollectionParams};
use collection::operations::CollectionUpdateOperations;
use collection::operations::point_ops::{
    PointInsertOperationsInternal, PointOperations, PointStructPersisted, VectorStructPersisted,
    WriteOrdering,
};
use collection::operations::shard_selector_internal::ShardSelectorInternal;
use collection::operations::shared_storage_config::SharedStorageConfig;
use collection::operations::types::{
    CollectionResult, NodeType, PointRequestInternal, UpdateStatus, VectorsConfig,
};
use collection::operations::vector_params_builder::VectorParamsBuilder;
use collection::shards::channel_service::ChannelService;
use collection::shards::collection_shard_distribution::CollectionShardDistribution;
use collection::shards::replica_set::ReplicaState;
use common::budget::ResourceBudget;
use common::counter::hardware_accumulator::HwMeasurementAcc;
use common::flags::{FeatureFlags, init_feature_flags};
use segment::payload_json;
use segment::types::{Distance, WithVector};
use shard::operations::payload_ops::{PayloadOps, SetPayloadOp};
use tempfile::Builder;
use tokio::time::sleep;

use crate::common::{
    REST_PORT, TEST_OPTIMIZERS_CONFIG, dummy_abort_shard_transfer, dummy_on_replica_failure,
    dummy_request_shard_transfer,
};

// RUST_LOG=trace cargo nextest run --all continuous --nocapture
#[tokio::test(flavor = "multi_thread")]
async fn test_continuous_snapshot() {
    // Initialize logger for tests
    let _ = env_logger::builder().is_test(true).try_init();
    // Feature flags
    init_feature_flags(FeatureFlags::default());

    let collection_params = CollectionParams {
        vectors: VectorsConfig::Single(VectorParamsBuilder::new(4, Distance::Dot).build()),
        ..CollectionParams::empty()
    };

    let config = CollectionConfigInternal {
        params: collection_params,
        optimizer_config: TEST_OPTIMIZERS_CONFIG.clone(),
        wal_config: Default::default(),
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

    let collection = Arc::new(collection);
    let stop_flag = Arc::new(AtomicBool::new(false));

    // Continuously insert the same point
    let points_count = 3;
    let points_task = {
        let collection = Arc::clone(&collection);
        let stop_flag = Arc::clone(&stop_flag);
        tokio::spawn(async move {
            while !stop_flag.load(Ordering::Relaxed) {
                // Delete all points
                let delete_points =
                    CollectionUpdateOperations::PointOperation(PointOperations::DeletePoints {
                        ids: (0..points_count).map(|i| i.into()).collect(),
                    });
                let hw_counter = HwMeasurementAcc::disposable();
                collection
                    .update_from_client_simple(
                        delete_points,
                        true,
                        WriteOrdering::default(),
                        hw_counter,
                    )
                    .await?;

                // Insert one point at a time
                for i in 0..points_count {
                    let point = PointStructPersisted {
                        id: i.into(),
                        vector: VectorStructPersisted::Single(vec![i as f32, 0.0, 0.0, 0.0]),
                        payload: Some(serde_json::from_str(r#"{"number": "John Doe"}"#).unwrap()),
                    };
                    let insert_points =
                        CollectionUpdateOperations::PointOperation(PointOperations::UpsertPoints(
                            PointInsertOperationsInternal::PointsList(vec![point]),
                        ));
                    let hw_counter = HwMeasurementAcc::disposable();
                    let insert = collection
                        .update_from_client_simple(
                            insert_points,
                            true,
                            WriteOrdering::default(),
                            hw_counter,
                        )
                        .await?;
                    assert_eq!(insert.status, UpdateStatus::Completed);
                }

                // Retrieve one point at a time
                for i in 0..points_count {
                    let retrieve_point = PointRequestInternal {
                        ids: vec![i.into()],
                        with_payload: None,
                        with_vector: WithVector::Bool(false),
                    };
                    let hw_counter = HwMeasurementAcc::disposable();
                    let retrieve_result = collection
                        .retrieve(
                            retrieve_point,
                            None,
                            &ShardSelectorInternal::All,
                            None,
                            hw_counter,
                        )
                        .await?;
                    assert_eq!(retrieve_result.len(), 1);
                }

                // Set payload one point at a time
                for i in 0..points_count {
                    let set_payload = CollectionUpdateOperations::PayloadOperation(
                        PayloadOps::SetPayload(SetPayloadOp {
                            payload: payload_json! {
                                "city": "London",
                                "color": "green",
                            },
                            points: Some(vec![i.into()]),
                            filter: None,
                            key: None,
                        }),
                    );
                    let hw_counter = HwMeasurementAcc::disposable();
                    let set_result = collection
                        .update_from_client_simple(
                            set_payload,
                            true,
                            WriteOrdering::default(),
                            hw_counter,
                        )
                        .await?;
                    assert_eq!(set_result.status, UpdateStatus::Completed);
                }

                // Retrieve one point at a time again with payload & vector
                for i in 0..points_count {
                    let retrieve_point = PointRequestInternal {
                        ids: vec![i.into()],
                        with_payload: Some(true.into()),
                        with_vector: WithVector::Bool(true),
                    };
                    let hw_counter = HwMeasurementAcc::disposable();
                    let retrieve_result = collection
                        .retrieve(
                            retrieve_point,
                            None,
                            &ShardSelectorInternal::All,
                            None,
                            hw_counter,
                        )
                        .await?;
                    assert_eq!(retrieve_result.len(), 1);
                    assert!(retrieve_result[0].vector.is_some(), "missing vector");
                    assert!(retrieve_result[0].payload.is_some(), "missing payload");
                }
            }
            CollectionResult::Ok(())
        })
    };

    // Loop taking snapshots and deletions of snapshots
    let snapshot_task = {
        let collection = Arc::clone(&collection);
        let stop_flag = Arc::clone(&stop_flag);
        let snapshots_temp_dir = Builder::new().prefix("temp_dir").tempdir().unwrap();
        tokio::spawn(async move {
            while !stop_flag.load(Ordering::Relaxed) {
                // Take snapshot
                let _snapshot = collection
                    .create_snapshot(snapshots_temp_dir.path(), 0)
                    .await?;
            }
            CollectionResult::Ok(())
        })
    };

    let timeout = sleep(Duration::from_secs(20));
    tokio::pin!(timeout);

    tokio::select! {
        res = points_task => {
            stop_flag.store(true, Ordering::Relaxed);
            match res {
                Ok(Ok(())) => {},
                Ok(Err(e)) => panic!("points_task error: {e}"),
                Err(e) => panic!("points_task panicked: {e}"),
            }
        }
        res = snapshot_task => {
            stop_flag.store(true, Ordering::Relaxed);
            match res {
                Ok(Ok(())) => {},
                Ok(Err(e)) => panic!("snapshot_task error: {e}"),
                Err(e) => panic!("snapshot_task panicked: {e}"),
            }
        }
        _ = &mut timeout => {
            stop_flag.store(true, Ordering::Relaxed);
            log::info!("Timeout reached, stopping test");
        }
    }
}
