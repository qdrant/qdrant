use api::rest::SearchRequestInternal;
use collection::operations::CollectionUpdateOperations;
use collection::operations::point_ops::{
    BatchPersisted, BatchVectorStructPersisted, PointInsertOperationsInternal, PointOperations,
};
use collection::operations::shard_selector_internal::ShardSelectorInternal;
use collection::operations::types::{CollectionError, CountRequestInternal};
use common::counter::hardware_accumulator::HwMeasurementAcc;
use tempfile::Builder;

use crate::common::{N_SHARDS, load_local_collection_read_only, simple_collection_fixture};

#[tokio::test(flavor = "multi_thread")]
async fn test_read_only_mode() {
    // Setup: Create and populate collection with multiple shards
    let collection_dir = Builder::new().prefix("read_only").tempdir().unwrap();
    let collection = simple_collection_fixture(collection_dir.path(), N_SHARDS).await;

    let batch = BatchPersisted {
        ids: vec![1, 2, 3, 4, 5].into_iter().map(|x| x.into()).collect(),
        vectors: BatchVectorStructPersisted::Single(vec![
            vec![1.0, 0.0, 1.0, 1.0],
            vec![1.0, 0.0, 1.0, 0.0],
            vec![1.0, 1.0, 1.0, 1.0],
            vec![1.0, 1.0, 0.0, 1.0],
            vec![1.0, 0.0, 0.0, 0.0],
        ]),
        payloads: None,
    };

    collection
        .update_from_client_simple(
            CollectionUpdateOperations::PointOperation(PointOperations::UpsertPoints(
                PointInsertOperationsInternal::from(batch),
            )),
            true,
            Default::default(),
            HwMeasurementAcc::new(),
        )
        .await
        .expect("Initial data insertion should succeed");

    collection.trigger_optimizers().await;
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    drop(collection);

    // Reload in read-only mode
    let collection = load_local_collection_read_only(
        "test".to_string(),
        collection_dir.path(),
        &collection_dir.path().join("snapshots"),
    )
    .await;

    // Test 1: Search works
    let search_result = collection
        .search(
            SearchRequestInternal {
                vector: vec![1.0, 1.0, 1.0, 1.0].into(),
                with_payload: None,
                with_vector: None,
                filter: None,
                params: None,
                limit: 3,
                offset: None,
                score_threshold: None,
            }
            .into(),
            None,
            &ShardSelectorInternal::All,
            None,
            HwMeasurementAcc::new(),
        )
        .await
        .expect("Search should work in read-only mode");
    assert_eq!(search_result.len(), 3);

    // Test 2: Count works across shards
    let count_result = collection
        .count(
            CountRequestInternal {
                filter: None,
                exact: true,
            }
            .into(),
            None,
            &ShardSelectorInternal::All,
            None,
            HwMeasurementAcc::new(),
        )
        .await
        .expect("Count should work in read-only mode");
    assert_eq!(count_result.count, 5);

    // Test 3: Upsert fails with correct error
    let upsert_result = collection
        .update_from_client_simple(
            CollectionUpdateOperations::PointOperation(PointOperations::UpsertPoints(
                PointInsertOperationsInternal::from(BatchPersisted {
                    ids: vec![100.into()],
                    vectors: BatchVectorStructPersisted::Single(vec![vec![0.0; 4]]),
                    payloads: None,
                }),
            )),
            true,
            Default::default(),
            HwMeasurementAcc::new(),
        )
        .await;
    assert!(matches!(
        upsert_result,
        Err(CollectionError::ServiceError { .. })
    ));

    // Test 4: Delete fails
    let delete_result = collection
        .update_from_client_simple(
            CollectionUpdateOperations::PointOperation(PointOperations::DeletePoints {
                ids: vec![1.into()],
            }),
            true,
            Default::default(),
            HwMeasurementAcc::new(),
        )
        .await;
    assert!(matches!(
        delete_result,
        Err(CollectionError::ServiceError { .. })
    ));

    // Test 5: Data remains unchanged after failed operations
    let final_count = collection
        .count(
            CountRequestInternal {
                filter: None,
                exact: true,
            }
            .into(),
            None,
            &ShardSelectorInternal::All,
            None,
            HwMeasurementAcc::new(),
        )
        .await
        .expect("Count should still work");
    assert_eq!(final_count.count, 5, "Data should remain unchanged");
}
