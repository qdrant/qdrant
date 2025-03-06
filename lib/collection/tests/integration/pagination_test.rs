use api::rest::SearchRequestInternal;
use collection::operations::CollectionUpdateOperations;
use collection::operations::point_ops::{
    PointInsertOperationsInternal, PointOperations, PointStructPersisted, VectorStructPersisted,
    WriteOrdering,
};
use collection::operations::shard_selector_internal::ShardSelectorInternal;
use common::counter::hardware_accumulator::HwMeasurementAcc;
use segment::types::WithPayloadInterface;
use tempfile::Builder;

use crate::common::{N_SHARDS, simple_collection_fixture};

#[tokio::test(flavor = "multi_thread")]
async fn test_collection_paginated_search() {
    test_collection_paginated_search_with_shards(1).await;
    test_collection_paginated_search_with_shards(N_SHARDS).await;
}

async fn test_collection_paginated_search_with_shards(shard_number: u32) {
    let collection_dir = Builder::new()
        .prefix("test_collection_paginated_search")
        .tempdir()
        .unwrap();

    let collection = simple_collection_fixture(collection_dir.path(), shard_number).await;

    // Upload 1000 random vectors to the collection
    let mut points = Vec::new();
    for i in 0..1000 {
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

    let query_vector = vec![1.0, 0.0, 0.0, 0.0];

    let full_search_request = SearchRequestInternal {
        vector: query_vector.clone().into(),
        filter: None,
        limit: 100,
        offset: Some(0),
        with_payload: Some(WithPayloadInterface::Bool(true)),
        with_vector: None,
        params: None,
        score_threshold: None,
    };

    let hw_acc = HwMeasurementAcc::new();
    let reference_result = collection
        .search(
            full_search_request.into(),
            None,
            &ShardSelectorInternal::All,
            None,
            hw_acc,
        )
        .await
        .unwrap();

    assert_eq!(reference_result.len(), 100);
    assert_eq!(reference_result[0].id, 999.into());

    let page_size = 10;

    let page_1_request = SearchRequestInternal {
        vector: query_vector.clone().into(),
        filter: None,
        limit: 10,
        offset: Some(page_size),
        with_payload: Some(WithPayloadInterface::Bool(true)),
        with_vector: None,
        params: None,
        score_threshold: None,
    };

    let hw_acc = HwMeasurementAcc::new();
    let page_1_result = collection
        .search(
            page_1_request.into(),
            None,
            &ShardSelectorInternal::All,
            None,
            hw_acc,
        )
        .await
        .unwrap();

    // Check that the first page is the same as the reference result
    assert_eq!(page_1_result.len(), 10);
    for i in 0..10 {
        assert_eq!(page_1_result[i], reference_result[page_size + i]);
    }

    let page_9_request = SearchRequestInternal {
        vector: query_vector.into(),
        filter: None,
        limit: 10,
        offset: Some(page_size * 9),
        with_payload: Some(WithPayloadInterface::Bool(true)),
        with_vector: None,
        params: None,
        score_threshold: None,
    };

    let hw_acc = HwMeasurementAcc::new();
    let page_9_result = collection
        .search(
            page_9_request.into(),
            None,
            &ShardSelectorInternal::All,
            None,
            hw_acc,
        )
        .await
        .unwrap();

    // Check that the 9th page is the same as the reference result
    assert_eq!(page_9_result.len(), 10);
    for i in 0..10 {
        assert_eq!(page_9_result[i], reference_result[page_size * 9 + i]);
    }
}
