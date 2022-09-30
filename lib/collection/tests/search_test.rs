use collection::operations::point_ops::{PointInsertOperations, PointOperations, PointStruct};
use collection::operations::types::SearchRequest;
use collection::operations::CollectionUpdateOperations;
use segment::types::WithPayloadInterface;
use tempfile::Builder;
use tokio::runtime::Handle;

use crate::common::{simple_collection_fixture, N_SHARDS};

mod common;

#[tokio::test]
async fn test_collection_search() {
    test_collection_search_limit_with_shards(1).await;
    test_collection_search_limit_with_shards(N_SHARDS).await;
}

// test that `limit:0` is equivalent to `limit:$total_point_count`
async fn test_collection_search_limit_with_shards(shard_number: u32) {
    let collection_dir = Builder::new()
        .prefix("test_collection_paginated_search")
        .tempdir()
        .unwrap();

    let mut collection = simple_collection_fixture(collection_dir.path(), shard_number).await;

    // Upload 1000 random vectors to the collection
    let mut points = Vec::new();
    let total_points: usize = 1000;
    for i in 0..total_points {
        points.push(PointStruct {
            id: (i as u64).into(),
            vector: vec![i as f32, 0.0, 0.0, 0.0].into(),
            payload: Some(serde_json::from_str(r#"{"number": "John Doe"}"#).unwrap()),
        });
    }
    let insert_points = CollectionUpdateOperations::PointOperation(PointOperations::UpsertPoints(
        PointInsertOperations::PointsList(points),
    ));
    collection
        .update_from_client(insert_points, true)
        .await
        .unwrap();

    let query_vector = vec![1.0, 0.0, 0.0, 0.0];

    let search_request_with_limit = SearchRequest {
        vector: query_vector.clone().into(),
        filter: None,
        limit: total_points as usize,
        offset: 0,
        with_payload: Some(WithPayloadInterface::Bool(true)),
        with_vector: None,
        params: None,
        score_threshold: None,
    };

    let with_limit_result = collection
        .search(search_request_with_limit, &Handle::current(), None)
        .await
        .unwrap();

    assert_eq!(with_limit_result.len(), total_points);
    assert_eq!(with_limit_result[0].id, (total_points as u64 - 1).into());

    let search_request_without_limit = SearchRequest {
        vector: query_vector.clone().into(),
        filter: None,
        limit: 0,
        offset: 0,
        with_payload: Some(WithPayloadInterface::Bool(true)),
        with_vector: None,
        params: None,
        score_threshold: None,
    };

    let without_limit_result = collection
        .search(search_request_without_limit, &Handle::current(), None)
        .await
        .unwrap();

    assert_eq!(without_limit_result.len(), total_points);
    assert_eq!(with_limit_result[0].id, (total_points as u64 - 1).into());

    // equivalent result at all positions
    assert_eq!(with_limit_result, without_limit_result);

    collection.before_drop().await;
}
