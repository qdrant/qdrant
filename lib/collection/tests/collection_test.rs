use std::collections::HashMap;
use std::sync::Arc;

use tempdir::TempDir;
use tokio::runtime::Handle;

use collection::collection_builder::collection_loader::load_collection;
use collection::operations::payload_ops::{PayloadOps, SetPayload};
use collection::operations::point_ops::{BatchPoints, PointOperations, PointStruct};
use collection::operations::types::{RecommendRequest, ScrollRequest, SearchRequest, UpdateStatus};
use collection::operations::CollectionUpdateOperations;
use segment::types::{
    PayloadInterface, PayloadKeyType, PayloadVariant, WithPayload, WithPayloadInterface,
};

use crate::common::simple_collection_fixture;
use collection::collection_manager::collection_managers::CollectionSearcher;
use collection::collection_manager::simple_collection_searcher::SimpleCollectionSearcher;

mod common;

#[tokio::test]
async fn test_collection_updater() {
    let collection_dir = TempDir::new("collection").unwrap();

    let collection = simple_collection_fixture(collection_dir.path()).await;

    let insert_points = CollectionUpdateOperations::PointOperation(
        BatchPoints {
            ids: vec![0, 1, 2, 3, 4],
            vectors: vec![
                vec![1.0, 0.0, 1.0, 1.0],
                vec![1.0, 0.0, 1.0, 0.0],
                vec![1.0, 1.0, 1.0, 1.0],
                vec![1.0, 1.0, 0.0, 1.0],
                vec![1.0, 0.0, 0.0, 0.0],
            ],
            payloads: None,
        }
        .into(),
    );

    let insert_result = collection.update(insert_points, true).await;

    match insert_result {
        Ok(res) => {
            assert_eq!(res.status, UpdateStatus::Completed)
        }
        Err(err) => panic!("operation failed: {:?}", err),
    }

    let search_request = SearchRequest {
        vector: vec![1.0, 1.0, 1.0, 1.0],
        with_payload: None,
        with_vector: None,
        filter: None,
        params: None,
        top: 3,
    };

    let segment_searcher = SimpleCollectionSearcher::new();
    let search_res = segment_searcher
        .search(
            collection.segments(),
            Arc::new(search_request),
            &Handle::current(),
        )
        .await;

    match search_res {
        Ok(res) => {
            assert_eq!(res.len(), 3);
            assert_eq!(res[0].id, 2);
            assert!(res[0].payload.is_none());
        }
        Err(err) => panic!("search failed: {:?}", err),
    }
}

#[tokio::test]
async fn test_collection_search_with_payload_and_vector() {
    let collection_dir = TempDir::new("collection").unwrap();

    let collection = simple_collection_fixture(collection_dir.path()).await;

    let insert_points = CollectionUpdateOperations::PointOperation(
        BatchPoints {
            ids: vec![0, 1],
            vectors: vec![vec![1.0, 0.0, 1.0, 1.0], vec![1.0, 0.0, 1.0, 0.0]],
            payloads: serde_json::from_str(
                r#"[{ "k": { "type": "keyword", "value": "v1" } }, { "k": "v2" , "v": "v3"}]"#,
            )
            .unwrap(),
        }
        .into(),
    );

    let insert_result = collection.update(insert_points, true).await;

    match insert_result {
        Ok(res) => {
            assert_eq!(res.status, UpdateStatus::Completed)
        }
        Err(err) => panic!("operation failed: {:?}", err),
    }

    let search_request = SearchRequest {
        vector: vec![1.0, 0.0, 1.0, 1.0],
        with_payload: Some(WithPayloadInterface::Bool(true)),
        with_vector: Some(true),
        filter: None,
        params: None,
        top: 3,
    };

    let segment_searcher = SimpleCollectionSearcher::new();
    let search_res = segment_searcher
        .search(
            collection.segments(),
            Arc::new(search_request),
            &Handle::current(),
        )
        .await;

    match search_res {
        Ok(res) => {
            assert_eq!(res.len(), 2);
            assert_eq!(res[0].id, 0);
            assert_eq!(res[0].payload.as_ref().unwrap().len(), 1);
            assert_eq!(&res[0].vector, &Some(vec![1.0, 0.0, 1.0, 1.0]));
        }
        Err(err) => panic!("search failed: {:?}", err),
    }
}

#[tokio::test]
async fn test_collection_loading() {
    let collection_dir = TempDir::new("collection").unwrap();

    {
        let collection = simple_collection_fixture(collection_dir.path()).await;
        let insert_points = CollectionUpdateOperations::PointOperation(
            BatchPoints {
                ids: vec![0, 1, 2, 3, 4],
                vectors: vec![
                    vec![1.0, 0.0, 1.0, 1.0],
                    vec![1.0, 0.0, 1.0, 0.0],
                    vec![1.0, 1.0, 1.0, 1.0],
                    vec![1.0, 1.0, 0.0, 1.0],
                    vec![1.0, 0.0, 0.0, 0.0],
                ],
                payloads: None,
            }
            .into(),
        );

        collection.update(insert_points, true).await.unwrap();

        let mut payload: HashMap<PayloadKeyType, PayloadInterface> = Default::default();

        payload.insert(
            "color".to_string(),
            PayloadInterface::KeywordShortcut(PayloadVariant::Value("red".to_string())),
        );

        let assign_payload =
            CollectionUpdateOperations::PayloadOperation(PayloadOps::SetPayload(SetPayload {
                payload,
                points: vec![2, 3],
            }));

        collection.update(assign_payload, true).await.unwrap();
    }

    let loaded_collection = load_collection(collection_dir.path());
    let segment_searcher = SimpleCollectionSearcher::new();
    let retrieved = segment_searcher
        .retrieve(
            loaded_collection.segments(),
            &[1, 2],
            &WithPayload::from(true),
            true,
        )
        .await
        .unwrap();

    assert_eq!(retrieved.len(), 2);

    for record in retrieved {
        if record.id == 2 {
            let non_empty_payload = record.payload.unwrap();

            assert_eq!(non_empty_payload.len(), 1)
        }
    }
}

#[test]
fn test_deserialization() {
    let insert_points = CollectionUpdateOperations::PointOperation(
        BatchPoints {
            ids: vec![0, 1],
            vectors: vec![vec![1.0, 0.0, 1.0, 1.0], vec![1.0, 0.0, 1.0, 0.0]],
            payloads: None,
        }
        .into(),
    );
    let json_str = serde_json::to_string_pretty(&insert_points).unwrap();

    let _read_obj: CollectionUpdateOperations = serde_json::from_str(&json_str).unwrap();

    let crob_bytes = rmp_serde::to_vec(&insert_points).unwrap();

    let _read_obj2: CollectionUpdateOperations = rmp_serde::from_read_ref(&crob_bytes).unwrap();
}

#[test]
fn test_deserialization2() {
    let insert_points = CollectionUpdateOperations::PointOperation(
        vec![
            PointStruct {
                id: 0,
                vector: vec![1.0, 0.0, 1.0, 1.0],
                payload: None,
            },
            PointStruct {
                id: 1,
                vector: vec![1.0, 0.0, 1.0, 0.0],
                payload: None,
            },
        ]
        .into(),
    );

    let json_str = serde_json::to_string_pretty(&insert_points).unwrap();

    let _read_obj: CollectionUpdateOperations = serde_json::from_str(&json_str).unwrap();

    let raw_bytes = rmp_serde::to_vec(&insert_points).unwrap();

    let _read_obj2: CollectionUpdateOperations = rmp_serde::from_read_ref(&raw_bytes).unwrap();
}

#[tokio::test]
async fn test_recommendation_api() {
    let collection_dir = TempDir::new("collection").unwrap();
    let collection = simple_collection_fixture(collection_dir.path()).await;

    let insert_points = CollectionUpdateOperations::PointOperation(
        BatchPoints {
            ids: vec![0, 1, 2, 3, 4, 5, 6, 7, 8],
            vectors: vec![
                vec![0.0, 0.0, 1.0, 1.0],
                vec![1.0, 0.0, 0.0, 0.0],
                vec![1.0, 0.0, 0.0, 0.0],
                vec![0.0, 1.0, 0.0, 0.0],
                vec![0.0, 1.0, 0.0, 0.0],
                vec![0.0, 0.0, 1.0, 0.0],
                vec![0.0, 0.0, 1.0, 0.0],
                vec![0.0, 0.0, 0.0, 1.0],
                vec![0.0, 0.0, 0.0, 1.0],
            ],
            payloads: None,
        }
        .into(),
    );

    collection.update(insert_points, true).await.unwrap();
    let segment_searcher = SimpleCollectionSearcher::new();
    let result = collection
        .recommend_by(
            Arc::new(RecommendRequest {
                positive: vec![0],
                negative: vec![8],
                filter: None,
                params: None,
                top: 5,
            }),
            &segment_searcher,
            &Handle::current(),
        )
        .await
        .unwrap();
    assert!(!result.is_empty());
    let top1 = &result[0];

    assert!(top1.id == 5 || top1.id == 6);
}

#[tokio::test]
async fn test_read_api() {
    let collection_dir = TempDir::new("collection").unwrap();
    let collection = simple_collection_fixture(collection_dir.path()).await;

    let insert_points = CollectionUpdateOperations::PointOperation(PointOperations::UpsertPoints(
        BatchPoints {
            ids: vec![0, 1, 2, 3, 4, 5, 6, 7, 8],
            vectors: vec![
                vec![0.0, 0.0, 1.0, 1.0],
                vec![1.0, 0.0, 0.0, 0.0],
                vec![1.0, 0.0, 0.0, 0.0],
                vec![0.0, 1.0, 0.0, 0.0],
                vec![0.0, 1.0, 0.0, 0.0],
                vec![0.0, 0.0, 1.0, 0.0],
                vec![0.0, 0.0, 1.0, 0.0],
                vec![0.0, 0.0, 0.0, 1.0],
                vec![0.0, 0.0, 0.0, 1.0],
            ],
            payloads: None,
        }
        .into(),
    ));

    collection.update(insert_points, true).await.unwrap();

    let segment_searcher = SimpleCollectionSearcher::new();
    let result = collection
        .scroll_by(
            ScrollRequest {
                offset: Some(0),
                limit: Some(2),
                filter: None,
                with_payload: Some(WithPayloadInterface::Bool(true)),
                with_vector: None,
            },
            &segment_searcher,
        )
        .await
        .unwrap();

    assert_eq!(result.next_page_offset, Some(2));
    assert_eq!(result.points.len(), 2);
}
