mod common;

use collection::operations::CollectionUpdateOperations;
use collection::operations::point_ops::{PointOperations, PointStruct};

use crate::common::{simple_collection_fixture};
use collection::operations::types::{UpdateStatus, SearchRequest, RecommendRequest, ScrollRequest};
use std::sync::Arc;
use collection::operations::payload_ops::PayloadOps;
use std::collections::HashMap;
use segment::types::{PayloadKeyType, PayloadVariant, PayloadInterface};
use collection::collection_builder::collection_loader::load_collection;
use tempdir::TempDir;
use tokio::runtime;
use collection::operations::point_ops::PointInsertOperations::{BatchPoints, PointsList};


#[test]
fn test_collection_updater() {
    let collection_dir = TempDir::new("collection").unwrap();

    let (_rt, collection) = simple_collection_fixture(collection_dir.path());

    let insert_points = CollectionUpdateOperations::PointOperation(
        PointOperations::UpsertPoints(BatchPoints {
            ids: vec![0, 1, 2, 3, 4],
            vectors: vec![
                vec![1.0, 0.0, 1.0, 1.0],
                vec![1.0, 0.0, 1.0, 0.0],
                vec![1.0, 1.0, 1.0, 1.0],
                vec![1.0, 1.0, 0.0, 1.0],
                vec![1.0, 0.0, 0.0, 0.0],
            ],
            payloads: None,
        })
    );

    let insert_result = collection.update(insert_points, true);

    match insert_result {
        Ok(res) => {
            assert_eq!(res.status, UpdateStatus::Completed)
        }
        Err(err) => assert!(false, "operation failed: {:?}", err),
    }

    let search_request = Arc::new(SearchRequest {
        vector: vec![1.0, 1.0, 1.0, 1.0],
        filter: None,
        params: None,
        top: 3,
    });

    let search_res = collection.search(search_request);


    match search_res {
        Ok(res) => {
            assert_eq!(res.len(), 3);
            assert_eq!(res[0].id, 2);
        }
        Err(err) => assert!(false, "search failed: {:?}", err),
    }
}


#[test]
fn test_collection_loading() {
    let collection_dir = TempDir::new("collection").unwrap();

    {
        let (_rt, collection) = simple_collection_fixture(collection_dir.path());

        let insert_points = CollectionUpdateOperations::PointOperation(
            PointOperations::UpsertPoints(BatchPoints {
                ids: vec![0, 1, 2, 3, 4],
                vectors: vec![
                    vec![1.0, 0.0, 1.0, 1.0],
                    vec![1.0, 0.0, 1.0, 0.0],
                    vec![1.0, 1.0, 1.0, 1.0],
                    vec![1.0, 1.0, 0.0, 1.0],
                    vec![1.0, 0.0, 0.0, 0.0],
                ],
                payloads: None,
            })
        );

        collection.update(insert_points, true).unwrap();

        let mut payload: HashMap<PayloadKeyType, PayloadInterface> = Default::default();

        payload.insert(
            "color".to_string(),
            PayloadInterface::KeywordShortcut(PayloadVariant::Value("red".to_string())),
        );

        let assign_payload = CollectionUpdateOperations::PayloadOperation(
            PayloadOps::SetPayload {
                payload,
                points: vec![2, 3],
            }
        );

        collection.update(assign_payload, true).unwrap();
    }

    let rt = runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .build().unwrap();


    // sleep(Duration::from_secs(120));

    let loaded_collection = load_collection(
        collection_dir.path(),
        rt.handle().clone(),
    );

    let retrieved = loaded_collection.retrieve(&vec![1, 2], true, true).unwrap();

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
        PointOperations::UpsertPoints(BatchPoints {
            ids: vec![0, 1],
            vectors: vec![
                vec![1.0, 0.0, 1.0, 1.0],
                vec![1.0, 0.0, 1.0, 0.0],
            ],
            payloads: None,
        })
    );

    let json_str = serde_json::to_string_pretty(&insert_points).unwrap();

    eprintln!("&json_str = {}", &json_str);

    let read_obj: CollectionUpdateOperations = serde_json::from_str(json_str.as_str()).unwrap();

    eprintln!("read_obj = {:#?}", read_obj);


    let crob_bytes = rmp_serde::to_vec(&insert_points).unwrap();

    let read_obj2: CollectionUpdateOperations = rmp_serde::from_read_ref(&crob_bytes).unwrap();

    eprintln!("read_obj2 = {:#?}", read_obj2);
}


#[test]
fn test_deserialization2() {
    let insert_points = CollectionUpdateOperations::PointOperation(
        PointOperations::UpsertPoints(PointsList(vec![
            PointStruct {
                id: 0,
                vector: vec![1.0, 0.0, 1.0, 1.0],
                payload: None,
            },
            PointStruct {
                id: 1,
                vector: vec![1.0, 0.0, 1.0, 0.0],
                payload: None,
            }
        ]))
    );

    let json_str = serde_json::to_string_pretty(&insert_points).unwrap();

    eprintln!("&json_str = {}", &json_str);

    let read_obj: CollectionUpdateOperations = serde_json::from_str(json_str.as_str()).unwrap();

    eprintln!("read_obj = {:#?}", read_obj);


    let raw_bytes = rmp_serde::to_vec(&insert_points).unwrap();

    let read_obj2: CollectionUpdateOperations = rmp_serde::from_read_ref(&raw_bytes).unwrap();

    eprintln!("read_obj2 = {:#?}", read_obj2);
}


#[test]
fn test_recommendation_api() {
    let collection_dir = TempDir::new("collection").unwrap();
    let (_rt, collection) = simple_collection_fixture(collection_dir.path());

    let insert_points = CollectionUpdateOperations::PointOperation(
        PointOperations::UpsertPoints(BatchPoints {
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
        })
    );

    collection.update(insert_points, true).unwrap();

    let result = collection.recommend(Arc::new(RecommendRequest {
        positive: vec![0],
        negative: vec![8],
        filter: None,
        params: None,
        top: 5
    })).unwrap();
    assert!(result.len() > 0);
    let top1 = result[0];

    assert!(top1.id == 5 || top1.id == 6);
}

#[test]
fn test_read_api() {
    let collection_dir = TempDir::new("collection").unwrap();
    let (_rt, collection) = simple_collection_fixture(collection_dir.path());

    let insert_points = CollectionUpdateOperations::PointOperation(
        PointOperations::UpsertPoints(BatchPoints {
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
        })
    );

    collection.update(insert_points, true).unwrap();

    let result = collection.scroll(Arc::new(ScrollRequest {
        offset: Some(0),
        limit: Some(2),
        filter: None,
        with_payload: Some(true),
        with_vector: None
    })).unwrap();


    assert_eq!(result.next_page_offset, Some(2));
    assert_eq!(result.points.len(), 2);
}