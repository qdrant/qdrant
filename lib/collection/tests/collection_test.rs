mod common;

use collection::operations::CollectionUpdateOperations;
use collection::operations::point_ops::PointOps;

use crate::common::simple_collection_fixture;
use collection::operations::types::{UpdateStatus, SearchRequest};
use std::sync::Arc;
use collection::operations::payload_ops::{PayloadOps, PayloadInterface, PayloadVariant};
use std::collections::HashMap;
use segment::types::PayloadKeyType;
use collection::collection_builder::collection_loader::load_collection;
use wal::WalOptions;
use tokio::runtime::Runtime;
use std::thread::sleep;
use std::time::Duration;
use tempdir::TempDir;
use tokio::runtime;


#[test]
fn test_collection_updater() {
    let collection_dir = TempDir::new("collection").unwrap();

    let (_rt, collection) = simple_collection_fixture(collection_dir.path());

    let insert_points = CollectionUpdateOperations::PointOperation(
        PointOps::UpsertPoints {
            ids: vec![0, 1, 2, 3, 4],
            vectors: vec![
                vec![1.0, 0.0, 1.0, 1.0],
                vec![1.0, 0.0, 1.0, 0.0],
                vec![1.0, 1.0, 1.0, 1.0],
                vec![1.0, 1.0, 0.0, 1.0],
                vec![1.0, 0.0, 0.0, 0.0],
            ],
        }
    );

    let insert_result = collection.update(insert_points, true);

    match insert_result {
        Ok(res) => {
            assert_eq!(res.status, UpdateStatus::Completed)
        }
        Err(err) => assert!(false, format!("operation failed: {:?}", err)),
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
            assert_eq!(res[0].idx, 2);
        }
        Err(err) => assert!(false, format!("search failed: {:?}", err)),
    }
}


#[test]
fn test_collection_loading() {
    let collection_dir = TempDir::new("collection").unwrap();

    {
        let (rt, collection) = simple_collection_fixture(collection_dir.path());

        let insert_points = CollectionUpdateOperations::PointOperation(
            PointOps::UpsertPoints {
                ids: vec![0, 1, 2, 3, 4],
                vectors: vec![
                    vec![1.0, 0.0, 1.0, 1.0],
                    vec![1.0, 0.0, 1.0, 0.0],
                    vec![1.0, 1.0, 1.0, 1.0],
                    vec![1.0, 1.0, 0.0, 1.0],
                    vec![1.0, 0.0, 0.0, 0.0],
                ],
            }
        );

        let insert_result = collection.update(insert_points, true).unwrap();

        let mut payload: HashMap<PayloadKeyType, PayloadInterface> = Default::default();

        payload.insert(
            "color".to_string(),
            PayloadInterface::Keyword(PayloadVariant::Value("red".to_string())),
        );

        let assign_payload = CollectionUpdateOperations::PayloadOperation(
            PayloadOps::SetPayload {
                payload,
                points: vec![2, 3],
            }
        );

        collection.update(assign_payload, true).unwrap();
    }


    let wal_options = WalOptions {
        segment_capacity: 100,
        segment_queue_len: 0,
    };

    let rt: Runtime = runtime::Builder::new()
        .threaded_scheduler()
        .max_threads(2)
        .build().unwrap();


    let optimizers = Arc::new(vec![]);

    // sleep(Duration::from_secs(120));

    let loaded_collection = load_collection(
        collection_dir.path(),
        &wal_options,
        rt.handle().clone(),
        rt.handle().clone(),
        optimizers,
        10,
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
        PointOps::UpsertPoints {
            ids: vec![0, 1, 2, 3, 4],
            vectors: vec![
                vec![1.0, 0.0, 1.0, 1.0],
                vec![1.0, 0.0, 1.0, 0.0],
                vec![1.0, 1.0, 1.0, 1.0],
                vec![1.0, 1.0, 0.0, 1.0],
                vec![1.0, 0.0, 0.0, 0.0],
            ],
        }
    );

    let json_str = serde_json::to_string_pretty(&insert_points).unwrap();

    eprintln!("&json_str = {}", &json_str);

    let read_obj: CollectionUpdateOperations = serde_json::from_str(json_str.as_str()).unwrap();

    eprintln!("read_obj = {:#?}", read_obj);


    let crob_bytes = rmp_serde::to_vec(&insert_points).unwrap();

    let read_obj2: CollectionUpdateOperations = rmp_serde::from_read_ref(&crob_bytes).unwrap();

    eprintln!("read_obj2 = {:#?}", read_obj2);

}