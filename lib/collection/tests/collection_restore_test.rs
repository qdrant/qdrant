use std::sync::Arc;

use tempdir::TempDir;

use collection::collection_builder::collection_loader::load_collection;
use collection::collection_manager::simple_collection_searcher::SimpleCollectionSearcher;
use collection::collection_manager::simple_collection_updater::SimpleCollectionUpdater;
use collection::operations::point_ops::{PointInsertOperations, PointOperations};
use collection::operations::types::ScrollRequest;
use collection::operations::CollectionUpdateOperations;
use segment::types::PayloadType;

use crate::common::simple_collection_fixture;

mod common;

#[tokio::test]
async fn test_collection_reloading() {
    let collection_dir = TempDir::new("collection").unwrap();

    {
        let _collection = simple_collection_fixture(collection_dir.path()).await;
    }
    let updater = Arc::new(SimpleCollectionUpdater::new());
    for _i in 0..5 {
        let collection = load_collection(collection_dir.path(), updater.clone());
        let insert_points = CollectionUpdateOperations::PointOperation(
            PointOperations::UpsertPoints(PointInsertOperations::BatchPoints {
                ids: vec![0, 1],
                vectors: vec![vec![1.0, 0.0, 1.0, 1.0], vec![1.0, 0.0, 1.0, 0.0]],
                payloads: None,
            }),
        );
        collection
            .update_by(insert_points, true, updater.clone())
            .await
            .unwrap();
    }

    let collection = load_collection(collection_dir.path(), updater.clone());
    assert_eq!(collection.info().await.unwrap().vectors_count, 2)
}

#[tokio::test]
async fn test_collection_payload_reloading() {
    let collection_dir = TempDir::new("collection").unwrap();
    let updater = Arc::new(SimpleCollectionUpdater::new());
    {
        let collection = simple_collection_fixture(collection_dir.path()).await;
        let insert_points = CollectionUpdateOperations::PointOperation(
            PointOperations::UpsertPoints(PointInsertOperations::BatchPoints {
                ids: vec![0, 1],
                vectors: vec![vec![1.0, 0.0, 1.0, 1.0], vec![1.0, 0.0, 1.0, 0.0]],
                payloads: serde_json::from_str(
                    &r#"[{ "k": { "type": "keyword", "value": "v1" } }, { "k": "v2" }]"#,
                )
                .unwrap(),
            }),
        );
        collection
            .update_by(insert_points, true, updater.clone())
            .await
            .unwrap();
    }

    let collection = load_collection(collection_dir.path(), updater.clone());

    let searcher = SimpleCollectionSearcher::new();
    let res = collection
        .scroll_by(
            ScrollRequest {
                offset: Some(0),
                limit: Some(10),
                filter: None,
                with_payload: Some(true),
                with_vector: Some(true),
            },
            &searcher,
        )
        .await
        .unwrap();

    assert_eq!(res.points.len(), 2);

    match res.points[0]
        .payload
        .as_ref()
        .expect("has payload")
        .get("k")
        .expect("has value")
    {
        PayloadType::Keyword(values) => assert_eq!(&vec!["v1".to_string()], values),
        _ => panic!("unexpected type"),
    }

    eprintln!(
        "res = {:#?}",
        res.points[0].payload.as_ref().unwrap().get("k")
    );
}
