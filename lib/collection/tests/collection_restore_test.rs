mod common;

use crate::common::simple_collection_fixture;
use collection::collection_builder::collection_loader::load_collection;
use collection::operations::point_ops::{PointInsertOperations, PointOperations};
use collection::operations::types::ScrollRequest;
use collection::operations::CollectionUpdateOperations;
use segment::types::PayloadType;
use std::sync::Arc;
use tempdir::TempDir;
use tokio::runtime::Handle;

#[tokio::test]
async fn test_collection_reloading() {
    let collection_dir = TempDir::new("collection").unwrap();

    {
        let _collection = simple_collection_fixture(collection_dir.path()).await;
    }

    for _i in 0..5 {
        let collection = load_collection(collection_dir.path(), Handle::current());
        let insert_points = CollectionUpdateOperations::PointOperation(
            PointOperations::UpsertPoints(PointInsertOperations::BatchPoints {
                ids: vec![0, 1],
                vectors: vec![vec![1.0, 0.0, 1.0, 1.0], vec![1.0, 0.0, 1.0, 0.0]],
                payloads: None,
            }),
        );
        collection.update(insert_points, true).await.unwrap();
    }

    let collection = load_collection(collection_dir.path(), Handle::current());
    assert_eq!(collection.info().unwrap().vectors_count, 2)
}

#[tokio::test]
async fn test_collection_payload_reloading() {
    let collection_dir = TempDir::new("collection").unwrap();

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
        collection.update(insert_points, true).await.unwrap();
    }

    let collection = load_collection(collection_dir.path(), Handle::current());

    let res = collection
        .scroll(Arc::new(ScrollRequest {
            offset: Some(0),
            limit: Some(10),
            filter: None,
            with_payload: Some(true),
            with_vector: Some(true),
        }))
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
