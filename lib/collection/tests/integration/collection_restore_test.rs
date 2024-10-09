use collection::operations::point_ops::{
    BatchPersisted, BatchVectorStructPersisted, PointInsertOperationsInternal, PointOperations,
    WriteOrdering,
};
use collection::operations::shard_selector_internal::ShardSelectorInternal;
use collection::operations::types::ScrollRequestInternal;
use collection::operations::CollectionUpdateOperations;
use itertools::Itertools;
use segment::json_path::JsonPath;
use segment::types::{PayloadContainer, PayloadSelectorExclude, WithPayloadInterface};
use serde_json::Value;
use tempfile::Builder;

use crate::common::{load_local_collection, simple_collection_fixture, N_SHARDS};

#[tokio::test(flavor = "multi_thread")]
async fn test_collection_reloading() {
    test_collection_reloading_with_shards(1).await;
    test_collection_reloading_with_shards(N_SHARDS).await;
}

async fn test_collection_reloading_with_shards(shard_number: u32) {
    let collection_dir = Builder::new().prefix("collection").tempdir().unwrap();

    let collection = simple_collection_fixture(collection_dir.path(), shard_number).await;
    drop(collection);
    for _i in 0..5 {
        let collection_path = collection_dir.path();
        let collection = load_local_collection(
            "test".to_string(),
            collection_path,
            &collection_path.join("snapshots"),
        )
        .await;
        let insert_points =
            CollectionUpdateOperations::PointOperation(PointOperations::UpsertPoints(
                PointInsertOperationsInternal::PointsBatch(BatchPersisted {
                    ids: vec![0, 1].into_iter().map(|x| x.into()).collect_vec(),
                    vectors: BatchVectorStructPersisted::Single(vec![
                        vec![1.0, 0.0, 1.0, 1.0],
                        vec![1.0, 0.0, 1.0, 0.0],
                    ]),
                    payloads: None,
                }),
            ));
        collection
            .update_from_client_simple(insert_points, true, WriteOrdering::default())
            .await
            .unwrap();
    }

    let collection_path = collection_dir.path();
    let collection = load_local_collection(
        "test".to_string(),
        collection_path,
        &collection_path.join("snapshots"),
    )
    .await;
    assert_eq!(
        collection
            .info(&ShardSelectorInternal::All)
            .await
            .unwrap()
            .points_count,
        Some(2),
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_collection_payload_reloading() {
    test_collection_payload_reloading_with_shards(1).await;
    test_collection_payload_reloading_with_shards(N_SHARDS).await;
}

async fn test_collection_payload_reloading_with_shards(shard_number: u32) {
    let collection_dir = Builder::new().prefix("collection").tempdir().unwrap();
    {
        let collection = simple_collection_fixture(collection_dir.path(), shard_number).await;
        let insert_points =
            CollectionUpdateOperations::PointOperation(PointOperations::UpsertPoints(
                PointInsertOperationsInternal::PointsBatch(BatchPersisted {
                    ids: vec![0, 1].into_iter().map(|x| x.into()).collect_vec(),
                    vectors: BatchVectorStructPersisted::Single(vec![
                        vec![1.0, 0.0, 1.0, 1.0],
                        vec![1.0, 0.0, 1.0, 0.0],
                    ]),
                    payloads: serde_json::from_str(r#"[{ "k": "v1" } , { "k": "v2"}]"#).unwrap(),
                }),
            ));
        collection
            .update_from_client_simple(insert_points, true, WriteOrdering::default())
            .await
            .unwrap();
    }
    let collection_path = collection_dir.path();
    let collection = load_local_collection(
        "test".to_string(),
        collection_path,
        &collection_path.join("snapshots"),
    )
    .await;

    let res = collection
        .scroll_by(
            ScrollRequestInternal {
                offset: None,
                limit: Some(10),
                filter: None,
                with_payload: Some(WithPayloadInterface::Bool(true)),
                with_vector: true.into(),
                order_by: None,
            },
            None,
            &ShardSelectorInternal::All,
            None,
        )
        .await
        .unwrap();

    assert_eq!(res.points.len(), 2);

    match res.points[0]
        .payload
        .as_ref()
        .expect("has payload")
        .get_value(&JsonPath::new("k"))
        .into_iter()
        .next()
        .expect("has value")
    {
        Value::String(value) => assert_eq!("v1", value),
        _ => panic!("unexpected type"),
    }

    eprintln!(
        "res = {:#?}",
        res.points[0]
            .payload
            .as_ref()
            .unwrap()
            .get_value(&JsonPath::new("k"))
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_collection_payload_custom_payload() {
    test_collection_payload_custom_payload_with_shards(1).await;
    test_collection_payload_custom_payload_with_shards(N_SHARDS).await;
}

async fn test_collection_payload_custom_payload_with_shards(shard_number: u32) {
    let collection_dir = Builder::new().prefix("collection").tempdir().unwrap();
    {
        let collection = simple_collection_fixture(collection_dir.path(), shard_number).await;
        let insert_points =
            CollectionUpdateOperations::PointOperation(PointOperations::UpsertPoints(
                PointInsertOperationsInternal::PointsBatch(BatchPersisted {
                    ids: vec![0.into(), 1.into()],
                    vectors: BatchVectorStructPersisted::Single(vec![
                        vec![1.0, 0.0, 1.0, 1.0],
                        vec![1.0, 0.0, 1.0, 0.0],
                    ]),
                    payloads: serde_json::from_str(
                        r#"[{ "k1": "v1" }, { "k1": "v2" , "k2": "v3", "k3": "v4"}]"#,
                    )
                    .unwrap(),
                }),
            ));
        collection
            .update_from_client_simple(insert_points, true, WriteOrdering::default())
            .await
            .unwrap();
    }

    let collection_path = collection_dir.path();
    let collection = load_local_collection(
        "test".to_string(),
        collection_path,
        &collection_path.join("snapshots"),
    )
    .await;

    // Test res with filter payload
    let res_with_custom_payload = collection
        .scroll_by(
            ScrollRequestInternal {
                offset: None,
                limit: Some(10),
                filter: None,
                with_payload: Some(WithPayloadInterface::Fields(vec![JsonPath::new("k2")])),
                with_vector: true.into(),
                order_by: None,
            },
            None,
            &ShardSelectorInternal::All,
            None,
        )
        .await
        .unwrap();
    assert!(res_with_custom_payload.points[0]
        .payload
        .as_ref()
        .expect("has payload")
        .is_empty());

    match res_with_custom_payload.points[1]
        .payload
        .as_ref()
        .expect("has payload")
        .get_value(&JsonPath::new("k2"))
        .into_iter()
        .next()
        .expect("has value")
    {
        Value::String(value) => assert_eq!("v3", value),
        _ => panic!("unexpected type"),
    }

    // Test res with filter payload dict
    let res_with_custom_payload = collection
        .scroll_by(
            ScrollRequestInternal {
                offset: None,
                limit: Some(10),
                filter: None,
                with_payload: Some(PayloadSelectorExclude::new(vec![JsonPath::new("k1")]).into()),
                with_vector: false.into(),
                order_by: None,
            },
            None,
            &ShardSelectorInternal::All,
            None,
        )
        .await
        .unwrap();
    assert!(res_with_custom_payload.points[0]
        .payload
        .as_ref()
        .expect("has payload")
        .is_empty());

    assert_eq!(
        res_with_custom_payload.points[1]
            .payload
            .as_ref()
            .expect("has payload")
            .len(),
        2
    );

    match res_with_custom_payload.points[1]
        .payload
        .as_ref()
        .expect("has payload")
        .get_value(&JsonPath::new("k3"))
        .into_iter()
        .next()
        .expect("has value")
    {
        Value::String(value) => assert_eq!("v4", value),
        _ => panic!("unexpected type"),
    };
}
