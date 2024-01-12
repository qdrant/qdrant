use std::num::NonZeroU64;
use std::sync::Arc;

use segment::types::{Distance, PayloadFieldSchema, PayloadSchemaType};
use tempfile::Builder;
use tokio::runtime::Handle;
use tokio::sync::RwLock;

use crate::config::{CollectionConfig, CollectionParams, WalConfig};
use crate::operations::point_ops::{PointOperations, PointStruct};
use crate::operations::types::{VectorParams, VectorsConfig};
use crate::operations::{CollectionUpdateOperations, CreateIndex, FieldIndexOperations};
use crate::shards::local_shard::LocalShard;
use crate::shards::shard_trait::ShardOperation;
use crate::tests::snapshot_test::TEST_OPTIMIZERS_CONFIG;

fn create_collection_config() -> CollectionConfig {
    let wal_config = WalConfig {
        wal_capacity_mb: 1,
        wal_segments_ahead: 0,
    };

    let collection_params = CollectionParams {
        vectors: VectorsConfig::Single(VectorParams {
            size: NonZeroU64::new(4).unwrap(),
            distance: Distance::Dot,
            hnsw_config: None,
            quantization_config: None,
            on_disk: None,
        }),
        ..CollectionParams::empty()
    };

    let mut optimizer_config = TEST_OPTIMIZERS_CONFIG.clone();

    optimizer_config.default_segment_number = 1;
    optimizer_config.flush_interval_sec = 0;

    CollectionConfig {
        params: collection_params,
        optimizer_config,
        wal_config,
        hnsw_config: Default::default(),
        quantization_config: Default::default(),
    }
}

fn upsert_operation() -> CollectionUpdateOperations {
    CollectionUpdateOperations::PointOperation(
        vec![
            PointStruct {
                id: 1.into(),
                vector: vec![1.0, 2.0, 3.0, 4.0].into(),
                payload: Some(
                    serde_json::from_str(r#"{ "location": { "lat": 10.12, "lon": 32.12  } }"#).unwrap(),
                ),
            },
            PointStruct {
                id: 2.into(),
                vector: vec![2.0, 1.0, 3.0, 4.0].into(),
                payload: Some(
                    serde_json::from_str(r#"{ "location": { "lat": 11.12, "lon": 34.82  } }"#).unwrap(),
                ),
            },
            PointStruct {
                id: 3.into(),
                vector: vec![3.0, 2.0, 1.0, 4.0].into(),
                payload: Some(
                    serde_json::from_str(r#"{ "location": [ { "lat": 12.12, "lon": 34.82  }, { "lat": 12.2, "lon": 12.82  }] }"#).unwrap(),
                ),
            },
            PointStruct {
                id: 4.into(),
                vector: vec![4.0, 2.0, 3.0, 1.0].into(),
                payload: Some(
                    serde_json::from_str(r#"{ "location": { "lat": 13.12, "lon": 34.82  } }"#).unwrap(),
                ),
            },
            PointStruct {
                id: 5.into(),
                vector: vec![5.0, 2.0, 3.0, 4.0].into(),
                payload: Some(
                    serde_json::from_str(r#"{ "location": { "lat": 14.12, "lon": 32.12  } }"#).unwrap(),
                ),
            },

        ]
        .into(),
    )
}

fn create_payload_index_operation() -> CollectionUpdateOperations {
    CollectionUpdateOperations::FieldIndexOperation(FieldIndexOperations::CreateIndex(
        CreateIndex {
            field_name: "location".to_string(),
            field_schema: Some(PayloadFieldSchema::FieldType(PayloadSchemaType::Geo)),
        },
    ))
}

fn delete_point_operation(idx: u64) -> CollectionUpdateOperations {
    CollectionUpdateOperations::PointOperation(PointOperations::DeletePoints {
        ids: vec![idx.into()],
    })
}

#[tokio::test(flavor = "multi_thread")]
async fn test_delete_from_indexed_payload() {
    let collection_dir = Builder::new().prefix("test_collection").tempdir().unwrap();

    let config = create_collection_config();

    let collection_name = "test".to_string();

    let current_runtime: Handle = Handle::current();

    let shard = LocalShard::build(
        0,
        collection_name.clone(),
        collection_dir.path(),
        Arc::new(RwLock::new(config.clone())),
        Arc::new(Default::default()),
        current_runtime.clone(),
    )
    .await
    .unwrap();

    let upsert_ops = upsert_operation();

    shard
        .update(upsert_ops, true, cancel::CancellationToken::new())
        .await
        .unwrap();

    let index_op = create_payload_index_operation();

    shard
        .update(index_op, true, cancel::CancellationToken::new())
        .await
        .unwrap();

    let delete_point_op = delete_point_operation(4);
    shard
        .update(delete_point_op, true, cancel::CancellationToken::new())
        .await
        .unwrap();

    let info = shard.info().await.unwrap();
    eprintln!("info = {:#?}", info.payload_schema);
    let number_of_indexed_points = info.payload_schema.get("location").unwrap().points;

    drop(shard);

    let shard = LocalShard::load(
        0,
        collection_name.clone(),
        collection_dir.path(),
        Arc::new(RwLock::new(config.clone())),
        Arc::new(Default::default()),
        current_runtime.clone(),
    )
    .await
    .unwrap();

    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    eprintln!("dropping point 5");
    let delete_point_op = delete_point_operation(5);
    shard
        .update(delete_point_op, true, cancel::CancellationToken::new())
        .await
        .unwrap();

    drop(shard);

    let shard = LocalShard::load(
        0,
        collection_name,
        collection_dir.path(),
        Arc::new(RwLock::new(config)),
        Arc::new(Default::default()),
        current_runtime,
    )
    .await
    .unwrap();

    let info = shard.info().await.unwrap();
    eprintln!("info = {:#?}", info.payload_schema);

    let number_of_indexed_points_after_load = info.payload_schema.get("location").unwrap().points;

    assert_eq!(number_of_indexed_points, 4);
    assert_eq!(number_of_indexed_points_after_load, 3);
}
