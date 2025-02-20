use std::sync::Arc;

use common::budget::ResourceBudget;
use common::counter::hardware_accumulator::HwMeasurementAcc;
use segment::types::{PayloadFieldSchema, PayloadSchemaType};
use tempfile::Builder;
use tokio::runtime::Handle;
use tokio::sync::RwLock;

use crate::save_on_disk::SaveOnDisk;
use crate::shards::local_shard::LocalShard;
use crate::shards::shard_trait::ShardOperation;
use crate::tests::fixtures::*;

#[tokio::test(flavor = "multi_thread")]
async fn test_fix_payload_indices() {
    let collection_dir = Builder::new().prefix("test_collection").tempdir().unwrap();

    let config = create_collection_config();

    let collection_name = "test".to_string();

    let current_runtime: Handle = Handle::current();

    let payload_index_schema_dir = Builder::new().prefix("qdrant-test").tempdir().unwrap();
    let payload_index_schema_file = payload_index_schema_dir.path().join("payload-schema.json");
    let payload_index_schema =
        Arc::new(SaveOnDisk::load_or_init_default(payload_index_schema_file).unwrap());

    let shard = LocalShard::build(
        0,
        collection_name.clone(),
        collection_dir.path(),
        Arc::new(RwLock::new(config.clone())),
        Arc::new(Default::default()),
        payload_index_schema.clone(),
        current_runtime.clone(),
        current_runtime.clone(),
        ResourceBudget::default(),
        config.optimizer_config.clone(),
    )
    .await
    .unwrap();

    let hw_acc = HwMeasurementAcc::new();

    let upsert_ops = upsert_operation();
    shard
        .update(upsert_ops.into(), true, hw_acc.clone())
        .await
        .unwrap();

    // Create payload index in shard locally, not in global collection configuration
    let index_op = create_payload_index_operation();
    shard
        .update(index_op.into(), true, hw_acc.clone())
        .await
        .unwrap();

    let delete_point_op = delete_point_operation(4);
    shard
        .update(delete_point_op.into(), true, hw_acc.clone())
        .await
        .unwrap();

    std::thread::sleep(std::time::Duration::from_secs(1));

    drop(shard);

    payload_index_schema
        .write(|schema| {
            schema.schema.insert(
                "a".parse().unwrap(),
                PayloadFieldSchema::FieldType(PayloadSchemaType::Integer),
            );
            schema.schema.insert(
                "b".parse().unwrap(),
                PayloadFieldSchema::FieldType(PayloadSchemaType::Keyword),
            );
        })
        .unwrap();

    let shard = LocalShard::load(
        0,
        collection_name,
        collection_dir.path(),
        Arc::new(RwLock::new(config.clone())),
        config.optimizer_config.clone(),
        Arc::new(Default::default()),
        payload_index_schema,
        current_runtime.clone(),
        current_runtime,
        ResourceBudget::default(),
    )
    .await
    .unwrap();

    let info = shard.info().await.unwrap();
    // Deleting existing payload index is not supported
    // assert!(!info
    //     .payload_schema
    //     .contains_key(&"location".parse().unwrap()));

    assert_eq!(
        info.payload_schema
            .get(&"a".parse().unwrap())
            .unwrap()
            .data_type,
        PayloadSchemaType::Integer
    );
    assert_eq!(
        info.payload_schema
            .get(&"b".parse().unwrap())
            .unwrap()
            .data_type,
        PayloadSchemaType::Keyword
    );
}
