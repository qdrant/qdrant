use std::sync::Arc;

use common::budget::ResourceBudget;
use common::counter::hardware_accumulator::HwMeasurementAcc;
use common::save_on_disk::SaveOnDisk;
use segment::types::{PayloadFieldSchema, PayloadSchemaType};
use tempfile::Builder;
use tokio::runtime::Handle;
use tokio::sync::RwLock;

use crate::shards::local_shard::LocalShard;
use crate::shards::shard_trait::ShardOperation;
use crate::tests::fixtures::*;

#[tokio::test(flavor = "multi_thread")]
async fn test_delete_from_indexed_payload() {
    //  Init the logger
    let _ = env_logger::builder().is_test(true).try_init();
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

    let upsert_ops = upsert_operation();

    let hw_acc = HwMeasurementAcc::new();

    shard
        .update(upsert_ops.into(), true, hw_acc.clone())
        .await
        .unwrap();

    let index_op = create_payload_index_operation();

    payload_index_schema
        .write(|schema| {
            schema.schema.insert(
                "location".parse().unwrap(),
                PayloadFieldSchema::FieldType(PayloadSchemaType::Geo),
            );
        })
        .unwrap();
    shard
        .update(index_op.into(), true, hw_acc.clone())
        .await
        .unwrap();

    let delete_point_op = delete_point_operation(4);
    shard
        .update(delete_point_op.into(), true, hw_acc.clone())
        .await
        .unwrap();

    let info = shard.info().await.unwrap();
    eprintln!("info = {:#?}", info.payload_schema);
    let number_of_indexed_points = info
        .payload_schema
        .get(&"location".parse().unwrap())
        .unwrap()
        .points;

    drop(shard);

    let shard = LocalShard::load(
        0,
        collection_name.clone(),
        collection_dir.path(),
        Arc::new(RwLock::new(config.clone())),
        config.optimizer_config.clone(),
        Arc::new(Default::default()),
        payload_index_schema.clone(),
        true,
        current_runtime.clone(),
        current_runtime.clone(),
        ResourceBudget::default(),
    )
    .await
    .unwrap();

    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    eprintln!("dropping point 5");
    let delete_point_op = delete_point_operation(5);
    shard
        .update(delete_point_op.into(), true, hw_acc.clone())
        .await
        .unwrap();

    drop(shard);

    let shard = LocalShard::load(
        0,
        collection_name,
        collection_dir.path(),
        Arc::new(RwLock::new(config.clone())),
        config.optimizer_config.clone(),
        Arc::new(Default::default()),
        payload_index_schema,
        true,
        current_runtime.clone(),
        current_runtime,
        ResourceBudget::default(),
    )
    .await
    .unwrap();

    let info = shard.info().await.unwrap();
    eprintln!("info = {:#?}", info.payload_schema);

    let number_of_indexed_points_after_load = info
        .payload_schema
        .get(&"location".parse().unwrap())
        .unwrap()
        .points;

    assert_eq!(number_of_indexed_points, 4);
    assert_eq!(number_of_indexed_points_after_load, 3);
}
