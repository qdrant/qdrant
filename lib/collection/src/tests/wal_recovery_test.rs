use std::sync::Arc;

use common::cpu::CpuBudget;
use tempfile::Builder;
use tokio::runtime::Handle;
use tokio::sync::RwLock;

use crate::shards::local_shard::LocalShard;
use crate::shards::shard_trait::ShardOperation;
use crate::tests::fixtures::*;

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
        CpuBudget::default(),
    )
    .await
    .unwrap();

    let upsert_ops = upsert_operation();

    shard.update(upsert_ops.into(), true).await.unwrap();

    let index_op = create_payload_index_operation();

    shard.update(index_op.into(), true).await.unwrap();

    let delete_point_op = delete_point_operation(4);
    shard.update(delete_point_op.into(), true).await.unwrap();

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
        Arc::new(Default::default()),
        current_runtime.clone(),
        CpuBudget::default(),
    )
    .await
    .unwrap();

    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    eprintln!("dropping point 5");
    let delete_point_op = delete_point_operation(5);
    shard.update(delete_point_op.into(), true).await.unwrap();

    drop(shard);

    let shard = LocalShard::load(
        0,
        collection_name,
        collection_dir.path(),
        Arc::new(RwLock::new(config)),
        Arc::new(Default::default()),
        current_runtime,
        CpuBudget::default(),
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
