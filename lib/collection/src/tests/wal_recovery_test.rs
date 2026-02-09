use std::sync::Arc;

use common::budget::ResourceBudget;
use common::counter::hardware_accumulator::HwMeasurementAcc;
use common::save_on_disk::SaveOnDisk;
use segment::data_types::vectors::VectorStructInternal;
use segment::types::{PayloadFieldSchema, PayloadSchemaType, WithPayload, WithVector};
use shard::operations::CollectionUpdateOperations;
use shard::operations::point_ops::{
    PointInsertOperationsInternal, PointOperations, PointStructPersisted,
};
use tempfile::Builder;
use tokio::runtime::Handle;
use tokio::sync::RwLock;

use crate::operations::shared_storage_config::SharedStorageConfig;
use crate::operations::types::PointRequestInternal;
use crate::shards::local_shard::LocalShard;
use crate::shards::shard_trait::ShardOperation;
use crate::tests::fixtures::*;
use crate::update_workers::applied_seq::{APPLIED_SEQ_SAVE_INTERVAL, AppliedSeqHandler};

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
        .update(upsert_ops.into(), true, None, hw_acc.clone())
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
        .update(index_op.into(), true, None, hw_acc.clone())
        .await
        .unwrap();

    let delete_point_op = delete_point_operation(4);
    shard
        .update(delete_point_op.into(), true, None, hw_acc.clone())
        .await
        .unwrap();

    let info = shard.info().await.unwrap();
    eprintln!("info = {:#?}", info.payload_schema);
    let number_of_indexed_points = info
        .payload_schema
        .get(&"location".parse().unwrap())
        .unwrap()
        .points;

    shard.stop_gracefully().await;

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
        .update(delete_point_op.into(), true, None, hw_acc.clone())
        .await
        .unwrap();

    shard.stop_gracefully().await;

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

#[tokio::test(flavor = "multi_thread")]
async fn test_partial_flush_recovery() {
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
        .update(upsert_ops.into(), true, None, hw_acc.clone())
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
        .update(index_op.into(), true, None, hw_acc.clone())
        .await
        .unwrap();

    shard.stop_flush_worker().await;

    shard.full_flush();

    let delete_point_op = delete_point_operation(4);
    shard
        .update(delete_point_op.into(), true, None, hw_acc.clone())
        .await
        .unwrap();

    // This only flushed id-tracker-mapping, but not the storage change
    shard.partial_flush();

    shard.stop_gracefully().await;

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

    assert_eq!(number_of_indexed_points_after_load, 4);
}

/// Test that truncate_unapplied_wal correctly drops unapplied records from a non-empty WAL
/// and that new writes still work after the truncation.
#[tokio::test(flavor = "multi_thread")]
async fn test_truncate_unapplied_wal() {
    // Init the logger
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

    // Try to truncate a fresh WAL with last_index = 0.
    let removed_records = shard.truncate_unapplied_wal().await.unwrap();
    assert_eq!(
        removed_records, 0,
        "Expected 0 records removed on an empty WAL"
    );

    let hw_acc = HwMeasurementAcc::new();

    // Insert many individual points with wait=false to fill up the WAL.
    // We need more than APPLIED_SEQ_SAVE_INTERVAL + 1 updates to potentially have something to truncate.
    // Use a large number to increase chances of having unapplied records when truncate is called.
    let num_points = (APPLIED_SEQ_SAVE_INTERVAL + 10) * 5;

    // Collect all futures and execute them concurrently to maximize WAL fill-up speed
    let mut update_futures = Vec::with_capacity(num_points as usize);
    for i in 0..num_points {
        let point = PointStructPersisted {
            id: i.into(),
            vector: VectorStructInternal::from(vec![i as f32, 2.0, 3.0, 4.0]).into(),
            payload: None,
        };
        let op = CollectionUpdateOperations::PointOperation(PointOperations::UpsertPoints(
            PointInsertOperationsInternal::PointsList(vec![point]),
        ));

        // Use wait=false so updates queue up faster than they're processed
        update_futures.push(shard.update(op.into(), false, None, hw_acc.clone()));
    }

    // Send all updates as fast as possible
    for future in update_futures {
        future.await.unwrap();
    }

    // Call truncate_unapplied_wal immediately - this will skip any pending updates and truncate
    let removed_records = shard.truncate_unapplied_wal().await.unwrap();
    eprintln!("Removed {removed_records} records from WAL");

    // We expect some records to be truncated since we sent many updates with wait=false.
    // If the update worker was very fast and processed everything, we might get 0 records removed,
    // but this is acceptable behavior - it just means all updates were applied before truncation.
    // The important test is that the function works correctly and writing still works afterward.
    if removed_records == 0 {
        eprintln!("Note: All updates were applied before truncation (update worker was fast)");
    }

    // Count how many points were actually applied by trying to retrieve each point ID
    let all_point_ids: Vec<_> = (0..num_points).map(|i| i.into()).collect();
    let request = Arc::new(PointRequestInternal {
        ids: all_point_ids,
        with_payload: None,
        with_vector: WithVector::Bool(false),
    });

    let retrieved = shard
        .retrieve(
            request,
            &WithPayload::from(false),
            &WithVector::Bool(false),
            &current_runtime,
            None,
            hw_acc.clone(),
        )
        .await
        .unwrap();

    let applied_count = retrieved.len();
    let truncated_count = removed_records as usize;
    let total_pushed = num_points as usize;
    let missing_count = total_pushed.saturating_sub(applied_count + truncated_count);

    eprintln!(
        "Applied points: {applied_count}, Truncated points: {truncated_count}, \
         Total pushed: {total_pushed}, Missing: {missing_count}"
    );

    // Verify that the sum of applied + truncated equals total pushed
    // This assertion may fail due to an open issue - we want to reproduce it
    assert_eq!(
        applied_count + truncated_count,
        total_pushed,
        "Sum of applied ({applied_count}) and truncated ({truncated_count}) operations \
         should equal total pushed ({total_pushed}). Missing {missing_count} operations!"
    );

    // Try truncate WAL with nothing to truncate
    let removed_records = shard.truncate_unapplied_wal().await.unwrap();
    eprintln!("Removed {removed_records} records from WAL on second truncate");
    assert_eq!(
        removed_records, 0,
        "Expected 0 records removed on second truncate"
    );

    // Now verify that we can still write to the shard after truncation
    let new_point = PointStructPersisted {
        id: 99999.into(),
        vector: VectorStructInternal::from(vec![99.0, 99.0, 99.0, 99.0]).into(),
        payload: None,
    };
    let op = CollectionUpdateOperations::PointOperation(PointOperations::UpsertPoints(
        PointInsertOperationsInternal::PointsList(vec![new_point]),
    ));

    // Use wait=true to ensure the update is fully applied
    let update_result = shard
        .update(op.into(), true, None, hw_acc.clone())
        .await
        .unwrap();

    eprintln!("Update after truncate succeeded: {update_result:?}");

    // Verify the shard is still functional
    let info = shard.info().await.unwrap();
    assert_eq!(
        info.points_count.unwrap_or(0) as usize,
        applied_count + 1,
        "Shard should have applied_count + 1 points after update"
    );

    shard.stop_gracefully().await;
}

/// Test that verifies the WAL recovery process correctly loads pending updates into the update queue.
#[tokio::test(flavor = "multi_thread")]
async fn test_wal_replay_loads_pending_to_queue() {
    let _ = env_logger::builder().is_test(true).try_init();
    let collection_dir = Builder::new().prefix("test_collection").tempdir().unwrap();

    let config = create_collection_config();

    let collection_name = "test".to_string();

    let current_runtime: Handle = Handle::current();

    let payload_index_schema_dir = Builder::new().prefix("qdrant-test").tempdir().unwrap();
    let payload_index_schema_file = payload_index_schema_dir.path().join("payload-schema.json");
    let payload_index_schema =
        Arc::new(SaveOnDisk::load_or_init_default(payload_index_schema_file).unwrap());

    let shared_storage_config = Arc::new(SharedStorageConfig {
        update_queue_size: 10_000,
        ..Default::default()
    });

    // We need WAL length > applied_seq + 65 to trigger the queue loading path
    let total_ops = 500u64;

    let shard = LocalShard::build(
        0,
        collection_name.clone(),
        collection_dir.path(),
        Arc::new(RwLock::new(config.clone())),
        shared_storage_config.clone(),
        payload_index_schema.clone(),
        current_runtime.clone(),
        current_runtime.clone(),
        ResourceBudget::default(),
        config.optimizer_config.clone(),
    )
    .await
    .unwrap();

    // Stop flush worker to prevent automatic WAL truncation.
    shard.stop_flush_worker().await;

    let hw_acc = HwMeasurementAcc::new();

    // Insert all operations
    for i in 0..total_ops {
        let point = PointStructPersisted {
            id: i.into(),
            vector: VectorStructInternal::from(vec![1.0, 2.0, 3.0, 4.0]).into(),
            payload: None,
        };
        let op = CollectionUpdateOperations::PointOperation(PointOperations::UpsertPoints(
            PointInsertOperationsInternal::PointsList(vec![point]),
        ));
        shard
            .update(op.into(), true, None, hw_acc.clone())
            .await
            .unwrap();
    }

    // Stop the shard without flush to preserve WAL.
    shard.stop_gracefully().await;

    // Use AppliedSeqHandler to read and manipulate the applied_seq file.
    // This simulates a scenario where applied_seq is lower than the actual WAL length.
    // We need to ensure: WAL first_index <= applied_seq < WAL last_index - 65
    // The WAL might be truncated, so it's important to be careful with the replaced value.
    let applied_seq_handler = AppliedSeqHandler::load_or_init(collection_dir.path(), total_ops);
    eprintln!("Applied seq path: {:?}", applied_seq_handler.path());

    // Read the current applied_seq value
    let current_applied_seq = applied_seq_handler.op_num().unwrap_or(0);
    eprintln!("Current applied_seq: {current_applied_seq}");

    // Calculate the target low value:
    // - upper_bound = (total_ops - 100) + 64 = total_ops - 36
    // - At least 36 entries should go to update queue.
    // It's ok if they are applied already, segment will skip them.
    // We only need to ensure they are in the update queue.
    let low_applied_seq = total_ops.saturating_sub(100);

    // Only modify if the current value is too large
    if current_applied_seq > low_applied_seq {
        applied_seq_handler
            .force_set_and_persist(low_applied_seq)
            .unwrap();
        eprintln!(
            "Reduced applied_seq from {current_applied_seq} to {low_applied_seq}, total_ops: {total_ops}"
        );
    } else {
        eprintln!(
            "Applied_seq {current_applied_seq} is already <= target {low_applied_seq}, total_ops: {total_ops}"
        );
    }

    // Reload the shard
    let shard = LocalShard::load(
        0,
        collection_name,
        collection_dir.path(),
        Arc::new(RwLock::new(config.clone())),
        config.optimizer_config.clone(),
        shared_storage_config,
        payload_index_schema,
        false,
        current_runtime.clone(),
        current_runtime.clone(),
        ResourceBudget::default(),
    )
    .await
    .unwrap();

    // Check update queue info after load.
    let post_load_info = shard.local_update_queue_info();
    eprintln!("Post-load update queue info: {post_load_info:?}");

    // The applied_seq should be the value we set
    assert!(
        post_load_info.op_num.is_some(),
        "applied_seq should be tracked"
    );

    // Length should be not zero, as there should be pending ops loaded into the queue.
    // This check may be potentially flaky if the update worker can process
    // all pending operations between WAL load and `local_update_queue_info`.
    assert!(
        post_load_info.length > 0,
        "update queue should have pending operations after WAL replay"
    );

    // Wait for update worker to process all queued operations with a timeout
    let timeout = std::time::Duration::from_secs(2);
    let start = std::time::Instant::now();
    let poll_interval = std::time::Duration::from_millis(10);

    while shard.local_update_queue_info().length > 0 {
        assert!(
            start.elapsed() <= timeout,
            "Timeout waiting for update queue to empty"
        );
        tokio::time::sleep(poll_interval).await;
    }

    // Verify all points are present after processing
    let info = shard.info().await.unwrap();
    let points_count = info.points_count.unwrap_or(0);
    assert_eq!(
        points_count, total_ops as usize,
        "All {total_ops} points should be present after WAL replay and update queue processing",
    );

    shard.stop_gracefully().await;
}

/// Test that verifies the WAL recovery process correctly loads pending updates into the update queue.
#[tokio::test(flavor = "multi_thread")]
async fn test_wal_replay_with_smaller_queue_size() {
    let _ = env_logger::builder().is_test(true).try_init();
    let collection_dir = Builder::new().prefix("test_collection").tempdir().unwrap();

    let config = create_collection_config();

    let collection_name = "test".to_string();

    let current_runtime: Handle = Handle::current();

    let payload_index_schema_dir = Builder::new().prefix("qdrant-test").tempdir().unwrap();
    let payload_index_schema_file = payload_index_schema_dir.path().join("payload-schema.json");
    let payload_index_schema =
        Arc::new(SaveOnDisk::load_or_init_default(payload_index_schema_file).unwrap());

    let mut shared_storage_config = SharedStorageConfig {
        update_queue_size: 10_000,
        ..Default::default()
    };

    // We need WAL length > applied_seq + 65 to trigger the queue loading path
    let total_ops = 500u64;

    let shard = LocalShard::build(
        0,
        collection_name.clone(),
        collection_dir.path(),
        Arc::new(RwLock::new(config.clone())),
        Arc::new(shared_storage_config.clone()),
        payload_index_schema.clone(),
        current_runtime.clone(),
        current_runtime.clone(),
        ResourceBudget::default(),
        config.optimizer_config.clone(),
    )
    .await
    .unwrap();

    // Stop flush worker to prevent automatic WAL truncation.
    shard.stop_flush_worker().await;

    let hw_acc = HwMeasurementAcc::new();

    // Insert all operations
    for i in 0..total_ops {
        let point = PointStructPersisted {
            id: i.into(),
            vector: VectorStructInternal::from(vec![1.0, 2.0, 3.0, 4.0]).into(),
            payload: None,
        };
        let op = CollectionUpdateOperations::PointOperation(PointOperations::UpsertPoints(
            PointInsertOperationsInternal::PointsList(vec![point]),
        ));
        shard
            .update(op.into(), true, None, hw_acc.clone())
            .await
            .unwrap();
    }

    // Stop the shard without flush to preserve WAL.
    shard.stop_gracefully().await;

    // Use AppliedSeqHandler to read and manipulate the applied_seq file.
    // This simulates a scenario where applied_seq is lower than the actual WAL length.
    // We need to ensure: WAL first_index <= applied_seq < WAL last_index - 65
    // The WAL might be truncated, so it's important to be careful with the replaced value.
    let applied_seq_handler = AppliedSeqHandler::load_or_init(collection_dir.path(), total_ops);
    eprintln!("Applied seq path: {:?}", applied_seq_handler.path());

    // Read the current applied_seq value
    let current_applied_seq = applied_seq_handler.op_num().unwrap_or(0);
    eprintln!("Current applied_seq: {current_applied_seq}");

    // Calculate the target low value:
    // - upper_bound = (total_ops - 100) + 64 = total_ops - 36
    // - At least 36 entries should go to update queue.
    // It's ok if they are applied already, segment will skip them.
    // We only need to ensure they are in the update queue.
    let low_applied_seq = total_ops.saturating_sub(100);

    // Only modify if the current value is too large
    if current_applied_seq > low_applied_seq {
        applied_seq_handler
            .force_set_and_persist(low_applied_seq)
            .unwrap();
        eprintln!(
            "Reduced applied_seq from {current_applied_seq} to {low_applied_seq}, total_ops: {total_ops}"
        );
    } else {
        eprintln!(
            "Applied_seq {current_applied_seq} is already <= target {low_applied_seq}, total_ops: {total_ops}"
        );
    }

    // Set smaller queue size to force loading into the queue in batches
    shared_storage_config.update_queue_size = 5;

    // Reload the shard
    let shard = LocalShard::load(
        0,
        collection_name,
        collection_dir.path(),
        Arc::new(RwLock::new(config.clone())),
        config.optimizer_config.clone(),
        Arc::new(shared_storage_config),
        payload_index_schema,
        false,
        current_runtime.clone(),
        current_runtime.clone(),
        ResourceBudget::default(),
    )
    .await
    .unwrap();

    // Wait for update worker to process all queued operations with a timeout
    let timeout = std::time::Duration::from_secs(2);
    let start = std::time::Instant::now();
    let poll_interval = std::time::Duration::from_millis(10);

    while shard.local_update_queue_info().length > 0 {
        assert!(
            start.elapsed() <= timeout,
            "Timeout waiting for update queue to empty"
        );
        tokio::time::sleep(poll_interval).await;
    }

    // Verify all points are present after processing
    let info = shard.info().await.unwrap();
    let points_count = info.points_count.unwrap_or(0);
    assert_eq!(
        points_count, total_ops as usize,
        "All {total_ops} points should be present after WAL replay and update queue processing",
    );

    shard.stop_gracefully().await;
}
