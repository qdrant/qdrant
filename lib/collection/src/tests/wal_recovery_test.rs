use std::sync::Arc;

use common::budget::ResourceBudget;
use common::counter::hardware_accumulator::HwMeasurementAcc;
use common::save_on_disk::SaveOnDisk;
use common::types::DeferredBehavior;
use segment::data_types::vectors::VectorStructInternal;
use segment::types::{PayloadFieldSchema, PayloadSchemaType, WithPayload, WithVector};
use shard::operations::CollectionUpdateOperations;
use shard::operations::point_ops::{
    PointInsertOperationsInternal, PointOperations, PointStructPersisted,
};
use shard::wal::{SerdeWal, WalRawRecord};
use tempfile::Builder;
use tokio::runtime::Handle;
use tokio::sync::RwLock;

use crate::common::adaptive_handle::AdaptiveSearchHandle;
use crate::operations::shared_storage_config::SharedStorageConfig;
use crate::operations::types::PointRequestInternal;
use crate::shards::local_shard::LocalShard;
use crate::shards::shard_trait::{ShardOperation, WaitUntil};
use crate::tests::fixtures::*;
use crate::update_workers::applied_seq::{APPLIED_SEQ_SAVE_INTERVAL, AppliedSeqHandler};

#[tokio::test(flavor = "multi_thread")]
async fn test_delete_from_indexed_payload() {
    //  Init the logger
    let _ = env_logger::builder().is_test(true).try_init();
    let collection_dir = Builder::new().prefix("test_collection").tempdir().unwrap();

    let config = create_collection_config();

    let collection_name = "test".to_string();

    let update_runtime = Handle::current();
    let current_runtime: AdaptiveSearchHandle = AdaptiveSearchHandle::current_for_tests();

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
        update_runtime.clone(),
        current_runtime.clone(),
        ResourceBudget::default(),
        config.optimizer_config.clone(),
    )
    .await
    .unwrap();

    let upsert_ops = upsert_operation();

    let hw_acc = HwMeasurementAcc::new();

    shard
        .update(upsert_ops.into(), WaitUntil::Visible, None, hw_acc.clone())
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
        .update(index_op.into(), WaitUntil::Visible, None, hw_acc.clone())
        .await
        .unwrap();

    let delete_point_op = delete_point_operation(4);
    shard
        .update(
            delete_point_op.into(),
            WaitUntil::Visible,
            None,
            hw_acc.clone(),
        )
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
        update_runtime.clone(),
        current_runtime.clone(),
        ResourceBudget::default(),
    )
    .await
    .unwrap();

    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    eprintln!("dropping point 5");
    let delete_point_op = delete_point_operation(5);
    shard
        .update(
            delete_point_op.into(),
            WaitUntil::Visible,
            None,
            hw_acc.clone(),
        )
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
        update_runtime.clone(),
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

    let update_runtime = Handle::current();
    let current_runtime: AdaptiveSearchHandle = AdaptiveSearchHandle::current_for_tests();

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
        update_runtime.clone(),
        current_runtime.clone(),
        ResourceBudget::default(),
        config.optimizer_config.clone(),
    )
    .await
    .unwrap();

    let upsert_ops = upsert_operation();

    let hw_acc = HwMeasurementAcc::new();

    shard
        .update(upsert_ops.into(), WaitUntil::Visible, None, hw_acc.clone())
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
        .update(index_op.into(), WaitUntil::Visible, None, hw_acc.clone())
        .await
        .unwrap();

    shard.stop_flush_worker().await;

    shard.full_flush();

    let delete_point_op = delete_point_operation(4);
    shard
        .update(
            delete_point_op.into(),
            WaitUntil::Visible,
            None,
            hw_acc.clone(),
        )
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
        update_runtime.clone(),
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

    let update_runtime = Handle::current();
    let current_runtime: AdaptiveSearchHandle = AdaptiveSearchHandle::current_for_tests();

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
        update_runtime.clone(),
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
        update_futures.push(shard.update(op.into(), WaitUntil::Wal, None, hw_acc.clone()));
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
    let all_point_ids: Vec<_> = (0..num_points).map(u64::into).collect();
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
            DeferredBehavior::VisibleOnly,
        )
        .await
        .unwrap();

    let applied_count = retrieved.len();
    let truncated_count = removed_records;
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
        .update(op.into(), WaitUntil::Visible, None, hw_acc.clone())
        .await
        .unwrap();

    eprintln!("Update after truncate succeeded: {update_result:?}");

    // Verify the shard is still functional
    let info = shard.info().await.unwrap();
    assert_eq!(
        info.points_count.unwrap_or(0),
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

    let update_runtime = Handle::current();
    let current_runtime: AdaptiveSearchHandle = AdaptiveSearchHandle::current_for_tests();

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
        update_runtime.clone(),
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
            .update(op.into(), WaitUntil::Visible, None, hw_acc.clone())
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
        update_runtime.clone(),
        current_runtime.clone(),
        ResourceBudget::default(),
    )
    .await
    .unwrap();

    // Check update queue info after load.
    let post_load_info = shard.local_update_queue_info().await;
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

    while shard.local_update_queue_info().await.length > 0 {
        assert!(
            start.elapsed() <= timeout,
            "Timeout waiting for update queue to empty"
        );
        tokio::time::sleep(poll_interval).await;
    }

    // Channel length reaching zero only means the last operation was received,
    // not that it finished applying; wait for the worker to go idle.
    shard.plunge_async().await.unwrap().await.unwrap();

    // Verify all points are present after processing
    let info = shard.info().await.unwrap();
    let points_count = info.points_count.unwrap_or(0);
    assert_eq!(
        points_count, total_ops as usize,
        "All {total_ops} points should be present after WAL replay and update queue processing",
    );

    shard.stop_gracefully().await;
}

/// A deferred-tail WAL entry that fails deserialization must not fail shard load.
///
/// `load_from_wal` reads the tail past `applied_seq` to advance the newest clocks before
/// queueing it to the update worker. That pass must log-and-skip an unreadable entry (the
/// worker tolerates the same failure when it re-reads the entry), not propagate it: failing
/// there turns one bad tail record into a shard, and by default a node, that cannot start.
#[tokio::test(flavor = "multi_thread")]
async fn test_wal_replay_tolerates_corrupt_tail_entry() {
    let _ = env_logger::builder().is_test(true).try_init();
    let collection_dir = Builder::new().prefix("test_collection").tempdir().unwrap();

    let config = create_collection_config();

    let collection_name = "test".to_string();

    let update_runtime = Handle::current();
    let current_runtime: AdaptiveSearchHandle = AdaptiveSearchHandle::current_for_tests();

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
        update_runtime.clone(),
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
            .update(op.into(), WaitUntil::Visible, None, hw_acc.clone())
            .await
            .unwrap();
    }

    // Stop the shard without flush to preserve WAL.
    shard.stop_gracefully().await;

    // Lower applied_seq (as in `test_wal_replay_loads_pending_to_queue`) so the entries past it
    // are deferred to the update queue instead of replayed synchronously.
    let applied_seq_handler = AppliedSeqHandler::load_or_init(collection_dir.path(), total_ops);
    let current_applied_seq = applied_seq_handler.op_num().unwrap_or(0);
    let low_applied_seq = total_ops.saturating_sub(100);
    if current_applied_seq > low_applied_seq {
        applied_seq_handler
            .force_set_and_persist(low_applied_seq)
            .unwrap();
    }

    // Append a record that fails `OperationWithClockTag` deserialization into the deferred tail:
    // a CBOR-encoded string gets valid WAL framing, but decodes as neither of the record codecs.
    {
        let wal_path = LocalShard::wal_path(collection_dir.path());
        let mut raw_wal: SerdeWal<String> =
            SerdeWal::new(&wal_path, (&config.wal_config).into()).unwrap();
        raw_wal
            .write(&WalRawRecord::new(&"not an operation".to_string()).unwrap())
            .unwrap();
    }

    // Shard load must survive the corrupt tail entry.
    let shard = LocalShard::load(
        0,
        collection_name,
        collection_dir.path(),
        Arc::new(RwLock::new(config.clone())),
        config.optimizer_config.clone(),
        shared_storage_config,
        payload_index_schema,
        false,
        update_runtime.clone(),
        current_runtime.clone(),
        ResourceBudget::default(),
    )
    .await
    .expect("shard load must tolerate a corrupt WAL entry in the deferred tail");

    // The update worker must drain the queued tail past the corrupt entry (it skips it the same
    // way: log and continue).
    let timeout = std::time::Duration::from_secs(2);
    let start = std::time::Instant::now();
    let poll_interval = std::time::Duration::from_millis(10);

    while shard.local_update_queue_info().await.length > 0 {
        assert!(
            start.elapsed() <= timeout,
            "Timeout waiting for update queue to empty"
        );
        tokio::time::sleep(poll_interval).await;
    }

    // Channel length reaching zero only means the last operation was received,
    // not that it finished applying; wait for the worker to go idle.
    shard.plunge_async().await.unwrap().await.unwrap();

    // Every readable operation around the corrupt entry is applied.
    let info = shard.info().await.unwrap();
    let points_count = info.points_count.unwrap_or(0);
    assert_eq!(
        points_count, total_ops as usize,
        "All {total_ops} points should be present after WAL replay with a corrupt tail entry",
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

    let update_runtime = Handle::current();
    let current_runtime: AdaptiveSearchHandle = AdaptiveSearchHandle::current_for_tests();

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
        update_runtime.clone(),
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
            .update(op.into(), WaitUntil::Visible, None, hw_acc.clone())
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
        update_runtime.clone(),
        current_runtime.clone(),
        ResourceBudget::default(),
    )
    .await
    .unwrap();

    // Wait for update worker to process all queued operations with a timeout
    let timeout = std::time::Duration::from_secs(2);
    let start = std::time::Instant::now();
    let poll_interval = std::time::Duration::from_millis(10);

    while shard.local_update_queue_info().await.length > 0 {
        assert!(
            start.elapsed() <= timeout,
            "Timeout waiting for update queue to empty"
        );
        tokio::time::sleep(poll_interval).await;
    }

    // Channel length reaching zero only means the last operation was received,
    // not that it finished applying; wait for the worker to go idle.
    shard.plunge_async().await.unwrap().await.unwrap();

    // Verify all points are present after processing
    let info = shard.info().await.unwrap();
    let points_count = info.points_count.unwrap_or(0);
    assert_eq!(
        points_count, total_ops as usize,
        "All {total_ops} points should be present after WAL replay and update queue processing",
    );

    shard.stop_gracefully().await;
}

/// Filter-resolving operations must be rewritten to id-based operations
/// before they are written to the WAL, so WAL replay applies exactly the same
/// point set as the original run (issue #9575).
#[tokio::test(flavor = "multi_thread")]
async fn test_filter_ops_resolved_to_ids_in_wal() {
    use ahash::AHashSet;
    use segment::types::{Condition, Filter};
    use shard::operations::point_ops::{ConditionalInsertOperationInternal, UpdateMode};
    use shard::operations::{ClockTag, OperationWithClockTag};

    let _ = env_logger::builder().is_test(true).try_init();
    let collection_dir = Builder::new().prefix("test_collection").tempdir().unwrap();

    let config = create_collection_config();
    let collection_name = "test".to_string();

    let update_runtime = Handle::current();
    let current_runtime: AdaptiveSearchHandle = AdaptiveSearchHandle::current_for_tests();

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
        update_runtime.clone(),
        current_runtime.clone(),
        ResourceBudget::default(),
        config.optimizer_config.clone(),
    )
    .await
    .unwrap();

    let hw_acc = HwMeasurementAcc::new();

    // Points 1..=5
    shard
        .update(
            upsert_operation().into(),
            WaitUntil::Visible,
            None,
            hw_acc.clone(),
        )
        .await
        .unwrap();

    // A conditional insert-only upsert touching an existing point (1) and a
    // new point (6): only the new point may reach the WAL.
    let conditional = CollectionUpdateOperations::PointOperation(
        PointOperations::UpsertPointsConditional(ConditionalInsertOperationInternal {
            points_op: PointInsertOperationsInternal::PointsList(vec![
                PointStructPersisted {
                    id: 1.into(),
                    vector: VectorStructInternal::from(vec![9.0, 9.0, 9.0, 9.0]).into(),
                    payload: None,
                },
                PointStructPersisted {
                    id: 6.into(),
                    vector: VectorStructInternal::from(vec![6.0, 6.0, 6.0, 6.0]).into(),
                    payload: None,
                },
            ]),
            condition: filter_single_id(1),
            update_mode: Some(UpdateMode::InsertOnly),
        }),
    );
    shard
        .update(conditional.into(), WaitUntil::Visible, None, hw_acc.clone())
        .await
        .unwrap();

    // Delete-by-filter matching points 2 and 3, submitted with a clock tag:
    // the rewritten record must reuse it (one tag covers exactly one record,
    // and WAL-delta recovery relies on the tag surviving the rewrite).
    let delete_clock_tag = ClockTag::new_with_token(1, 0, 1, 42);
    let delete_by_filter = CollectionUpdateOperations::PointOperation(
        PointOperations::DeletePointsByFilter(Filter::new_must(Condition::HasId(
            AHashSet::from([2.into(), 3.into()]).into(),
        ))),
    );
    shard
        .update(
            OperationWithClockTag::new(delete_by_filter, Some(delete_clock_tag)),
            WaitUntil::Visible,
            None,
            hw_acc.clone(),
        )
        .await
        .unwrap();

    // The WAL must contain only id-based operations: the conditional upsert
    // reduced to the surviving point, the filter delete to the matched ids.
    let wal_records = shard.read_all_wal_operations().await;
    let mut saw_resolved_upsert = false;
    let mut saw_resolved_delete = false;
    for (_op_num, record) in &wal_records {
        assert!(
            !shard::resolve::is_filter_resolving(&record.operation),
            "filter-resolving operation reached the WAL: {:?}",
            record.operation,
        );
        if let CollectionUpdateOperations::PointOperation(PointOperations::UpsertPoints(op)) =
            &record.operation
            && op.point_ids() == vec![6.into()]
        {
            saw_resolved_upsert = true;
        }
        if let CollectionUpdateOperations::PointOperation(PointOperations::DeletePoints { ids }) =
            &record.operation
            && *ids == vec![2.into(), 3.into()]
        {
            saw_resolved_delete = true;
            assert_eq!(
                record.clock_tag,
                Some(delete_clock_tag),
                "rewritten record must reuse the incoming operation's clock tag",
            );
        }
    }
    assert!(
        saw_resolved_upsert,
        "conditional upsert was not rewritten to a plain upsert of the new point: {wal_records:?}",
    );
    assert!(
        saw_resolved_delete,
        "delete-by-filter was not rewritten to a delete of the matched ids: {wal_records:?}",
    );

    shard.stop_gracefully().await;

    // Reload: replay applies the recorded ids; state must match the original run.
    let shard = LocalShard::load(
        0,
        collection_name,
        collection_dir.path(),
        Arc::new(RwLock::new(config.clone())),
        config.optimizer_config.clone(),
        Arc::new(Default::default()),
        payload_index_schema,
        true,
        update_runtime.clone(),
        current_runtime.clone(),
        ResourceBudget::default(),
    )
    .await
    .unwrap();

    let request = Arc::new(PointRequestInternal {
        ids: (1..=6u64).map(Into::into).collect(),
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
            DeferredBehavior::VisibleOnly,
        )
        .await
        .unwrap();

    let present: Vec<_> = retrieved.iter().map(|record| record.id).collect();
    assert_eq!(
        present,
        vec![1.into(), 4.into(), 5.into(), 6.into()],
        "replayed state diverged from the original run",
    );

    shard.stop_gracefully().await;
}

/// A WAL written before filter operations were resolved at submit time may
/// still contain filter-carrying records after an upgrade. The by-filter
/// apply paths are kept so such records replay one final time with the old
/// apply semantics.
#[tokio::test(flavor = "multi_thread")]
async fn test_old_wal_filter_op_replays_with_apply_semantics() {
    use ahash::AHashSet;
    use segment::types::{Condition, Filter};
    use shard::operations::OperationWithClockTag;

    let _ = env_logger::builder().is_test(true).try_init();
    let collection_dir = Builder::new().prefix("test_collection").tempdir().unwrap();

    let config = create_collection_config();
    let collection_name = "test".to_string();

    let update_runtime = Handle::current();
    let current_runtime: AdaptiveSearchHandle = AdaptiveSearchHandle::current_for_tests();

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
        update_runtime.clone(),
        current_runtime.clone(),
        ResourceBudget::default(),
        config.optimizer_config.clone(),
    )
    .await
    .unwrap();

    let hw_acc = HwMeasurementAcc::new();

    // Points 1..=5, applied in this run.
    shard
        .update(
            upsert_operation().into(),
            WaitUntil::Visible,
            None,
            hw_acc.clone(),
        )
        .await
        .unwrap();

    // Emulate an old-version WAL: append a raw DeletePointsByFilter record
    // directly, bypassing submit-time resolution. It is never applied in
    // this run, like a filter op fsynced right before a crash on the old
    // version.
    let delete_by_filter = CollectionUpdateOperations::PointOperation(
        PointOperations::DeletePointsByFilter(Filter::new_must(Condition::HasId(
            AHashSet::from([2.into(), 3.into()]).into(),
        ))),
    );
    shard
        .append_raw_wal_operation(&OperationWithClockTag::new(delete_by_filter, None))
        .await;

    // The WAL genuinely contains an unresolved filter record.
    let wal_records = shard.read_all_wal_operations().await;
    assert!(
        wal_records
            .iter()
            .any(|(_, record)| shard::resolve::is_filter_resolving(&record.operation)),
        "old-style filter record missing from the WAL: {wal_records:?}",
    );

    shard.stop_gracefully().await;

    // Reload: replay must apply the filter record through the kept by-filter
    // apply path.
    let shard = LocalShard::load(
        0,
        collection_name,
        collection_dir.path(),
        Arc::new(RwLock::new(config.clone())),
        config.optimizer_config.clone(),
        Arc::new(Default::default()),
        payload_index_schema,
        true,
        update_runtime.clone(),
        current_runtime.clone(),
        ResourceBudget::default(),
    )
    .await
    .unwrap();

    // Barrier: replayed records past the synchronous window are enqueued to
    // the update worker; a wait-visible no-op serializes behind them.
    let barrier = CollectionUpdateOperations::PointOperation(PointOperations::DeletePoints {
        ids: vec![999.into()],
    });
    shard
        .update(barrier.into(), WaitUntil::Visible, None, hw_acc.clone())
        .await
        .unwrap();

    let request = Arc::new(PointRequestInternal {
        ids: (1..=5u64).map(Into::into).collect(),
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
            DeferredBehavior::VisibleOnly,
        )
        .await
        .unwrap();

    let present: Vec<_> = retrieved.iter().map(|record| record.id).collect();
    assert_eq!(
        present,
        vec![1.into(), 4.into(), 5.into()],
        "old-style filter record was not replayed with the by-filter apply semantics",
    );

    shard.stop_gracefully().await;
}
