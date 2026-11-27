use std::sync::Arc;

use common::ambient::{self, AmbientContext, AmbientFutureExt};
use common::budget::ResourceBudget;
use common::save_on_disk::SaveOnDisk;
use segment::data_types::vectors::VectorStructInternal;
use segment::pending_changes::PersistedProxyChanges;
use shard::operations::point_ops::{
    PointInsertOperationsInternal, PointOperations, PointStructPersisted,
};
use shard::operations::{ClockTag, CollectionUpdateOperations, OperationWithClockTag};
use shard::segment_holder::FlushMode;
use tempfile::Builder;
use tokio::runtime::Handle;
use tokio::sync::RwLock;

use crate::common::adaptive_handle::AdaptiveSearchHandle;
use crate::shards::local_shard::LocalShard;
use crate::shards::shard_trait::{ShardOperation, WaitUntil};
use crate::tests::fixtures::{create_collection_config, init_test_feature_flags};
use crate::update_workers::UpdateWorkers;

fn upsert(id: u64, clock_tag: Option<ClockTag>) -> OperationWithClockTag {
    let point = PointStructPersisted {
        id: id.into(),
        vector: VectorStructInternal::from(vec![1.0, 2.0, 3.0, 4.0]).into(),
        payload: None,
    };

    let operation = CollectionUpdateOperations::PointOperation(PointOperations::UpsertPoints(
        PointInsertOperationsInternal::PointsList(vec![point]),
    ));

    OperationWithClockTag::new(operation, clock_tag)
}

/// An untagged record below the acknowledged WAL index still limits a WAL delta after a restart.
///
/// The shard doesn't read records below the acknowledged index on load, so it only knows about the
/// untagged record from the persisted value.
#[tokio::test(flavor = "multi_thread")]
async fn test_tagged_from_survives_restart() {
    let _scope = ambient::async_test_guard();
    init_test_feature_flags();

    let collection_dir = Builder::new().prefix("tagged_from").tempdir().unwrap();
    let config = create_collection_config();

    let payload_index_schema_dir = Builder::new().prefix("qdrant-test").tempdir().unwrap();
    let payload_index_schema = Arc::new(
        SaveOnDisk::load_or_init_default(
            payload_index_schema_dir.path().join("payload-schema.json"),
        )
        .unwrap(),
    );

    let shard = LocalShard::build(
        0,
        "test".to_string(),
        collection_dir.path(),
        Arc::new(RwLock::new(config.clone())),
        Arc::new(Default::default()),
        payload_index_schema.clone(),
        Handle::current(),
        AdaptiveSearchHandle::current_for_tests(),
        ResourceBudget::default(),
        config.optimizer_config.clone(),
    )
    .await
    .unwrap();

    shard.stop_flush_worker().await;

    // Record 0 is the untagged operation `build` inserts. Record 1 is untagged, records 2 to 4 are
    // tagged.
    let operations = [
        upsert(1, None),
        upsert(2, Some(ClockTag::new(1, 0, 1))),
        upsert(3, Some(ClockTag::new(1, 0, 2))),
        upsert(4, Some(ClockTag::new(1, 0, 3))),
    ];

    let ctx = AmbientContext::new();

    for operation in operations {
        shard
            .update(operation, WaitUntil::Visible, None)
            .measured(AmbientContext::clone(&ctx))
            .await
            .unwrap();
    }

    assert_eq!(shard.wal.tagged_records().await, 3);

    // Flush segments, persist clocks and acknowledge the WAL, like the flush worker. The flush
    // worker flushes segments in the background and confirms versions from an earlier flush, so
    // flush them first.
    shard
        .segments()
        .read()
        .flush_all(FlushMode::Sync, true)
        .unwrap();

    let (segments, wal, wal_ack_pins, clocks, applied_seq_handler) = {
        let update_handler = shard.update_handler.lock().await;
        (
            shard.segments().clone(),
            shard.wal.wal.clone(),
            update_handler.wal_ack_pins.clone(),
            update_handler.clocks.clone(),
            shard.applied_seq_handler.clone(),
        )
    };

    let shard_path = shard.path.clone();

    tokio::task::spawn_blocking(move || {
        UpdateWorkers::flush_worker_internal(
            segments,
            wal,
            wal_ack_pins,
            clocks,
            shard_path,
            applied_seq_handler,
        )
    })
    .await
    .unwrap();

    let first_index = shard.wal.wal.lock().await.first_index();
    assert!(
        first_index > 2,
        "WAL must be acknowledged past the untagged record, or this proves nothing",
    );

    shard.stop_gracefully().await;

    let shard = LocalShard::load(
        0,
        "test".to_string(),
        collection_dir.path(),
        Arc::new(RwLock::new(config.clone())),
        config.optimizer_config.clone(),
        Arc::new(Default::default()),
        payload_index_schema,
        true,
        PersistedProxyChanges::Replay,
        Handle::current(),
        AdaptiveSearchHandle::current_for_tests(),
        ResourceBudget::default(),
    )
    .await
    .unwrap();

    assert_eq!(shard.wal.tagged_records().await, 3);

    shard.stop_gracefully().await;
}
