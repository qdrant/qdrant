use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use common::budget::ResourceBudget;
use common::save_on_disk::SaveOnDisk;
use common::tar_ext;
use fs_err::File;
use segment::common::operation_error::OperationError;
use segment::types::SnapshotFormat;
use shard::fixtures::{build_segment_1, build_segment_2};
use shard::payload_index_schema::PayloadIndexSchema;
use shard::segment_holder::SegmentHolder;
use shard::segment_holder::locked::LockedSegmentHolder;
use tempfile::Builder;
use tokio::runtime::Handle;

use crate::common::adaptive_handle::AdaptiveSearchHandle;
use crate::operations::types::CollectionError;
use crate::shards::local_shard::LocalShard;
use crate::shards::local_shard::snapshot::{proxy_all_segments_and_apply, snapshot_all_segments};
use crate::tests::fixtures::create_collection_config;

#[test]
fn test_snapshot_all() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let segment1 = build_segment_1(dir.path());
    let segment2 = build_segment_2(dir.path());

    let mut holder = SegmentHolder::default();

    let sid1 = holder.add_new(segment1);
    let sid2 = holder.add_new(segment2);
    assert_ne!(sid1, sid2);

    let holder = LockedSegmentHolder::new(holder);

    let before_ids = holder
        .read()
        .iter()
        .map(|(id, _)| id)
        .collect::<HashSet<_>>();

    let segments_dir = Builder::new().prefix("segments_dir").tempdir().unwrap();
    let temp_dir = Builder::new().prefix("temp_dir").tempdir().unwrap();
    let snapshot_file = Builder::new().suffix(".snapshot.tar").tempfile().unwrap();
    let tar = tar_ext::BuilderExt::new_seekable_owned(File::create(snapshot_file.path()).unwrap());

    let payload_schema_file = dir.path().join("payload.schema");
    let schema: Arc<SaveOnDisk<PayloadIndexSchema>> =
        Arc::new(SaveOnDisk::load_or_init_default(payload_schema_file).unwrap());

    snapshot_all_segments(
        holder.clone(),
        segments_dir.path(),
        None,
        schema,
        None,
        temp_dir.path(),
        &tar,
        SnapshotFormat::Regular,
        None,
    )
    .unwrap();

    let after_ids = holder
        .read()
        .iter()
        .map(|(id, _)| id)
        .collect::<HashSet<_>>();

    assert_eq!(
        before_ids, after_ids,
        "segment holder IDs before and after snapshotting must be equal",
    );

    let mut tar = tar::Archive::new(File::open(snapshot_file.path()).unwrap());
    let archive_count = tar.entries_with_seek().unwrap().count();
    // one archive produced per concrete segment in the SegmentHolder
    assert_eq!(archive_count, 2);
}

/// Taking the segment holder lock for snapshotting must be bounded: this wait happens on a
/// blocking thread, and an unbounded wait pins that thread for as long as the current lock
/// holder is stuck (e.g. a snapshot paced by a stalled remote consumer).
#[test]
fn test_snapshot_segment_holder_lock_timeout() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let segment = build_segment_1(dir.path());

    let mut holder = SegmentHolder::default();
    holder.add_new(segment);
    let holder = LockedSegmentHolder::new(holder);

    let segments_dir = Builder::new().prefix("segments_dir").tempdir().unwrap();

    let payload_schema_file = dir.path().join("payload.schema");
    let schema: Arc<SaveOnDisk<PayloadIndexSchema>> =
        Arc::new(SaveOnDisk::load_or_init_default(payload_schema_file).unwrap());

    // Hold the single upgradable slot, like a wedged snapshot or a running optimizer would
    let _guard = holder.upgradable_read();

    let result = proxy_all_segments_and_apply(
        holder.clone(),
        segments_dir.path(),
        None,
        schema,
        None,
        Duration::from_millis(100),
        |_segment| Ok(()),
    );

    assert!(
        matches!(result, Err(OperationError::Timeout { .. })),
        "expected timeout error, got {result:?}",
    );
}

/// Only one snapshot of a shard may be in flight at a time. A second snapshot attempt must wait
/// for the first one in async context (cancellable, threadless), not on the segment holder lock
/// inside a blocking task.
#[tokio::test(flavor = "multi_thread")]
async fn test_shard_snapshot_single_flight() {
    let collection_dir = Builder::new().prefix("test_collection").tempdir().unwrap();
    let config = create_collection_config();

    let payload_index_schema_dir = Builder::new().prefix("qdrant-test").tempdir().unwrap();
    let payload_index_schema_file = payload_index_schema_dir.path().join("payload-schema.json");
    let payload_index_schema =
        Arc::new(SaveOnDisk::load_or_init_default(payload_index_schema_file).unwrap());

    let shard = LocalShard::build(
        0,
        "test".to_string(),
        collection_dir.path(),
        Arc::new(tokio::sync::RwLock::new(config.clone())),
        Arc::new(Default::default()),
        payload_index_schema,
        Handle::current(),
        AdaptiveSearchHandle::current_for_tests(),
        ResourceBudget::default(),
        config.optimizer_config.clone(),
    )
    .await
    .unwrap();

    // Simulate an in-flight snapshot by taking the shard's snapshot permit
    let permit = shard
        .snapshot_semaphore
        .clone()
        .try_acquire_owned()
        .unwrap();

    let temp_dir = Builder::new().prefix("temp_dir").tempdir().unwrap();
    let snapshot_file = Builder::new().suffix(".snapshot.tar").tempfile().unwrap();
    let tar = tar_ext::BuilderExt::new_seekable_owned(File::create(snapshot_file.path()).unwrap());

    let snapshot_creator = shard
        .get_snapshot_creator(temp_dir.path(), &tar, SnapshotFormat::Regular, None, true)
        .await
        .unwrap();
    let mut snapshot_creator = std::pin::pin!(snapshot_creator);

    // While the permit is held, the snapshot must stay queued on the semaphore
    let queued = tokio::time::timeout(Duration::from_millis(300), &mut snapshot_creator).await;
    assert!(
        queued.is_err(),
        "second snapshot must wait for the in-flight one, got {queued:?}",
    );

    // Releasing the permit must let the queued snapshot proceed and succeed
    drop(permit);
    tokio::time::timeout(Duration::from_secs(30), snapshot_creator)
        .await
        .expect("queued snapshot did not proceed after the in-flight one finished")
        .unwrap();
}

/// Waiting for an in-flight snapshot is bounded: a queued snapshot fails with a transient
/// timeout error instead of waiting forever.
///
/// The shard is built on a regular runtime, but the snapshot creator future — pending only on
/// the semaphore wait and its timeout — runs on a paused-time runtime, which auto-advances
/// through the 10 minute wait instantly.
#[test]
fn test_shard_snapshot_permit_timeout() {
    let build_runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    let collection_dir = Builder::new().prefix("test_collection").tempdir().unwrap();
    let config = create_collection_config();

    let payload_index_schema_dir = Builder::new().prefix("qdrant-test").tempdir().unwrap();
    let payload_index_schema_file = payload_index_schema_dir.path().join("payload-schema.json");
    let payload_index_schema =
        Arc::new(SaveOnDisk::load_or_init_default(payload_index_schema_file).unwrap());

    let shard = build_runtime
        .block_on(LocalShard::build(
            0,
            "test".to_string(),
            collection_dir.path(),
            Arc::new(tokio::sync::RwLock::new(config.clone())),
            Arc::new(Default::default()),
            payload_index_schema,
            build_runtime.handle().clone(),
            AdaptiveSearchHandle::new_fixed(build_runtime.handle().clone()),
            ResourceBudget::default(),
            config.optimizer_config.clone(),
        ))
        .unwrap();

    // Simulate an in-flight snapshot that never finishes
    let _permit = shard
        .snapshot_semaphore
        .clone()
        .try_acquire_owned()
        .unwrap();

    let temp_dir = Builder::new().prefix("temp_dir").tempdir().unwrap();
    let snapshot_file = Builder::new().suffix(".snapshot.tar").tempfile().unwrap();
    let tar = tar_ext::BuilderExt::new_seekable_owned(File::create(snapshot_file.path()).unwrap());

    let snapshot_creator = build_runtime
        .block_on(shard.get_snapshot_creator(
            temp_dir.path(),
            &tar,
            SnapshotFormat::Regular,
            None,
            true,
        ))
        .unwrap();

    let paused_runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .unwrap();

    let result = paused_runtime.block_on(snapshot_creator);
    assert!(
        matches!(result, Err(CollectionError::Timeout { .. })),
        "queued snapshot must fail with a transient timeout, got {result:?}",
    );
}
