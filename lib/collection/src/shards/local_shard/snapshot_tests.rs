use std::collections::HashSet;
use std::io::Read as _;
use std::sync::Arc;

use common::save_on_disk::SaveOnDisk;
use common::tar_ext;
use fs_err::File;
use segment::entry::ReadSegmentEntry as _;
use segment::types::SnapshotFormat;
use shard::files::{SEGMENT_MANIFEST_FILE, SEGMENTS_PATH};
use shard::fixtures::{build_segment_1, build_segment_2};
use shard::payload_index_schema::PayloadIndexSchema;
use shard::segment_holder::SegmentHolder;
use shard::segment_holder::locked::LockedSegmentHolder;
use shard::segment_manifest::{SegmentManifestState, SegmentsManifest};
use tempfile::Builder;

use crate::shards::local_shard::snapshot::snapshot_all_segments;
use crate::tests::fixtures::init_test_feature_flags;

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

/// With the `write_segment_manifest` flag enabled, a shard snapshot includes the segment manifest
/// (`segments_manifest.json`, next to the `segments/` directory) listing every snapshotted segment
/// as `active`.
#[test]
fn test_snapshot_includes_segment_manifest() {
    init_test_feature_flags();

    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let segment1 = build_segment_1(dir.path());
    let segment2 = build_segment_2(dir.path());

    let expected_uuids = [segment1.segment_uuid(), segment2.segment_uuid()]
        .into_iter()
        .collect::<HashSet<_>>();

    // The manifest is written to `<shard>/segments_manifest.json`, next to the `segments/`
    // directory, so that directory must exist for the segments themselves.
    fs_err::create_dir_all(dir.path().join(SEGMENTS_PATH)).unwrap();

    let mut holder = SegmentHolder::builder();
    holder.add_new(segment1);
    holder.add_new(segment2);
    let holder = holder.build(dir.path()).unwrap();

    let holder = LockedSegmentHolder::new(holder);

    let segments_dir = Builder::new().prefix("segments_dir").tempdir().unwrap();
    let temp_dir = Builder::new().prefix("temp_dir").tempdir().unwrap();
    let snapshot_file = Builder::new().suffix(".snapshot.tar").tempfile().unwrap();
    let tar = tar_ext::BuilderExt::new_seekable_owned(File::create(snapshot_file.path()).unwrap());

    let payload_schema_file = dir.path().join("payload.schema");
    let schema: Arc<SaveOnDisk<PayloadIndexSchema>> =
        Arc::new(SaveOnDisk::load_or_init_default(payload_schema_file).unwrap());

    // Write the segment manifest next to `segments/` at the snapshot root, mirroring how the local
    // shard snapshot is produced.
    let segment_manifest = holder
        .read()
        .segment_manifest_for_snapshot()
        .expect("manifest must be present when flag is enabled");
    let segment_manifest_json = serde_json::to_vec(&segment_manifest).unwrap();
    tar.blocking_append_data(
        &segment_manifest_json,
        std::path::Path::new(SEGMENT_MANIFEST_FILE),
    )
    .unwrap();

    snapshot_all_segments(
        holder.clone(),
        segments_dir.path(),
        None,
        schema,
        None,
        temp_dir.path(),
        // Descend into `segments/`, mirroring how the local shard snapshot is produced.
        &tar.descend(std::path::Path::new(SEGMENTS_PATH)).unwrap(),
        SnapshotFormat::Regular,
        None,
        None,
    )
    .unwrap();

    // The manifest sits at the snapshot root, next to (not inside) `segments/`.
    let manifest_entry_path = SEGMENT_MANIFEST_FILE.to_string();

    let mut tar = tar::Archive::new(File::open(snapshot_file.path()).unwrap());
    let mut manifest_bytes = None;
    for entry in tar.entries_with_seek().unwrap() {
        let mut entry = entry.unwrap();
        let path = entry.path().unwrap().to_string_lossy().into_owned();
        if path == manifest_entry_path {
            let mut buf = Vec::new();
            entry.read_to_end(&mut buf).unwrap();
            manifest_bytes = Some(buf);
        }
    }

    let manifest_bytes =
        manifest_bytes.expect("snapshot must contain segments_manifest.json when flag is enabled");
    let manifest: SegmentsManifest = serde_json::from_slice(&manifest_bytes).unwrap();

    let manifest_uuids = manifest
        .iter()
        .map(|(uuid, _)| *uuid)
        .collect::<HashSet<_>>();
    assert_eq!(
        manifest_uuids, expected_uuids,
        "manifest must list exactly the snapshotted segments",
    );
    for (_uuid, state) in manifest.iter() {
        assert_eq!(*state, SegmentManifestState::Active);
    }
}

/// A snapshot that includes the WAL pins the WAL acknowledge before it copies the segment files,
/// and holds that pin until the WAL is archived. Operations applied in between are persisted by
/// the proxies — into their pending changes logs while installed, into the wrapped segments once
/// unproxied — so a flush pass reports them durable and would otherwise have them acknowledged
/// away. The copied segment files predate them, so they only survive in the archived WAL: the pin
/// is what keeps them there.
///
/// This pins exactly as [`LocalShard::get_snapshot_creator`] does, then checks the version the
/// flush worker would acknowledge.
#[tokio::test(flavor = "multi_thread")]
async fn test_wal_snapshot_pin_keeps_changes_made_during_copy_replayable() {
    use common::budget::ResourceBudget;
    use common::counter::hardware_accumulator::HwMeasurementAcc;
    use segment::data_types::vectors::VectorStructInternal;
    use shard::operations::CollectionUpdateOperations;
    use shard::operations::point_ops::{
        PointInsertOperationsInternal, PointOperations, PointStructPersisted,
    };
    use shard::segment_holder::FlushMode;
    use tokio::runtime::Handle;
    use tokio::sync::RwLock;

    use crate::common::adaptive_handle::AdaptiveSearchHandle;
    use crate::shards::local_shard::LocalShard;
    use crate::shards::shard_trait::{ShardOperation, WaitUntil};
    use crate::tests::fixtures::{create_collection_config, delete_point_operation};
    use crate::update_workers::flush_workers::wal_ack_version;

    init_test_feature_flags();

    let collection_dir = Builder::new().prefix("wal_snapshot_pin").tempdir().unwrap();
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
        payload_index_schema,
        Handle::current(),
        AdaptiveSearchHandle::current_for_tests(),
        ResourceBudget::default(),
        config.optimizer_config.clone(),
    )
    .await
    .unwrap();

    // Keep flushing and WAL acknowledging under the test's control
    shard.stop_flush_worker().await;

    let hw_acc = HwMeasurementAcc::new();

    // Insert points; WAL indices: fake operation at 0, then one entry per upsert (1..=total)
    let total_points = 10u64;
    for i in 0..total_points {
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

    // Pin the WAL acknowledge exactly as a WAL-including snapshot does, before the segment files
    // would be copied
    let wal_ack_pins = shard.update_handler.lock().await.wal_ack_pins.clone();
    let wal_first_index = shard.wal.wal.lock().await.first_index();
    let pin = wal_ack_pins.pin(wal_first_index);

    // A change that lands while the snapshot runs, and is made durable by a flush
    let delete_op_num = total_points + 1;
    shard
        .update(
            delete_point_operation(3).into(),
            WaitUntil::Visible,
            None,
            hw_acc.clone(),
        )
        .await
        .unwrap();
    let confirmed = shard
        .segments()
        .read()
        .flush_all(FlushMode::Sync, true)
        .unwrap();
    assert!(
        confirmed >= delete_op_num,
        "the delete must be durable ({confirmed} >= {delete_op_num}), or this proves nothing",
    );

    // While the pin is held the delete must stay in the WAL, however durable it is
    let acknowledged = wal_ack_version(confirmed, &wal_ack_pins);
    assert!(
        acknowledged.is_none_or(|acknowledged| acknowledged < delete_op_num),
        "the snapshot copied the segment files before the delete at {delete_op_num}, but the WAL \
         would be acknowledged at {acknowledged:?}, so a WAL copied afterwards cannot replay it",
    );

    // Releasing the pin, as the snapshot does once the WAL is archived, lifts the hold
    drop(pin);
    assert_eq!(
        wal_ack_version(confirmed, &wal_ack_pins),
        Some(confirmed),
        "releasing the snapshot's pin must let the acknowledge catch up",
    );
}

/// A WAL acknowledge pin at index 0 makes `wal_ack_version` return `None`, and the flush worker
/// returns right there, above `clocks.store_if_changed`. The pin a WAL-including snapshot takes
/// is `wal.first_index()`, which is 0 on a shard that never acknowledged, so it suppresses clock
/// persistence for as long as it is held instead of only holding back the acknowledge.
#[tokio::test(flavor = "multi_thread")]
async fn test_wal_ack_pin_at_zero_does_not_suppress_clock_persistence() {
    use common::budget::ResourceBudget;
    use common::counter::hardware_accumulator::HwMeasurementAcc;
    use segment::data_types::vectors::VectorStructInternal;
    use shard::files::NEWEST_CLOCKS_PATH;
    use shard::operations::point_ops::{
        PointInsertOperationsInternal, PointOperations, PointStructPersisted,
    };
    use shard::operations::{ClockTag, CollectionUpdateOperations, OperationWithClockTag};
    use tokio::runtime::Handle;
    use tokio::sync::RwLock;

    use crate::common::adaptive_handle::AdaptiveSearchHandle;
    use crate::shards::local_shard::LocalShard;
    use crate::shards::shard_trait::{ShardOperation, WaitUntil};
    use crate::tests::fixtures::create_collection_config;
    use crate::update_workers::UpdateWorkers;

    init_test_feature_flags();

    let collection_dir = Builder::new()
        .prefix("wal_ack_pin_clocks")
        .tempdir()
        .unwrap();
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
        payload_index_schema,
        Handle::current(),
        AdaptiveSearchHandle::current_for_tests(),
        ResourceBudget::default(),
        config.optimizer_config.clone(),
    )
    .await
    .unwrap();

    shard.stop_flush_worker().await;

    let hw_acc = HwMeasurementAcc::new();
    let upsert = |id: u64, tick: u64| {
        let point = PointStructPersisted {
            id: id.into(),
            vector: VectorStructInternal::from(vec![1.0, 2.0, 3.0, 4.0]).into(),
            payload: None,
        };
        let op = CollectionUpdateOperations::PointOperation(PointOperations::UpsertPoints(
            PointInsertOperationsInternal::PointsList(vec![point]),
        ));
        OperationWithClockTag::new(op, Some(ClockTag::new(1, 0, tick)))
    };

    shard
        .update(upsert(1, 1), WaitUntil::Visible, None, hw_acc.clone())
        .await
        .unwrap();

    let shard_path = shard.path.clone();
    let newest_clocks = shard_path.join(NEWEST_CLOCKS_PATH);
    assert!(
        !newest_clocks.exists(),
        "clocks must not be persisted yet, or this proves nothing",
    );

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

    let flush_pass = || {
        let segments = segments.clone();
        let wal = wal.clone();
        let wal_ack_pins = wal_ack_pins.clone();
        let clocks = clocks.clone();
        let shard_path = shard_path.clone();
        let applied_seq_handler = applied_seq_handler.clone();
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
    };

    // Pin exactly as a WAL-including snapshot does on a shard that never acknowledged
    let wal_first_index = wal.lock().await.first_index();
    assert_eq!(
        wal_first_index, 0,
        "setup must reproduce the pin-at-zero case",
    );
    let pin = wal_ack_pins.pin(wal_first_index);
    flush_pass().await.unwrap();
    let clocks_stored_while_pinned = newest_clocks.exists();

    // Same flush pass, same pending clock change, only the pin released
    drop(pin);
    shard
        .update(upsert(2, 2), WaitUntil::Visible, None, hw_acc.clone())
        .await
        .unwrap();
    flush_pass().await.unwrap();

    assert!(
        newest_clocks.exists(),
        "control: without a pin the flush worker must persist the clock maps",
    );
    assert!(
        clocks_stored_while_pinned,
        "a WAL acknowledge pin must hold back the acknowledge only, but it also skipped \
         persisting the clock maps for as long as it was held",
    );
}
