use std::collections::HashSet;
use std::sync::Arc;

use common::save_on_disk::SaveOnDisk;
use common::tar_ext;
use fs_err::File;
use parking_lot::RwLock;
use segment::types::SnapshotFormat;
use shard::fixtures::{build_segment_1, build_segment_2};
use shard::payload_index_schema::PayloadIndexSchema;
use shard::segment_holder::SegmentHolder;
use tempfile::Builder;

use crate::shards::local_shard::snapshot::snapshot_all_segments;

#[test]
fn test_snapshot_all() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let segment1 = build_segment_1(dir.path());
    let segment2 = build_segment_2(dir.path());

    let mut holder = SegmentHolder::default();

    let sid1 = holder.add_new(segment1);
    let sid2 = holder.add_new(segment2);
    assert_ne!(sid1, sid2);

    let holder = Arc::new(RwLock::new(holder));

    let before_ids = holder
        .read()
        .iter()
        .map(|(id, _)| *id)
        .collect::<HashSet<_>>();

    let segments_dir = Builder::new().prefix("segments_dir").tempdir().unwrap();
    let temp_dir = Builder::new().prefix("temp_dir").tempdir().unwrap();
    let snapshot_file = Builder::new().suffix(".snapshot.tar").tempfile().unwrap();
    let tar = tar_ext::BuilderExt::new_seekable_owned(File::create(snapshot_file.path()).unwrap());

    let payload_schema_file = dir.path().join("payload.schema");
    let schema: Arc<SaveOnDisk<PayloadIndexSchema>> =
        Arc::new(SaveOnDisk::load_or_init_default(payload_schema_file).unwrap());

    let update_lock = Arc::new(tokio::sync::RwLock::new(()));

    snapshot_all_segments(
        holder.clone(),
        segments_dir.path(),
        None,
        schema,
        temp_dir.path(),
        &tar,
        SnapshotFormat::Regular,
        None,
        update_lock,
    )
    .unwrap();

    let after_ids = holder
        .read()
        .iter()
        .map(|(id, _)| *id)
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
