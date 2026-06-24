use std::collections::HashSet;
use std::io::Read as _;
use std::sync::Arc;

use common::flags::{FeatureFlags, init_feature_flags};
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

/// With the `write_segment_manifest` flag enabled, a shard snapshot includes the segment manifest
/// (`segments_manifest.json`, next to the `segments/` directory) listing every snapshotted segment
/// as `active`.
#[test]
#[allow(clippy::field_reassign_with_default)]
fn test_snapshot_includes_segment_manifest() {
    let mut flags = FeatureFlags::default();
    flags.write_segment_manifest = true;
    init_feature_flags(flags);

    // Another test in this process may have initialized the feature flags first; the manifest is
    // only written when the flag is actually enabled.
    if !common::flags::feature_flags().write_segment_manifest {
        return;
    }

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
