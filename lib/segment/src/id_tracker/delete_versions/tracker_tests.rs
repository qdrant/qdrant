//! Every writer of an immutable or disk-resident segment's deleted mask keeps the delete log
//! holding every delete.

use std::path::Path;

use common::types::{DeferredBehavior, PointOffsetType};
use common::universal_io::{MmapFile, MmapFs};
use tempfile::Builder;

use super::{DeleteLogEntry, DeleteLogReader, DeleteLogWriter, DeleteVersioning, delete_log_path};
use crate::id_tracker::compressed::compressed_point_mappings::CompressedPointMappings;
use crate::id_tracker::disk_id_tracker::DiskIdTracker;
use crate::id_tracker::disk_id_tracker::update_only::UpdateOnlyDiskIdTracker;
use crate::id_tracker::immutable_id_tracker::ImmutableIdTracker;
use crate::id_tracker::immutable_id_tracker::update_only::UpdateOnlyImmutableIdTracker;
use crate::id_tracker::in_memory_id_tracker::InMemoryIdTracker;
use crate::id_tracker::{IdTracker, IdTrackerRead};
use crate::types::{PointIdType, SeqNumberType};

const POINTS: u64 = 10;

fn num(id: u64) -> PointIdType {
    PointIdType::NumId(id)
}

fn entry(slot: PointOffsetType, version: SeqNumberType) -> DeleteLogEntry {
    DeleteLogEntry { slot, version }
}

/// Points 0 to 9 at slots 0 to 9, all at version 3, point 2 deleted.
fn data() -> (Vec<SeqNumberType>, CompressedPointMappings) {
    let mut in_memory = InMemoryIdTracker::new();
    for id in 0..POINTS {
        in_memory.set_link(num(id), id as PointOffsetType).unwrap();
        in_memory
            .set_internal_version(id as PointOffsetType, 3)
            .unwrap();
    }
    in_memory.drop(num(2)).unwrap();
    let (versions, mappings) = in_memory.into_internal();
    (versions, CompressedPointMappings::from_mappings(mappings))
}

fn build_immutable(path: &Path) -> ImmutableIdTracker<MmapFile> {
    let (versions, mappings) = data();
    ImmutableIdTracker::new(&MmapFs, path, &versions, mappings).unwrap()
}

fn build_disk(path: &Path) -> DiskIdTracker<MmapFile> {
    let (versions, mappings) = data();
    DiskIdTracker::new(&MmapFs, path, &versions, mappings).unwrap()
}

fn read_log(path: &Path) -> Vec<DeleteLogEntry> {
    let mut reader = DeleteLogReader::<MmapFile>::new(path);
    let entries = reader.read_new(&MmapFs).unwrap();
    assert!(reader.is_started());
    entries
}

fn start_log(path: &Path) {
    // The mask's current deletes, as a recording writer would start the log with
    let mut writer = DeleteLogWriter::new(path);
    let mut mask = common::bitvec::BitVec::repeat(false, POINTS as usize);
    mask.set(2, true);
    writer
        .append(&MmapFs, DeleteVersioning::Record, &mask, &[])
        .unwrap();
}

/// An update-only writer that records versions starts the log with the mask's deletes, then logs
/// its own with their version, and sets them in the mask too.
#[test]
fn update_only_trackers_log_ahead_of_the_mask() {
    for disk in [false, true] {
        let dir = Builder::new().prefix("deletes").tempdir().unwrap();
        if disk {
            drop(build_disk(dir.path()));
        } else {
            drop(build_immutable(dir.path()));
        }
        assert!(!delete_log_path(dir.path()).exists(), "built without a log");

        let points = [(num(5), 5), (num(7), 7)];
        if disk {
            let mut writer = UpdateOnlyDiskIdTracker::new(dir.path(), None).unwrap();
            writer
                .tombstone_points(&MmapFs, &points, 11, DeleteVersioning::Record)
                .unwrap();
            writer
                .tombstone_points(&MmapFs, &[(num(8), 8)], 12, DeleteVersioning::Legacy)
                .unwrap();
        } else {
            let mut writer = UpdateOnlyImmutableIdTracker::new(dir.path(), None).unwrap();
            writer
                .tombstone_points(&MmapFs, &points, 11, DeleteVersioning::Record)
                .unwrap();
            writer
                .tombstone_points(&MmapFs, &[(num(8), 8)], 12, DeleteVersioning::Legacy)
                .unwrap();
        }

        assert_eq!(
            read_log(dir.path()),
            [entry(2, 0), entry(5, 11), entry(7, 11), entry(8, 12)],
            "disk: {disk}",
        );
        let reopened = ImmutableIdTracker::<MmapFile>::open(&MmapFs, dir.path());
        let deleted: Vec<_> = if disk {
            let reopened = DiskIdTracker::<MmapFile>::open(
                &MmapFs,
                dir.path(),
                common::universal_io::Populate::No,
            )
            .unwrap();
            (0..POINTS as PointOffsetType)
                .filter(|&slot| reopened.is_deleted_point(slot))
                .collect()
        } else {
            let reopened = reopened.unwrap();
            (0..POINTS as PointOffsetType)
                .filter(|&slot| reopened.is_deleted_point(slot))
                .collect()
        };
        assert_eq!(deleted, [2, 5, 7, 8], "disk: {disk}");
    }
}

/// A legacy update-only writer leaves a segment without a log without one.
#[test]
fn legacy_update_only_writer_starts_no_log() {
    let dir = Builder::new().prefix("deletes").tempdir().unwrap();
    drop(build_immutable(dir.path()));
    let mut writer = UpdateOnlyImmutableIdTracker::new(dir.path(), None).unwrap();
    writer
        .tombstone_points(&MmapFs, &[(num(5), 5)], 11, DeleteVersioning::Legacy)
        .unwrap();
    assert!(!delete_log_path(dir.path()).exists());
}

/// A regular tracker deleting in a segment with a log logs the delete on flush, with the version
/// the delete stamped on the point, and lists the log among its files.
#[test]
fn regular_trackers_log_their_deletes() {
    let dir = Builder::new().prefix("deletes").tempdir().unwrap();
    let tracker = build_immutable(dir.path());
    assert_regular_tracker_logs(dir.path(), tracker);

    let dir = Builder::new().prefix("deletes").tempdir().unwrap();
    let tracker = build_disk(dir.path());
    assert_regular_tracker_logs(dir.path(), tracker);
}

fn assert_regular_tracker_logs(path: &Path, mut tracker: impl IdTracker) {
    assert!(!tracker.files().contains(&delete_log_path(path)));
    start_log(path);
    assert!(tracker.files().contains(&delete_log_path(path)));

    // As a segment deletes: drop the point, then stamp its slot with the operation number
    let slot = tracker
        .internal_id_with_behavior(num(6), DeferredBehavior::VisibleOnly)
        .unwrap();
    tracker.drop(num(6)).unwrap();
    tracker.set_internal_version(slot, 21).unwrap();
    tracker.mapping_flusher()().unwrap();
    tracker.versions_flusher()().unwrap();

    // Nothing new, nothing logged
    tracker.mapping_flusher()().unwrap();

    assert_eq!(read_log(path), [entry(2, 0), entry(6, 21)]);
}
