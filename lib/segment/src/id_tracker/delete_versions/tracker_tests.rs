//! A delete of an immutable or disk-resident segment goes to the delete log or to the mask, and
//! regular trackers read the union of the two.

use std::path::Path;

use common::mmap::AdviceSetting;
use common::stored_bitslice::StoredBitSlice;
use common::types::{DeferredBehavior, PointOffsetType};
use common::universal_io::{MmapFile, MmapFs, OpenOptions, Populate};
use tempfile::Builder;

use super::{DeleteLogEntry, DeleteLogReader, DeleteLogWriter, DeleteVersioning, delete_log_path};
use crate::id_tracker::compressed::compressed_point_mappings::CompressedPointMappings;
use crate::id_tracker::disk_id_tracker::DiskIdTracker;
use crate::id_tracker::disk_id_tracker::update_only::UpdateOnlyDiskIdTracker;
use crate::id_tracker::immutable_id_tracker::update_only::UpdateOnlyImmutableIdTracker;
use crate::id_tracker::immutable_id_tracker::{DELETED_FILE_NAME, ImmutableIdTracker};
use crate::id_tracker::in_memory_id_tracker::InMemoryIdTracker;
use crate::id_tracker::{IdTracker, IdTrackerEnum, IdTrackerRead};
use crate::types::{PointIdType, SeqNumberType};

const POINTS: u64 = 10;

fn num(id: u64) -> PointIdType {
    PointIdType::NumId(id)
}

fn entry(slot: PointOffsetType, version: SeqNumberType) -> DeleteLogEntry {
    DeleteLogEntry { slot, version }
}

#[derive(Clone, Copy, Debug)]
enum Format {
    Immutable,
    DiskResident,
}

const FORMATS: [Format; 2] = [Format::Immutable, Format::DiskResident];

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

fn build(format: Format, path: &Path) {
    let (versions, mappings) = data();
    match format {
        Format::Immutable => {
            drop(ImmutableIdTracker::<MmapFile>::new(&MmapFs, path, &versions, mappings).unwrap())
        }
        Format::DiskResident => {
            drop(DiskIdTracker::<MmapFile>::new(&MmapFs, path, &versions, mappings).unwrap())
        }
    }
}

fn open_regular(format: Format, path: &Path) -> IdTrackerEnum {
    match format {
        Format::Immutable => IdTrackerEnum::ImmutableIdTracker(
            ImmutableIdTracker::<MmapFile>::open(&MmapFs, path).unwrap(),
        ),
        Format::DiskResident => IdTrackerEnum::DiskIdTracker(
            DiskIdTracker::<MmapFile>::open(&MmapFs, path, Populate::No).unwrap(),
        ),
    }
}

fn deleted_slots(tracker: &impl IdTrackerRead) -> Vec<PointOffsetType> {
    (0..POINTS as PointOffsetType)
        .filter(|&slot| tracker.is_deleted_point(slot))
        .collect()
}

/// The slots the mask file of the segment at `path` holds.
fn mask(path: &Path) -> Vec<PointOffsetType> {
    StoredBitSlice::<MmapFile>::open(
        &MmapFs,
        path.join(DELETED_FILE_NAME),
        OpenOptions {
            writeable: false,
            need_sequential: false,
            populate: Populate::No,
            advice: AdviceSetting::Global,
        },
        (),
    )
    .unwrap()
    .read_all()
    .unwrap()
    .iter_ones()
    .map(|slot| slot as PointOffsetType)
    .filter(|&slot| slot < POINTS as PointOffsetType)
    .collect()
}

fn read_log(path: &Path) -> Vec<DeleteLogEntry> {
    DeleteLogReader::<MmapFile>::new(path)
        .read_new(&MmapFs)
        .unwrap()
}

fn tombstone(
    format: Format,
    path: &Path,
    points: &[(PointIdType, PointOffsetType)],
    version: SeqNumberType,
    versioning: DeleteVersioning,
) {
    match format {
        Format::Immutable => UpdateOnlyImmutableIdTracker::new(path, None)
            .unwrap()
            .tombstone_points(&MmapFs, points, version, versioning)
            .unwrap(),
        Format::DiskResident => UpdateOnlyDiskIdTracker::new(path, None)
            .unwrap()
            .tombstone_points(&MmapFs, points, version, versioning)
            .unwrap(),
    }
}

/// A versioned update-only writer appends to the log and leaves the mask alone, an
/// unversioned one sets the mask and writes no log. A regular tracker reads the union.
#[test]
fn update_only_trackers_write_the_log_or_the_mask() {
    for format in FORMATS {
        let dir = Builder::new().prefix("deletes").tempdir().unwrap();
        build(format, dir.path());
        assert!(!delete_log_path(dir.path()).exists(), "built without a log");

        let points = [(num(5), 5), (num(7), 7)];
        tombstone(format, dir.path(), &points, 11, DeleteVersioning::Versioned);
        tombstone(
            format,
            dir.path(),
            &[(num(8), 8)],
            12,
            DeleteVersioning::Unversioned,
        );

        assert_eq!(
            read_log(dir.path()),
            [entry(5, 11), entry(7, 11)],
            "{format:?}",
        );
        assert_eq!(mask(dir.path()), [2, 8], "{format:?}");
        assert_eq!(
            deleted_slots(&open_regular(format, dir.path())),
            [2, 5, 7, 8],
            "{format:?}",
        );
    }
}

/// An unversioned update-only writer starts no log.
#[test]
fn unversioned_update_only_writer_starts_no_log() {
    let dir = Builder::new().prefix("deletes").tempdir().unwrap();
    build(Format::Immutable, dir.path());
    tombstone(
        Format::Immutable,
        dir.path(),
        &[(num(5), 5)],
        11,
        DeleteVersioning::Unversioned,
    );
    assert!(!delete_log_path(dir.path()).exists());
}

/// A versioned update-only writer refuses a slot beyond the segment.
#[test]
fn versioned_writer_checks_slots() {
    for format in FORMATS {
        let dir = Builder::new().prefix("deletes").tempdir().unwrap();
        build(format, dir.path());
        let result = match format {
            Format::Immutable => UpdateOnlyImmutableIdTracker::new(dir.path(), None)
                .unwrap()
                .tombstone_points(&MmapFs, &[(num(99), 999)], 4, DeleteVersioning::Versioned),
            Format::DiskResident => UpdateOnlyDiskIdTracker::new(dir.path(), None)
                .unwrap()
                .tombstone_points(&MmapFs, &[(num(99), 999)], 4, DeleteVersioning::Versioned),
        };
        assert!(result.is_err(), "{format:?}");
        assert!(!delete_log_path(dir.path()).exists(), "{format:?}");
    }
}

/// A regular tracker applies the log's deletes at open, writes its own to the mask only, and lists
/// the log among its files.
#[test]
fn regular_trackers_read_the_union_and_write_the_mask() {
    for format in FORMATS {
        let dir = Builder::new().prefix("deletes").tempdir().unwrap();
        build(format, dir.path());
        DeleteLogWriter::new(dir.path())
            .append(&MmapFs, &[entry(4, 5)])
            .unwrap();

        let mut tracker = open_regular(format, dir.path());
        assert!(tracker.files().contains(&delete_log_path(dir.path())));
        assert!(tracker.is_deleted_point(4), "{format:?}");
        assert!(
            tracker
                .internal_id_with_behavior(num(4), DeferredBehavior::VisibleOnly)
                .is_none()
        );

        tracker.drop(num(6)).unwrap();
        tracker.mapping_flusher()().unwrap();
        tracker.versions_flusher()().unwrap();
        drop(tracker);

        assert_eq!(read_log(dir.path()), [entry(4, 5)], "{format:?}");
        assert_eq!(mask(dir.path()), [2, 6], "{format:?}");
        assert_eq!(
            deleted_slots(&open_regular(format, dir.path())),
            [2, 4, 6],
            "{format:?}",
        );
    }
}
