//! A move log found by a regular id tracker: the copies it moved out are deleted.

use std::path::Path;

use common::types::PointOffsetType;
use common::universal_io::{MmapFile, MmapFs, Populate};
use rstest::rstest;
use tempfile::Builder;
use uuid::Uuid;

use super::regular::{delete_moved_out, read_moved_out};
use super::{MoveEntry, MoveKind, PointMovesMode, PointMovesWriter};
use crate::id_tracker::disk_id_tracker::DiskIdTracker;
use crate::id_tracker::immutable_id_tracker::ImmutableIdTracker;
use crate::id_tracker::in_memory_id_tracker::InMemoryIdTracker;
use crate::id_tracker::mutable_id_tracker::MutableIdTracker;
use crate::id_tracker::read_only_tracker_enum::ReadOnlyIdTrackerEnum;
use crate::id_tracker::{IdTracker, IdTrackerEnum, IdTrackerRead};
use crate::types::PointIdType;

#[derive(Clone, Copy, Debug)]
enum Regular {
    Mutable,
    Immutable,
    DiskResident,
}

/// A regular tracker of `kind` at `path` with points 0 to 9 on slots 0 to 9, and slot 3 deleted.
fn build(kind: Regular, path: &Path) -> IdTrackerEnum {
    let mut in_memory = InMemoryIdTracker::new();
    for id in 0..10u64 {
        let slot = id as PointOffsetType;
        in_memory.set_link(PointIdType::NumId(id), slot).unwrap();
        in_memory.set_internal_version(slot, 10).unwrap();
    }
    let mut id_tracker = match kind {
        Regular::Mutable => {
            let mut id_tracker = MutableIdTracker::open(path, None).unwrap();
            for id in 0..10u64 {
                let slot = id as PointOffsetType;
                id_tracker.set_link(PointIdType::NumId(id), slot).unwrap();
                id_tracker.set_internal_version(slot, 10).unwrap();
            }
            IdTrackerEnum::MutableIdTracker(id_tracker)
        }
        Regular::Immutable => IdTrackerEnum::ImmutableIdTracker(
            ImmutableIdTracker::from_in_memory_tracker(&MmapFs, in_memory, path).unwrap(),
        ),
        Regular::DiskResident => IdTrackerEnum::DiskIdTracker(
            DiskIdTracker::from_in_memory_tracker(&MmapFs, in_memory, path).unwrap(),
        ),
    };
    id_tracker.drop_internal(3).unwrap();
    flush(&id_tracker);
    id_tracker
}

fn open(kind: Regular, path: &Path) -> IdTrackerEnum {
    match kind {
        Regular::Mutable => {
            IdTrackerEnum::MutableIdTracker(MutableIdTracker::open(path, None).unwrap())
        }
        Regular::Immutable => {
            IdTrackerEnum::ImmutableIdTracker(ImmutableIdTracker::open(&MmapFs, path).unwrap())
        }
        Regular::DiskResident => {
            IdTrackerEnum::DiskIdTracker(DiskIdTracker::open(&MmapFs, path, Populate::No).unwrap())
        }
    }
}

fn flush(id_tracker: &IdTrackerEnum) {
    id_tracker.mapping_flusher()().unwrap();
    id_tracker.versions_flusher()().unwrap();
}

/// Copies 2 and 3 moved out, and one on slot 42, which the tracker does not hold.
fn write_moves(path: &Path) {
    PointMovesWriter::new(path)
        .append(
            &MmapFs,
            &[MoveEntry {
                kind: MoveKind::MovedOut,
                peer: Uuid::from_u128(7),
                pairs: vec![(2, 20), (3, 30), (42, 40)],
            }],
        )
        .unwrap();
}

/// The copies a move log names as moved out are deleted through the regular delete path, so the
/// tombstones persist. Deleted copies and slots the tracker does not hold are skipped.
#[rstest]
fn regular_tracker_deletes_moved_out_copies(
    #[values(Regular::Mutable, Regular::Immutable, Regular::DiskResident)] kind: Regular,
) {
    let dir = Builder::new().prefix("point_moves").tempdir().unwrap();
    let mut id_tracker = build(kind, dir.path());
    assert_eq!(read_moved_out(dir.path()).unwrap(), None);

    write_moves(dir.path());
    let moved_out = read_moved_out(dir.path()).unwrap().unwrap();
    assert_eq!(moved_out, [2, 3, 42]);
    delete_moved_out(&mut id_tracker, &moved_out).unwrap();
    assert!(id_tracker.is_deleted_point(2));
    assert_eq!(id_tracker.deleted_point_count(), 2);

    flush(&id_tracker);
    drop(id_tracker);
    let reader = ReadOnlyIdTrackerEnum::<MmapFile>::detect_and_load_with_moves(
        &MmapFs,
        dir.path(),
        None,
        None,
        Populate::No,
        PointMovesMode::Resolve,
    )
    .unwrap();
    assert!(
        reader.is_deleted_point(2),
        "the tombstone persists, also to a reader resolving moves",
    );
    assert_eq!(reader.deleted_point_count(), 2);
}

/// A regular tracker never expects a move log: loading one fails a debug assertion. Otherwise it
/// logs a warning, and deletes the copies moved out.
#[rstest]
#[cfg_attr(
    debug_assertions,
    should_panic(expected = "regular id tracker found a point move log")
)]
fn regular_tracker_load_finds_a_move_log(
    #[values(Regular::Mutable, Regular::Immutable, Regular::DiskResident)] kind: Regular,
) {
    let dir = Builder::new().prefix("point_moves").tempdir().unwrap();
    drop(build(kind, dir.path()));
    write_moves(dir.path());

    let id_tracker = open(kind, dir.path());
    assert!(id_tracker.is_deleted_point(2));
    assert_eq!(id_tracker.deleted_point_count(), 2);
}
