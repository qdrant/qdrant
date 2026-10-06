//! The disk-resident tracker resolving point moves: tombstones in the deleted mask are held back
//! until the shard resolves them, and every read path answers from the effective deleted state.

use std::path::Path;

use common::types::PointOffsetType;
use common::universal_io::{MmapFile, MmapFs, Populate};
use roaring::RoaringBitmap;
use tempfile::Builder;
use uuid::Uuid;

use super::ReadOnlyDiskIdTracker;
use crate::id_tracker::compressed::compressed_point_mappings::CompressedPointMappings;
use crate::id_tracker::disk_id_tracker::DiskIdTracker;
use crate::id_tracker::disk_id_tracker::update_only::UpdateOnlyDiskIdTracker;
use crate::id_tracker::in_memory_id_tracker::InMemoryIdTracker;
use crate::id_tracker::point_moves::{PointMovesMode, Retirement, SlotRef};
use crate::id_tracker::{IdTracker, IdTrackerRead};
use crate::types::PointIdType;

type Reader = ReadOnlyDiskIdTracker<MmapFile>;

/// Points 0 to 9, on slots 0 to 9.
fn build_segment(path: &Path) {
    let mut id_tracker = InMemoryIdTracker::new();
    for id in 0..10u64 {
        let slot = id as PointOffsetType;
        id_tracker.set_link(PointIdType::NumId(id), slot).unwrap();
        id_tracker.set_internal_version(slot, 10).unwrap();
    }
    let (versions, mappings) = id_tracker.into_internal();
    let mappings = CompressedPointMappings::from_mappings(mappings);
    DiskIdTracker::<MmapFile>::new(&MmapFs, path, &versions, mappings).unwrap();
}

fn retire(path: &Path, slot: PointOffsetType, moved_to: Option<SlotRef>) {
    UpdateOnlyDiskIdTracker::new(path, None)
        .unwrap()
        .retire_points(
            &MmapFs,
            &[Retirement {
                id: PointIdType::NumId(u64::from(slot)),
                slot,
                moved_to,
            }],
        )
        .unwrap();
}

fn target(slot: PointOffsetType) -> SlotRef {
    SlotRef {
        segment: Uuid::from_u128(7),
        slot,
    }
}

fn open_reader(path: &Path) -> Reader {
    Reader::try_open_with_moves(
        &MmapFs,
        &MmapFs,
        path,
        Populate::No,
        PointMovesMode::Resolve,
    )
    .unwrap()
    .expect("a disk-resident tracker")
}

fn nothing_settled(_: SlotRef) -> bool {
    false
}

/// Whether `slot` reads as deleted on the point and the whole-slice paths alike.
fn deleted(reader: &Reader, slot: PointOffsetType) -> bool {
    let point = reader.is_deleted_point(slot);
    let slice = reader.deleted_point_bitslice()[slot as usize];
    assert_eq!(point, slice, "slot {slot}: point and slice paths disagree");
    point
}

/// At open, the log is read after the deleted mask, so a tombstone no record names is a plain
/// delete and applies at once, and one of a move is held.
#[test]
fn open_holds_move_tombstones_only() {
    let dir = Builder::new().prefix("moves").tempdir().unwrap();
    build_segment(dir.path());
    retire(dir.path(), 2, None);
    retire(dir.path(), 3, Some(target(30)));

    let reader = open_reader(dir.path());
    assert!(deleted(&reader, 2));
    assert!(!deleted(&reader, 3), "the tombstone of a move is held");
    assert!(reader.point_moves().unwrap().holds_moves());
}

/// A tombstone read on reload is held and not reported; the resolution that releases it reports
/// it once, and later reloads do not report it again.
#[test]
#[cfg_attr(
    windows,
    ignore = "the writer replaces the deleted mask the reader holds mapped"
)]
fn held_tombstone_is_reported_once_released() {
    let dir = Builder::new().prefix("moves").tempdir().unwrap();
    build_segment(dir.path());
    let mut reader = open_reader(dir.path());

    retire(dir.path(), 3, Some(target(30)));
    assert!(reader.live_reload(&MmapFs).unwrap().deleted.is_empty());
    assert!(!deleted(&reader, 3));
    assert!(
        reader
            .resolve_point_moves(&nothing_settled, None)
            .is_empty()
    );

    let settled = |slot_ref: SlotRef| slot_ref == target(30);
    let resolution = reader.resolve_point_moves(&settled, None);
    assert_eq!(resolution.superseded, [3]);
    assert_eq!(reader.apply_point_moves(&resolution).deleted, [3]);
    assert!(deleted(&reader, 3));
    assert!(!reader.point_moves().unwrap().holds_moves());

    assert!(reader.live_reload(&MmapFs).unwrap().deleted.is_empty());
    assert!(reader.resolve_point_moves(&settled, None).is_empty());
}

/// A moved-out record read after its slot was masked is not kept: the slot is deleted for good, so
/// the record cannot decide anything anymore.
#[test]
#[cfg_attr(
    windows,
    ignore = "the writer replaces the deleted mask the reader holds mapped"
)]
fn record_of_a_masked_slot_is_not_kept() {
    let dir = Builder::new().prefix("moves").tempdir().unwrap();
    build_segment(dir.path());
    let mut reader = open_reader(dir.path());

    // The target's moved-in record masks slot 4 before this segment's own record arrives
    let masked = RoaringBitmap::from_iter([4u32]);
    let resolution = reader.resolve_point_moves(&nothing_settled, Some(&masked));
    assert_eq!(reader.apply_point_moves(&resolution).deleted, [4]);

    // The record and the tombstone land later
    retire(dir.path(), 4, Some(target(40)));
    assert!(reader.live_reload(&MmapFs).unwrap().deleted.is_empty());
    let moves = reader.point_moves().unwrap();
    assert_eq!(moves.state.moved_out_slots().count(), 0);
    assert!(!moves.holds_moves());
    assert!(deleted(&reader, 4));
}
