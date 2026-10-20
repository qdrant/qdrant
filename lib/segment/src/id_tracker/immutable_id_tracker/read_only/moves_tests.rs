//! The in-RAM immutable tracker reading point moves: applied at once, or resolved by the shard.
//! Tombstones in the deleted mask apply at once either way.

use std::path::Path;

use common::bitvec::BitVec;
use common::types::PointOffsetType;
use common::universal_io::{MmapFile, MmapFs};
use roaring::RoaringBitmap;
use tempfile::Builder;
use uuid::Uuid;

use super::ReadOnlyImmutableIdTracker;
use crate::id_tracker::immutable_id_tracker::ImmutableIdTracker;
use crate::id_tracker::immutable_id_tracker::update_only::UpdateOnlyImmutableIdTracker;
use crate::id_tracker::in_memory_id_tracker::InMemoryIdTracker;
use crate::id_tracker::point_moves::{
    MoveEntry, MoveKind, PointMovesMode, PointMovesWriter, SlotRef,
};
use crate::id_tracker::{IdTracker, IdTrackerRead};
use crate::types::PointIdType;

type Reader = ReadOnlyImmutableIdTracker<MmapFile>;

/// Points 0 to 9, on slots 0 to 9.
fn build_segment(path: &Path) {
    let mut id_tracker = InMemoryIdTracker::new();
    for id in 0..10u64 {
        let slot = id as PointOffsetType;
        id_tracker.set_link(PointIdType::NumId(id), slot).unwrap();
        id_tracker.set_internal_version(slot, 10).unwrap();
    }
    ImmutableIdTracker::<MmapFile>::from_in_memory_tracker(&MmapFs, id_tracker, path).unwrap();
}

fn target(slot: PointOffsetType) -> SlotRef {
    SlotRef {
        segment: Uuid::from_u128(7),
        slot,
    }
}

/// Record that the copy on `slot` moved to `to`, without a tombstone.
fn move_out(path: &Path, slot: PointOffsetType, to: SlotRef) {
    PointMovesWriter::new(path)
        .append(
            &MmapFs,
            &[MoveEntry {
                kind: MoveKind::MovedOut,
                peer: to.segment,
                pairs: vec![(slot, to.slot)],
            }],
        )
        .unwrap();
}

/// Tombstone the point on `slot` in the deleted mask, resuming from `mask` if given.
fn tombstone(path: &Path, slot: PointOffsetType, mask: Option<BitVec>) {
    UpdateOnlyImmutableIdTracker::new(path, mask)
        .unwrap()
        .tombstone_points(&MmapFs, &[(PointIdType::NumId(u64::from(slot)), slot)])
        .unwrap();
}

fn open_reader(path: &Path, mode: PointMovesMode) -> Reader {
    Reader::open_with_moves(&MmapFs, path, mode).unwrap()
}

/// The local slots the shard finds superseded once the `settled` targets have settled: those a
/// moved-out record names with one of them.
fn superseded_by(reader: &Reader, settled: &[SlotRef]) -> RoaringBitmap {
    reader
        .point_moves()
        .unwrap()
        .moved_out()
        .filter(|(_, target)| settled.contains(target))
        .map(|(slot, _)| slot)
        .collect()
}

/// Applying moves, a moved-out record deletes its copy at open and on reload, and the deleted count
/// includes it. Tombstones apply alongside.
#[test]
fn apply_deletes_moved_copies_at_once() {
    let dir = Builder::new().prefix("moves").tempdir().unwrap();
    build_segment(dir.path());
    move_out(dir.path(), 2, target(20));
    // A slot past the last point names no copy, also where the mask is padded
    move_out(dir.path(), 42, target(21));
    tombstone(dir.path(), 4, None);

    let mut reader = open_reader(dir.path(), PointMovesMode::Apply);
    assert!(reader.is_deleted_point(2));
    assert!(reader.is_deleted_point(4));
    assert_eq!(reader.deleted_point_count(), 2);

    move_out(dir.path(), 3, target(30));
    assert_eq!(reader.live_reload(&MmapFs).unwrap().deleted, [3]);
    assert!(reader.is_deleted_point(3));
    assert_eq!(reader.deleted_point_count(), 3);
    assert!(reader.point_moves().is_none());
    assert!(!reader.holds_point_moves());
}

/// Resolving moves, a moved copy stays until the shard finds its target settled, and counts as
/// deleted from then on. A later reload does not report it again.
#[test]
fn resolve_keeps_a_moved_copy_until_its_target_settles() {
    let dir = Builder::new().prefix("moves").tempdir().unwrap();
    build_segment(dir.path());
    move_out(dir.path(), 3, target(30));

    let mut reader = open_reader(dir.path(), PointMovesMode::Resolve);
    assert!(!reader.is_deleted_point(3));
    assert_eq!(reader.deleted_point_count(), 0);
    assert!(reader.holds_point_moves());
    assert!(reader.resolve_point_moves(None).is_empty());

    let settled = [target(30)];
    let resolution = reader.resolve_point_moves(Some(&superseded_by(&reader, &settled)));
    assert_eq!(resolution.superseded, [3]);
    assert!(!reader.holds_point_moves_beyond(&resolution));
    assert_eq!(reader.apply_point_moves(&resolution).deleted, [3]);
    assert!(reader.is_deleted_point(3));
    assert_eq!(reader.deleted_point_count(), 1);
    assert!(!reader.holds_point_moves());

    assert!(reader.live_reload(&MmapFs).unwrap().deleted.is_empty());
}

/// Resolving moves, a tombstone still applies at once, and takes the record of its slot along.
#[test]
#[cfg_attr(
    windows,
    ignore = "the writer replaces the deleted mask the reader holds mapped"
)]
fn resolve_applies_tombstones_at_once() {
    let dir = Builder::new().prefix("moves").tempdir().unwrap();
    build_segment(dir.path());
    tombstone(dir.path(), 2, None);
    move_out(dir.path(), 5, target(50));

    let mut reader = open_reader(dir.path(), PointMovesMode::Resolve);
    assert!(reader.is_deleted_point(2));
    assert!(reader.holds_point_moves());

    tombstone(dir.path(), 4, None);
    assert_eq!(reader.live_reload(&MmapFs).unwrap().deleted, [4]);
    assert!(reader.is_deleted_point(4));

    tombstone(dir.path(), 5, None);
    assert_eq!(reader.live_reload(&MmapFs).unwrap().deleted, [5]);
    assert!(!reader.holds_point_moves(), "the record goes with its slot");
}

/// A copy is masked without a record of its own when another segment's settled moved-in record
/// names it. A tombstone arriving later reports nothing.
#[test]
#[cfg_attr(
    windows,
    ignore = "the writer replaces the deleted mask the reader holds mapped"
)]
fn settled_moves_mask_live_copies() {
    let dir = Builder::new().prefix("moves").tempdir().unwrap();
    build_segment(dir.path());
    let mut reader = open_reader(dir.path(), PointMovesMode::Resolve);

    let masked = RoaringBitmap::from_iter([7u32]);
    let resolution = reader.resolve_point_moves(Some(&masked));
    assert_eq!(resolution.superseded, [7]);
    assert_eq!(reader.apply_point_moves(&resolution).deleted, [7]);
    assert_eq!(reader.available_point_count(), 9);

    tombstone(dir.path(), 7, None);
    assert!(reader.live_reload(&MmapFs).unwrap().deleted.is_empty());
    assert!(reader.resolve_point_moves(Some(&masked)).is_empty());
}

/// A moved-out record read after its slot was masked is not kept: the slot is deleted for good, so
/// the record cannot decide anything anymore.
#[test]
fn record_of_a_masked_slot_is_not_kept() {
    let dir = Builder::new().prefix("moves").tempdir().unwrap();
    build_segment(dir.path());
    let mut reader = open_reader(dir.path(), PointMovesMode::Resolve);

    // The target's moved-in record masks slot 4 before this segment's own record arrives
    let masked = RoaringBitmap::from_iter([4u32]);
    let resolution = reader.resolve_point_moves(Some(&masked));
    assert_eq!(reader.apply_point_moves(&resolution).deleted, [4]);

    move_out(dir.path(), 4, target(40));
    assert!(reader.live_reload(&MmapFs).unwrap().deleted.is_empty());
    assert_eq!(reader.point_moves().unwrap().moved_out().count(), 0);
    assert!(!reader.holds_point_moves());
}

/// The writer resumes from the mask as stored: the copies deleted for a move stay out of it, also
/// once the writer tombstones more.
#[test]
fn writer_resumes_from_the_stored_mask() {
    let dir = Builder::new().prefix("moves").tempdir().unwrap();
    build_segment(dir.path());
    move_out(dir.path(), 2, target(20));
    tombstone(dir.path(), 3, None);

    let reader = open_reader(dir.path(), PointMovesMode::Apply);
    let mask = reader.stored_deleted_mask();
    assert_eq!(mask.iter_ones().collect::<Vec<_>>(), [3]);
    drop(reader);

    tombstone(dir.path(), 5, Some(mask));
    let reader = open_reader(dir.path(), PointMovesMode::Resolve);
    assert!(
        !reader.is_deleted_point(2),
        "the move is the shard's to resolve"
    );
    assert!(reader.is_deleted_point(3) && reader.is_deleted_point(5));
    assert_eq!(
        reader.stored_deleted_mask().iter_ones().collect::<Vec<_>>(),
        [3, 5]
    );
}
