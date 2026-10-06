//! The in-RAM immutable tracker resolving point moves: tombstones in the deleted mask are held back
//! until the shard resolves them.

use std::path::Path;

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
    MoveEntry, MoveKind, PointMovesMode, PointMovesWriter, Retirement, SlotRef,
    read_point_moves_tail,
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

fn retirement(slot: PointOffsetType, moved_to: Option<SlotRef>) -> Retirement {
    Retirement {
        id: PointIdType::NumId(u64::from(slot)),
        slot,
        moved_to,
    }
}

fn target(slot: PointOffsetType) -> SlotRef {
    SlotRef {
        segment: Uuid::from_u128(7),
        slot,
    }
}

fn open_reader(path: &Path) -> Reader {
    Reader::open_with_moves(&MmapFs, &MmapFs, path, PointMovesMode::Resolve).unwrap()
}

fn refresh(reader: &mut Reader) {
    let tail_to_read = reader.point_moves().unwrap().tail_to_read();
    if let Some((path, start)) = tail_to_read {
        let tail = read_point_moves_tail(&MmapFs, &path, start).unwrap();
        reader.ingest_point_moves_tail(start, &tail);
    }
}

fn nothing_settled(_: SlotRef) -> bool {
    false
}

/// At open, a tombstone a moved-out record names is held, and the other tombstones apply: the log is
/// read after the mask.
#[test]
fn open_holds_move_tombstones_only() {
    let dir = Builder::new().prefix("moves").tempdir().unwrap();
    build_segment(dir.path());
    let mut writer = UpdateOnlyImmutableIdTracker::new(dir.path(), None).unwrap();
    writer
        .retire_points(
            &MmapFs,
            &[retirement(2, None), retirement(3, Some(target(30)))],
        )
        .unwrap();

    let reader = open_reader(dir.path());
    assert!(reader.is_deleted_point(2), "a plain delete applies");
    assert!(!reader.is_deleted_point(3), "a move's tombstone waits");
    assert_eq!(reader.available_point_count(), 9);

    let resolution = reader.resolve_point_moves(&nothing_settled, None);
    assert!(resolution.is_empty());
    let settled = |slot_ref: SlotRef| slot_ref == target(30);
    let resolution = reader.resolve_point_moves(&settled, None);
    assert_eq!(resolution.superseded, [3]);
}

/// A tombstone read on reload waits for a tail read, then applies as a plain delete, or waits for
/// its target as a move.
#[test]
#[cfg_attr(
    windows,
    ignore = "the writer replaces the deleted mask the reader holds mapped"
)]
fn reload_classifies_new_tombstones() {
    let dir = Builder::new().prefix("moves").tempdir().unwrap();
    build_segment(dir.path());
    let mut reader = open_reader(dir.path());

    let mut writer = UpdateOnlyImmutableIdTracker::new(dir.path(), None).unwrap();
    writer
        .retire_points(
            &MmapFs,
            &[retirement(4, None), retirement(5, Some(target(50)))],
        )
        .unwrap();

    let delta = reader.live_reload(&MmapFs).unwrap();
    assert!(delta.deleted.is_empty());
    assert!(!reader.is_deleted_point(4) && !reader.is_deleted_point(5));

    refresh(&mut reader);
    let resolution = reader.resolve_point_moves(&nothing_settled, None);
    assert_eq!(resolution.plain, [4]);
    assert!(resolution.superseded.is_empty());
    let delta = reader.apply_point_moves(&resolution);
    assert_eq!(delta.deleted, [4]);
    assert!(reader.is_deleted_point(4));
    assert!(!reader.is_deleted_point(5));
    assert!(reader.point_moves().unwrap().holds_moves());

    let settled = |slot_ref: SlotRef| slot_ref == target(50);
    let resolution = reader.resolve_point_moves(&settled, None);
    assert_eq!(resolution.superseded, [5]);
    assert_eq!(reader.apply_point_moves(&resolution).deleted, [5]);
    assert!(!reader.point_moves().unwrap().holds_moves());

    // The applied tombstones are not reported again
    assert!(reader.live_reload(&MmapFs).unwrap().deleted.is_empty());
}

/// A copy is masked without its own tombstone: when its moved-out record names a settled target, or
/// when another segment's settled moved-in record names it. The tombstone arriving later reports
/// nothing.
#[test]
#[cfg_attr(
    windows,
    ignore = "the writer replaces the deleted mask the reader holds mapped"
)]
fn settled_moves_mask_live_copies() {
    let dir = Builder::new().prefix("moves").tempdir().unwrap();
    build_segment(dir.path());
    let mut reader = open_reader(dir.path());

    // A moved-out record whose tombstone never landed, as after a crash between the two
    PointMovesWriter::new(dir.path())
        .append(
            &MmapFs,
            &[MoveEntry {
                kind: MoveKind::MovedOut,
                peer: target(60).segment,
                pairs: vec![(6, 60)],
            }],
        )
        .unwrap();
    reader.live_reload(&MmapFs).unwrap();
    assert!(!reader.is_deleted_point(6));

    let settled = |slot_ref: SlotRef| slot_ref == target(60);
    let masked = RoaringBitmap::from_iter([7u32]);
    let resolution = reader.resolve_point_moves(&settled, Some(&masked));
    assert_eq!(resolution.superseded, [6, 7]);
    assert_eq!(reader.apply_point_moves(&resolution).deleted, [6, 7]);
    assert_eq!(reader.available_point_count(), 8);

    // The tombstones of both land later: nothing to report, they are deleted already
    let mut writer = UpdateOnlyImmutableIdTracker::new(dir.path(), None).unwrap();
    writer
        .retire_points(&MmapFs, &[retirement(7, None)])
        .unwrap();
    assert!(reader.live_reload(&MmapFs).unwrap().deleted.is_empty());
    let resolution = reader.resolve_point_moves(&settled, Some(&masked));
    assert!(resolution.is_empty());
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
    let mut writer = UpdateOnlyImmutableIdTracker::new(dir.path(), None).unwrap();
    writer
        .retire_points(&MmapFs, &[retirement(4, Some(target(40)))])
        .unwrap();
    assert!(reader.live_reload(&MmapFs).unwrap().deleted.is_empty());
    let moves = reader.point_moves().unwrap();
    assert_eq!(moves.state.moved_out_slots().count(), 0);
    assert!(!moves.holds_moves());
}
