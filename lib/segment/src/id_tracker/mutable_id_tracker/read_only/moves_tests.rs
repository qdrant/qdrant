//! The appendable tracker reading point moves: applied at once, or resolved by the shard.

use std::path::Path;

use common::types::{DeferredBehavior, PointOffsetType};
use common::universal_io::{MmapFile, MmapFs};
use roaring::RoaringBitmap;
use tempfile::Builder;
use uuid::Uuid;

use super::{LiveReloadResult, ReadOnlyAppendableIdTracker};
use crate::id_tracker::IdTrackerRead;
use crate::id_tracker::mutable_id_tracker::update_only::{
    MappingOperation, UpdateOnlyAppendableIdTracker,
};
use crate::id_tracker::point_moves::{
    MoveEntry, MoveKind, MoveResolution, PointMovesMode, SlotRef,
};
use crate::types::{PointIdType, SeqNumberType};

type Reader = ReadOnlyAppendableIdTracker<MmapFile>;

fn num(id: u64) -> PointIdType {
    PointIdType::NumId(id)
}

fn target(slot: PointOffsetType) -> SlotRef {
    SlotRef {
        segment: Uuid::from_u128(7),
        slot,
    }
}

fn open_writer(path: &Path) -> UpdateOnlyAppendableIdTracker {
    UpdateOnlyAppendableIdTracker::new(&MmapFs, path, None, [], 0).unwrap()
}

/// Insert and publish `id` at a fresh slot, returning the slot.
fn store(writer: &mut UpdateOnlyAppendableIdTracker, id: u64, version: SeqNumberType) -> u32 {
    let inserted = writer
        .insert_operations(&MmapFs, &[MappingOperation::Insert(num(id))])
        .unwrap();
    let slot = inserted[0].1;
    writer
        .set_internal_versions(&MmapFs, &[slot], &[version])
        .unwrap();
    slot
}

/// Record that the copy on `slot` moved to `to`, without a tombstone.
fn move_out(writer: &mut UpdateOnlyAppendableIdTracker, slot: PointOffsetType, to: SlotRef) {
    writer
        .record_moves(
            &MmapFs,
            &[MoveEntry {
                kind: MoveKind::MovedOut,
                peer: to.segment,
                pairs: vec![(slot, to.slot)],
            }],
        )
        .unwrap();
}

/// Live-reload and publish, as a segment whose components all reload does.
fn reload(reader: &mut Reader) -> LiveReloadResult {
    let delta = reader.live_reload(&MmapFs, None).unwrap();
    reader.publish_staged();
    delta
}

fn open_reader(path: &Path, mode: PointMovesMode) -> Reader {
    Reader::open_with_moves(&MmapFs, path, None, None, mode).unwrap()
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

fn lookup(reader: &Reader, id: PointIdType) -> Option<PointOffsetType> {
    reader.internal_id_with_behavior(id, DeferredBehavior::VisibleOnly)
}

/// Applying moves, a moved-out record deletes its copy as soon as it is read, by a reload or an
/// open, and the deleted count includes it.
#[test]
fn apply_deletes_a_moved_copy_at_once() {
    let dir = Builder::new().prefix("moves").tempdir().unwrap();
    let mut writer = open_writer(dir.path());
    let moved = store(&mut writer, 1, 10);
    let kept = store(&mut writer, 2, 11);

    let mut reader = open_reader(dir.path(), PointMovesMode::Apply);
    move_out(&mut writer, moved, target(3));
    let delta = reload(&mut reader);
    assert_eq!(delta.deleted, [moved]);
    assert_eq!(lookup(&reader, num(1)), None);
    assert_eq!(lookup(&reader, num(2)), Some(kept));
    assert_eq!(reader.deleted_point_count(), 1);
    assert!(reader.point_moves().is_none());
    assert!(!reader.holds_point_moves());

    let reader = open_reader(dir.path(), PointMovesMode::Apply);
    assert_eq!(lookup(&reader, num(1)), None);
    assert_eq!(lookup(&reader, num(2)), Some(kept));
    assert_eq!(reader.deleted_point_count(), 1);
}

/// Resolving moves, a moved copy stays until the shard finds its target settled, and counts as
/// deleted from then on.
#[test]
fn resolve_keeps_a_moved_copy_until_its_target_settles() {
    let dir = Builder::new().prefix("moves").tempdir().unwrap();
    let mut writer = open_writer(dir.path());
    let slot = store(&mut writer, 1, 10);
    let mut reader = open_reader(dir.path(), PointMovesMode::Resolve);

    move_out(&mut writer, slot, target(3));
    assert!(reload(&mut reader).deleted.is_empty());
    assert!(reader.holds_point_moves());
    assert_eq!(lookup(&reader, num(1)), Some(slot));
    assert_eq!(reader.deleted_point_count(), 0);

    let resolution = reader.resolve_point_moves(None);
    assert!(resolution.is_empty(), "the target has not settled");

    let resolution = reader.resolve_point_moves(Some(&superseded_by(&reader, &[target(3)])));
    assert_eq!(resolution.superseded, [slot]);
    assert!(!reader.holds_point_moves_beyond(&resolution));
    let delta = reader.apply_point_moves(&resolution);
    assert_eq!(delta.deleted, [slot]);
    assert_eq!(lookup(&reader, num(1)), None);
    assert_eq!(reader.deleted_point_count(), 1);
    assert!(!reader.holds_point_moves());
}

/// A delete applies at once in either mode, and takes the record of its slot along.
#[test]
fn delete_applies_at_once() {
    for mode in [PointMovesMode::Apply, PointMovesMode::Resolve] {
        let dir = Builder::new().prefix("moves").tempdir().unwrap();
        let mut writer = open_writer(dir.path());
        let deleted = store(&mut writer, 1, 10);
        let moved = store(&mut writer, 2, 11);
        let mut reader = open_reader(dir.path(), mode);

        writer.delete_points(&MmapFs, [num(1)]).unwrap();
        assert_eq!(reload(&mut reader).deleted, [deleted], "{mode:?}");
        assert_eq!(lookup(&reader, num(1)), None);

        // A record the reader keeps, then the delete of its point
        if mode == PointMovesMode::Resolve {
            move_out(&mut writer, moved, target(3));
            reload(&mut reader);
            assert!(reader.holds_point_moves());
            writer.delete_points(&MmapFs, [num(2)]).unwrap();
            assert_eq!(reload(&mut reader).deleted, [moved]);
            assert!(!reader.holds_point_moves(), "the record goes with its slot");
        }
    }
}

/// A move can mask a slot the view has not committed yet: a moved-out record read ahead of the
/// mappings, or a settled move the shard found. The point is then dropped as soon as it commits,
/// and never reported as inserted.
#[test]
fn masked_slot_is_dropped_when_it_commits() {
    for mode in [PointMovesMode::Apply, PointMovesMode::Resolve] {
        let dir = Builder::new().prefix("moves").tempdir().unwrap();
        let mut writer = open_writer(dir.path());
        let inserted = writer
            .insert_operations(&MmapFs, &[MappingOperation::Insert(num(1))])
            .unwrap();
        let slot = inserted[0].1;
        move_out(&mut writer, slot, target(3));

        let mut reader = open_reader(dir.path(), mode);
        assert_eq!(lookup(&reader, num(1)), None, "not committed yet");

        if mode == PointMovesMode::Resolve {
            let superseded = superseded_by(&reader, &[target(3)]);
            let resolution = reader.resolve_point_moves(Some(&superseded));
            assert_eq!(resolution.superseded, [slot]);
            assert!(reader.apply_point_moves(&resolution).deleted.is_empty());
        }
        assert!(reader.is_retired(slot), "{mode:?}");

        writer
            .set_internal_versions(&MmapFs, &[slot], &[10])
            .unwrap();
        let delta = reload(&mut reader);
        assert!(delta.inserted.is_empty(), "{mode:?}");
        assert_eq!(lookup(&reader, num(1)), None);
        assert_eq!(reader.deleted_point_count(), 1);
    }
}

/// A slot settles once its insert is read and its version is published, and never on the
/// placeholder version.
#[test]
fn settled_needs_a_published_version() {
    let dir = Builder::new().prefix("moves").tempdir().unwrap();
    let mut writer = open_writer(dir.path());
    let inserted = writer
        .insert_operations(
            &MmapFs,
            &[
                MappingOperation::Insert(num(1)),
                MappingOperation::Insert(num(2)),
            ],
        )
        .unwrap();
    let (first, second): (PointOffsetType, PointOffsetType) = (inserted[0].1, inserted[1].1);

    let mut reader = open_reader(dir.path(), PointMovesMode::Resolve);
    assert!(!reader.is_settled(first) && !reader.is_settled(second));

    // Publishing the second slot covers the skipped first one with the placeholder
    writer
        .set_internal_versions(&MmapFs, &[second], &[10])
        .unwrap();
    reload(&mut reader);
    assert!(!reader.is_settled(first));
    assert!(reader.is_settled(second));
    assert!(!reader.is_settled(second + 1));
}

/// A slot the view hides as deferred has not settled: a move into it must not mask the copy it
/// replaces, which is the only one the view shows.
#[test]
fn deferred_slot_does_not_settle() {
    let dir = Builder::new().prefix("moves").tempdir().unwrap();
    let mut writer = open_writer(dir.path());
    let visible = store(&mut writer, 1, 10);
    let deferred = store(&mut writer, 2, 11);

    let reader = Reader::open_with_moves(
        &MmapFs,
        dir.path(),
        Some(deferred),
        None,
        PointMovesMode::Resolve,
    )
    .unwrap();
    assert!(reader.is_settled(visible));
    assert!(!reader.is_settled(deferred));
}

/// Moved-in records fold into the per-source index once their local slot settles.
#[test]
fn moved_in_records_settle_with_their_slot() {
    let dir = Builder::new().prefix("moves").tempdir().unwrap();
    let source = Uuid::from_u128(3);
    let mut writer = open_writer(dir.path());
    let inserted = writer
        .insert_operations(&MmapFs, &[MappingOperation::Insert(num(1))])
        .unwrap();
    let slot = inserted[0].1;
    writer
        .record_moves(
            &MmapFs,
            &[MoveEntry {
                kind: MoveKind::MovedIn,
                peer: source,
                pairs: vec![(slot, 42)],
            }],
        )
        .unwrap();

    let mut reader = open_reader(dir.path(), PointMovesMode::Resolve);
    assert!(
        reader.point_moves().unwrap().settled_moved_in().is_empty(),
        "the record counts only once its slot is published",
    );

    writer
        .set_internal_versions(&MmapFs, &[slot], &[10])
        .unwrap();
    reload(&mut reader);
    let settled = reader.point_moves().unwrap().settled_moved_in();
    assert!(settled[&source].contains(42));

    // Nothing moved out of this segment: its own resolution is empty
    assert_eq!(reader.resolve_point_moves(None), MoveResolution::default());
}

/// A moved-out record read after its slot was masked is not kept: the slot is deleted for good, so
/// the record cannot decide anything anymore.
#[test]
fn record_of_a_masked_slot_is_not_kept() {
    let dir = Builder::new().prefix("moves").tempdir().unwrap();
    let mut writer = open_writer(dir.path());
    let slot = store(&mut writer, 1, 10);
    let mut reader = open_reader(dir.path(), PointMovesMode::Resolve);

    // The target's moved-in record masks the slot before this segment's own record arrives
    let masked = RoaringBitmap::from_iter([slot]);
    let resolution = reader.resolve_point_moves(Some(&masked));
    assert_eq!(reader.apply_point_moves(&resolution).deleted, [slot]);

    move_out(&mut writer, slot, target(3));
    assert!(reload(&mut reader).deleted.is_empty());
    assert_eq!(reader.point_moves().unwrap().moved_out().count(), 0);
    assert!(!reader.holds_point_moves());
}
