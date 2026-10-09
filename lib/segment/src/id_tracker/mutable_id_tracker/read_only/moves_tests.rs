//! The appendable tracker resolving point moves: deletes are held back until the shard resolves
//! them, and applied per slot, in the order the log implies.

use std::path::Path;

use common::types::{DeferredBehavior, PointOffsetType};
use common::universal_io::{MmapFile, MmapFs};
use roaring::RoaringBitmap;
use tempfile::Builder;
use uuid::Uuid;

use super::{LiveReloadResult, ReadOnlyAppendableIdTracker};
use crate::id_tracker::IdTrackerRead;
use crate::id_tracker::mutable_id_tracker::mappings_storage::mappings_path;
use crate::id_tracker::mutable_id_tracker::update_only::{
    MappingOperation, UpdateOnlyAppendableIdTracker,
};
use crate::id_tracker::point_moves::{
    MoveEntry, MoveKind, MoveResolution, PointMovesMode, SlotRef, read_point_moves_tail,
};
use crate::types::{PointIdType, SeqNumberType};

type Reader = ReadOnlyAppendableIdTracker<MmapFile>;

fn num(id: u64) -> PointIdType {
    PointIdType::NumId(id)
}

fn target() -> Uuid {
    Uuid::from_u128(7)
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

/// Live-reload and publish, as a segment whose components all reload does.
fn reload(reader: &mut Reader) -> LiveReloadResult {
    let delta = reader.live_reload(&MmapFs, None).unwrap();
    reader.publish_staged();
    delta
}

fn open_reader(path: &Path) -> Reader {
    Reader::open_with_moves(&MmapFs, &MmapFs, path, None, None, PointMovesMode::Resolve).unwrap()
}

/// Classify the held deletes the way the shard does: a tail read of the move log from where the
/// reader stopped.
fn refresh(reader: &mut Reader) {
    if let Some((path, start)) = reader.point_moves_tail_to_read() {
        let tail = read_point_moves_tail(&MmapFs, &path, start).unwrap();
        reader.ingest_point_moves_tail(start, &tail);
    }
}

fn nothing_settled(_: SlotRef) -> bool {
    false
}

fn lookup(reader: &Reader, id: PointIdType) -> Option<PointOffsetType> {
    reader.internal_id_with_behavior(id, DeferredBehavior::VisibleOnly)
}

/// A plain delete is held until it is classified, then applied by the resolution.
#[test]
fn plain_delete_waits_for_its_classification() {
    let dir = Builder::new().prefix("moves").tempdir().unwrap();
    let mut writer = open_writer(dir.path());
    let slot = store(&mut writer, 1, 10);

    let mut reader = open_reader(dir.path());
    assert_eq!(lookup(&reader, num(1)), Some(slot));

    writer.delete_points(&MmapFs, [num(1)]).unwrap();
    let delta = reload(&mut reader);
    assert!(
        delta.deleted.is_empty(),
        "a delete waits for the resolution"
    );
    assert_eq!(lookup(&reader, num(1)), Some(slot));
    assert!(reader.point_moves_tail_to_read().is_some());

    refresh(&mut reader);
    let resolution = reader.resolve_point_moves(&nothing_settled, None);
    assert_eq!(resolution.plain, [slot]);
    let delta = reader.apply_point_moves(&resolution);
    assert_eq!(delta.deleted, [slot]);
    assert_eq!(lookup(&reader, num(1)), None);
}

/// Opening applies the plain deletes at once: the log is read after the mappings, so nothing can
/// make them wait.
#[test]
fn open_applies_plain_deletes() {
    let dir = Builder::new().prefix("moves").tempdir().unwrap();
    let mut writer = open_writer(dir.path());
    store(&mut writer, 1, 10);
    store(&mut writer, 2, 11);
    writer.delete_points(&MmapFs, [num(1)]).unwrap();

    let reader = open_reader(dir.path());
    assert_eq!(lookup(&reader, num(1)), None);
    assert!(lookup(&reader, num(2)).is_some());
    assert!(!reader.holds_point_moves());
}

/// The delete of a move keeps the old copy until the move's target settles.
#[test]
fn move_delete_waits_for_its_target() {
    let dir = Builder::new().prefix("moves").tempdir().unwrap();
    let mut writer = open_writer(dir.path());
    let slot = store(&mut writer, 1, 10);
    let mut reader = open_reader(dir.path());

    let moved_to = SlotRef {
        segment: target(),
        slot: 3,
    };
    writer
        .retire_points(
            &MmapFs,
            &[crate::id_tracker::point_moves::Retirement {
                id: num(1),
                slot,
                moved_to: Some(moved_to),
            }],
        )
        .unwrap();
    reload(&mut reader);
    assert!(reader.holds_point_moves());
    assert!(
        reader.point_moves_tail_to_read().is_none(),
        "the record classifies the delete, no tail read needed",
    );

    let resolution = reader.resolve_point_moves(&nothing_settled, None);
    assert!(resolution.is_empty(), "the target has not settled");
    assert_eq!(lookup(&reader, num(1)), Some(slot));

    let settled = |slot_ref: SlotRef| slot_ref == moved_to;
    let resolution = reader.resolve_point_moves(&settled, None);
    assert_eq!(resolution.superseded, [slot]);
    let delta = reader.apply_point_moves(&resolution);
    assert_eq!(delta.deleted, [slot]);
    assert_eq!(lookup(&reader, num(1)), None);
    assert!(!reader.holds_point_moves());
}

/// A held delete retires the slots it saw, not the point: a re-insert of the same id that follows
/// it in the log survives the delete's late application.
#[test]
fn held_delete_spares_a_later_reinsert() {
    let dir = Builder::new().prefix("moves").tempdir().unwrap();
    let mut writer = open_writer(dir.path());
    let first = store(&mut writer, 1, 10);
    let mut reader = open_reader(dir.path());
    assert_eq!(lookup(&reader, num(1)), Some(first));

    writer.delete_points(&MmapFs, [num(1)]).unwrap();
    let second = store(&mut writer, 1, 12);

    let delta = reload(&mut reader);
    assert_eq!(delta.inserted, [second]);
    assert_eq!(
        delta.deleted,
        [first],
        "the re-insert supersedes the old slot"
    );
    assert_eq!(lookup(&reader, num(1)), Some(second));

    refresh(&mut reader);
    let resolution = reader.resolve_point_moves(&nothing_settled, None);
    assert_eq!(resolution.plain, [first]);
    let delta = reader.apply_point_moves(&resolution);
    assert!(delta.deleted.is_empty());
    assert_eq!(lookup(&reader, num(1)), Some(second));
}

/// A settled move can mask a slot the view has not committed yet. The point is then dropped as soon
/// as it commits, and never reported as inserted.
#[test]
fn masked_slot_is_dropped_when_it_commits() {
    let dir = Builder::new().prefix("moves").tempdir().unwrap();
    let mut writer = open_writer(dir.path());
    let inserted = writer
        .insert_operations(&MmapFs, &[MappingOperation::Insert(num(1))])
        .unwrap();
    let slot = inserted[0].1;

    let mut reader = open_reader(dir.path());
    assert_eq!(lookup(&reader, num(1)), None, "not committed yet");

    let masked = RoaringBitmap::from_iter([slot]);
    let resolution = reader.resolve_point_moves(&nothing_settled, Some(&masked));
    assert_eq!(resolution.superseded, [slot]);
    assert!(reader.apply_point_moves(&resolution).deleted.is_empty());

    writer
        .set_internal_versions(&MmapFs, &[slot], &[10])
        .unwrap();
    let delta = reload(&mut reader);
    assert!(delta.inserted.is_empty());
    assert_eq!(lookup(&reader, num(1)), None);
}

/// A writer that stopped before publishing leaves a claimed slot, which the next writer retires with
/// a delete and a later publish covers with the placeholder version. Holding that delete must not
/// let the half-written point commit.
#[test]
fn placeholder_slot_stays_cancelled_while_its_delete_is_held() {
    let dir = Builder::new().prefix("moves").tempdir().unwrap();

    let mut crashed = open_writer(dir.path());
    crashed
        .insert_operations(&MmapFs, &[MappingOperation::Insert(num(1))])
        .unwrap();
    drop(crashed);

    let mappings_end = fs_err::metadata(mappings_path(dir.path())).unwrap().len();
    let mut writer =
        UpdateOnlyAppendableIdTracker::new(&MmapFs, dir.path(), Some(0), [num(1)], mappings_end)
            .unwrap();
    let published = store(&mut writer, 2, 20);
    assert_eq!(published, 1);

    // Everything in one reload: the insert, its retiring delete, and the placeholder version
    let reader = Reader::open_with_moves(
        &MmapFs,
        &MmapFs,
        dir.path(),
        None,
        None,
        PointMovesMode::Resolve,
    )
    .unwrap();
    assert_eq!(lookup(&reader, num(1)), None);
    assert_eq!(reader.external_id(0), None);
    assert!(lookup(&reader, num(2)).is_some());

    // And across reloads, while the delete waits
    let dir = Builder::new().prefix("moves").tempdir().unwrap();
    let mut crashed = open_writer(dir.path());
    crashed
        .insert_operations(&MmapFs, &[MappingOperation::Insert(num(1))])
        .unwrap();
    drop(crashed);
    let mut reader = open_reader(dir.path());
    let mappings_end = fs_err::metadata(mappings_path(dir.path())).unwrap().len();
    let mut writer =
        UpdateOnlyAppendableIdTracker::new(&MmapFs, dir.path(), Some(0), [num(1)], mappings_end)
            .unwrap();
    store(&mut writer, 2, 20);
    reload(&mut reader);
    assert_eq!(lookup(&reader, num(1)), None);
    assert_eq!(reader.external_id(0), None);

    refresh(&mut reader);
    let resolution = reader.resolve_point_moves(&nothing_settled, None);
    reader.apply_point_moves(&resolution);
    reload(&mut reader);
    assert_eq!(lookup(&reader, num(1)), None);
    assert!(!reader.holds_point_moves());
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

    let mut reader = open_reader(dir.path());
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

    let mut reader = open_reader(dir.path());
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

    // Nothing was held back: the resolution for this segment is empty
    assert_eq!(
        reader.resolve_point_moves(&nothing_settled, None),
        MoveResolution::default()
    );
}

/// A moved-out record read after its slot was masked is not kept: the slot is deleted for good, so
/// the record cannot decide anything anymore.
#[test]
fn record_of_a_masked_slot_is_not_kept() {
    let dir = Builder::new().prefix("moves").tempdir().unwrap();
    let mut writer = open_writer(dir.path());
    let slot = store(&mut writer, 1, 10);
    let mut reader = open_reader(dir.path());

    // The target's moved-in record masks the slot before this segment's own record arrives
    let masked = RoaringBitmap::from_iter([slot]);
    let resolution = reader.resolve_point_moves(&nothing_settled, Some(&masked));
    assert_eq!(reader.apply_point_moves(&resolution).deleted, [slot]);

    // The record and the delete land later
    writer
        .retire_points(
            &MmapFs,
            &[crate::id_tracker::point_moves::Retirement {
                id: num(1),
                slot,
                moved_to: Some(SlotRef {
                    segment: target(),
                    slot: 3,
                }),
            }],
        )
        .unwrap();
    assert!(reload(&mut reader).deleted.is_empty());
    assert_eq!(
        reader.point_moves().unwrap().moved_out_slots().count(),
        0,
        "the record of a masked slot is skipped",
    );
    assert!(!reader.holds_point_moves());
}
