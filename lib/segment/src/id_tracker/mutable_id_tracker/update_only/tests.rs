use std::collections::BTreeMap;
use std::io::Write as _;
use std::path::Path;

use common::types::{DeferredBehavior, PointOffsetType};
use common::universal_io::{MmapFile, MmapFs};
use tempfile::Builder;
use uuid::Uuid;

use super::MappingOperation::{Delete, Insert};
use super::UpdateOnlyAppendableIdTracker;
use crate::id_tracker::mutable_id_tracker::MutableIdTracker;
use crate::id_tracker::mutable_id_tracker::mappings_storage::{load_mappings, mappings_path};
use crate::id_tracker::mutable_id_tracker::read_only::ReadOnlyAppendableIdTracker;
use crate::id_tracker::mutable_id_tracker::versions_storage::{
    load_versions, store_version_changes, versions_path,
};
use crate::id_tracker::{DELETED_POINT_VERSION, IdTracker, IdTrackerRead};
use crate::types::{PointIdType, SeqNumberType};

type Tracker = UpdateOnlyAppendableIdTracker<MmapFile>;
type ReadOnlyTracker = ReadOnlyAppendableIdTracker<MmapFile>;

fn num(id: u64) -> PointIdType {
    PointIdType::NumId(id)
}

fn uuid(id: u128) -> PointIdType {
    PointIdType::Uuid(Uuid::from_u128(id))
}

/// The end of the mappings log as the view a writer shares would report it.
/// Sound only for a log with no torn tail, which every caller here has.
fn log_end(segment_path: &Path) -> u64 {
    fs_err::metadata(mappings_path(segment_path)).unwrap().len()
}

/// Slots are handed out consecutively above the highest one in use — across
/// calls, across tracker instances resuming from a known maximum, and skipping
/// deletes, which claim nothing.
#[test]
fn allocates_consecutive_slots() {
    let dir = Builder::new().prefix("update_only").tempdir().unwrap();

    let mut tracker = Tracker::new(MmapFs, dir.path(), None, [], 0).unwrap();

    // Deletes are recorded but claim no slot, so only inserts come back.
    assert_eq!(
        tracker.insert_operations(&[Insert(num(10)), Delete(num(20)), Insert(uuid(11))]),
        Ok(vec![(num(10), 0), (uuid(11), 1)]),
    );
    // A batch of deletes alone allocates nothing, and the next insert still
    // takes the slot right above the last one claimed.
    assert_eq!(
        tracker.insert_operations(&[Delete(num(10))]),
        Ok(Vec::new()),
    );
    assert_eq!(
        tracker.insert_operations(&[Insert(num(12))]),
        Ok(vec![(num(12), 2)]),
    );

    // Re-inserting a live id moves it to a fresh slot rather than rewriting its
    // mapping — the update-only shape of an update.
    assert_eq!(
        tracker.insert_operations(&[Insert(uuid(11))]),
        Ok(vec![(uuid(11), 3)]),
    );

    // A fresh tracker resuming from slot 3, and from the end of the log that
    // handed it out, continues above both.
    let mut resumed = Tracker::new(MmapFs, dir.path(), Some(3), [], log_end(dir.path())).unwrap();
    assert_eq!(
        resumed.insert_operations(&[Insert(num(13))]),
        Ok(vec![(num(13), 4)]),
    );

    // Nothing to record, nothing written.
    assert_eq!(tracker.insert_operations(&[]), Ok(Vec::new()));
}

/// Append a torn entry to the mappings log: a valid entry header and part of
/// the payload it promises, which is what a write that died mid-entry leaves.
fn tear_the_log(segment_path: &Path) {
    let mut file = fs_err::OpenOptions::new()
        .append(true)
        .open(mappings_path(segment_path))
        .unwrap();
    // `MappingChangeType::InsertNum`, then 3 of the 12 bytes it announces.
    file.write_all(&[1, 0xAA, 0xBB, 0xCC]).unwrap();
}

/// A torn entry at the end of the mappings log is cut off by the next write
/// rather than appended after — the point of writing at the end of the *log*
/// instead of the file. Left in place, it would misframe every entry after.
#[test]
#[cfg_attr(
    target_os = "windows",
    ignore = "heal replaces the file while it is still mmap'd, which Windows denies"
)]
fn heals_a_torn_mappings_tail() {
    let dir = Builder::new().prefix("update_only").tempdir().unwrap();
    let path = mappings_path(dir.path());

    let mut tracker = Tracker::new(MmapFs, dir.path(), None, [], 0).unwrap();
    tracker.insert_operations(&[Insert(num(10))]).unwrap();

    let committed = fs_err::read(&path).unwrap();
    tear_the_log(dir.path());

    // A writer resumes from the log's end, which is below the torn bytes.
    let mut resumed =
        Tracker::new(MmapFs, dir.path(), Some(0), [], committed.len() as u64).unwrap();
    assert_eq!(
        resumed.insert_operations(&[Insert(num(11))]),
        Ok(vec![(num(11), 1)]),
    );

    // The entries before the tear are untouched, and the new one took the place
    // of the torn bytes instead of following them.
    let healed = fs_err::read(&path).unwrap();
    assert_eq!(healed[..committed.len()], committed);

    let (mappings, read_to) = load_mappings(&path, None).unwrap();
    assert_eq!(read_to, healed.len() as u64, "log must parse to its end");
    assert_eq!(
        mappings.internal_id_with_behavior(&num(10), DeferredBehavior::VisibleOnly),
        Some(0),
    );
    assert_eq!(
        mappings.internal_id_with_behavior(&num(11), DeferredBehavior::VisibleOnly),
        Some(1),
    );
}

/// A batch that landed unacknowledged is rewritten over itself, not after
/// itself: the writer never learned the first attempt landed, so it still holds
/// the same slots and offset, and the retry leaves one copy rather than two.
#[test]
#[cfg_attr(
    target_os = "windows",
    ignore = "heal replaces the file while it is still mmap'd, which Windows denies"
)]
fn re_appends_a_batch_that_landed_unacknowledged() {
    let landed = Builder::new().prefix("update_only").tempdir().unwrap();
    let retried = Builder::new().prefix("update_only").tempdir().unwrap();

    let batch = [Insert(num(10)), Delete(num(20)), Insert(uuid(11))];

    // One writer gets through the batch; another is left believing it did not,
    // and runs the same batch against the file the first one wrote.
    let mut writer = Tracker::new(MmapFs, landed.path(), None, [], 0).unwrap();
    let inserted = writer.insert_operations(&batch).unwrap();

    fs_err::copy(mappings_path(landed.path()), mappings_path(retried.path())).unwrap();
    let mut retry = Tracker::new(MmapFs, retried.path(), None, [], 0).unwrap();

    // Same slots, and the same bytes in the log: the second attempt is the
    // first one, not a duplicate of it.
    assert_eq!(retry.insert_operations(&batch), Ok(inserted));
    assert_eq!(
        fs_err::read(mappings_path(retried.path())).unwrap(),
        fs_err::read(mappings_path(landed.path())).unwrap(),
    );
}

/// A log the file cannot hold is refused, not healed: healing only drops bytes
/// past the log's end, and a file stopping short of it has none to drop.
#[test]
fn rejects_a_mappings_file_shorter_than_the_log() {
    let dir = Builder::new().prefix("update_only").tempdir().unwrap();

    let mut tracker = Tracker::new(MmapFs, dir.path(), None, [], 64).unwrap();
    tracker
        .insert_operations(&[Insert(num(10))])
        .expect_err("a log longer than its file must be rejected");

    assert_eq!(log_end(dir.path()), 0, "nothing may be appended");
}

/// The array can only grow at its end, and only over slots the log handed out:
/// a covered slot cannot be rewritten through an append, a slot cannot be given
/// two versions, and a slot nobody claimed cannot be published at all. None of
/// the rejections leaves anything behind.
#[test]
fn versions_reject_rewrites_duplicates_and_unclaimed_slots() {
    let dir = Builder::new().prefix("update_only").tempdir().unwrap();
    let path = versions_path(dir.path());

    let mut tracker = Tracker::new(MmapFs, dir.path(), Some(2), [], 0).unwrap();
    tracker.set_internal_versions(&[0, 1], &[100, 101]).unwrap();

    // The log claimed up to slot 2, so slot 3 is nobody's.
    tracker
        .set_internal_versions(&[3], &[103])
        .expect_err("an unclaimed slot must be rejected");
    // Slot 1 is already committed.
    tracker
        .set_internal_versions(&[1], &[111])
        .expect_err("a rewrite must be rejected");
    // The same slot twice in one call.
    tracker
        .set_internal_versions(&[2, 2], &[102, 102])
        .expect_err("a duplicate must be rejected");
    // One id short of the versions it is paired with.
    tracker
        .set_internal_versions(&[2], &[102, 103])
        .expect_err("mismatched lengths must be rejected");

    assert_eq!(load_versions(&path).unwrap(), vec![100, 101]);

    // The rejections left the array appendable.
    tracker.set_internal_versions(&[2], &[102]).unwrap();
    assert_eq!(load_versions(&path).unwrap(), vec![100, 101, 102]);
}

/// Resume a writer from what a reader makes of the segment on disk, the way a
/// caller reopening the segment has to.
fn resume(segment_path: &Path) -> Tracker {
    let reader = ReadOnlyTracker::open(&MmapFs, segment_path, None).unwrap();
    Tracker::new(
        MmapFs,
        segment_path,
        reader.max_claimed_internal_id(),
        reader.pending_inserts(),
        reader.mappings_read_to(),
    )
    .unwrap()
}

/// A claimed slot the call skips over is covered with [`DELETED_POINT_VERSION`]
/// rather than refused. The array is dense, so the slots above it cannot be
/// published any other way.
///
/// [`DELETED_POINT_VERSION`]: crate::id_tracker::DELETED_POINT_VERSION
#[test]
fn fills_skipped_slots() {
    let dir = Builder::new().prefix("update_only").tempdir().unwrap();
    let path = versions_path(dir.path());

    let mut tracker = Tracker::new(MmapFs, dir.path(), None, [], 0).unwrap();
    tracker
        .insert_operations(&[
            Insert(num(10)),
            Insert(num(11)),
            Insert(num(12)),
            Insert(num(13)),
        ])
        .unwrap();

    // Slots 1 and 2 are skipped, one below the committed slot and one between.
    tracker.set_internal_versions(&[0, 3], &[100, 103]).unwrap();

    assert_eq!(
        load_versions(&path).unwrap(),
        vec![100, DELETED_POINT_VERSION, DELETED_POINT_VERSION, 103],
    );
}

/// A slot is spoken for from the moment the log claims it, whatever becomes of
/// the point: a writer that claimed it may have written data at it under any
/// component, so the next writer allocates above it even once its external id
/// is gone from the mapping and from the pending inserts alike.
#[test]
fn resumes_above_a_slot_claimed_and_then_deleted() {
    let dir = Builder::new().prefix("update_only").tempdir().unwrap();

    let mut crashed = Tracker::new(MmapFs, dir.path(), None, [], 0).unwrap();
    crashed.insert_operations(&[Insert(num(10))]).unwrap();
    crashed.set_internal_versions(&[0], &[100]).unwrap();
    // Slot 1 is claimed, never versioned, and then retired.
    crashed
        .insert_operations(&[Insert(num(11)), Delete(num(11))])
        .unwrap();

    let reader = ReadOnlyTracker::open(&MmapFs, dir.path(), None).unwrap();
    assert_eq!(reader.max_claimed_internal_id(), Some(1));
    assert_eq!(
        reader.pending_inserts().count(),
        0,
        "nothing left to retire"
    );
    assert_eq!(
        reader.internal_id_with_behavior(num(11), DeferredBehavior::VisibleOnly),
        None,
        "the point is gone, but its slot is not free",
    );

    assert_eq!(
        resume(dir.path()).insert_operations(&[Insert(num(12))]),
        Ok(vec![(num(12), 2)]),
    );
}

/// A point on a slot the log claimed but no writer ever versioned is retired by
/// the writer that inherits it, before anything of its own reaches the log. Its
/// data was left half-written, so it is in no state to be adopted, and the slot
/// has to be covered for the slots above it to be published at all.
#[test]
fn retires_inherited_pending_inserts() {
    let dir = Builder::new().prefix("update_only").tempdir().unwrap();

    let mut crashed = Tracker::new(MmapFs, dir.path(), None, [], 0).unwrap();
    crashed.insert_operations(&[Insert(num(10))]).unwrap();
    crashed.set_internal_versions(&[0], &[100]).unwrap();
    // Slot 1 is claimed and its data left half-written; the writer stops here.
    crashed.insert_operations(&[Insert(num(11))]).unwrap();

    let inherited = ReadOnlyTracker::open(&MmapFs, dir.path(), None).unwrap();
    assert_eq!(
        inherited.pending_inserts().collect::<Vec<_>>(),
        vec![num(11)],
    );

    // The next writer has to cover slot 1 to publish slot 2.
    let mut resumed = resume(dir.path());
    assert_eq!(
        resumed.insert_operations(&[Insert(num(12))]),
        Ok(vec![(num(12), 2)]),
    );
    resumed.set_internal_versions(&[2], &[102]).unwrap();

    let reader = ReadOnlyTracker::open(&MmapFs, dir.path(), None).unwrap();
    assert_eq!(
        reader.internal_id_with_behavior(num(11), DeferredBehavior::VisibleOnly),
        None,
        "an abandoned point must not surface once its slot is covered",
    );
    assert_eq!(reader.pending_inserts().count(), 0);
    assert!(reader.is_deleted_point(1));
    assert_eq!(
        reader.internal_id_with_behavior(num(10), DeferredBehavior::VisibleOnly),
        Some(0),
    );
    assert_eq!(
        reader.internal_id_with_behavior(num(12), DeferredBehavior::VisibleOnly),
        Some(2),
    );
    assert_eq!(reader.available_point_count(), 2);
}

/// Retiring is done by the time the writer exists, so no write path can reach
/// the versions file without it — not even one that commits versions and never
/// touches the mappings log itself. Opening the writer is enough.
#[test]
fn retires_inherited_pending_inserts_at_construction() {
    let dir = Builder::new().prefix("update_only").tempdir().unwrap();

    let mut crashed = Tracker::new(MmapFs, dir.path(), None, [], 0).unwrap();
    crashed
        .insert_operations(&[Insert(num(10)), Insert(num(11))])
        .unwrap();
    // Slot 0 is versioned below; slot 1 is left abandoned.
    crashed.set_internal_versions(&[0], &[100]).unwrap();

    let mut resumed = resume(dir.path());
    // Construction alone retired it: the log already says so, before this writer
    // has been asked to do anything.
    assert_eq!(
        ReadOnlyTracker::open(&MmapFs, dir.path(), None)
            .unwrap()
            .pending_inserts()
            .count(),
        0,
    );

    resumed.set_internal_versions(&[1], &[101]).unwrap();

    let reader = ReadOnlyTracker::open(&MmapFs, dir.path(), None).unwrap();
    assert_eq!(
        reader.internal_id_with_behavior(num(11), DeferredBehavior::VisibleOnly),
        None,
    );
    assert!(reader.is_deleted_point(1));
    assert_eq!(reader.available_point_count(), 1);
}

/// An update whose new slot is abandoned costs the point, not just the
/// unacknowledged update: the same partial write that abandoned the new slot
/// has likely tombstoned the old one in the components already, so there is no
/// earlier state to fall back to.
#[test]
fn an_abandoned_update_retires_the_point() {
    let dir = Builder::new().prefix("update_only").tempdir().unwrap();

    let mut crashed = Tracker::new(MmapFs, dir.path(), None, [], 0).unwrap();
    crashed.insert_operations(&[Insert(num(10))]).unwrap();
    crashed.set_internal_versions(&[0], &[100]).unwrap();
    // Re-inserting moves the id to a fresh slot; the writer stops before the
    // version that would publish it.
    assert_eq!(
        crashed.insert_operations(&[Insert(num(10))]),
        Ok(vec![(num(10), 1)]),
    );

    let mut resumed = resume(dir.path());
    resumed.insert_operations(&[Insert(num(11))]).unwrap();
    resumed.set_internal_versions(&[2], &[102]).unwrap();

    let reader = ReadOnlyTracker::open(&MmapFs, dir.path(), None).unwrap();
    assert_eq!(
        reader.internal_id_with_behavior(num(10), DeferredBehavior::VisibleOnly),
        None,
        "neither the abandoned slot nor the one it superseded may be served",
    );
    assert!(reader.is_deleted_point(0));
    assert!(reader.is_deleted_point(1));
    assert_eq!(
        reader.internal_id_with_behavior(num(11), DeferredBehavior::VisibleOnly),
        Some(2),
    );
}

/// A write that dies mid-entry leaves the versions file ending inside a slot.
/// The next write heals it rather than being stuck with it forever: those bytes
/// are a slot no reader ever saw, so the new entries take their place.
#[test]
#[cfg_attr(
    target_os = "windows",
    ignore = "heal replaces the file while it is still mmap'd, which Windows denies"
)]
fn heals_a_partial_versions_tail() {
    let dir = Builder::new().prefix("update_only").tempdir().unwrap();
    let path = versions_path(dir.path());

    let mut tracker = Tracker::new(MmapFs, dir.path(), Some(3), [], 0).unwrap();
    tracker.set_internal_versions(&[0, 1], &[100, 101]).unwrap();

    // Three bytes into slot 2, as a torn write would leave it.
    let mut torn = fs_err::read(&path).unwrap();
    torn.extend_from_slice(&[0xFF; 3]);
    fs_err::write(&path, &torn).unwrap();

    tracker.set_internal_versions(&[2, 3], &[102, 103]).unwrap();

    // The committed slots survived, and the appended ones landed where the
    // stray bytes were rather than after them.
    assert_eq!(load_versions(&path).unwrap(), vec![100, 101, 102, 103]);
    assert_eq!(
        fs_err::metadata(&path).unwrap().len(),
        4 * size_of::<SeqNumberType>() as u64,
    );
}

/// Healing a file that holds nothing but a torn entry leaves it empty, and the
/// array starts over at slot 0.
#[test]
#[cfg_attr(
    target_os = "windows",
    ignore = "heal replaces the file while it is still mmap'd, which Windows denies"
)]
fn heals_a_versions_file_of_only_a_partial_tail() {
    let dir = Builder::new().prefix("update_only").tempdir().unwrap();
    let path = versions_path(dir.path());

    fs_err::write(&path, [0xFF; 5]).unwrap();

    let mut tracker = Tracker::new(MmapFs, dir.path(), Some(0), [], 0).unwrap();
    tracker.set_internal_versions(&[0], &[100]).unwrap();

    assert_eq!(load_versions(&path).unwrap(), vec![100]);
}

/// The append-only writer and the in-place one lay the versions file out
/// identically, byte for byte. This is what keeps the two write paths from
/// drifting apart: a change to either one's layout has to be made in both.
#[test]
fn both_writers_produce_the_same_versions_file() {
    let appended = Builder::new().prefix("update_only").tempdir().unwrap();
    let stored = Builder::new().prefix("mutable").tempdir().unwrap();

    let internal_ids: Vec<PointOffsetType> = vec![0, 1, 2, 3];
    let versions: Vec<SeqNumberType> = vec![100, 7, u64::MAX, 0];

    let mut tracker = Tracker::new(MmapFs, appended.path(), Some(3), [], 0).unwrap();
    tracker
        .set_internal_versions(&internal_ids, &versions)
        .unwrap();

    let changes: BTreeMap<PointOffsetType, SeqNumberType> = internal_ids
        .iter()
        .copied()
        .zip(versions.iter().copied())
        .collect();
    store_version_changes(&versions_path(stored.path()), &changes).unwrap();

    let appended_bytes = fs_err::read(versions_path(appended.path())).unwrap();
    assert_eq!(
        appended_bytes,
        fs_err::read(versions_path(stored.path())).unwrap(),
    );
    // Not vacuous: both hold the versions, one entry per slot.
    assert_eq!(
        appended_bytes.len(),
        versions.len() * size_of::<SeqNumberType>()
    );
    assert_eq!(
        load_versions(&versions_path(appended.path())).unwrap(),
        versions,
    );
}

/// Everything the two readers must agree on for the segments to be
/// interchangeable.
///
/// Versions are compared for live points only. A deleted point's version is
/// gone as far as readers are concerned, and the two writers leave different
/// things behind: [`MutableIdTracker::drop`] overwrites the slot with
/// [`DELETED_POINT_VERSION`], while the append-only writer cannot rewrite a
/// committed slot and leaves the point's original version there.
///
/// [`DELETED_POINT_VERSION`]: crate::id_tracker::DELETED_POINT_VERSION
fn assert_trackers_agree(appended: &ReadOnlyTracker, mutated: &ReadOnlyTracker) {
    assert_eq!(appended.total_point_count(), mutated.total_point_count());
    assert_eq!(
        appended.available_point_count(),
        mutated.available_point_count(),
    );
    assert_eq!(
        appended.deleted_point_count(),
        mutated.deleted_point_count()
    );

    for slot in 0..appended.total_point_count() as PointOffsetType {
        assert_eq!(
            appended.is_deleted_point(slot),
            mutated.is_deleted_point(slot),
            "deleted state mismatch at slot {slot}",
        );
        assert_eq!(
            appended.external_id(slot),
            mutated.external_id(slot),
            "external id mismatch at slot {slot}",
        );
        if !appended.is_deleted_point(slot) {
            assert_eq!(
                appended.internal_version(slot),
                mutated.internal_version(slot),
                "version mismatch at slot {slot}",
            );
        }
    }
}

/// The append-only writer and [`MutableIdTracker`] are two ways of producing
/// the same segment files. Drive both with the same points, versions and
/// deletes, then read each back through [`ReadOnlyAppendableIdTracker`]: the
/// two views must be indistinguishable.
#[test]
fn matches_the_mutable_tracker() {
    let appended_dir = Builder::new().prefix("update_only").tempdir().unwrap();
    let mutated_dir = Builder::new().prefix("mutable").tempdir().unwrap();

    let mut writer = Tracker::new(MmapFs, appended_dir.path(), None, [], 0).unwrap();
    let mut mutable = MutableIdTracker::open(mutated_dir.path(), None).unwrap();

    // The append-only writer hands out the slots; the mutable tracker is told
    // to use exactly the ones it handed out. A delete of an id that was never
    // inserted is recorded by both and must change nothing.
    let inserted = writer
        .insert_operations(&[
            Insert(num(10)),
            Insert(uuid(11)),
            Insert(num(12)),
            Delete(num(99)),
        ])
        .unwrap();
    let versions: Vec<SeqNumberType> = vec![100, 101, 102];
    let slots: Vec<PointOffsetType> = inserted.iter().map(|(_, slot)| *slot).collect();
    writer.set_internal_versions(&slots, &versions).unwrap();

    for ((external_id, slot), version) in inserted.iter().zip(&versions) {
        mutable.set_link(*external_id, *slot).unwrap();
        mutable.set_internal_version(*slot, *version).unwrap();
    }
    mutable.drop(num(99)).unwrap();

    // Then retire a live point through both.
    writer.insert_operations(&[Delete(uuid(11))]).unwrap();
    mutable.drop(uuid(11)).unwrap();

    mutable.mapping_flusher()().unwrap();
    mutable.versions_flusher()().unwrap();

    let from_appended = ReadOnlyTracker::open(&MmapFs, appended_dir.path(), None).unwrap();
    let from_mutated = ReadOnlyTracker::open(&MmapFs, mutated_dir.path(), None).unwrap();

    assert_trackers_agree(&from_appended, &from_mutated);

    for external_id in [num(10), uuid(11), num(12), num(99)] {
        assert_eq!(
            from_appended.internal_id_with_behavior(external_id, DeferredBehavior::VisibleOnly),
            from_mutated.internal_id_with_behavior(external_id, DeferredBehavior::VisibleOnly),
            "resolution mismatch for {external_id}",
        );
    }

    // Not vacuous: three points were committed, one of them since retired.
    assert_eq!(from_appended.total_point_count(), 3);
    assert_eq!(from_appended.available_point_count(), 2);
    assert_eq!(
        from_appended.internal_id_with_behavior(num(10), DeferredBehavior::VisibleOnly),
        Some(0),
    );
    assert_eq!(
        from_appended.internal_id_with_behavior(uuid(11), DeferredBehavior::VisibleOnly),
        None,
    );
    assert_eq!(from_appended.internal_version(2), Some(102));
}
