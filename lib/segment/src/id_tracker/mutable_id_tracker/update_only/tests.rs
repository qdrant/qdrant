use std::collections::BTreeMap;

use common::types::{DeferredBehavior, PointOffsetType};
use common::universal_io::{MmapFile, MmapFs};
use tempfile::Builder;
use uuid::Uuid;

use super::MappingOperation::{Delete, Insert};
use super::UpdateOnlyAppendableIdTracker;
use crate::id_tracker::mutable_id_tracker::MutableIdTracker;
use crate::id_tracker::mutable_id_tracker::read_only::ReadOnlyAppendableIdTracker;
use crate::id_tracker::mutable_id_tracker::versions_storage::{
    load_versions, store_version_changes, versions_path,
};
use crate::id_tracker::{IdTracker, IdTrackerRead};
use crate::types::{PointIdType, SeqNumberType};

type Tracker = UpdateOnlyAppendableIdTracker<MmapFile>;
type ReadOnlyTracker = ReadOnlyAppendableIdTracker<MmapFile>;

fn num(id: u64) -> PointIdType {
    PointIdType::NumId(id)
}

fn uuid(id: u128) -> PointIdType {
    PointIdType::Uuid(Uuid::from_u128(id))
}

/// Slots are handed out consecutively above the highest one in use — across
/// calls, across tracker instances resuming from a known maximum, and skipping
/// deletes, which claim nothing.
#[test]
fn allocates_consecutive_slots() {
    let dir = Builder::new().prefix("update_only").tempdir().unwrap();

    let mut tracker = Tracker::new(MmapFs, dir.path(), None);

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

    // A fresh tracker resuming from slot 3 continues above it.
    let mut resumed = Tracker::new(MmapFs, dir.path(), Some(3));
    assert_eq!(
        resumed.insert_operations(&[Insert(num(13))]),
        Ok(vec![(num(13), 4)]),
    );

    // Nothing to record, nothing written.
    assert_eq!(tracker.insert_operations(&[]), Ok(Vec::new()));
}

/// The array can only grow at its end: a hole would publish a slot whose data
/// is not written yet, and a covered slot cannot be rewritten through an
/// append. Both are rejected, and neither leaves anything behind.
#[test]
fn versions_reject_holes_and_rewrites() {
    let dir = Builder::new().prefix("update_only").tempdir().unwrap();
    let path = versions_path(dir.path());

    let mut tracker = Tracker::new(MmapFs, dir.path(), None);
    tracker.set_internal_versions(&[0, 1], &[100, 101]).unwrap();

    // Slot 2 is missing.
    tracker
        .set_internal_versions(&[3], &[103])
        .expect_err("a hole must be rejected");
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

/// The append-only writer and the in-place one lay the versions file out
/// identically, byte for byte. This is what keeps the two write paths from
/// drifting apart: a change to either one's layout has to be made in both.
#[test]
fn both_writers_produce_the_same_versions_file() {
    let appended = Builder::new().prefix("update_only").tempdir().unwrap();
    let stored = Builder::new().prefix("mutable").tempdir().unwrap();

    let internal_ids: Vec<PointOffsetType> = vec![0, 1, 2, 3];
    let versions: Vec<SeqNumberType> = vec![100, 7, u64::MAX, 0];

    let mut tracker = Tracker::new(MmapFs, appended.path(), None);
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

    let mut writer = Tracker::new(MmapFs, appended_dir.path(), None);
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
