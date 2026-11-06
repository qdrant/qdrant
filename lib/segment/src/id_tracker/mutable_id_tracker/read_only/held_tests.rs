//! A read-only appendable tracker that gates deletes holds a versioned delete above its gate, and
//! applies it on release to the slots the point held when it was deleted.

use common::types::{DeferredBehavior, PointOffsetType};
use common::universal_io::{MmapFile, MmapFs};
use tempfile::{Builder, TempDir};

use super::ReadOnlyAppendableIdTracker;
use crate::id_tracker::IdTrackerRead;
use crate::id_tracker::delete_versions::{DeleteGating, DeleteVersioning};
use crate::id_tracker::mutable_id_tracker::update_only::MappingOperation::Insert;
use crate::id_tracker::mutable_id_tracker::update_only::UpdateOnlyAppendableIdTracker;
use crate::types::{PointIdType, SeqNumberType};

type ReadOnlyTracker = ReadOnlyAppendableIdTracker<MmapFile>;

fn num(id: u64) -> PointIdType {
    PointIdType::NumId(id)
}

/// A writer over a fresh segment directory.
fn writer() -> (TempDir, UpdateOnlyAppendableIdTracker) {
    let dir = Builder::new().prefix("held").tempdir().unwrap();
    let writer = UpdateOnlyAppendableIdTracker::new(&MmapFs, dir.path(), None, [], 0).unwrap();
    (dir, writer)
}

/// Store `ids` at `version`, returning their slots.
fn store(
    writer: &mut UpdateOnlyAppendableIdTracker,
    ids: &[u64],
    version: SeqNumberType,
) -> Vec<PointOffsetType> {
    let operations: Vec<_> = ids.iter().map(|&id| Insert(num(id))).collect();
    let slots: Vec<PointOffsetType> = writer
        .insert_operations(&MmapFs, &operations)
        .unwrap()
        .into_iter()
        .map(|(_, slot)| slot)
        .collect();
    writer
        .set_internal_versions(&MmapFs, &slots, &vec![version; slots.len()])
        .unwrap();
    slots
}

fn delete(writer: &mut UpdateOnlyAppendableIdTracker, ids: &[u64], version: SeqNumberType) {
    writer
        .delete_points(
            &MmapFs,
            ids.iter().map(|&id| num(id)),
            version,
            DeleteVersioning::Record,
        )
        .unwrap();
}

fn slot_of(tracker: &ReadOnlyTracker, id: u64) -> Option<PointOffsetType> {
    tracker.internal_id_with_behavior(num(id), DeferredBehavior::VisibleOnly)
}

fn reload(tracker: &mut ReadOnlyTracker) -> Vec<PointOffsetType> {
    let result = tracker.live_reload(&MmapFs, None).unwrap();
    tracker.publish_staged();
    result.deleted
}

/// Deletes at or below the gate apply at once, at open and on reload; newer ones keep their point
/// visible until a release covers them.
#[test]
fn deletes_wait_for_their_gate() {
    let (dir, mut writer) = writer();
    let slots = store(&mut writer, &[1, 2, 3], 4);
    delete(&mut writer, &[1], 5);
    delete(&mut writer, &[2], 9);

    let mut tracker = ReadOnlyTracker::open_capped(
        &MmapFs,
        dir.path(),
        None,
        None,
        DeleteGating::Held { gate: 6 },
    )
    .unwrap();
    assert_eq!(slot_of(&tracker, 1), None);
    assert_eq!(slot_of(&tracker, 2), Some(slots[1]));
    assert!(!tracker.releases_held_deletes(8));

    delete(&mut writer, &[3], 6);
    assert_eq!(reload(&mut tracker), [slots[2]]);

    assert!(tracker.releases_held_deletes(9));
    assert_eq!(tracker.release_held_deletes(9), [slots[1]]);
    assert_eq!(slot_of(&tracker, 2), None);
    assert_eq!(tracker.release_held_deletes(10), Vec::<u32>::new());

    // An immediate tracker applies them all at once
    let immediate =
        ReadOnlyTracker::open_capped(&MmapFs, dir.path(), None, None, DeleteGating::Immediate)
            .unwrap();
    for id in [1, 2, 3] {
        assert_eq!(slot_of(&immediate, id), None);
    }
}

/// A point stored again after a held delete keeps its new copy: the release only drops the slot it
/// held when it was deleted.
#[test]
fn release_keeps_a_later_copy() {
    let (dir, mut writer) = writer();
    let first = store(&mut writer, &[7], 4)[0];
    delete(&mut writer, &[7], 9);
    let second = store(&mut writer, &[7], 10)[0];

    let mut tracker = ReadOnlyTracker::open_capped(
        &MmapFs,
        dir.path(),
        None,
        None,
        DeleteGating::Held { gate: 0 },
    )
    .unwrap();
    assert_eq!(slot_of(&tracker, 7), Some(second));
    assert_eq!(tracker.release_held_deletes(9), Vec::<u32>::new());
    assert_eq!(slot_of(&tracker, 7), Some(second));
    assert!(tracker.is_deleted_point(first));
}

/// A delete held while the point's insert waits for its version applies to that insert too, once
/// it is published and the delete released.
#[test]
fn release_covers_an_insert_published_later() {
    let (dir, mut writer) = writer();
    store(&mut writer, &[1], 4);
    let slot = store(&mut writer, &[3], 5)[0];
    delete(&mut writer, &[3], 9);

    // The reader does not see the version of point 3 yet
    let mut tracker = ReadOnlyTracker::open_capped(
        &MmapFs,
        dir.path(),
        None,
        Some(slot),
        DeleteGating::Held { gate: 0 },
    )
    .unwrap();
    assert_eq!(slot_of(&tracker, 3), None);
    reload(&mut tracker);
    assert_eq!(slot_of(&tracker, 3), Some(slot));

    assert_eq!(tracker.release_held_deletes(9), [slot]);
    assert_eq!(slot_of(&tracker, 3), None);
    assert_eq!(slot_of(&tracker, 1), Some(0));
}
