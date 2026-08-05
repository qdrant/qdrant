use common::types::{DeferredBehavior, PointOffsetType};
use common::universal_io::{MmapFile, MmapFs};
use tempfile::Builder;
use uuid::Uuid;

use super::MappingOperation::{Delete, Insert};
use super::UpdateOnlyAppendableIdTracker;
use crate::id_tracker::mutable_id_tracker::mappings_storage::load_mappings;
use crate::id_tracker::mutable_id_tracker::versions_storage::{load_versions, versions_path};
use crate::id_tracker::mutable_id_tracker::{mappings_storage, versions_storage};
use crate::id_tracker::point_mappings::PointMappings;
use crate::types::{PointIdType, SeqNumberType};

type Tracker = UpdateOnlyAppendableIdTracker<MmapFile>;

fn num(id: u64) -> PointIdType {
    PointIdType::NumId(id)
}

fn uuid(id: u128) -> PointIdType {
    PointIdType::Uuid(Uuid::from_u128(id))
}

/// The slot `external_id` resolves to for a reader of the persisted log.
fn internal_id(mappings: &PointMappings, external_id: PointIdType) -> Option<PointOffsetType> {
    mappings.internal_id_with_behavior(&external_id, DeferredBehavior::VisibleOnly)
}

/// Slots are handed out consecutively above the highest one in use, across
/// calls and across tracker instances resuming from a known maximum.
#[test]
fn allocates_consecutive_slots() {
    let dir = Builder::new().prefix("update_only").tempdir().unwrap();

    let mut tracker = Tracker::new(MmapFs, dir.path(), None);
    assert_eq!(
        tracker.insert_operations(&[Insert(num(10)), Insert(uuid(11))]),
        Ok(vec![(num(10), 0), (uuid(11), 1)]),
    );
    assert_eq!(
        tracker.insert_operations(&[Insert(num(12))]),
        Ok(vec![(num(12), 2)]),
    );

    // A fresh tracker resuming from slot 2 continues above it.
    let mut resumed = Tracker::new(MmapFs, dir.path(), Some(2));
    assert_eq!(
        resumed.insert_operations(&[Insert(num(13))]),
        Ok(vec![(num(13), 3)]),
    );

    // Nothing to record, nothing written.
    assert_eq!(tracker.insert_operations(&[]), Ok(Vec::new()));
}

/// Deletes are recorded but claim no slot: only inserts come back, and only
/// they move the allocation forward.
#[test]
fn deletes_claim_no_slot() {
    let dir = Builder::new().prefix("update_only").tempdir().unwrap();
    let path = mappings_storage::mappings_path(dir.path());

    let mut tracker = Tracker::new(MmapFs, dir.path(), None);
    let operations = [
        Insert(num(10)),
        Delete(num(20)),
        Insert(uuid(11)),
        Delete(uuid(21)),
    ];
    assert_eq!(
        tracker.insert_operations(&operations),
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

    // The reader ends up with what the log says: 10 was inserted then deleted,
    // and the deletes of ids that were never inserted changed nothing.
    let (mappings, _read_to) = load_mappings(&path, None).unwrap();
    assert_eq!(internal_id(&mappings, num(10)), None);
    assert_eq!(internal_id(&mappings, uuid(11)), Some(1));
    assert_eq!(internal_id(&mappings, num(12)), Some(2));
    assert_eq!(internal_id(&mappings, num(20)), None);
}

/// Re-inserting a live external id moves it to a fresh slot instead of
/// rewriting its mapping — the update-only shape of an update.
#[test]
fn reinsert_moves_the_point_to_a_fresh_slot() {
    let dir = Builder::new().prefix("update_only").tempdir().unwrap();
    let path = mappings_storage::mappings_path(dir.path());

    let mut tracker = Tracker::new(MmapFs, dir.path(), None);
    assert_eq!(
        tracker.insert_operations(&[Insert(num(10)), Insert(num(11))]),
        Ok(vec![(num(10), 0), (num(11), 1)]),
    );
    assert_eq!(
        tracker.insert_operations(&[Insert(num(10))]),
        Ok(vec![(num(10), 2)]),
    );

    let (mappings, _read_to) = load_mappings(&path, None).unwrap();
    assert_eq!(internal_id(&mappings, num(10)), Some(2));
    // The superseded slot keeps its data; it is no longer reachable by id.
    assert_eq!(mappings.external_id(0), None);
}

/// What the writer appends is what the reader's loader reconstructs: every
/// entry is durable by the time the call returns, without any flush of ours.
#[test]
fn mappings_are_persisted_per_call() {
    let dir = Builder::new().prefix("update_only").tempdir().unwrap();
    let path = mappings_storage::mappings_path(dir.path());

    let mut tracker = Tracker::new(MmapFs, dir.path(), None);
    // The file only appears with the first write.
    assert!(!path.exists());

    let inserted = tracker
        .insert_operations(&[Insert(num(10)), Insert(uuid(11)), Insert(num(12))])
        .unwrap();

    let (mappings, read_to) = load_mappings(&path, None).unwrap();
    assert_eq!(read_to, fs_err::metadata(&path).unwrap().len());
    for (external_id, slot) in inserted {
        assert_eq!(internal_id(&mappings, external_id), Some(slot));
        assert_eq!(mappings.external_id(slot), Some(external_id));
    }
}

/// Versions extend the dense array, one slot per element, and are readable
/// right after the call returns.
#[test]
fn versions_extend_the_dense_array() {
    let dir = Builder::new().prefix("update_only").tempdir().unwrap();
    let path = versions_path(dir.path());

    let mut tracker = Tracker::new(MmapFs, dir.path(), None);
    assert!(!path.exists());

    // Out-of-order ids are fine, as long as they cover the next slots.
    tracker.set_internal_versions(&[1, 0], &[101, 100]).unwrap();
    assert_eq!(load_versions(&path).unwrap(), vec![100, 101]);

    tracker.set_internal_versions(&[2], &[102]).unwrap();
    assert_eq!(load_versions(&path).unwrap(), vec![100, 101, 102]);

    // Nothing to commit, nothing written.
    tracker.set_internal_versions(&[], &[]).unwrap();
    assert_eq!(load_versions(&path).unwrap(), vec![100, 101, 102]);
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

/// The two files a segment's readers consume line up: every allocated slot has
/// a version at its own index.
#[test]
fn slots_and_versions_line_up() {
    let dir = Builder::new().prefix("update_only").tempdir().unwrap();

    let mut tracker = Tracker::new(MmapFs, dir.path(), None);
    let inserted = tracker
        .insert_operations(&[Insert(num(10)), Delete(num(9)), Insert(uuid(11))])
        .unwrap();

    // Versions are committed for the slots the inserts claimed, in slot order.
    let internal_ids: Vec<PointOffsetType> = inserted.iter().map(|(_, slot)| *slot).collect();
    let versions: Vec<SeqNumberType> = vec![100, 101];
    tracker
        .set_internal_versions(&internal_ids, &versions)
        .unwrap();

    let (mappings, _read_to) =
        load_mappings(&mappings_storage::mappings_path(dir.path()), None).unwrap();
    let persisted_versions = load_versions(&versions_storage::versions_path(dir.path())).unwrap();

    for (index, (external_id, slot)) in inserted.iter().enumerate() {
        assert_eq!(internal_id(&mappings, *external_id), Some(*slot));
        assert_eq!(*slot, index as PointOffsetType);
        assert_eq!(persisted_versions[*slot as usize], versions[index]);
    }
}
