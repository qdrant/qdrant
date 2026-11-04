//! Read-only immutable and disk-resident trackers apply the union of the mask and the delete log.
//! Gating ones apply the log's deletes at or below the gate, and hold newer ones until a release
//! covers them.

use std::path::Path;

use common::mmap::AdviceSetting;
use common::stored_bitslice::StoredBitSlice;
use common::types::{DeferredBehavior, PointOffsetType};
use common::universal_io::{MmapFile, MmapFs, OpenOptions, Populate};
use tempfile::{Builder, TempDir};

use super::{DeleteGating, DeleteVersioning};
use crate::id_tracker::compressed::compressed_point_mappings::CompressedPointMappings;
use crate::id_tracker::disk_id_tracker::{DiskIdTracker, ReadOnlyDiskIdTracker};
use crate::id_tracker::immutable_id_tracker::ImmutableIdTracker;
use crate::id_tracker::immutable_id_tracker::read_only::ReadOnlyImmutableIdTracker;
use crate::id_tracker::immutable_id_tracker::update_only::UpdateOnlyImmutableIdTracker;
use crate::id_tracker::in_memory_id_tracker::InMemoryIdTracker;
use crate::id_tracker::read_only_tracker_enum::ReadOnlyIdTrackerEnum;
use crate::id_tracker::{IdTracker as _, IdTrackerRead};
use crate::types::{PointIdType, SeqNumberType};

const POINTS: u64 = 10;

fn num(id: u64) -> PointIdType {
    PointIdType::NumId(id)
}

#[derive(Clone, Copy, Debug)]
enum Format {
    Immutable,
    DiskResident,
}

const FORMATS: [Format; 2] = [Format::Immutable, Format::DiskResident];

/// A segment of points 0 to 9 at slots 0 to 9, point 2 deleted at build time.
fn segment(format: Format) -> TempDir {
    let dir = Builder::new().prefix("held").tempdir().unwrap();
    let mut in_memory = InMemoryIdTracker::new();
    for id in 0..POINTS {
        in_memory.set_link(num(id), id as PointOffsetType).unwrap();
        in_memory
            .set_internal_version(id as PointOffsetType, 3)
            .unwrap();
    }
    in_memory.drop(num(2)).unwrap();
    let (versions, mappings) = in_memory.into_internal();
    let mappings = CompressedPointMappings::from_mappings(mappings);
    match format {
        Format::Immutable => drop(
            ImmutableIdTracker::<MmapFile>::new(&MmapFs, dir.path(), &versions, mappings).unwrap(),
        ),
        Format::DiskResident => {
            drop(DiskIdTracker::<MmapFile>::new(&MmapFs, dir.path(), &versions, mappings).unwrap())
        }
    }
    dir
}

/// Delete `slots` the way the updater does, recording `version`.
fn delete(path: &Path, slots: &[PointOffsetType], version: SeqNumberType) {
    let points: Vec<_> = slots
        .iter()
        .map(|&slot| (num(u64::from(slot)), slot))
        .collect();
    UpdateOnlyImmutableIdTracker::new(path, None)
        .unwrap()
        .tombstone_points(&MmapFs, &points, version, DeleteVersioning::Record)
        .unwrap();
}

fn open(format: Format, path: &Path, gating: DeleteGating) -> ReadOnlyIdTrackerEnum<MmapFile> {
    match format {
        Format::Immutable => ReadOnlyIdTrackerEnum::Immutable(
            ReadOnlyImmutableIdTracker::open(&MmapFs, path, gating).unwrap(),
        ),
        Format::DiskResident => ReadOnlyIdTrackerEnum::DiskResident(
            ReadOnlyDiskIdTracker::try_open(&MmapFs, path, Populate::No, gating)
                .unwrap()
                .unwrap(),
        ),
    }
}

/// The slots `tracker` serves as deleted, checked through point lookups and the deleted set alike.
fn deleted(tracker: &ReadOnlyIdTrackerEnum<MmapFile>) -> Vec<PointOffsetType> {
    let deleted: Vec<PointOffsetType> = (0..POINTS as PointOffsetType)
        .filter(|&slot| tracker.is_deleted_point(slot))
        .collect();
    for slot in 0..POINTS as PointOffsetType {
        let resolved =
            tracker.internal_id_with_behavior(num(u64::from(slot)), DeferredBehavior::VisibleOnly);
        assert_eq!(resolved.is_none(), deleted.contains(&slot), "slot {slot}");
    }
    let from_bitslice: Vec<PointOffsetType> = tracker
        .deleted_point_bitslice()
        .iter_ones()
        .map(|slot| slot as PointOffsetType)
        .filter(|&slot| slot < POINTS as PointOffsetType)
        .collect();
    assert_eq!(from_bitslice, deleted);
    deleted
}

fn reload(tracker: &mut ReadOnlyIdTrackerEnum<MmapFile>) -> Vec<PointOffsetType> {
    tracker.live_reload(&MmapFs, None).unwrap().deleted
}

/// Deletes at or below the gate apply at open and on reload, newer ones are held until a release
/// covers them. The deletes in the mask apply at once.
#[cfg_attr(
    windows,
    ignore = "the mask rewrite replaces id_tracker.deleted while the reader holds it \
              memory-mapped, which Windows refuses"
)]
#[test]
fn deletes_wait_for_their_gate() {
    for format in FORMATS {
        let dir = segment(format);
        delete(dir.path(), &[5], 7);
        delete(dir.path(), &[6], 9);

        let mut tracker = open(format, dir.path(), DeleteGating::Held { gate: 7 });
        assert_eq!(deleted(&tracker), [2, 5], "{format:?}");
        assert!(!tracker.releases_held_deletes(8));

        delete(dir.path(), &[4], 7);
        delete(dir.path(), &[8], 8);
        assert_eq!(reload(&mut tracker), [4], "{format:?}");
        assert_eq!(deleted(&tracker), [2, 4, 5], "{format:?}");

        assert!(tracker.releases_held_deletes(8));
        assert_eq!(tracker.release_held_deletes(8), [8], "{format:?}");
        // A lower gate never lowers it
        assert_eq!(tracker.release_held_deletes(1), Vec::<u32>::new());
        assert_eq!(tracker.release_held_deletes(9), [6], "{format:?}");
        assert_eq!(deleted(&tracker), [2, 4, 5, 6, 8], "{format:?}");

        // Once the gate covers a version, its deletes apply as they are read
        delete(dir.path(), &[9], 9);
        assert_eq!(reload(&mut tracker), [9], "{format:?}");
    }
}

/// An immediate tracker applies every delete of the log as read.
#[cfg_attr(
    windows,
    ignore = "the mask rewrite replaces id_tracker.deleted while the reader holds it \
              memory-mapped, which Windows refuses"
)]
#[test]
fn immediate_trackers_apply_the_log() {
    for format in FORMATS {
        let dir = segment(format);
        delete(dir.path(), &[5], 7);
        let mut tracker = open(format, dir.path(), DeleteGating::Immediate);
        assert_eq!(deleted(&tracker), [2, 5], "{format:?}");
        delete(dir.path(), &[6], 9);
        assert_eq!(reload(&mut tracker), [6], "{format:?}");
        assert!(!tracker.releases_held_deletes(u64::MAX));
    }
}

/// A legacy writer's deletes reach the mask and apply at once, a recording one's reach the log and
/// wait for their gate.
#[cfg_attr(
    windows,
    ignore = "the mask rewrite replaces id_tracker.deleted while the reader holds it \
              memory-mapped, which Windows refuses"
)]
#[test]
fn mask_and_log_apply_as_a_union() {
    for format in FORMATS {
        let dir = segment(format);
        let mut tracker = open(format, dir.path(), DeleteGating::Held { gate: 0 });
        assert_eq!(deleted(&tracker), [2], "{format:?}");

        UpdateOnlyImmutableIdTracker::new(dir.path(), None)
            .unwrap()
            .tombstone_points(&MmapFs, &[(num(3), 3)], 5, DeleteVersioning::Legacy)
            .unwrap();
        assert_eq!(reload(&mut tracker), [3], "{format:?}");

        delete(dir.path(), &[7], 6);
        assert_eq!(reload(&mut tracker), Vec::<u32>::new());
        assert_eq!(deleted(&tracker), [2, 3], "{format:?}");
        assert_eq!(tracker.release_held_deletes(6), [7], "{format:?}");
    }
}

/// A delete set in the mask applies at once, also for a slot whose versioned delete is held: a
/// later release does not report it again.
#[cfg_attr(
    windows,
    ignore = "the mask rewrite replaces id_tracker.deleted while the reader holds it \
              memory-mapped, which Windows refuses"
)]
#[test]
fn mask_deletes_apply_at_once() {
    for format in FORMATS {
        let dir = segment(format);
        delete(dir.path(), &[6], 200);
        let mut tracker = open(format, dir.path(), DeleteGating::Held { gate: 100 });
        assert_eq!(deleted(&tracker), [2], "{format:?}");

        StoredBitSlice::<MmapFile>::atomic_update(
            &MmapFs,
            dir.path().join("id_tracker.deleted"),
            OpenOptions {
                writeable: false,
                need_sequential: false,
                populate: Populate::No,
                advice: AdviceSetting::Global,
            },
            (),
            None,
            |mask| {
                mask.set(5, true);
                mask.set(6, true);
                Ok::<_, std::convert::Infallible>(())
            },
        )
        .unwrap()
        .unwrap();
        assert_eq!(reload(&mut tracker), [5, 6], "{format:?}");
        assert_eq!(deleted(&tracker), [2, 5, 6], "{format:?}");
        assert_eq!(tracker.release_held_deletes(200), Vec::<u32>::new());

        let fresh = open(format, dir.path(), DeleteGating::Held { gate: 100 });
        assert_eq!(deleted(&fresh), [2, 5, 6], "{format:?}");
    }
}

/// A disk-resident tracker's mask file is padded past its point count: applying deletes from the
/// log, it serves the same deleted set and counts whether it gates them or not.
#[test]
fn padded_disk_mask_reads_like_the_log() {
    let dir = segment(Format::DiskResident);
    delete(dir.path(), &[5, 9], 7);
    let from_log = open(
        Format::DiskResident,
        dir.path(),
        DeleteGating::Held { gate: 7 },
    );
    let from_mask = open(Format::DiskResident, dir.path(), DeleteGating::Immediate);
    assert_eq!(deleted(&from_log), deleted(&from_mask));
    assert_eq!(
        from_log.deleted_point_bitslice(),
        from_mask.deleted_point_bitslice(),
    );
    assert_eq!(
        from_log.available_point_count(),
        from_mask.available_point_count(),
    );
    assert_eq!(
        from_log.deleted_point_count(),
        from_mask.deleted_point_count(),
    );
    assert!(from_log.deleted_point_bitslice().len() > POINTS as usize);
}
