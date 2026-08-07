use common::types::DeferredBehavior;
use common::universal_io::{MmapFile, MmapFs};
use rand::SeedableRng as _;
use rand::rngs::StdRng;
use tempfile::Builder;

use super::UpdateOnlyDiskIdTracker;
use crate::id_tracker::IdTrackerRead;
use crate::id_tracker::compressed::compressed_point_mappings::CompressedPointMappings;
use crate::id_tracker::disk_id_tracker::{DiskIdTracker, ReadOnlyDiskIdTracker};
use crate::id_tracker::in_memory_id_tracker::InMemoryIdTracker;
use crate::types::{PointIdType, SeqNumberType};

fn make_data(seed: u64) -> (Vec<SeqNumberType>, CompressedPointMappings) {
    let mut rng = StdRng::seed_from_u64(seed);
    let in_memory = InMemoryIdTracker::random(&mut rng, 2_000, 1_700, 32);
    let (versions, mappings) = in_memory.into_internal();
    (versions, CompressedPointMappings::from_mappings(mappings))
}

/// Deletions applied through `UpdateOnlyDiskIdTracker` must be visible to
/// `DiskIdTracker`/`ReadOnlyDiskIdTracker` reading the same path, and must not
/// disturb points that were not deleted. Versions are left untouched.
#[test]
fn delete_batch_is_visible_to_disk_id_tracker_readers() {
    let (versions, mappings) = make_data(1);
    let dir = Builder::new().prefix("disk").tempdir().unwrap();
    let disk = DiskIdTracker::<MmapFile>::new(&MmapFs, dir.path(), &versions, mappings).unwrap();

    let live: Vec<(PointIdType, u32)> = disk.point_mappings().iter_from(None).collect();
    let (to_delete, to_keep) = live.split_at(50);
    let to_delete_offsets: Vec<u32> = to_delete.iter().map(|&(_, offset)| offset).collect();

    let mut update_only = UpdateOnlyDiskIdTracker::<MmapFile>::open(&MmapFs, dir.path()).unwrap();
    update_only.delete_batch(&to_delete_offsets).unwrap();

    let reopened_disk = DiskIdTracker::<MmapFile>::open(&MmapFs, dir.path()).unwrap();
    let read_only = ReadOnlyDiskIdTracker::<MmapFile>::open(&MmapFs, dir.path()).unwrap();

    for (external_id, offset) in to_delete {
        assert!(reopened_disk.is_deleted_point(*offset));
        assert_eq!(
            reopened_disk.internal_version(*offset),
            Some(versions[*offset as usize]),
            "delete_batch must not touch the version mapping",
        );
        assert_eq!(
            reopened_disk.internal_id_with_behavior(*external_id, DeferredBehavior::VisibleOnly),
            None,
        );

        assert!(read_only.is_deleted_point(*offset));
        assert_eq!(
            read_only.internal_id_with_behavior(*external_id, DeferredBehavior::VisibleOnly),
            None,
        );
    }

    // Untouched points must survive exactly as built.
    for (external_id, offset) in to_keep {
        assert!(!reopened_disk.is_deleted_point(*offset));
        assert_eq!(
            reopened_disk.internal_id_with_behavior(*external_id, DeferredBehavior::VisibleOnly),
            Some(*offset),
        );
        assert!(!read_only.is_deleted_point(*offset));
    }
}

#[test]
fn delete_batch_is_idempotent() {
    let (versions, mappings) = make_data(2);
    let dir = Builder::new().prefix("disk").tempdir().unwrap();
    let _disk = DiskIdTracker::<MmapFile>::new(&MmapFs, dir.path(), &versions, mappings).unwrap();

    let mut update_only = UpdateOnlyDiskIdTracker::<MmapFile>::open(&MmapFs, dir.path()).unwrap();
    update_only.delete_batch(&[3, 7, 7, 3]).unwrap();
    update_only.delete_batch(&[7]).unwrap();

    let read_only = ReadOnlyDiskIdTracker::<MmapFile>::open(&MmapFs, dir.path()).unwrap();
    assert!(read_only.is_deleted_point(3));
    assert!(read_only.is_deleted_point(7));
    assert_eq!(read_only.internal_version(3), Some(versions[3]));
    assert_eq!(read_only.internal_version(7), Some(versions[7]));
}

#[test]
fn empty_batch_is_a_noop() {
    let (versions, mappings) = make_data(3);
    let dir = Builder::new().prefix("disk").tempdir().unwrap();
    let _disk = DiskIdTracker::<MmapFile>::new(&MmapFs, dir.path(), &versions, mappings).unwrap();

    let before = fs_err::read(dir.path().join("id_tracker.deleted")).unwrap();

    let mut update_only = UpdateOnlyDiskIdTracker::<MmapFile>::open(&MmapFs, dir.path()).unwrap();
    update_only.delete_batch(&[]).unwrap();

    let after = fs_err::read(dir.path().join("id_tracker.deleted")).unwrap();
    assert_eq!(before, after, "empty batch must not touch the file");
}
