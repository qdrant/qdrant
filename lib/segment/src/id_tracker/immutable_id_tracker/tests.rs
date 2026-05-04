use std::collections::{HashMap, HashSet};
use std::path::Path;

use common::types::PointOffsetType;
use itertools::Itertools;
use rand::prelude::*;
use tempfile::Builder;
use uuid::Uuid;

use super::mappings_storage::{load_mapping, read_entry, store_mapping, write_entry};
use super::*;
use crate::id_tracker::compressed::compressed_point_mappings::CompressedPointMappings;
use crate::id_tracker::in_memory_id_tracker::InMemoryIdTracker;
use crate::id_tracker::{IdTracker, IdTrackerRead};
use crate::types::{PointIdType, SeqNumberType};

const RAND_SEED: u64 = 42;

#[test]
fn test_iterator() {
    let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();

    let mut id_tracker = InMemoryIdTracker::new();

    id_tracker.set_link(200.into(), 0).unwrap();
    id_tracker.set_link(100.into(), 1).unwrap();
    id_tracker.set_link(150.into(), 2).unwrap();
    id_tracker.set_link(120.into(), 3).unwrap();
    id_tracker.set_link(180.into(), 4).unwrap();
    id_tracker.set_link(110.into(), 5).unwrap();
    id_tracker.set_link(115.into(), 6).unwrap();
    id_tracker.set_link(190.into(), 7).unwrap();
    id_tracker.set_link(177.into(), 8).unwrap();
    id_tracker.set_link(118.into(), 9).unwrap();

    let id_tracker = ImmutableIdTracker::from_in_memory_tracker(id_tracker, dir.path()).unwrap();

    let first_four = id_tracker
        .point_mappings()
        .iter_from(None)
        .take(4)
        .collect_vec();

    assert_eq!(first_four.len(), 4);
    assert_eq!(first_four[0].0, 100.into());

    let last = id_tracker
        .point_mappings()
        .iter_from(Some(first_four[3].0))
        .collect_vec();
    assert_eq!(last.len(), 7);
}

pub const TEST_POINTS: &[PointIdType] = &[
    PointIdType::NumId(100),
    PointIdType::Uuid(Uuid::from_u128(123_u128)),
    PointIdType::Uuid(Uuid::from_u128(156_u128)),
    PointIdType::NumId(150),
    PointIdType::NumId(120),
    PointIdType::Uuid(Uuid::from_u128(12_u128)),
    PointIdType::NumId(180),
    PointIdType::NumId(110),
    PointIdType::NumId(115),
    PointIdType::Uuid(Uuid::from_u128(673_u128)),
    PointIdType::NumId(190),
    PointIdType::NumId(177),
    PointIdType::Uuid(Uuid::from_u128(971_u128)),
];

#[test]
fn test_mixed_types_iterator() {
    let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();
    let id_tracker = make_immutable_tracker(dir.path());

    let sorted_from_tracker = id_tracker
        .point_mappings()
        .iter_from(None)
        .map(|(k, _)| k)
        .collect_vec();

    let mut values = TEST_POINTS.to_vec();
    values.sort();

    assert_eq!(sorted_from_tracker, values);
}

#[test]
fn test_load_store() {
    let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();
    let (old_mappings, old_versions) = {
        let id_tracker = make_immutable_tracker(dir.path());
        (id_tracker.mappings, id_tracker.internal_to_version)
    };

    let mut loaded_id_tracker = ImmutableIdTracker::open(dir.path()).unwrap();

    // We may extend the length of deleted bitvec as memory maps need to be aligned to
    // a multiple of `usize-width`.
    assert_eq!(
        old_versions.len(),
        loaded_id_tracker.internal_to_version.len()
    );
    for i in 0..old_versions.len() as u32 {
        assert_eq!(
            old_versions.get(i),
            loaded_id_tracker.internal_to_version.get(i),
            "Version mismatch at index {i}",
        );
    }

    assert_eq!(old_mappings, loaded_id_tracker.mappings);

    loaded_id_tracker.drop(PointIdType::NumId(180)).unwrap();
}

/// Mutates an ID tracker and stores it to disk. Tests whether loading results in the exact same
/// ID tracker.
#[test]
fn test_store_load_mutated() {
    let mut rng = StdRng::seed_from_u64(RAND_SEED);

    let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();
    let (dropped_points, custom_version) = {
        let mut id_tracker = make_immutable_tracker(dir.path());

        let mut dropped_points = HashSet::new();
        let mut custom_version = HashMap::new();

        for (index, point) in TEST_POINTS.iter().enumerate() {
            if index % 2 == 0 {
                continue;
            }

            if index % 3 == 0 {
                id_tracker.drop(*point).unwrap();
                dropped_points.insert(*point);
                continue;
            }

            if index % 5 == 0 {
                let new_version = rng.next_u64();
                id_tracker
                    .set_internal_version(index as PointOffsetType, new_version)
                    .unwrap();
                custom_version.insert(index as PointOffsetType, new_version);
            }
        }

        id_tracker.mapping_flusher()().unwrap();
        id_tracker.versions_flusher()().unwrap();

        (dropped_points, custom_version)
    };

    let id_tracker = ImmutableIdTracker::open(dir.path()).unwrap();
    for (index, point) in TEST_POINTS.iter().enumerate() {
        let internal_id = index as PointOffsetType;

        if dropped_points.contains(point) {
            assert!(id_tracker.is_deleted_point(internal_id));
            assert_eq!(id_tracker.external_id(internal_id), None);
            assert!(id_tracker.mappings.internal_id(point).is_none());

            continue;
        }

        // Check version
        let expect_version = custom_version
            .get(&internal_id)
            .copied()
            .unwrap_or(DEFAULT_VERSION);

        assert_eq!(
            id_tracker.internal_to_version.get(internal_id),
            Some(expect_version)
        );

        // Check that unmodified points still haven't changed.
        assert_eq!(
            id_tracker.external_id(index as PointOffsetType),
            Some(*point)
        );
    }
}

#[test]
fn test_all_points_have_version() {
    let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();
    let id_tracker = make_immutable_tracker(dir.path());
    for i in id_tracker.point_mappings().iter_internal() {
        assert!(id_tracker.internal_version(i).is_some());
    }
}

#[test]
fn test_point_deletion_correctness() {
    let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();
    let mut id_tracker = make_immutable_tracker(dir.path());

    let deleted_points = id_tracker.total_point_count() - id_tracker.available_point_count();

    let point_to_delete = PointIdType::NumId(100);

    assert!(
        id_tracker
            .point_mappings()
            .iter_external()
            .contains(&point_to_delete)
    );

    assert_eq!(id_tracker.internal_id(point_to_delete), Some(0));

    id_tracker.drop(point_to_delete).unwrap();

    let point_exists = id_tracker.internal_id(point_to_delete).is_some()
        && id_tracker
            .point_mappings()
            .iter_external()
            .contains(&point_to_delete)
        && id_tracker
            .point_mappings()
            .iter_from(None)
            .any(|i| i.0 == point_to_delete);

    assert!(!point_exists);

    let new_deleted_points = id_tracker.total_point_count() - id_tracker.available_point_count();

    assert_eq!(new_deleted_points, deleted_points + 1);
}

#[test]
fn test_point_deletion_persists_reload() {
    let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();

    let point_to_delete = PointIdType::NumId(100);

    let old_mappings = {
        let mut id_tracker = make_immutable_tracker(dir.path());
        let intetrnal_id = id_tracker
            .internal_id(point_to_delete)
            .expect("Point to delete exists.");
        assert!(!id_tracker.is_deleted_point(intetrnal_id));
        id_tracker.drop(point_to_delete).unwrap();
        id_tracker.mapping_flusher()().unwrap();
        id_tracker.versions_flusher()().unwrap();
        id_tracker.mappings
    };

    // Point should still be gone
    let id_tracker = ImmutableIdTracker::open(dir.path()).unwrap();
    assert_eq!(id_tracker.internal_id(point_to_delete), None);

    old_mappings
        .iter_internal_raw()
        .zip(id_tracker.mappings.iter_internal_raw())
        .for_each(
            |((old_internal, old_external), (new_internal, new_external))| {
                assert_eq!(old_internal, new_internal);
                assert_eq!(old_external, new_external);
            },
        );
}

/// Tests de/serializing of whole `PointMappings`.
#[test]
fn test_point_mappings_de_serialization() {
    let mut rng = StdRng::seed_from_u64(RAND_SEED);

    let mut buf = vec![];

    // Test different sized PointMappings, growing exponentially to also test large ones.
    // This way we test up to 2^16 entries.
    for size_exp in (0..16u32).step_by(3) {
        buf.clear();

        let size = 2usize.pow(size_exp);

        let mappings = CompressedPointMappings::random(&mut rng, size as u32);

        store_mapping(&mappings, &mut buf).unwrap();

        // 16 is the min byte size of an entry. The exact number is not that important
        // we just want to ensure that the written bytes correlate to the amount of entries.
        assert!(buf.len() >= size * 16);

        let new_mappings = load_mapping(&*buf, None).unwrap();

        assert_eq!(new_mappings.total_point_count(), size);
        assert_eq!(mappings, new_mappings);
    }
}

/// Verifies that de/serializing works properly for empty `PointMappings`.
#[test]
fn test_point_mappings_de_serialization_empty() {
    let mut rng = StdRng::seed_from_u64(RAND_SEED);
    let mappings = CompressedPointMappings::random(&mut rng, 0);

    let mut buf = vec![];

    store_mapping(&mappings, &mut buf).unwrap();

    // We still have a header!
    assert!(!buf.is_empty());

    let new_mappings = load_mapping(&*buf, None).unwrap();

    assert_eq!(new_mappings.total_point_count(), 0);
    assert_eq!(mappings, new_mappings);
}

/// Tests de/serializing of only single ID mappings.
#[test]
fn test_point_mappings_de_serialization_single() {
    let mut rng = StdRng::seed_from_u64(RAND_SEED);

    const SIZE: usize = 400_000;

    let mappings = CompressedPointMappings::random(&mut rng, SIZE as u32);

    for i in 0..SIZE {
        let mut buf = vec![];

        let internal_id = i as PointOffsetType;

        let expected_external = mappings.external_id(internal_id).unwrap();

        write_entry(&mut buf, internal_id, expected_external).unwrap();

        let (got_internal, got_external) = read_entry(&*buf).unwrap();

        assert_eq!(i as PointOffsetType, got_internal);
        assert_eq!(expected_external, got_external);
    }
}

const DEFAULT_VERSION: SeqNumberType = 42;

fn make_in_memory_tracker_from_memory() -> InMemoryIdTracker {
    let mut id_tracker = InMemoryIdTracker::new();

    for value in TEST_POINTS.iter() {
        let internal_id = id_tracker.total_point_count() as PointOffsetType;
        id_tracker.set_link(*value, internal_id).unwrap();
        id_tracker
            .set_internal_version(internal_id, DEFAULT_VERSION)
            .unwrap()
    }

    id_tracker
}

fn make_immutable_tracker(path: &Path) -> ImmutableIdTracker {
    let id_tracker = make_in_memory_tracker_from_memory();
    ImmutableIdTracker::from_in_memory_tracker(id_tracker, path).unwrap()
}

#[test]
fn test_id_tracker_equal() {
    let in_memory_id_tracker = make_in_memory_tracker_from_memory();

    let immutable_id_tracker_dir = Builder::new()
        .prefix("storage_dir_immutable")
        .tempdir()
        .unwrap();
    let immutable_id_tracker = make_immutable_tracker(immutable_id_tracker_dir.path());

    assert_eq!(
        in_memory_id_tracker.available_point_count(),
        immutable_id_tracker.available_point_count()
    );
    assert_eq!(
        in_memory_id_tracker.total_point_count(),
        immutable_id_tracker.total_point_count()
    );

    for (internal, external) in TEST_POINTS.iter().enumerate() {
        let internal = internal as PointOffsetType;

        assert_eq!(
            in_memory_id_tracker.internal_id(*external),
            immutable_id_tracker.internal_id(*external)
        );

        assert_eq!(
            in_memory_id_tracker
                .internal_version(internal)
                .unwrap_or_default(),
            immutable_id_tracker
                .internal_version(internal)
                .unwrap_or_default()
        );

        assert_eq!(
            in_memory_id_tracker.external_id(internal),
            immutable_id_tracker.external_id(internal)
        );
    }
}
