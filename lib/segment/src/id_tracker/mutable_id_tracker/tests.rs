use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::{Cursor, Seek};
use std::path::Path;

use common::types::PointOffsetType;
use fs_err as fs;
use itertools::Itertools;
use rand::prelude::*;
use tempfile::Builder;
use uuid::Uuid;

use super::MutableIdTracker;
use super::change::{MappingChange, read_entry, write_entry};
use super::mappings_storage::{load_mappings, mappings_path, read_mappings};
use super::versions_storage::{
    VERSION_ELEMENT_SIZE, load_versions, store_version_changes, versions_path,
};
use crate::id_tracker::IdTracker;
use crate::id_tracker::compressed::compressed_point_mappings::CompressedPointMappings;
use crate::id_tracker::in_memory_id_tracker::InMemoryIdTracker;
use crate::types::{PointIdType, SeqNumberType};

const RAND_SEED: u64 = 42;
const DEFAULT_VERSION: SeqNumberType = 42;

#[test]
fn test_iterator() {
    let segment_dir = Builder::new().prefix("segment_dir").tempdir().unwrap();

    let mut id_tracker = MutableIdTracker::open(segment_dir.path()).unwrap();

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
    let segment_dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let id_tracker = make_mutable_tracker(segment_dir.path());

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
    let segment_dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let (old_mappings, old_versions) = {
        let id_tracker = make_mutable_tracker(segment_dir.path());
        (id_tracker.mappings, id_tracker.internal_to_version)
    };

    let mut loaded_id_tracker = MutableIdTracker::open(segment_dir.path()).unwrap();

    assert_eq!(
        old_versions.len(),
        loaded_id_tracker.internal_to_version.len(),
    );
    for i in 0..old_versions.len() {
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

    let segment_dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let (dropped_points, custom_version) = {
        let mut id_tracker = make_mutable_tracker(segment_dir.path());

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

    let id_tracker = MutableIdTracker::open(segment_dir.path()).unwrap();
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
            id_tracker.internal_version(internal_id),
            Some(expect_version),
        );

        // Check that unmodified points still haven't changed.
        assert_eq!(
            id_tracker.external_id(index as PointOffsetType),
            Some(*point),
        );
    }
}

#[test]
fn test_all_points_have_version() {
    let segment_dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let id_tracker = make_mutable_tracker(segment_dir.path());
    for i in id_tracker.point_mappings().iter_internal() {
        assert!(id_tracker.internal_version(i).is_some());
    }
}

#[test]
fn test_point_deletion_correctness() {
    let segment_dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let mut id_tracker = make_mutable_tracker(segment_dir.path());

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
    let segment_dir = Builder::new().prefix("segment_dir").tempdir().unwrap();

    let point_to_delete = PointIdType::NumId(100);

    let old_mappings = {
        let mut id_tracker = make_mutable_tracker(segment_dir.path());
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
    let id_tracker = MutableIdTracker::open(segment_dir.path()).unwrap();
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

        let change = MappingChange::Insert(expected_external, internal_id);

        write_entry(&mut buf, change).unwrap();

        let (got_change, bytes_read) = read_entry(&mut buf.as_slice()).unwrap();

        assert_eq!(change, got_change);
        assert!(bytes_read == 13 || bytes_read == 21);
    }
}

/// Some more special test cases for deserializing point mappings.
#[test]
fn test_point_mappings_deserializing_special() {
    // Empty reader creates empty mappings
    let buf = Cursor::new(b"");
    assert_eq!(read_mappings(buf).unwrap().total_point_count(), 0);

    // Corrupt if reading invalid type byte
    let buf = Cursor::new(b"\x00");
    assert!(
        read_mappings(buf)
            .unwrap_err()
            .to_string()
            .contains("Corrupted ID tracker mapping storage")
    );

    let buf = Cursor::new(b"malformed!");
    assert!(read_mappings(buf).is_err());

    // Empty if change is not fully written
    let buf = Cursor::new(b"\x01\x01\x00\x00\x00\x00");
    assert_eq!(read_mappings(buf).unwrap().total_point_count(), 0);

    // Exactly one entry
    let buf = Cursor::new(b"\x01\x01\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00");
    assert_eq!(
        read_mappings(buf)
            .unwrap()
            .internal_id(&PointIdType::NumId(1)),
        Some(2)
    );

    // Exactly one entry and a malformed second one
    let buf = Cursor::new(b"\x01\x01\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00\x00\x01\x00");
    assert!(
        read_mappings(buf)
            .unwrap_err()
            .to_string()
            .contains("Corrupted ID tracker mapping storage")
    );

    // Exactly one entry and an incomplete second one
    let buf = Cursor::new(b"\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x03\x01\x00");
    let mappings = read_mappings(buf).unwrap();
    assert_eq!(mappings.total_point_count(), 1);
    assert_eq!(mappings.internal_id(&PointIdType::NumId(1)), Some(0));
}

/// Test that `operation_size` returns the correct size
///
/// Must have read exactly the same number of bytes from the stream as we return.
#[test]
fn test_operation_size() {
    let items = [
        (
            b"\x01\x01\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00".as_slice(),
            MappingChange::Insert(PointIdType::NumId(1), 2),
        ),
        (
            b"\x02\x10\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00"
                .as_slice(),
            MappingChange::Insert(
                PointIdType::Uuid(Uuid::parse_str("10000000-0000-0000-0000-000000000000").unwrap()),
                2,
            ),
        ),
        (
            b"\x03\x01\x00\x00\x00\x00\x00\x00\x00".as_slice(),
            MappingChange::Delete(PointIdType::NumId(1)),
        ),
        (
            b"\x04\x10\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00".as_slice(),
            MappingChange::Delete(PointIdType::Uuid(
                Uuid::parse_str("10000000-0000-0000-0000-000000000000").unwrap(),
            )),
        ),
    ];

    // Test each change type
    for (buf, expected_change) in items {
        let mut buf = Cursor::new(buf);
        let (entry, bytes_read) = read_entry(&mut buf).unwrap();
        assert_eq!(entry, expected_change);
        assert_eq!(
            bytes_read,
            buf.stream_position().unwrap(),
            "read different number of bytes from stream than returned",
        );
    }
}

/// Test that we truncate a partially written entry at the end.
#[test]
fn test_point_mappings_truncation() {
    let segment_dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let mappings_path = mappings_path(segment_dir.path());

    // Exactly one entry
    fs::write(
        &mappings_path,
        b"\x01\x01\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00",
    )
    .unwrap();
    assert_eq!(fs::metadata(&mappings_path).unwrap().len(), 13);
    assert_eq!(
        load_mappings(&mappings_path)
            .unwrap()
            .0
            .internal_id(&PointIdType::NumId(1)),
        Some(2)
    );
    assert_eq!(fs::metadata(&mappings_path).unwrap().len(), 13);

    // One entry and and one extra byte, file must be truncated
    fs::write(
        &mappings_path,
        b"\x01\x01\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00\x01",
    )
    .unwrap();
    assert_eq!(fs::metadata(&mappings_path).unwrap().len(), 14);
    assert_eq!(
        load_mappings(&mappings_path)
            .unwrap()
            .0
            .internal_id(&PointIdType::NumId(1)),
        Some(2)
    );
    assert_eq!(fs::metadata(&mappings_path).unwrap().len(), 13);

    // One entry and an incomplete second one, file must be truncated
    fs::write(
        &mappings_path,
        b"\x01\x01\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00\x01\x02\x00",
    )
    .unwrap();
    assert_eq!(fs::metadata(&mappings_path).unwrap().len(), 16);
    assert_eq!(
        load_mappings(&mappings_path)
            .unwrap()
            .0
            .internal_id(&PointIdType::NumId(1)),
        Some(2)
    );
    assert_eq!(fs::metadata(&mappings_path).unwrap().len(), 13);

    // Two entries and an incomplete third one, file must be truncated
    fs::write(
        &mappings_path,
        b"\x01\x01\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00\x01\x02\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x02\x00",
    ).unwrap();
    assert_eq!(fs::metadata(&mappings_path).unwrap().len(), 28);
    assert_eq!(
        load_mappings(&mappings_path)
            .unwrap()
            .0
            .internal_id(&PointIdType::NumId(1)),
        Some(2)
    );
    assert_eq!(fs::metadata(&mappings_path).unwrap().len(), 26);
}

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

fn make_mutable_tracker(path: &Path) -> MutableIdTracker {
    let mut id_tracker = MutableIdTracker::open(path).expect("failed to open mutable ID tracker");

    for value in TEST_POINTS.iter() {
        let internal_id = id_tracker.total_point_count() as PointOffsetType;
        id_tracker.set_link(*value, internal_id).unwrap();
        id_tracker
            .set_internal_version(internal_id, DEFAULT_VERSION)
            .unwrap()
    }

    id_tracker.mapping_flusher()().expect("failed to flush ID tracker mappings");
    id_tracker.versions_flusher()().expect("failed to flush ID tracker versions");

    id_tracker
}

#[test]
fn test_id_tracker_equal() {
    let in_memory_id_tracker = make_in_memory_tracker_from_memory();

    let mutable_id_tracker_dir = Builder::new()
        .prefix("segment_dir_mutable")
        .tempdir()
        .unwrap();
    let mutable_id_tracker = make_mutable_tracker(mutable_id_tracker_dir.path());

    assert_eq!(
        in_memory_id_tracker.available_point_count(),
        mutable_id_tracker.available_point_count(),
    );
    assert_eq!(
        in_memory_id_tracker.total_point_count(),
        mutable_id_tracker.total_point_count(),
    );

    for (internal, external) in TEST_POINTS.iter().enumerate() {
        let internal = internal as PointOffsetType;

        assert_eq!(
            in_memory_id_tracker.internal_id(*external),
            mutable_id_tracker.internal_id(*external),
        );

        assert_eq!(
            in_memory_id_tracker
                .internal_version(internal)
                .unwrap_or_default(),
            mutable_id_tracker
                .internal_version(internal)
                .unwrap_or_default(),
        );

        assert_eq!(
            in_memory_id_tracker.external_id(internal),
            mutable_id_tracker.external_id(internal),
        );
    }
}

#[test]
// TODO(rocksdb): fix and re-enable
// https://github.com/qdrant/qdrant/pull/8529#discussion_r3014389245
#[cfg(false)]
fn simple_id_tracker_vs_mutable_tracker_congruence() {
    use crate::common::rocksdb_wrapper::{DB_VECTOR_CF, open_db};

    let segment_dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let db = open_db(segment_dir.path(), &[DB_VECTOR_CF]).unwrap();

    let mut mutable_id_tracker = MutableIdTracker::open(segment_dir.path()).unwrap();
    let mut simple_id_tracker = SimpleIdTracker::open(db).unwrap();

    // Insert 100 random points into id_tracker

    let num_points = 200;
    let mut rng = StdRng::seed_from_u64(RAND_SEED);

    for _ in 0..num_points {
        // Generate num id in range from 0 to 100

        let point_id = PointIdType::NumId(rng.random_range(0..num_points as u64));

        let version = rng.random_range(0..1000);

        let internal_id_mmap = mutable_id_tracker.total_point_count() as PointOffsetType;
        let internal_id_simple = simple_id_tracker.total_point_count() as PointOffsetType;

        assert_eq!(internal_id_mmap, internal_id_simple);

        if mutable_id_tracker.internal_id(point_id).is_some() {
            mutable_id_tracker.drop(point_id).unwrap();
        }
        mutable_id_tracker
            .set_link(point_id, internal_id_mmap)
            .unwrap();
        mutable_id_tracker
            .set_internal_version(internal_id_mmap, version)
            .unwrap();

        if simple_id_tracker.internal_id(point_id).is_some() {
            simple_id_tracker.drop(point_id).unwrap();
        }
        simple_id_tracker
            .set_link(point_id, internal_id_simple)
            .unwrap();
        simple_id_tracker
            .set_internal_version(internal_id_simple, version)
            .unwrap();
    }

    fn check_trackers(a: &SimpleIdTracker, b: &MutableIdTracker) {
        for (external_id, internal_id) in a.point_mappings().iter_from(None) {
            assert_eq!(
                a.internal_version(internal_id).unwrap(),
                b.internal_version(internal_id).unwrap()
            );
            assert_eq!(a.external_id(internal_id), b.external_id(internal_id));
            assert_eq!(external_id, b.external_id(internal_id).unwrap());
            assert_eq!(
                a.external_id(internal_id).unwrap(),
                b.external_id(internal_id).unwrap()
            );
        }

        for (external_id, internal_id) in b.point_mappings().iter_from(None) {
            assert_eq!(
                a.internal_version(internal_id).unwrap(),
                b.internal_version(internal_id).unwrap()
            );
            assert_eq!(a.external_id(internal_id), b.external_id(internal_id));
            assert_eq!(external_id, a.external_id(internal_id).unwrap());
            assert_eq!(
                a.external_id(internal_id).unwrap(),
                b.external_id(internal_id).unwrap()
            );
        }
    }

    check_trackers(&simple_id_tracker, &mutable_id_tracker);

    // Persist and reload mutable tracker and test again
    mutable_id_tracker.mapping_flusher()().unwrap();
    mutable_id_tracker.versions_flusher()().unwrap();
    drop(mutable_id_tracker);
    let mutable_id_tracker = MutableIdTracker::open(segment_dir.path()).unwrap();

    check_trackers(&simple_id_tracker, &mutable_id_tracker);
}

/// Loading versions with a partial trailing entry should ignore the incomplete bytes
/// without modifying the file (read-only safe).
#[test]
fn test_load_versions_partial_tail_ignored() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let path = versions_path(dir.path());

    // Write 3 complete u64 entries (24 bytes) + 5 garbage bytes = 29 bytes
    let mut data = Vec::new();
    for v in [10u64, 20, 30] {
        data.extend_from_slice(&v.to_le_bytes());
    }
    data.extend_from_slice(&[0xFF; 5]);
    fs::write(&path, &data).unwrap();
    assert_eq!(fs::metadata(&path).unwrap().len(), 29);

    let versions = load_versions(&path).unwrap();
    assert_eq!(versions, vec![10, 20, 30]);

    // File must NOT be truncated by load (read-only operation)
    assert_eq!(fs::metadata(&path).unwrap().len(), 29);
}

/// Writing versions should truncate a partial trailing entry left from a previous crash,
/// then correctly write the new versions.
#[test]
fn test_store_versions_truncates_partial_tail() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let path = versions_path(dir.path());

    // Write 2 complete entries + 3 garbage bytes
    let mut data = Vec::new();
    for v in [10u64, 20] {
        data.extend_from_slice(&v.to_le_bytes());
    }
    data.extend_from_slice(&[0xFF; 3]);
    fs::write(&path, &data).unwrap();
    assert_eq!(fs::metadata(&path).unwrap().len(), 19);

    // Store a version change for internal_id=1
    let mut changes = BTreeMap::new();
    changes.insert(1u32, 99u64);
    store_version_changes(&path, &changes).unwrap();

    // File should be aligned and contain correct data
    let file_len = fs::metadata(&path).unwrap().len();
    assert_eq!(file_len % VERSION_ELEMENT_SIZE, 0);

    let versions = load_versions(&path).unwrap();
    assert_eq!(versions.len(), 2);
    assert_eq!(versions[0], 10); // untouched
    assert_eq!(versions[1], 99); // updated
}

/// Writing versions past the current file end should extend the file via seek+write,
/// with zero-filled gaps (version 0 = DELETED_POINT_VERSION).
#[test]
fn test_store_versions_extends_without_set_len() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let path = versions_path(dir.path());

    // Write 2 initial entries
    let mut changes = BTreeMap::new();
    changes.insert(0u32, 100u64);
    changes.insert(1u32, 200u64);
    store_version_changes(&path, &changes).unwrap();

    let versions = load_versions(&path).unwrap();
    assert_eq!(versions, vec![100, 200]);

    // Now write at internal_id=5, leaving a gap at 2,3,4
    let mut changes = BTreeMap::new();
    changes.insert(5u32, 500u64);
    store_version_changes(&path, &changes).unwrap();

    let versions = load_versions(&path).unwrap();
    assert_eq!(versions.len(), 6);
    assert_eq!(versions[0], 100);
    assert_eq!(versions[1], 200);
    // Gap entries should be zero-filled (DELETED_POINT_VERSION)
    assert_eq!(versions[2], 0);
    assert_eq!(versions[3], 0);
    assert_eq!(versions[4], 0);
    assert_eq!(versions[5], 500);
}
