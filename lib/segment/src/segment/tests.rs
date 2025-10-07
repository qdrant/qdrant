use std::sync::atomic::AtomicBool;

use common::counter::hardware_counter::HardwareCounterCell;
use common::tar_ext;
use fs_err as fs;
use fs_err::File;
use rstest::rstest;
use tempfile::Builder;

use super::*;
use crate::common::operation_error::OperationError::PointIdError;
use crate::common::{check_named_vectors, check_vector, check_vector_name};
use crate::data_types::named_vectors::NamedVectors;
use crate::data_types::query_context::QueryContext;
use crate::data_types::vectors::{DEFAULT_VECTOR_NAME, only_default_vector};
use crate::entry::SnapshotEntry as _;
use crate::entry::entry_point::SegmentEntry;
use crate::segment_constructor::load_segment;
use crate::segment_constructor::simple_segment_constructor::{
    VECTOR1_NAME, VECTOR2_NAME, build_multivec_segment, build_simple_segment,
};
use crate::types::{Distance, Filter, Payload, SnapshotFormat, WithPayload, WithVector};

#[test]
fn test_search_batch_equivalence_single() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let dim = 4;

    let mut segment = build_simple_segment(dir.path(), dim, Distance::Dot).unwrap();

    let hw_counter = HardwareCounterCell::new();

    let vec4 = vec![1.1, 1.0, 0.0, 1.0];
    segment
        .upsert_point(100, 4.into(), only_default_vector(&vec4), &hw_counter)
        .unwrap();
    let vec6 = vec![1.0, 1.0, 0.5, 1.0];
    segment
        .upsert_point(101, 6.into(), only_default_vector(&vec6), &hw_counter)
        .unwrap();
    segment.delete_point(102, 1.into(), &hw_counter).unwrap();

    let query_vector = [1.0, 1.0, 1.0, 1.0].into();
    let search_result = segment
        .search(
            DEFAULT_VECTOR_NAME,
            &query_vector,
            &WithPayload::default(),
            &false.into(),
            None,
            10,
            None,
        )
        .unwrap();
    eprintln!("search_result = {search_result:#?}");

    let query_context = QueryContext::default();
    let segment_query_context = query_context.get_segment_query_context();

    let search_batch_result = segment
        .search_batch(
            DEFAULT_VECTOR_NAME,
            &[&query_vector],
            &WithPayload::default(),
            &false.into(),
            None,
            10,
            None,
            &segment_query_context,
        )
        .unwrap();
    eprintln!("search_batch_result = {search_batch_result:#?}");

    assert!(!search_result.is_empty());
    assert_eq!(search_result, search_batch_result[0].clone());
}

#[test]
fn test_from_filter_attributes() {
    let data = r#"
        {
            "name": "John Doe",
            "age": 43,
            "metadata": {
                "height": 50,
                "width": 60
            }
        }"#;

    let dir = Builder::new().prefix("payload_dir").tempdir().unwrap();
    let dim = 2;

    let hw_counter = HardwareCounterCell::new();

    let mut segment = build_simple_segment(dir.path(), dim, Distance::Dot).unwrap();
    segment
        .upsert_point(0, 0.into(), only_default_vector(&[1.0, 1.0]), &hw_counter)
        .unwrap();

    let payload: Payload = serde_json::from_str(data).unwrap();

    segment
        .set_full_payload(0, 0.into(), &payload, &hw_counter)
        .unwrap();

    let filter_valid_str = r#"
        {
            "must": [
                {
                    "key": "metadata.height",
                    "match": {
                        "value": 50
                    }
                }
            ]
        }"#;

    let filter_valid: Filter = serde_json::from_str(filter_valid_str).unwrap();
    let filter_invalid_str = r#"
        {
            "must": [
                {
                    "key": "metadata.height",
                    "match": {
                        "value": 60
                    }
                }
            ]
        }"#;

    let filter_invalid: Filter = serde_json::from_str(filter_invalid_str).unwrap();
    let results_with_valid_filter = segment
        .search(
            DEFAULT_VECTOR_NAME,
            &[1.0, 1.0].into(),
            &WithPayload::default(),
            &false.into(),
            Some(&filter_valid),
            1,
            None,
        )
        .unwrap();
    assert_eq!(results_with_valid_filter.len(), 1);
    assert_eq!(results_with_valid_filter.first().unwrap().id, 0.into());
    let results_with_invalid_filter = segment
        .search(
            DEFAULT_VECTOR_NAME,
            &[1.0, 1.0].into(),
            &WithPayload::default(),
            &false.into(),
            Some(&filter_invalid),
            1,
            None,
        )
        .unwrap();
    assert!(results_with_invalid_filter.is_empty());
}

#[rstest]
#[case::regular(SnapshotFormat::Regular)]
#[case::streamable(SnapshotFormat::Streamable)]
fn test_snapshot(#[case] format: SnapshotFormat) {
    let _ = env_logger::builder().is_test(true).try_init();

    let data = r#"
        {
            "name": "John Doe",
            "age": 43,
            "metadata": {
                "height": 50,
                "width": 60
            }
        }"#;

    let segment_base_dir = Builder::new().prefix("segment_dir").tempdir().unwrap();

    let hw_counter = HardwareCounterCell::new();

    let mut segment = build_simple_segment(segment_base_dir.path(), 2, Distance::Dot).unwrap();

    segment
        .upsert_point(0, 0.into(), only_default_vector(&[1.0, 1.0]), &hw_counter)
        .unwrap();

    segment
        .set_full_payload(
            1,
            0.into(),
            &serde_json::from_str(data).unwrap(),
            &hw_counter,
        )
        .unwrap();

    let temp_dir = Builder::new().prefix("temp_dir").tempdir().unwrap();
    // The segment snapshot is a part of a parent collection/shard snapshot.
    let parent_snapshot_tar = Builder::new()
        .prefix("parent_snapshot")
        .suffix(".tar")
        .tempfile()
        .unwrap();
    let segment_id = segment
        .current_path
        .file_stem()
        .and_then(|f| f.to_str())
        .unwrap();

    // snapshotting!
    let tar =
        tar_ext::BuilderExt::new_seekable_owned(File::create(parent_snapshot_tar.path()).unwrap());
    segment
        .take_snapshot(temp_dir.path(), &tar, format, None)
        .unwrap();
    tar.blocking_finish().unwrap();

    let parent_snapshot_unpacked = Builder::new().prefix("parent_snapshot").tempdir().unwrap();
    tar::Archive::new(File::open(parent_snapshot_tar.path()).unwrap())
        .unpack(parent_snapshot_unpacked.path())
        .unwrap();

    // Should be exactly one entry in the snapshot.
    let mut entries = fs::read_dir(parent_snapshot_unpacked.path()).unwrap();
    let entry = entries.next().unwrap().unwrap();
    assert!(entries.next().is_none());

    match format {
        SnapshotFormat::Ancient => unreachable!("The old days are gone"),
        SnapshotFormat::Regular => {
            assert_eq!(entry.file_name(), format!("{segment_id}.tar").as_str());
            assert!(entry.path().is_file());
        }
        SnapshotFormat::Streamable => {
            assert_eq!(entry.file_name(), segment_id);
            assert!(entry.path().is_dir());
        }
    }

    // restore snapshot
    Segment::restore_snapshot_in_place(&entry.path()).unwrap();

    // Should be exactly one entry in the snapshot.
    let mut entries = fs::read_dir(parent_snapshot_unpacked.path()).unwrap();
    let entry = entries.next().unwrap().unwrap();
    assert!(entries.next().is_none());

    // It should be unpacked entry, not tar archive.
    assert!(entry.path().is_dir());
    assert_eq!(entry.file_name(), segment_id);

    let restored_segment = load_segment(&entry.path(), &AtomicBool::new(false))
        .unwrap()
        .unwrap();

    // validate restored snapshot is the same as original segment
    assert_eq!(
        segment.total_point_count(),
        restored_segment.total_point_count(),
    );
    assert_eq!(
        segment.available_point_count(),
        restored_segment.available_point_count(),
    );
    assert_eq!(
        segment.deleted_point_count(),
        restored_segment.deleted_point_count(),
    );

    for id in segment.iter_points() {
        let vectors = segment.all_vectors(id, &hw_counter).unwrap();
        let restored_vectors = restored_segment.all_vectors(id, &hw_counter).unwrap();
        assert_eq!(vectors, restored_vectors);

        let payload = segment.payload(id, &hw_counter).unwrap();
        let restored_payload = restored_segment.payload(id, &hw_counter).unwrap();
        assert_eq!(payload, restored_payload);
    }
}

#[test]
fn test_background_flush() {
    let data = r#"
        {
            "name": "John Doe",
            "age": 43,
            "metadata": {
                "height": 50,
                "width": 60
            }
        }"#;

    let segment_base_dir = Builder::new().prefix("segment_dir").tempdir().unwrap();

    let hw_counter = HardwareCounterCell::new();

    let mut segment = build_simple_segment(segment_base_dir.path(), 2, Distance::Dot).unwrap();
    segment
        .upsert_point(0, 0.into(), only_default_vector(&[1.0, 1.0]), &hw_counter)
        .unwrap();

    let payload: Payload = serde_json::from_str(data).unwrap();
    segment
        .set_full_payload(0, 0.into(), &payload, &hw_counter)
        .unwrap();
    segment.flush(false, false).unwrap();

    // call flush second time to check that background flush finished successful
    segment.flush(true, false).unwrap();
}

#[test]
fn test_check_consistency() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let dim = 4;

    let mut segment = build_simple_segment(dir.path(), dim, Distance::Dot).unwrap();

    let hw_counter = HardwareCounterCell::new();

    let vec4 = vec![1.1, 1.0, 0.0, 1.0];
    segment
        .upsert_point(100, 4.into(), only_default_vector(&vec4), &hw_counter)
        .unwrap();
    let vec6 = vec![1.0, 1.0, 0.5, 1.0];
    segment
        .upsert_point(101, 6.into(), only_default_vector(&vec6), &hw_counter)
        .unwrap();

    // first pass on consistent data
    segment.check_consistency_and_repair().unwrap();

    let query_vector = [1.0, 1.0, 1.0, 1.0].into();
    let search_result = segment
        .search(
            DEFAULT_VECTOR_NAME,
            &query_vector,
            &WithPayload::default(),
            &false.into(),
            None,
            10,
            None,
        )
        .unwrap();

    assert_eq!(search_result.len(), 2);
    assert_eq!(search_result[0].id, 6.into());
    assert_eq!(search_result[1].id, 4.into());

    assert!(
        segment
            .vector(DEFAULT_VECTOR_NAME, 6.into(), &hw_counter)
            .is_ok()
    );

    let internal_id = segment.lookup_internal_id(6.into()).unwrap();

    // make id_tracker inconsistent
    segment.id_tracker.borrow_mut().drop(6.into()).unwrap();

    let search_result = segment
        .search(
            DEFAULT_VECTOR_NAME,
            &query_vector,
            &WithPayload::default(),
            &false.into(),
            None,
            10,
            None,
        )
        .unwrap();

    // only one result because of inconsistent id_tracker
    assert_eq!(search_result.len(), 1);
    assert_eq!(search_result[0].id, 4.into());

    // querying by external id is broken
    assert!(
        matches!(segment.vector(DEFAULT_VECTOR_NAME, 6.into(), &hw_counter), Err(PointIdError {missed_point_id }) if missed_point_id == 6.into())
    );

    // but querying by internal id still works
    matches!(
        segment.vector_by_offset(DEFAULT_VECTOR_NAME, internal_id, &hw_counter),
        Ok(Some(_))
    );

    // fix segment's data
    segment.check_consistency_and_repair().unwrap();

    // querying by internal id now consistent
    matches!(
        segment.vector_by_offset(DEFAULT_VECTOR_NAME, internal_id, &hw_counter),
        Ok(None)
    );
}

#[test]
fn test_point_vector_count() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let dim = 1;

    let mut segment = build_simple_segment(dir.path(), dim, Distance::Dot).unwrap();

    let hw_counter = HardwareCounterCell::new();

    // Insert point ID 4 and 6, assert counts
    segment
        .upsert_point(100, 4.into(), only_default_vector(&[0.4]), &hw_counter)
        .unwrap();
    segment
        .upsert_point(101, 6.into(), only_default_vector(&[0.6]), &hw_counter)
        .unwrap();
    let segment_info = segment.info();
    assert_eq!(segment_info.num_points, 2);
    assert_eq!(segment_info.num_vectors, 2);

    // Delete nonexistent point, counts should remain the same
    segment.delete_point(102, 1.into(), &hw_counter).unwrap();
    let segment_info = segment.info();
    assert_eq!(segment_info.num_points, 2);
    assert_eq!(segment_info.num_vectors, 2);

    // Delete point 4, counts should decrease by 1
    segment.delete_point(103, 4.into(), &hw_counter).unwrap();
    let segment_info = segment.info();
    assert_eq!(segment_info.num_points, 1);
    assert_eq!(segment_info.num_vectors, 2); // We don't propagate deletes to vectors at this time

    // // Delete vector of point 6, vector count should now be zero
    // segment
    //     .delete_vector(104, 6.into(), DEFAULT_VECTOR_NAME)
    //     .unwrap();
    // let segment_info = segment.info();
    // assert_eq!(segment_info.num_points, 1);
    // assert_eq!(segment_info.num_vectors, 1);
}

#[test]
fn test_point_vector_count_multivec() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let dim = 1;

    let mut segment = build_multivec_segment(dir.path(), dim, dim, Distance::Dot).unwrap();

    let hw_counter = HardwareCounterCell::new();

    // Insert point ID 4 and 6 fully, 8 and 10 partially, assert counts
    segment
        .upsert_point(
            100,
            4.into(),
            NamedVectors::from_pairs([
                (VECTOR1_NAME.into(), vec![0.4]),
                (VECTOR2_NAME.into(), vec![0.5]),
            ]),
            &hw_counter,
        )
        .unwrap();
    segment
        .upsert_point(
            101,
            6.into(),
            NamedVectors::from_pairs([
                (VECTOR1_NAME.into(), vec![0.6]),
                (VECTOR2_NAME.into(), vec![0.7]),
            ]),
            &hw_counter,
        )
        .unwrap();
    segment
        .upsert_point(
            102,
            8.into(),
            NamedVectors::from_pairs([(VECTOR1_NAME.into(), vec![0.0])]),
            &hw_counter,
        )
        .unwrap();
    segment
        .upsert_point(
            103,
            10.into(),
            NamedVectors::from_pairs([(VECTOR2_NAME.into(), vec![1.0])]),
            &hw_counter,
        )
        .unwrap();
    let segment_info = segment.info();
    assert_eq!(segment_info.num_points, 4);
    assert_eq!(segment_info.num_vectors, 6);

    // Delete nonexistent point, counts should remain the same
    segment.delete_point(104, 1.into(), &hw_counter).unwrap();
    let segment_info = segment.info();
    assert_eq!(segment_info.num_points, 4);
    assert_eq!(segment_info.num_vectors, 6);

    // Delete point 4, counts should decrease by 1
    segment.delete_point(105, 4.into(), &hw_counter).unwrap();
    let segment_info = segment.info();
    assert_eq!(segment_info.num_points, 3);
    assert_eq!(segment_info.num_vectors, 6); // We don't propagate deletes to vectors at this time

    // Delete vector 'a' of point 6, vector count should decrease by 1
    segment.delete_vector(106, 6.into(), VECTOR1_NAME).unwrap();
    let segment_info = segment.info();
    assert_eq!(segment_info.num_points, 3);
    assert_eq!(segment_info.num_vectors, 5);

    // Deleting it again shouldn't chain anything
    segment.delete_vector(107, 6.into(), VECTOR1_NAME).unwrap();
    let segment_info = segment.info();
    assert_eq!(segment_info.num_points, 3);
    assert_eq!(segment_info.num_vectors, 5);

    // Replace vector 'a' for point 8, counts should remain the same
    let internal_8 = segment.lookup_internal_id(8.into()).unwrap();
    segment
        .replace_all_vectors(
            internal_8,
            0,
            &NamedVectors::from_pairs([(VECTOR1_NAME.into(), vec![0.1])]),
            &hw_counter,
        )
        .unwrap();
    let segment_info = segment.info();
    assert_eq!(segment_info.num_points, 3);
    assert_eq!(segment_info.num_vectors, 5);

    // Replace both vectors for point 8, adding a new vector
    segment
        .replace_all_vectors(
            internal_8,
            0,
            &NamedVectors::from_pairs([
                (VECTOR1_NAME.into(), vec![0.1]),
                (VECTOR2_NAME.into(), vec![0.1]),
            ]),
            &hw_counter,
        )
        .unwrap();
    let segment_info = segment.info();
    assert_eq!(segment_info.num_points, 3);
    assert_eq!(segment_info.num_vectors, 6);
}

/// Tests segment functions to ensure invalid requests do error
#[test]
fn test_vector_compatibility_checks() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();

    let mut segment = build_multivec_segment(dir.path(), 4, 2, Distance::Dot).unwrap();

    let hw_counter = HardwareCounterCell::new();

    // Insert one point for a reference internal ID
    let point_id = 4.into();
    segment
        .upsert_point(
            100,
            point_id,
            NamedVectors::from_pairs([
                (VECTOR1_NAME.into(), vec![0.1, 0.2, 0.3, 0.4]),
                (VECTOR2_NAME.into(), vec![1.0, 0.9]),
            ]),
            &hw_counter,
        )
        .unwrap();
    let internal_id = segment.lookup_internal_id(point_id).unwrap();

    // A set of broken vectors
    let wrong_vectors_single = vec![
        // Incorrect dimensionality
        (VECTOR1_NAME, vec![]),
        (VECTOR1_NAME, vec![0.0, 1.0, 0.0]),
        (VECTOR1_NAME, vec![0.0, 1.0, 0.0, 1.0, 0.0]),
        (VECTOR2_NAME, vec![]),
        (VECTOR2_NAME, vec![0.5]),
        (VECTOR2_NAME, vec![0.0, 0.1, 0.2, 0.3]),
        // Incorrect names
        ("aa", vec![0.0, 0.1, 0.2, 0.3]),
        ("bb", vec![0.0, 0.1]),
    ];
    let wrong_vectors_multi = vec![
        // Incorrect dimensionality
        NamedVectors::from_ref(VECTOR1_NAME, [].as_slice().into()),
        NamedVectors::from_ref(VECTOR1_NAME, [0.0, 1.0, 0.0].as_slice().into()),
        NamedVectors::from_ref(VECTOR1_NAME, [0.0, 1.0, 0.0, 1.0, 0.0].as_slice().into()),
        NamedVectors::from_ref(VECTOR2_NAME, [].as_slice().into()),
        NamedVectors::from_ref(VECTOR2_NAME, [0.5].as_slice().into()),
        NamedVectors::from_ref(VECTOR2_NAME, [0.0, 0.1, 0.2, 0.3].as_slice().into()),
        NamedVectors::from_pairs([
            (VECTOR1_NAME.into(), vec![0.1, 0.2, 0.3]),
            (VECTOR2_NAME.into(), vec![1.0, 0.9]),
        ]),
        NamedVectors::from_pairs([
            (VECTOR1_NAME.into(), vec![0.1, 0.2, 0.3, 0.4]),
            (VECTOR2_NAME.into(), vec![1.0, 0.9, 0.0]),
        ]),
        // Incorrect names
        NamedVectors::from_ref("aa", [0.0, 0.1, 0.2, 0.3].as_slice().into()),
        NamedVectors::from_ref("bb", [0.0, 0.1].as_slice().into()),
        NamedVectors::from_pairs([
            ("aa".into(), vec![0.1, 0.2, 0.3, 0.4]),
            (VECTOR2_NAME.into(), vec![1.0, 0.9]),
        ]),
        NamedVectors::from_pairs([
            (VECTOR1_NAME.into(), vec![0.1, 0.2, 0.3, 0.4]),
            ("bb".into(), vec![1.0, 0.9]),
        ]),
    ];
    let wrong_names = vec!["aa", "bb", ""];

    for (vector_name, vector) in wrong_vectors_single.iter() {
        let query_vector = vector.to_owned().into();
        check_vector(vector_name, &query_vector, &segment.segment_config)
            .err()
            .unwrap();
        segment
            .search(
                vector_name,
                &query_vector,
                &WithPayload {
                    enable: false,
                    payload_selector: None,
                },
                &WithVector::Bool(true),
                None,
                1,
                None,
            )
            .err()
            .unwrap();
        let query_context = QueryContext::default();
        let segment_query_context = query_context.get_segment_query_context();
        segment
            .search_batch(
                vector_name,
                &[&query_vector, &query_vector],
                &WithPayload {
                    enable: false,
                    payload_selector: None,
                },
                &WithVector::Bool(true),
                None,
                1,
                None,
                &segment_query_context,
            )
            .err()
            .unwrap();
    }

    for vectors in wrong_vectors_multi {
        check_named_vectors(&vectors, &segment.segment_config)
            .err()
            .unwrap();
        segment
            .upsert_point(101, point_id, vectors.clone(), &hw_counter)
            .err()
            .unwrap();
        segment
            .update_vectors(internal_id, 0, vectors.clone(), &hw_counter)
            .err()
            .unwrap();
        segment
            .insert_new_vectors(point_id, 0, &vectors, &hw_counter)
            .err()
            .unwrap();
        segment
            .replace_all_vectors(internal_id, 0, &vectors, &hw_counter)
            .err()
            .unwrap();
    }

    for wrong_name in wrong_names {
        check_vector_name(wrong_name, &segment.segment_config)
            .err()
            .unwrap();
        segment
            .vector(wrong_name, point_id, &hw_counter)
            .err()
            .unwrap();
        segment
            .delete_vector(101, point_id, wrong_name)
            .err()
            .unwrap();
        segment.available_vector_count(wrong_name).err().unwrap();
        segment
            .vector_by_offset(wrong_name, internal_id, &hw_counter)
            .err()
            .unwrap();
    }
}

/// Test handling point versions
///
/// Apply if the point version is equal or higher. Always apply if the point does not exist
/// yet.
///
/// Before <https://github.com/qdrant/qdrant/pull/4060> this function would reject operations
/// on non-existent points if the operation ID was lower than the current segment version. That
/// should not happen, and this test asserts correct behavior.
#[test]
fn test_handle_point_version() {
    // Create base segment with a single point
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let dim = 4;

    let hw_counter = HardwareCounterCell::new();

    let mut segment = build_simple_segment(dir.path(), dim, Distance::Dot).unwrap();
    segment
        .upsert_point(
            100,
            1.into(),
            only_default_vector(&[1.1, 1.0, 0.0, 1.0]),
            &hw_counter,
        )
        .unwrap();

    // Do not handle operation on existing point when providing an old version
    let applied = segment
        .handle_point_version(99, Some(0), |_segment| Ok((true, None)))
        .unwrap();
    assert!(!applied);

    // Do handle operation on existing point when providing the current version
    let applied = segment
        .handle_point_version(100, Some(0), |_segment| Ok((true, None)))
        .unwrap();
    assert!(applied);

    // Do handle operation on existing point when providing a newer version
    let applied = segment
        .handle_point_version(101, Some(0), |_segment| Ok((true, None)))
        .unwrap();
    assert!(applied);

    // Always handle operations on non-existent points
    let applied = segment
        .handle_point_version(99, None, |_segment| Ok((true, None)))
        .unwrap();
    assert!(applied);
    let applied = segment
        .handle_point_version(100, None, |_segment| Ok((true, None)))
        .unwrap();
    assert!(applied);
    let applied = segment
        .handle_point_version(101, None, |_segment| Ok((true, None)))
        .unwrap();
    assert!(applied);
}
