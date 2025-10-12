use std::sync::atomic::AtomicBool;

use common::counter::hardware_accumulator::HwMeasurementAcc;
use common::tar_ext;
use fs_err::File;
use segment::data_types::named_vectors::NamedVectors;
use segment::data_types::query_context::QueryContext;
use segment::data_types::vectors::{DEFAULT_VECTOR_NAME, QueryVector, only_default_vector};
use segment::entry::{SegmentEntry, SnapshotEntry as _};
use segment::types::{FieldCondition, PayloadSchemaType};
use tempfile::Builder;

use super::*;
use crate::fixtures::*;

impl ProxySegment {
    /// This function is a simplified version of `search_batch` intended for testing purposes.
    #[allow(clippy::too_many_arguments)]
    pub fn search(
        &self,
        vector_name: &VectorName,
        vector: &QueryVector,
        with_payload: &WithPayload,
        with_vector: &WithVector,
        filter: Option<&Filter>,
        top: usize,
        params: Option<&SearchParams>,
    ) -> OperationResult<Vec<ScoredPoint>> {
        use segment::data_types::query_context::QueryContext;

        let query_context = QueryContext::default();
        let segment_query_context = query_context.get_segment_query_context();
        let result = self.search_batch(
            vector_name,
            &[vector],
            with_payload,
            with_vector,
            filter,
            top,
            params,
            &segment_query_context,
        )?;

        Ok(result.into_iter().next().unwrap())
    }
}

#[test]
fn test_search_batch_equivalence_single() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let original_segment = LockedSegment::new(build_segment_1(dir.path()));
    let hw_counter = HardwareCounterCell::new();

    let vec4 = vec![1.1, 1.0, 0.0, 1.0];
    original_segment
        .get()
        .write()
        .upsert_point(100, 4.into(), only_default_vector(&vec4), &hw_counter)
        .unwrap();
    let vec6 = vec![1.0, 1.0, 0.5, 1.0];
    original_segment
        .get()
        .write()
        .upsert_point(101, 6.into(), only_default_vector(&vec6), &hw_counter)
        .unwrap();

    let mut proxy_segment = ProxySegment::new(original_segment);

    proxy_segment
        .delete_point(102, 1.into(), &hw_counter)
        .unwrap();

    let query_vector = [1.0, 1.0, 1.0, 1.0].into();
    let search_result = proxy_segment
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

    let hardware_accumulator = HwMeasurementAcc::new();
    let query_context = QueryContext::new(10000, hardware_accumulator.clone());
    let segment_query_context = query_context.get_segment_query_context();

    let search_batch_result = proxy_segment
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
    assert!(hardware_accumulator.get_cpu() > 0);
}

#[test]
fn test_search_batch_equivalence_single_random() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let original_segment = LockedSegment::new(random_segment(dir.path(), 100, 200, 4));

    let proxy_segment = ProxySegment::new(original_segment);

    let query_vector = [1.0, 1.0, 1.0, 1.0].into();
    let search_result = proxy_segment
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

    let search_batch_result = proxy_segment
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
    assert_eq!(search_result, search_batch_result[0].clone())
}

#[test]
fn test_search_batch_equivalence_multi_random() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let original_segment = LockedSegment::new(random_segment(dir.path(), 100, 200, 4));

    let proxy_segment = ProxySegment::new(original_segment);

    let q1 = [1.0, 1.0, 1.0, 0.1];
    let q2 = [1.0, 1.0, 0.1, 0.1];
    let q3 = [1.0, 0.1, 1.0, 0.1];
    let q4 = [0.1, 1.0, 1.0, 0.1];

    let query_vectors: &[&QueryVector] = &[&q1.into(), &q2.into(), &q3.into(), &q4.into()];

    let mut all_single_results = Vec::with_capacity(query_vectors.len());
    for query_vector in query_vectors {
        let res = proxy_segment
            .search(
                DEFAULT_VECTOR_NAME,
                query_vector,
                &WithPayload::default(),
                &false.into(),
                None,
                10,
                None,
            )
            .unwrap();
        all_single_results.push(res);
    }

    eprintln!("search_result = {all_single_results:#?}");

    let query_context = QueryContext::default();
    let segment_query_context = query_context.get_segment_query_context();

    let search_batch_result = proxy_segment
        .search_batch(
            DEFAULT_VECTOR_NAME,
            query_vectors,
            &WithPayload::default(),
            &false.into(),
            None,
            10,
            None,
            &segment_query_context,
        )
        .unwrap();

    eprintln!("search_batch_result = {search_batch_result:#?}");

    assert_eq!(all_single_results, search_batch_result)
}

fn wrap_proxy(original_segment: LockedSegment) -> ProxySegment {
    ProxySegment::new(original_segment)
}

#[test]
fn test_read_filter() {
    let is_stopped = AtomicBool::new(false);
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let original_segment = LockedSegment::new(build_segment_1(dir.path()));

    let hw_counter = HardwareCounterCell::new();

    let filter = Filter::new_must_not(Condition::Field(FieldCondition::new_match(
        "color".parse().unwrap(),
        "blue".to_string().into(),
    )));

    let original_points = original_segment.get().read().read_filtered(
        None,
        Some(100),
        None,
        &is_stopped,
        &hw_counter,
    );

    let original_points_filtered = original_segment.get().read().read_filtered(
        None,
        Some(100),
        Some(&filter),
        &is_stopped,
        &hw_counter,
    );

    let mut proxy_segment = wrap_proxy(original_segment);

    let hw_counter = HardwareCounterCell::new();

    proxy_segment
        .delete_point(100, 2.into(), &hw_counter)
        .unwrap();

    let proxy_res = proxy_segment.read_filtered(None, Some(100), None, &is_stopped, &hw_counter);
    let proxy_res_filtered =
        proxy_segment.read_filtered(None, Some(100), Some(&filter), &is_stopped, &hw_counter);

    assert_eq!(original_points_filtered.len() - 1, proxy_res_filtered.len());
    assert_eq!(original_points.len() - 1, proxy_res.len());
}

#[test]
fn test_read_range() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let original_segment = LockedSegment::new(build_segment_1(dir.path()));

    let original_points = original_segment
        .get()
        .read()
        .read_range(None, Some(10.into()));

    let mut proxy_segment = wrap_proxy(original_segment);

    let hw_cell = HardwareCounterCell::new();

    proxy_segment.delete_point(100, 2.into(), &hw_cell).unwrap();

    let proxy_res = proxy_segment.read_range(None, Some(10.into()));

    assert_eq!(original_points.len() - 1, proxy_res.len());
}

#[test]
fn test_sync_indexes() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let original_segment = LockedSegment::new(build_segment_1(dir.path()));
    let write_segment = LockedSegment::new(empty_segment(dir.path()));

    original_segment
        .get()
        .write()
        .create_field_index(
            10,
            &"color".parse().unwrap(),
            Some(&PayloadSchemaType::Keyword.into()),
            &HardwareCounterCell::new(),
        )
        .unwrap();

    let proxy_segment = ProxySegment::new(original_segment.clone());

    let hw_cell = HardwareCounterCell::new();

    proxy_segment
        .replicate_field_indexes(0, &hw_cell, &write_segment)
        .unwrap();

    assert!(
        write_segment
            .get()
            .read()
            .get_indexed_fields()
            .contains_key(&"color".parse().unwrap()),
    );

    original_segment
        .get()
        .write()
        .create_field_index(
            11,
            &"location".parse().unwrap(),
            Some(&PayloadSchemaType::Geo.into()),
            &hw_cell,
        )
        .unwrap();

    original_segment
        .get()
        .write()
        .delete_field_index(12, &"color".parse().unwrap())
        .unwrap();

    proxy_segment
        .replicate_field_indexes(0, &hw_cell, &write_segment)
        .unwrap();

    assert!(
        write_segment
            .get()
            .read()
            .get_indexed_fields()
            .contains_key(&"location".parse().unwrap()),
    );
    assert!(
        !write_segment
            .get()
            .read()
            .get_indexed_fields()
            .contains_key(&"color".parse().unwrap()),
    );
}

#[test]
fn test_take_snapshot() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let original_segment = LockedSegment::new(build_segment_1(dir.path()));
    let original_segment_2 = LockedSegment::new(build_segment_2(dir.path()));

    let hw_cell = HardwareCounterCell::new();

    let mut proxy_segment = ProxySegment::new(original_segment);

    let proxy_segment2 = ProxySegment::new(original_segment_2);

    proxy_segment.delete_point(102, 1.into(), &hw_cell).unwrap();

    let snapshot_file = Builder::new().suffix(".snapshot.tar").tempfile().unwrap();
    eprintln!("Snapshot into {:?}", snapshot_file.path());
    let tar = tar_ext::BuilderExt::new_seekable_owned(File::create(snapshot_file.path()).unwrap());
    let temp_dir = Builder::new().prefix("temp_dir").tempdir().unwrap();
    let temp_dir2 = Builder::new().prefix("temp_dir").tempdir().unwrap();
    proxy_segment
        .take_snapshot(temp_dir.path(), &tar, SnapshotFormat::Regular, None)
        .unwrap();
    proxy_segment2
        .take_snapshot(temp_dir2.path(), &tar, SnapshotFormat::Regular, None)
        .unwrap();
    tar.blocking_finish().unwrap();

    // validate that 2 archives were created:
    // wrapped_segment1, wrapped_segment2
    let mut tar = tar::Archive::new(File::open(snapshot_file.path()).unwrap());
    let archive_count = tar.entries_with_seek().unwrap().count();
    assert_eq!(archive_count, 2);

    let mut tar = tar::Archive::new(File::open(snapshot_file.path()).unwrap());
    for entry in tar.entries_with_seek().unwrap() {
        let archive_path = entry.unwrap().path().unwrap().into_owned();
        let archive_extension = archive_path.extension().unwrap();
        // correct file extension
        assert_eq!(archive_extension, "tar");
    }
}

#[test]
fn test_point_vector_count() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let original_segment = LockedSegment::new(build_segment_1(dir.path()));

    let hw_cell = HardwareCounterCell::new();

    let mut proxy_segment = ProxySegment::new(original_segment);

    // We have 5 points by default, assert counts
    let segment_info = proxy_segment.info();
    assert_eq!(segment_info.num_points, 5);
    assert_eq!(segment_info.num_vectors, 5);

    // Delete nonexistent point, counts should remain the same
    proxy_segment
        .delete_point(101, 99999.into(), &hw_cell)
        .unwrap();
    let segment_info = proxy_segment.info();
    assert_eq!(segment_info.num_points, 5);
    assert_eq!(segment_info.num_vectors, 5);

    // Delete point 1, counts should decrease by 1
    proxy_segment.delete_point(102, 4.into(), &hw_cell).unwrap();
    let segment_info = proxy_segment.info();
    assert_eq!(segment_info.num_points, 4);
    assert_eq!(segment_info.num_vectors, 4);
}

#[test]
fn test_point_vector_count_multivec() {
    use segment::segment_constructor::simple_segment_constructor::{
        VECTOR1_NAME, VECTOR2_NAME, build_multivec_segment,
    };
    use segment::types::Distance;

    // Create proxied multivec segment
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let dim = 1;

    let mut original_segment = build_multivec_segment(dir.path(), dim, dim, Distance::Dot).unwrap();

    let hw_cell = HardwareCounterCell::new();

    original_segment
        .upsert_point(
            100,
            4.into(),
            NamedVectors::from_pairs([
                (VECTOR1_NAME.into(), vec![0.4]),
                (VECTOR2_NAME.into(), vec![0.5]),
            ]),
            &hw_cell,
        )
        .unwrap();
    original_segment
        .upsert_point(
            101,
            6.into(),
            NamedVectors::from_pairs([
                (VECTOR1_NAME.into(), vec![0.6]),
                (VECTOR2_NAME.into(), vec![0.7]),
            ]),
            &hw_cell,
        )
        .unwrap();

    let original_segment = LockedSegment::new(original_segment);

    let mut proxy_segment = ProxySegment::new(original_segment);

    // Assert counts from original segment
    let segment_info = proxy_segment.info();
    assert_eq!(segment_info.num_points, 2);
    assert_eq!(segment_info.num_vectors, 4);

    // Delete nonexistent point, counts should remain the same
    proxy_segment.delete_point(104, 1.into(), &hw_cell).unwrap();
    let segment_info = proxy_segment.info();
    assert_eq!(segment_info.num_points, 2);
    assert_eq!(segment_info.num_vectors, 4);

    // Delete point 4, counts should decrease by 1
    proxy_segment.delete_point(105, 4.into(), &hw_cell).unwrap();
    let segment_info = proxy_segment.info();
    assert_eq!(segment_info.num_points, 1);
    assert_eq!(segment_info.num_vectors, 2);
}

#[test]
fn test_proxy_segment_flush() {
    let tmp_dir = tempfile::Builder::new()
        .prefix("segment_dir")
        .tempdir()
        .unwrap();

    let locked_wrapped_segment = LockedSegment::new(build_segment_1(tmp_dir.path()));

    let mut proxy_segment = ProxySegment::new(locked_wrapped_segment.clone());

    let flushed_version_1 = proxy_segment.flush(false).unwrap();

    proxy_segment
        .delete_point(100, 2.into(), &HardwareCounterCell::new())
        .unwrap();

    let flushed_version_2 = proxy_segment.flush(false).unwrap();

    assert_eq!(flushed_version_2, flushed_version_1);

    let version_after_delete = proxy_segment.version();

    // We can never fully persist proxy segment, as list of deleted points is always in-memory only.
    // So we have to keep WAL for deleted points.
    assert!(version_after_delete > flushed_version_2);
}
