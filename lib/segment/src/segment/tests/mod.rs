mod test_facets;
mod test_immutable_payload_index_files;
mod test_vector_name_ops;

use std::assert_matches;
use std::sync::atomic::AtomicBool;

use ahash::AHashSet;
use common::counter::hardware_counter::HardwareCounterCell;
use common::tar_ext;
use common::tar_unpack::tar_unpack_file;
use common::types::{DeferredBehavior, PointOffsetType};
use fs_err as fs;
use fs_err::File;
use ordered_float::OrderedFloat;
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use rstest::rstest;
use serde_json::{Number, Value};
use sparse::common::sparse_vector::SparseVector;
use tempfile::{Builder, TempDir};

use super::*;
use crate::common::operation_error::OperationError::PointIdError;
use crate::common::{check_named_vectors, check_vector, check_vector_name};
use crate::data_types::facets::{FacetParams, FacetValue};
use crate::data_types::named_vectors::NamedVectors;
use crate::data_types::order_by::OrderBy;
use crate::data_types::query_context::QueryContext;
use crate::data_types::vectors::{
    DEFAULT_VECTOR_NAME, MultiDenseVectorInternal, QueryVector, VectorInternal, VectorRef,
    only_default_multi_vector, only_default_vector,
};
use crate::entry::entry_point::{
    NonAppendableSegmentEntry as _, ReadSegmentEntry as _, SegmentEntry as _,
};
use crate::entry::{SnapshotEntry as _, StorageSegmentEntry as _};
use crate::id_tracker::{IdTracker, IdTrackerRead};
use crate::index::sparse_index::sparse_index_config::{SparseIndexConfig, SparseIndexType};
use crate::json_path::JsonPath;
use crate::segment_constructor::simple_segment_constructor::{
    VECTOR1_NAME, VECTOR2_NAME, build_segment_with_two_named_vecs, build_simple_segment,
};
use crate::segment_constructor::{build_segment, load_segment};
use crate::types::{
    Condition, Distance, ExtendedPointId, FieldCondition, Filter, HasIdCondition, Indexes, Match,
    MultiVectorConfig, Payload, PayloadContainer, PayloadFieldSchema, PayloadSchemaType,
    PointIdType, SearchParams, SnapshotFormat, SparseVectorDataConfig, SparseVectorStorageType,
    ValueVariants, VectorDataConfig, VectorStorageDatatype, VectorStorageType, WithPayload,
    WithVector,
};
use crate::utils::maybe_arc::MaybeArc;
use crate::vector_storage::query::{FeedbackItem, NaiveFeedbackCoefficients, NaiveFeedbackQuery};

fn init_logger() {
    let _ = env_logger::builder().is_test(true).try_init();
}

#[test]
fn test_search_batch_equivalence_single() {
    init_logger();
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
    init_logger();
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
    use crate::entry::StorageSegmentEntry as _;

    init_logger();

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
        .segment_path
        .file_stem()
        .and_then(|f| f.to_str())
        .unwrap();

    segment.flush(true).unwrap();

    // snapshotting!
    let tar =
        tar_ext::BuilderExt::new_seekable_owned(File::create(parent_snapshot_tar.path()).unwrap());
    segment
        .take_snapshot(temp_dir.path(), &tar, format, None)
        .unwrap();
    tar.blocking_finish().unwrap();

    let parent_snapshot_unpacked = Builder::new().prefix("parent_snapshot").tempdir().unwrap();
    tar_unpack_file(parent_snapshot_tar.path(), parent_snapshot_unpacked.path()).unwrap();

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

    let restored_segment =
        load_segment(&entry.path(), Uuid::nil(), None, &AtomicBool::new(false)).unwrap();

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

/// Streamable snapshots historically wrap all segment files in a nested `files/` directory.
/// Recovery must also support streamable snapshots where the files are placed directly in the
/// segment directory, without the `files/` wrapper.
#[test]
fn test_snapshot_streamable_without_files_wrapper() {
    use crate::entry::StorageSegmentEntry as _;

    init_logger();

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
    let parent_snapshot_tar = Builder::new()
        .prefix("parent_snapshot")
        .suffix(".tar")
        .tempfile()
        .unwrap();
    let segment_id = segment
        .segment_path
        .file_stem()
        .and_then(|f| f.to_str())
        .unwrap()
        .to_string();

    segment.flush(true).unwrap();

    // Take a streamable snapshot, which produces a `<segment_id>/files/...` layout.
    let tar =
        tar_ext::BuilderExt::new_seekable_owned(File::create(parent_snapshot_tar.path()).unwrap());
    segment
        .take_snapshot(temp_dir.path(), &tar, SnapshotFormat::Streamable, None)
        .unwrap();
    tar.blocking_finish().unwrap();

    let parent_snapshot_unpacked = Builder::new().prefix("parent_snapshot").tempdir().unwrap();
    tar_unpack_file(parent_snapshot_tar.path(), parent_snapshot_unpacked.path()).unwrap();

    let segment_path = parent_snapshot_unpacked.path().join(&segment_id);
    assert!(segment_path.is_dir());

    // Simulate a streamable snapshot without the nested `files/` wrapper by hoisting the files
    // out of `files/` and removing it.
    let files_path = segment_path.join("files");
    assert!(files_path.is_dir(), "expected nested `files/` directory");
    crate::utils::fs::move_all(&files_path, &segment_path).unwrap();
    fs::remove_dir(&files_path).unwrap();

    // Restore snapshot from the flattened layout.
    Segment::restore_snapshot_in_place(&segment_path).unwrap();

    assert!(segment_path.is_dir());

    let restored_segment =
        load_segment(&segment_path, Uuid::nil(), None, &AtomicBool::new(false)).unwrap();

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
fn test_check_consistency() {
    init_logger();
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

    let internal_id = segment
        .with_view(|v| v.lookup_internal_id(6.into(), DeferredBehavior::VisibleOnly))
        .unwrap();

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
    assert_matches!(
        segment.vector(DEFAULT_VECTOR_NAME, 6.into(), &hw_counter),
        Err(PointIdError { missed_point_id }) if missed_point_id == 6.into(),
    );

    // but querying by internal id still works
    matches!(
        segment.with_view(|v| v.vector_by_offset(DEFAULT_VECTOR_NAME, internal_id, &hw_counter)),
        Ok(Some(_))
    );

    // fix segment's data
    segment.check_consistency_and_repair().unwrap();

    // querying by internal id now consistent
    matches!(
        segment.with_view(|v| v.vector_by_offset(DEFAULT_VECTOR_NAME, internal_id, &hw_counter)),
        Ok(None)
    );
}

#[test]
fn test_point_vector_count() {
    init_logger();
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
    let segment_info = segment.info().unwrap();
    assert_eq!(segment_info.num_points, 2);
    assert_eq!(segment_info.num_vectors, 2);

    // Delete nonexistent point, counts should remain the same
    segment.delete_point(102, 1.into(), &hw_counter).unwrap();
    let segment_info = segment.info().unwrap();
    assert_eq!(segment_info.num_points, 2);
    assert_eq!(segment_info.num_vectors, 2);

    // Delete point 4, counts should decrease by 1
    segment.delete_point(103, 4.into(), &hw_counter).unwrap();
    let segment_info = segment.info().unwrap();
    assert_eq!(segment_info.num_points, 1);
    assert_eq!(segment_info.num_vectors, 2); // We don't propagate deletes to vectors at this time

    // // Delete vector of point 6, vector count should now be zero
    // segment
    //     .delete_vector(104, 6.into(), DEFAULT_VECTOR_NAME)
    //     .unwrap();
    // let segment_info = segment.info().unwrap();
    // assert_eq!(segment_info.num_points, 1);
    // assert_eq!(segment_info.num_vectors, 1);
}

#[test]
fn test_point_vector_count_multivec() {
    init_logger();
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let dim = 1;

    let mut segment =
        build_segment_with_two_named_vecs(dir.path(), dim, dim, Distance::Dot).unwrap();

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
    let segment_info = segment.info().unwrap();
    assert_eq!(segment_info.num_points, 4);
    assert_eq!(segment_info.num_vectors, 6);

    // Delete nonexistent point, counts should remain the same
    segment.delete_point(104, 1.into(), &hw_counter).unwrap();
    let segment_info = segment.info().unwrap();
    assert_eq!(segment_info.num_points, 4);
    assert_eq!(segment_info.num_vectors, 6);

    // Delete point 4, counts should decrease by 1
    segment.delete_point(105, 4.into(), &hw_counter).unwrap();
    let segment_info = segment.info().unwrap();
    assert_eq!(segment_info.num_points, 3);
    assert_eq!(segment_info.num_vectors, 6); // We don't propagate deletes to vectors at this time

    // Delete vector 'a' of point 6, vector count should decrease by 1
    segment.delete_vector(106, 6.into(), VECTOR1_NAME).unwrap();
    let segment_info = segment.info().unwrap();
    assert_eq!(segment_info.num_points, 3);
    assert_eq!(segment_info.num_vectors, 5);

    // Deleting it again shouldn't chain anything
    segment.delete_vector(107, 6.into(), VECTOR1_NAME).unwrap();
    let segment_info = segment.info().unwrap();
    assert_eq!(segment_info.num_points, 3);
    assert_eq!(segment_info.num_vectors, 5);

    // Replace vector 'a' for point 8, counts should remain the same
    let internal_8 = segment
        .with_view(|v| v.lookup_internal_id(8.into(), DeferredBehavior::VisibleOnly))
        .unwrap();
    segment
        .replace_all_vectors(
            internal_8,
            0,
            &NamedVectors::from_pairs([(VECTOR1_NAME.into(), vec![0.1])]),
            &hw_counter,
        )
        .unwrap();
    let segment_info = segment.info().unwrap();
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
    let segment_info = segment.info().unwrap();
    assert_eq!(segment_info.num_points, 3);
    assert_eq!(segment_info.num_vectors, 6);
}

/// `retrieve_raw` on a plain dense segment must return the verbatim f32 bytes
/// of the stored vector (zero-copy `bytemuck` cast), with payload untouched.
#[test]
fn test_retrieve_raw_dense_bytes() {
    init_logger();
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let dim = 4;

    let mut segment = build_simple_segment(dir.path(), dim, Distance::Dot).unwrap();
    let hw_counter = HardwareCounterCell::new();

    let vec = vec![0.1_f32, 0.2, 0.3, 0.4];
    segment
        .upsert_point(100, 7.into(), only_default_vector(&vec), &hw_counter)
        .unwrap();

    let is_stopped = AtomicBool::new(false);
    let raw = segment
        .retrieve_raw(
            &[7.into()],
            &WithPayload::default(),
            &true.into(),
            &hw_counter,
            &is_stopped,
            DeferredBehavior::VisibleOnly,
        )
        .unwrap();

    let record = raw.get(&7.into()).expect("point 7 must be retrieved");
    let vectors = record.vectors.as_ref().expect("vectors requested");
    let (_, bytes) = vectors
        .iter()
        .find(|(name, _)| name == DEFAULT_VECTOR_NAME)
        .expect("default vector must be present");

    let expected: &[u8] = bytemuck::cast_slice(&vec);
    assert_eq!(bytes.as_slice(), expected);
}

/// `retrieve_raw` over a multi-dense (ColBERT-style) storage must return the
/// flattened inner vectors as raw bytes; the inner count is derived from the
/// byte length, so the blob must cover all inner vectors, not just the first.
#[test]
fn test_retrieve_raw_multivec_bytes() {
    init_logger();
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let dim = 3;

    let (mut segment, _) = build_segment(
        dir.path(),
        &SegmentConfig {
            vector_data: HashMap::from([(
                DEFAULT_VECTOR_NAME.to_owned(),
                VectorDataConfig {
                    size: dim,
                    distance: Distance::Dot,
                    storage_type: VectorStorageType::default(),
                    index: Indexes::Plain {},
                    quantization_config: None,
                    multivector_config: Some(MultiVectorConfig::default()),
                    datatype: None,
                },
            )]),
            sparse_vector_data: Default::default(),
            payload_storage_type: Default::default(),
        },
        None,
        true,
    )
    .unwrap();
    let hw_counter = HardwareCounterCell::new();

    // Two inner vectors of `dim` elements each, flattened.
    let flattened = vec![0.1_f32, 0.2, 0.3, 0.4, 0.5, 0.6];
    let multi_vec = MultiDenseVectorInternal::new(flattened.clone(), dim);
    segment
        .upsert_point(
            100,
            4.into(),
            only_default_multi_vector(&multi_vec),
            &hw_counter,
        )
        .unwrap();

    let is_stopped = AtomicBool::new(false);
    let raw = segment
        .retrieve_raw(
            &[4.into()],
            &WithPayload::default(),
            &true.into(),
            &hw_counter,
            &is_stopped,
            DeferredBehavior::VisibleOnly,
        )
        .unwrap();

    let record = raw.get(&4.into()).expect("point 4 must be retrieved");
    let vectors = record.vectors.as_ref().expect("vectors requested");
    let (_, bytes) = vectors
        .iter()
        .find(|(name, _)| name == DEFAULT_VECTOR_NAME)
        .expect("default vector must be present");

    let expected: &[u8] = bytemuck::cast_slice(&flattened);
    assert_eq!(bytes.as_slice(), expected);
}

/// `retrieve_raw` over a sparse storage must return the on-disk
/// `StoredSparseVector` bincode form, which decodes back to the original vector.
#[test]
fn test_retrieve_raw_sparse_bytes() {
    use gridstore::Blob;

    use crate::vector_storage::sparse::StoredSparseVector;

    init_logger();
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();

    let sparse_name = "sparse";
    let (mut segment, _) = build_segment(
        dir.path(),
        &SegmentConfig {
            vector_data: HashMap::new(),
            sparse_vector_data: HashMap::from_iter([(
                sparse_name.to_string(),
                SparseVectorDataConfig {
                    index: SparseIndexConfig::new(Some(1), SparseIndexType::MutableRam, None, None),
                    storage_type: SparseVectorStorageType::Mmap,
                    modifier: None,
                },
            )]),
            payload_storage_type: Default::default(),
        },
        None,
        true,
    )
    .unwrap();

    let hw_counter = HardwareCounterCell::new();
    let sparse = SparseVector::new(vec![1, 5, 42], vec![0.5, 1.5, 2.5]).unwrap();
    let mut vectors = NamedVectors::default();
    vectors.insert(
        sparse_name.to_string(),
        VectorInternal::Sparse(sparse.clone()),
    );
    segment
        .upsert_point(100, 7.into(), vectors, &hw_counter)
        .unwrap();

    let is_stopped = AtomicBool::new(false);
    let raw = segment
        .retrieve_raw(
            &[7.into()],
            &WithPayload::default(),
            &true.into(),
            &hw_counter,
            &is_stopped,
            DeferredBehavior::VisibleOnly,
        )
        .unwrap();

    let record = raw.get(&7.into()).expect("point 7 must be retrieved");
    let vectors = record.vectors.as_ref().expect("vectors requested");
    let (_, bytes) = vectors
        .iter()
        .find(|(name, _)| name == sparse_name)
        .expect("sparse vector must be present");

    let decoded = SparseVector::try_from(StoredSparseVector::from_bytes(bytes)).unwrap();
    assert_eq!(decoded, sparse);
}

/// Fetch one named vector of one point via `retrieve_raw`.
fn retrieve_raw_vector(segment: &Segment, point_id: PointIdType, name: &str) -> Vec<u8> {
    let hw_counter = HardwareCounterCell::new();
    let is_stopped = AtomicBool::new(false);
    let raw = segment
        .retrieve_raw(
            &[point_id],
            &WithPayload::default(),
            &true.into(),
            &hw_counter,
            &is_stopped,
            DeferredBehavior::VisibleOnly,
        )
        .unwrap();
    let record = raw.get(&point_id).expect("point must be retrieved");
    let vectors = record.vectors.as_ref().expect("vectors requested");
    vectors
        .iter()
        .find(|(n, _)| n == name)
        .map(|(_, bytes)| bytes.clone())
        .expect("vector must be present")
}

/// `upsert_point_raw` with `retrieve_raw` bytes must reproduce the point: the
/// destination decodes to the original vector and re-serializes to identical
/// bytes. Also covers the replace path (raw upsert over an existing point).
#[test]
fn test_upsert_raw_dense_roundtrip() {
    init_logger();
    let src_dir = Builder::new().prefix("segment_src").tempdir().unwrap();
    let dst_dir = Builder::new().prefix("segment_dst").tempdir().unwrap();
    let dim = 4;

    let mut src = build_simple_segment(src_dir.path(), dim, Distance::Dot).unwrap();
    let mut dst = build_simple_segment(dst_dir.path(), dim, Distance::Dot).unwrap();
    let hw_counter = HardwareCounterCell::new();

    let vec = vec![0.1_f32, 0.2, 0.3, 0.4];
    src.upsert_point(100, 7.into(), only_default_vector(&vec), &hw_counter)
        .unwrap();
    let bytes = retrieve_raw_vector(&src, 7.into(), DEFAULT_VECTOR_NAME);

    // Insert path: the point does not exist in the destination yet.
    dst.upsert_point_raw(
        100,
        7.into(),
        &[(DEFAULT_VECTOR_NAME.to_owned(), bytes.clone())],
        &hw_counter,
    )
    .unwrap();

    assert_eq!(
        dst.vector(DEFAULT_VECTOR_NAME, 7.into(), &hw_counter)
            .unwrap(),
        Some(VectorInternal::Dense(vec)),
    );
    assert_eq!(
        retrieve_raw_vector(&dst, 7.into(), DEFAULT_VECTOR_NAME),
        bytes
    );

    // Replace path: raw upsert over the existing point with different bytes.
    let vec2 = vec![1.0_f32, 2.0, 3.0, 4.0];
    src.upsert_point(101, 8.into(), only_default_vector(&vec2), &hw_counter)
        .unwrap();
    let bytes2 = retrieve_raw_vector(&src, 8.into(), DEFAULT_VECTOR_NAME);
    dst.upsert_point_raw(
        101,
        7.into(),
        &[(DEFAULT_VECTOR_NAME.to_owned(), bytes2)],
        &hw_counter,
    )
    .unwrap();
    assert_eq!(
        dst.vector(DEFAULT_VECTOR_NAME, 7.into(), &hw_counter)
            .unwrap(),
        Some(VectorInternal::Dense(vec2)),
    );
}

/// Raw upsert over an existing point on an append-only segment must go
/// through the clone-and-tombstone path: vectors are replaced from the raw
/// bytes at a fresh internal id, the payload survives the move.
#[test]
fn test_upsert_raw_append_only_replace() {
    init_logger();
    let src_dir = Builder::new().prefix("segment_src").tempdir().unwrap();
    let dst_dir = Builder::new().prefix("segment_dst").tempdir().unwrap();
    let dim = 4;

    let mut src = build_simple_segment(src_dir.path(), dim, Distance::Dot).unwrap();
    let mut dst = build_simple_segment(dst_dir.path(), dim, Distance::Dot).unwrap();
    dst.append_only_mutations = true;
    let hw_counter = HardwareCounterCell::new();

    let old_vec = vec![1.0_f32, 0.0, 1.0, 0.0];
    dst.upsert_point(100, 7.into(), only_default_vector(&old_vec), &hw_counter)
        .unwrap();
    let payload: Payload = serde_json::from_str(r#"{"color": "red"}"#).unwrap();
    dst.set_full_payload(101, 7.into(), &payload, &hw_counter)
        .unwrap();
    let old_internal_id = dst
        .with_view(|v| v.lookup_internal_id(7.into(), DeferredBehavior::VisibleOnly))
        .unwrap();

    let new_vec = vec![0.1_f32, 0.2, 0.3, 0.4];
    src.upsert_point(100, 7.into(), only_default_vector(&new_vec), &hw_counter)
        .unwrap();
    let bytes = retrieve_raw_vector(&src, 7.into(), DEFAULT_VECTOR_NAME);

    dst.upsert_point_raw(
        102,
        7.into(),
        &[(DEFAULT_VECTOR_NAME.to_owned(), bytes.clone())],
        &hw_counter,
    )
    .unwrap();

    // The point moved to a fresh internal id (old slot tombstoned)...
    let new_internal_id = dst
        .with_view(|v| v.lookup_internal_id(7.into(), DeferredBehavior::VisibleOnly))
        .unwrap();
    assert_ne!(old_internal_id, new_internal_id);
    // ...carrying the replaced vector (byte-identical) and the old payload.
    assert_eq!(
        dst.vector(DEFAULT_VECTOR_NAME, 7.into(), &hw_counter)
            .unwrap(),
        Some(VectorInternal::Dense(new_vec)),
    );
    assert_eq!(
        retrieve_raw_vector(&dst, 7.into(), DEFAULT_VECTOR_NAME),
        bytes
    );
    assert_eq!(dst.payload(7.into(), &hw_counter).unwrap(), payload);
}

/// Clone-based mutations rewrite the point's payload row at a fresh internal
/// id, mutating payload storage even when the triggering operation is
/// vectors-only — the snapshot version stamp must move, or partial snapshots
/// skip the changed payload files. Payload operations must stamp exactly
/// once: a same-version double bump collapses the tracked version to `None`.
#[test]
fn test_append_only_clone_stamps_payload_version() {
    init_logger();
    let src_dir = Builder::new().prefix("segment_src").tempdir().unwrap();
    let dst_dir = Builder::new().prefix("segment_dst").tempdir().unwrap();
    let dim = 4;

    let mut src = build_simple_segment(src_dir.path(), dim, Distance::Dot).unwrap();
    let mut segment = build_simple_segment(dst_dir.path(), dim, Distance::Dot).unwrap();
    segment.append_only_mutations = true;
    let hw_counter = HardwareCounterCell::new();

    let vec = vec![1.0_f32, 0.0, 1.0, 0.0];
    segment
        .upsert_point(100, 7.into(), only_default_vector(&vec), &hw_counter)
        .unwrap();
    // A fresh insert writes no payload row.
    assert_eq!(segment.version_tracker.get_payload(), None);

    // A payload operation stamps exactly once (clone arm only, no
    // caller-side double bump that would collapse the version to `None`).
    let payload: Payload = serde_json::from_str(r#"{"color": "red"}"#).unwrap();
    segment
        .set_full_payload(101, 7.into(), &payload, &hw_counter)
        .unwrap();
    assert_eq!(segment.version_tracker.get_payload(), Some(101));

    // A vectors-only update clones the point, rewriting its payload row at
    // the fresh id: the stamp must follow.
    let vec2 = vec![0.1_f32, 0.2, 0.3, 0.4];
    segment
        .upsert_point(102, 7.into(), only_default_vector(&vec2), &hw_counter)
        .unwrap();
    assert_eq!(segment.version_tracker.get_payload(), Some(102));

    // Same for the raw clone path.
    src.upsert_point(100, 7.into(), only_default_vector(&vec), &hw_counter)
        .unwrap();
    let bytes = retrieve_raw_vector(&src, 7.into(), DEFAULT_VECTOR_NAME);
    segment
        .upsert_point_raw(
            103,
            7.into(),
            &[(DEFAULT_VECTOR_NAME.to_owned(), bytes)],
            &hw_counter,
        )
        .unwrap();
    assert_eq!(segment.version_tracker.get_payload(), Some(103));
    assert_eq!(segment.payload(7.into(), &hw_counter).unwrap(), payload);

    // In-place payload operations (append-only off) still stamp.
    let plain_dir = Builder::new().prefix("segment_plain").tempdir().unwrap();
    let mut plain = build_simple_segment(plain_dir.path(), dim, Distance::Dot).unwrap();
    plain
        .upsert_point(100, 7.into(), only_default_vector(&vec), &hw_counter)
        .unwrap();
    plain
        .set_full_payload(101, 7.into(), &payload, &hw_counter)
        .unwrap();
    assert_eq!(plain.version_tracker.get_payload(), Some(101));
}

/// In append-only mode a point deletion must be tombstone-only even on a
/// non-appendable segment: clearing the payload row mutates committed state
/// of an offset that stays visible to live-reload followers until the
/// id-tracker drop is flushed — a follower refreshing in that window
/// observes the point alive with an empty payload. Observed 2026-07-13 on
/// the CoW-update path, which deletes the old copy from a non-appendable
/// segment.
#[test]
fn test_append_only_delete_is_tombstone_only_on_non_appendable() {
    use crate::id_tracker::IdTrackerRead as _;
    use crate::index::PayloadIndexRead as _;

    init_logger();
    let dir = Builder::new().prefix("segment").tempdir().unwrap();
    let dim = 4;

    let mut segment = build_simple_segment(dir.path(), dim, Distance::Dot).unwrap();
    let hw_counter = HardwareCounterCell::new();

    let vec = vec![1.0_f32, 0.0, 1.0, 0.0];
    segment
        .upsert_point(100, 7.into(), only_default_vector(&vec), &hw_counter)
        .unwrap();
    let payload: Payload = serde_json::from_str(r#"{"color": "red"}"#).unwrap();
    segment
        .set_full_payload(101, 7.into(), &payload, &hw_counter)
        .unwrap();

    let internal_id = segment.get_internal_id(7.into()).unwrap();

    // Emulate a non-appendable segment in an append-only deployment.
    segment.appendable_flag = false;
    segment.append_only_mutations = true;

    assert!(segment.delete_point(102, 7.into(), &hw_counter).unwrap());

    // The point is masked via the id tracker...
    assert!(segment.id_tracker.borrow().is_deleted_point(internal_id));
    // ...but its payload row is untouched: committed state of the old offset
    // must not be mutated in place while readers may still resolve to it.
    let stored = segment
        .payload_index
        .borrow()
        .with_view(|view| view.get_payload(internal_id, &hw_counter))
        .unwrap();
    assert_eq!(stored, payload);
}

/// `upsert_moved_point` writes the whole point — raw bytes, decoded overlay,
/// payload — in one operation, allocating exactly one internal slot where the
/// separate `upsert_point_raw` + `update_vectors` + `set_full_payload` steps
/// would clone the point once per step on an append-only segment.
#[test]
fn test_upsert_moved_point_single_slot() {
    use crate::id_tracker::IdTrackerRead as _;

    init_logger();
    let src_dir = Builder::new().prefix("segment_src").tempdir().unwrap();
    let dst_dir = Builder::new().prefix("segment_dst").tempdir().unwrap();
    let dim = 4;

    let mut src = build_simple_segment(src_dir.path(), dim, Distance::Dot).unwrap();
    let mut dst = build_simple_segment(dst_dir.path(), dim, Distance::Dot).unwrap();
    dst.append_only_mutations = true;
    let hw_counter = HardwareCounterCell::new();

    let raw_vec = vec![1.0_f32, 0.0, 1.0, 0.0];
    src.upsert_point(100, 7.into(), only_default_vector(&raw_vec), &hw_counter)
        .unwrap();
    let bytes = retrieve_raw_vector(&src, 7.into(), DEFAULT_VECTOR_NAME);
    let payload: Payload = serde_json::from_str(r#"{"color": "red"}"#).unwrap();

    // Insert path: raw bytes only, exactly one slot.
    dst.upsert_moved_point(
        101,
        7.into(),
        &[(DEFAULT_VECTOR_NAME.to_owned(), bytes.clone())],
        NamedVectors::default(),
        &payload,
        &hw_counter,
    )
    .unwrap();
    assert_eq!(dst.id_tracker.borrow().total_point_count(), 1);
    assert_eq!(
        dst.vector(DEFAULT_VECTOR_NAME, 7.into(), &hw_counter)
            .unwrap(),
        Some(VectorInternal::Dense(raw_vec)),
    );
    assert_eq!(dst.payload(7.into(), &hw_counter).unwrap(), payload);
    // Payload storage was mutated: its snapshot version stamp must move to
    // this op, or partial snapshots would skip the changed payload files.
    assert_eq!(dst.version_tracker.get_payload(), Some(101));

    // Replace path with a decoded overlay shadowing the raw bytes: exactly
    // one fresh slot (the clone), and the overlay wins over the raw bytes.
    let new_vec = vec![0.1_f32, 0.2, 0.3, 0.4];
    let payload2: Payload = serde_json::from_str(r#"{"color": "blue"}"#).unwrap();
    dst.upsert_moved_point(
        102,
        7.into(),
        &[(DEFAULT_VECTOR_NAME.to_owned(), bytes)],
        only_default_vector(&new_vec),
        &payload2,
        &hw_counter,
    )
    .unwrap();
    assert_eq!(dst.id_tracker.borrow().total_point_count(), 2);
    assert_eq!(
        dst.vector(DEFAULT_VECTOR_NAME, 7.into(), &hw_counter)
            .unwrap(),
        Some(VectorInternal::Dense(new_vec)),
    );
    assert_eq!(dst.payload(7.into(), &hw_counter).unwrap(), payload2);
    assert_eq!(dst.version_tracker.get_payload(), Some(102));
}

/// On an append-only segment, the follow-up steps of a multi-step point
/// write (e.g. the `set_full_payload` after an `upsert_point`, sharing its
/// `op_num`) must mutate the slot written by the same operation in place
/// instead of cloning it: one operation allocates exactly one slot.
#[test]
fn test_append_only_same_op_multi_step_single_slot() {
    use crate::id_tracker::IdTrackerRead as _;

    init_logger();
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let dim = 4;

    let mut segment = build_simple_segment(dir.path(), dim, Distance::Dot).unwrap();
    segment.append_only_mutations = true;
    let hw_counter = HardwareCounterCell::new();

    // New point: insert + payload at the same op_num — one slot.
    let vec = vec![1.0_f32, 0.0, 1.0, 0.0];
    let payload: Payload = serde_json::from_str(r#"{"color": "red"}"#).unwrap();
    segment
        .upsert_point(100, 7.into(), only_default_vector(&vec), &hw_counter)
        .unwrap();
    segment
        .set_full_payload(100, 7.into(), &payload, &hw_counter)
        .unwrap();
    assert_eq!(segment.id_tracker.borrow().total_point_count(), 1);

    // Update of a committed point: the first step clones to a fresh slot,
    // the second step mutates that same-op slot in place — one new slot.
    let vec2 = vec![0.1_f32, 0.2, 0.3, 0.4];
    let payload2: Payload = serde_json::from_str(r#"{"color": "blue"}"#).unwrap();
    segment
        .upsert_point(101, 7.into(), only_default_vector(&vec2), &hw_counter)
        .unwrap();
    segment
        .set_full_payload(101, 7.into(), &payload2, &hw_counter)
        .unwrap();
    assert_eq!(segment.id_tracker.borrow().total_point_count(), 2);
    assert_eq!(
        segment
            .vector(DEFAULT_VECTOR_NAME, 7.into(), &hw_counter)
            .unwrap(),
        Some(VectorInternal::Dense(vec2)),
    );
    assert_eq!(segment.payload(7.into(), &hw_counter).unwrap(), payload2);

    // A later operation still clones: committed slots are never mutated.
    let payload3: Payload = serde_json::from_str(r#"{"color": "green"}"#).unwrap();
    segment
        .set_full_payload(102, 7.into(), &payload3, &hw_counter)
        .unwrap();
    assert_eq!(segment.id_tracker.borrow().total_point_count(), 3);
    assert_eq!(segment.payload(7.into(), &hw_counter).unwrap(), payload3);
}

/// Multi-dense raw round-trip: the flattened inner-vector blob must survive
/// `retrieve_raw` → `upsert_point_raw` byte-identically and decode to the
/// original multivector.
#[test]
fn test_upsert_raw_multivec_roundtrip() {
    init_logger();
    let dim = 3;
    let config = SegmentConfig {
        vector_data: HashMap::from([(
            DEFAULT_VECTOR_NAME.to_owned(),
            VectorDataConfig {
                size: dim,
                distance: Distance::Dot,
                storage_type: VectorStorageType::default(),
                index: Indexes::Plain {},
                quantization_config: None,
                multivector_config: Some(MultiVectorConfig::default()),
                datatype: None,
            },
        )]),
        sparse_vector_data: Default::default(),
        payload_storage_type: Default::default(),
    };
    let src_dir = Builder::new().prefix("segment_src").tempdir().unwrap();
    let dst_dir = Builder::new().prefix("segment_dst").tempdir().unwrap();
    let (mut src, _) = build_segment(src_dir.path(), &config, None, true).unwrap();
    let (mut dst, _) = build_segment(dst_dir.path(), &config, None, true).unwrap();
    let hw_counter = HardwareCounterCell::new();

    // Two inner vectors of `dim` elements each, flattened.
    let flattened = vec![0.1_f32, 0.2, 0.3, 0.4, 0.5, 0.6];
    let multi_vec = MultiDenseVectorInternal::new(flattened, dim);
    src.upsert_point(
        100,
        4.into(),
        only_default_multi_vector(&multi_vec),
        &hw_counter,
    )
    .unwrap();
    let bytes = retrieve_raw_vector(&src, 4.into(), DEFAULT_VECTOR_NAME);

    dst.upsert_point_raw(
        100,
        4.into(),
        &[(DEFAULT_VECTOR_NAME.to_owned(), bytes.clone())],
        &hw_counter,
    )
    .unwrap();

    assert_eq!(
        dst.vector(DEFAULT_VECTOR_NAME, 4.into(), &hw_counter)
            .unwrap(),
        Some(VectorInternal::MultiDense(multi_vec)),
    );
    assert_eq!(
        retrieve_raw_vector(&dst, 4.into(), DEFAULT_VECTOR_NAME),
        bytes
    );
}

/// Sparse raw round-trip: the `StoredSparseVector` bincode blob must insert
/// back and decode to the original sparse vector.
#[test]
fn test_upsert_raw_sparse_roundtrip() {
    init_logger();
    let sparse_name = "sparse";
    let config = SegmentConfig {
        vector_data: HashMap::new(),
        sparse_vector_data: HashMap::from_iter([(
            sparse_name.to_string(),
            SparseVectorDataConfig {
                index: SparseIndexConfig::new(Some(1), SparseIndexType::MutableRam, None, None),
                storage_type: SparseVectorStorageType::Mmap,
                modifier: None,
            },
        )]),
        payload_storage_type: Default::default(),
    };
    let src_dir = Builder::new().prefix("segment_src").tempdir().unwrap();
    let dst_dir = Builder::new().prefix("segment_dst").tempdir().unwrap();
    let (mut src, _) = build_segment(src_dir.path(), &config, None, true).unwrap();
    let (mut dst, _) = build_segment(dst_dir.path(), &config, None, true).unwrap();
    let hw_counter = HardwareCounterCell::new();

    let sparse = SparseVector::new(vec![1, 5, 42], vec![0.5, 1.5, 2.5]).unwrap();
    let mut vectors = NamedVectors::default();
    vectors.insert(
        sparse_name.to_string(),
        VectorInternal::Sparse(sparse.clone()),
    );
    src.upsert_point(100, 7.into(), vectors, &hw_counter)
        .unwrap();
    let bytes = retrieve_raw_vector(&src, 7.into(), sparse_name);

    dst.upsert_point_raw(
        100,
        7.into(),
        &[(sparse_name.to_string(), bytes.clone())],
        &hw_counter,
    )
    .unwrap();

    assert_eq!(
        dst.vector(sparse_name, 7.into(), &hw_counter).unwrap(),
        Some(VectorInternal::Sparse(sparse)),
    );
    assert_eq!(retrieve_raw_vector(&dst, 7.into(), sparse_name), bytes);
}

/// Byte/half dense raw round-trip: the packed `[T]` blob must survive
/// `retrieve_raw` → `upsert_point_raw` byte-identically for the narrow
/// element types too — the `T` → `f32` → `T` hop through the regular insert
/// path is lossless.
#[test]
fn test_upsert_raw_dense_narrow_datatypes_roundtrip() {
    init_logger();
    let dim = 4;
    for datatype in [VectorStorageDatatype::Uint8, VectorStorageDatatype::Float16] {
        let config = SegmentConfig {
            vector_data: HashMap::from([(
                DEFAULT_VECTOR_NAME.to_owned(),
                VectorDataConfig {
                    size: dim,
                    distance: Distance::Dot,
                    storage_type: VectorStorageType::ChunkedMmap,
                    index: Indexes::Plain {},
                    quantization_config: None,
                    multivector_config: None,
                    datatype: Some(datatype),
                },
            )]),
            sparse_vector_data: Default::default(),
            payload_storage_type: Default::default(),
        };
        let src_dir = Builder::new().prefix("segment_src").tempdir().unwrap();
        let dst_dir = Builder::new().prefix("segment_dst").tempdir().unwrap();
        let (mut src, _) = build_segment(src_dir.path(), &config, None, true).unwrap();
        let (mut dst, _) = build_segment(dst_dir.path(), &config, None, true).unwrap();
        let hw_counter = HardwareCounterCell::new();

        // Values exactly representable in both u8 and f16.
        let vec = vec![0.0_f32, 1.0, 128.0, 255.0];
        src.upsert_point(100, 7.into(), only_default_vector(&vec), &hw_counter)
            .unwrap();
        let bytes = retrieve_raw_vector(&src, 7.into(), DEFAULT_VECTOR_NAME);

        dst.upsert_point_raw(
            100,
            7.into(),
            &[(DEFAULT_VECTOR_NAME.to_owned(), bytes.clone())],
            &hw_counter,
        )
        .unwrap();

        assert_eq!(
            retrieve_raw_vector(&dst, 7.into(), DEFAULT_VECTOR_NAME),
            bytes,
            "raw bytes must round-trip for {datatype:?}",
        );
        assert_eq!(
            dst.vector(DEFAULT_VECTOR_NAME, 7.into(), &hw_counter)
                .unwrap(),
            src.vector(DEFAULT_VECTOR_NAME, 7.into(), &hw_counter)
                .unwrap(),
            "decoded vector must round-trip for {datatype:?}",
        );
    }
}

/// A raw blob whose size doesn't match the storage layout must be rejected
/// with an error, not silently mis-written.
#[test]
fn test_upsert_raw_malformed_blob_rejected() {
    init_logger();
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let mut segment = build_simple_segment(dir.path(), 4, Distance::Dot).unwrap();
    let hw_counter = HardwareCounterCell::new();

    // 3 bytes is not a valid packed-[f32; 4] blob.
    let result = segment.upsert_point_raw(
        100,
        7.into(),
        &[(DEFAULT_VECTOR_NAME.to_owned(), vec![0_u8, 1, 2])],
        &hw_counter,
    );
    // Must be a user error (`MalformedVectorBlob`), not a `ServiceError`: a
    // malformed blob that reached the WAL is skipped on replay instead of
    // crash-looping recovery.
    assert!(
        matches!(
            result,
            Err(crate::common::operation_error::OperationError::MalformedVectorBlob { .. })
        ),
        "malformed blob must be rejected as MalformedVectorBlob, got {result:?}",
    );
}

/// TurboQuant dense raw round-trip: the encoded TQ blob must be ingested
/// verbatim — byte-identical after `upsert_point_raw` → `retrieve_raw`, with
/// no dequantize/requantize drift (destination decodes exactly like the
/// source).
#[test]
fn test_upsert_raw_dense_turbo_bytes() {
    init_logger();
    let dim = 64;
    let config = SegmentConfig {
        vector_data: HashMap::from([(
            DEFAULT_VECTOR_NAME.to_owned(),
            VectorDataConfig {
                size: dim,
                distance: Distance::Dot,
                storage_type: VectorStorageType::ChunkedMmap,
                index: Indexes::Plain {},
                quantization_config: None,
                multivector_config: None,
                datatype: Some(VectorStorageDatatype::Turbo4),
            },
        )]),
        sparse_vector_data: Default::default(),
        payload_storage_type: Default::default(),
    };
    let src_dir = Builder::new().prefix("segment_src").tempdir().unwrap();
    let dst_dir = Builder::new().prefix("segment_dst").tempdir().unwrap();
    let (mut src, _) = build_segment(src_dir.path(), &config, None, true).unwrap();
    let (mut dst, _) = build_segment(dst_dir.path(), &config, None, true).unwrap();
    let hw_counter = HardwareCounterCell::new();

    let vec: Vec<f32> = (0..dim).map(|i| (i as f32).sin()).collect();
    src.upsert_point(100, 7.into(), only_default_vector(&vec), &hw_counter)
        .unwrap();
    let bytes = retrieve_raw_vector(&src, 7.into(), DEFAULT_VECTOR_NAME);

    dst.upsert_point_raw(
        100,
        7.into(),
        &[(DEFAULT_VECTOR_NAME.to_owned(), bytes.clone())],
        &hw_counter,
    )
    .unwrap();

    // Encoded bytes survive verbatim — no requantization round-trip.
    assert_eq!(
        retrieve_raw_vector(&dst, 7.into(), DEFAULT_VECTOR_NAME),
        bytes
    );
    // And both segments dequantize to exactly the same vector.
    assert_eq!(
        dst.vector(DEFAULT_VECTOR_NAME, 7.into(), &hw_counter)
            .unwrap(),
        src.vector(DEFAULT_VECTOR_NAME, 7.into(), &hw_counter)
            .unwrap(),
    );
}

/// TurboQuant multivector raw round-trip: the concatenated encoded inner
/// records must be ingested verbatim.
#[test]
fn test_upsert_raw_multivec_turbo_bytes() {
    init_logger();
    let dim = 64;
    let config = SegmentConfig {
        vector_data: HashMap::from([(
            DEFAULT_VECTOR_NAME.to_owned(),
            VectorDataConfig {
                size: dim,
                distance: Distance::Dot,
                storage_type: VectorStorageType::ChunkedMmap,
                index: Indexes::Plain {},
                quantization_config: None,
                multivector_config: Some(MultiVectorConfig::default()),
                datatype: Some(VectorStorageDatatype::Turbo4),
            },
        )]),
        sparse_vector_data: Default::default(),
        payload_storage_type: Default::default(),
    };
    let src_dir = Builder::new().prefix("segment_src").tempdir().unwrap();
    let dst_dir = Builder::new().prefix("segment_dst").tempdir().unwrap();
    let (mut src, _) = build_segment(src_dir.path(), &config, None, true).unwrap();
    let (mut dst, _) = build_segment(dst_dir.path(), &config, None, true).unwrap();
    let hw_counter = HardwareCounterCell::new();

    // Three inner vectors of `dim` elements each, flattened.
    let flattened: Vec<f32> = (0..3 * dim).map(|i| (i as f32).cos()).collect();
    let multi_vec = MultiDenseVectorInternal::new(flattened, dim);
    src.upsert_point(
        100,
        4.into(),
        only_default_multi_vector(&multi_vec),
        &hw_counter,
    )
    .unwrap();
    let bytes = retrieve_raw_vector(&src, 4.into(), DEFAULT_VECTOR_NAME);

    dst.upsert_point_raw(
        100,
        4.into(),
        &[(DEFAULT_VECTOR_NAME.to_owned(), bytes.clone())],
        &hw_counter,
    )
    .unwrap();

    assert_eq!(
        retrieve_raw_vector(&dst, 4.into(), DEFAULT_VECTOR_NAME),
        bytes
    );
    assert_eq!(
        dst.vector(DEFAULT_VECTOR_NAME, 4.into(), &hw_counter)
            .unwrap(),
        src.vector(DEFAULT_VECTOR_NAME, 4.into(), &hw_counter)
            .unwrap(),
    );
}

/// TurboQuant-as-datatype vectors must survive append-only mutations that
/// don't touch them — e.g. payload-only ops — without degrading.
///
/// On an append-only segment every mutating op (except same-operation
/// follow-up steps, which mutate the just-written slot in place) routes
/// through `Segment::clone_and_mutate_point`, which rewrites the point at a
/// fresh internal id. It used to snapshot the point's vectors decoded to `f32`,
/// dequantizing and requantizing a TQ-datatype vector on every mutation,
/// even one that only touched the payload. TurboQuant requantization is not
/// idempotent — for Dot/L2 the stored per-vector scale factor picks up the
/// centroid-norm bias on every cycle — so each op degraded the vector a
/// little further beyond the initial (expected, one-off) quantization loss.
/// Vectors the op doesn't overwrite now travel to the fresh id as
/// storage-native bytes, verbatim; this test guards that.
///
/// This is the segment-level analogue of the segment-holder test
/// `test_cow_move_does_not_degrade_turbo_vectors` (CoW moves between
/// segments); here the clone-and-tombstone cycle happens within one segment.
///
/// The test applies repeated payload-only ops to one point on an append-only
/// TQ segment and asserts the decoded vector never drifts from its
/// first-generation value (the read-back right after initial ingestion).
#[test]
fn test_append_only_mutate_does_not_degrade_turbo_vectors() {
    init_logger();
    const DIM: usize = 128;
    const ROUNDS: u64 = 32;

    let config = SegmentConfig {
        vector_data: HashMap::from([(
            DEFAULT_VECTOR_NAME.to_owned(),
            VectorDataConfig {
                size: DIM,
                distance: Distance::Dot,
                storage_type: VectorStorageType::ChunkedMmap,
                index: Indexes::Plain {},
                quantization_config: None,
                multivector_config: None,
                datatype: Some(VectorStorageDatatype::Turbo4),
            },
        )]),
        sparse_vector_data: Default::default(),
        payload_storage_type: Default::default(),
    };
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let (mut segment, _) = build_segment(dir.path(), &config, None, true).unwrap();
    segment.append_only_mutations = true;

    let hw_counter = HardwareCounterCell::new();
    let point_id: PointIdType = 7.into();
    let original: Vec<f32> = (0..DIM).map(|i| (i as f32 * 0.37).sin()).collect();
    segment
        .upsert_point(100, point_id, only_default_vector(&original), &hw_counter)
        .unwrap();

    let read_dense = |segment: &Segment| -> Vec<f32> {
        match segment
            .vector(DEFAULT_VECTOR_NAME, point_id, &hw_counter)
            .unwrap()
            .unwrap()
        {
            VectorInternal::Dense(vector) => vector,
            VectorInternal::Sparse(_) | VectorInternal::MultiDense(_) => {
                panic!("expected a dense vector")
            }
        }
    };

    // First-generation read-back: the original vector after its initial
    // (expected, one-off) quantization. Payload ops must preserve it exactly.
    let first_generation = read_dense(&segment);

    let payload: Payload = serde_json::from_str(r#"{"color": "red"}"#).unwrap();

    // L2 distance of each round's read-back from the first generation.
    let mut drift = Vec::new();
    for round in 0..ROUNDS {
        let old_internal_id = segment
            .with_view(|v| v.lookup_internal_id(point_id, DeferredBehavior::VisibleOnly))
            .unwrap();
        segment
            .set_payload(101 + round, point_id, &payload, &None, &hw_counter)
            .unwrap();

        // The op must have taken the append-only clone-and-tombstone path
        // (fresh internal id), not the in-place path.
        let new_internal_id = segment
            .with_view(|v| v.lookup_internal_id(point_id, DeferredBehavior::VisibleOnly))
            .unwrap();
        assert_ne!(old_internal_id, new_internal_id);

        let read_back = read_dense(&segment);
        drift.push(
            first_generation
                .iter()
                .zip(&read_back)
                .map(|(&a, &b)| (a - b).powi(2))
                .sum::<f32>()
                .sqrt(),
        );
    }

    assert!(
        drift.iter().all(|&distance| distance == 0.0),
        "TurboQuant vector degraded across append-only payload ops; L2 \
         distance from the first-generation read-back after each op: {drift:?}",
    );
}

/// Tests segment functions to ensure invalid requests do error
#[test]
fn test_vector_compatibility_checks() {
    init_logger();
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();

    let mut segment = build_segment_with_two_named_vecs(dir.path(), 4, 2, Distance::Dot).unwrap();

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
    let internal_id = segment
        .with_view(|v| v.lookup_internal_id(point_id, DeferredBehavior::VisibleOnly))
        .unwrap();

    // A set of broken vectors
    let wrong_vectors_single = [
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
    let wrong_vectors_multi = [
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
    let wrong_names = ["aa", "bb", ""];

    for (vector_name, vector) in &wrong_vectors_single {
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
            .with_view(|v| v.vector_by_offset(wrong_name, internal_id, &hw_counter))
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
    init_logger();
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

fn create_deferred_segment(
    dir: &TempDir,
    dim: usize,
    n_vectors: usize,
    n_deferred: usize,
) -> Segment {
    let hw_counter = HardwareCounterCell::new();

    let deferred_internal_id = (n_deferred > 0).then_some(n_vectors as PointOffsetType);

    let total_vectors = n_vectors + n_deferred;
    let (mut segment, _) = build_segment(
        dir.path(),
        &SegmentConfig {
            vector_data: HashMap::from([(
                DEFAULT_VECTOR_NAME.to_owned(),
                VectorDataConfig {
                    size: dim,
                    distance: Distance::Dot,
                    storage_type: VectorStorageType::default(),
                    index: Indexes::Plain {},
                    quantization_config: None,
                    multivector_config: None,
                    datatype: None,
                },
            )]),
            sparse_vector_data: HashMap::from_iter([
                (
                    "sparse".to_string(),
                    SparseVectorDataConfig {
                        // Don't do full scan unless explicitly enabled so we cover more parts with our tests.
                        index: SparseIndexConfig::new(
                            Some(1),
                            SparseIndexType::MutableRam,
                            None,
                            None,
                        ),
                        storage_type: SparseVectorStorageType::Mmap,
                        modifier: None,
                    },
                ),
                (
                    "sparse-plain".to_string(),
                    SparseVectorDataConfig {
                        index: SparseIndexConfig::new(
                            Some(usize::MAX),
                            SparseIndexType::MutableRam,
                            None,
                            None,
                        ),
                        storage_type: SparseVectorStorageType::Mmap,
                        modifier: None,
                    },
                ),
            ]),
            payload_storage_type: Default::default(),
        },
        deferred_internal_id,
        true,
    )
    .unwrap();

    // Initially, no deferred points (empty segment)
    assert!(!segment.has_deferred_points());
    for i in 0..n_vectors {
        assert!(
            !segment.point_is_deferred(PointIdType::from(i as u64)),
            "Point {i} should not be deferred in an empty segment"
        );
    }

    let default_vector: Vec<_> = (0..dim).map(|i| i as f32 / 10.0).collect();

    let mut op_num_counter = 0u64;

    let mut rng = StdRng::seed_from_u64(41);

    let mut vectors = NamedVectors::from_ref(DEFAULT_VECTOR_NAME, VectorRef::from(&default_vector));
    for sparse_name in ["sparse", "sparse-plain"] {
        vectors.insert(
            sparse_name.to_string(),
            VectorInternal::Sparse(
                SparseVector::new(
                    vec![0, 42, 120, 420, 512, 1000, 1024, 4200],
                    vec![0.1, 0.42, 0.9, 1.1, 3.9, 0.21, 1.28, 0.99],
                )
                .unwrap(),
            ),
        );
    }

    for i in 1..=total_vectors {
        let point_id = PointIdType::from(i as u64);
        segment
            .insert_new_vectors(point_id, op_num_counter, &vectors, &hw_counter)
            .unwrap();
        op_num_counter += 1;

        let mut payload = Payload::default();

        let color_payload: Value = ["red", "blue", "yellow"][i % 3].to_string().into();
        payload
            .0
            .insert("color-indexed".to_string(), color_payload.clone());
        payload.0.insert("color".to_string(), color_payload);
        payload.0.insert(
            "number".to_string(),
            Value::Number(Number::from_u128(rng.random_range(0..2)).unwrap()),
        );

        let is_deferred = i > n_vectors;
        payload
            .0
            .insert("is-deferred".to_string(), is_deferred.into());

        segment
            .set_full_payload(op_num_counter, point_id, &payload, &hw_counter)
            .unwrap();
        op_num_counter += 1;
    }

    segment
        .create_field_index(
            op_num_counter,
            &JsonPath::new("color-indexed"),
            Some(&PayloadFieldSchema::FieldType(PayloadSchemaType::Keyword)),
            &hw_counter,
        )
        .unwrap();

    op_num_counter += 1;

    segment
        .create_field_index(
            op_num_counter,
            &JsonPath::new("number"),
            Some(&PayloadFieldSchema::FieldType(PayloadSchemaType::Integer)),
            &hw_counter,
        )
        .unwrap();
    op_num_counter += 1;

    segment
        .create_field_index(
            op_num_counter,
            &JsonPath::new("is-deferred"),
            Some(&PayloadFieldSchema::FieldType(PayloadSchemaType::Bool)),
            &hw_counter,
        )
        .unwrap();

    // Now we should have deferred points
    assert_eq!(segment.has_deferred_points(), n_deferred > 0);
    if n_deferred > 0 {
        assert_eq!(
            segment.id_tracker.borrow().deferred_internal_id(),
            Some(n_vectors as u32)
        );
    }

    // Points 1 to n_vectors should NOT be deferred
    for i in 1..=n_vectors {
        assert!(
            !segment.point_is_deferred(PointIdType::from(i as u64)),
            "Point {i} should not be deferred"
        );
        // Check the `is-deferred` payload is correct.
        let is_deferred_payload = segment
            .with_view(|v| v.payload_by_offset(i as u32 - 1, &hw_counter))
            .unwrap()
            .get_value(&JsonPath::new("is-deferred"))[0]
            .as_bool()
            .unwrap();
        assert!(!is_deferred_payload);
    }

    for i in (n_vectors + 1)..=total_vectors {
        let point_id = PointIdType::from(i as u64);
        assert!(
            segment.point_is_deferred(point_id),
            "Point {i} should be deferred"
        );

        // Check the `is-deferred` payload is correct.
        let is_deferred_payload = segment
            .with_view(|v| v.payload_by_offset(i as u32 - 1, &hw_counter))
            .unwrap()
            .get_value(&JsonPath::new("is-deferred"))[0]
            .as_bool()
            .unwrap();
        assert!(is_deferred_payload);
    }

    // Non-existent point should be non deferred
    assert!(
        !segment.point_is_deferred(PointIdType::from(total_vectors as u64 + 1)),
        "Non-existent point should not be deferred"
    );

    // Test deferred point count estimation.
    assert_eq!(segment.deferred_point_count(), n_deferred);
    assert_eq!(segment.available_point_count_without_deferred(), n_vectors);

    segment
}

#[test]
fn test_dense_deferred_points() {
    init_logger();
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let dim = 4;

    let mut segment = create_deferred_segment(&dir, dim, 13, 7);

    // Points 1-13 (internal_ids 0-12) should NOT be deferred
    for i in 1..=13 {
        assert!(
            !segment.point_is_deferred(PointIdType::from(i as u64)),
            "Point {i} should not be deferred"
        );
    }

    // Points 14+ (internal_ids 13+) should be deferred
    for i in 14..=20 {
        assert!(
            segment.point_is_deferred(PointIdType::from(i as u64)),
            "Point {i} should be deferred"
        );
    }

    // Point 100 (non-existent) should not be deferred
    assert!(
        !segment.point_is_deferred(PointIdType::from(100)),
        "Non-existent point should not be deferred"
    );

    // Set version so flush actually persists data (insert_new_vectors doesn't set it)
    segment.version = Some(20);

    // Close segment
    segment.flush(true).unwrap();
    let path = segment.segment_path.clone();

    drop(segment);

    // Reopen segment to ensure deferred points are loaded correctly from disk
    let segment = load_segment(&path, Uuid::nil(), Some(13), &AtomicBool::new(false)).unwrap();

    // Deferred points should still be the same after reopening
    assert!(
        segment.has_deferred_points(),
        "Segment should still have deferred points after reopening"
    );
    assert_eq!(
        segment.id_tracker.borrow().deferred_internal_id(),
        Some(13),
        "Deferred internal ID should still be `DEFERRED_POINTS_ID` after reopening"
    );
}

#[test]
#[cfg_attr(target_os = "windows", ignore = "slow on Windows, not OS-specific")]
fn test_dense_deferred_point_segment_combinations() {
    init_logger();

    // Reduce test matrix on Windows where segment creation is extremely slow due to IO.
    #[cfg(target_os = "windows")]
    let dims = [2, 50];
    #[cfg(not(target_os = "windows"))]
    let dims = [2, 17, 50];

    #[cfg(target_os = "windows")]
    let deferred_counts = [0, 1, 14];
    #[cfg(not(target_os = "windows"))]
    let deferred_counts = [0, 1, 5, 14];

    for dim in dims {
        for n_deferred in deferred_counts {
            for n_vectors in [1, 5, 14] {
                let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
                create_deferred_segment(&dir, dim, n_vectors, n_deferred);
            }
        }
    }
}

const N_POINTS: usize = 12;

#[test]
fn test_deferred_point_estimation_with_filter() {
    init_logger();
    let hw_counter = HardwareCounterCell::new();

    let filter = Filter::new_must(Condition::Field(FieldCondition::new_match(
        JsonPath::new("color"),
        Match::new_value(ValueVariants::String("blue".to_string())),
    )));

    // On Windows, reduce iteration count since each segment creation is very IO-heavy.
    #[cfg(target_os = "windows")]
    let deferred_counts: &[usize] = &[0, 10, 100];
    #[cfg(not(target_os = "windows"))]
    let deferred_counts: &[usize] = &[0, 3, 10, 20, 100];

    for &n_deferred in deferred_counts {
        let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
        let segment = create_deferred_segment(&dir, 5, N_POINTS, n_deferred);

        let estimation = segment
            .estimate_point_count(Some(&filter), &hw_counter)
            .unwrap();

        // We test with different amount of deferred points (including no deferred points) and expect the
        // cardinality to not change.
        assert_eq!(estimation.exp, 6);
        assert_eq!(estimation.max, 12);

        // For consistency we also test that the same cardinality is estimated if no deferred points exist.
        if n_deferred == 0 {
            assert_eq!(segment.id_tracker.borrow().deferred_internal_id(), None);
            let estimation = segment
                .estimate_point_count(Some(&filter), &hw_counter)
                .unwrap();
            assert_eq!(estimation.exp, 6);
            assert_eq!(estimation.max, 12);
        }
    }
}

#[test]
#[cfg_attr(target_os = "windows", ignore = "slow on Windows, not OS-specific")]
fn test_deferred_point_read_operations() {
    init_logger();
    let hw_counter = HardwareCounterCell::new();

    // Search
    assert_deferred_points_excluded(
        "Search",
        |segment, filter| {
            segment
                .search(
                    DEFAULT_VECTOR_NAME,
                    &QueryVector::Nearest(VectorInternal::Dense(vec![0.1, 0.1, 0.2, 0.2, 0.3])),
                    &WithPayload::default(),
                    &WithVector::Bool(false),
                    filter,
                    usize::MAX,
                    None,
                )
                .unwrap()
        },
        |i| i.id,
        true,
        false,
    );

    // Read filtered (count API)
    assert_deferred_points_excluded(
        "Read Filtered",
        |segment, filter| {
            segment
                .read_filtered(
                    None,
                    None,
                    filter,
                    &AtomicBool::new(false),
                    &hw_counter,
                    DeferredBehavior::VisibleOnly,
                )
                .unwrap()
        },
        |i| *i,
        true,
        false,
    );

    // Read filtered ordered (scroll)
    assert_deferred_points_excluded(
        "Read ordered filtered",
        |segment, filter| {
            segment
                .read_ordered_filtered(
                    None,
                    filter,
                    &OrderBy {
                        key: JsonPath::new("number"),
                        direction: None,
                        start_from: None,
                    },
                    &AtomicBool::new(false),
                    &hw_counter,
                    DeferredBehavior::VisibleOnly,
                )
                .unwrap()
        },
        |i| i.1,
        true,
        false,
    );

    // Read random filtered (random scroll)
    assert_deferred_points_excluded(
        "Read random filtered",
        |segment, filter| {
            segment
                .read_random_filtered(500, filter, &AtomicBool::new(false), &hw_counter)
                .unwrap()
        },
        |i| *i,
        true,
        false,
    );

    // Retrieve API
    assert_deferred_points_excluded(
        "Retrieve",
        |segment, _| {
            let point_ids: Vec<_> = (1..=segment.available_point_count())
                .map(|i| ExtendedPointId::NumId(i as u64))
                .collect();

            segment
                .retrieve(
                    &point_ids,
                    &WithPayload::default(),
                    &WithVector::Bool(false),
                    &hw_counter,
                    &AtomicBool::new(false),
                    DeferredBehavior::VisibleOnly,
                )
                .unwrap()
                .into_iter()
                .map(|i| i.0)
                .collect::<Vec<_>>()
        },
        |i| *i,
        false,
        false,
    );
}

/// A deferred point is invisible to ordinary (`VisibleOnly`) per-point reads,
/// but the copy-on-write move path must still be able to read its data via
/// `retrieve`/`retrieve_raw` with `DeferredBehavior::WithDeferred`.
///
/// Regression test for a spurious "No point with id ... found" error: the CoW
/// move path read the source point's payload with `VisibleOnly`, which raised
/// `PointIdError` for a deferred source point (e.g. a point upserted under
/// `prevent_unoptimized` whose internal id is beyond the deferred threshold).
#[test]
fn test_deferred_point_with_deferred_reads() {
    let hw_counter = HardwareCounterCell::new();
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let dim = 4;

    // Threshold so any new point lands beyond the cutoff (deferred).
    let (mut segment, _) = build_segment(
        dir.path(),
        &SegmentConfig {
            vector_data: HashMap::from([(
                DEFAULT_VECTOR_NAME.to_owned(),
                VectorDataConfig {
                    size: dim,
                    distance: Distance::Dot,
                    storage_type: VectorStorageType::default(),
                    index: Indexes::Plain {},
                    quantization_config: None,
                    multivector_config: None,
                    datatype: None,
                },
            )]),
            sparse_vector_data: Default::default(),
            payload_storage_type: Default::default(),
        },
        Some(0),
        true,
    )
    .unwrap();

    let point_id = PointIdType::from(42u64);
    let vector = vec![0.1, 0.2, 0.3, 0.4];
    let vectors = NamedVectors::from_ref(DEFAULT_VECTOR_NAME, VectorRef::from(&vector));
    let payload: Payload = serde_json::from_str(r#"{"number": 7}"#).unwrap();

    segment
        .upsert_point(0, point_id, vectors, &hw_counter)
        .unwrap();
    segment
        .set_full_payload(0, point_id, &payload, &hw_counter)
        .unwrap();

    assert!(
        segment.point_is_deferred(point_id),
        "point must be deferred for this regression scenario",
    );

    // VisibleOnly hides the deferred point: both payload and vector reads
    // fail to resolve it (this is exactly what made the CoW move path raise a
    // spurious PointNotFound before the fix).
    assert!(
        matches!(
            segment.payload(point_id, &hw_counter),
            Err(PointIdError { missed_point_id }) if missed_point_id == point_id,
        ),
        "VisibleOnly payload read must not resolve a deferred point",
    );
    assert!(
        matches!(
            segment.all_vectors(point_id, &hw_counter),
            Err(PointIdError { missed_point_id }) if missed_point_id == point_id,
        ),
        "VisibleOnly vector read must not resolve a deferred point",
    );

    // WithDeferred resolves the deferred point with its real data — this is
    // what the CoW move path uses (via `retrieve_raw`), fixing the spurious
    // PointNotFound.
    let records = segment
        .retrieve(
            &[point_id],
            &WithPayload {
                enable: true,
                payload_selector: None,
            },
            &WithVector::Bool(true),
            &hw_counter,
            &AtomicBool::new(false),
            DeferredBehavior::WithDeferred,
        )
        .unwrap();
    let record = records
        .get(&point_id)
        .expect("WithDeferred retrieve must resolve a deferred point");
    assert_eq!(
        record.payload.clone().unwrap_or_default(),
        payload,
        "WithDeferred retrieve must return the deferred point's payload",
    );
    assert!(
        record
            .vectors
            .as_ref()
            .expect("vectors were requested")
            .iter()
            .any(|(name, _)| name == DEFAULT_VECTOR_NAME),
        "WithDeferred retrieve must return the deferred point's vector",
    );
}

#[test]
#[cfg_attr(target_os = "windows", ignore = "slow on Windows, not OS-specific")]
fn test_deferred_point_sparse() {
    init_logger();

    let sparse_query_vec = VectorInternal::Sparse(
        SparseVector::new(
            vec![42, 512, 600, 1023, 1024],
            vec![0.1, 0.42, 0.1, 1.0, 100.0],
        )
        .unwrap(),
    );
    let sparse_nearest_vec = QueryVector::Nearest(sparse_query_vec.clone());

    let sparse_feedback_query = QueryVector::FeedbackNaive(NaiveFeedbackQuery {
        target: sparse_query_vec.clone(),
        feedback: vec![FeedbackItem {
            vector: sparse_query_vec.clone(),
            score: OrderedFloat(1.0),
        }],
        coefficients: NaiveFeedbackCoefficients {
            a: OrderedFloat(1.0),
            b: OrderedFloat(1.1),
            c: OrderedFloat(1.2),
        },
    });

    for sparse_vec_name in ["sparse", "sparse-plain"] {
        for exact in [false, true] {
            // Search nearest
            assert_deferred_points_excluded(
                &format!("Sparse search nearest; exact={exact}; sparse_vec_name={sparse_vec_name}"),
                |segment, filter| {
                    segment
                        .search(
                            sparse_vec_name,
                            &sparse_nearest_vec,
                            &WithPayload::default(),
                            &WithVector::Bool(false),
                            filter,
                            500,
                            Some(&SearchParams {
                                exact,
                                ..Default::default()
                            }),
                        )
                        .unwrap()
                },
                |i| i.id,
                true,
                true,
            );

            // Search feedback
            assert_deferred_points_excluded(
                &format!(
                    "Sparse search feedback; exact={exact}; sparse_vec_name={sparse_vec_name}"
                ),
                |segment, filter| {
                    segment
                        .search(
                            sparse_vec_name,
                            &sparse_feedback_query,
                            &WithPayload::default(),
                            &WithVector::Bool(false),
                            filter,
                            500,
                            Some(&SearchParams {
                                exact,
                                ..Default::default()
                            }),
                        )
                        .unwrap()
                },
                |i| i.id,
                true,
                true,
            );
        }
    }
}

#[test]
#[cfg_attr(target_os = "windows", ignore = "slow on Windows, not OS-specific")]
fn test_deferred_point_facets() {
    init_logger();
    let hw_counter = HardwareCounterCell::new();

    let key = JsonPath::new("color-indexed");

    let filter_field = Filter::new_must(Condition::Field(FieldCondition::new_match(
        key.clone(),
        Match::new_value(ValueVariants::String("blue".to_string())),
    )));

    // On Windows, reduce iteration count since each segment creation is very IO-heavy.
    #[cfg(target_os = "windows")]
    let deferred_counts: &[usize] = &[0, 10];
    #[cfg(not(target_os = "windows"))]
    let deferred_counts: &[usize] = &[0, 1, 10, 300];

    for &n_deferred in deferred_counts {
        // Test both exact and estimated.
        for exact in [false, true] {
            for filter in [None, Some(&filter_field)] {
                log::debug!(
                    "  => deferred points = {n_deferred}. Exact={exact}. Filter={}",
                    filter.is_some()
                );

                let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
                let segment = create_deferred_segment(&dir, 5, N_POINTS, n_deferred);

                let request = FacetParams {
                    key: key.clone(),
                    limit: 1000, // High limit to include all points.
                    filter: filter.cloned(),
                    exact,
                };

                let facet_res_deferred = segment
                    .facet(&request, &AtomicBool::new(false), &hw_counter)
                    .unwrap();

                // Compare against the same point set without deferred mode by
                // rebuilding the segment with `deferred_internal_id = None`.
                let no_deferred_dir = Builder::new()
                    .prefix("segment_dir_no_deferred")
                    .tempdir()
                    .unwrap();
                let no_deferred_segment =
                    create_deferred_segment(&no_deferred_dir, 5, N_POINTS + n_deferred, 0);
                let facet_res = no_deferred_segment
                    .facet(&request, &AtomicBool::new(false), &hw_counter)
                    .unwrap();

                let expected_deferred = if filter.is_some() {
                    n_deferred.div_ceil(3)
                } else {
                    n_deferred
                };

                if n_deferred == 0 {
                    assert_eq!(facet_res_deferred, facet_res);
                } else {
                    let facet_res_sum: usize = facet_res.values().sum();
                    let facet_res_deferred_sum: usize = facet_res_deferred.values().sum();
                    assert_eq!(facet_res_sum, facet_res_deferred_sum + expected_deferred);
                    assert!(facet_res_deferred.values().all(|&i| i > 0));
                }

                // Test that `unique_values()` excludes values from deferred points,
                // if there is no *visible* point with that value.
                let is_deferred_values = segment
                    .unique_values(
                        &JsonPath::new("is-deferred"),
                        filter,
                        &AtomicBool::new(false),
                        &hw_counter,
                    )
                    .unwrap()
                    .into_iter()
                    .map(|i| match i {
                        FacetValue::Bool(b) => b,
                        FacetValue::Keyword(_) | FacetValue::Int(_) | FacetValue::Uuid(_) => {
                            unreachable!()
                        }
                    })
                    .collect::<Vec<_>>();

                // Only deferred points have a `true` value. Therefore they must be excluded in `unique_values`.
                assert_eq!(is_deferred_values, vec![false]);
            }
        }
    }
}

/// Extensively tests whether deferred points are excluded from the result of the given `operation`.
fn assert_deferred_points_excluded<F, R, T>(
    name: &str,
    operation: F,
    to_external_id: R,
    test_with_filter: bool,
    need_rebuilt_segment: bool,
) where
    F: Fn(&Segment, Option<&Filter>) -> Vec<T>,
    R: Fn(&T) -> ExtendedPointId,
{
    init_logger();

    /// Helper to craft more complex cases where we also can use filters.
    struct FilterSet {
        // Optional filter to be applied to the operation.
        filter: Option<Filter>,

        // How many visible points we expect to be selected by the filter.
        expected_visible: usize,

        // How many deferred points we expect to be selected by the filter.
        expected_deferred: Box<dyn Fn(usize) -> usize>,
    }

    let filter_field = Filter::new_must(Condition::Field(FieldCondition::new_match(
        JsonPath::new("color"),
        Match::new_value(ValueVariants::String("blue".to_string())),
    )));

    let filter_ids = Filter::new_must(Condition::HasId(HasIdCondition {
        has_id: MaybeArc::NoArc(AHashSet::from_iter([
            // Normally visible points.
            ExtendedPointId::NumId(1),
            ExtendedPointId::NumId(3),
            ExtendedPointId::NumId(2),
            // Deferred point.
            ExtendedPointId::NumId(13),
        ])),
    }));

    // Case 1: No filter, we expect all points to be available and deferred.
    let filter_case_1 = FilterSet {
        filter: None,
        expected_visible: N_POINTS,
        expected_deferred: Box::new(|n_deferred| n_deferred),
    };

    // Case 2: Simple field condition. We expect 1/3 of the points being selected since the field filters 1/3 of the points.
    let filter_case_2 = FilterSet {
        filter: Some(filter_field),
        expected_visible: N_POINTS.div_ceil(3),
        expected_deferred: Box::new(|n_deferred| n_deferred.div_ceil(3)),
    };

    // Case 3: Filter by ID condition, which has 3 visible points and 1 deferred point.
    let filter_case_3 = FilterSet {
        filter: Some(filter_ids),
        expected_visible: 3,
        expected_deferred: Box::new(|n_deferred| if n_deferred > 0 { 1 } else { 0 }),
    };

    log::debug!("Testing deferred read operation: {name:?}");

    let set_of_filters = if test_with_filter {
        vec![&filter_case_1, &filter_case_2, &filter_case_3]
    } else {
        vec![&filter_case_1]
    };

    // On Windows, reduce iteration count since each segment creation is very IO-heavy.
    #[cfg(target_os = "windows")]
    let deferred_counts: &[usize] = &[0, 10];
    #[cfg(not(target_os = "windows"))]
    let deferred_counts: &[usize] = &[0, 1, 10, 300];

    for &n_deferred in deferred_counts {
        // Test with different types of filters.
        for (filter_set_id, filter_set) in set_of_filters.iter().enumerate() {
            log::debug!("  => deferred points = {n_deferred}; filter-set ID = {filter_set_id}",);

            let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
            let segment = create_deferred_segment(&dir, 5, N_POINTS, n_deferred);

            // Search with deferred mode
            let search_res_deferred = operation(&segment, filter_set.filter.as_ref());
            assert!(!search_res_deferred.is_empty());
            assert_eq!(search_res_deferred.len(), filter_set.expected_visible);

            // All points in result must always be non deferred.
            for id_t in &search_res_deferred {
                let external_id = to_external_id(id_t);
                assert!(!segment.point_is_deferred(external_id));
            }

            // Disable deferred points and search again.
            // On Windows segment creation is extremely IO-heavy; skip the rebuild path
            // for tests where it noticeably slows the suite down.
            if need_rebuilt_segment && cfg!(target_os = "windows") {
                drop(segment);
                dir.close().unwrap();
                continue;
            }

            // Deferred state is owned by the id tracker and only set at segment
            // construction time, so we rebuild a fresh non-deferred segment with the
            // same total point count to compare against.
            let no_deferred_dir = Builder::new()
                .prefix("segment_dir_no_deferred")
                .tempdir()
                .unwrap();
            let no_deferred_segment =
                create_deferred_segment(&no_deferred_dir, 5, N_POINTS + n_deferred, 0);

            let search_res_normal = operation(&no_deferred_segment, filter_set.filter.as_ref());
            assert_eq!(
                search_res_normal.len(),
                filter_set.expected_visible + (filter_set.expected_deferred)(n_deferred)
            );

            drop(segment);
            drop(no_deferred_segment);
            dir.close().unwrap();
            no_deferred_dir.close().unwrap();
        }
    }
}

#[test]
fn test_deleted_deferred_point_count() {
    let hw_counter = HardwareCounterCell::new();

    // On Windows, reduce iteration count since each segment creation is very IO-heavy.
    #[cfg(target_os = "windows")]
    let deferred_counts: &[usize] = &[0, 10];
    #[cfg(not(target_os = "windows"))]
    let deferred_counts: &[usize] = &[0, 1, 10, 300];

    for &n_deferred in deferred_counts {
        let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
        let mut segment = create_deferred_segment(&dir, 5, N_POINTS, n_deferred);

        assert_eq!(segment.deferred_point_count(), n_deferred);

        if n_deferred == 0 {
            continue;
        }

        assert_eq!(segment.available_point_count_without_deferred(), N_POINTS);

        for d in 0..n_deferred {
            let delete_id = segment.id_tracker.borrow().deferred_internal_id().unwrap() + d as u32;
            segment
                .delete_point_internal(delete_id, &hw_counter)
                .unwrap();

            let deleted_count = d + 1; // The first index is 0 but this point is deleted, so count must be 1.
            assert_eq!(
                segment.deferred_point_count(),
                n_deferred.checked_sub(deleted_count).unwrap()
            );
            assert_eq!(
                segment.id_tracker.borrow().deferred_deleted_count(),
                deleted_count,
            );

            // Do the operation twice to test that we don't double count the same point.
            segment
                .delete_point_internal(delete_id, &hw_counter)
                .unwrap();

            assert_eq!(
                segment.deferred_point_count(),
                n_deferred.checked_sub(deleted_count).unwrap()
            );

            assert_eq!(
                segment.id_tracker.borrow().deferred_deleted_count(),
                deleted_count
            );

            assert_eq!(segment.available_point_count_without_deferred(), N_POINTS);
        }

        // We delete all deferred points in the segment.
        assert_eq!(segment.deferred_point_count(), 0);
        assert_eq!(
            segment.id_tracker.borrow().deferred_deleted_count(),
            n_deferred
        );
        assert_eq!(segment.available_point_count_without_deferred(), N_POINTS);
    }
}

/// A field index dropped between flusher capture and execution (a `DropIndex`
/// racing the background flush) must not abort the rest of the flush sequence.
///
/// Aborting used to leave the field indexes flushed before the cancellation
/// point durably ahead of payload storage and point versions; WAL replay then
/// re-derived filter-based operations through that too-new index and silently
/// skipped points whose payload still needed the operation re-applied.
#[test]
fn test_flush_survives_concurrent_field_index_drop() {
    init_logger();
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let mut segment = build_simple_segment(dir.path(), 4, Distance::Dot).unwrap();
    let hw_counter = HardwareCounterCell::new();

    segment
        .upsert_point(
            1,
            1.into(),
            only_default_vector(&[1.0, 0.0, 0.0, 0.0]),
            &hw_counter,
        )
        .unwrap();
    let payload: Payload = serde_json::from_str(r#"{"num": 20}"#).unwrap();
    segment
        .set_full_payload(2, 1.into(), &payload, &hw_counter)
        .unwrap();
    segment
        .create_field_index(
            3,
            &JsonPath::new("num"),
            Some(&PayloadFieldSchema::FieldType(PayloadSchemaType::Integer)),
            &hw_counter,
        )
        .unwrap();

    // Capture the flushers (as the background flush thread does), then drop the
    // index before executing them.
    let flusher = segment
        .flusher(true)
        .expect("segment has unflushed changes");
    segment
        .delete_field_index(4, &JsonPath::new("num"))
        .unwrap();

    flusher().expect("flush must not fail");

    // The flush must skip the dropped index storage and still complete the
    // sequence: payload storage, point versions, and the segment state captured
    // at version 3. A cancelled (aborted) flush would have left the persisted
    // version at 0.
    assert_eq!(segment.persistent_version(), 3);
}
