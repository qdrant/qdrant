use std::collections::HashMap;
use std::sync::atomic::AtomicBool;

use common::counter::hardware_counter::HardwareCounterCell;
use sparse::common::sparse_vector::SparseVector;
use tempfile::Builder;

use crate::data_types::named_vectors::NamedVectors;
use crate::data_types::vector_name_config::{
    DenseVectorConfig, SparseVectorConfig, VectorNameConfig,
};
use crate::data_types::vectors::DEFAULT_VECTOR_NAME;
use crate::entry::entry_point::{
    NonAppendableSegmentEntry as _, ReadSegmentEntry as _, SegmentEntry as _,
    StorageSegmentEntry as _,
};
use crate::segment::Segment;
use crate::segment_constructor::segment_builder::SegmentBuilder;
use crate::segment_constructor::{build_segment, load_segment};
use crate::types::{
    Distance, HnswGlobalConfig, Indexes, SegmentConfig, VectorDataConfig, VectorStorageType,
};
use crate::vector_storage::VectorStorageRead as _;

const DIM: usize = 4;
const NUM_POINTS: usize = 5;

fn default_dense_config(dim: usize) -> VectorDataConfig {
    VectorDataConfig {
        size: dim,
        distance: Distance::Dot,
        storage_type: VectorStorageType::default(),
        index: Indexes::Plain {},
        quantization_config: None,
        multivector_config: None,
        datatype: None,
    }
}

fn mmap_dense_config(dim: usize) -> VectorDataConfig {
    VectorDataConfig {
        storage_type: VectorStorageType::Mmap,
        ..default_dense_config(dim)
    }
}

fn dense_vector_name_config(dim: usize) -> VectorNameConfig {
    VectorNameConfig::dense(DenseVectorConfig {
        size: dim,
        distance: Distance::Dot,
        multivector_config: None,
        datatype: None,
    })
}

fn sparse_vector_name_config() -> VectorNameConfig {
    VectorNameConfig::sparse(SparseVectorConfig {
        modifier: None,
        datatype: None,
    })
}

fn hw() -> HardwareCounterCell {
    HardwareCounterCell::new()
}

/// Build an appendable segment with NUM_POINTS points on the default vector.
fn build_appendable_segment_with_data(path: &std::path::Path) -> Segment {
    let mut segment = build_segment(
        path,
        &SegmentConfig {
            vector_data: HashMap::from([(
                DEFAULT_VECTOR_NAME.to_owned(),
                default_dense_config(DIM),
            )]),
            sparse_vector_data: Default::default(),
            payload_storage_type: Default::default(),
        },
        None,
        true,
    )
    .unwrap();

    let hw = hw();
    for i in 0..NUM_POINTS {
        let vec = vec![(i + 1) as f32; DIM];
        let vectors = NamedVectors::from_ref(DEFAULT_VECTOR_NAME, vec.as_slice().into());
        segment
            .upsert_point((i + 1) as u64, (i as u64 + 1).into(), vectors, &hw)
            .unwrap();
    }

    assert_eq!(segment.available_point_count(), NUM_POINTS);
    segment
}

/// Build a proper immutable segment by optimizing an appendable segment through SegmentBuilder.
fn build_immutable_segment_with_data(
    segments_path: &std::path::Path,
    temp_path: &std::path::Path,
) -> Segment {
    let source = build_appendable_segment_with_data(segments_path);

    // Target config with Mmap storage -> non-appendable
    let target_config = SegmentConfig {
        vector_data: HashMap::from([(DEFAULT_VECTOR_NAME.to_owned(), mmap_dense_config(DIM))]),
        sparse_vector_data: Default::default(),
        payload_storage_type: Default::default(),
    };
    assert!(!target_config.is_appendable());

    let mut builder =
        SegmentBuilder::new(temp_path, &target_config, &HnswGlobalConfig::default()).unwrap();

    let stopped = AtomicBool::new(false);
    builder.update(&[&source], &stopped, &hw()).unwrap();

    let segment = builder.build_for_test(segments_path);
    assert!(!segment.appendable_flag);
    assert_eq!(segment.available_point_count(), NUM_POINTS);
    segment
}

// ---------------------------------------------------------------------------
// Creating dense vectors
// ---------------------------------------------------------------------------

#[test]
fn test_create_dense_vector_on_appendable_segment() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let mut segment = build_appendable_segment_with_data(dir.path());
    let hw = hw();

    assert_eq!(segment.available_point_count(), NUM_POINTS);

    // Create a new dense vector
    let new_dim = 8;
    let result = segment
        .create_vector_name(100, "v2", &dense_vector_name_config(new_dim))
        .unwrap();
    assert!(result);

    assert_eq!(segment.segment_config.vector_data["v2"].size, new_dim);
    assert!(segment.vector_data.contains_key("v2"));

    // Insert a new point with both vectors
    let mut vectors = NamedVectors::default();
    vectors.insert(DEFAULT_VECTOR_NAME.to_owned(), vec![9.0f32; DIM].into());
    vectors.insert("v2".to_owned(), vec![1.0f32; new_dim].into());
    segment
        .upsert_point(101, (NUM_POINTS as u64 + 1).into(), vectors, &hw)
        .unwrap();

    assert_eq!(segment.available_point_count(), NUM_POINTS + 1);

    // The new point has both vectors
    let all_vecs = segment
        .all_vectors((NUM_POINTS as u64 + 1).into(), &hw)
        .unwrap();
    assert!(all_vecs.contains_key(DEFAULT_VECTOR_NAME));
    assert!(all_vecs.contains_key("v2"));

    // Can read the new vector back
    let v2_vec = segment
        .vector("v2", (NUM_POINTS as u64 + 1).into(), &hw)
        .unwrap();
    assert!(v2_vec.is_some());
}

#[test]
fn test_create_sparse_vector_on_appendable_segment() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let mut segment = build_appendable_segment_with_data(dir.path());
    let hw = hw();

    let result = segment
        .create_vector_name(100, "sparse1", &sparse_vector_name_config())
        .unwrap();
    assert!(result);
    assert!(
        segment
            .segment_config
            .sparse_vector_data
            .contains_key("sparse1")
    );

    // Insert a point with sparse data
    let sparse_vec = SparseVector::new(vec![0, 2, 5], vec![1.0, 0.5, 0.3]).unwrap();
    let mut vectors = NamedVectors::default();
    vectors.insert(DEFAULT_VECTOR_NAME.to_owned(), vec![7.0f32; DIM].into());
    vectors.insert("sparse1".to_owned(), sparse_vec.into());
    segment
        .upsert_point(101, (NUM_POINTS as u64 + 1).into(), vectors, &hw)
        .unwrap();

    // Verify the sparse vector is retrievable
    let all_vecs = segment
        .all_vectors((NUM_POINTS as u64 + 1).into(), &hw)
        .unwrap();
    assert!(all_vecs.contains_key("sparse1"));
}

#[test]
fn test_create_dense_vector_on_immutable_segment() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let temp_dir = Builder::new().prefix("segment_temp").tempdir().unwrap();
    let mut segment = build_immutable_segment_with_data(dir.path(), temp_dir.path());
    let hw = hw();

    assert!(!segment.appendable_flag);
    assert_eq!(segment.available_point_count(), NUM_POINTS);

    // Create a new dense vector on immutable segment -> empty placeholder
    let new_dim = 8;
    let result = segment
        .create_vector_name(100, "v2", &dense_vector_name_config(new_dim))
        .unwrap();
    assert!(result);
    assert!(segment.segment_config.vector_data.contains_key("v2"));

    // Empty placeholder: all existing points have the vector deleted
    let storage = segment.vector_data["v2"].vector_storage.borrow();
    assert_eq!(storage.total_vector_count(), NUM_POINTS);
    assert_eq!(storage.available_vector_count(), 0);
    assert_eq!(storage.deleted_vector_count(), NUM_POINTS);
    drop(storage);

    // Existing points should not have the new vector
    for i in 1..=NUM_POINTS as u64 {
        let v2_vec = segment.vector("v2", i.into(), &hw).unwrap();
        assert!(v2_vec.is_none(), "point {i} should not have v2");
    }

    // Original default vector is still readable for all points
    for i in 1..=NUM_POINTS as u64 {
        let default_vec = segment.vector(DEFAULT_VECTOR_NAME, i.into(), &hw).unwrap();
        assert!(
            default_vec.is_some(),
            "point {i} should have default vector"
        );
    }
}

#[test]
fn test_create_sparse_vector_on_immutable_segment() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let temp_dir = Builder::new().prefix("segment_temp").tempdir().unwrap();
    let mut segment = build_immutable_segment_with_data(dir.path(), temp_dir.path());
    let hw = hw();

    assert!(!segment.appendable_flag);
    assert_eq!(segment.available_point_count(), NUM_POINTS);

    // Create a sparse vector on immutable segment with MutableRam config.
    // The implementation should upgrade to a non-appendable index type (Mmap).
    let result = segment
        .create_vector_name(100, "sp", &sparse_vector_name_config())
        .unwrap();
    assert!(result);
    assert!(segment.segment_config.sparse_vector_data.contains_key("sp"));

    // Verify the index type was upgraded from MutableRam to Mmap
    let stored_index_type = segment.segment_config.sparse_vector_data["sp"]
        .index
        .index_type;
    assert!(
        stored_index_type.is_immutable(),
        "expected immutable sparse index type on immutable segment, got {stored_index_type:?}"
    );

    // Empty placeholder: all existing points have the vector deleted
    let storage = segment.vector_data["sp"].vector_storage.borrow();
    assert_eq!(storage.total_vector_count(), NUM_POINTS);
    assert_eq!(storage.available_vector_count(), 0);
    assert_eq!(storage.deleted_vector_count(), NUM_POINTS);
    drop(storage);

    // Existing points should not have the sparse vector
    for i in 1..=NUM_POINTS as u64 {
        let sp_vec = segment.vector("sp", i.into(), &hw).unwrap();
        assert!(sp_vec.is_none(), "point {i} should not have sp");
    }

    // Original default vector is still readable
    for i in 1..=NUM_POINTS as u64 {
        let default_vec = segment.vector(DEFAULT_VECTOR_NAME, i.into(), &hw).unwrap();
        assert!(
            default_vec.is_some(),
            "point {i} should have default vector"
        );
    }
}

// ---------------------------------------------------------------------------
// Idempotency
// ---------------------------------------------------------------------------

#[test]
fn test_create_vector_idempotent() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let mut segment = build_appendable_segment_with_data(dir.path());

    let config = dense_vector_name_config(8);

    assert!(segment.create_vector_name(100, "v2", &config).unwrap());
    // Second call returns false (already exists)
    assert!(!segment.create_vector_name(101, "v2", &config).unwrap());
}

#[test]
fn test_delete_vector_idempotent() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let mut segment = build_appendable_segment_with_data(dir.path());

    assert!(!segment.delete_vector_name(100, "nonexistent").unwrap());
}

// ---------------------------------------------------------------------------
// Deleting named vectors
// ---------------------------------------------------------------------------

#[test]
fn test_delete_dense_vector_with_data() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let mut segment = build_appendable_segment_with_data(dir.path());
    let hw = hw();

    // Create vector and insert data
    let new_dim = 8;
    segment
        .create_vector_name(100, "to_delete", &dense_vector_name_config(new_dim))
        .unwrap();

    let mut vectors = NamedVectors::default();
    vectors.insert(DEFAULT_VECTOR_NAME.to_owned(), vec![5.0f32; DIM].into());
    vectors.insert("to_delete".to_owned(), vec![2.0f32; new_dim].into());
    segment
        .upsert_point(101, (NUM_POINTS as u64 + 1).into(), vectors, &hw)
        .unwrap();

    // Confirm the new vector is there
    let all_vecs = segment
        .all_vectors((NUM_POINTS as u64 + 1).into(), &hw)
        .unwrap();
    assert!(all_vecs.contains_key("to_delete"));

    // Delete the vector
    assert!(segment.delete_vector_name(102, "to_delete").unwrap());

    assert!(!segment.vector_data.contains_key("to_delete"));
    assert!(!segment.segment_config.vector_data.contains_key("to_delete"));

    // Original points and the default vector are still intact
    assert_eq!(segment.available_point_count(), NUM_POINTS + 1);
    for i in 1..=NUM_POINTS as u64 {
        let default_vec = segment.vector(DEFAULT_VECTOR_NAME, i.into(), &hw).unwrap();
        assert!(default_vec.is_some());
    }
}

#[test]
fn test_delete_sparse_vector_with_data() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let mut segment = build_appendable_segment_with_data(dir.path());
    let hw = hw();

    segment
        .create_vector_name(100, "sp", &sparse_vector_name_config())
        .unwrap();

    // Insert a point with sparse data
    let sparse_vec = SparseVector::new(vec![1, 3], vec![0.5, 0.8]).unwrap();
    let mut vectors = NamedVectors::default();
    vectors.insert(DEFAULT_VECTOR_NAME.to_owned(), vec![4.0f32; DIM].into());
    vectors.insert("sp".to_owned(), sparse_vec.into());
    segment
        .upsert_point(101, (NUM_POINTS as u64 + 1).into(), vectors, &hw)
        .unwrap();

    // Verify data is there
    let all_vecs = segment
        .all_vectors((NUM_POINTS as u64 + 1).into(), &hw)
        .unwrap();
    assert!(all_vecs.contains_key("sp"));

    // Delete
    assert!(segment.delete_vector_name(102, "sp").unwrap());
    assert!(!segment.segment_config.sparse_vector_data.contains_key("sp"));
    assert!(!segment.vector_data.contains_key("sp"));
}

// ---------------------------------------------------------------------------
// Persistence (save/reload)
// ---------------------------------------------------------------------------

#[test]
fn test_persistence_after_create_with_data() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let mut segment = build_appendable_segment_with_data(dir.path());
    let hw = hw();

    let new_dim = 6;
    segment
        .create_vector_name(100, "persisted", &dense_vector_name_config(new_dim))
        .unwrap();

    // Insert data into the new vector
    let mut vectors = NamedVectors::default();
    vectors.insert(DEFAULT_VECTOR_NAME.to_owned(), vec![8.0f32; DIM].into());
    vectors.insert("persisted".to_owned(), vec![1.5f32; new_dim].into());
    segment
        .upsert_point(101, (NUM_POINTS as u64 + 1).into(), vectors, &hw)
        .unwrap();

    // Save, drop, reload
    let segment_path = segment.data_path();
    let segment_uuid = segment.uuid;
    segment.flush(true).unwrap();
    drop(segment);

    let stopped = AtomicBool::new(false);
    let loaded = load_segment(&segment_path, segment_uuid, None, &stopped).unwrap();

    // Config persisted
    assert_eq!(loaded.segment_config.vector_data["persisted"].size, new_dim);
    assert_eq!(loaded.available_point_count(), NUM_POINTS + 1);

    // Data persisted - vector is readable
    let vec = loaded
        .vector("persisted", (NUM_POINTS as u64 + 1).into(), &hw)
        .unwrap();
    assert!(vec.is_some());

    // Original data intact
    for i in 1..=NUM_POINTS as u64 {
        let original = loaded.vector(DEFAULT_VECTOR_NAME, i.into(), &hw).unwrap();
        assert!(
            original.is_some(),
            "point {i} should have default vector after reload"
        );
    }
}

#[test]
fn test_persistence_after_delete_with_data() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let mut segment = build_appendable_segment_with_data(dir.path());
    let hw = hw();

    let new_dim = 8;
    segment
        .create_vector_name(100, "temp", &dense_vector_name_config(new_dim))
        .unwrap();

    // Insert data, then delete the vector
    let mut vectors = NamedVectors::default();
    vectors.insert(DEFAULT_VECTOR_NAME.to_owned(), vec![6.0f32; DIM].into());
    vectors.insert("temp".to_owned(), vec![2.0f32; new_dim].into());
    segment
        .upsert_point(101, (NUM_POINTS as u64 + 1).into(), vectors, &hw)
        .unwrap();

    segment.delete_vector_name(102, "temp").unwrap();

    // Save, drop, reload
    let segment_path = segment.data_path();
    let segment_uuid = segment.uuid;
    segment.flush(true).unwrap();
    drop(segment);

    let stopped = AtomicBool::new(false);
    let loaded = load_segment(&segment_path, segment_uuid, None, &stopped).unwrap();

    assert!(!loaded.segment_config.vector_data.contains_key("temp"));
    assert_eq!(loaded.available_point_count(), NUM_POINTS + 1);

    // Original data still intact
    for i in 1..=NUM_POINTS as u64 {
        let original = loaded.vector(DEFAULT_VECTOR_NAME, i.into(), &hw).unwrap();
        assert!(
            original.is_some(),
            "point {i} should have default vector after reload"
        );
    }
}
