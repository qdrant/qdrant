//! `max_available_vectors_size_in_bytes` is implemented as a single pass over
//! the vector data for `Segment` and `ReadOnlySegment`. These tests check it
//! against the previous trait default implementation: max of
//! `available_vectors_size_in_bytes` over every name in `vector_names()`.

use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::AtomicBool;

use common::ambient;
use common::flags::FeatureFlags;
use common::universal_io::{MmapFile, MmapFs};
use sparse::common::sparse_vector::SparseVector;
use tempfile::Builder;

use crate::data_types::named_vectors::NamedVectors;
use crate::data_types::vectors::VectorInternal;
use crate::entry::entry_point::{
    NonAppendableSegmentEntry as _, ReadSegmentEntry, SegmentEntry as _, StorageSegmentEntry as _,
};
use crate::index::sparse_index::sparse_index_config::{SparseIndexConfig, SparseIndexType};
use crate::segment::Segment;
use crate::segment::read_only::ReadOnlySegment;
use crate::segment_constructor::build_segment;
use crate::segment_constructor::segment_builder::SegmentBuilder;
use crate::types::{
    Distance, HnswGlobalConfig, Indexes, SegmentConfig, SparseVectorDataConfig,
    SparseVectorStorageType, VectorDataConfig, VectorStorageType,
};

const SMALL: &str = "small";
const MID: &str = "mid";
const LARGE: &str = "large";
const SPARSE: &str = "sparse";

const SMALL_DIM: usize = 4;
const MID_DIM: usize = 16;
const LARGE_DIM: usize = 64;

const NUM_POINTS: u64 = 60;

/// The previous default `ReadSegmentEntry::max_available_vectors_size_in_bytes`.
fn reference_max(segment: &impl ReadSegmentEntry) -> usize {
    let mut max_size = 0;
    for vector_name in segment.vector_names() {
        let inner_size = segment
            .available_vectors_size_in_bytes(&vector_name)
            .unwrap();
        max_size = std::cmp::max(max_size, inner_size);
    }
    max_size
}

#[track_caller]
fn assert_same_max(segment: &impl ReadSegmentEntry, context: &str) -> usize {
    let expected = reference_max(segment);
    let actual = segment.max_available_vectors_size_in_bytes().unwrap();
    assert_eq!(actual, expected, "max size mismatch: {context}");
    actual
}

fn dense_config(dim: usize, storage_type: VectorStorageType) -> VectorDataConfig {
    VectorDataConfig {
        size: dim,
        distance: Distance::Dot,
        storage_type,
        index: Indexes::Plain {},
        quantization_config: None,
        multivector_config: None,
        datatype: None,
    }
}

fn segment_config(
    dense_storage: VectorStorageType,
    sparse_index: SparseIndexType,
) -> SegmentConfig {
    SegmentConfig {
        vector_data: HashMap::from([
            (SMALL.to_owned(), dense_config(SMALL_DIM, dense_storage)),
            (MID.to_owned(), dense_config(MID_DIM, dense_storage)),
            (LARGE.to_owned(), dense_config(LARGE_DIM, dense_storage)),
        ]),
        sparse_vector_data: HashMap::from([(
            SPARSE.to_owned(),
            SparseVectorDataConfig {
                index: SparseIndexConfig::new(None, sparse_index, None, None),
                storage_type: SparseVectorStorageType::Mmap,
                modifier: None,
            },
        )]),
        payload_storage_type: Default::default(),
        id_tracker_memory: None,
    }
}

fn dense(dim: usize, seed: u64) -> VectorInternal {
    VectorInternal::from(
        (0..dim)
            .map(|j| ((seed as usize * 7 + j * 3) % 11) as f32 + 0.5)
            .collect::<Vec<f32>>(),
    )
}

/// Points carry different subsets of named vectors so that the per-name
/// sizes differ and the winner is not always the same name.
fn point_vectors(i: u64) -> NamedVectors<'static> {
    let mut vectors = NamedVectors::default();
    vectors.insert(SMALL.to_owned(), dense(SMALL_DIM, i));
    if i.is_multiple_of(3) {
        vectors.insert(MID.to_owned(), dense(MID_DIM, i));
    }
    if i.is_multiple_of(10) {
        vectors.insert(LARGE.to_owned(), dense(LARGE_DIM, i));
    }
    if i.is_multiple_of(2) {
        let nnz = (i % 7 + 1) as u32;
        let sparse = SparseVector::new(
            (0..nnz).map(|d| d * 3 + i as u32).collect(),
            (0..nnz).map(|d| d as f32 + 1.0).collect(),
        )
        .unwrap();
        vectors.insert(SPARSE.to_owned(), VectorInternal::from(sparse));
    }
    vectors
}

fn build_appendable(path: &Path) -> Segment {
    let (segment, _) = build_segment(
        path,
        &segment_config(VectorStorageType::default(), SparseIndexType::MutableRam),
        None,
        true,
    )
    .unwrap();
    segment
}

fn fill(segment: &mut Segment) {
    for i in 0..NUM_POINTS {
        segment
            .upsert_point(i + 1, (i + 1).into(), point_vectors(i))
            .unwrap();
    }
}

#[test]
fn max_available_vectors_size_matches_reference_on_appendable_segment() {
    let _scope = ambient::test_guard();
    let dir = Builder::new().prefix("max_size_seg").tempdir().unwrap();
    let mut segment = build_appendable(dir.path());

    // Empty segment: every name is present but holds nothing.
    assert_eq!(assert_same_max(&segment, "empty"), 0);

    let mut op_num = 0;
    for i in 0..NUM_POINTS {
        op_num += 1;
        segment
            .upsert_point(op_num, (i + 1).into(), point_vectors(i))
            .unwrap();
        assert_same_max(&segment, &format!("after upsert of point {}", i + 1));
    }
    assert!(assert_same_max(&segment, "filled") > 0);

    // Drop the `large` vector from every point that has one, so the winner changes.
    for i in (0..NUM_POINTS).filter(|i| i.is_multiple_of(10)) {
        op_num += 1;
        segment
            .delete_vector(op_num, (i + 1).into(), LARGE)
            .unwrap();
        assert_same_max(&segment, &format!("after delete_vector large {}", i + 1));
    }

    // Delete whole points.
    for i in (0..NUM_POINTS).step_by(4) {
        op_num += 1;
        segment.delete_point(op_num, (i + 1).into()).unwrap();
        assert_same_max(&segment, &format!("after delete_point {}", i + 1));
    }

    // Everything gone.
    for i in 0..NUM_POINTS {
        op_num += 1;
        segment.delete_point(op_num, (i + 1).into()).unwrap();
    }
    assert_same_max(&segment, "all points deleted");
}

#[test]
fn max_available_vectors_size_matches_reference_on_read_only_segment() {
    let _scope = ambient::test_guard();
    let source_dir = Builder::new().prefix("max_size_src").tempdir().unwrap();
    let segments_dir = Builder::new().prefix("max_size_segs").tempdir().unwrap();
    let temp_dir = Builder::new().prefix("max_size_tmp").tempdir().unwrap();

    let mut source = build_appendable(source_dir.path());
    fill(&mut source);

    let target_config = segment_config(VectorStorageType::Mmap, SparseIndexType::Mmap);
    assert!(!target_config.is_appendable());
    let mut builder = SegmentBuilder::new(
        temp_dir.path(),
        &target_config,
        &HnswGlobalConfig::default(),
        FeatureFlags::default(),
    )
    .unwrap();
    builder.update(&[&source], &AtomicBool::new(false)).unwrap();
    let mut immutable = builder.build_for_test(segments_dir.path());

    let mutable_max = assert_same_max(&immutable, "immutable segment");
    assert!(mutable_max > 0);

    let read_only = ReadOnlySegment::<MmapFile>::open(
        &MmapFs,
        &immutable.data_path(),
        immutable.uuid,
        None,
        None,
    )
    .expect("read-only open");
    let read_only_max = assert_same_max(&read_only, "read-only segment");
    assert_eq!(read_only_max, mutable_max);

    // Deletes on the immutable segment, then a fresh read-only open.
    let mut op_num = NUM_POINTS;
    for i in (0..NUM_POINTS).step_by(3) {
        op_num += 1;
        immutable.delete_point(op_num, (i + 1).into()).unwrap();
    }
    immutable.flush(true).unwrap();
    let mutable_max = assert_same_max(&immutable, "immutable segment after deletes");

    let read_only = ReadOnlySegment::<MmapFile>::open(
        &MmapFs,
        &immutable.data_path(),
        immutable.uuid,
        None,
        None,
    )
    .expect("read-only reopen");
    let read_only_max = assert_same_max(&read_only, "read-only segment after deletes");
    assert_eq!(read_only_max, mutable_max);
}
