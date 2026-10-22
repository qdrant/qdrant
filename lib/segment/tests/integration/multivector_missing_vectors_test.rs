use std::collections::HashMap;
use std::sync::atomic::AtomicBool;

use common::ambient;
use common::flags::FeatureFlags;
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use segment::data_types::named_vectors::NamedVectors;
use segment::data_types::vectors::{MultiDenseVectorInternal, QueryVector, VectorInternal};
use segment::entry::entry_point::{ReadSegmentEntry, SegmentEntry};
use segment::segment::Segment;
use segment::segment_constructor::build_segment;
use segment::segment_constructor::segment_builder::SegmentBuilder;
use segment::types::{
    Distance, HnswGlobalConfig, Indexes, MultiVectorConfig, PointIdType, SegmentConfig,
    VectorDataConfig, VectorStorageType, WithPayload,
};
use segment::vector_storage::{MultiVectorStorageRead, VectorStorageEnum};
use tempfile::Builder;

const DIM: usize = 8;
const POINTS: u64 = 200;
const DENSE: &str = "dense";
const MULTI: &str = "multi";

fn config() -> SegmentConfig {
    let vector = |multivector_config| VectorDataConfig {
        size: DIM,
        distance: Distance::Dot,
        storage_type: VectorStorageType::ChunkedMmap,
        index: Indexes::Plain {},
        quantization_config: None,
        multivector_config,
        datatype: None,
    };
    SegmentConfig {
        vector_data: HashMap::from([
            (DENSE.to_owned(), vector(None)),
            (MULTI.to_owned(), vector(Some(MultiVectorConfig::default()))),
        ]),
        sparse_vector_data: Default::default(),
        payload_storage_type: Default::default(),
        id_tracker_memory: None,
    }
}

/// Inner vectors held by the multi-vector storage, placeholders included.
fn stored_inner_vectors(segment: &Segment) -> usize {
    match &*segment.vector_data[MULTI].vector_storage.borrow() {
        VectorStorageEnum::MultiDenseAppendableMemmap(storage) => {
            storage.iterate_inner_vectors().count()
        }
        _ => panic!("expected an appendable multi-dense storage"),
    }
}

fn multi(vector: &[f32]) -> MultiDenseVectorInternal {
    MultiDenseVectorInternal::new(vector.to_vec(), DIM)
}

/// Every point has the multi-vector it should, none of the others do, and an
/// exact search ranks exactly the points that have one.
fn check(segment: &Segment, vectors: &HashMap<u64, Vec<f32>>, queries: &[Vec<f32>]) {
    for id in 0..POINTS {
        let stored = segment.vector(MULTI, PointIdType::from(id)).unwrap();
        match vectors.get(&id) {
            Some(vector) => assert_eq!(stored, Some(VectorInternal::from(multi(vector))), "{id}"),
            None => assert_eq!(stored, None, "{id}"),
        }
    }

    for query in queries {
        let mut expected: Vec<(u64, f32)> = vectors
            .iter()
            .map(|(&id, v)| (id, v.iter().zip(query).map(|(a, b)| a * b).sum()))
            .collect();
        expected.sort_by(|a, b| b.1.total_cmp(&a.1));
        let found = segment
            .search(
                MULTI,
                &QueryVector::Nearest(VectorInternal::from(multi(query))),
                &WithPayload::default(),
                &false.into(),
                None,
                10,
                None,
            )
            .unwrap();
        let found_ids: Vec<_> = found.iter().map(|p| p.id).collect();
        let expected_ids: Vec<_> = expected[..10]
            .iter()
            .map(|&(id, _)| PointIdType::from(id))
            .collect();
        assert_eq!(found_ids, expected_ids);
        for (point, (_, score)) in found.iter().zip(&expected) {
            assert!((point.score - score).abs() < 1e-4);
        }
    }
}

/// Points without a multi-vector store no inner vectors, neither in the
/// appendable segment nor in the one the optimizer builds from it, and both
/// read and search exactly as if the placeholders were there.
#[test]
fn missing_multivectors_take_no_rows() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let temp_dir = Builder::new().prefix("segment_temp_dir").tempdir().unwrap();
    let _scope = ambient::test_guard();
    let mut rng = StdRng::seed_from_u64(42);
    let mut random = || -> Vec<f32> { (0..DIM).map(|_| rng.random_range(-1.0..1.0)).collect() };

    let (mut segment, _) = build_segment(dir.path(), &config(), None, true).unwrap();
    let mut vectors = HashMap::new();
    for id in 0..POINTS {
        let mut named = NamedVectors::default();
        named.insert(DENSE.into(), VectorInternal::from(random()));
        // Every other point has the multi-vector
        if id % 2 == 0 {
            let vector = random();
            named.insert(MULTI.into(), VectorInternal::from(multi(&vector)));
            vectors.insert(id, vector);
        }
        segment.upsert_point(id, id.into(), named).unwrap();
    }
    let queries: Vec<_> = (0..5).map(|_| random()).collect();

    assert_eq!(stored_inner_vectors(&segment), vectors.len());
    check(&segment, &vectors, &queries);

    // A vector deleted from a point keeps its rows until the optimizer runs
    segment
        .delete_vector(POINTS, PointIdType::from(0), MULTI)
        .unwrap();
    vectors.remove(&0);
    // A point without the vector gets one
    let late = random();
    let mut named = NamedVectors::default();
    named.insert(MULTI.into(), VectorInternal::from(multi(&late)));
    segment
        .update_vectors(POINTS + 1, PointIdType::from(1), named)
        .unwrap();
    vectors.insert(1, late);

    assert_eq!(stored_inner_vectors(&segment), vectors.len() + 1);
    check(&segment, &vectors, &queries);

    let mut builder = SegmentBuilder::new(
        temp_dir.path(),
        &segment.segment_config,
        &HnswGlobalConfig::default(),
        FeatureFlags::default(),
    )
    .unwrap();
    builder
        .update(&[&segment], &AtomicBool::new(false))
        .unwrap();
    let built: Segment = builder.build_for_test(dir.path());

    assert_eq!(stored_inner_vectors(&built), vectors.len());
    check(&built, &vectors, &queries);
}
