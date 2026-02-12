use std::collections::HashMap;
use std::sync::atomic::AtomicBool;

use ahash::AHashSet;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::TelemetryDetail;
use itertools::Itertools;
use rand::prelude::StdRng;
use rand::{Rng, SeedableRng};
use segment::data_types::named_vectors::NamedVectors;
use segment::data_types::query_context::{QueryContext, VectorQueryContext};
use segment::data_types::vectors::{QueryVector, VectorElementType, VectorInternal};
use segment::entry::entry_point::SegmentEntry;
use segment::fixtures::payload_fixtures::random_vector;
use segment::index::VectorIndex;
use segment::index::sparse_index::sparse_index_config::{SparseIndexConfig, SparseIndexType};
use segment::index::sparse_index::sparse_vector_index::SparseVectorIndexOpenArgs;
use segment::segment_constructor::{build_segment, create_sparse_vector_index_test};
use segment::types::{
    Condition, DEFAULT_SPARSE_FULL_SCAN_THRESHOLD, Distance, ExtendedPointId, Filter,
    HasIdCondition, Indexes, PointIdType, SegmentConfig, SeqNumberType, SparseVectorDataConfig,
    SparseVectorStorageType, VectorDataConfig, VectorStorageDatatype, VectorStorageType,
};
use segment::vector_storage::query::{ContextPair, DiscoveryQuery};
use sparse::common::sparse_vector::SparseVector;
use tempfile::Builder;

use crate::fixtures::segment::SPARSE_VECTOR_NAME;

const MAX_EXAMPLE_PAIRS: usize = 3;

fn convert_to_sparse_vector(vector: &[VectorElementType]) -> SparseVector {
    let mut sparse_vector = SparseVector::default();
    for (idx, value) in vector.iter().enumerate() {
        sparse_vector.indices.push(idx as u32);
        sparse_vector.values.push(*value);
    }
    sparse_vector
}

fn random_named_vector<R: Rng + ?Sized>(
    rnd: &mut R,
    dim: usize,
) -> (NamedVectors<'_>, NamedVectors<'_>) {
    let dense_vector = random_vector(rnd, dim);
    let sparse_vector = convert_to_sparse_vector(&dense_vector);

    let mut sparse_result = NamedVectors::default();
    sparse_result.insert(SPARSE_VECTOR_NAME.to_owned(), sparse_vector.into());

    let mut dense_result = NamedVectors::default();
    dense_result.insert(SPARSE_VECTOR_NAME.to_owned(), dense_vector.into());

    (sparse_result, dense_result)
}

fn random_discovery_query<R: Rng + ?Sized>(rnd: &mut R, dim: usize) -> (QueryVector, QueryVector) {
    let num_pairs: usize = rnd.random_range(1..MAX_EXAMPLE_PAIRS);
    let dense_target = random_vector(rnd, dim);
    let sparse_target = convert_to_sparse_vector(&dense_target);

    let dense_pairs = (0..num_pairs)
        .map(|_| {
            let positive = random_vector(rnd, dim);
            let negative = random_vector(rnd, dim);
            (positive, negative)
        })
        .collect_vec();
    let sparse_pairs = (0..num_pairs)
        .map(|i| {
            let positive = convert_to_sparse_vector(&dense_pairs[i].0);
            let negative = convert_to_sparse_vector(&dense_pairs[i].1);
            (positive, negative)
        })
        .collect_vec();

    let dense_query = DiscoveryQuery::new(
        dense_target.into(),
        dense_pairs
            .into_iter()
            .map(|(positive, negative)| ContextPair {
                positive: positive.into(),
                negative: negative.into(),
            })
            .collect(),
    )
    .into();
    let sparse_query = DiscoveryQuery::new(
        sparse_target.into(),
        sparse_pairs
            .into_iter()
            .map(|(positive, negative)| ContextPair {
                positive: positive.into(),
                negative: negative.into(),
            })
            .collect(),
    )
    .into();

    (sparse_query, dense_query)
}

fn random_nearest_query<R: Rng + ?Sized>(rnd: &mut R, dim: usize) -> (QueryVector, QueryVector) {
    let dense_target = random_vector(rnd, dim);
    let sparse_target = convert_to_sparse_vector(&dense_target);
    (sparse_target.into(), dense_target.into())
}

#[test]
fn sparse_index_discover_test() {
    let stopped = AtomicBool::new(false);

    let dim = 8;
    let num_vectors: u64 = 5_000;
    let distance = Distance::Dot;

    let mut rnd = StdRng::seed_from_u64(42);

    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let index_dir = Builder::new().prefix("hnsw_dir").tempdir().unwrap();

    let sparse_config = SegmentConfig {
        vector_data: Default::default(),
        sparse_vector_data: HashMap::from([(
            SPARSE_VECTOR_NAME.to_owned(),
            SparseVectorDataConfig {
                index: SparseIndexConfig {
                    full_scan_threshold: Some(DEFAULT_SPARSE_FULL_SCAN_THRESHOLD),
                    index_type: SparseIndexType::MutableRam,
                    datatype: Some(VectorStorageDatatype::Float32),
                },
                storage_type: SparseVectorStorageType::default(),
                modifier: None,
            },
        )]),
        payload_storage_type: Default::default(),
    };
    let dense_config = SegmentConfig {
        vector_data: HashMap::from([(
            SPARSE_VECTOR_NAME.to_owned(),
            VectorDataConfig {
                size: dim,
                distance,
                storage_type: VectorStorageType::default(),
                index: Indexes::Plain {},
                quantization_config: None,
                multivector_config: None,
                datatype: None,
            },
        )]),
        payload_storage_type: Default::default(),
        sparse_vector_data: Default::default(),
    };

    let mut sparse_segment = build_segment(dir.path(), &sparse_config, true).unwrap();
    let mut dense_segment = build_segment(dir.path(), &dense_config, true).unwrap();

    let hw_counter = HardwareCounterCell::new();

    for n in 0..num_vectors {
        let (sparse_vector, dense_vector) = random_named_vector(&mut rnd, dim);

        let idx = n.into();
        sparse_segment
            .upsert_point(n as SeqNumberType, idx, sparse_vector, &hw_counter)
            .unwrap();
        dense_segment
            .upsert_point(n as SeqNumberType, idx, dense_vector, &hw_counter)
            .unwrap();
    }

    let payload_index_ptr = sparse_segment.payload_index.clone();

    let vector_storage = &sparse_segment.vector_data[SPARSE_VECTOR_NAME].vector_storage;
    let sparse_index = create_sparse_vector_index_test(SparseVectorIndexOpenArgs {
        config: SparseIndexConfig {
            full_scan_threshold: Some(DEFAULT_SPARSE_FULL_SCAN_THRESHOLD),
            index_type: SparseIndexType::ImmutableRam,
            datatype: Some(VectorStorageDatatype::Float32),
        },
        id_tracker: sparse_segment.id_tracker.clone(),
        vector_storage: vector_storage.clone(),
        payload_index: payload_index_ptr,
        path: index_dir.path(),
        stopped: &stopped,
        tick_progress: || (),
    })
    .unwrap();

    let top = 3;
    let attempts = 100;
    for i in 0..attempts {
        // do discovery search
        let (sparse_query, dense_query) = random_discovery_query(&mut rnd, dim);

        let vec_context = VectorQueryContext::default();
        let sparse_discovery_result = sparse_index
            .search(&[&sparse_query], None, top, None, &vec_context)
            .unwrap();

        let dense_discovery_result = dense_segment.vector_data[SPARSE_VECTOR_NAME]
            .vector_index
            .borrow()
            .search(&[&dense_query], None, top, None, &vec_context)
            .unwrap();

        // check id only because scores can be epsilon-size different
        assert_eq!(
            sparse_discovery_result[0]
                .iter()
                .map(|r| r.idx)
                .collect_vec(),
            dense_discovery_result[0]
                .iter()
                .map(|r| r.idx)
                .collect_vec(),
        );

        // do regular nearest search
        let (sparse_query, dense_query) = random_nearest_query(&mut rnd, dim);

        let query_context = QueryContext::default();
        let segment_query_context = query_context.get_segment_query_context();
        let vector_context = segment_query_context.get_vector_context(SPARSE_VECTOR_NAME);

        let sparse_search_result = sparse_index
            .search(&[&sparse_query], None, top, None, &vector_context)
            .unwrap();

        let cpu_usage = query_context.hardware_usage_accumulator().get_cpu();
        assert!(cpu_usage > 0);

        let dense_search_result = dense_segment.vector_data[SPARSE_VECTOR_NAME]
            .vector_index
            .borrow()
            .search(&[&dense_query], None, top, None, &vector_context)
            .unwrap();

        // check that nearest search uses sparse index
        let telemetry = sparse_index.get_telemetry_data(TelemetryDetail::default());
        assert_eq!(telemetry.unfiltered_sparse.count, i + 1);

        // check id only because scores can be epsilon-size different
        assert_eq!(
            sparse_search_result[0].iter().map(|r| r.idx).collect_vec(),
            dense_search_result[0].iter().map(|r| r.idx).collect_vec(),
        );
    }
}

#[test]
fn sparse_index_hardware_measurement_test() {
    let stopped = AtomicBool::new(false);

    let dim = 8;
    let num_vectors: u64 = 5_000;

    let mut rnd = StdRng::seed_from_u64(42);

    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let index_dir = Builder::new().prefix("hnsw_dir").tempdir().unwrap();

    let sparse_config = SegmentConfig {
        vector_data: Default::default(),
        sparse_vector_data: HashMap::from([(
            SPARSE_VECTOR_NAME.to_owned(),
            SparseVectorDataConfig {
                index: SparseIndexConfig {
                    full_scan_threshold: Some(DEFAULT_SPARSE_FULL_SCAN_THRESHOLD),
                    index_type: SparseIndexType::MutableRam,
                    datatype: Some(VectorStorageDatatype::Float32),
                },
                storage_type: SparseVectorStorageType::default(),
                modifier: None,
            },
        )]),
        payload_storage_type: Default::default(),
    };

    let mut sparse_segment = build_segment(dir.path(), &sparse_config, true).unwrap();

    let hw_counter = HardwareCounterCell::new();

    for n in 0..num_vectors {
        let (sparse_vector, _) = random_named_vector(&mut rnd, dim);

        let idx = n.into();
        sparse_segment
            .upsert_point(n as SeqNumberType, idx, sparse_vector, &hw_counter)
            .unwrap();
    }
    let payload_index_ptr = sparse_segment.payload_index.clone();

    let vector_storage = &sparse_segment.vector_data[SPARSE_VECTOR_NAME].vector_storage;
    let sparse_index = create_sparse_vector_index_test(SparseVectorIndexOpenArgs {
        config: SparseIndexConfig {
            full_scan_threshold: Some(DEFAULT_SPARSE_FULL_SCAN_THRESHOLD),
            index_type: SparseIndexType::ImmutableRam,
            datatype: Some(VectorStorageDatatype::Float32),
        },
        id_tracker: sparse_segment.id_tracker.clone(),
        vector_storage: vector_storage.clone(),
        payload_index: payload_index_ptr,
        path: index_dir.path(),
        stopped: &stopped,
        tick_progress: || (),
    })
    .unwrap();

    let query_vec = QueryVector::Nearest(VectorInternal::Sparse(
        SparseVector::new(vec![0, 1, 2], vec![42.0, 42.42, 42.4242]).unwrap(),
    ));

    let query_context = QueryContext::default();
    let segment_query_context = query_context.get_segment_query_context();
    let vector_context = segment_query_context.get_vector_context(SPARSE_VECTOR_NAME);

    let cpu_usage = query_context.hardware_usage_accumulator().get_cpu();
    assert_eq!(cpu_usage, 0);

    // Some filter so we do plain sparse search
    let ids: AHashSet<PointIdType> = (0..3).map(ExtendedPointId::NumId).collect();
    let filter = Filter::new_must(Condition::HasId(HasIdCondition::from(ids)));

    sparse_index
        .search(&[&query_vec], Some(&filter), 1, None, &vector_context)
        .unwrap();

    let cpu_usage = query_context.hardware_usage_accumulator().get_cpu();
    assert!(cpu_usage > 0);
}
