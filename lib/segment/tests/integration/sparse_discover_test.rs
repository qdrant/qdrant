use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use common::cpu::CpuPermit;
use common::types::TelemetryDetail;
use itertools::Itertools;
use rand::prelude::StdRng;
use rand::{Rng, SeedableRng};
use segment::data_types::named_vectors::NamedVectors;
use segment::data_types::vectors::{QueryVector, VectorElementType};
use segment::entry::entry_point::SegmentEntry;
use segment::fixtures::payload_fixtures::random_vector;
use segment::index::hnsw_index::num_rayon_threads;
use segment::index::sparse_index::sparse_index_config::{SparseIndexConfig, SparseIndexType};
use segment::index::sparse_index::sparse_vector_index::SparseVectorIndex;
use segment::index::VectorIndex;
use segment::segment_constructor::build_segment;
use segment::types::{
    Distance, Indexes, SegmentConfig, SeqNumberType, SparseVectorDataConfig, VectorDataConfig,
    VectorStorageType, DEFAULT_SPARSE_FULL_SCAN_THRESHOLD,
};
use segment::vector_storage::query::context_query::ContextPair;
use segment::vector_storage::query::discovery_query::DiscoveryQuery;
use sparse::common::sparse_vector::SparseVector;
use sparse::index::inverted_index::inverted_index_ram::InvertedIndexRam;
use tempfile::Builder;

const MAX_EXAMPLE_PAIRS: usize = 3;
const SPARSE_VECTOR_NAME: &str = "sparse_test";

fn convert_to_sparse_vector(vector: &[VectorElementType]) -> SparseVector {
    let mut sparse_vector = SparseVector::default();
    for (idx, value) in vector.iter().enumerate() {
        sparse_vector.indices.push(idx as u32);
        sparse_vector.values.push(*value);
    }
    sparse_vector
}

fn random_named_vector<R: Rng + ?Sized>(rnd: &mut R, dim: usize) -> (NamedVectors, NamedVectors) {
    let dense_vector = random_vector(rnd, dim);
    let sparse_vector = convert_to_sparse_vector(&dense_vector);

    let mut sparse_result = NamedVectors::default();
    sparse_result.insert(SPARSE_VECTOR_NAME.to_owned(), sparse_vector.into());

    let mut dense_result = NamedVectors::default();
    dense_result.insert(SPARSE_VECTOR_NAME.to_owned(), dense_vector.into());

    (sparse_result, dense_result)
}

fn random_discovery_query<R: Rng + ?Sized>(rnd: &mut R, dim: usize) -> (QueryVector, QueryVector) {
    let num_pairs: usize = rnd.gen_range(1..MAX_EXAMPLE_PAIRS);
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
                },
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
                storage_type: VectorStorageType::Memory,
                index: Indexes::Plain {},
                quantization_config: None,
                multi_vec_config: None,
                datatype: None,
            },
        )]),
        payload_storage_type: Default::default(),
        sparse_vector_data: Default::default(),
    };

    let mut sparse_segment = build_segment(dir.path(), &sparse_config, true).unwrap();
    let mut dense_segment = build_segment(dir.path(), &dense_config, true).unwrap();

    let permit_cpu_count = num_rayon_threads(0);
    let permit = Arc::new(CpuPermit::dummy(permit_cpu_count as u32));

    for n in 0..num_vectors {
        let (sparse_vector, dense_vector) = random_named_vector(&mut rnd, dim);

        let idx = n.into();
        sparse_segment
            .upsert_point(n as SeqNumberType, idx, sparse_vector)
            .unwrap();
        dense_segment
            .upsert_point(n as SeqNumberType, idx, dense_vector)
            .unwrap();
    }

    let payload_index_ptr = sparse_segment.payload_index.clone();

    let vector_storage = &sparse_segment.vector_data[SPARSE_VECTOR_NAME].vector_storage;
    let mut sparse_index = SparseVectorIndex::<InvertedIndexRam>::open(
        SparseIndexConfig {
            full_scan_threshold: Some(DEFAULT_SPARSE_FULL_SCAN_THRESHOLD),
            index_type: SparseIndexType::ImmutableRam,
        },
        sparse_segment.id_tracker.clone(),
        vector_storage.clone(),
        payload_index_ptr.clone(),
        index_dir.path(),
        &stopped,
    )
    .unwrap();

    sparse_index.build_index(permit, &stopped).unwrap();

    let top = 3;
    let attempts = 100;
    for i in 0..attempts {
        // do discovery search
        let (sparse_query, dense_query) = random_discovery_query(&mut rnd, dim);

        let sparse_discovery_result = sparse_index
            .search(
                &[&sparse_query],
                None,
                top,
                None,
                &false.into(),
                &Default::default(),
            )
            .unwrap();

        let dense_discovery_result = dense_segment.vector_data[SPARSE_VECTOR_NAME]
            .vector_index
            .borrow()
            .search(
                &[&dense_query],
                None,
                top,
                None,
                &false.into(),
                &Default::default(),
            )
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
        let sparse_search_result = sparse_index
            .search(
                &[&sparse_query],
                None,
                top,
                None,
                &false.into(),
                &Default::default(),
            )
            .unwrap();

        let dense_search_result = dense_segment.vector_data[SPARSE_VECTOR_NAME]
            .vector_index
            .borrow()
            .search(
                &[&dense_query],
                None,
                top,
                None,
                &false.into(),
                &Default::default(),
            )
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
