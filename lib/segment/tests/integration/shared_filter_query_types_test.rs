//! One user with several query vectors shares its filter on a graph search, for the query
//! types the plain dense `Nearest` tests don't cover: multivector (`max_sim`), `Discover` and
//! `Context`. The shared search must return points that match the original filter, about as
//! good as an exact search of it.

// Deprecated storage placement params (`on_disk`) are still handled here
#![allow(deprecated)]

use std::collections::HashMap;
use std::sync::atomic::AtomicBool;

use common::ambient;
use common::budget::ResourcePermit;
use common::flags::FeatureFlags;
use common::progress_tracker::ProgressTracker;
use rand::prelude::StdRng;
use rand::{RngExt, SeedableRng};
use segment::data_types::filter_user::FilterUser;
use segment::data_types::query_context::QueryContext;
use segment::data_types::vectors::{
    DEFAULT_VECTOR_NAME, QueryVector, only_default_multi_vector, only_default_vector,
};
use segment::entry::entry_point::{NonAppendableSegmentEntry, ReadSegmentEntry, SegmentEntry};
use segment::fixtures::index_fixtures::random_vector;
use segment::fixtures::payload_fixtures::random_multi_vector;
use segment::fixtures::query_fixtures::{QueryVariant, random_multi_vec_query, random_query};
use segment::json_path::JsonPath;
use segment::payload_json;
use segment::segment::Segment;
use segment::segment_constructor::build_segment;
use segment::segment_constructor::segment_builder::SegmentBuilder;
use segment::segment_constructor::simple_segment_constructor::build_simple_segment;
use segment::types::{
    Condition, Distance, FieldCondition, Filter, HnswConfig, HnswGlobalConfig, Indexes,
    MultiVectorConfig, PayloadSchemaType, PointIdType, ScoredPoint, SearchParams, SegmentConfig,
    SeqNumberType, VectorDataConfig, VectorStorageType, WithPayload,
};
use tempfile::{Builder, TempDir};
use uuid::Uuid;

const DIM: usize = 8;
const NUM_POINTS: u64 = 1_500;
const TOP: usize = 10;
const EF: usize = 400;

/// Points with an indexed `half` in `0..2`: even ids have 0, odd ids 1.
fn source_segment(path: &std::path::Path, multivector: bool, rng: &mut StdRng) -> Segment {
    let mut segment = if multivector {
        let config = SegmentConfig {
            vector_data: HashMap::from([(
                DEFAULT_VECTOR_NAME.to_owned(),
                VectorDataConfig {
                    size: DIM,
                    distance: Distance::Cosine,
                    storage_type: VectorStorageType::default(),
                    index: Indexes::Plain {},
                    quantization_config: None,
                    multivector_config: Some(MultiVectorConfig::default()),
                    datatype: None,
                },
            )]),
            sparse_vector_data: Default::default(),
            payload_storage_type: Default::default(),
            id_tracker_memory: None,
        };
        build_segment(path, &config, None, true).unwrap().0
    } else {
        build_simple_segment(path, DIM, Distance::Cosine).unwrap()
    };
    segment
        .create_field_index(
            0,
            &JsonPath::new("half"),
            Some(&PayloadSchemaType::Integer.into()),
        )
        .unwrap();
    for n in 0..NUM_POINTS {
        let id = n.into();
        if multivector {
            let count = rng.random_range(1..=3);
            let vector = random_multi_vector(rng, DIM, count);
            segment
                .upsert_point(n as SeqNumberType, id, only_default_multi_vector(&vector))
                .unwrap();
        } else {
            let vector = random_vector(rng, DIM);
            segment
                .upsert_point(n as SeqNumberType, id, only_default_vector(&vector))
                .unwrap();
        }
        segment
            .set_full_payload(n as SeqNumberType, id, &payload_json! {"half": n % 2})
            .unwrap();
    }
    segment
}

/// `source_segment` rebuilt with an HNSW index. The full scan threshold is 4 KB: about 128
/// dense vectors of 32 bytes, or about 64 multivectors of 1 to 3 such vectors. A filter matching
/// 750 of the 1,500 points is far above either, so it takes the graph search.
fn hnsw_segment(multivector: bool, rng: &mut StdRng) -> (Segment, Vec<TempDir>) {
    let source_dir = Builder::new().prefix("source_dir").tempdir().unwrap();
    let source = source_segment(source_dir.path(), multivector, rng);

    let hnsw_config = HnswConfig {
        memory: None,
        m: 8,
        ef_construct: 64,
        full_scan_threshold: 4,
        max_indexing_threads: 2,
        on_disk: Some(false),
        payload_m: None,
        inline_storage: None,
    };
    let source_config = &source.segment_config.vector_data[DEFAULT_VECTOR_NAME];
    let segment_config = SegmentConfig {
        vector_data: HashMap::from([(
            DEFAULT_VECTOR_NAME.to_owned(),
            VectorDataConfig {
                size: DIM,
                distance: Distance::Cosine,
                storage_type: VectorStorageType::default(),
                index: Indexes::Hnsw(hnsw_config),
                quantization_config: None,
                multivector_config: source_config.multivector_config,
                datatype: None,
            },
        )]),
        sparse_vector_data: Default::default(),
        payload_storage_type: Default::default(),
        id_tracker_memory: None,
    };

    let temp_dir = Builder::new().prefix("builder_temp").tempdir().unwrap();
    let out_dir = Builder::new().prefix("builder_out").tempdir().unwrap();
    let mut builder = SegmentBuilder::new(
        temp_dir.path(),
        &segment_config,
        &HnswGlobalConfig::default(),
        FeatureFlags::default(),
    )
    .unwrap();
    builder.update(&[&source], &AtomicBool::new(false)).unwrap();
    let segment = builder
        .build(
            out_dir.path(),
            Uuid::new_v4(),
            None,
            true,
            ResourcePermit::dummy(2),
            &AtomicBool::new(false),
            rng,
            ProgressTracker::new_for_test(),
        )
        .unwrap();
    (segment, vec![source_dir, temp_dir, out_dir])
}

fn half_is_one() -> Filter {
    Filter::new_must(Condition::Field(FieldCondition::new_match(
        JsonPath::new("half"),
        1.into(),
    )))
}

/// Shares `batch` as one user, and checks the search with the shared filter without
/// comparing it to the search with the original one, which is approximate: every result has
/// `half == 1` (an odd id) and there are `TOP` of them, and the fraction of results at least
/// as good as the exact search's `TOP`th, averaged over the queries, is at least `min_recall`
/// and not much below that of the search with the original filter on the same graph. Scores
/// are compared, not ids, because `Discover` and `Context` queries score many points the same.
/// Returns that fraction.
fn check_one_user_shares(segment: &Segment, batch: &[QueryVector], min_recall: f64) -> f64 {
    let query_context = QueryContext::default();
    let segment_query_context = query_context.get_segment_query_context();
    let filter = half_is_one();
    // A large `ef` makes a graph search of this small segment nearly exhaustive, so the
    // recall doesn't depend on how the graph, built by several threads, turned out
    let params = SearchParams {
        hnsw_ef: Some(EF),
        ..Default::default()
    };
    let user = |query_vectors| FilterUser {
        vector_name: DEFAULT_VECTOR_NAME,
        query_vectors,
        top: TOP,
        params: Some(&params),
    };

    // One search never shares; several searches on a graph do
    assert!(
        segment
            .share_filter(&filter, &[user(&batch[..1])], &segment_query_context)
            .unwrap()
            .is_none()
    );
    let shared = segment
        .share_filter(&filter, &[user(batch)], &segment_query_context)
        .unwrap()
        .expect("several graph searches share their filter");

    let refs: Vec<&QueryVector> = batch.iter().collect();
    let search = |filter: &Filter, params: Option<&SearchParams>| -> Vec<Vec<ScoredPoint>> {
        segment
            .search_batch(
                DEFAULT_VECTOR_NAME,
                &refs,
                &WithPayload::default(),
                &false.into(),
                Some(filter),
                TOP,
                params,
                &segment_query_context,
            )
            .unwrap()
    };
    let exact_params = SearchParams {
        exact: true,
        ..Default::default()
    };
    let results = search(&shared, Some(&params));
    let unshared = search(&filter, Some(&params));
    let exact = search(&filter, Some(&exact_params));

    let good_fraction = |results: &[Vec<ScoredPoint>]| -> f64 {
        let mut total = 0.0;
        for (result, exact) in results.iter().zip(&exact) {
            let kth = exact.last().expect("exact search found points").score;
            let good = result.iter().filter(|p| p.score >= kth - 1e-6).count();
            total += good as f64 / TOP as f64;
        }
        total / results.len() as f64
    };
    let unshared_recall = good_fraction(&unshared);
    let mut recall = 0.0;
    for (result, exact) in results.iter().zip(&exact) {
        assert_eq!(result.len(), TOP);
        for point in result {
            let PointIdType::NumId(id) = point.id else {
                panic!("unexpected id {:?}", point.id);
            };
            assert_eq!(id % 2, 1, "{id} doesn't match the filter");
        }
        let kth = exact.last().expect("exact search found points").score;
        let good = result
            .iter()
            .filter(|point| point.score >= kth - 1e-6)
            .count();
        recall += good as f64 / TOP as f64;
    }
    let recall = recall / results.len() as f64;
    assert!(recall >= min_recall, "recall {recall} against exact search");
    // The same graph, searched with the original filter, is the best the shared one can match
    assert!(
        recall >= unshared_recall - 0.1,
        "recall {recall} with the shared filter, {unshared_recall} without"
    );
    recall
}

#[test]
fn test_multivector_one_user_shares_on_graph_search() {
    let _scope = ambient::test_guard();
    let mut rng = StdRng::seed_from_u64(21);
    let (segment, _dirs) = hnsw_segment(true, &mut rng);
    let batch: Vec<QueryVector> = (0..4)
        .map(|_| random_multi_vec_query(&QueryVariant::Nearest, &mut rng, DIM, 2))
        .collect();
    // A multivector graph is approximate even with a large `ef`: 0.85 to 0.975 over repeated
    // builds, the same with and without the shared filter
    check_one_user_shares(&segment, &batch, 0.7);
}

#[test]
fn test_discover_one_user_shares_on_graph_search() {
    let _scope = ambient::test_guard();
    let mut rng = StdRng::seed_from_u64(22);
    let (segment, _dirs) = hnsw_segment(false, &mut rng);
    let batch: Vec<QueryVector> = (0..4)
        .map(|_| {
            random_query(&QueryVariant::Discover, &mut rng, |rng| {
                random_vector(rng, DIM).into()
            })
        })
        .collect();
    check_one_user_shares(&segment, &batch, 0.9);
}

#[test]
fn test_context_one_user_shares_on_graph_search() {
    let _scope = ambient::test_guard();
    let mut rng = StdRng::seed_from_u64(23);
    let (segment, _dirs) = hnsw_segment(false, &mut rng);
    let batch: Vec<QueryVector> = (0..4)
        .map(|_| {
            random_query(&QueryVariant::Context, &mut rng, |rng| {
                random_vector(rng, DIM).into()
            })
        })
        .collect();
    check_one_user_shares(&segment, &batch, 0.9);
}
