//! A filter resolved once for several searches of a batch must select the same points as the
//! filter itself, on every search strategy.

// Deprecated storage placement params (`on_disk`) are still handled here
#![allow(deprecated)]

use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use common::ambient;
use common::budget::ResourcePermit;
use common::flags::FeatureFlags;
use common::progress_tracker::ProgressTracker;
use rand::SeedableRng;
use rand::prelude::StdRng;
use segment::data_types::filter_user::FilterUser;
use segment::data_types::named_vectors::NamedVectors;
use segment::data_types::query_context::QueryContext;
use segment::data_types::vectors::{DEFAULT_VECTOR_NAME, QueryVector, only_default_vector};
use segment::entry::entry_point::{NonAppendableSegmentEntry, ReadSegmentEntry, SegmentEntry};
use segment::fixtures::index_fixtures::random_vector;
use segment::index::field_index::CardinalityEstimation;
use segment::index::hnsw_index::get_num_indexing_threads;
use segment::index::hnsw_index::hnsw::{HNSWIndex, HnswIndexOpenArgs};
use segment::index::query_optimization::filter_checks::FilterChecks;
use segment::index::query_optimization::point_set::{PointSet, PointSetContext};
use segment::index::query_optimization::resolved_points::ResolvedPoints;
use segment::index::{PayloadIndexRead, VectorIndexRead};
use segment::json_path::JsonPath;
use segment::payload_json;
use segment::segment::Segment;
use segment::segment_constructor::VectorIndexBuildArgs;
use segment::segment_constructor::simple_segment_constructor::{
    VECTOR1_NAME, VECTOR2_NAME, build_segment_with_two_named_vecs, build_simple_segment,
};
use segment::types::{
    Condition, Distance, FieldCondition, Filter, HnswConfig, HnswGlobalConfig, PayloadSchemaType,
    SeqNumberType, WithPayload,
};
use tempfile::Builder;

const NUM_POINTS: u64 = 1_000;

fn match_filter(key: &str, value: i64) -> Filter {
    Filter::new_must(Condition::Field(FieldCondition::new_match(
        JsonPath::new(key),
        value.into(),
    )))
}

/// Points with `half` in `0..2` and `tenth` in `0..10`, both indexed
fn two_vector_segment(path: &std::path::Path, rng: &mut StdRng) -> Segment {
    let mut segment = build_segment_with_two_named_vecs(path, 4, 6, Distance::Dot).unwrap();
    for key in ["half", "tenth"] {
        segment
            .create_field_index(
                0,
                &JsonPath::new(key),
                Some(&PayloadSchemaType::Integer.into()),
            )
            .unwrap();
    }
    for n in 0..NUM_POINTS {
        let mut vectors = NamedVectors::default();
        vectors.insert(VECTOR1_NAME.to_owned(), random_vector(rng, 4).into());
        vectors.insert(VECTOR2_NAME.to_owned(), random_vector(rng, 6).into());
        segment
            .upsert_point(n as SeqNumberType, n.into(), vectors)
            .unwrap();
        let payload = payload_json! {"half": n % 2, "tenth": n % 10};
        segment
            .set_full_payload(n as SeqNumberType, n.into(), &payload)
            .unwrap();
    }
    segment
}

#[test]
fn test_shared_filter_matches_original_filter() {
    let _scope = ambient::test_guard();
    let mut rng = StdRng::seed_from_u64(42);
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let segment = two_vector_segment(dir.path(), &mut rng);

    let query_context = QueryContext::default();
    let segment_query_context = query_context.get_segment_query_context();

    let query1 = [QueryVector::from(random_vector(&mut rng, 4))];
    let query2 = [QueryVector::from(random_vector(&mut rng, 6))];
    let users = [
        FilterUser {
            vector_name: VECTOR1_NAME,
            query_vectors: &query1,
            top: 10,
            params: None,
        },
        FilterUser {
            vector_name: VECTOR2_NAME,
            query_vectors: &query2,
            top: 10,
            params: None,
        },
    ];

    for filter in [match_filter("half", 1), match_filter("tenth", 3)] {
        // Plain indexes check every candidate: two searches always share
        let shared = segment
            .share_filter(&filter, &users, &segment_query_context)
            .unwrap()
            .expect("two plain searches share their filter");

        // A single search never does
        let alone = segment
            .share_filter(&filter, &users[..1], &segment_query_context)
            .unwrap();
        assert!(alone.is_none());

        for (vector_name, query) in [(VECTOR1_NAME, &query1[0]), (VECTOR2_NAME, &query2[0])] {
            let search = |filter: &Filter| {
                segment
                    .search_batch(
                        vector_name,
                        &[query],
                        &WithPayload::default(),
                        &false.into(),
                        Some(filter),
                        10,
                        None,
                        &segment_query_context,
                    )
                    .unwrap()
            };
            let expected = search(&filter);
            assert!(!expected[0].is_empty());
            assert_eq!(search(&shared), expected);
        }
    }
}

#[test]
fn test_shared_filter_is_rejected_by_other_segment() {
    let _scope = ambient::test_guard();
    let mut rng = StdRng::seed_from_u64(42);
    let dir1 = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let dir2 = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let segment1 = two_vector_segment(dir1.path(), &mut rng);
    let segment2 = two_vector_segment(dir2.path(), &mut rng);

    let query_context = QueryContext::default();
    let segment_query_context = query_context.get_segment_query_context();

    let query = [QueryVector::from(random_vector(&mut rng, 4))];
    let user = FilterUser {
        vector_name: VECTOR1_NAME,
        query_vectors: &query,
        top: 10,
        params: None,
    };
    let shared = segment1
        .share_filter(
            &match_filter("half", 1),
            &[user, user],
            &segment_query_context,
        )
        .unwrap()
        .unwrap();

    let result = segment2.search_batch(
        VECTOR1_NAME,
        &[&query[0]],
        &WithPayload::default(),
        &false.into(),
        Some(&shared),
        10,
        None,
        &segment_query_context,
    );
    assert!(
        result.is_err(),
        "offsets of another segment must be rejected"
    );
}

/// HNSW checks every candidate below the full scan threshold and only the points it visits
/// above it. The resolved filter must give the same results as the filter on both paths.
#[test]
fn test_hnsw_with_resolved_filter() {
    let _scope = ambient::test_guard();
    let mut rng = StdRng::seed_from_u64(42);
    let dim = 8;
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let mut segment = build_simple_segment(dir.path(), dim, Distance::Cosine).unwrap();
    for key in ["half", "tenth"] {
        segment
            .create_field_index(
                0,
                &JsonPath::new(key),
                Some(&PayloadSchemaType::Integer.into()),
            )
            .unwrap();
    }
    for n in 0..NUM_POINTS {
        let vector = random_vector(&mut rng, dim);
        segment
            .upsert_point(n as SeqNumberType, n.into(), only_default_vector(&vector))
            .unwrap();
        let payload = payload_json! {"half": n % 2, "tenth": n % 10};
        segment
            .set_full_payload(n as SeqNumberType, n.into(), &payload)
            .unwrap();
    }

    // 10 KB of 8-dim f32 vectors: a full scan threshold of 320 points
    let hnsw_config = HnswConfig {
        memory: None,
        m: 8,
        ef_construct: 64,
        full_scan_threshold: 10,
        max_indexing_threads: 2,
        on_disk: Some(false),
        payload_m: None,
        inline_storage: None,
    };
    let permit_cpu_count = get_num_indexing_threads(hnsw_config.max_indexing_threads);
    let hnsw_dir = Builder::new().prefix("hnsw_dir").tempdir().unwrap();
    let stopped = AtomicBool::new(false);
    let vector_data = &segment.vector_data[DEFAULT_VECTOR_NAME];
    let hnsw_index = HNSWIndex::build(
        HnswIndexOpenArgs {
            path: hnsw_dir.path(),
            id_tracker: segment.id_tracker.clone(),
            vector_storage: vector_data.vector_storage.clone(),
            quantized_vectors: vector_data.quantized_vectors.clone(),
            payload_index: segment.payload_index.clone(),
            hnsw_config,
        },
        VectorIndexBuildArgs {
            permit: Arc::new(ResourcePermit::dummy(permit_cpu_count as u32)),
            old_indices: &[],
            gpu_device: None,
            rng: &mut rng,
            stopped: &stopped,
            hnsw_global_config: &HnswGlobalConfig::default(),
            feature_flags: FeatureFlags::default(),
            inline_vectors: false,
            progress: ProgressTracker::new_for_test(),
        },
    )
    .unwrap();

    let queries: Vec<QueryVector> = (0..3)
        .map(|_| QueryVector::from(random_vector(&mut rng, dim)))
        .collect();
    let query_refs: Vec<&QueryVector> = queries.iter().collect();

    // 500 matches: graph search, lazy checks. 100 matches: plain search, every candidate
    for (filter, graph) in [
        (match_filter("half", 1), true),
        (match_filter("tenth", 3), false),
    ] {
        let (cardinality, points) = segment.payload_index.borrow().with_view(|view| {
            let cardinality = view.estimate_cardinality(&filter).unwrap();
            let points = view.query_points(&filter, &stopped).unwrap();
            (cardinality, points)
        });

        let checks = hnsw_index
            .filter_checks(&queries, &cardinality, 10, None)
            .unwrap();
        match checks {
            FilterChecks::Lazy { expected_checks } => {
                assert!(graph, "{checks:?}");
                assert!(expected_checks > 0, "{checks:?}");
            }
            FilterChecks::Full => assert!(!graph, "{checks:?}"),
        }

        // Both set representations
        for prefer_bits in [true, false] {
            let context = PointSetContext {
                total_points: NUM_POINTS as usize,
                expected_checks: if prefer_bits { 1_000_000 } else { 0 },
            };
            let set = PointSet::union(vec![points.clone()], context);
            assert_eq!(set.len(), points.len());
            let resolved = Filter::new_must(Condition::ResolvedPoints(ResolvedPoints::new(
                segment.segment_uuid(),
                set,
            )));

            let resolved_cardinality = segment
                .payload_index
                .borrow()
                .with_view(|view| view.estimate_cardinality(&resolved).unwrap());
            let CardinalityEstimation {
                primary_clauses,
                min,
                exp,
                max,
            } = resolved_cardinality;
            assert_eq!(primary_clauses.len(), 1);
            assert_eq!((min, exp, max), (points.len(), points.len(), points.len()));

            let search = |filter: &Filter| {
                hnsw_index
                    .search(&query_refs, Some(filter), 10, None, &Default::default())
                    .unwrap()
            };
            let expected = search(&filter);
            assert!(expected.iter().all(|result| result.len() == 10));
            assert_eq!(search(&resolved), expected);
        }
    }
}

/// A materialized filter holds exactly the points the filter iterates, on every filtering path
/// (primary clauses only, primary clauses plus checks, full scan) and in both representations.
#[test]
fn test_materialize_filter_matches_query_points() {
    let _scope = ambient::test_guard();
    let mut rng = StdRng::seed_from_u64(42);
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let mut segment = two_vector_segment(dir.path(), &mut rng);

    // An unindexed field, and deleted points the posting lists still hold
    let op_num = NUM_POINTS as SeqNumberType;
    for n in 0..NUM_POINTS {
        let payload = payload_json! {"half": n % 2, "tenth": n % 10, "free": n % 3};
        segment
            .set_full_payload(op_num, n.into(), &payload)
            .unwrap();
    }
    for n in (0..NUM_POINTS).step_by(7) {
        segment.delete_point(op_num + 1, n.into()).unwrap();
    }

    let indexed = || match_filter("tenth", 3).must.unwrap().remove(0);
    let unindexed = || match_filter("free", 1).must.unwrap().remove(0);
    let filters = [
        // Primary clause only
        Filter::new_must(indexed()),
        // Several primary clauses, with points in both
        Filter {
            should: Some(vec![
                indexed(),
                match_filter("half", 1).must.unwrap().remove(0),
            ]),
            min_should: None,
            must: None,
            must_not: None,
        },
        // Primary clause and a checked condition
        Filter {
            should: None,
            min_should: None,
            must: Some(vec![indexed(), unindexed()]),
            must_not: None,
        },
        // Primary clause and a negated one
        Filter {
            should: None,
            min_should: None,
            must: Some(vec![indexed()]),
            must_not: Some(vec![unindexed()]),
        },
        // Full scan
        Filter::new_must(unindexed()),
    ];

    let stopped = AtomicBool::new(false);
    let payload_index = segment.payload_index.borrow();
    payload_index.with_view(|view| {
        for filter in &filters {
            let mut expected = view.query_points(filter, &stopped).unwrap();
            expected.sort_unstable();
            expected.dedup();
            assert!(!expected.is_empty(), "{filter:?}");

            let cardinality = view.estimate_cardinality(filter).unwrap();
            let set = view
                .materialize_filter(
                    filter,
                    &cardinality,
                    &stopped,
                    common::types::DeferredBehavior::VisibleOnly,
                )
                .unwrap();
            assert_eq!(set.iter().collect::<Vec<_>>(), expected, "{filter:?}");
        }

        // A stopped materialization fails instead of returning a partial set
        let cardinality = view.estimate_cardinality(&filters[0]).unwrap();
        let result = view.materialize_filter(
            &filters[0],
            &cardinality,
            &AtomicBool::new(true),
            common::types::DeferredBehavior::VisibleOnly,
        );
        assert!(result.is_err());
    });
}
