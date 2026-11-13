//! A filter resolved once for several searches of a batch must select the same points as the
//! filter itself, on every search strategy.

// Deprecated storage placement params (`on_disk`) are still handled here
#![allow(deprecated)]

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use common::ambient;
use common::budget::ResourcePermit;
use common::flags::FeatureFlags;
use common::progress_tracker::ProgressTracker;
use rand::prelude::StdRng;
use rand::{RngExt, SeedableRng};
use segment::common::operation_error::OperationResult;
use segment::data_types::filter_user::FilterUser;
use segment::data_types::named_vectors::NamedVectors;
use segment::data_types::query_context::QueryContext;
use segment::data_types::vectors::{DEFAULT_VECTOR_NAME, QueryVector, only_default_vector};
use segment::entry::entry_point::{NonAppendableSegmentEntry, ReadSegmentEntry, SegmentEntry};
use segment::fixtures::index_fixtures::random_vector;
use segment::index::field_index::CardinalityEstimation;
use segment::index::hnsw_index::get_num_indexing_threads;
use segment::index::hnsw_index::hnsw::{HNSWIndex, HnswIndexOpenArgs};
use segment::index::query_optimization::filter_checks::{FilterChecks, LazyCardinality};
use segment::index::query_optimization::point_set::{PointSet, PointSetContext};
use segment::index::query_optimization::resolved_points::ResolvedPoints;
use segment::index::{PayloadIndexRead, VectorIndexRead};
use segment::json_path::JsonPath;
use segment::payload_json;
use segment::segment::Segment;
use segment::segment_constructor::VectorIndexBuildArgs;
use segment::segment_constructor::segment_builder::SegmentBuilder;
use segment::segment_constructor::simple_segment_constructor::{
    VECTOR1_NAME, VECTOR2_NAME, build_segment_with_two_named_vecs, build_simple_segment,
};
use segment::types::{
    Condition, Distance, FieldCondition, Filter, HnswConfig, HnswGlobalConfig, Indexes, MinShould,
    PayloadSchemaType, Range, SearchParams, SegmentConfig, SeqNumberType, VectorDataConfig,
    VectorStorageType, WithPayload,
};
use tempfile::Builder;
use uuid::Uuid;

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
fn build_hnsw(
    segment: &Segment,
    hnsw_config: HnswConfig,
    path: &std::path::Path,
    rng: &mut StdRng,
) -> HNSWIndex {
    let permit_cpu_count = get_num_indexing_threads(hnsw_config.max_indexing_threads);
    let stopped = AtomicBool::new(false);
    let vector_data = &segment.vector_data[DEFAULT_VECTOR_NAME];
    HNSWIndex::build(
        HnswIndexOpenArgs {
            path,
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
            rng,
            stopped: &stopped,
            hnsw_global_config: &HnswGlobalConfig::default(),
            feature_flags: FeatureFlags::default(),
            inline_vectors: false,
            progress: ProgressTracker::new_for_test(),
        },
    )
    .unwrap()
}

/// A nested filter has a loose estimate, so the search samples to choose between plain and
/// graph search, while the resolved points are counted exactly. Near the full scan threshold
/// the two can disagree, and a graph search there finds other points than a plain one. The
/// resolved filter reports the original filter's estimate, so the search plans the same way.
#[test]
fn test_hnsw_resolved_filter_plans_like_original() {
    let _scope = ambient::test_guard();
    let mut rng = StdRng::seed_from_u64(1);
    let dim = 8;
    let num_points = 4_000;
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let mut segment = build_simple_segment(dir.path(), dim, Distance::Cosine).unwrap();
    segment
        .create_field_index(
            0,
            &JsonPath::new("items[].cat"),
            Some(&PayloadSchemaType::Integer.into()),
        )
        .unwrap();
    segment
        .create_field_index(
            0,
            &JsonPath::new("items[].val"),
            Some(&PayloadSchemaType::Integer.into()),
        )
        .unwrap();
    for n in 0..num_points {
        let vector = random_vector(&mut rng, dim);
        segment
            .upsert_point(n as SeqNumberType, n.into(), only_default_vector(&vector))
            .unwrap();
        let items: Vec<_> = (0..3)
            .map(|_| serde_json::json!({"cat": rng.random_range(0..5), "val": rng.random_range(0..1000)}))
            .collect();
        segment
            .set_full_payload(
                n as SeqNumberType,
                n.into(),
                &payload_json! {"items": items},
            )
            .unwrap();
    }

    // 16 KB of 8-dim f32 vectors: a full scan threshold of 512 points
    let hnsw_config = HnswConfig {
        memory: None,
        m: 16,
        ef_construct: 100,
        full_scan_threshold: 16,
        max_indexing_threads: 2,
        on_disk: Some(false),
        payload_m: None,
        inline_storage: None,
    };
    let hnsw_dir = Builder::new().prefix("hnsw_dir").tempdir().unwrap();
    let hnsw_index = build_hnsw(&segment, hnsw_config, hnsw_dir.path(), &mut rng);
    let stopped = AtomicBool::new(false);

    let queries: Vec<QueryVector> = (0..5)
        .map(|_| QueryVector::from(random_vector(&mut rng, dim)))
        .collect();
    let query_refs: Vec<&QueryVector> = queries.iter().collect();

    // From well below to well above the threshold
    for max_val in (100..=400).step_by(10) {
        let filter = Filter::new_must(Condition::new_nested(
            JsonPath::new("items"),
            Filter {
                must: Some(vec![
                    Condition::Field(FieldCondition::new_match(JsonPath::new("cat"), 0.into())),
                    Condition::Field(FieldCondition::new_range(
                        JsonPath::new("val"),
                        Range {
                            lt: Some(f64::from(max_val).into()),
                            ..Default::default()
                        },
                    )),
                ]),
                should: None,
                min_should: None,
                must_not: None,
            },
        ));
        let (cardinality, points) = segment.payload_index.borrow().with_view(|view| {
            let cardinality = view.estimate_cardinality(&filter).unwrap();
            let points = view.query_points(&filter, &stopped).unwrap();
            (cardinality, points)
        });
        let set = PointSet::union(
            vec![points],
            PointSetContext::by_density(num_points as usize),
        );
        let resolved = Filter::new_must(Condition::ResolvedPoints(ResolvedPoints::new(
            segment.segment_uuid(),
            set,
            &cardinality,
        )));

        let search = |filter: &Filter| {
            hnsw_index
                .search(&query_refs, Some(filter), 50, None, &Default::default())
                .unwrap()
        };
        assert_eq!(
            search(&resolved),
            search(&filter),
            "items[].val < {max_val}"
        );
    }
}

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
    let hnsw_dir = Builder::new().prefix("hnsw_dir").tempdir().unwrap();
    let hnsw_index = build_hnsw(&segment, hnsw_config, hnsw_dir.path(), &mut rng);
    let stopped = AtomicBool::new(false);

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
            .filter_checks(
                &queries,
                &LazyCardinality::known(cardinality.clone()),
                10,
                None,
            )
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
                &cardinality,
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
            // The original filter's estimate, so the search plans the same way
            assert_eq!(
                (min, exp, max),
                (cardinality.min, cardinality.exp, cardinality.max)
            );
            assert!(min <= points.len() && points.len() <= max);

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

/// A shared filter holds exactly the points the filter iterates, on every filtering path:
/// primary clauses only, primary clauses plus checks, full scan.
#[test]
fn test_shared_filter_holds_the_filtered_points() {
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

    let query_context = QueryContext::default();
    let segment_query_context = query_context.get_segment_query_context();
    let query = [QueryVector::from(random_vector(&mut rng, 4))];
    let user = FilterUser {
        vector_name: VECTOR1_NAME,
        query_vectors: &query,
        top: 10,
        params: None,
    };

    let stopped = AtomicBool::new(false);
    for filter in &filters {
        let mut expected = segment
            .payload_index
            .borrow()
            .with_view(|view| view.query_points(filter, &stopped).unwrap());
        expected.sort_unstable();
        expected.dedup();
        assert!(!expected.is_empty(), "{filter:?}");

        // Plain indexes check every candidate: two searches always share
        let shared = segment
            .share_filter(filter, &[user, user], &segment_query_context)
            .unwrap()
            .unwrap();
        let points = resolved_points(&shared);
        assert_eq!(points.len(), expected.len(), "{filter:?}");
        assert_eq!(points.iter().collect::<Vec<_>>(), expected, "{filter:?}");
    }

    // A stopped resolution fails instead of sharing a partial set
    let stopped_context = QueryContext::default().with_is_stopped(Arc::new(AtomicBool::new(true)));
    let result = segment.share_filter(
        &filters[0],
        &[user, user],
        &stopped_context.get_segment_query_context(),
    );
    assert!(result.is_err());
}

/// With `min_count > 1`, the primary clauses of `min_should` are only candidates: a condition
/// repeated in it makes every condition primary, yet the filter doesn't match their union.
#[test]
fn test_shared_filter_min_should_with_repeated_condition() {
    let _scope = ambient::test_guard();
    let mut rng = StdRng::seed_from_u64(42);
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let segment = two_vector_segment(dir.path(), &mut rng);

    // Larger and repeated, so it counts twice: the filter means `half = 1`
    let half = || match_filter("half", 1).must.unwrap().remove(0);
    // Smaller, and disjoint from `half = 1`
    let tenth = || match_filter("tenth", 2).must.unwrap().remove(0);
    let filter = Filter {
        should: None,
        min_should: Some(MinShould {
            conditions: vec![half(), tenth(), half()],
            min_count: 2,
        }),
        must: None,
        must_not: None,
    };

    let stopped = AtomicBool::new(false);
    let query_points = |filter: &Filter| {
        let mut points = segment
            .payload_index
            .borrow()
            .with_view(|view| view.query_points(filter, &stopped).unwrap());
        points.sort_unstable();
        points.dedup();
        points
    };
    let expected = query_points(&Filter::new_must(half()));
    assert_eq!(expected.len(), NUM_POINTS as usize / 2);
    assert_eq!(query_points(&filter), expected);

    let query_context = QueryContext::default();
    let segment_query_context = query_context.get_segment_query_context();
    let query = [QueryVector::from(random_vector(&mut rng, 4))];
    let user = FilterUser {
        vector_name: VECTOR1_NAME,
        query_vectors: &query,
        top: 10,
        params: None,
    };
    let shared = segment
        .share_filter(&filter, &[user, user], &segment_query_context)
        .unwrap()
        .unwrap();
    let points = resolved_points(&shared);
    assert_eq!(points.iter().collect::<Vec<_>>(), expected);
}

/// The points of a filter returned by `share_filter`
fn resolved_points(shared: &Filter) -> &PointSet {
    match shared.must.as_deref() {
        Some([Condition::ResolvedPoints(resolved)]) => resolved.points(),
        other => panic!("a shared filter is a single `ResolvedPoints` condition, got {other:?}"),
    }
}

/// `two_vector_segment` rebuilt with an HNSW index on both vectors. Their full scan thresholds
/// are 256 and 170 points: `half = 1` matches 500 points (graph search, lazy checks) and
/// `tenth = 3` matches 100 (plain search, every candidate checked).
fn hnsw_two_vector_segment(rng: &mut StdRng) -> (Segment, tempfile::TempDir, tempfile::TempDir) {
    let source_dir = Builder::new().prefix("source_dir").tempdir().unwrap();
    let source = two_vector_segment(source_dir.path(), rng);

    let hnsw_config = HnswConfig {
        memory: None,
        m: 8,
        ef_construct: 32,
        full_scan_threshold: 4,
        max_indexing_threads: 2,
        on_disk: Some(false),
        payload_m: None,
        inline_storage: None,
    };
    let vector_data = |name: &str| {
        let config = &source.segment_config.vector_data[name];
        (
            name.to_owned(),
            VectorDataConfig {
                size: config.size,
                distance: config.distance,
                storage_type: VectorStorageType::default(),
                index: Indexes::Hnsw(hnsw_config),
                quantization_config: None,
                multivector_config: None,
                datatype: None,
            },
        )
    };
    let segment_config = SegmentConfig {
        vector_data: HashMap::from([vector_data(VECTOR1_NAME), vector_data(VECTOR2_NAME)]),
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
    let permit = ResourcePermit::dummy(2);
    let segment = builder
        .build(
            out_dir.path(),
            Uuid::new_v4(),
            None,
            true,
            permit,
            &AtomicBool::new(false),
            rng,
            ProgressTracker::new_for_test(),
        )
        .unwrap();
    (segment, temp_dir, out_dir)
}

fn queries(rng: &mut StdRng, dim: usize, count: usize) -> Vec<QueryVector> {
    (0..count)
        .map(|_| QueryVector::from(random_vector(rng, dim)))
        .collect()
}

fn search_with(
    segment: &Segment,
    vector_name: &str,
    queries: &[QueryVector],
    filter: &Filter,
    top: usize,
    params: Option<&SearchParams>,
) -> Vec<Vec<segment::types::ScoredPoint>> {
    let query_context = QueryContext::default();
    let segment_query_context = query_context.get_segment_query_context();
    let refs: Vec<&QueryVector> = queries.iter().collect();
    segment
        .search_batch(
            vector_name,
            &refs,
            &WithPayload::default(),
            &false.into(),
            Some(filter),
            top,
            params,
            &segment_query_context,
        )
        .unwrap()
}

/// Smallest mean recall against an exact search that a shared-filter graph search must reach.
/// A 1000-point graph searched with a large `ef` is far above it, and a broken shared set is
/// far below.
const MIN_RECALL: f64 = 0.9;

/// Checks a graph search with the shared filter without comparing it to the unshared search.
/// A graph search is approximate, and its graph is built by several threads, so two equivalent
/// filters may legitimately return different near neighbours. What must hold is:
///
/// - every result satisfies the original filter (`point_matches` on the numeric id), and
///   there are `top` of them, so the shared set holds no wrong point and misses none that
///   matter;
/// - the mean recall against an exact search with the original filter is at least
///   [`MIN_RECALL`], so sharing doesn't make the search worse than an approximate one.
fn assert_shared_search_is_sound(
    segment: &Segment,
    vector_name: &str,
    queries: &[QueryVector],
    (filter, shared): (&Filter, &Filter),
    top: usize,
    params: &SearchParams,
    point_matches: impl Fn(u64) -> bool,
) {
    let results = search_with(segment, vector_name, queries, shared, top, Some(params));
    let exact_params = SearchParams {
        exact: true,
        ..Default::default()
    };
    let exact = search_with(
        segment,
        vector_name,
        queries,
        filter,
        top,
        Some(&exact_params),
    );

    let mut recall = 0.0;
    for (result, exact) in results.iter().zip(&exact) {
        assert_eq!(result.len(), top);
        assert!(result.iter().all(|point| match point.id {
            segment::types::PointIdType::NumId(n) => point_matches(n),
            segment::types::PointIdType::Uuid(_) => false,
        }));
        let found = exact
            .iter()
            .filter(|point| result.iter().any(|r| r.id == point.id))
            .count();
        recall += found as f64 / exact.len() as f64;
    }
    let recall = recall / results.len() as f64;
    assert!(recall >= MIN_RECALL, "recall {recall} against exact search");
}

/// One search call with several query vectors on a graph search checks the filter once per
/// vector, so a single user can share it; the shared search returns the same results.
#[test]
fn test_one_user_with_several_vectors_shares_on_graph_search() {
    let _scope = ambient::test_guard();
    let mut rng = StdRng::seed_from_u64(7);
    let (segment, _temp, _out) = hnsw_two_vector_segment(&mut rng);
    let query_context = QueryContext::default();
    let segment_query_context = query_context.get_segment_query_context();
    let filter = match_filter("half", 1);
    let batch = queries(&mut rng, 4, 6);
    let params = SearchParams {
        hnsw_ef: Some(200),
        ..Default::default()
    };
    let user = |query_vectors| FilterUser {
        vector_name: VECTOR1_NAME,
        query_vectors,
        top: 10,
        params: Some(&params),
    };

    // A single search never shares, with or without the change
    assert!(
        segment
            .share_filter(&filter, &[user(&batch[..1])], &segment_query_context)
            .unwrap()
            .is_none()
    );

    // Six searches on one vector do
    let shared = segment
        .share_filter(&filter, &[user(&batch)], &segment_query_context)
        .unwrap()
        .expect("six graph searches share their filter");

    let expected = search_with(&segment, VECTOR1_NAME, &batch, &filter, 10, Some(&params));
    assert_shared_search_is_sound(
        &segment,
        VECTOR1_NAME,
        &batch,
        (&filter, &shared),
        10,
        &params,
        |n| n % 2 == 1,
    );
    // ... and each as a search of its own
    for (index, query) in batch.iter().enumerate() {
        let alone = search_with(
            &segment,
            VECTOR1_NAME,
            std::slice::from_ref(query),
            &filter,
            10,
            Some(&params),
        );
        assert_eq!(alone[0], expected[index]);
    }
}

/// A filter below the full scan threshold takes the plain path, which checks every candidate
/// once for all query vectors: one user never shares, as before.
#[test]
fn test_one_user_with_several_vectors_does_not_share_on_plain_search() {
    let _scope = ambient::test_guard();
    let mut rng = StdRng::seed_from_u64(8);
    let (segment, _temp, _out) = hnsw_two_vector_segment(&mut rng);
    let query_context = QueryContext::default();
    let segment_query_context = query_context.get_segment_query_context();
    let batch = queries(&mut rng, 4, 6);
    let user = FilterUser {
        vector_name: VECTOR1_NAME,
        query_vectors: &batch,
        top: 10,
        params: None,
    };
    let shared = segment
        .share_filter(&match_filter("tenth", 3), &[user], &segment_query_context)
        .unwrap();
    assert!(shared.is_none());
}

/// The cost model declines when the searches check fewer points than resolving the filter
/// would: a tiny `ef` and `top` against a filter matching 500 points.
#[test]
fn test_cost_model_declines_to_share_cheap_graph_searches() {
    let _scope = ambient::test_guard();
    let mut rng = StdRng::seed_from_u64(9);
    let (segment, _temp, _out) = hnsw_two_vector_segment(&mut rng);
    let query_context = QueryContext::default();
    let segment_query_context = query_context.get_segment_query_context();
    let filter = match_filter("half", 1);
    let batch = queries(&mut rng, 4, 6);
    let cheap = SearchParams {
        hnsw_ef: Some(2),
        ..Default::default()
    };
    let shared = |params: &SearchParams, top| {
        segment
            .share_filter(
                &filter,
                &[FilterUser {
                    vector_name: VECTOR1_NAME,
                    query_vectors: &batch,
                    top,
                    params: Some(params),
                }],
                &segment_query_context,
            )
            .unwrap()
    };
    // 2 × 16 neighbours × 6 vectors checks, against about 500 candidates
    assert!(shared(&cheap, 1).is_none());
    // top == 0 runs no search at all
    assert!(shared(&cheap, 0).is_none());
    // The same batch with a large ef pays for resolving
    let costly = SearchParams {
        hnsw_ef: Some(200),
        ..Default::default()
    };
    assert!(shared(&costly, 10).is_some());

    // The declined search is the plain search of the original filter
    let expected = search_with(&segment, VECTOR1_NAME, &batch, &filter, 1, Some(&cheap));
    for (index, query) in batch.iter().enumerate() {
        let alone = search_with(
            &segment,
            VECTOR1_NAME,
            std::slice::from_ref(query),
            &filter,
            1,
            Some(&cheap),
        );
        assert_eq!(alone[0], expected[index]);
    }
}

/// A group with several vectors and another group with the same filter resolve it once, and
/// both searches see the same points.
#[test]
fn test_group_with_several_vectors_shares_with_another_group() {
    let _scope = ambient::test_guard();
    let mut rng = StdRng::seed_from_u64(10);
    let (segment, _temp, _out) = hnsw_two_vector_segment(&mut rng);
    let query_context = QueryContext::default();
    let segment_query_context = query_context.get_segment_query_context();
    let filter = match_filter("half", 0);
    let first = queries(&mut rng, 4, 3);
    let second = queries(&mut rng, 6, 1);
    let params = SearchParams {
        hnsw_ef: Some(200),
        ..Default::default()
    };
    let users = [
        FilterUser {
            vector_name: VECTOR1_NAME,
            query_vectors: &first,
            top: 10,
            params: Some(&params),
        },
        FilterUser {
            vector_name: VECTOR2_NAME,
            query_vectors: &second,
            top: 10,
            params: Some(&params),
        },
    ];
    let shared = segment
        .share_filter(&filter, &users, &segment_query_context)
        .unwrap()
        .expect("four graph searches share their filter");

    for (name, batch) in [(VECTOR1_NAME, &first), (VECTOR2_NAME, &second)] {
        assert_shared_search_is_sound(
            &segment,
            name,
            batch,
            (&filter, &shared),
            10,
            &params,
            |n| n % 2 == 0,
        );
    }
}

/// An estimation that fails the test if an index asks for it.
fn estimation_not_needed() -> LazyCardinality<'static> {
    LazyCardinality::new(|| -> OperationResult<CardinalityEstimation> {
        panic!("the filter's cardinality must not be estimated here")
    })
}

/// Searches whose way of evaluating the filter doesn't depend on how many points it matches
/// must say so without the estimation: it costs a pass over the payload index, and the search
/// repeats it.
#[test]
fn test_filter_checks_without_estimation() {
    let _scope = ambient::test_guard();
    let mut rng = StdRng::seed_from_u64(11);
    let batch = queries(&mut rng, 4, 3);

    // A plain index checks every candidate once per call
    let plain_dir = Builder::new().prefix("plain_dir").tempdir().unwrap();
    let plain = two_vector_segment(plain_dir.path(), &mut rng);
    let checks = plain.vector_data[VECTOR1_NAME]
        .vector_index
        .borrow()
        .filter_checks(&batch, &estimation_not_needed(), 10, None)
        .unwrap();
    assert_eq!(checks, FilterChecks::Full);

    // HNSW: an exact search, no query vectors and `top == 0`
    let (hnsw, _temp, _out) = hnsw_two_vector_segment(&mut rng);
    let index = hnsw.vector_data[VECTOR1_NAME].vector_index.borrow();
    let exact = SearchParams {
        exact: true,
        ..Default::default()
    };
    assert_eq!(
        index
            .filter_checks(&batch, &estimation_not_needed(), 10, Some(&exact))
            .unwrap(),
        FilterChecks::Full
    );
    let no_checks = FilterChecks::Lazy { expected_checks: 0 };
    assert_eq!(
        index
            .filter_checks(&batch, &estimation_not_needed(), 0, None)
            .unwrap(),
        no_checks
    );
    assert_eq!(
        index
            .filter_checks(&[], &estimation_not_needed(), 10, None)
            .unwrap(),
        no_checks
    );

    // A graph search of the same index does need it
    let estimation = LazyCardinality::known(CardinalityEstimation::exact(500));
    assert!(matches!(
        index.filter_checks(&batch, &estimation, 10, None).unwrap(),
        FilterChecks::Lazy { expected_checks } if expected_checks > 0
    ));
    assert!(estimation.is_computed());
}

/// An HNSW index without links (`m == 0`) searches plain: every candidate is checked.
#[test]
fn test_filter_checks_hnsw_disabled_without_estimation() {
    let _scope = ambient::test_guard();
    let mut rng = StdRng::seed_from_u64(12);
    let dim = 8;
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let mut segment = build_simple_segment(dir.path(), dim, Distance::Cosine).unwrap();
    for n in 0..200u64 {
        let vector = random_vector(&mut rng, dim);
        segment
            .upsert_point(n as SeqNumberType, n.into(), only_default_vector(&vector))
            .unwrap();
    }
    let hnsw_config = HnswConfig {
        memory: None,
        m: 0,
        ef_construct: 32,
        full_scan_threshold: 4,
        max_indexing_threads: 2,
        on_disk: Some(false),
        payload_m: None,
        inline_storage: None,
    };
    let hnsw_dir = Builder::new().prefix("hnsw_dir").tempdir().unwrap();
    let index = build_hnsw(&segment, hnsw_config, hnsw_dir.path(), &mut rng);
    let batch = queries(&mut rng, dim, 3);
    assert_eq!(
        index
            .filter_checks(&batch, &estimation_not_needed(), 10, None)
            .unwrap(),
        FilterChecks::Full
    );
}
