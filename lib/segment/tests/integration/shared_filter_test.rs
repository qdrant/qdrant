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
use ordered_float::OrderedFloat;
use rand::prelude::StdRng;
use rand::{RngExt, SeedableRng};
use segment::data_types::filter_user::FilterUser;
use segment::data_types::index::{
    GeoIndexParams, GeoIndexType, IntegerIndexParams, IntegerIndexType,
};
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
use segment::index::query_optimization::resolve_filter::resolve_for_query_vectors;
use segment::index::query_optimization::resolved_points::ResolvedPoints;
use segment::index::{PayloadIndexRead, VectorIndexRead};
use segment::json_path::JsonPath;
use segment::payload_json;
use segment::payload_storage::PayloadStorageRead;
use segment::segment::Segment;
use segment::segment_constructor::segment_builder::SegmentBuilder;
use segment::segment_constructor::simple_segment_constructor::{
    VECTOR1_NAME, VECTOR2_NAME, build_segment_with_two_named_vecs, build_simple_segment,
};
use segment::segment_constructor::{VectorIndexBuildArgs, build_segment};
use segment::types::{
    Condition, Distance, FieldCondition, Filter, GeoPoint, GeoRadius, HnswConfig, HnswGlobalConfig,
    Indexes, Memory, MinShould, PayloadFieldSchema, PayloadSchemaParams, PayloadSchemaType,
    PayloadStorageType, Range, SearchParams, SegmentConfig, SeqNumberType, VectorDataConfig,
    VectorStorageType, WithPayload,
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

/// Build an optimized two-vector HNSW segment with configurable payload storage and field index
/// memory placement so tests can contrast hot vs cold lazy filter checks around the in-RAM
/// break-even threshold.
fn two_vector_hnsw_segment(
    out_dir: &std::path::Path,
    payload_storage_type: PayloadStorageType,
    index_memory: Memory,
    rng: &mut StdRng,
) -> Segment {
    const DIM: usize = 8;
    const POINTS: u64 = 4_000;

    let make_config = |index: Indexes| {
        let vec_cfg = VectorDataConfig {
            size: DIM,
            distance: Distance::Cosine,
            storage_type: VectorStorageType::InRamChunkedMmap,
            index,
            quantization_config: None,
            multivector_config: None,
            datatype: None,
        };
        SegmentConfig {
            vector_data: HashMap::from([
                (VECTOR1_NAME.to_owned(), vec_cfg.clone()),
                (VECTOR2_NAME.to_owned(), vec_cfg),
            ]),
            sparse_vector_data: Default::default(),
            payload_storage_type,
            id_tracker_memory: None,
        }
    };

    let plain_dir = Builder::new().prefix("plain").tempdir().unwrap();
    let (mut plain, _) = build_segment(
        plain_dir.path(),
        &make_config(Indexes::Plain {}),
        None,
        true,
    )
    .unwrap();

    for n in 0..POINTS {
        let mut vectors = NamedVectors::default();
        vectors.insert(VECTOR1_NAME.to_owned(), random_vector(rng, DIM).into());
        vectors.insert(VECTOR2_NAME.to_owned(), random_vector(rng, DIM).into());
        let op = n as SeqNumberType + 1;
        plain.upsert_point(op, n.into(), vectors).unwrap();

        let items: Vec<_> = (0..2)
            .map(|i| serde_json::json!({"cat": (n + i) % 3, "val": (n * 7 + i) % 100}))
            .collect();
        let lon = 13.0 + rng.random_range(0.0..0.8);
        let lat = 52.1 + rng.random_range(0.0..0.8);
        let payload = payload_json! {
            "half": n % 2,
            "score": (n % 100) as i64,
            "loc": {"lon": lon, "lat": lat},
            "items": items,
        };
        plain.set_full_payload(op, n.into(), &payload).unwrap();
    }

    let op = POINTS as SeqNumberType + 1;
    let int_schema =
        PayloadFieldSchema::FieldParams(PayloadSchemaParams::Integer(IntegerIndexParams {
            r#type: IntegerIndexType::Integer,
            lookup: Some(true),
            range: Some(true),
            memory: Some(index_memory),
            enable_hnsw: Some(false),
            ..Default::default()
        }));
    for key in ["half", "score"] {
        plain
            .create_field_index(op, &JsonPath::new(key), Some(&int_schema))
            .unwrap();
    }
    let geo_schema = PayloadFieldSchema::FieldParams(PayloadSchemaParams::Geo(GeoIndexParams {
        r#type: GeoIndexType::Geo,
        memory: Some(index_memory),
        enable_hnsw: Some(false),
        ..Default::default()
    }));
    plain
        .create_field_index(op, &JsonPath::new("loc"), Some(&geo_schema))
        .unwrap();

    // Small m and low full_scan_threshold (1 KB = 32 8-dim f32 vectors):
    // graph search with ef=16 has 16 * 8 = 128 checks per query (+ 1000 sampling when min <= 32),
    // so two groups stay below 4_000 in RAM (2 * 1_128 = 2_256 < 4_000) but cross the threshold
    // once weighted by 64 on cold storage.
    let hnsw_config = HnswConfig {
        memory: None,
        m: 4,
        ef_construct: 32,
        full_scan_threshold: 1,
        max_indexing_threads: 2,
        on_disk: Some(false),
        payload_m: None,
        inline_storage: None,
    };

    let temp_dir = Builder::new().prefix("builder").tempdir().unwrap();
    let mut builder = SegmentBuilder::new(
        temp_dir.path(),
        &make_config(Indexes::Hnsw(hnsw_config)),
        &HnswGlobalConfig::default(),
        FeatureFlags::default(),
    )
    .unwrap();
    builder.update(&[&plain], &AtomicBool::new(false)).unwrap();
    builder.build_for_test(out_dir)
}

/// On cold payload storage (`Mmap`), a `nested` filter without per-element indexes reads
/// payload from disk on every lazy graph check. Weighting lazy checks by 64 lets two
/// graph-route groups share above the in-RAM break-even, while hot payload storage
/// (`InRamMmap`) declines at the exact same check count, and shared search results match
/// searching with the original filter.
#[test]
fn test_cold_payload_storage_shares_nested_filter_above_ram_break_even() {
    let _scope = ambient::test_guard();
    let cold_dir = Builder::new().prefix("cold_seg").tempdir().unwrap();
    let hot_dir = Builder::new().prefix("hot_seg").tempdir().unwrap();

    let cold_segment = two_vector_hnsw_segment(
        cold_dir.path(),
        PayloadStorageType::Mmap,
        Memory::Cached,
        &mut StdRng::seed_from_u64(7),
    );
    let hot_segment = two_vector_hnsw_segment(
        hot_dir.path(),
        PayloadStorageType::InRamMmap,
        Memory::Cached,
        &mut StdRng::seed_from_u64(7),
    );

    assert!(cold_segment.payload_storage.borrow().is_cold());
    assert!(!hot_segment.payload_storage.borrow().is_cold());

    let nested_filter = Filter::new_must(Condition::new_nested(
        JsonPath::new("items"),
        Filter::new_must(Condition::Field(FieldCondition::new_match(
            JsonPath::new("cat"),
            1.into(),
        ))),
    ));

    let mut rng = StdRng::seed_from_u64(99);
    let query1 = [QueryVector::from(random_vector(&mut rng, 8))];
    let query2 = [QueryVector::from(random_vector(&mut rng, 8))];
    let params = SearchParams {
        hnsw_ef: Some(16),
        ..Default::default()
    };
    let users = [
        FilterUser {
            vector_name: VECTOR1_NAME,
            query_vectors: &query1,
            top: 10,
            params: Some(&params),
        },
        FilterUser {
            vector_name: VECTOR2_NAME,
            query_vectors: &query2,
            top: 10,
            params: Some(&params),
        },
    ];

    let query_context = QueryContext::default();
    let segment_query_context = query_context.get_segment_query_context();

    // Hot payload storage stays below in-RAM break-even: declines to share.
    assert!(
        hot_segment
            .share_filter(&nested_filter, &users, &segment_query_context)
            .unwrap()
            .is_none(),
        "hot payload storage should not share below the in-RAM break-even"
    );

    // Cold payload storage weights lazy checks by 64: shares the nested filter.
    let shared = cold_segment
        .share_filter(&nested_filter, &users, &segment_query_context)
        .unwrap()
        .expect("cold payload storage must share nested filter above the in-RAM break-even");

    for (vector_name, query) in [(VECTOR1_NAME, &query1[0]), (VECTOR2_NAME, &query2[0])] {
        let search = |filter: &Filter| {
            cold_segment
                .search_batch(
                    vector_name,
                    &[query],
                    &WithPayload::default(),
                    &false.into(),
                    Some(filter),
                    10,
                    Some(&params),
                    &segment_query_context,
                )
                .unwrap()
        };
        let expected = search(&nested_filter);
        assert!(!expected[0].is_empty());
        assert_eq!(search(&shared), expected);
    }
}

/// Cold field indexes without serial posting-list materialization (`geo`) weight lazy checks
/// and trigger sharing even when payload storage is in RAM, whereas cold `match` / `range`
/// indexes are materialized into a `PointSet` up front by the serial filter context and stay
/// unweighted.
#[test]
fn test_cold_geo_index_shares_while_cold_materialized_index_does_not() {
    let _scope = ambient::test_guard();
    let cold_idx_dir = Builder::new().prefix("cold_idx_seg").tempdir().unwrap();
    let hot_idx_dir = Builder::new().prefix("hot_idx_seg").tempdir().unwrap();

    // Hot payload storage isolates the effect of field index coldness.
    let cold_idx_segment = two_vector_hnsw_segment(
        cold_idx_dir.path(),
        PayloadStorageType::InRamMmap,
        Memory::Cold,
        &mut StdRng::seed_from_u64(11),
    );
    let hot_idx_segment = two_vector_hnsw_segment(
        hot_idx_dir.path(),
        PayloadStorageType::InRamMmap,
        Memory::Cached,
        &mut StdRng::seed_from_u64(11),
    );

    let geo_filter = Filter::new_must(Condition::Field(FieldCondition::new_geo_radius(
        JsonPath::new("loc"),
        GeoRadius {
            center: GeoPoint::new_unchecked(13.4, 52.5),
            radius: OrderedFloat(60_000.0),
        },
    )));
    let cold_match_filter = match_filter("half", 1);
    let cold_range_filter = Filter::new_must(Condition::Field(FieldCondition::new_range(
        JsonPath::new("score"),
        Range {
            gte: Some(OrderedFloat(20.0)),
            ..Default::default()
        },
    )));

    let mut rng = StdRng::seed_from_u64(123);
    let query1 = [QueryVector::from(random_vector(&mut rng, 8))];
    let query2 = [QueryVector::from(random_vector(&mut rng, 8))];
    let params = SearchParams {
        hnsw_ef: Some(16),
        ..Default::default()
    };
    let users = [
        FilterUser {
            vector_name: VECTOR1_NAME,
            query_vectors: &query1,
            top: 10,
            params: Some(&params),
        },
        FilterUser {
            vector_name: VECTOR2_NAME,
            query_vectors: &query2,
            top: 10,
            params: Some(&params),
        },
    ];

    let query_context = QueryContext::default();
    let segment_query_context = query_context.get_segment_query_context();

    // Hot geo index stays below break-even.
    assert!(
        hot_idx_segment
            .share_filter(&geo_filter, &users, &segment_query_context)
            .unwrap()
            .is_none(),
        "hot geo index should not share below the in-RAM break-even"
    );

    // Cold geo index stays lazy on disk: weighted by 64 and shares.
    let shared_geo = cold_idx_segment
        .share_filter(&geo_filter, &users, &segment_query_context)
        .unwrap()
        .expect("cold geo index must share above the in-RAM break-even");

    for (vector_name, query) in [(VECTOR1_NAME, &query1[0]), (VECTOR2_NAME, &query2[0])] {
        let search = |filter: &Filter| {
            cold_idx_segment
                .search_batch(
                    vector_name,
                    &[query],
                    &WithPayload::default(),
                    &false.into(),
                    Some(filter),
                    10,
                    Some(&params),
                    &segment_query_context,
                )
                .unwrap()
        };
        let expected = search(&geo_filter);
        assert!(!expected[0].is_empty());
        assert_eq!(search(&shared_geo), expected);
    }

    // Cold match and range conditions are materialized up front for serial checks:
    // lazy checks are in-RAM bit tests, so they must NOT receive the 64x cold weight.
    assert!(
        cold_idx_segment
            .share_filter(&cold_match_filter, &users, &segment_query_context)
            .unwrap()
            .is_none(),
        "cold match filter is materialized by serial checks and must not be weighted as cold lazy"
    );
    assert!(
        cold_idx_segment
            .share_filter(&cold_range_filter, &users, &segment_query_context)
            .unwrap()
            .is_none(),
        "cold range filter is materialized by serial checks and must not be weighted as cold lazy"
    );
}

/// Same-vector batch filter sharing (`resolve_for_query_vectors`) uses the same cost model
/// as `share_filter`: cold lazy checks are weighted by 64, while hot lazy checks, `Full`,
/// and zero-check `Lazy` stay unweighted, and batched search matches single-query search.
#[test]
fn test_resolve_for_query_vectors_weights_cold_lazy_checks() {
    let _scope = ambient::test_guard();
    let cold_dir = Builder::new().prefix("cold_qv_seg").tempdir().unwrap();
    let hot_dir = Builder::new().prefix("hot_qv_seg").tempdir().unwrap();

    let cold_segment = two_vector_hnsw_segment(
        cold_dir.path(),
        PayloadStorageType::Mmap,
        Memory::Cached,
        &mut StdRng::seed_from_u64(19),
    );
    let hot_segment = two_vector_hnsw_segment(
        hot_dir.path(),
        PayloadStorageType::InRamMmap,
        Memory::Cached,
        &mut StdRng::seed_from_u64(19),
    );

    let nested_filter = Filter::new_must(Condition::new_nested(
        JsonPath::new("items"),
        Filter::new_must(Condition::Field(FieldCondition::new_match(
            JsonPath::new("cat"),
            2.into(),
        ))),
    ));
    let stopped = AtomicBool::new(false);

    // 256 checks < 4_000 candidates (unweighted declines) < 256 * 64 = 16_384 (cold shares).
    let lazy_checks = FilterChecks::Lazy {
        expected_checks: 256,
    };
    let hot_resolved = hot_segment.payload_index.borrow().with_view(|view| {
        let cardinality = view.estimate_cardinality(&nested_filter).unwrap();
        resolve_for_query_vectors(
            &view,
            4_000,
            &nested_filter,
            &cardinality,
            lazy_checks,
            &stopped,
        )
        .unwrap()
    });
    assert!(
        hot_resolved.is_none(),
        "hot payload storage should not resolve below in-RAM break-even"
    );

    let cold_resolved = cold_segment.payload_index.borrow().with_view(|view| {
        let cardinality = view.estimate_cardinality(&nested_filter).unwrap();
        assert!(
            resolve_for_query_vectors(
                &view,
                4_000,
                &nested_filter,
                &cardinality,
                FilterChecks::Full,
                &stopped,
            )
            .unwrap()
            .is_none(),
            "single Full check must never trigger resolution"
        );
        assert!(
            resolve_for_query_vectors(
                &view,
                4_000,
                &nested_filter,
                &cardinality,
                FilterChecks::Lazy { expected_checks: 0 },
                &stopped,
            )
            .unwrap()
            .is_none(),
            "zero-check Lazy must never trigger resolution"
        );
        resolve_for_query_vectors(
            &view,
            4_000,
            &nested_filter,
            &cardinality,
            lazy_checks,
            &stopped,
        )
        .unwrap()
    });
    assert!(
        cold_resolved.is_some(),
        "cold payload storage must resolve when weighted lazy checks exceed candidates"
    );

    // Same-vector batch search of 2 queries through HNSW matches single-query searches.
    let mut rng = StdRng::seed_from_u64(202);
    let q1 = QueryVector::from(random_vector(&mut rng, 8));
    let q2 = QueryVector::from(random_vector(&mut rng, 8));
    let params = SearchParams {
        hnsw_ef: Some(16),
        ..Default::default()
    };
    let query_context = QueryContext::default();
    let segment_query_context = query_context.get_segment_query_context();

    let batched = cold_segment
        .search_batch(
            VECTOR1_NAME,
            &[&q1, &q2],
            &WithPayload::default(),
            &false.into(),
            Some(&nested_filter),
            10,
            Some(&params),
            &segment_query_context,
        )
        .unwrap();
    let alone_1 = cold_segment
        .search_batch(
            VECTOR1_NAME,
            &[&q1],
            &WithPayload::default(),
            &false.into(),
            Some(&nested_filter),
            10,
            Some(&params),
            &segment_query_context,
        )
        .unwrap();
    let alone_2 = cold_segment
        .search_batch(
            VECTOR1_NAME,
            &[&q2],
            &WithPayload::default(),
            &false.into(),
            Some(&nested_filter),
            10,
            Some(&params),
            &segment_query_context,
        )
        .unwrap();
    assert!(!batched[0].is_empty());
    assert!(!batched[1].is_empty());
    assert_eq!(batched[0], alone_1[0]);
    assert_eq!(batched[1], alone_2[0]);
}
