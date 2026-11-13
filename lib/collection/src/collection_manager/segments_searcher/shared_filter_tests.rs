//! A batch whose searches share a filter must return what each search returns on its own.
//!
//! The segments are built so that sharing really happens (`share_group_filters` resolves the
//! filter): HNSW indexes, filters matching far more points than the full scan threshold. Each
//! case asserts that, so a change of the cost model can't turn this into a test of the
//! unshared path. How much sharing saves is measured on the benchmark platform, not here.

// `on_disk` is deprecated, but a config literal must still set it
#![allow(deprecated)]

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use common::ambient;
use common::ambient::{AmbientContext, Handoff};
use common::budget::ResourcePermit;
use common::progress_tracker::ProgressTracker;
use itertools::Itertools;
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use segment::data_types::named_vectors::NamedVectors;
use segment::data_types::query_context::QueryContext;
use segment::data_types::vectors::{NamedQuery, VectorInternal};
use segment::entry::entry_point::{NonAppendableSegmentEntry, SegmentEntry};
use segment::index::sparse_index::sparse_index_config::{SparseIndexConfig, SparseIndexType};
use segment::json_path::JsonPath;
use segment::payload_json;
use segment::segment::Segment;
use segment::segment_constructor::build_segment;
use segment::segment_constructor::segment_builder::SegmentBuilder;
use segment::segment_constructor::simple_segment_constructor::{VECTOR1_NAME, VECTOR2_NAME};
use segment::types::{
    Condition, Distance, FieldCondition, Filter, HnswConfig, HnswGlobalConfig, Indexes, Nested,
    NestedCondition, PayloadSchemaType, PointIdType, Range, ScoredPoint, SearchParams,
    SegmentConfig, SeqNumberType, SparseVectorDataConfig, SparseVectorStorageType,
    VectorDataConfig, VectorStorageType,
};
use segment::vector_storage::query::{ContextPair, ContextQuery, DiscoverQuery, RecoQuery};
use shard::locked_segment::LockedSegment;
use shard::optimizers::config::DEFAULT_INDEXING_THRESHOLD_KB;
use shard::proxy_segment::ProxySegment;
use shard::query::query_enum::QueryEnum;
use shard::search::{CoreSearchRequestBatch, group_search_batches};
use shard::search_shared_filter::share_group_filters;
use shard::segment_holder::locked::LockedSegmentHolder;
use sparse::common::sparse_vector_fixture::random_sparse_vector;
use tempfile::{Builder, TempDir};
use uuid::Uuid;

use super::SegmentsSearcher;
use crate::collection_manager::fixtures::TEST_TIMEOUT;
use crate::collection_manager::holders::segment_holder::SegmentHolder;
use crate::common::adaptive_handle::AdaptiveSearchHandle;
use crate::operations::types::CoreSearchRequest;

const DIM: usize = 8;
const SPARSE_NAME: &str = "sparse";
const SPARSE_DIM: usize = 100;
const POINTS_PER_SEGMENT: u64 = 2_000;

/// Two dense vectors and a sparse one: plain dense indexes and a mutable sparse index for an
/// appendable segment, HNSW and an immutable sparse index for an optimized one.
///
/// The HNSW full scan threshold of 1 KB is 32 points of 32 bytes, and the sparse one is 50
/// points: the `tiny` filter is below both, the others far above.
fn segment_config(optimized: bool) -> SegmentConfig {
    let index = if optimized {
        Indexes::Hnsw(HnswConfig {
            memory: None,
            m: 8,
            ef_construct: 64,
            full_scan_threshold: 1,
            max_indexing_threads: 2,
            on_disk: Some(false),
            payload_m: None,
            inline_storage: None,
        })
    } else {
        Indexes::Plain {}
    };
    let dense = |name: &str| {
        (
            name.to_owned(),
            VectorDataConfig {
                size: DIM,
                distance: Distance::Cosine,
                storage_type: VectorStorageType::default(),
                index: index.clone(),
                quantization_config: None,
                multivector_config: None,
                datatype: None,
            },
        )
    };
    let sparse = SparseVectorDataConfig {
        index: SparseIndexConfig {
            full_scan_threshold: Some(50),
            index_type: if optimized {
                SparseIndexType::ImmutableRam
            } else {
                SparseIndexType::MutableRam
            },
            datatype: None,
            memory: None,
        },
        storage_type: SparseVectorStorageType::default(),
        modifier: None,
    };
    SegmentConfig {
        vector_data: HashMap::from([dense(VECTOR1_NAME), dense(VECTOR2_NAME)]),
        sparse_vector_data: HashMap::from([(SPARSE_NAME.to_owned(), sparse)]),
        payload_storage_type: Default::default(),
        id_tracker_memory: None,
    }
}

/// Points `first_id..first_id + POINTS_PER_SEGMENT` with:
/// - `half`: `n % 2`, matched by a flat filter on half of the points;
/// - `num`: random in `0..1000`, matched by a range filter, whose index iterates the points in
///   value order rather than offset order, and by the `tiny` filter on 0.1% of the points;
/// - `items[].k`: `n % 20` and `(n + 7) % 20`, matched by a nested filter on 10% of the points.
fn plain_segment(path: &std::path::Path, first_id: u64, rng: &mut StdRng) -> Segment {
    let (mut segment, _token) = build_segment(path, &segment_config(false), None, true).unwrap();
    for key in ["half", "num", "items[].k"] {
        segment
            .create_field_index(
                0,
                &JsonPath::new(key),
                Some(&PayloadSchemaType::Integer.into()),
            )
            .unwrap();
    }
    for n in first_id..first_id + POINTS_PER_SEGMENT {
        let mut vectors = NamedVectors::default();
        for name in [VECTOR1_NAME, VECTOR2_NAME, SPARSE_NAME] {
            vectors.insert(name.to_owned(), random_vector(name, rng));
        }
        let op = n as SeqNumberType;
        segment.upsert_point(op, n.into(), vectors).unwrap();
        let payload = payload_json! {
            "half": n % 2,
            "num": rng.random_range(0..1000),
            "items": [{"k": n % 20}, {"k": (n + 7) % 20}],
        };
        segment.set_full_payload(op, n.into(), &payload).unwrap();
    }
    segment
}

/// `plain_segment` rebuilt as an optimized segment: HNSW and an immutable sparse index.
fn hnsw_segment(first_id: u64, rng: &mut StdRng, dirs: &mut Vec<TempDir>) -> Segment {
    let source_dir = Builder::new().prefix("source").tempdir().unwrap();
    let source = plain_segment(source_dir.path(), first_id, rng);

    let temp_dir = Builder::new().prefix("builder_temp").tempdir().unwrap();
    let out_dir = Builder::new().prefix("builder_out").tempdir().unwrap();
    let mut builder = SegmentBuilder::new(
        temp_dir.path(),
        &segment_config(true),
        &HnswGlobalConfig::default(),
        Default::default(),
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
    dirs.extend([source_dir, temp_dir, out_dir]);
    segment
}

/// Three segments, each a way a filter is shared in:
/// - an HNSW segment;
/// - an HNSW segment behind a proxy that deleted some of its points;
/// - a plain segment, where one group of searches checks every candidate once and so doesn't
///   share.
struct Fixture {
    holder: LockedSegmentHolder,
    hnsw: LockedSegment,
    proxy: LockedSegment,
    _dirs: Vec<TempDir>,
}

fn fixture(rng: &mut StdRng) -> Fixture {
    let mut dirs = Vec::new();
    let hnsw = LockedSegment::new(hnsw_segment(0, rng, &mut dirs));

    let mut proxy = ProxySegment::new(LockedSegment::new(hnsw_segment(
        POINTS_PER_SEGMENT,
        rng,
        &mut dirs,
    )));
    // Every third point of the proxied segment, so deleted points would show in any result
    for (op, n) in (POINTS_PER_SEGMENT..2 * POINTS_PER_SEGMENT)
        .step_by(3)
        .enumerate()
    {
        proxy
            .delete_point(10_000 + op as SeqNumberType, n.into())
            .unwrap();
    }
    let proxy = LockedSegment::from(proxy);

    let plain_dir = Builder::new().prefix("plain").tempdir().unwrap();
    let plain = plain_segment(plain_dir.path(), 2 * POINTS_PER_SEGMENT, rng);
    dirs.push(plain_dir);

    let mut holder = SegmentHolder::default();
    holder.add_new_locked(hnsw.clone());
    holder.add_new_locked(proxy.clone());
    holder.add_new(plain);
    Fixture {
        holder: LockedSegmentHolder::new(holder),
        hnsw,
        proxy,
        _dirs: dirs,
    }
}

fn filters() -> [(&'static str, Filter); 4] {
    let flat = Filter::new_must(Condition::Field(FieldCondition::new_match(
        JsonPath::new("half"),
        1.into(),
    )));
    let range = Filter::new_must(Condition::Field(FieldCondition::new_range(
        JsonPath::new("num"),
        Range {
            lt: Some(300.0.into()),
            gt: None,
            gte: None,
            lte: None,
        },
    )));
    let nested = Filter::new_must(Condition::Nested(NestedCondition::new(Nested {
        key: JsonPath::new("items"),
        filter: Filter::new_must(Condition::Field(FieldCondition::new_match(
            JsonPath::new("k"),
            3.into(),
        ))),
    })));
    let tiny = Filter::new_must(Condition::Field(FieldCondition::new_match(
        JsonPath::new("num"),
        5.into(),
    )));
    [
        ("flat", flat),
        ("range", range),
        ("nested", nested),
        ("tiny", tiny),
    ]
}

fn random_vector(name: &str, rng: &mut StdRng) -> VectorInternal {
    if name == SPARSE_NAME {
        return VectorInternal::from(random_sparse_vector(rng, SPARSE_DIM));
    }
    let vector: Vec<f32> = (0..DIM).map(|_| rng.random_range(-1.0..1.0)).collect();
    VectorInternal::from(vector)
}

#[derive(Clone, Copy, Debug)]
enum Kind {
    Nearest,
    Recommend,
    Discover,
    Context,
}

fn query(kind: Kind, name: &str, rng: &mut StdRng) -> QueryEnum {
    let pair = |rng: &mut StdRng| ContextPair {
        positive: random_vector(name, rng),
        negative: random_vector(name, rng),
    };
    match kind {
        Kind::Nearest => QueryEnum::Nearest(NamedQuery::new(random_vector(name, rng), name)),
        Kind::Recommend => {
            let positives = vec![random_vector(name, rng), random_vector(name, rng)];
            let negatives = vec![random_vector(name, rng)];
            QueryEnum::RecommendBestScore(NamedQuery::new(
                RecoQuery::new(positives, negatives),
                name,
            ))
        }
        Kind::Discover => {
            let target = random_vector(name, rng);
            let pairs = vec![pair(rng), pair(rng)];
            QueryEnum::Discover(NamedQuery::new(DiscoverQuery::new(target, pairs), name))
        }
        Kind::Context => {
            let pairs = vec![pair(rng), pair(rng)];
            QueryEnum::Context(NamedQuery::new(ContextQuery::new(pairs), name))
        }
    }
}

/// A batch of searches, and whether the HNSW segments must share its filter.
struct Case {
    name: &'static str,
    /// `(kind, vector name, exact)` of each search, in batch order
    searches: Vec<(Kind, &'static str, bool)>,
    /// Whether the optimized segments share the filter of the large filters, if that is part
    /// of what the case checks
    shares: Option<bool>,
}

fn cases() -> Vec<Case> {
    let nearest1 = (Kind::Nearest, VECTOR1_NAME, false);
    let nearest2 = (Kind::Nearest, VECTOR2_NAME, false);
    vec![
        Case {
            name: "one search",
            searches: vec![nearest1],
            shares: Some(false),
        },
        Case {
            name: "one group of six searches",
            searches: vec![nearest1; 6],
            shares: Some(true),
        },
        Case {
            name: "one group of six searches and a search on another vector",
            searches: [vec![nearest1; 6], vec![nearest2]].concat(),
            shares: Some(true),
        },
        Case {
            name: "two vectors interleaved, one search per group",
            searches: vec![nearest1, nearest2, nearest1, nearest2],
            shares: Some(true),
        },
        Case {
            name: "one group of three discover searches",
            searches: vec![(Kind::Discover, VECTOR1_NAME, false); 3],
            shares: Some(true),
        },
        Case {
            name: "one group of three context searches",
            searches: vec![(Kind::Context, VECTOR1_NAME, false); 3],
            shares: Some(true),
        },
        Case {
            name: "one group of three recommend searches",
            searches: vec![(Kind::Recommend, VECTOR1_NAME, false); 3],
            shares: None,
        },
        Case {
            name: "one group of six sparse searches",
            searches: vec![(Kind::Nearest, SPARSE_NAME, false); 6],
            shares: None,
        },
        Case {
            name: "one group of three sparse recommend searches",
            searches: vec![(Kind::Recommend, SPARSE_NAME, false); 3],
            shares: None,
        },
        Case {
            name: "one group of six exact searches",
            searches: vec![(Kind::Nearest, VECTOR1_NAME, true); 6],
            shares: Some(false),
        },
    ]
}

fn requests(
    case: &Case,
    filter: &Filter,
    limit: usize,
    rng: &mut StdRng,
) -> Vec<CoreSearchRequest> {
    case.searches
        .iter()
        .map(|&(kind, name, exact)| CoreSearchRequest {
            query: query(kind, name, rng),
            filter: Some(filter.clone()),
            params: Some(SearchParams {
                hnsw_ef: Some(64),
                exact,
                quantization: None,
                indexed_only: false,
                acorn: None,
                idf: None,
            }),
            limit,
            offset: 0,
            with_payload: None,
            with_vector: None,
            score_threshold: None,
        })
        .collect()
}

/// Whether `segment` resolves the filter of `searches` for at least one group, as
/// `search_in_segment` does before searching.
fn segment_shares(segment: &LockedSegment, searches: &[CoreSearchRequest]) -> bool {
    let _scope = ambient::test_guard();
    let query_context = QueryContext::new(
        DEFAULT_INDEXING_THRESHOLD_KB,
        Handoff::Measured(AmbientContext::new()),
    );
    let segment_query_context = query_context.get_segment_query_context();
    let groups = group_search_batches(searches);
    let read = segment.get_read().read();
    share_group_filters(&groups, &*read, &segment_query_context)
        .unwrap()
        .iter()
        .any(Option::is_some)
}

/// Whether two results hold the same scores in the same order, and the same points for every
/// score but the last. Points of the last score are a tie cut by the limit, which any order of
/// iterating the candidates may cut differently: `Context` queries score many points 0.
fn same_results(a: &[ScoredPoint], b: &[ScoredPoint]) -> bool {
    let scores = |result: &[ScoredPoint]| result.iter().map(|point| point.score).collect_vec();
    if scores(a) != scores(b) {
        return false;
    }
    let Some(last) = a.last().map(|point| point.score) else {
        return true;
    };
    let above_tie = |result: &[ScoredPoint]| {
        result
            .iter()
            .filter(|point| point.score != last)
            .map(|point| (point.id, point.version))
            .sorted()
            .collect_vec()
    };
    above_tie(a) == above_tie(b)
}

#[tokio::test]
async fn test_batch_with_shared_filter_equals_single_searches() {
    let mut rng = StdRng::seed_from_u64(42);
    let fixture = {
        let _scope = ambient::test_guard();
        fixture(&mut rng)
    };
    // The index build draws from `rng` a varying number of times: the queries get their own
    let mut rng = StdRng::seed_from_u64(43);
    let handle = AdaptiveSearchHandle::current_for_tests();
    let search = |searches: Vec<CoreSearchRequest>| {
        SegmentsSearcher::search(
            fixture.holder.clone(),
            Arc::new(CoreSearchRequestBatch { searches }),
            &handle,
            false,
            QueryContext::new(
                DEFAULT_INDEXING_THRESHOLD_KB,
                Handoff::Measured(AmbientContext::new()),
            ),
            TEST_TIMEOUT,
        )
    };

    let mut mismatches = Vec::new();
    for (filter_name, filter) in filters() {
        for case in cases() {
            for limit in [10, 100] {
                let searches = requests(&case, &filter, limit, &mut rng);
                // A filter below the full scan threshold is checked on every candidate: a single
                // group of searches checks it once and doesn't share
                let shares = if filter_name == "tiny" {
                    (group_search_batches(&searches).len() == 1).then_some(false)
                } else {
                    case.shares
                };
                for segment in [&fixture.hnsw, &fixture.proxy] {
                    if let Some(shares) = shares {
                        assert_eq!(
                            segment_shares(segment, &searches),
                            shares,
                            "{filter_name} filter, {}: sharing",
                            case.name,
                        );
                    }
                }

                let batched = search(searches.clone()).await.unwrap();
                assert_eq!(batched.len(), searches.len());
                // A sparse vector scores only the points it shares a dimension with, which the
                // few points of a small filter may not: some searches of a batch can find
                // nothing, but not all of them
                assert!(
                    filter_name == "tiny" || batched.iter().any(|result| !result.is_empty()),
                    "{filter_name} filter, {}: nothing found",
                    case.name,
                );
                for (index, (request, batched)) in searches.into_iter().zip(batched).enumerate() {
                    let alone = search(vec![request]).await.unwrap().remove(0);
                    let deleted = |point: &ScoredPoint| match point.id {
                        PointIdType::NumId(n) => {
                            (POINTS_PER_SEGMENT..2 * POINTS_PER_SEGMENT).contains(&n)
                                && (n - POINTS_PER_SEGMENT).is_multiple_of(3)
                        }
                        PointIdType::Uuid(_) => false,
                    };
                    assert!(!batched.iter().any(deleted), "a deleted point is found");
                    if !same_results(&batched, &alone) {
                        mismatches.push(format!(
                            "{filter_name} filter, {}, limit {limit}, search {index}",
                            case.name,
                        ));
                    }
                }
            }
        }
    }
    assert!(
        mismatches.is_empty(),
        "batched results differ from single searches:\n{}",
        mismatches.join("\n"),
    );
}
