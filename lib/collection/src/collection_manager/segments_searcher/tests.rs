#![expect(clippy::wildcard_enum_match_arm, reason = "test code")]

use std::assert_matches;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

use ahash::AHashSet;
use api::rest::SearchRequestInternal;
use common::ambient;
use common::ambient::hw::HwMetric;
use common::ambient::{AmbientContext, AmbientFutureExt, Handoff};
use common::types::{DeferredBehavior, ScoreType};
use segment::common::operation_error::OperationError;
use segment::data_types::query_context::QueryContext;
use segment::data_types::vectors::DEFAULT_VECTOR_NAME;
use segment::fixtures::index_fixtures::random_vector;
use segment::index::VectorIndexEnum;
use segment::index::field_index::full_text_index::Bm25Params;
use segment::json_path::JsonPath;
use segment::types::{
    Condition, Filter, HasIdCondition, PointIdType, ScoredPoint, WithPayload, WithVector,
};
use shard::common::stopping_guard::StoppingGuard;
use shard::optimizers::config::DEFAULT_INDEXING_THRESHOLD_KB;
use shard::retrieve::retrieve_blocking::retrieve_blocking;
use shard::search::CoreSearchRequestBatch;
use shard::segment_holder::locked::LockedSegmentHolder;
use tempfile::Builder;

use super::SegmentsSearcher;
use crate::collection_manager::fixtures::{TEST_TIMEOUT, build_test_holder, random_segment};
use crate::collection_manager::holders::segment_holder::SegmentHolder;
use crate::common::adaptive_handle::AdaptiveSearchHandle;
use crate::operations::types::CoreSearchRequest;

#[test]
fn test_is_indexed_enough_condition() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();

    let segment1 = random_segment(dir.path(), 10, 200, 256);

    let vector_index = segment1
        .vector_data
        .get(DEFAULT_VECTOR_NAME)
        .unwrap()
        .vector_index
        .clone();

    let vector_index_borrow = vector_index.borrow();

    let _scope = ambient::test_guard();

    match &*vector_index_borrow {
        VectorIndexEnum::Plain(plain_index) => {
            let res_1 = plain_index
                .is_small_enough_for_unindexed_search(25, None)
                .unwrap();
            assert!(!res_1);

            let res_2 = plain_index
                .is_small_enough_for_unindexed_search(225, None)
                .unwrap();
            assert!(res_2);

            let ids: AHashSet<_> = vec![1, 2].into_iter().map(PointIdType::from).collect();

            let ids_filter = Filter::new_must(Condition::HasId(HasIdCondition::from(ids)));

            let res_3 = plain_index
                .is_small_enough_for_unindexed_search(25, Some(&ids_filter))
                .unwrap();
            assert!(res_3);
        }
        _ => panic!("Expected plain index"),
    }
}

#[tokio::test]
async fn test_segments_search() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();

    let segment_holder = build_test_holder(dir.path());

    let query = vec![1.0, 1.0, 1.0, 1.0];

    let req = CoreSearchRequest {
        query: query.into(),
        with_payload: None,
        with_vector: None,
        filter: None,
        params: None,
        limit: 5,
        score_threshold: None,
        offset: 0,
    };

    let batch_request = CoreSearchRequestBatch {
        searches: vec![req],
    };

    let ctx = AmbientContext::new();
    let result = SegmentsSearcher::search(
        segment_holder,
        Arc::new(batch_request),
        &AdaptiveSearchHandle::current_for_tests(),
        true,
        QueryContext::new(DEFAULT_INDEXING_THRESHOLD_KB, Handoff::Measured(ctx)),
        TEST_TIMEOUT,
    )
    .await
    .unwrap()
    .into_iter()
    .next()
    .unwrap();

    // eprintln!("result = {:?}", &result);

    assert_eq!(result.len(), 5);

    assert!(result[0].id == 3.into() || result[0].id == 11.into());
    assert!(result[1].id == 3.into() || result[1].id == 11.into());
}

/// Groups of one batch sharing a filter get it resolved to ids once per segment; the
/// results must match searching each request on its own.
#[tokio::test]
async fn test_segments_search_shared_filter() {
    use segment::data_types::vectors::{NamedQuery, VectorInternal};
    use segment::json_path::JsonPath;
    use segment::segment_constructor::simple_segment_constructor::{VECTOR1_NAME, VECTOR2_NAME};
    use segment::types::FieldCondition;
    use shard::query::query_enum::QueryEnum;

    use crate::collection_manager::fixtures::random_multi_vec_segment;

    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let mut holder = SegmentHolder::default();
    holder.add_new(random_multi_vec_segment(dir.path(), 10, 300, 4, 6));
    holder.add_new(random_multi_vec_segment(dir.path(), 11, 300, 4, 6));
    let holder = LockedSegmentHolder::new(holder);

    let filter = Filter::new_must(Condition::Field(FieldCondition::new_match(
        JsonPath::new("keyword"),
        "keyword_3".to_string().into(),
    )));
    let request = |name: &str, dim: usize| CoreSearchRequest {
        query: QueryEnum::Nearest(NamedQuery::new(
            VectorInternal::from(random_vector(&mut rand::rng(), dim)),
            name,
        )),
        with_payload: None,
        with_vector: None,
        filter: Some(filter.clone()),
        params: None,
        limit: 10,
        score_threshold: None,
        offset: 0,
    };
    let searches = vec![
        request(VECTOR1_NAME, 4),
        request(VECTOR2_NAME, 6),
        request(VECTOR1_NAME, 4),
    ];

    let handle = AdaptiveSearchHandle::current_for_tests();
    let search = |searches: Vec<CoreSearchRequest>| {
        SegmentsSearcher::search(
            holder.clone(),
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

    let batched = search(searches.clone()).await.unwrap();
    assert_eq!(batched.len(), searches.len());
    for (request, batched) in searches.into_iter().zip(batched) {
        let alone = search(vec![request]).await.unwrap().remove(0);
        assert!(!alone.is_empty());
        assert_eq!(batched, alone);
    }
}

/// Six searches on one named vector with the same filter, alone and next to a group on another
/// vector, return what each search returns on its own.
#[tokio::test]
async fn test_segments_search_same_vector_shared_filter() {
    use segment::data_types::vectors::{NamedQuery, VectorInternal};
    use segment::json_path::JsonPath;
    use segment::segment_constructor::simple_segment_constructor::{VECTOR1_NAME, VECTOR2_NAME};
    use segment::types::FieldCondition;
    use shard::query::query_enum::QueryEnum;

    use crate::collection_manager::fixtures::random_multi_vec_segment;

    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let mut holder = SegmentHolder::default();
    holder.add_new(random_multi_vec_segment(dir.path(), 10, 300, 4, 6));
    holder.add_new(random_multi_vec_segment(dir.path(), 11, 300, 4, 6));
    let holder = LockedSegmentHolder::new(holder);

    let filter = Filter::new_must(Condition::Field(FieldCondition::new_match(
        JsonPath::new("keyword"),
        "keyword_3".to_string().into(),
    )));
    let request = |name: &str, dim: usize| CoreSearchRequest {
        query: QueryEnum::Nearest(NamedQuery::new(
            VectorInternal::from(random_vector(&mut rand::rng(), dim)),
            name,
        )),
        with_payload: None,
        with_vector: None,
        filter: Some(filter.clone()),
        params: None,
        limit: 10,
        score_threshold: None,
        offset: 0,
    };
    let same_vector: Vec<_> = (0..6).map(|_| request(VECTOR1_NAME, 4)).collect();
    // One group of six vectors, then another vector with the same filter
    let mixed: Vec<_> = same_vector
        .iter()
        .cloned()
        .chain([request(VECTOR2_NAME, 6)])
        .collect();

    let handle = AdaptiveSearchHandle::current_for_tests();
    let search = |searches: Vec<CoreSearchRequest>| {
        SegmentsSearcher::search(
            holder.clone(),
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

    for searches in [same_vector, mixed] {
        let batched = search(searches.clone()).await.unwrap();
        assert_eq!(batched.len(), searches.len());
        for (request, batched) in searches.into_iter().zip(batched) {
            let alone = search(vec![request]).await.unwrap().remove(0);
            assert!(!alone.is_empty());
            assert_eq!(batched, alone);
        }
    }
}

#[tokio::test]
async fn test_segments_search_sampling() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();

    let segment1 = random_segment(dir.path(), 10, 2000, 4);
    let segment2 = random_segment(dir.path(), 10, 4000, 4);

    let mut holder = SegmentHolder::default();

    let _sid1 = holder.add_new(segment1);
    let _sid2 = holder.add_new(segment2);

    let segment_holder = LockedSegmentHolder::new(holder);

    let mut rnd = rand::rng();

    for _ in 0..100 {
        let req1 = SearchRequestInternal {
            vector: random_vector(&mut rnd, 4).into(),
            limit: 150, // more than LOWER_SEARCH_LIMIT_SAMPLING
            offset: None,
            with_payload: None,
            with_vector: None,
            filter: None,
            params: None,
            score_threshold: None,
        };
        let req2 = SearchRequestInternal {
            vector: random_vector(&mut rnd, 4).into(),
            limit: 50, // less than LOWER_SEARCH_LIMIT_SAMPLING
            offset: None,
            filter: None,
            params: None,
            with_payload: None,
            with_vector: None,
            score_threshold: None,
        };

        let batch_request = CoreSearchRequestBatch {
            searches: vec![req1.into(), req2.into()],
        };

        let batch_request = Arc::new(batch_request);

        let ctx = AmbientContext::new();
        let query_context = QueryContext::new(
            DEFAULT_INDEXING_THRESHOLD_KB,
            Handoff::Measured(AmbientContext::clone(&ctx)),
        );

        let result_no_sampling = SegmentsSearcher::search(
            segment_holder.clone(),
            batch_request.clone(),
            &AdaptiveSearchHandle::current_for_tests(),
            false,
            query_context,
            TEST_TIMEOUT,
        )
        .await
        .unwrap();

        assert_ne!(ctx.hw_data()[HwMetric::Cpu], 0);

        let ctx = AmbientContext::new();
        let query_context = QueryContext::new(
            DEFAULT_INDEXING_THRESHOLD_KB,
            Handoff::Measured(AmbientContext::clone(&ctx)),
        );

        assert!(!result_no_sampling.is_empty());

        let result_sampling = SegmentsSearcher::search(
            segment_holder.clone(),
            batch_request,
            &AdaptiveSearchHandle::current_for_tests(),
            true,
            query_context,
            TEST_TIMEOUT,
        )
        .await
        .unwrap();
        assert!(!result_sampling.is_empty());

        assert_ne!(ctx.hw_data()[HwMetric::Cpu], 0);

        // assert equivalence in depth
        assert_eq!(result_no_sampling[0].len(), result_sampling[0].len());
        assert_eq!(result_no_sampling[1].len(), result_sampling[1].len());

        for (no_sampling, sampling) in result_no_sampling[0].iter().zip(result_sampling[0].iter()) {
            assert_eq!(no_sampling.score, sampling.score); // different IDs may have same scores
        }
    }
}

#[test]
fn test_retrieve() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let segment_holder = build_test_holder(dir.path());
    let records = ambient::test(|| {
        retrieve_blocking(
            segment_holder,
            &[1.into(), 2.into(), 3.into()],
            &WithPayload::from(true),
            &true.into(),
            Duration::from_secs(1),
            &AtomicBool::new(false),
            DeferredBehavior::VisibleOnly,
        )
    })
    .unwrap();
    assert_eq!(records.len(), 3);
}

#[test]
fn test_retrieve_timeout() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let segment_holder = build_test_holder(dir.path());
    // keep write guard to prevent subsequent read
    let _segment_holder_write_guard = segment_holder.write();
    let records = retrieve_blocking(
        segment_holder.clone(),
        &[1.into(), 2.into(), 3.into()],
        &WithPayload::from(true),
        &true.into(),
        Duration::from_secs(1),
        &AtomicBool::new(false),
        DeferredBehavior::VisibleOnly,
    );
    assert_matches!(records, Err(OperationError::Timeout { .. }));
}

/// A text-indexed segment holding `docs`, each `(point id, op num, text)`.
fn text_segment(
    path: &std::path::Path,
    docs: impl IntoIterator<Item = (u64, u64, String)>,
) -> segment::segment::Segment {
    use segment::data_types::index::TextIndexParams;
    use segment::data_types::vectors::only_default_vector;
    use segment::entry::{NonAppendableSegmentEntry as _, SegmentEntry as _};
    use segment::payload_json;
    use segment::segment_constructor::simple_segment_constructor::build_simple_segment;
    use segment::types::{Distance, PayloadFieldSchema, PayloadSchemaParams};

    let _scope = ambient::test_guard();
    let mut segment = build_simple_segment(path, 4, Distance::Dot).unwrap();
    let params = TextIndexParams {
        phrase_matching: Some(true),
        ..TextIndexParams::default()
    };
    segment
        .create_field_index(
            0,
            &JsonPath::new("text"),
            Some(&PayloadFieldSchema::FieldParams(PayloadSchemaParams::Text(
                params,
            ))),
        )
        .unwrap();
    for (id, op_num, text) in docs {
        let vector = only_default_vector(&[1.0, 0.0, 0.0, 0.0]);
        segment.upsert_point(op_num, id.into(), vector).unwrap();
        segment
            .set_payload(op_num, id.into(), &payload_json! { "text": text }, &None)
            .unwrap();
    }
    segment
}

/// Distinct term counts for every `i` below 105, so no two documents tie.
/// `text_of(90)`, short and heavy in `gamma`, is among the best matches
/// for `alpha gamma`.
fn text_of(i: u64) -> String {
    ["alpha"; 5][..(i % 5 + 1) as usize].join(" ")
        + &" beta".repeat((i % 3) as usize)
        + &" gamma".repeat((i % 7) as usize)
}

/// The shard's BM25 ranking against each segment scored with statistics
/// over both, then merged: the statistics are shard-wide, every segment
/// contributes its best, and a point both segments return is ranked by
/// its newer copy only.
///
/// Both copies rank here on purpose. Like a vector search, the merge only
/// sees the versions the segments return, so a stale copy whose newer one
/// misses its segment's top `limit` would surface. A shard does not hold
/// such a pair: an upsert deletes the old copy, and during an optimization
/// the proxy hides it.
#[tokio::test]
async fn test_score_bm25_across_segments() {
    use segment::data_types::index::TextIndexParams;
    use segment::entry::ReadSegmentEntry as _;
    use shard::query::text::init_text_query_context;

    let _scoring = TextIndexParams::override_scoring(true);
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let moved = 5;
    let older = text_segment(
        &dir.path().join("a"),
        (0..40).map(|i| (i, i + 1, text_of(i))),
    );
    let newer = text_segment(
        &dir.path().join("b"),
        (40..80)
            .map(|i| (i, i + 1, text_of(i)))
            .chain([(moved, 100, text_of(90))]),
    );

    let field = JsonPath::new("text");
    let terms = vec!["alpha".to_owned(), "gamma".to_owned()];
    let params = Bm25Params::default();
    let limit = 15;

    // Reference: each segment ranked in full against both segments'
    // statistics, the older copy of the moved point dropped.
    let mut query_context =
        ambient::test(|| init_text_query_context(&field, None, &terms, Default::default()));
    older.fill_query_context(&mut query_context).unwrap();
    newer.fill_query_context(&mut query_context).unwrap();
    let segment_context = query_context.get_segment_query_context();
    let rank = |segment: &segment::segment::Segment| {
        segment
            .score_bm25(
                &field,
                &terms,
                params,
                &WithPayload::from(false),
                &WithVector::Bool(false),
                None,
                usize::MAX,
                &segment_context,
            )
            .unwrap()
    };
    let mut expected: Vec<ScoredPoint> = rank(&older)
        .into_iter()
        .filter(|point| point.id != moved.into())
        .chain(rank(&newer))
        .collect();
    expected.sort_by(|a, b| b.score.total_cmp(&a.score));
    expected.truncate(limit);
    assert!(
        rank(&older)
            .iter()
            .take(limit)
            .any(|point| point.id == moved.into()),
        "its older copy ranks in its segment too, so the merge has both to choose from",
    );

    let mut holder = SegmentHolder::default();
    holder.add_new(older);
    holder.add_new(newer);
    let actual = SegmentsSearcher::score_bm25(
        LockedSegmentHolder::new(holder),
        field.clone(),
        terms.clone(),
        params,
        None,
        None,
        limit,
        WithPayload::from(false),
        WithVector::Bool(false),
        &AdaptiveSearchHandle::current_for_tests(),
        &StoppingGuard::new(),
        TEST_TIMEOUT,
    )
    .measured(AmbientContext::new())
    .await
    .unwrap();

    let ranked = |points: &[ScoredPoint]| -> Vec<(PointIdType, u64, ScoreType)> {
        points
            .iter()
            .map(|point| (point.id, point.version, point.score))
            .collect()
    };
    assert_eq!(ranked(&actual), ranked(&expected));
    assert!(
        actual
            .iter()
            .any(|point| point.id == moved.into() && point.version == 100),
        "the moved point ranks, by its newer copy",
    );
}
