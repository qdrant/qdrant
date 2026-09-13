//! A routed search must return exactly what the broadcast search returns: it
//! asks fewer segments, not different ones.

use std::sync::Arc;

use common::counter::hardware_accumulator::HwMeasurementAcc;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::DeferredBehavior;
use ordered_float::OrderedFloat;
use segment::data_types::query_context::{FormulaContext, QueryContext};
use segment::index::query_optimization::rescore_formula::parsed_formula::{
    ParsedExpression, ParsedFormula, VariableId,
};
use segment::types::{
    Condition, Filter, HasIdCondition, PointIdType, ScoredPoint, WithPayload, WithVector,
};
use shard::optimizers::config::DEFAULT_INDEXING_THRESHOLD_KB;
use shard::search::{CoreSearchRequest, CoreSearchRequestBatch};
use shard::segment_holder::locked::LockedSegmentHolder;
use shard::update::delete_points;
use tempfile::Builder;

use crate::collection_manager::fixtures::{TEST_TIMEOUT, build_test_holder};
use crate::collection_manager::holders::proxy_segment::ProxySegment;
use crate::collection_manager::provenance::{Provenance, Routes};
use crate::collection_manager::segments_searcher::SegmentsSearcher;
use crate::common::adaptive_handle::AdaptiveSearchHandle;

/// Points ordered by score, then by id.
///
/// Equally scored points come out in the iteration order of the `has_id` set,
/// which differs between two runs of the very same search, so the comparisons
/// below pin the score of every point rather than a tie order nothing
/// guarantees.
fn by_score(points: &[ScoredPoint]) -> Vec<(OrderedFloat<f32>, PointIdType, u64)> {
    let mut points: Vec<_> = points
        .iter()
        .map(|point| (OrderedFloat(point.score), point.id, point.version))
        .collect();
    points.sort_unstable();
    points
}

/// A search over the whole holder, or over `ids` only when given.
async fn search(
    segments: &LockedSegmentHolder,
    ids: Option<&[PointIdType]>,
    routes: Option<&Routes>,
) -> (Vec<ScoredPoint>, Provenance) {
    let filter = ids.map(|ids| {
        Filter::new_must(Condition::HasId(HasIdCondition::from(
            ids.iter().copied().collect::<ahash::AHashSet<_>>(),
        )))
    });
    let request = CoreSearchRequestBatch {
        searches: vec![CoreSearchRequest {
            query: vec![1.0, 1.0, 1.0, 1.0].into(),
            filter,
            params: None,
            limit: 10,
            offset: 0,
            with_payload: None,
            with_vector: None,
            score_threshold: None,
        }],
    };
    let (mut results, provenance) = SegmentsSearcher::search(
        segments.clone(),
        Arc::new(request),
        &AdaptiveSearchHandle::current_for_tests(),
        false,
        QueryContext::new(DEFAULT_INDEXING_THRESHOLD_KB, HwMeasurementAcc::new()),
        TEST_TIMEOUT,
        routes,
    )
    .await
    .unwrap();
    (results.pop().unwrap(), provenance)
}

/// Prefetch over the whole holder, then rescore its candidates both ways.
async fn prefetch_then_rescore(
    segments: &LockedSegmentHolder,
    routes_of: impl FnOnce(&Provenance, &[PointIdType]) -> Routes,
) -> (Vec<ScoredPoint>, Vec<ScoredPoint>) {
    let (candidates, provenance) = search(segments, None, None).await;
    let ids: Vec<_> = candidates.iter().map(|point| point.id).collect();
    assert!(!ids.is_empty());

    let routes = routes_of(&provenance, &ids);
    let (broadcast, _) = search(segments, Some(&ids), None).await;
    let (routed, _) = search(segments, Some(&ids), Some(&routes)).await;
    (broadcast, routed)
}

/// Routes for every candidate; the fixture's segments share points 4 and 5 at
/// different versions, so this also pins that the newer one is picked.
fn all_routes(provenance: &Provenance, ids: &[PointIdType]) -> Routes {
    let (routes, unrouted) = provenance.routes(ids.iter().copied());
    assert!(unrouted.is_empty(), "every candidate came from a segment");
    assert!(
        routes.len() > 1,
        "the fixture spreads candidates over segments"
    );
    routes
}

#[tokio::test]
async fn routed_rescore_matches_broadcast() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let segments = build_test_holder(dir.path());

    let (broadcast, routed) = prefetch_then_rescore(&segments, all_routes).await;
    assert_eq!(by_score(&broadcast), by_score(&routed));
}

#[tokio::test]
async fn routed_rescore_excludes_deleted() {
    for proxy_the_segment in [false, true] {
        let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
        let segments = build_test_holder(dir.path());
        let hw_counter = HardwareCounterCell::new();

        let (candidates, provenance) = search(&segments, None, None).await;
        let ids: Vec<_> = candidates.iter().map(|point| point.id).collect();
        let deleted = ids[0];

        if proxy_the_segment {
            // A proxy keeps the holder id of the segment it wraps, so the route
            // recorded before the optimization started still reaches it — and
            // the delete lands on the proxy, not on the frozen segment inside.
            let mut holder = segments.write();
            let ids_to_proxy: Vec<_> = holder.segment_ids();
            for segment_id in ids_to_proxy {
                let wrapped = holder.get(segment_id).unwrap().clone();
                holder
                    .replace(segment_id, ProxySegment::new(wrapped))
                    .unwrap();
            }
        }
        delete_points(&segments.read(), 100, &[deleted], &hw_counter).unwrap();

        let (routes, unrouted) = provenance.routes(ids.iter().copied());
        assert!(unrouted.is_empty());
        let (routed, _) = search(&segments, Some(&ids), Some(&routes)).await;
        assert!(
            !routed.iter().any(|point| point.id == deleted),
            "deleted point must not survive a routed rescore (proxied: {proxy_the_segment})",
        );

        let (broadcast, _) = search(&segments, Some(&ids), None).await;
        assert_eq!(by_score(&broadcast), by_score(&routed));
    }
}

#[tokio::test]
async fn stale_route_falls_back_to_broadcast() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let segments = build_test_holder(dir.path());

    // Pretend an optimization finished between the stages: the candidates of
    // one segment now point at an id the holder no longer has. The route count
    // still matches the segment count, so nothing but the missing id itself
    // can reveal the fallback.
    let (broadcast, routed) = prefetch_then_rescore(&segments, |provenance, ids| {
        let (mut routes, _) = provenance.routes(ids.iter().copied());
        let replaced = *routes.keys().next().unwrap();
        let stale = routes.keys().copied().max().unwrap() + 1;
        let moved = routes.remove(&replaced).unwrap();
        routes.insert(stale, moved);
        routes
    })
    .await;
    assert_eq!(by_score(&broadcast), by_score(&routed));
}

/// Retrieving each point from the segment it came from must return the same
/// records as asking every segment — including for points that live in two
/// segments at different versions, where the newest one wins either way.
#[tokio::test]
async fn routed_retrieve_matches_broadcast() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let segments = build_test_holder(dir.path());

    let (candidates, provenance) = search(&segments, None, None).await;
    let ids: Vec<_> = candidates.iter().map(|point| point.id).collect();

    let broadcast = SegmentsSearcher::retrieve(
        segments.clone(),
        &ids,
        &WithPayload::from(true),
        &WithVector::from(true),
        &AdaptiveSearchHandle::current_for_tests(),
        TEST_TIMEOUT,
        HwMeasurementAcc::new(),
        DeferredBehavior::VisibleOnly,
    )
    .await
    .unwrap();
    assert_eq!(broadcast.len(), ids.len());

    let (routes, unrouted) = provenance.routes(ids.iter().copied());
    assert!(unrouted.is_empty());
    let routed = SegmentsSearcher::retrieve_routed(
        segments.clone(),
        routes.clone(),
        Vec::new(),
        &WithPayload::from(true),
        &WithVector::from(true),
        &AdaptiveSearchHandle::current_for_tests(),
        TEST_TIMEOUT,
        HwMeasurementAcc::new(),
        DeferredBehavior::VisibleOnly,
    )
    .await
    .unwrap();
    assert_eq!(broadcast, routed);

    // A stale route and an id nothing recorded a segment for both fall back to
    // the broadcast retrieve, mixed into the same result.
    let mut stale_routes = routes;
    let stale = stale_routes.keys().copied().max().unwrap() + 1;
    let moved = stale_routes.values_mut().next().unwrap().split_off(0);
    stale_routes.insert(stale, moved);
    let unrouted = stale_routes.values_mut().last().unwrap().split_off(0);
    let mixed = SegmentsSearcher::retrieve_routed(
        segments,
        stale_routes,
        unrouted,
        &WithPayload::from(true),
        &WithVector::from(true),
        &AdaptiveSearchHandle::current_for_tests(),
        TEST_TIMEOUT,
        HwMeasurementAcc::new(),
        DeferredBehavior::VisibleOnly,
    )
    .await
    .unwrap();
    assert_eq!(broadcast, mixed);
}

/// A formula rescore sees only its own segment's share of the prefetch
/// results, and must produce what the whole list produces everywhere.
#[tokio::test]
async fn routed_formula_rescore_matches_broadcast() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let segments = build_test_holder(dir.path());

    let (candidates, provenance) = search(&segments, None, None).await;
    let ids: Vec<_> = candidates.iter().map(|point| point.id).collect();

    // Score by the prefetch score, so the rescore is exactly the prefetch.
    let formula = ParsedFormula {
        payload_vars: Default::default(),
        conditions: Vec::new(),
        defaults: Default::default(),
        formula: ParsedExpression::Variable(VariableId::Score(0)),
    };
    let rescore = |routes: Option<Routes>| {
        let ctx = Arc::new(FormulaContext {
            formula: formula.clone(),
            prefetches_results: vec![candidates.clone()],
            limit: ids.len(),
            score_threshold: None,
            is_stopped: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        });
        let segments = segments.clone();
        async move {
            SegmentsSearcher::rescore_with_formula(
                segments,
                ctx,
                &AdaptiveSearchHandle::current_for_tests(),
                HwMeasurementAcc::new(),
                TEST_TIMEOUT,
                routes.as_ref(),
            )
            .await
            .unwrap()
            .0
        }
    };

    let (routes, unrouted) = provenance.routes(ids.iter().copied());
    assert!(unrouted.is_empty());
    let broadcast = rescore(None).await;
    let routed = rescore(Some(routes)).await;
    assert_eq!(by_score(&broadcast), by_score(&routed));
    assert_eq!(broadcast.len(), ids.len());
}
