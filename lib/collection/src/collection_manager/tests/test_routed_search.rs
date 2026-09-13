//! A routed `has_id` search must return exactly what asking every segment
//! returns: the routing cache decides which segments are asked, never what
//! they answer.

use std::sync::Arc;

use ahash::AHashSet;
use common::counter::hardware_accumulator::HwMeasurementAcc;
use common::counter::hardware_counter::HardwareCounterCell;
use ordered_float::OrderedFloat;
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use segment::data_types::query_context::QueryContext;
use segment::payload_json;
use segment::types::{Condition, Filter, HasIdCondition, PointIdType, ScoredPoint};
use shard::optimizers::config::DEFAULT_INDEXING_THRESHOLD_KB;
use shard::search::{CoreSearchRequest, CoreSearchRequestBatch};
use shard::segment_holder::locked::LockedSegmentHolder;
use shard::update::{delete_points, set_payload, upsert_points};
use tempfile::Builder;

use crate::collection_manager::fixtures::{TEST_TIMEOUT, build_test_holder};
use crate::collection_manager::holders::proxy_segment::ProxySegment;
use crate::collection_manager::segments_searcher::SegmentsSearcher;
use crate::common::adaptive_handle::AdaptiveSearchHandle;
use crate::operations::point_ops::{PointStructPersisted, VectorStructPersisted};

const QUERY: [f32; 4] = [1.0, 1.0, 1.0, 1.0];

/// Points ordered by score, then by id.
///
/// Equally scored points come out in the iteration order of the `has_id`
/// set, which differs between two runs of the very same search, so the
/// comparisons below pin the score of every point rather than a tie order
/// nothing guarantees.
fn by_score(points: &[ScoredPoint]) -> Vec<(OrderedFloat<f32>, PointIdType, u64)> {
    let mut points: Vec<_> = points
        .iter()
        .map(|point| (OrderedFloat(point.score), point.id, point.version))
        .collect();
    points.sort_unstable();
    points
}

fn has_id(ids: &[PointIdType]) -> Filter {
    Filter::new_must(Condition::HasId(HasIdCondition::from(
        ids.iter().copied().collect::<AHashSet<_>>(),
    )))
}

/// One search over the holder: every point when `ids` is `None`, else the
/// given ids, routed with whatever the holder's cache has recorded.
async fn search(
    segments: &LockedSegmentHolder,
    query: Vec<f32>,
    ids: Option<&[PointIdType]>,
) -> Vec<ScoredPoint> {
    let request = CoreSearchRequestBatch {
        searches: vec![CoreSearchRequest {
            query: query.into(),
            filter: ids.map(has_id),
            params: None,
            limit: 10,
            offset: 0,
            with_payload: None,
            with_vector: None,
            score_threshold: None,
        }],
    };
    let mut results = SegmentsSearcher::search(
        segments.clone(),
        Arc::new(request),
        &AdaptiveSearchHandle::current_for_tests(),
        false,
        QueryContext::new(DEFAULT_INDEXING_THRESHOLD_KB, HwMeasurementAcc::new()),
        TEST_TIMEOUT,
    )
    .await
    .unwrap();
    results.pop().unwrap()
}

/// The `ids` search as the cache routes it, then once more with the cache
/// cleared, which asks every segment.
async fn routed_and_broadcast(
    segments: &LockedSegmentHolder,
    query: Vec<f32>,
    ids: &[PointIdType],
) -> (Vec<ScoredPoint>, Vec<ScoredPoint>) {
    let routed = search(segments, query.clone(), Some(ids)).await;
    segments.read().routing_cache().clear();
    let broadcast = search(segments, query, Some(ids)).await;
    (routed, broadcast)
}

/// Every point of the holder, which fills the routing cache the way a
/// prefetch does. The fixture's segments share points 4 and 5 at different
/// versions, so this also pins that the newer one is recorded.
async fn prefetch(segments: &LockedSegmentHolder) -> Vec<PointIdType> {
    let candidates = search(segments, QUERY.to_vec(), None).await;
    let ids: Vec<_> = candidates.iter().map(|point| point.id).collect();

    let (routes, uncached) = segments.read().routing_cache().routes(ids.iter().copied());
    assert!(uncached.is_empty(), "every candidate was seen in a segment");
    assert!(
        routes.len() > 1,
        "the fixture spreads candidates over segments"
    );
    ids
}

fn proxy(segments: &LockedSegmentHolder, segment_id: usize) {
    let mut holder = segments.write();
    let wrapped = holder.get(segment_id).unwrap().clone();
    holder
        .replace(segment_id, ProxySegment::new(wrapped))
        .unwrap();
}

#[tokio::test]
async fn routed_rescore_matches_broadcast() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let segments = build_test_holder(dir.path());

    let ids = prefetch(&segments).await;
    let (routed, broadcast) = routed_and_broadcast(&segments, QUERY.to_vec(), &ids).await;
    assert_eq!(by_score(&routed), by_score(&broadcast));
}

#[tokio::test]
async fn routed_rescore_excludes_point_deleted_between_stages() {
    for proxy_the_segments in [false, true] {
        let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
        let segments = build_test_holder(dir.path());
        let hw_counter = HardwareCounterCell::new();

        let ids = prefetch(&segments).await;
        if proxy_the_segments {
            // A proxy keeps the holder id of the segment it wraps, so the routes
            // recorded before the optimization started still reach it — and the
            // delete lands on the proxy, not on the frozen segment inside.
            for segment_id in segments.read().segment_ids() {
                proxy(&segments, segment_id);
            }
        }
        let deleted = ids[0];
        delete_points(&segments.read(), 100, &[deleted], &hw_counter).unwrap();

        let (routed, broadcast) = routed_and_broadcast(&segments, QUERY.to_vec(), &ids).await;
        assert!(
            !routed.iter().any(|point| point.id == deleted),
            "deleted point must not survive a routed rescore (proxied: {proxy_the_segments})",
        );
        assert_eq!(by_score(&routed), by_score(&broadcast));
    }
}

#[tokio::test]
async fn moved_point_is_found_where_the_update_put_it() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let segments = build_test_holder(dir.path());
    let hw_counter = HardwareCounterCell::new();

    let ids = prefetch(&segments).await;

    // Point 1 lives only in the first segment. Once that is proxied, an update
    // cannot land in place and copies the point into the other, appendable
    // segment. The update drops the route, so the next read asks every segment
    // for the point and finds the copy the update produced.
    let moved = PointIdType::NumId(1);
    let first = segments.read().segment_ids()[0];
    proxy(&segments, first);
    set_payload(
        &segments.read(),
        1001,
        &payload_json! {"color": "green"},
        &[moved],
        &None,
        None,
        &hw_counter,
    )
    .unwrap();
    let (_, uncached) = segments.read().routing_cache().routes([moved]);
    assert_eq!(uncached, vec![moved], "the update invalidated the route");

    let (routed, broadcast) = routed_and_broadcast(&segments, QUERY.to_vec(), &ids).await;
    assert_eq!(by_score(&routed), by_score(&broadcast));
    let version = routed
        .iter()
        .find(|point| point.id == moved)
        .map(|point| point.version);
    assert_eq!(version, Some(1001), "the routed read sees the moved copy");
}

#[tokio::test]
async fn swapped_segment_falls_back_to_broadcast() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let segments = build_test_holder(dir.path());

    let ids = prefetch(&segments).await;

    // An optimization finishes: the segment comes back under a new id. The
    // routes into the old id still exist but count as unknown, so the next
    // read asks every segment for those ids and re-records them under the new
    // id, at the very same versions.
    {
        let mut holder = segments.write();
        let old = holder.segment_ids()[0];
        let segment = holder.get(old).unwrap().clone();
        holder.swap_new(segment, &[old]);
    }
    let routed = search(&segments, QUERY.to_vec(), Some(&ids)).await;
    {
        let holder = segments.read();
        let (routes, uncached) = holder.routing_cache().routes(ids.iter().copied());
        assert!(uncached.is_empty());
        assert!(
            routes
                .keys()
                .all(|segment_id| holder.get(*segment_id).is_some()),
            "every route points at a live segment again"
        );
    }

    segments.read().routing_cache().clear();
    let broadcast = search(&segments, QUERY.to_vec(), Some(&ids)).await;
    assert_eq!(by_score(&routed), by_score(&broadcast));
}

fn random_vector(rng: &mut StdRng) -> Vec<f32> {
    (0..QUERY.len())
        .map(|_| rng.random_range(0.0..1.0))
        .collect()
}

/// Random updates interleaved with `has_id` searches: whatever the cache has
/// recorded, a routed read must agree with asking every segment.
#[tokio::test]
async fn routed_reads_agree_with_broadcast_under_random_updates() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let segments = build_test_holder(dir.path());
    let hw_counter = HardwareCounterCell::new();
    let mut rng = StdRng::seed_from_u64(42);
    let id = |n: u64| PointIdType::NumId(n);
    let mut proxied = false;

    for op_num in 100..400u64 {
        match rng.random_range(0..10) {
            0..=3 => {
                let points: Vec<_> = (0..rng.random_range(1..=3))
                    .map(|_| PointStructPersisted {
                        id: id(rng.random_range(1..=20)),
                        vector: VectorStructPersisted::from(random_vector(&mut rng)),
                        payload: None,
                    })
                    .collect();
                upsert_points(&segments.read(), op_num, &points, None, &hw_counter).unwrap();
            }
            4..=5 => {
                set_payload(
                    &segments.read(),
                    op_num,
                    &payload_json! {"n": op_num},
                    &[id(rng.random_range(1..=20))],
                    &None,
                    None,
                    &hw_counter,
                )
                .unwrap();
            }
            6..=7 => {
                delete_points(
                    &segments.read(),
                    op_num,
                    &[id(rng.random_range(1..=20))],
                    &hw_counter,
                )
                .unwrap();
            }
            8 if !proxied => {
                // An optimization starts: the first segment is proxied under
                // its own id, so updates to its points copy them elsewhere.
                proxied = true;
                let first = segments.read().segment_ids()[0];
                proxy(&segments, first);
            }
            _ => {}
        }

        let wanted: Vec<_> = (0..rng.random_range(1..=8))
            .map(|_| id(rng.random_range(1..=25)))
            .collect();
        let (routed, broadcast) =
            routed_and_broadcast(&segments, random_vector(&mut rng), &wanted).await;
        assert_eq!(
            by_score(&routed),
            by_score(&broadcast),
            "op {op_num}: routed read diverged from broadcast for {wanted:?}",
        );
    }
}
