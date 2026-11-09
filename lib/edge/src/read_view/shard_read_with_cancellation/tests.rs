use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use common::ambient::hw::{HwMetric, HwSharedDrain};
use common::universal_io::MmapFile;
use parking_lot::{RwLock, RwLockReadGuard};
use segment::segment::read_only::ReadOnlySegment;

use super::*;
use crate::read_only::tests::{VECTOR_NAME, open_follower, test_config, upsert};
use crate::read_view::shard_read::run;
use crate::read_view::{EdgeReadView, build_segment_pool};
use crate::{
    EdgeShard, NamedQuery, OperationError, OperationResult, QueryEnum, QueryRequestBuilder,
    ReadSegmentHandle, ScoringQuery, VectorInternal,
};

fn assert_cancelled<T>(result: OperationResult<T>) {
    assert!(matches!(result, Err(OperationError::Cancelled { .. })));
}

fn nearest() -> QueryEnum {
    QueryEnum::Nearest(NamedQuery::new(
        VectorInternal::from(vec![1.0]),
        VECTOR_NAME.to_string(),
    ))
}

fn query() -> QueryRequest {
    QueryRequestBuilder::new(3)
        .query(ScoringQuery::Vector(nearest()))
        .build()
}

#[test]
fn every_method_rejects_a_cancelled_flag() {
    let dir = tempfile::tempdir().unwrap();
    let leader = EdgeShard::new(dir.path(), test_config()).unwrap();
    leader.flush().unwrap();
    let shard = open_follower(dir.path());
    let stopped = Arc::new(AtomicBool::new(true));
    let scope = EdgeScope::new().with_cancellation(stopped.clone());
    let api = shard.with(&scope);

    assert_cancelled(api.search(SearchRequest::new(nearest(), 3)));
    assert_cancelled(api.query(query()));
    assert_cancelled(api.query_batch(QueryBatchRequest::new(vec![])));
    assert_cancelled(api.scroll(ScrollRequest::new()));
    assert_cancelled(api.retrieve(RetrieveRequest::new(vec![])));
    assert_cancelled(api.count(CountRequest::new()));
    assert_cancelled(api.facet(FacetRequest::new("field".parse().unwrap())));
    assert_cancelled(api.search_matrix(SearchMatrixRequest::new(0, 0, VECTOR_NAME.to_string())));
    assert_cancelled(api.query_groups(GroupRequest::new(query(), "field".parse().unwrap(), 1, 1)));
    assert_cancelled(api.info());
    assert!(stopped.load(Ordering::Relaxed));
}

#[test]
fn uncancelled_reads_match_normal_reads_and_leave_flag_unset() {
    let dir = tempfile::tempdir().unwrap();
    let leader = EdgeShard::new(dir.path(), test_config()).unwrap();
    upsert(&leader, 1..=5);
    leader.flush().unwrap();
    let shard = open_follower(dir.path());
    let stopped = Arc::new(AtomicBool::new(false));
    let scope = EdgeScope::new().with_cancellation(stopped.clone());
    let api = shard.with(&scope);
    let plain_scope = EdgeScope::new();
    let plain = shard.with(&plain_scope);

    assert_eq!(api.query(query()).unwrap(), plain.query(query()).unwrap());
    assert_eq!(
        api.query_batch(QueryBatchRequest::new(vec![query(), QueryRequest::new(2)]))
            .unwrap(),
        plain
            .query_batch(QueryBatchRequest::new(vec![query(), QueryRequest::new(2)]))
            .unwrap()
    );
    assert_eq!(api.count(CountRequest::new()).unwrap(), 5);
    assert_eq!(api.scroll(ScrollRequest::new()).unwrap().0.len(), 5);
    assert_eq!(
        api.retrieve(RetrieveRequest::new(vec![1.into(), 3.into()]))
            .unwrap()
            .len(),
        2
    );
    assert_eq!(api.info().unwrap().points_count, 5);
    assert_eq!(api.path(), dir.path());
    assert!(
        api.query_batch(QueryBatchRequest::new(vec![]))
            .unwrap()
            .is_empty()
    );
    assert!(!stopped.load(Ordering::Relaxed));

    // A cancellation is scoped to its flag, not stored on the shard.
    stopped.store(true, Ordering::Relaxed);
    assert_cancelled(api.query(query()));
    assert_eq!(plain.count(CountRequest::new()).unwrap(), 5);
}

/// Inject cancellation at a deterministic point after an operation has entered its
/// segment work. This avoids timing-dependent sleeps or very large test datasets.
struct CancelOnRead {
    segment: Arc<RwLock<ReadOnlySegment<MmapFile>>>,
    stopped: Arc<AtomicBool>,
    reads: AtomicUsize,
    cancel_at: usize,
}

impl ReadSegmentHandle for CancelOnRead {
    type Segment = ReadOnlySegment<MmapFile>;

    fn read_segment(&self) -> RwLockReadGuard<'_, Self::Segment> {
        let guard = self.segment.read();
        if self.reads.fetch_add(1, Ordering::Relaxed) + 1 == self.cancel_at {
            self.stopped.store(true, Ordering::Relaxed);
        }
        guard
    }

    fn segment_arc(&self) -> Arc<RwLock<Self::Segment>> {
        self.segment.clone()
    }
}

#[test]
fn cancellation_during_segment_work_is_an_error_in_serial_and_parallel_reads() {
    let dir = tempfile::tempdir().unwrap();
    let leader = EdgeShard::new(dir.path(), test_config()).unwrap();
    upsert(&leader, 1..=5);
    leader.flush().unwrap();
    let shard = open_follower(dir.path());

    for threads in [1, 2] {
        for (cancel_at, search) in [(1, false), (2, true)] {
            let stopped = Arc::new(AtomicBool::new(false));
            let segments = ReadViewProvider::read_segments(&shard)
                .into_iter()
                .map(|segment| CancelOnRead {
                    segment,
                    stopped: stopped.clone(),
                    reads: AtomicUsize::new(0),
                    cancel_at,
                })
                .collect();
            let view = EdgeReadView::new(
                segments,
                ReadViewProvider::config_snapshot(&shard),
                build_segment_pool("cancel-test", threads, None).unwrap(),
                &EdgeScope::new().with_cancellation(stopped.clone()),
            );
            if search {
                // The first read fills the query context. The second cancels during
                // search, which must see the same flag as the surrounding query.
                assert_cancelled(view.query(query().into()));
            } else {
                // Filtered iteration can stop early with partial results. The
                // per-segment wrapper must turn that into an error.
                assert_cancelled(view.count(CountRequest::new().into()));
            }
            assert!(stopped.load(Ordering::Relaxed));
        }
    }
}

#[test]
fn cancellation_before_return_discards_successful_partial_results() {
    let dir = tempfile::tempdir().unwrap();
    let leader = EdgeShard::new(dir.path(), test_config()).unwrap();
    leader.flush().unwrap();
    let shard = open_follower(dir.path());
    let stopped = Arc::new(AtomicBool::new(false));
    let scope = EdgeScope::new().with_cancellation(stopped.clone());
    assert_cancelled(run(&shard.with(&scope), |_| {
        stopped.store(true, Ordering::Relaxed);
        Ok(vec![1, 2])
    }));
}

#[test]
fn measured_scope_reports_usage_of_its_reads() {
    let dir = tempfile::tempdir().unwrap();
    let leader = EdgeShard::new(dir.path(), test_config()).unwrap();
    upsert(&leader, 1..=5);
    leader.flush().unwrap();
    let shard = open_follower(dir.path());
    let drain = Arc::new(HwSharedDrain::default());

    let idle = EdgeScope::new().with_drain(drain.clone());
    assert_eq!(idle.hw_data()[HwMetric::Cpu], 0);

    let scope = EdgeScope::new().with_drain(drain.clone());
    let hits = shard.with(&scope).query(query()).unwrap();
    assert_eq!(hits.len(), 3);
    let one_query = scope.hw_data()[HwMetric::Cpu];
    assert!(one_query > 0);

    // One scope over several operations counts them together.
    let scope = EdgeScope::new().with_drain(drain.clone());
    shard.with(&scope).query(query()).unwrap();
    shard.with(&scope).query(query()).unwrap();
    assert_eq!(scope.hw_data()[HwMetric::Cpu], 2 * one_query);

    assert_eq!(drain.load()[HwMetric::Cpu], 3 * one_query);

    // A scope without a drain keeps its usage to itself.
    let scope = EdgeScope::new();
    shard.with(&scope).query(query()).unwrap();
    assert_eq!(scope.hw_data()[HwMetric::Cpu], one_query);
    assert_eq!(drain.load()[HwMetric::Cpu], 3 * one_query);
}
