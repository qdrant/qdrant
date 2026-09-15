use std::sync::atomic::{AtomicUsize, Ordering};

use common::universal_io::MmapFile;
use parking_lot::{RwLock, RwLockReadGuard};
use segment::common::operation_error::OperationError;
use segment::data_types::vectors::{NamedQuery, VectorInternal};
use segment::segment::read_only::ReadOnlySegment;
use shard::query::ScoringQuery;
use shard::query::query_enum::QueryEnum;

use super::*;
use crate::read_only::tests::{VECTOR_NAME, open_follower, test_config, upsert};
use crate::read_view::{EdgeShardRead, ReadSegmentHandle, build_segment_pool};
use crate::{EdgeShard, QueryRequestBuilder};

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
    let api: &dyn EdgeShardReadWithCancellation = &shard;

    assert_cancelled(api.config_snapshot(stopped.clone()));
    assert_cancelled(api.path(stopped.clone()));
    assert_cancelled(api.search(SearchRequest::new(nearest(), 3), stopped.clone()));
    assert_cancelled(api.query(query(), stopped.clone()));
    assert_cancelled(api.query_batch(QueryBatchRequest::new(vec![]), stopped.clone()));
    assert_cancelled(api.scroll(ScrollRequest::new(), stopped.clone()));
    assert_cancelled(api.retrieve(RetrieveRequest::new(vec![]), stopped.clone()));
    assert_cancelled(api.count(CountRequest::new(), stopped.clone()));
    assert_cancelled(api.facet(FacetRequest::new("field".parse().unwrap()), stopped.clone()));
    assert_cancelled(api.search_matrix(
        SearchMatrixRequest::new(0, 0, VECTOR_NAME.to_string()),
        stopped.clone(),
    ));
    assert_cancelled(api.query_groups(
        GroupRequest::new(query(), "field".parse().unwrap(), 1, 1),
        stopped.clone(),
    ));
    assert_cancelled(api.info(stopped.clone()));
    assert!(stopped.load(Ordering::Relaxed));
}

#[test]
fn uncancelled_reads_match_normal_reads_and_leave_flag_unset() {
    let dir = tempfile::tempdir().unwrap();
    let leader = EdgeShard::new(dir.path(), test_config()).unwrap();
    upsert(&leader, 1..=5);
    leader.flush().unwrap();
    let shard = open_follower(dir.path());
    let api: &dyn EdgeShardReadWithCancellation = &shard;
    let stopped = Arc::new(AtomicBool::new(false));

    assert_eq!(
        api.query(query(), stopped.clone()).unwrap(),
        EdgeShardRead::query(&shard, query()).unwrap()
    );
    assert_eq!(
        api.query_batch(
            QueryBatchRequest::new(vec![query(), QueryRequest::new(2)]),
            stopped.clone(),
        )
        .unwrap(),
        EdgeShardRead::query_batch(
            &shard,
            QueryBatchRequest::new(vec![query(), QueryRequest::new(2)]),
        )
        .unwrap()
    );
    assert_eq!(api.count(CountRequest::new(), stopped.clone()).unwrap(), 5);
    assert_eq!(
        api.scroll(ScrollRequest::new(), stopped.clone())
            .unwrap()
            .0
            .len(),
        5
    );
    assert_eq!(
        api.retrieve(
            RetrieveRequest::new(vec![1.into(), 3.into()]),
            stopped.clone()
        )
        .unwrap()
        .len(),
        2
    );
    assert_eq!(api.info(stopped.clone()).unwrap().points_count, 5);
    assert_eq!(api.path(stopped.clone()).unwrap(), dir.path());
    api.config_snapshot(stopped.clone()).unwrap();
    assert!(
        api.query_batch(QueryBatchRequest::new(vec![]), stopped.clone())
            .unwrap()
            .is_empty()
    );
    assert!(!stopped.load(Ordering::Relaxed));

    // A cancellation is scoped to its flag, not stored on the shard.
    stopped.store(true, Ordering::Relaxed);
    assert_cancelled(api.query(query(), stopped));
    assert_eq!(
        EdgeShardRead::count(&shard, CountRequest::new()).unwrap(),
        5
    );
    assert_eq!(
        api.count(CountRequest::new(), Arc::new(AtomicBool::new(false)))
            .unwrap(),
        5
    );
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
            let mut view = EdgeReadView::new(
                segments,
                ReadViewProvider::config_snapshot(&shard),
                build_segment_pool("cancel-test", threads, None).unwrap(),
            );
            view.is_stopped = stopped.clone();
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
    assert_cancelled(run(&shard, stopped.clone(), |_| {
        stopped.store(true, Ordering::Relaxed);
        Ok(vec![1, 2])
    }));
}
