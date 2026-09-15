use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use segment::common::operation_error::{OperationResult, check_process_stopped};
use segment::data_types::facets::FacetResponse;
use segment::index::UniversalReadExt;
use segment::types::{PointIdType, ScoredPoint};
use shard::retrieve::record_internal::RecordInternal;

use super::shard_read::{sealed, view};
use super::{EdgeReadView, Group, ReadViewProvider, SearchMatrixResponse, ShardInfo};
use crate::requests::{
    CountRequest, FacetRequest, GroupRequest, QueryBatchRequest, QueryRequest, RetrieveRequest,
    ScrollRequest, SearchMatrixRequest, SearchRequest,
};
use crate::{EdgeConfig, ReadOnlyEdgeShard};

/// Cancellation-aware counterpart to [`EdgeShardRead`](super::EdgeShardRead) for
/// [`ReadOnlyEdgeShard`]. Set the shared flag to `true` to cancel a synchronous read.
///
/// Cancellation is cooperative: engine loops and boundaries between stages check the flag.
/// Blocking I/O, lock acquisition, and indivisible computations can delay observation.
/// Once cancellation is observed, the call returns [`OperationError::Cancelled`](segment::common::operation_error::OperationError::Cancelled),
/// never a successful partial result. The read never sets or resets the caller's flag;
/// use a fresh flag for a new operation after cancellation.
///
/// All nested queries, prefetches, rescoring, and retrieval share the same flag.
/// Metadata accessors also return a result so they can report cancellation.
/// If both read traits are imported, use qualified calls such as
/// `EdgeShardReadWithCancellation::query(&shard, request, is_stopped)`.
pub trait EdgeShardReadWithCancellation: sealed::Sealed {
    /// Snapshot the current config.
    fn config_snapshot(&self, is_stopped: Arc<AtomicBool>) -> OperationResult<Arc<EdgeConfig>>;

    fn path(&self, is_stopped: Arc<AtomicBool>) -> OperationResult<&Path>;

    /// This method is DEPRECATED and should be replaced with query.
    fn search(
        &self,
        request: SearchRequest,
        is_stopped: Arc<AtomicBool>,
    ) -> OperationResult<Vec<ScoredPoint>>;

    fn query(
        &self,
        request: QueryRequest,
        is_stopped: Arc<AtomicBool>,
    ) -> OperationResult<Vec<ScoredPoint>>;

    /// Execute several [`QueryRequest`]s as one planned batch.
    ///
    /// Cheaper than running the same requests one by one: the batch is planned as a whole, so its
    /// leaf searches share a single pass over the segments, and leaves that differ only in their
    /// query vector are pushed down to each segment as one multi-vector search.
    ///
    /// Returns one result list per request, in request order.
    fn query_batch(
        &self,
        request: QueryBatchRequest,
        is_stopped: Arc<AtomicBool>,
    ) -> OperationResult<Vec<Vec<ScoredPoint>>>;

    fn scroll(
        &self,
        request: ScrollRequest,
        is_stopped: Arc<AtomicBool>,
    ) -> OperationResult<(Vec<RecordInternal>, Option<PointIdType>)>;

    fn retrieve(
        &self,
        request: RetrieveRequest,
        is_stopped: Arc<AtomicBool>,
    ) -> OperationResult<Vec<RecordInternal>>;

    fn count(&self, request: CountRequest, is_stopped: Arc<AtomicBool>) -> OperationResult<usize>;

    fn facet(
        &self,
        request: FacetRequest,
        is_stopped: Arc<AtomicBool>,
    ) -> OperationResult<FacetResponse>;

    fn search_matrix(
        &self,
        request: SearchMatrixRequest,
        is_stopped: Arc<AtomicBool>,
    ) -> OperationResult<SearchMatrixResponse>;

    fn query_groups(
        &self,
        request: GroupRequest,
        is_stopped: Arc<AtomicBool>,
    ) -> OperationResult<Vec<Group>>;

    fn info(&self, is_stopped: Arc<AtomicBool>) -> OperationResult<ShardInfo>;
}

impl<S: UniversalReadExt + 'static> EdgeShardReadWithCancellation for ReadOnlyEdgeShard<S>
where
    S::Fs: Send + Sync,
{
    fn config_snapshot(&self, is_stopped: Arc<AtomicBool>) -> OperationResult<Arc<EdgeConfig>> {
        check_process_stopped(&is_stopped)?;
        let config = ReadViewProvider::config_snapshot(self);
        check_process_stopped(&is_stopped)?;
        Ok(config)
    }

    fn path(&self, is_stopped: Arc<AtomicBool>) -> OperationResult<&Path> {
        check_process_stopped(&is_stopped)?;
        Ok(ReadViewProvider::path(self))
    }

    fn search(
        &self,
        request: SearchRequest,
        is_stopped: Arc<AtomicBool>,
    ) -> OperationResult<Vec<ScoredPoint>> {
        run(self, is_stopped, |view| view.search(request.into()))
    }

    fn query(
        &self,
        request: QueryRequest,
        is_stopped: Arc<AtomicBool>,
    ) -> OperationResult<Vec<ScoredPoint>> {
        run(self, is_stopped, |view| view.query(request.into()))
    }

    fn query_batch(
        &self,
        request: QueryBatchRequest,
        is_stopped: Arc<AtomicBool>,
    ) -> OperationResult<Vec<Vec<ScoredPoint>>> {
        run(self, is_stopped, |view| {
            let QueryBatchRequest { queries } = request;
            view.query_batch(queries.into_iter().map(Into::into).collect())
        })
    }

    fn scroll(
        &self,
        request: ScrollRequest,
        is_stopped: Arc<AtomicBool>,
    ) -> OperationResult<(Vec<RecordInternal>, Option<PointIdType>)> {
        run(self, is_stopped, |view| view.scroll(request.into()))
    }

    fn retrieve(
        &self,
        request: RetrieveRequest,
        is_stopped: Arc<AtomicBool>,
    ) -> OperationResult<Vec<RecordInternal>> {
        run(self, is_stopped, |view| {
            let RetrieveRequest {
                point_ids,
                with_payload,
                with_vector,
            } = request;
            view.retrieve(&point_ids, with_payload, with_vector)
        })
    }

    fn count(&self, request: CountRequest, is_stopped: Arc<AtomicBool>) -> OperationResult<usize> {
        run(self, is_stopped, |view| view.count(request.into()))
    }

    fn facet(
        &self,
        request: FacetRequest,
        is_stopped: Arc<AtomicBool>,
    ) -> OperationResult<FacetResponse> {
        run(self, is_stopped, |view| view.facet(request.into()))
    }

    fn search_matrix(
        &self,
        request: SearchMatrixRequest,
        is_stopped: Arc<AtomicBool>,
    ) -> OperationResult<SearchMatrixResponse> {
        run(self, is_stopped, |view| view.search_matrix(request))
    }

    fn query_groups(
        &self,
        request: GroupRequest,
        is_stopped: Arc<AtomicBool>,
    ) -> OperationResult<Vec<Group>> {
        run(self, is_stopped, |view| view.query_groups(request))
    }

    fn info(&self, is_stopped: Arc<AtomicBool>) -> OperationResult<ShardInfo> {
        run(self, is_stopped, |view| view.info())
    }
}

fn run<T: ReadViewProvider, R>(
    shard: &T,
    is_stopped: Arc<AtomicBool>,
    operation: impl FnOnce(&EdgeReadView<T::Handle>) -> OperationResult<R>,
) -> OperationResult<R> {
    check_process_stopped(&is_stopped)?;
    let mut view = view(shard);
    view.is_stopped = is_stopped;
    view.check_stopped()?;
    let result = operation(&view);
    view.check_stopped()?;
    result
}

#[cfg(test)]
mod tests;
