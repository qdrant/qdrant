#![expect(deprecated)]

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use segment::common::operation_error::check_process_stopped;
use segment::index::UniversalReadExt;
use segment::types::PointIdType;
use shard::retrieve::record_internal::RecordInternal;

use super::ReadViewProvider;
use crate::{
    CountRequest, EdgeConfig, EdgeScope, EdgeShardRead, FacetRequest, FacetResponse, Group,
    GroupRequest, OperationResult, QueryBatchRequest, QueryRequest, ReadOnlyEdgeShard,
    RetrieveRequest, ScoredPoint, ScrollRequest, SearchMatrixRequest, SearchMatrixResponse,
    SearchRequest, ShardInfo,
};

#[deprecated(note = "use `EdgeShardRead` on `shard.with(&scope)` with a cancellable `EdgeScope`")]
pub trait EdgeShardReadWithCancellation: sealed::Sealed {
    fn config_snapshot(&self, is_stopped: Arc<AtomicBool>) -> OperationResult<Arc<EdgeConfig>>;

    fn path(&self, is_stopped: Arc<AtomicBool>) -> OperationResult<&Path>;

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

mod sealed {
    pub trait Sealed {}
}

impl<S: UniversalReadExt + 'static> sealed::Sealed for ReadOnlyEdgeShard<S> {}

impl<S: UniversalReadExt + 'static> EdgeShardReadWithCancellation for ReadOnlyEdgeShard<S>
where
    S::Fs: Send + Sync,
{
    fn config_snapshot(&self, is_stopped: Arc<AtomicBool>) -> OperationResult<Arc<EdgeConfig>> {
        check_process_stopped(&is_stopped)?;
        Ok(ReadViewProvider::config_snapshot(self))
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
        self.with(&scope(is_stopped)).search(request)
    }

    fn query(
        &self,
        request: QueryRequest,
        is_stopped: Arc<AtomicBool>,
    ) -> OperationResult<Vec<ScoredPoint>> {
        self.with(&scope(is_stopped)).query(request)
    }

    fn query_batch(
        &self,
        request: QueryBatchRequest,
        is_stopped: Arc<AtomicBool>,
    ) -> OperationResult<Vec<Vec<ScoredPoint>>> {
        self.with(&scope(is_stopped)).query_batch(request)
    }

    fn scroll(
        &self,
        request: ScrollRequest,
        is_stopped: Arc<AtomicBool>,
    ) -> OperationResult<(Vec<RecordInternal>, Option<PointIdType>)> {
        self.with(&scope(is_stopped)).scroll(request)
    }

    fn retrieve(
        &self,
        request: RetrieveRequest,
        is_stopped: Arc<AtomicBool>,
    ) -> OperationResult<Vec<RecordInternal>> {
        self.with(&scope(is_stopped)).retrieve(request)
    }

    fn count(&self, request: CountRequest, is_stopped: Arc<AtomicBool>) -> OperationResult<usize> {
        self.with(&scope(is_stopped)).count(request)
    }

    fn facet(
        &self,
        request: FacetRequest,
        is_stopped: Arc<AtomicBool>,
    ) -> OperationResult<FacetResponse> {
        self.with(&scope(is_stopped)).facet(request)
    }

    fn search_matrix(
        &self,
        request: SearchMatrixRequest,
        is_stopped: Arc<AtomicBool>,
    ) -> OperationResult<SearchMatrixResponse> {
        self.with(&scope(is_stopped)).search_matrix(request)
    }

    fn query_groups(
        &self,
        request: GroupRequest,
        is_stopped: Arc<AtomicBool>,
    ) -> OperationResult<Vec<Group>> {
        self.with(&scope(is_stopped)).query_groups(request)
    }

    fn info(&self, is_stopped: Arc<AtomicBool>) -> OperationResult<ShardInfo> {
        self.with(&scope(is_stopped)).info()
    }
}

fn scope(is_stopped: Arc<AtomicBool>) -> EdgeScope {
    EdgeScope::new().with_cancellation(is_stopped)
}

#[cfg(test)]
mod tests;
