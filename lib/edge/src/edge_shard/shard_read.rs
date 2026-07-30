use std::path::Path;
use std::sync::Arc;

use segment::common::operation_error::OperationResult;
use segment::data_types::facets::FacetResponse;
use segment::types::{PointIdType, ScoredPoint};
use shard::locked_segment::LockedSegment;
use shard::retrieve::record_internal::RecordInternal;

use crate::read_view::{EdgeShardRead, ReadViewProvider};
use crate::requests::{
    CountRequest, FacetRequest, QueryRequest, RetrieveRequest, ScrollRequest, SearchRequest,
};
use crate::{EdgeConfig, EdgeShard, ShardInfo};

/// The read-write shard's segments are heterogeneous — `Segment` and `ProxySegment` coexist during
/// optimization — so the handle is the [`LockedSegment`] enum (static enum dispatch), and the read
/// view is built over that.
impl ReadViewProvider for EdgeShard {
    type Handle = LockedSegment;

    fn read_segments(&self) -> Vec<LockedSegment> {
        self.segments
            .read()
            .non_appendable_then_appendable_segments()
            .collect()
    }

    fn config_snapshot(&self) -> Arc<EdgeConfig> {
        Arc::new(self.config.read().clone())
    }

    fn search_pool(&self) -> Arc<rayon::ThreadPool> {
        self.search_pool.clone()
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

/// Inherent read API of [`EdgeShard`], kept for backwards compatibility (`edge` is a public crate):
/// callers can use these without importing [`EdgeShardRead`]. Each just forwards to the trait's
/// shared implementation.
impl EdgeShard {
    /// This method is DEPRECATED and should be replaced with query.
    pub fn search(&self, request: SearchRequest) -> OperationResult<Vec<ScoredPoint>> {
        EdgeShardRead::search(self, request)
    }

    pub fn query(&self, request: QueryRequest) -> OperationResult<Vec<ScoredPoint>> {
        EdgeShardRead::query(self, request)
    }

    pub fn scroll(
        &self,
        request: ScrollRequest,
    ) -> OperationResult<(Vec<RecordInternal>, Option<PointIdType>)> {
        EdgeShardRead::scroll(self, request)
    }

    pub fn retrieve(&self, request: RetrieveRequest) -> OperationResult<Vec<RecordInternal>> {
        EdgeShardRead::retrieve(self, request)
    }

    pub fn count(&self, request: CountRequest) -> OperationResult<usize> {
        EdgeShardRead::count(self, request)
    }

    pub fn facet(&self, request: FacetRequest) -> OperationResult<FacetResponse> {
        EdgeShardRead::facet(self, request)
    }

    pub fn info(&self) -> OperationResult<ShardInfo> {
        EdgeShardRead::info(self)
    }
}
