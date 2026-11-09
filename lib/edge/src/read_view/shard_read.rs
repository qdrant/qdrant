use std::path::Path;
use std::sync::Arc;

use rayon::ThreadPool;
use segment::types::PointIdType;
use shard::retrieve::record_internal::RecordInternal;

use super::EdgeReadView;
use crate::{
    CountRequest, EdgeConfig, EdgeScope, FacetRequest, FacetResponse, Group, GroupRequest,
    OperationResult, QueryBatchRequest, QueryRequest, ReadSegmentHandle, RetrieveRequest, Scoped,
    ScoredPoint, ScrollRequest, SearchMatrixRequest, SearchMatrixResponse, SearchRequest,
    ShardInfo,
};

pub(super) mod sealed {
    /// Empty marker supertrait of [`EdgeShardRead`](super::EdgeShardRead). Unnameable outside the
    /// crate, so downstream crates cannot implement `EdgeShardRead`; it carries no methods, so
    /// nothing internal becomes callable through it.
    pub trait Sealed {}
}

impl<T: ScopedReadViewProvider + ?Sized> sealed::Sealed for T {}

/// The snapshot half of the read path: how a shard exposes its segments, search pool, and config
/// for the shared read logic. Crate-private plumbing — [`EdgeShardRead`] is implemented for every
/// provider through a blanket impl, so these methods never appear on the public trait.
pub(crate) trait ReadViewProvider {
    /// Concrete segment handle backing this shard. A follower uses the monomorphic
    /// `Arc<RwLock<ReadOnlySegment<S>>>`; the read-write shard uses `LockedSegment`.
    type Handle: ReadSegmentHandle;

    /// Snapshot the current segments in retrieval order (non-appendable first, then appendable).
    fn read_segments(&self) -> Vec<Self::Handle>;

    /// Snapshot the current config.
    fn config_snapshot(&self) -> Arc<EdgeConfig>;

    /// The shard's search thread pool, used to run per-segment reads in parallel.
    fn search_pool(&self) -> Arc<ThreadPool>;

    fn path(&self) -> &Path;
}

/// What [`EdgeShardRead`] is implemented for: a shard and the scope its reads run in.
pub(crate) trait ScopedReadViewProvider {
    type Shard: ReadViewProvider + ?Sized;

    fn shard(&self) -> &Self::Shard;

    fn view(&self) -> EdgeReadView<<Self::Shard as ReadViewProvider>::Handle>;
}

impl<T: ReadViewProvider + ?Sized> ScopedReadViewProvider for Scoped<'_, T> {
    type Shard = T;

    fn shard(&self) -> &T {
        self.shard
    }

    fn view(&self) -> EdgeReadView<T::Handle> {
        view(self.shard, self.scope)
    }
}

impl<T: ReadViewProvider> ScopedReadViewProvider for T {
    type Shard = T;

    fn shard(&self) -> &T {
        self
    }

    fn view(&self) -> EdgeReadView<T::Handle> {
        view(self, &EdgeScope::new())
    }
}

/// Read API shared by the read-write [`EdgeShard`](crate::EdgeShard) and the read-only follower
/// shard.
///
/// Every method takes its edge request type from [`crate::requests`]; the blanket impl converts
/// it into the internal request the read logic executes.
///
/// A shard only implements the crate-private snapshot provider; this trait comes for free through
/// a blanket impl whose methods build an [`EdgeReadView`] from that snapshot and run the shared
/// logic, so the read code is never duplicated and the snapshot plumbing stays invisible to crate
/// users. Sealed: cannot be implemented outside the crate.
pub trait EdgeShardRead: sealed::Sealed {
    /// Snapshot the current config.
    fn config_snapshot(&self) -> Arc<EdgeConfig>;

    fn path(&self) -> &Path;

    /// This method is DEPRECATED and should be replaced with query.
    fn search(&self, request: SearchRequest) -> OperationResult<Vec<ScoredPoint>>;

    fn query(&self, request: QueryRequest) -> OperationResult<Vec<ScoredPoint>>;

    /// Execute several [`QueryRequest`]s as one planned batch.
    ///
    /// Cheaper than running the same requests one by one: the batch is planned as a whole, so its
    /// leaf searches share a single pass over the segments, and leaves that differ only in their
    /// query vector are pushed down to each segment as one multi-vector search.
    ///
    /// Returns one result list per request, in request order.
    fn query_batch(&self, request: QueryBatchRequest) -> OperationResult<Vec<Vec<ScoredPoint>>>;

    fn scroll(
        &self,
        request: ScrollRequest,
    ) -> OperationResult<(Vec<RecordInternal>, Option<PointIdType>)>;

    fn retrieve(&self, request: RetrieveRequest) -> OperationResult<Vec<RecordInternal>>;

    fn count(&self, request: CountRequest) -> OperationResult<usize>;

    fn facet(&self, request: FacetRequest) -> OperationResult<FacetResponse>;

    fn search_matrix(&self, request: SearchMatrixRequest) -> OperationResult<SearchMatrixResponse>;

    fn query_groups(&self, request: GroupRequest) -> OperationResult<Vec<Group>>;

    fn info(&self) -> OperationResult<ShardInfo>;
}

impl<T: ScopedReadViewProvider + ?Sized> EdgeShardRead for T {
    fn config_snapshot(&self) -> Arc<EdgeConfig> {
        self.shard().config_snapshot()
    }

    fn path(&self) -> &Path {
        self.shard().path()
    }

    fn search(&self, request: SearchRequest) -> OperationResult<Vec<ScoredPoint>> {
        run(self, |view| view.search(request.into()))
    }

    fn query(&self, request: QueryRequest) -> OperationResult<Vec<ScoredPoint>> {
        run(self, |view| view.query(request.into()))
    }

    fn query_batch(&self, request: QueryBatchRequest) -> OperationResult<Vec<Vec<ScoredPoint>>> {
        let QueryBatchRequest { queries } = request;
        run(self, |view| {
            view.query_batch(queries.into_iter().map(Into::into).collect())
        })
    }

    fn scroll(
        &self,
        request: ScrollRequest,
    ) -> OperationResult<(Vec<RecordInternal>, Option<PointIdType>)> {
        run(self, |view| view.scroll(request.into()))
    }

    fn retrieve(&self, request: RetrieveRequest) -> OperationResult<Vec<RecordInternal>> {
        let RetrieveRequest {
            point_ids,
            with_payload,
            with_vector,
        } = request;
        run(self, |view| {
            view.retrieve(&point_ids, with_payload, with_vector)
        })
    }

    fn count(&self, request: CountRequest) -> OperationResult<usize> {
        run(self, |view| view.count(request.into()))
    }

    fn facet(&self, request: FacetRequest) -> OperationResult<FacetResponse> {
        run(self, |view| view.facet(request.into()))
    }

    fn search_matrix(&self, request: SearchMatrixRequest) -> OperationResult<SearchMatrixResponse> {
        run(self, |view| view.search_matrix(request))
    }

    fn query_groups(&self, request: GroupRequest) -> OperationResult<Vec<Group>> {
        run(self, |view| view.query_groups(request))
    }

    fn info(&self) -> OperationResult<ShardInfo> {
        run(self, |view| view.info())
    }
}

pub(super) fn run<T: ScopedReadViewProvider + ?Sized, R, F>(
    shard: &T,
    operation: F,
) -> OperationResult<R>
where
    F: FnOnce(&EdgeReadView<<T::Shard as ReadViewProvider>::Handle>) -> OperationResult<R>,
{
    let view = shard.view();
    view.check_stopped()?;
    let result = operation(&view);
    view.check_stopped()?;
    result
}

/// Build a one-shot read snapshot for a shard. Private so it is not part of the trait's surface —
/// the snapshot is an implementation detail of the blanket [`EdgeShardRead`] impl.
pub(super) fn view<T: ReadViewProvider + ?Sized>(
    shard: &T,
    scope: &EdgeScope,
) -> EdgeReadView<T::Handle> {
    EdgeReadView::new(
        shard.read_segments(),
        shard.config_snapshot(),
        shard.search_pool(),
        scope,
    )
}
