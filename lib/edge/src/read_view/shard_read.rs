use std::path::Path;
use std::sync::Arc;

use rayon::ThreadPool;
use segment::common::operation_error::OperationResult;
use segment::data_types::facets::FacetResponse;
use segment::types::{ExtendedPointId, PointIdType, ScoredPoint, WithPayloadInterface, WithVector};
use shard::count::CountRequestInternal;
use shard::facet::FacetRequestInternal;
use shard::query::ShardQueryRequest;
use shard::retrieve::record_internal::RecordInternal;
use shard::scroll::ScrollRequestInternal;
use shard::search::CoreSearchRequest;

use super::{
    EdgeReadView, Group, GroupRequest, ReadSegmentHandle, SearchMatrixRequest,
    SearchMatrixResponse, ShardInfo,
};
use crate::EdgeConfig;

mod sealed {
    /// Empty marker supertrait of [`EdgeShardRead`](super::EdgeShardRead). Unnameable outside the
    /// crate, so downstream crates cannot implement `EdgeShardRead`; it carries no methods, so
    /// nothing internal becomes callable through it.
    pub trait Sealed {}
}

impl<T: ReadViewProvider + ?Sized> sealed::Sealed for T {}

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

/// Read API shared by the read-write [`EdgeShard`](crate::EdgeShard) and the read-only follower
/// shard.
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
    fn search(&self, search: CoreSearchRequest) -> OperationResult<Vec<ScoredPoint>>;

    fn query(&self, request: ShardQueryRequest) -> OperationResult<Vec<ScoredPoint>>;

    fn scroll(
        &self,
        request: ScrollRequestInternal,
    ) -> OperationResult<(Vec<RecordInternal>, Option<PointIdType>)>;

    fn retrieve(
        &self,
        point_ids: &[ExtendedPointId],
        with_payload: Option<WithPayloadInterface>,
        with_vector: Option<WithVector>,
    ) -> OperationResult<Vec<RecordInternal>>;

    fn count(&self, request: CountRequestInternal) -> OperationResult<usize>;

    fn facet(&self, request: FacetRequestInternal) -> OperationResult<FacetResponse>;

    fn search_matrix(&self, request: SearchMatrixRequest) -> OperationResult<SearchMatrixResponse>;

    fn query_groups(&self, request: GroupRequest) -> OperationResult<Vec<Group>>;

    fn info(&self) -> OperationResult<ShardInfo>;
}

impl<T: ReadViewProvider + ?Sized> EdgeShardRead for T {
    fn config_snapshot(&self) -> Arc<EdgeConfig> {
        ReadViewProvider::config_snapshot(self)
    }

    fn path(&self) -> &Path {
        ReadViewProvider::path(self)
    }

    fn search(&self, search: CoreSearchRequest) -> OperationResult<Vec<ScoredPoint>> {
        view(self).search(search)
    }

    fn query(&self, request: ShardQueryRequest) -> OperationResult<Vec<ScoredPoint>> {
        view(self).query(request)
    }

    fn scroll(
        &self,
        request: ScrollRequestInternal,
    ) -> OperationResult<(Vec<RecordInternal>, Option<PointIdType>)> {
        view(self).scroll(request)
    }

    fn retrieve(
        &self,
        point_ids: &[ExtendedPointId],
        with_payload: Option<WithPayloadInterface>,
        with_vector: Option<WithVector>,
    ) -> OperationResult<Vec<RecordInternal>> {
        view(self).retrieve(point_ids, with_payload, with_vector)
    }

    fn count(&self, request: CountRequestInternal) -> OperationResult<usize> {
        view(self).count(request)
    }

    fn facet(&self, request: FacetRequestInternal) -> OperationResult<FacetResponse> {
        view(self).facet(request)
    }

    fn search_matrix(&self, request: SearchMatrixRequest) -> OperationResult<SearchMatrixResponse> {
        view(self).search_matrix(request)
    }

    fn query_groups(&self, request: GroupRequest) -> OperationResult<Vec<Group>> {
        view(self).query_groups(request)
    }

    fn info(&self) -> OperationResult<ShardInfo> {
        view(self).info()
    }
}

/// Build a one-shot read snapshot for a shard. Private so it is not part of the trait's surface —
/// the snapshot is an implementation detail of the blanket [`EdgeShardRead`] impl.
fn view<T: ReadViewProvider + ?Sized>(shard: &T) -> EdgeReadView<T::Handle> {
    EdgeReadView::new(
        shard.read_segments(),
        shard.config_snapshot(),
        shard.search_pool(),
    )
}
