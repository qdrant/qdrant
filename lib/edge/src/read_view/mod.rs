mod count;
mod facet;
mod grouping;
mod info;
mod matrix;
mod query;
mod retrieve;
mod scroll;
mod search;

use std::path::Path;
use std::sync::Arc;

use common::counter::hardware_accumulator::HwMeasurementAcc;
use common::types::ScoreType;
use parking_lot::{RwLock, RwLockReadGuard};
use rayon::prelude::*;
use rayon::{ThreadPool, ThreadPoolBuilder};
use segment::common::operation_error::{OperationError, OperationResult};
use segment::data_types::facets::FacetResponse;
use segment::entry::ReadSegmentEntry;
use segment::index::UniversalReadExt;
use segment::index::query_optimization::rescore_formula::parsed_formula::ParsedFormula;
use segment::segment::read_only::ReadOnlySegment;
use segment::types::{ExtendedPointId, PointIdType, ScoredPoint, WithPayloadInterface, WithVector};
use shard::count::CountRequestInternal;
use shard::facet::FacetRequestInternal;
use shard::locked_segment::LockedSegment;
use shard::query::ShardQueryRequest;
use shard::query::scroll::QueryScrollRequestInternal;
use shard::retrieve::record_internal::RecordInternal;
use shard::scroll::ScrollRequestInternal;
use shard::search::CoreSearchRequest;

pub use self::grouping::{Group, GroupRequest};
pub use self::info::ShardInfo;
pub use self::matrix::{SearchMatrixRequest, SearchMatrixResponse};
use crate::EdgeConfig;

/// A handle to a single segment that can be read-locked to yield a [`ReadSegmentEntry`].
///
/// Abstracting over the handle (rather than the segment type) keeps the read path monomorphic for
/// homogeneous callers — a read-only follower's handle is the concrete
/// `Arc<RwLock<ReadOnlySegment<S>>>` — while the read-write shard, whose holder is intrinsically
/// heterogeneous (`Segment` and `ProxySegment` coexist during optimization), uses the
/// [`LockedSegment`] enum. Dynamic dispatch is confined to the `LockedSegment` impl, mirroring the
/// pre-existing [`LockedSegment::get_read`].
///
/// `Send + Sync` so a snapshot of handles can be shared across the shard's search thread pool and
/// read in parallel (see [`EdgeReadView::par_map_segments`]). Both concrete handles —
/// `Arc<RwLock<ReadOnlySegment<S>>>` and [`LockedSegment`] — already satisfy this.
pub trait ReadSegmentHandle: Send + Sync {
    type Segment: ReadSegmentEntry + ?Sized;

    /// Acquire a read guard. One guard is held for the whole per-segment operation, so reads that
    /// call several methods on the same segment observe a consistent state.
    fn read_segment(&self) -> RwLockReadGuard<'_, Self::Segment>;

    /// Owned handle for the retrieval / version-dedup path ([`retrieve_over`]).
    ///
    /// [`retrieve_over`]: shard::retrieve::retrieve_blocking::retrieve_over
    fn segment_arc(&self) -> Arc<RwLock<Self::Segment>>;
}

impl<S: UniversalReadExt + 'static> ReadSegmentHandle for Arc<RwLock<ReadOnlySegment<S>>>
where
    S::Fs: Send + Sync,
{
    type Segment = ReadOnlySegment<S>;

    fn read_segment(&self) -> RwLockReadGuard<'_, ReadOnlySegment<S>> {
        self.read()
    }

    fn segment_arc(&self) -> Arc<RwLock<ReadOnlySegment<S>>> {
        self.clone()
    }
}

impl ReadSegmentHandle for LockedSegment {
    type Segment = dyn ReadSegmentEntry;

    fn read_segment(&self) -> RwLockReadGuard<'_, dyn ReadSegmentEntry> {
        self.get_read().read()
    }

    fn segment_arc(&self) -> Arc<RwLock<dyn ReadSegmentEntry>> {
        self.get_read_arc()
    }
}

/// A consistent read snapshot of an edge shard: owned segment handles (collected in retrieval order,
/// non-appendable first then appendable) plus an immutable config snapshot.
///
/// All edge read logic is implemented exactly once, here, generic over the segment handle `H`.
/// Callers do not use this type directly — they go through [`EdgeShardRead`], which builds a snapshot
/// and delegates. Because the holder lock is released once the handles are collected, a single
/// top-level read runs over one immutable snapshot; sub-reads (e.g. the searches inside a `query`)
/// share that snapshot instead of re-locking the holder.
pub struct EdgeReadView<H: ReadSegmentHandle> {
    pub(crate) segments: Vec<H>,
    pub(crate) config: Arc<EdgeConfig>,
    /// Shard search thread pool, used to run per-segment reads in parallel.
    pub(crate) pool: Arc<ThreadPool>,
}

impl<H: ReadSegmentHandle> EdgeReadView<H> {
    pub(crate) fn new(segments: Vec<H>, config: Arc<EdgeConfig>, pool: Arc<ThreadPool>) -> Self {
        Self {
            segments,
            config,
            pool,
        }
    }

    /// Owned read handles for the retrieval / version-dedup path.
    pub(crate) fn segment_arcs(&self) -> Vec<Arc<RwLock<H::Segment>>> {
        self.segments
            .iter()
            .map(ReadSegmentHandle::segment_arc)
            .collect()
    }

    /// Run `f` over every segment in parallel on the shard's search thread pool, returning the
    /// per-segment results in segment order. This is the single seam through which all per-segment
    /// reads (search, scroll, count, facet, ...) are parallelized: the caller supplies the
    /// per-segment work and merges the ordered results itself.
    ///
    /// The whole map runs inside [`ThreadPool::install`], so the configured pool — not the global
    /// rayon pool — bounds the per-segment concurrency. With a single segment this is effectively a
    /// direct call.
    pub(crate) fn par_map_segments<R, F>(&self, f: F) -> OperationResult<Vec<R>>
    where
        F: Fn(&H) -> OperationResult<R> + Send + Sync,
        R: Send,
    {
        self.pool
            .install(|| self.segments.par_iter().map(f).collect())
    }
}

/// Read API shared by the read-write [`EdgeShard`](crate::EdgeShard) and the read-only follower
/// shard.
///
/// An implementer only provides how to snapshot its segments ([`read_segments`](Self::read_segments))
/// and config ([`config_snapshot`](Self::config_snapshot)); every read operation is a default method
/// that builds an [`EdgeReadView`] from that snapshot and runs the shared logic, so the read code is
/// never duplicated.
pub trait EdgeShardRead {
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

    /// This method is DEPRECATED and should be replaced with query.
    fn search(&self, search: CoreSearchRequest) -> OperationResult<Vec<ScoredPoint>> {
        view(self).search(search)
    }

    fn query(&self, request: ShardQueryRequest) -> OperationResult<Vec<ScoredPoint>> {
        view(self).query(request)
    }

    fn query_scroll(
        &self,
        request: &QueryScrollRequestInternal,
    ) -> OperationResult<Vec<ScoredPoint>> {
        view(self).query_scroll(request)
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

    fn rescore_with_formula(
        &self,
        formula: ParsedFormula,
        prefetches_results: Vec<Vec<ScoredPoint>>,
        limit: usize,
        score_threshold: Option<ScoreType>,
        hw_measurement_acc: HwMeasurementAcc,
    ) -> OperationResult<Vec<ScoredPoint>> {
        view(self).rescore_with_formula(
            formula,
            prefetches_results,
            limit,
            score_threshold,
            hw_measurement_acc,
        )
    }
}

/// Build a one-shot read snapshot for a shard. Private so it is not part of the trait's surface —
/// the snapshot is an implementation detail of the default read methods.
fn view<T: EdgeShardRead + ?Sized>(shard: &T) -> EdgeReadView<T::Handle> {
    EdgeReadView::new(
        shard.read_segments(),
        shard.config_snapshot(),
        shard.search_pool(),
    )
}

/// Build a shard's search thread pool with `num_threads` worker threads — the pool behind
/// [`EdgeReadView::par_map_segments`]. Both the read-write [`EdgeShard`](crate::EdgeShard) and the
/// read-only [`ReadOnlyEdgeShard`](crate::ReadOnlyEdgeShard) build one at open and keep it for
/// their lifetime, so per-segment reads and parallel segment opens don't spawn fresh threads per
/// operation.
///
/// `num_threads` is the already-resolved thread count (see [`EdgeConfig::search_thread_count`]);
/// callers pass `config.search_thread_count()` so a configured `0` is expanded to the CPU-derived
/// default that matches the core search runtime.
///
/// Returns an error (rather than panicking) when the underlying thread spawn fails — this runs
/// during shard open/load and follower open, so a transient resource failure must not abort the
/// process.
pub(crate) fn build_search_pool(num_threads: usize) -> OperationResult<Arc<ThreadPool>> {
    let pool = ThreadPoolBuilder::new()
        .num_threads(num_threads)
        .thread_name(|idx| format!("edge-search-{idx}"))
        .build()
        .map_err(|err| {
            OperationError::service_error(format!("failed to build edge search thread pool: {err}"))
        })?;
    Ok(Arc::new(pool))
}
