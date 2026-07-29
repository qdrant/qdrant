mod handle;
mod ops;
mod shard_read;

use std::sync::Arc;

use rayon::prelude::*;
use rayon::{ThreadPool, ThreadPoolBuilder};
use segment::common::operation_error::{OperationError, OperationResult};

pub use self::handle::ReadSegmentHandle;
pub use self::ops::{Group, SearchMatrixResponse, ShardInfo};
pub use self::shard_read::EdgeShardRead;
pub(crate) use self::shard_read::ReadViewProvider;
use crate::EdgeConfig;

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
    pub(crate) fn segment_arcs(&self) -> Vec<Arc<parking_lot::RwLock<H::Segment>>> {
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
/// `pin_core` pins every pool thread to the given CPU core
/// ([`EdgeConfig::search_pool_core`]): the pool keeps its IO overlap but its compute is bounded
/// to one core. Pinning is best-effort — an unsupported platform or an out-of-range core id logs
/// a warning and the thread runs unpinned (macOS in particular only honours affinity as a hint).
///
/// Returns an error (rather than panicking) when the underlying thread spawn fails — this runs
/// during shard open/load and follower open, so a transient resource failure must not abort the
/// process.
pub(crate) fn build_search_pool(
    num_threads: usize,
    pin_core: Option<usize>,
) -> OperationResult<Arc<ThreadPool>> {
    let mut builder = ThreadPoolBuilder::new()
        .num_threads(num_threads)
        .thread_name(|idx| format!("edge-search-{idx}"));
    if let Some(core) = pin_core {
        builder = builder.start_handler(move |idx| {
            if !core_affinity::set_for_current(core_affinity::CoreId { id: core }) {
                log::warn!("failed to pin edge search thread {idx} to core {core}");
            }
        });
    }
    let pool = builder.build().map_err(|err| {
        OperationError::service_error(format!("failed to build edge search thread pool: {err}"))
    })?;
    Ok(Arc::new(pool))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Pinning is best-effort: the pool must build and run work both with a
    /// valid core id and with an out-of-range one (the start handler only
    /// warns on a failed pin — it must never break the pool).
    #[test]
    fn pinned_pool_builds_and_runs() {
        let pool = build_search_pool(2, Some(0)).unwrap();
        let sum: i32 = pool.install(|| (0..4).sum());
        assert_eq!(sum, 6);

        let pool = build_search_pool(1, Some(usize::MAX)).unwrap();
        assert_eq!(pool.install(|| 1 + 1), 2);
    }
}
