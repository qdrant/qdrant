//! Per-shard search thread pool.
//!
//! Both the read-write [`EdgeShard`](crate::EdgeShard) and the read-only
//! [`ReadOnlyEdgeShard`](crate::ReadOnlyEdgeShard) own a fixed-size [`rayon::ThreadPool`] used to
//! execute per-segment reads (search, scroll, count, facet, ...) in parallel and to open segments
//! in parallel on open/refresh. A long-lived pool avoids spawning a fresh thread per segment on
//! every query.

use std::sync::Arc;

use rayon::{ThreadPool, ThreadPoolBuilder};

/// Build a shard's search thread pool with `num_threads` worker threads.
///
/// `num_threads` is the already-resolved thread count (see [`EdgeConfig::search_thread_count`]);
/// callers pass `config.search_thread_count()` so a configured `0` is expanded to the CPU-derived
/// default that matches the core search runtime.
///
/// [`EdgeConfig::search_thread_count`]: crate::EdgeConfig::search_thread_count
pub(crate) fn build_search_pool(num_threads: usize) -> Arc<ThreadPool> {
    let pool = ThreadPoolBuilder::new()
        .num_threads(num_threads)
        .thread_name(|idx| format!("edge-search-{idx}"))
        .build()
        .expect("failed to build edge search thread pool");
    Arc::new(pool)
}
