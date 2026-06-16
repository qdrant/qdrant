//! Tokio runtimes owned by [`TableOfContent`].
//!
//! Two search runtimes are built side-by-side: `high_cpu` matches the
//! traditional sizing (`high_cpu_blocking_threads`) and is used when the
//! process is CPU-saturated, while `high_io` uses
//! [`common::defaults::search_thread_count`] via `high_io_blocking_threads` so
//! IO-bound search can hide latency with more parallel segment scans.
//! [`AdaptiveSearchHandle`](
//! collection::common::adaptive_handle::AdaptiveSearchHandle) routes
//! `spawn_blocking` calls between them based on observed CPU usage.

use std::cmp::max;
use std::io;
use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::runtime::{self, Runtime};

/// Async-worker thread count for the search runtimes. Kept small because the
/// heavy work runs on the blocking pool, not the async workers.
const SEARCH_ASYNC_WORKERS: usize = 1;

/// Build the CPU-friendly search runtime. Sized for steady-state CPU
/// saturation: one blocking thread per CPU, so concurrent searches don't
/// thrash.
pub(super) fn create_high_cpu_search_runtime(max_search_threads: usize) -> io::Result<Runtime> {
    let blocking_threads = high_cpu_blocking_threads(max_search_threads);
    let async_workers = SEARCH_ASYNC_WORKERS.min(blocking_threads.max(1));
    runtime::Builder::new_multi_thread()
        .worker_threads(async_workers)
        .max_blocking_threads(blocking_threads)
        .enable_all()
        .thread_name_fn(|| {
            static ATOMIC_ID: AtomicUsize = AtomicUsize::new(0);
            let id = ATOMIC_ID.fetch_add(1, Ordering::SeqCst);
            format!("search-cpu-{id}")
        })
        .build()
}

/// Build the IO-friendly search runtime. Blocking pool size comes from
/// [`common::defaults::search_thread_count`] so search can hide IO latency with
/// many parallel segment scans.
pub(super) fn create_high_io_search_runtime(max_search_threads: usize) -> io::Result<Runtime> {
    let blocking_threads = high_io_blocking_threads(max_search_threads);
    let async_workers = SEARCH_ASYNC_WORKERS.min(blocking_threads.max(1));
    runtime::Builder::new_multi_thread()
        .worker_threads(async_workers)
        .max_blocking_threads(blocking_threads)
        .enable_all()
        .thread_name_fn(|| {
            static ATOMIC_ID: AtomicUsize = AtomicUsize::new(0);
            let id = ATOMIC_ID.fetch_add(1, Ordering::SeqCst);
            format!("search-io-{id}")
        })
        .build()
}

pub(super) fn create_update_runtime(max_optimization_threads: usize) -> io::Result<Runtime> {
    let mut builder = runtime::Builder::new_multi_thread();
    let num_cpus = common::cpu::get_num_cpus();
    builder
        .enable_time()
        .enable_io()
        .worker_threads(num_cpus)
        .thread_name_fn(|| {
            static ATOMIC_ID: AtomicUsize = AtomicUsize::new(0);
            let id = ATOMIC_ID.fetch_add(1, Ordering::SeqCst);
            format!("update-{id}")
        });
    if max_optimization_threads > 0 {
        builder.max_blocking_threads(max_optimization_threads);
    }
    builder.build()
}

pub(super) fn create_general_purpose_runtime() -> io::Result<Runtime> {
    runtime::Builder::new_multi_thread()
        .enable_time()
        .enable_io()
        .worker_threads(max(common::cpu::get_num_cpus(), 2))
        .thread_name_fn(|| {
            static ATOMIC_ID: AtomicUsize = AtomicUsize::new(0);
            let id = ATOMIC_ID.fetch_add(1, Ordering::SeqCst);
            format!("general-{id}")
        })
        .build()
}

/// Blocking-thread count for the high-CPU search pool.
pub(super) fn high_cpu_blocking_threads(max_search_threads: usize) -> usize {
    if max_search_threads != 0 {
        return max_search_threads;
    }
    common::cpu::get_num_cpus().max(1)
}

/// Blocking-thread count for the high-IO search pool.
pub(super) fn high_io_blocking_threads(max_search_threads: usize) -> usize {
    common::defaults::search_thread_count(max_search_threads)
}
