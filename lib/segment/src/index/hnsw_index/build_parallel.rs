use std::cell::Cell;

#[cfg(target_os = "linux")]
use common::cpu::linux_low_thread_priority;
use orx_parallel::pools::BasicPool;
use orx_parallel::{IntoParIter, IterationOrder, Par, ParResult, Runner};

use crate::common::operation_error::OperationResult;
use super::hnsw::HNSW_BUILD_MAX_PAR_LEN;

/// Ensure HNSW build worker threads run at low priority (once per OS thread).
///
/// `BasicPool` does not expose a spawn hook, so we set priority lazily the first
/// time each worker executes HNSW build work.
fn ensure_hnsw_build_thread_priority() {
    thread_local! {
        static SET: Cell<bool> = const { Cell::new(false) };
    }
    SET.with(|set| {
        if set.get() {
            return;
        }
        #[cfg(target_os = "linux")]
        if let Err(err) = linux_low_thread_priority() {
            log::debug!(
                "Failed to set low thread priority for HNSW building, ignoring: {err}"
            );
        }
        set.set(true);
    });
}

/// Fallible parallel `for_each` for HNSW graph construction.
///
/// Uses orx-parallel adaptive chunking on the dedicated build pool. Chunk size is
/// capped at [`HNSW_BUILD_MAX_PAR_LEN`] so heterogeneous insert costs stay well
/// load-balanced.
pub(crate) fn par_try_for_each<T, I, F>(pool: &BasicPool, items: I, f: F) -> OperationResult<()>
where
    I: IntoParIter<Item = T>,
    T: Send,
    F: Fn(T) -> OperationResult<()> + Copy + Send + Sync,
{
    items
        .into_par()
        .runner(Runner::adaptive_with_pool(pool))
        .chunk_size(HNSW_BUILD_MAX_PAR_LEN)
        .iteration_order(IterationOrder::Arbitrary)
        .map(|item| {
            ensure_hnsw_build_thread_priority();
            f(item)
        })
        .into_fallible()
        .for_each(|_| {})
}
