use std::sync::atomic::AtomicBool;

use common::budget::ResourcePermit;
use once_cell::sync::OnceCell;
use parking_lot::Mutex;
use rayon::ThreadPool;

use super::build_thread_pool;
use crate::common::operation_error::{OperationError, OperationResult};

/// The CPUs of a graph build, and its thread pool, created when the first method asks for it.
///
/// A method that waits for the graph to be built elsewhere lends the CPUs meanwhile, see
/// [`BuildPool::lend_cpus`]. Nothing takes them back explicitly: whatever links on this
/// machine afterwards (the fallback of a failed remote build, the blocks the remote build
/// left) asks for the pool, and that takes them back. So every way a remote build can end
/// meets in one place.
pub(in crate::index::hnsw_index::hnsw) struct BuildPool<'a> {
    cpus: BuildCpus<'a>,
    pool: OnceCell<ThreadPool>,
}

enum BuildCpus<'a> {
    /// As many threads as given, with no budget behind them: the indexer's own.
    #[cfg(feature = "remote-indexing")]
    Fixed(usize),
    /// The permit of the optimization. The threads follow the CPUs it holds.
    Permit {
        permit: Mutex<&'a mut ResourcePermit>,
        stopped: &'a AtomicBool,
    },
}

impl<'a> BuildPool<'a> {
    #[cfg(feature = "remote-indexing")]
    pub fn fixed(num_threads: usize) -> Self {
        Self {
            cpus: BuildCpus::Fixed(num_threads),
            pool: OnceCell::new(),
        }
    }

    pub fn for_permit(permit: &'a mut ResourcePermit, stopped: &'a AtomicBool) -> Self {
        Self {
            cpus: BuildCpus::Permit {
                permit: Mutex::new(permit),
                stopped,
            },
            pool: OnceCell::new(),
        }
    }

    /// The thread pool. The first call takes back the CPUs that were lent, which may wait
    /// for other tasks to release theirs, and fails if the build is stopped meanwhile.
    pub fn get(&self) -> OperationResult<&ThreadPool> {
        self.pool.get_or_try_init(|| {
            let num_threads = match &self.cpus {
                #[cfg(feature = "remote-indexing")]
                BuildCpus::Fixed(num_threads) => *num_threads,
                BuildCpus::Permit { permit, stopped } => {
                    let mut permit = permit.lock();
                    if !permit.restore_cpu(stopped) {
                        return Err(OperationError::cancelled(
                            "index build cancelled while waiting for CPU budget",
                        ));
                    }
                    permit.num_cpus as usize
                }
            };
            build_thread_pool(num_threads.max(1))
        })
    }

    /// The build waits for work done elsewhere: keep one CPU, for what is still done here
    /// meanwhile, and give the others back to the budget.
    ///
    /// Does nothing once the pool exists: its threads are counted by the CPUs held.
    #[cfg(feature = "remote-indexing")]
    pub fn lend_cpus(&self) {
        if self.pool.get().is_some() {
            return;
        }
        match &self.cpus {
            #[cfg(feature = "remote-indexing")]
            BuildCpus::Fixed(_) => {}
            BuildCpus::Permit { permit, stopped: _ } => permit.lock().lend_cpu(1),
        }
    }
}
