use std::num::NonZeroIsize;
use std::time::Duration;

/// Default timeout for consensus meta operations.
pub const CONSENSUS_META_OP_WAIT: Duration = Duration::from_secs(10);

/// Default value of CPU budget parameter.
///
/// Dynamic based on CPU size.
///
/// On low CPU systems, we want to reserve the minimal amount of CPUs for other tasks to allow
/// efficient optimization. On high CPU systems we want to reserve more CPUs.
#[inline(always)]
pub fn default_cpu_budget_param(num_cpu: usize) -> NonZeroIsize {
    let cpu_budget = if num_cpu <= 32 {
        -1
    } else if num_cpu <= 48 {
        -2
    } else if num_cpu <= 64 {
        -3
    } else if num_cpu <= 96 {
        -4
    } else if num_cpu <= 128 {
        -6
    } else {
        -(num_cpu as isize / 16)
    };
    NonZeroIsize::new(cpu_budget).unwrap()
}

/// Default number of CPUs for HNSW graph building and optimization tasks in general.
///
/// Dynamic based on CPU size.
///
/// Even on high-CPU systems, a value higher than 16 is discouraged. It will most likely not
/// improve performance and is more likely to cause disconnected HNSW graphs.
/// Will be less if currently available CPU budget is lower.
#[inline(always)]
pub const fn thread_count_for_hnsw(num_cpu: usize) -> usize {
    if num_cpu <= 48 {
        8
    } else if num_cpu <= 64 {
        12
    } else {
        16
    }
}
