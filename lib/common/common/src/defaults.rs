use std::path::Path;
use std::sync::LazyLock;
use std::time::{Duration, Instant};

use semver::Version;

use crate::cpu;

/// Current Qdrant version string
pub const QDRANT_VERSION_STRING: &str = "1.17.1";

/// Current Qdrant semver version
pub static QDRANT_VERSION: LazyLock<Version> =
    LazyLock::new(|| Version::parse(QDRANT_VERSION_STRING).expect("valid version string"));

/// User-agent string to use in HTTP clients
pub static APP_USER_AGENT: LazyLock<String> =
    LazyLock::new(|| format!("Qdrant/{QDRANT_VERSION_STRING}"));

/// Number of retries for confirming a consensus operation.
pub const CONSENSUS_CONFIRM_RETRIES: usize = 3;

/// Default timeout for consensus meta operations.
pub const CONSENSUS_META_OP_WAIT: Duration = Duration::from_secs(10);

/// Log target for detailed storage-component load timing.
/// Enable via log config, e.g. `log_level: "INFO,qdrant::load_timing=debug"`.
pub const LOAD_TIMING_LOG_TARGET: &str = "qdrant::load_timing";

/// Minimum duration for a load-timing entry to be logged.
/// Sub-component loads faster than this are suppressed to reduce noise.
/// Matches the `{:.2}s` display format: anything below 5ms rounds to "0.00s".
pub const LOAD_TIMING_MIN_DURATION: Duration = Duration::from_millis(5);

/// Log a sub-component load time, suppressing entries faster than [`LOAD_TIMING_MIN_DURATION`].
pub fn log_load_timing(path: &Path, component: &str, started: Instant) {
    let elapsed = started.elapsed();
    if elapsed >= LOAD_TIMING_MIN_DURATION {
        log::debug!(
            target: LOAD_TIMING_LOG_TARGET,
            "{} - {component} loaded in {:.2}s",
            path.display(),
            elapsed.as_secs_f64(),
        );
    }
}

/// Max number of pooled elements to preserve in memory.
/// Scaled according to the number of logical CPU cores to account for concurrent operations.
pub static POOL_KEEP_LIMIT: LazyLock<usize> = LazyLock::new(|| cpu::get_num_cpus().clamp(16, 128));

/// Default value of CPU budget parameter.
///
/// Dynamic based on CPU size.
///
/// On low CPU systems, we want to reserve the minimal amount of CPUs for other tasks to allow
/// efficient optimization. On high CPU systems we want to reserve more CPUs.
#[inline(always)]
pub fn default_cpu_budget_unallocated(num_cpu: usize) -> isize {
    match num_cpu {
        0..=2 => 0,
        3..=32 => -1,
        33..=48 => -2,
        49..=64 => -3,
        65..=96 => -4,
        97..=128 => -6,
        num_cpu @ 129.. => -(num_cpu as isize / 16),
    }
}

/// Default number of CPUs for HNSW graph building and optimization tasks in general.
///
/// Dynamic based on CPU size.
///
/// Even on high-CPU systems, a value higher than 16 is discouraged. It will most likely not
/// improve performance and is more likely to cause disconnected HNSW graphs.
/// Will be less if currently available CPU budget is lower.
#[inline(always)]
pub fn thread_count_for_hnsw(num_cpu: usize) -> usize {
    match num_cpu {
        0..=48 => 8.min(num_cpu).max(1),
        49..=64 => 12,
        65.. => 16,
    }
}

/// Number of search threads to use in the search runtime.
///
/// Dynamic based on CPU size.
#[inline(always)]
pub fn search_thread_count(max_search_threads: usize) -> usize {
    if max_search_threads != 0 {
        return max_search_threads;
    }

    // At least one thread, but not more than number of CPUs - 1 if there are more than 2 CPU
    // Example:
    // Num CPU = 1 -> 1 thread
    // Num CPU = 2 -> 2 thread - if we use one thread with 2 cpus, its too much un-utilized resources
    // Num CPU = 3 -> 2 thread
    // Num CPU = 4 -> 3 thread
    // Num CPU = 5 -> 4 thread
    match cpu::get_num_cpus() {
        0..=1 => 1,
        2 => 2,
        num_cpu @ 3.. => num_cpu - 1,
    }
}
