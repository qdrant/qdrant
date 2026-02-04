use std::sync::{LazyLock, Mutex};
use std::time::Duration;

use semver::Version;

use crate::cpu;

/// Current Qdrant version string
pub const QDRANT_VERSION_STRING: &str = "1.16.4-dev";

/// Current Qdrant semver version
pub static QDRANT_VERSION: LazyLock<Version> =
    LazyLock::new(|| Version::parse(QDRANT_VERSION_STRING).expect("valid version string"));

/// User-agent string to use in HTTP clients
pub static APP_USER_AGENT: LazyLock<String> =
    LazyLock::new(|| format!("Qdrant/{QDRANT_VERSION_STRING}"));

const DEFAULT_MAX_CONCURRENT_COLLECTION_LOADS: usize = 1;
const DEFAULT_MAX_CONCURRENT_SHARD_LOADS: usize = 1;
const DEFAULT_MAX_CONCURRENT_SEGMENT_LOADS: usize = 8;

/// Types of concurrent load operations
/// Used internally for type-safe configuration access and default value lookup
#[derive(Debug, Clone, Copy)]
enum ConcurrentLoadType {
    Collection,
    Shard,
    Segment,
}

impl ConcurrentLoadType {
    /// Returns the default value for this load type
    const fn default_value(self) -> usize {
        match self {
            Self::Collection => DEFAULT_MAX_CONCURRENT_COLLECTION_LOADS,
            Self::Shard => DEFAULT_MAX_CONCURRENT_SHARD_LOADS,
            Self::Segment => DEFAULT_MAX_CONCURRENT_SEGMENT_LOADS,
        }
    }
}

/// Configuration options for concurrent loads (external API)
#[derive(Debug, Default)]
pub struct ConcurrentLoadsOptions {
    /// Maximum number of collections to load concurrently
    pub max_concurrent_collection_loads: Option<usize>,

    /// Maximum number of shards to load concurrently when loading a collection
    pub max_concurrent_shard_loads: Option<usize>,

    /// Maximum number of segments to load concurrently when loading a shard
    pub max_concurrent_segment_loads: Option<usize>,
}

/// Internal configuration for concurrent loads
#[derive(Debug, Clone, Copy)]
struct ConcurrentLoadsConfig {
    collection_loads: usize,
    shard_loads: usize,
    segment_loads: usize,
}

/// Helper function to handle setting a single concurrent load value
/// - None: use default value
/// - Some(0): invalid, log warning and use default
/// - Some(val): use provided value
fn set_concurrent_load_value(value: Option<usize>, load_type: ConcurrentLoadType) -> usize {
    match value {
        None => load_type.default_value(),
        Some(0) => {
            let default_value = load_type.default_value();
            log::warn!(
                "Received 0 for max concurrent {load_type:?} config, using default {default_value}"
            );
            default_value
        }
        Some(val) => val,
    }
}

/// Convert external Options (with Option<usize>) to internal Config (with usize)
/// This conversion always succeeds, validating and applying defaults as needed
impl From<ConcurrentLoadsOptions> for ConcurrentLoadsConfig {
    fn from(options: ConcurrentLoadsOptions) -> Self {
        let collection_loads = set_concurrent_load_value(
            options.max_concurrent_collection_loads,
            ConcurrentLoadType::Collection,
        );

        let shard_loads = set_concurrent_load_value(
            options.max_concurrent_shard_loads,
            ConcurrentLoadType::Shard,
        );

        let segment_loads = set_concurrent_load_value(
            options.max_concurrent_segment_loads,
            ConcurrentLoadType::Segment,
        );

        Self {
            collection_loads,
            shard_loads,
            segment_loads,
        }
    }
}

/// Global configuration for concurrent loads
/// Initialized with defaults via ConcurrentLoadsOptions::default().into()
static CONCURRENT_LOADS_CONFIG: LazyLock<Mutex<ConcurrentLoadsConfig>> =
    LazyLock::new(|| Mutex::new(ConcurrentLoadsOptions::default().into()));

/// Internal helper to get a specific load configuration value
/// Returns the default value if the mutex is poisoned
fn get_load_config(load_type: ConcurrentLoadType) -> usize {
    CONCURRENT_LOADS_CONFIG
        .lock()
        .map(|config| match load_type {
            ConcurrentLoadType::Collection => config.collection_loads,
            ConcurrentLoadType::Shard => config.shard_loads,
            ConcurrentLoadType::Segment => config.segment_loads,
        })
        .unwrap_or_else(|_| load_type.default_value())
}

pub fn set_concurrent_loads_config(options: ConcurrentLoadsOptions) -> Result<(), &'static str> {
    let config = ConcurrentLoadsConfig::from(options);

    CONCURRENT_LOADS_CONFIG
        .lock()
        .map(|mut guard| *guard = config)
        .map_err(|_| "Failed to lock concurrent loads config")
}

pub fn max_concurrent_collection_loads() -> usize {
    get_load_config(ConcurrentLoadType::Collection)
}

pub fn max_concurrent_shard_loads() -> usize {
    get_load_config(ConcurrentLoadType::Shard)
}

pub fn max_concurrent_segment_loads() -> usize {
    get_load_config(ConcurrentLoadType::Segment)
}

/// Number of retries for confirming a consensus operation.
pub const CONSENSUS_CONFIRM_RETRIES: usize = 3;

/// Default timeout for consensus meta operations.
pub const CONSENSUS_META_OP_WAIT: Duration = Duration::from_secs(10);

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
