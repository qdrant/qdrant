use std::sync::Arc;

use crate::mmap::AdviceSetting;
use crate::universal_io::disk_cache::CacheController;

/// Per-shard context bundling all backend-instance state needed to open files
/// through any [`UniversalRead`](super::UniversalRead) implementation.
///
/// Constructed once at shard load. Each backend implements
/// [`UniversalRead::extras_from_context`](super::UniversalRead::extras_from_context)
/// to pull out the slice it needs, so a single shard can mix backends across
/// its components.
#[derive(Debug, Default, Clone)]
#[non_exhaustive]
pub struct ShardStorageContext {
    pub mmap: MmapBackendConfig,
    /// Block-based disk cache controller. `None` when this shard does not use
    /// the block cache backend; opening a [`CachedSlice`](super::CachedSlice)
    /// in that case returns `Uninitialized`.
    pub block_cache: Option<BlockCacheBackendConfig>,
}

impl ShardStorageContext {
    pub fn new() -> Self {
        Self::default()
    }
}

/// Per-shard configuration for the [`MmapFile`](super::MmapFile) backend.
#[derive(Debug, Clone, Copy)]
pub struct MmapBackendConfig {
    /// Advice applied when the caller passes
    /// [`AccessHint::Default`](super::AccessHint::Default). When set to
    /// [`AdviceSetting::Global`], the process-wide global advice is used —
    /// this preserves the legacy behavior.
    pub default_advice: AdviceSetting,
}

impl Default for MmapBackendConfig {
    fn default() -> Self {
        Self {
            default_advice: AdviceSetting::Global,
        }
    }
}

/// Per-shard configuration for the block-based
/// [`CachedSlice`](super::CachedSlice) backend.
#[derive(Debug, Clone)]
pub struct BlockCacheBackendConfig {
    /// Controller owning the cache file and block lifecycle. Cloned `Arc` is
    /// cheap; multiple files on the same shard share one controller.
    pub controller: Arc<CacheController>,
}
