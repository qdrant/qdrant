use crate::mmap::AdviceSetting;

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
