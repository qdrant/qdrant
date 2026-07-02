use std::sync::OnceLock;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Global feature flags, normally initialized when starting Qdrant.
static FEATURE_FLAGS: OnceLock<FeatureFlags> = OnceLock::new();

#[derive(Debug, Serialize, Deserialize, Clone, Copy, Eq, PartialEq, JsonSchema)]
#[serde(default)]
pub struct FeatureFlags {
    /// Magic feature flag that enables all features.
    ///
    /// Note that this will only be applied to all flags when passed into [`init_feature_flags`].
    all: bool,

    /// Use incremental HNSW building.
    ///
    /// Enabled by default in Qdrant 1.14.1.
    pub incremental_hnsw_building: bool,

    /// Use appendable quantization in appendable plain segments.
    ///
    /// Enabled by default in Qdrant 1.16.0.
    pub appendable_quantization: bool,

    /// Use single-file mmap in-ram vector storage (InRamMmap)
    ///
    /// Enabled by default in Qdrant 1.18.3+
    pub single_file_mmap_vector_storage: bool,

    /// Use io_uring-based payload storage implementation.
    pub async_payload_storage: bool,

    /// Write a segment manifest (`segments_manifest.json`, next to the `segments/` directory)
    /// listing the shard's segments and their
    /// state, so out-of-process readers can discover segments without scanning the filesystem.
    pub write_segment_manifest: bool,

    /// Build new segments in append-only mode: in-place point mutations become clone-and-tombstone
    /// appends instead. Intended for testing the append-only storage path.
    pub append_only_mutations: bool,

    /// Serverless-compatible deployment mode. Automatically enables [`Self::write_segment_manifest`] and
    /// [`Self::append_only_mutations`].
    ///
    /// Note that this will only be applied when passed into [`init_feature_flags`].
    serverless_compatible: bool,
}

impl Default for FeatureFlags {
    fn default() -> FeatureFlags {
        FeatureFlags {
            all: false,
            incremental_hnsw_building: true,
            appendable_quantization: true,
            single_file_mmap_vector_storage: true,
            async_payload_storage: false,
            write_segment_manifest: false,
            append_only_mutations: false,
            serverless_compatible: false,
        }
    }
}

impl FeatureFlags {
    /// Check if the feature flags are set to default values.
    pub fn is_default(self) -> bool {
        self == FeatureFlags::default()
    }

    fn all() -> Self {
        Self {
            all: true,
            incremental_hnsw_building: true,
            appendable_quantization: true,
            single_file_mmap_vector_storage: true,
            async_payload_storage: true,
            write_segment_manifest: true,
            // Deliberately not enabled by `all`: this is a test-only escape hatch that changes
            // mutation semantics, and `all` is enabled in dev and e2e configs.
            append_only_mutations: false,
            serverless_compatible: false,
        }
    }

    fn normalize(mut self) -> Self {
        let serverless_compatible = self.serverless_compatible;

        if self.all {
            self = Self::all();
        }

        if serverless_compatible {
            self.serverless_compatible = true;
            self.write_segment_manifest = true;
            self.append_only_mutations = true;
        }

        self
    }
}

/// Initializes the global feature flags with `flags`. Must only be called once at
/// startup or otherwise throws a warning and discards the values.
pub fn init_feature_flags(flags: FeatureFlags) {
    let flags = flags.normalize();

    let res = FEATURE_FLAGS.set(flags);
    if res.is_err() {
        log::warn!("Feature flags already initialized!");
    }
}

/// Returns the configured global feature flags.
pub fn feature_flags() -> FeatureFlags {
    if let Some(flags) = FEATURE_FLAGS.get() {
        return *flags;
    }

    // They should always be initialized.
    log::warn!("Feature flags not initialized!");
    FeatureFlags::default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_defaults() {
        // Ensure we properly deserialize and don't crash on empty state
        let empty: FeatureFlags = serde_json::from_str("{}").unwrap();
        assert!(empty.is_default());

        assert!(feature_flags().is_default());
        assert!(FeatureFlags::default().is_default());
    }

    #[test]
    fn test_serverless_compatible_enables_sub_flags() {
        let flags = FeatureFlags {
            serverless_compatible: true,
            ..Default::default()
        }
        .normalize();

        assert!(flags.write_segment_manifest);
        assert!(flags.append_only_mutations);
    }

    #[test]
    fn test_serverless_compatible_after_all() {
        let flags = FeatureFlags {
            all: true,
            serverless_compatible: true,
            ..Default::default()
        }
        .normalize();

        assert!(flags.write_segment_manifest);
        assert!(flags.append_only_mutations);
    }
}
