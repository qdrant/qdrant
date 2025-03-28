use std::sync::OnceLock;

use serde::Deserialize;

/// Global feature flags, normally initialized when starting Qdrant.
static FEATURE_FLAGS: OnceLock<FeatureFlags> = OnceLock::new();

#[derive(Debug, Deserialize, Clone, Copy, Eq, PartialEq)]
#[serde(default)]
pub struct FeatureFlags {
    /// Magic feature flag that enables all features.
    ///
    /// Note that this will only be applied to all flags when passed into [`init_feature_flags`].
    all: bool,

    /// Whether to skip usage of RocksDB in immutable payload indices.
    ///
    /// First implemented in Qdrant 1.13.5
    // TODO(1.14): remove for release
    // ToDo(mmap-payload-index): remove for release
    pub payload_index_skip_rocksdb: bool,

    /// Whether to use incremental HNSW building.
    pub incremental_hnsw_building: bool,
}

impl Default for FeatureFlags {
    fn default() -> FeatureFlags {
        FeatureFlags {
            all: false,
            payload_index_skip_rocksdb: false,
            incremental_hnsw_building: true,
        }
    }
}

impl FeatureFlags {
    /// Check if the feature flags are set to default values.
    pub fn is_default(self) -> bool {
        self == FeatureFlags::default()
    }
}

/// Initializes the global feature flags with `flags`. Must only be called once at
/// startup or otherwise throws a warning and discards the values.
pub fn init_feature_flags(mut flags: FeatureFlags) {
    let FeatureFlags {
        all,
        payload_index_skip_rocksdb,
        incremental_hnsw_building,
    } = &mut flags;

    // If all is set, explicitly set all feature flags
    if *all {
        *payload_index_skip_rocksdb = true;
        *incremental_hnsw_building = true;
    }

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
}
