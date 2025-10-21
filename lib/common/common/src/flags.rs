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

    /// Skip usage of RocksDB in new immutable payload indices.
    ///
    /// First implemented in Qdrant 1.13.5.
    /// Enabled by default in Qdrant 1.14.1.
    pub payload_index_skip_rocksdb: bool,

    /// Skip usage of RocksDB in new mutable payload indices.
    ///
    /// First implemented in Qdrant 1.15.0.
    /// Enabled by default in Qdrant 1.16.0.
    pub payload_index_skip_mutable_rocksdb: bool,

    /// Skip usage of RocksDB in new payload storages.
    ///
    /// On-disk payload storages never use Gridstore.
    ///
    /// First implemented in Qdrant 1.15.0.
    /// Enabled by default in Qdrant 1.16.0.
    pub payload_storage_skip_rocksdb: bool,

    /// Use incremental HNSW building.
    ///
    /// Enabled by default in Qdrant 1.14.1.
    pub incremental_hnsw_building: bool,

    /// Migrate RocksDB based ID trackers into file based ID tracker on start.
    ///
    /// Enabled by default in Qdrant 1.15.0.
    pub migrate_rocksdb_id_tracker: bool,

    /// Migrate RocksDB based vector storages into new format on start.
    // TODO(1.16.1): enable by default
    pub migrate_rocksdb_vector_storage: bool,

    /// Migrate RocksDB based payload storages into new format on start.
    // TODO(1.16.1): enable by default
    pub migrate_rocksdb_payload_storage: bool,

    /// Migrate RocksDB based payload indices into new format on start.
    ///
    /// Rebuilds a new payload index from scratch.
    // TODO(1.16.1): enable by default
    pub migrate_rocksdb_payload_indices: bool,

    /// Use appendable quantization in appendable plain segments.
    // TODO(1.16.0): enable by default
    pub appendable_quantization: bool,
}

impl Default for FeatureFlags {
    fn default() -> FeatureFlags {
        FeatureFlags {
            all: false,
            payload_index_skip_rocksdb: true,
            payload_index_skip_mutable_rocksdb: true,
            payload_storage_skip_rocksdb: true,
            incremental_hnsw_building: true,
            migrate_rocksdb_id_tracker: true,
            migrate_rocksdb_vector_storage: false,
            migrate_rocksdb_payload_storage: false,
            migrate_rocksdb_payload_indices: false,
            appendable_quantization: false,
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
        payload_index_skip_mutable_rocksdb,
        payload_storage_skip_rocksdb,
        incremental_hnsw_building,
        migrate_rocksdb_id_tracker,
        migrate_rocksdb_vector_storage,
        migrate_rocksdb_payload_storage,
        migrate_rocksdb_payload_indices,
        appendable_quantization,
    } = &mut flags;

    // If all is set, explicitly set all feature flags
    if *all {
        *payload_index_skip_rocksdb = true;
        *payload_index_skip_mutable_rocksdb = true;
        *payload_storage_skip_rocksdb = true;
        *incremental_hnsw_building = true;
        *migrate_rocksdb_id_tracker = true;
        *migrate_rocksdb_vector_storage = true;
        *migrate_rocksdb_payload_storage = true;
        *migrate_rocksdb_payload_indices = true;
        *appendable_quantization = true;
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
