use std::sync::OnceLock;

use serde::Deserialize;

/// Global feature flags, normally initialized when starting Qdrant.
static FEATURE_FLAGS: OnceLock<FeatureFlags> = OnceLock::new();

#[derive(Default, Debug, Deserialize, Clone, Copy)]
pub struct FeatureFlags {
    /// Magic feature flag that enables all features.
    pub all: bool,

    /// Whether to use the new format to persist shard keys
    ///
    /// The old format fails to persist shard key numbers correctly, converting them into strings on
    /// load. While this is false, the new format is only used if any shard key is a number.
    ///
    /// First implemented in Qdrant 1.13.1
    // TODO(1.14): set to true, remove other branches in code, and remove this flag
    #[serde(default)]
    pub use_new_shard_key_mapping_format: bool,

    /// Whether to use the new mutable ID tracker without RocksDB.
    ///
    /// First implemented in Qdrant 1.13.5
    // TODO(1.14): set to true, remove other branches in code, and remove this flag
    #[serde(default)]
    pub use_mutable_id_tracker_without_rocksdb: bool,

    /// Whether to skip usage of RocksDB in immutable payload indices.
    ///
    /// First implemented in Qdrant 1.13.5
    // TODO(1.14): remove for release
    // ToDo(mmap-payload-index): remove for release
    #[serde(default)]
    pub payload_index_skip_rocksdb: bool,
}

/// Initializes the global feature flags with `flags`. Must only be called once at
/// startup or otherwise throws a warning and discards the values.
pub fn init_feature_flags(mut flags: FeatureFlags) {
    let FeatureFlags {
        all,
        use_new_shard_key_mapping_format,
        use_mutable_id_tracker_without_rocksdb,
        payload_index_skip_rocksdb,
    } = &mut flags;

    // If all is set, explicitly set all feature flags
    if *all {
        *use_new_shard_key_mapping_format = true;
        *use_mutable_id_tracker_without_rocksdb = true;
        *payload_index_skip_rocksdb = true;
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
