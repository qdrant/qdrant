use std::sync::OnceLock;

use serde::Deserialize;

/// Global feature flags, normally initialized when starting Qdrant.
pub static FEATURE_FLAGS: OnceLock<FeatureFlags> = OnceLock::new();

#[derive(Default, Debug, Deserialize, Clone, Copy)]
pub struct FeatureFlags {
    /// Whether to use the new format to persist shard keys
    ///
    /// The old format fails to persist shard key numbers correctly, converting them into strings on
    /// load. While this is false, the new format is only used if any shard key is a number.
    // TODO(1.14): set to true, remove other branches in code, and remove this flag
    #[serde(default)]
    pub use_new_shard_key_mapping_format: bool,
}
