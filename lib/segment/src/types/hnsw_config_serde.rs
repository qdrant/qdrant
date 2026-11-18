//! Deserialization of [`HnswConfig`] that tolerates a missing `ef_construct`.
//!
//! Newer versions may leave `ef_construct` unset, in the persisted collection config
//! (`config.json`) and the edge config, to derive it from other parameters. This lets a version
//! rolled back to read those configs, with the default `ef_construct`.
//!
//! The JSON schema of [`HnswConfig`] still has `ef_construct` as required: the API always
//! returns it.

use serde::Deserialize;

use super::{DEFAULT_HNSW_EF_CONSTRUCT, HnswConfig, Memory, default_max_indexing_threads};

/// Mirror of [`HnswConfig`] for deserialization, see the module docs.
///
/// The serde attributes of the fields must match those of [`HnswConfig`].
#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) struct HnswConfigDeserialize {
    m: usize,
    #[serde(default = "default_ef_construct")]
    ef_construct: usize,
    #[serde(alias = "full_scan_threshold_kb")]
    full_scan_threshold: usize,
    #[serde(default = "default_max_indexing_threads")]
    max_indexing_threads: usize,
    #[serde(default)]
    on_disk: Option<bool>,
    #[serde(default)]
    memory: Option<Memory>,
    #[serde(default)]
    payload_m: Option<usize>,
    #[serde(default)]
    inline_storage: Option<bool>,
}

const fn default_ef_construct() -> usize {
    DEFAULT_HNSW_EF_CONSTRUCT
}

impl From<HnswConfigDeserialize> for HnswConfig {
    fn from(config: HnswConfigDeserialize) -> Self {
        let HnswConfigDeserialize {
            m,
            ef_construct,
            full_scan_threshold,
            max_indexing_threads,
            on_disk,
            memory,
            payload_m,
            inline_storage,
        } = config;

        #[expect(deprecated)]
        HnswConfig {
            m,
            ef_construct,
            full_scan_threshold,
            max_indexing_threads,
            on_disk,
            memory,
            payload_m,
            inline_storage,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_ef_construct_reads_as_default() {
        let config: HnswConfig =
            serde_json::from_str(r#"{"m": 16, "full_scan_threshold": 10000}"#).unwrap();
        assert_eq!(config.ef_construct, DEFAULT_HNSW_EF_CONSTRUCT);
    }

    #[test]
    fn round_trips() {
        let config = HnswConfig {
            ef_construct: 123,
            payload_m: Some(8),
            memory: Some(Memory::Cold),
            ..HnswConfig::default()
        };
        let json = serde_json::to_string(&config).unwrap();
        assert_eq!(serde_json::from_str::<HnswConfig>(&json).unwrap(), config);
        // Still reads the alias and the newer fields, which it ignores
        let config: HnswConfig = serde_json::from_str(
            r#"{"m": 16, "full_scan_threshold_kb": 5, "indexing_effort": "high"}"#,
        )
        .unwrap();
        assert_eq!(config.full_scan_threshold, 5);
    }
}
