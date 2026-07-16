use std::borrow::Cow;

use lz4_flex::compress_prepend_size;
use serde::{Deserialize, Serialize};

/// Expect JSON values to have roughly 3–5 fields with mostly small values.
/// For 1M values, this would require 128MB of memory.
pub const DEFAULT_BLOCK_SIZE_BYTES: usize = 128;

/// Default page size used when not specified
pub const DEFAULT_PAGE_SIZE_BYTES: usize = 32 * 1024 * 1024; // 32MB

pub const DEFAULT_REGION_SIZE_BLOCKS: usize = 8_192;

#[derive(Debug, Copy, Clone, Serialize, Deserialize, Default)]
pub enum Compression {
    None,
    #[default]
    LZ4,
}

impl Compression {
    pub(crate) fn compress(self, value: Vec<u8>) -> Vec<u8> {
        match self {
            Compression::None => value,
            Compression::LZ4 => compress_lz4(&value),
        }
    }

    pub(crate) fn decompress(self, value: Cow<'_, [u8]>) -> Cow<'_, [u8]> {
        match self {
            Compression::None => value,
            Compression::LZ4 => decompress_lz4(&value).into(),
        }
    }
}

#[inline]
pub(crate) fn compress_lz4(value: &[u8]) -> Vec<u8> {
    compress_prepend_size(value)
}

#[inline]
pub(crate) fn decompress_lz4(value: &[u8]) -> Vec<u8> {
    lz4_flex::decompress_size_prepended(value).unwrap()
}

/// Operating mode of the storage
#[derive(Debug, Default, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Mode {
    /// Use Gridstore
    ///
    /// Read-write storage. Values can be updated and deleted, freed blocks are tracked and
    /// reused (bitmask, gaps and regions).
    #[default]
    Mutable,
    /// Use Logstore
    ///
    /// Append-only storage for serverless deployments. Files are only ever appended to, existing
    /// bytes are never rewritten. Values cannot be updated or deleted, and must be put in
    /// monotonically increasing point offset order.
    ///
    /// Values are packed back to back in page files, without blocks or alignment. Once a page
    /// reaches the configured page size, a new page is started, bounding the size of and the
    /// number of appends to each file (object stores limit appends per object).
    ///
    /// Puts buffer both the value data and the mapping in memory; each flush batches them into a
    /// single append per file. All files are read and written through the configured universal
    /// IO backend.
    AppendOnly,
}

/// Configuration options for the storage
#[derive(Debug, Default)]
pub struct StorageOptions {
    /// Size of a page in bytes.
    ///
    /// In mutable mode, must be a multiple of (`block_size_bytes` * `region_size_blocks`). In
    /// append-only mode, the capacity at which a page rolls over to the next one.
    ///
    /// Default is 32MB
    pub page_size_bytes: Option<usize>,

    /// Size of a block in bytes
    ///
    /// Default is 128 bytes
    pub block_size_bytes: Option<usize>,

    /// Size of a region in blocks
    ///
    /// Default is 8192 blocks
    pub region_size_blocks: Option<u16>,

    /// Use compression
    ///
    /// Default is LZ4
    pub compression: Option<Compression>,

    /// Operating mode of the storage
    ///
    /// Default is mutable
    pub mode: Option<Mode>,
}

/// Configuration of a Gridstore, the mutable mode storage.
///
/// The serde representation is the persisted config format, see [`StorageConfig`]. Configs
/// written before the mode field existed deserialize into exactly this type, so the field names
/// must not change.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct GridstoreConfig {
    /// Size of a page in bytes
    pub page_size_bytes: usize,

    /// Size of a block in bytes
    pub block_size_bytes: usize,

    /// Size of a region in blocks
    pub region_size_blocks: usize,

    /// Use compression
    #[serde(default)]
    pub compression: Compression,
}

impl GridstoreConfig {
    /// Validate the configured sizes, guarding against values that would break pointer
    /// arithmetic or the page layout.
    fn validate(&self) -> Result<(), String> {
        if self.block_size_bytes == 0 {
            return Err("block size must be greater than 0".to_string());
        }

        if self.region_size_blocks == 0 {
            return Err("region size must be greater than 0".to_string());
        }

        if self.page_size_bytes == 0 {
            return Err("page size must be greater than 0".to_string());
        }

        let region_size_bytes = self.block_size_bytes * self.region_size_blocks;

        if self.page_size_bytes < region_size_bytes {
            return Err(
                "page size must be greater than or equal to (block size * region size)".to_string(),
            );
        }

        if !self.page_size_bytes.is_multiple_of(region_size_bytes) {
            return Err("page size must be a multiple of (block size * region size)".to_string());
        }

        Ok(())
    }
}

impl TryFrom<StorageOptions> for GridstoreConfig {
    type Error = String;

    fn try_from(options: StorageOptions) -> Result<Self, Self::Error> {
        let config = Self {
            page_size_bytes: options.page_size_bytes.unwrap_or(DEFAULT_PAGE_SIZE_BYTES),
            block_size_bytes: options.block_size_bytes.unwrap_or(DEFAULT_BLOCK_SIZE_BYTES),
            region_size_blocks: options
                .region_size_blocks
                .map(usize::from)
                .unwrap_or(DEFAULT_REGION_SIZE_BLOCKS),
            compression: options.compression.unwrap_or_default(),
        };
        config.validate()?;
        Ok(config)
    }
}

/// Configuration of a Logstore, the append-only mode storage.
///
/// The append-only mode has no blocks or regions: values are packed back to back in page files.
///
/// The serde representation is the persisted config format, see [`StorageConfig`], so the field
/// names must not change.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct LogstoreConfig {
    /// Capacity in bytes at which a page rolls over to the next one
    ///
    /// Persisted as `page_size_bytes`, the field name shared with the mutable mode.
    #[serde(rename = "page_size_bytes")]
    pub page_capacity_bytes: usize,

    /// Use compression
    #[serde(default)]
    pub compression: Compression,
}

impl LogstoreConfig {
    /// Validate the configured sizes.
    fn validate(&self) -> Result<(), String> {
        if self.page_capacity_bytes == 0 {
            return Err("page size must be greater than 0".to_string());
        }

        Ok(())
    }
}

impl TryFrom<StorageOptions> for LogstoreConfig {
    type Error = String;

    fn try_from(options: StorageOptions) -> Result<Self, Self::Error> {
        // Blocks and regions are mutable mode concepts, those options are ignored here
        let config = Self {
            page_capacity_bytes: options.page_size_bytes.unwrap_or(DEFAULT_PAGE_SIZE_BYTES),
            compression: options.compression.unwrap_or_default(),
        };
        config.validate()?;
        Ok(config)
    }
}

/// On-disk configuration of the storage, the serde representation of `config.json`.
///
/// Serializes as the mode specific config with a `"mode"` tag added. Deserialization cannot rely
/// on the serde tag: configs written before the mode field existed carry no tag, and a tagged
/// enum cannot fall back to a default variant. [`Self::from_json`] probes the mode separately
/// instead, defaulting to mutable since Gridstore was the only variant back then.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub(crate) enum StorageConfig {
    Mutable(GridstoreConfig),
    AppendOnly(LogstoreConfig),
}

impl StorageConfig {
    /// Deserialize a persisted config, selecting the mode specific type via the `"mode"` field.
    ///
    /// Unknown fields are ignored: an append-only config written when both modes still shared
    /// one config type is tolerated even though it carries block and region sizes.
    pub(crate) fn from_json(bytes: &[u8]) -> Result<Self, serde_json::Error> {
        #[derive(Deserialize)]
        struct ModeProbe {
            #[serde(default)]
            mode: Mode,
        }

        let ModeProbe { mode } = serde_json::from_slice(bytes)?;
        match mode {
            Mode::Mutable => Ok(Self::Mutable(serde_json::from_slice(bytes)?)),
            Mode::AppendOnly => Ok(Self::AppendOnly(serde_json::from_slice(bytes)?)),
        }
    }

    /// Validate the mode specific config values.
    pub(crate) fn validate(&self) -> Result<(), String> {
        match self {
            Self::Mutable(config) => config.validate(),
            Self::AppendOnly(config) => config.validate(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Configs written before the `mode` field existed must load as a mutable Gridstore config.
    #[test]
    fn test_config_without_mode_is_mutable() {
        let json = r#"{
            "page_size_bytes": 33554432,
            "block_size_bytes": 128,
            "region_size_blocks": 8192,
            "compression": "LZ4"
        }"#;
        let StorageConfig::Mutable(config) = StorageConfig::from_json(json.as_bytes()).unwrap()
        else {
            panic!("expected a mutable config");
        };
        assert_eq!(config.page_size_bytes, 33554432);
        assert_eq!(config.block_size_bytes, 128);
        assert_eq!(config.region_size_blocks, 8192);
    }

    /// The mode is always serialized explicitly, even for the mutable default. Block and region
    /// sizes are mutable mode concepts and are not written for the append-only mode, which
    /// persists its page capacity under the shared `page_size_bytes` name.
    #[test]
    fn test_config_serializes_mode() {
        let config = GridstoreConfig::try_from(StorageOptions::default()).unwrap();
        let json = serde_json::to_string(&StorageConfig::Mutable(config)).unwrap();
        assert!(json.contains(r#""mode":"mutable""#));
        assert!(json.contains(r#""block_size_bytes""#));

        let config = LogstoreConfig::try_from(StorageOptions::default()).unwrap();
        let json = serde_json::to_string(&StorageConfig::AppendOnly(config)).unwrap();
        assert!(json.contains(r#""mode":"append_only""#));
        assert!(json.contains(r#""page_size_bytes""#));
        assert!(!json.contains(r#""page_capacity_bytes""#));
        assert!(!json.contains(r#""block_size_bytes""#));
        assert!(!json.contains(r#""region_size_blocks""#));
    }

    /// The mode specific configs must survive the round trip through the persisted format.
    #[test]
    fn test_configs_survive_storage_config_round_trip() {
        let config = GridstoreConfig::try_from(StorageOptions::default()).unwrap();
        let json = serde_json::to_vec(&StorageConfig::Mutable(config.clone())).unwrap();
        let StorageConfig::Mutable(restored) = StorageConfig::from_json(&json).unwrap() else {
            panic!("expected a mutable config");
        };
        assert_eq!(restored.page_size_bytes, config.page_size_bytes);
        assert_eq!(restored.block_size_bytes, config.block_size_bytes);
        assert_eq!(restored.region_size_blocks, config.region_size_blocks);

        let config = LogstoreConfig::try_from(StorageOptions::default()).unwrap();
        let json = serde_json::to_vec(&StorageConfig::AppendOnly(config.clone())).unwrap();
        let StorageConfig::AppendOnly(restored) = StorageConfig::from_json(&json).unwrap() else {
            panic!("expected an append-only config");
        };
        assert_eq!(restored.page_capacity_bytes, config.page_capacity_bytes);
    }

    /// A mutable mode config without block or region sizes must be rejected. The append-only
    /// mode has no blocks: the same config is fine there, and stray block fields from configs
    /// written when both modes still shared one config type are ignored.
    #[test]
    fn test_gridstore_config_requires_block_and_region_sizes() {
        let json = r#"{"page_size_bytes": 33554432, "mode": "mutable"}"#;
        assert!(StorageConfig::from_json(json.as_bytes()).is_err());

        let json = r#"{"page_size_bytes": 33554432, "mode": "append_only"}"#;
        assert!(matches!(
            StorageConfig::from_json(json.as_bytes()).unwrap(),
            StorageConfig::AppendOnly(_)
        ));

        let json = r#"{
            "page_size_bytes": 33554432,
            "block_size_bytes": 128,
            "region_size_blocks": 8192,
            "mode": "append_only"
        }"#;
        assert!(matches!(
            StorageConfig::from_json(json.as_bytes()).unwrap(),
            StorageConfig::AppendOnly(_)
        ));
    }
}
