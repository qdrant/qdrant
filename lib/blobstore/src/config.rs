use std::borrow::Cow;

use lz4_flex::compress_prepend_size;
use serde::{Deserialize, Serialize};

/// Expect JSON values to have roughly 3–5 fields with mostly small values.
/// For 1M values, this would require 128MB of memory.
pub const DEFAULT_BLOCK_SIZE_BYTES: usize = 128;

/// Default page size in bytes.
pub const DEFAULT_PAGE_SIZE_BYTES: usize = 32 * 1024 * 1024; // 32MB

/// Default number of blocks per region.
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

/// Configuration of Gridstore, the mutable mode storage.
///
/// The serde representation is the persisted config format, see [`StorageOptions`]. Configs
/// written before the mode field existed deserialize into exactly this type, so the field names
/// must not change.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GridstoreConfig {
    /// Size of a page in bytes
    ///
    /// Must be a multiple of (`block_size_bytes` * `region_size_blocks`)
    pub page_size_bytes: usize,

    /// Size of a block in bytes
    pub block_size_bytes: usize,

    /// Number of blocks in a region
    ///
    /// Must fit in 16 bits (less than 65536)
    pub region_size_blocks: usize,

    /// Use compression
    #[serde(default)]
    pub compression: Compression,
}

impl GridstoreConfig {
    /// Default options
    pub const DEFAULT: Self = Self {
        page_size_bytes: DEFAULT_PAGE_SIZE_BYTES,
        block_size_bytes: DEFAULT_BLOCK_SIZE_BYTES,
        region_size_blocks: DEFAULT_REGION_SIZE_BLOCKS,
        compression: Compression::LZ4,
    };

    /// Validate the configured sizes, guarding against values that would break pointer
    /// arithmetic or the page layout.
    fn validate(&self) -> Result<(), String> {
        if self.block_size_bytes == 0 {
            return Err("block size must be greater than 0".to_string());
        }

        if self.region_size_blocks == 0 {
            return Err("region size must be greater than 0".to_string());
        }

        // The bitmask tracks gaps with 16 bit counters
        if self.region_size_blocks > usize::from(u16::MAX) {
            return Err(format!(
                "region size must fit in 16 bits, got {}",
                self.region_size_blocks,
            ));
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

/// Configuration of a Logstore, the append-only mode storage.
///
/// The append-only mode has no blocks or regions: values are packed back to back in page files.
///
/// The serde representation is the persisted config format, see [`StorageOptions`], so the field
/// names must not change.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogstoreConfig {
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
    /// Default options
    pub const DEFAULT: Self = Self {
        page_capacity_bytes: DEFAULT_PAGE_SIZE_BYTES,
        compression: Compression::LZ4,
    };

    /// Validate the configured sizes.
    fn validate(&self) -> Result<(), String> {
        if self.page_capacity_bytes == 0 {
            return Err("page size must be greater than 0".to_string());
        }

        Ok(())
    }
}

/// Creation options for a storage: the mode specific options of the variant to create.
///
/// The variant chosen at creation decides the storage's operating mode for its whole lifetime:
/// the two modes persist incompatible file formats.
///
/// Doubles as the on-disk configuration of the storage, the serde representation of
/// `config.json`. Uses the `try_from` implementation to default to mutable when
/// `"mode"` tag is not present.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case", try_from = "serde_json::Value")]
pub enum StorageConfig {
    Mutable(GridstoreConfig),
    AppendOnly(LogstoreConfig),
}

impl TryFrom<serde_json::Value> for StorageConfig {
    type Error = serde_json::Error;

    fn try_from(value: serde_json::Value) -> Result<Self, Self::Error> {
        let mode = match value.get("mode") {
            Some(mode) => Mode::deserialize(mode)?,
            None => Mode::default(),
        };
        match mode {
            Mode::Mutable => GridstoreConfig::deserialize(value).map(Self::Mutable),
            Mode::AppendOnly => LogstoreConfig::deserialize(value).map(Self::AppendOnly),
        }
    }
}

impl StorageConfig {
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
        let StorageConfig::Mutable(config) = serde_json::from_str(json).unwrap()
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
        let config = GridstoreConfig::DEFAULT;
        let json = serde_json::to_string(&StorageConfig::Mutable(config)).unwrap();
        assert!(json.contains(r#""mode":"mutable""#));
        assert!(json.contains(r#""block_size_bytes""#));

        let config = LogstoreConfig::DEFAULT;
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
        let config = GridstoreConfig::DEFAULT;
        let json = serde_json::to_vec(&StorageConfig::Mutable(config.clone())).unwrap();
        let StorageConfig::Mutable(restored) = serde_json::from_slice(&json).unwrap() else {
            panic!("expected a mutable config");
        };
        assert_eq!(restored.page_size_bytes, config.page_size_bytes);
        assert_eq!(restored.block_size_bytes, config.block_size_bytes);
        assert_eq!(restored.region_size_blocks, config.region_size_blocks);

        let config = LogstoreConfig::DEFAULT;
        let json = serde_json::to_vec(&StorageConfig::AppendOnly(config.clone())).unwrap();
        let StorageConfig::AppendOnly(restored) = serde_json::from_slice(&json).unwrap() else {
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
        assert!(serde_json::from_str::<StorageConfig>(json).is_err());

        let json = r#"{"page_size_bytes": 33554432, "mode": "append_only"}"#;
        assert!(matches!(
            serde_json::from_str(json).unwrap(),
            StorageConfig::AppendOnly(_)
        ));

        let json = r#"{
            "page_size_bytes": 33554432,
            "block_size_bytes": 128,
            "region_size_blocks": 8192,
            "mode": "append_only"
        }"#;
        assert!(matches!(
            serde_json::from_str(json).unwrap(),
            StorageConfig::AppendOnly(_)
        ));
    }
}
