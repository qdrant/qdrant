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
    /// Read-write storage. Values can be updated and deleted, freed blocks are tracked and
    /// reused (bitmask, gaps and regions).
    #[default]
    Dynamic,
    /// Append-only storage for serverless deployments. Files are only ever appended to, existing
    /// bytes are never rewritten. Values cannot be updated or deleted, and must be put in
    /// monotonically increasing point offset order.
    ///
    /// Always reads and writes its files directly on the local filesystem, the configured
    /// universal IO backend is not used in this mode.
    Serverless,
}

/// Configuration options for the storage
#[derive(Debug, Default)]
pub struct StorageOptions {
    /// Size of a page in bytes. Must be a multiple of (`block_size_bytes` * `region_size_blocks`).
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
    /// Default is dynamic
    pub mode: Option<Mode>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct StorageConfig {
    /// Size of a page in bytes
    ///
    /// Default is 32MB
    pub page_size_bytes: usize,

    /// Size of a block in bytes
    ///
    /// Default is 128 bytes
    pub block_size_bytes: usize,

    /// Size of a region in blocks
    ///
    /// Default is 8192 blocks
    pub region_size_blocks: usize,

    /// Use compression
    ///
    /// Default is true
    #[serde(default)]
    pub compression: Compression,

    /// Operating mode of the storage
    ///
    /// Configs written before this field existed default to dynamic
    #[serde(default)]
    pub mode: Mode,
}

impl StorageConfig {
    /// Validate a config read from disk, guarding against corrupt values that would break
    /// pointer arithmetic.
    pub(crate) fn validate(&self) -> Result<(), String> {
        if self.block_size_bytes == 0 {
            return Err("block size must be greater than 0".to_string());
        }
        if self.page_size_bytes == 0 {
            return Err("page size must be greater than 0".to_string());
        }
        if self.region_size_blocks == 0 {
            return Err("region size must be greater than 0".to_string());
        }
        Ok(())
    }
}

impl TryFrom<StorageOptions> for StorageConfig {
    type Error = &'static str;

    fn try_from(options: StorageOptions) -> Result<Self, Self::Error> {
        let page_size_bytes = options.page_size_bytes.unwrap_or(DEFAULT_PAGE_SIZE_BYTES);
        let block_size_bytes = options.block_size_bytes.unwrap_or(DEFAULT_BLOCK_SIZE_BYTES);
        let region_size_blocks = options
            .region_size_blocks
            .map(|x| x as usize)
            .unwrap_or(DEFAULT_REGION_SIZE_BLOCKS);

        if block_size_bytes == 0 {
            return Err("Block size must be greater than 0");
        }

        if region_size_blocks == 0 {
            return Err("Region size must be greater than 0");
        }

        if page_size_bytes == 0 {
            return Err("Page size must be greater than 0");
        }

        let region_size_bytes = block_size_bytes * region_size_blocks;

        if page_size_bytes < region_size_bytes {
            return Err("Page size must be greater than or equal to (block size * region size)");
        }

        if !page_size_bytes.is_multiple_of(region_size_bytes) {
            return Err("Page size must be a multiple of (block size * region size)");
        }

        Ok(Self {
            page_size_bytes,
            block_size_bytes,
            region_size_blocks,
            compression: options.compression.unwrap_or_default(),
            mode: options.mode.unwrap_or_default(),
        })
    }
}

impl From<&StorageConfig> for StorageOptions {
    fn from(config: &StorageConfig) -> Self {
        Self {
            page_size_bytes: Some(config.page_size_bytes),
            block_size_bytes: Some(config.block_size_bytes),
            region_size_blocks: Some(config.region_size_blocks as u16),
            compression: Some(config.compression),
            mode: Some(config.mode),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Configs written before the `mode` field existed must load as dynamic.
    #[test]
    fn test_config_without_mode_is_dynamic() {
        let json = r#"{
            "page_size_bytes": 33554432,
            "block_size_bytes": 128,
            "region_size_blocks": 8192,
            "compression": "LZ4"
        }"#;
        let config: StorageConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.mode, Mode::Dynamic);
    }

    /// The mode is always serialized explicitly, even for the dynamic default.
    #[test]
    fn test_config_serializes_mode() {
        let config = StorageConfig::try_from(StorageOptions::default()).unwrap();
        let json = serde_json::to_string(&config).unwrap();
        assert!(json.contains(r#""mode":"dynamic""#));

        let options = StorageOptions {
            mode: Some(Mode::Serverless),
            ..Default::default()
        };
        let config = StorageConfig::try_from(options).unwrap();
        let json = serde_json::to_string(&config).unwrap();
        assert!(json.contains(r#""mode":"serverless""#));
    }

    /// The mode must survive the config -> options -> config round trip that `clear()` uses.
    #[test]
    fn test_mode_survives_options_round_trip() {
        let options = StorageOptions {
            mode: Some(Mode::Serverless),
            ..Default::default()
        };
        let config = StorageConfig::try_from(options).unwrap();
        let options = StorageOptions::from(&config);
        assert_eq!(options.mode, Some(Mode::Serverless));
        let config = StorageConfig::try_from(options).unwrap();
        assert_eq!(config.mode, Mode::Serverless);
    }
}
