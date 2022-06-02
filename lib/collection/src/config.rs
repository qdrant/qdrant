use std::fs::File;
use std::io::{Read, Write};
use std::num::NonZeroU32;
use std::path::Path;

use atomicwrites::AtomicFile;
use atomicwrites::OverwriteBehavior::AllowOverwrite;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use wal::WalOptions;

use segment::types::{Distance, HnswConfig};

use crate::operations::types::{CollectionError, CollectionResult};
use crate::optimizers_builder::OptimizersConfig;

pub const COLLECTION_CONFIG_FILE: &str = "config.json";

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, PartialEq)]
pub struct WalConfig {
    /// Size of a single WAL segment in MB
    pub wal_capacity_mb: usize,
    /// Number of WAL segments to create ahead of actually used ones
    pub wal_segments_ahead: usize,
}

impl From<&WalConfig> for WalOptions {
    fn from(config: &WalConfig) -> Self {
        WalOptions {
            segment_capacity: config.wal_capacity_mb * 1024 * 1024,
            segment_queue_len: config.wal_segments_ahead,
        }
    }
}

impl Default for WalConfig {
    fn default() -> Self {
        WalConfig {
            wal_capacity_mb: 32,
            wal_segments_ahead: 0,
        }
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, PartialEq)]
#[serde(rename_all = "snake_case")]
pub struct CollectionParams {
    /// Size of a vectors used
    pub vector_size: usize,
    /// Type of distance function used for measuring distance between vectors
    pub distance: Distance,
    /// Number of shards the collection has
    #[serde(default = "default_shard_number")]
    pub shard_number: NonZeroU32,
    /// If true - point's payload will not be stored in memory.
    /// It will be read from the disk every time it is requested.
    /// This setting saves RAM by (slightly) increasing the response time.
    /// Note: those payload values that are involved in filtering and are indexed - remain in RAM.
    #[serde(default = "default_on_disk_payload")]
    pub on_disk_payload: bool,
}

fn default_shard_number() -> NonZeroU32 {
    NonZeroU32::new(1).unwrap()
}
fn default_on_disk_payload() -> bool {
    false
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, PartialEq)]
pub struct CollectionConfig {
    pub params: CollectionParams,
    pub hnsw_config: HnswConfig,
    pub optimizer_config: OptimizersConfig,
    pub wal_config: WalConfig,
}

impl CollectionConfig {
    pub fn save(&self, path: &Path) -> CollectionResult<()> {
        let config_path = path.join(COLLECTION_CONFIG_FILE);
        let af = AtomicFile::new(&config_path, AllowOverwrite);
        let state_bytes = serde_json::to_vec(self).unwrap();
        af.write(|f| f.write_all(&state_bytes))
            .map_err(|err| CollectionError::ServiceError {
                error: format!("Can't write {:?}, error: {}", config_path, err),
            })?;
        Ok(())
    }

    pub fn load(path: &Path) -> CollectionResult<Self> {
        let config_path = path.join(COLLECTION_CONFIG_FILE);
        let mut contents = String::new();
        let mut file = File::open(config_path)?;
        file.read_to_string(&mut contents)?;
        Ok(serde_json::from_str(&contents)?)
    }
}
