use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::hash::Hash;
use std::io::{Read, Write};
use std::num::{NonZeroU32, NonZeroU64};
use std::path::Path;

use atomicwrites::AtomicFile;
use atomicwrites::OverwriteBehavior::AllowOverwrite;
use schemars::JsonSchema;
use segment::data_types::vectors::DEFAULT_VECTOR_NAME;
use segment::types::{Distance, HnswConfig, VectorDataConfig};
use serde::{Deserialize, Serialize};
use wal::WalOptions;

use crate::operations::types::{CollectionError, CollectionResult};
use crate::optimizers_builder::OptimizersConfig;

pub const COLLECTION_CONFIG_FILE: &str = "config.json";

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, PartialEq, Eq)]
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

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub struct CollectionParams {
    /// Configuration of the vector storage
    pub vectors: VectorsConfig,
    /// Number of shards the collection has
    #[serde(default = "default_shard_number")]
    pub shard_number: NonZeroU32,
    /// Number of replicas for each shard
    #[serde(default = "default_replication_factor")]
    pub replication_factor: NonZeroU32,
    /// If true - point's payload will not be stored in memory.
    /// It will be read from the disk every time it is requested.
    /// This setting saves RAM by (slightly) increasing the response time.
    /// Note: those payload values that are involved in filtering and are indexed - remain in RAM.
    #[serde(default = "default_on_disk_payload")]
    pub on_disk_payload: bool,
}

/// Params of single vector data storage
#[derive(Debug, Hash, Deserialize, Serialize, JsonSchema, Clone, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub struct VectorParams {
    /// Size of a vectors used
    pub size: NonZeroU64,
    /// Type of distance function used for measuring distance between vectors
    pub distance: Distance,
}

/// Vector params separator for single and multiple vector modes
/// Single mode:
///
/// { "size": 128, "distance": "Cosine" }
///
/// or multiple mode:
///
/// {
///      "default": {
///          "size": 128,
///          "distance": "Cosine"
///      }
/// }
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, PartialEq, Hash, Eq)]
#[serde(rename_all = "snake_case")]
#[serde(untagged)]
pub enum VectorsConfig {
    Single(VectorParams),
    Multi(BTreeMap<String, VectorParams>),
}

impl From<VectorParams> for VectorsConfig {
    fn from(params: VectorParams) -> Self {
        VectorsConfig::Single(params)
    }
}

impl VectorsConfig {
    fn get_params(&self, name: &str) -> Option<&VectorParams> {
        match self {
            VectorsConfig::Single(params) => {
                if name == DEFAULT_VECTOR_NAME {
                    Some(params)
                } else {
                    None
                }
            }
            VectorsConfig::Multi(params) => params.get(name),
        }
    }
}

fn default_shard_number() -> NonZeroU32 {
    NonZeroU32::new(1).unwrap()
}

pub fn default_replication_factor() -> NonZeroU32 {
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

    /// Check if collection config exists
    pub fn check(path: &Path) -> bool {
        let config_path = path.join(COLLECTION_CONFIG_FILE);
        config_path.exists()
    }
}

impl CollectionParams {
    pub fn get_vector_params(&self, vector_name: &str) -> CollectionResult<VectorParams> {
        if vector_name == DEFAULT_VECTOR_NAME {
            self.vectors
                .get_params(vector_name)
                .cloned()
                .ok_or_else(|| CollectionError::BadInput {
                    description: "Default vector params are not specified in config".to_string(),
                })
        } else {
            self.vectors
                .get_params(vector_name)
                .cloned()
                .ok_or_else(|| CollectionError::BadInput {
                    description: format!(
                        "vector params for {vector_name} are not specified in config"
                    ),
                })
        }
    }

    pub fn get_all_vector_params(&self) -> CollectionResult<HashMap<String, VectorDataConfig>> {
        let vector_config = match &self.vectors {
            VectorsConfig::Single(params) => {
                let mut map = HashMap::new();
                map.insert(
                    DEFAULT_VECTOR_NAME.to_string(),
                    VectorDataConfig {
                        size: params.size.get() as usize,
                        distance: params.distance,
                    },
                );
                map
            }
            VectorsConfig::Multi(ref map) => map
                .iter()
                .map(|(name, params)| {
                    (
                        name.clone(),
                        VectorDataConfig {
                            size: params.size.get() as usize,
                            distance: params.distance,
                        },
                    )
                })
                .collect(),
        };
        Ok(vector_config)
    }
}
